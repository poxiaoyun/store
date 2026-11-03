package inmemory_test

import (
	"context"
	"fmt"
	"math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	commonerrors "xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/inmemory"
	"xiaoshiai.cn/store/selector"
)

type queryRecord struct {
	store.ObjectMeta `json:",inline"`
	Group            string    `json:"group"`
	Number           *int64    `json:"number,omitempty"`
	Code             string    `json:"code"`
	At               time.Time `json:"at"`
	Enabled          bool      `json:"enabled"`
	Value            any       `json:"value"`
}

func querySchema(t testing.TB, indexed bool) *store.Schema {
	t.Helper()
	definition := store.ResourceSchema{ScopeKeys: []string{"tenant"}}
	if indexed {
		definition.Indexes = []store.Index{
			{Fields: []string{"group", "number", "id"}},
			{Fields: []string{"code", "id"}},
			{Fields: []string{"at", "id"}},
			{Fields: []string{"enabled", "id"}},
			{Fields: []string{"labels.region", "number", "id"}},
			{Fields: []string{"tenant", "group", "number", "id"}},
			{Fields: []string{"value", "id"}},
		}
	}
	schema := store.NewSchema()
	if err := schema.Register(&queryRecord{}, definition); err != nil {
		t.Fatal(err)
	}
	return schema
}

func queryStores(t *testing.T, objects []*queryRecord) (map[string]store.Store, *inmemory.InMemory, *inmemory.CacheStore) {
	t.Helper()
	scan, err := inmemory.New(querySchema(t, false))
	if err != nil {
		t.Fatal(err)
	}
	indexed, err := inmemory.New(querySchema(t, true))
	if err != nil {
		t.Fatal(err)
	}
	for _, source := range []*inmemory.InMemory{scan, indexed} {
		for _, object := range objects {
			if err := source.Create(t.Context(), object); err != nil {
				t.Fatal(err)
			}
		}
	}
	cache := startCache(t, indexed, &queryRecord{})
	waitSync(t, cache)
	return map[string]store.Store{"scan": scan, "indexed": indexed, "cache": cache}, indexed, cache
}

func queryIDs(t testing.TB, source store.Store, options ...store.ListOption) []string {
	t.Helper()
	result := &store.List[queryRecord]{}
	if err := source.List(t.Context(), result, append(options, store.WithResourceVersion(0))...); err != nil {
		t.Fatal(err)
	}
	ids := []string{}
	for _, object := range result.Items {
		ids = append(ids, object.ID)
	}
	return ids
}

func TestOrderedIndexQueries(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	n10, n20, n30, n100 := int64(10), int64(20), int64(30), int64(100)
	objects := []*queryRecord{
		{ObjectMeta: store.ObjectMeta{ID: "a", Labels: map[string]string{"region": "east"}}, Group: "g", Number: &n10, Code: "2", At: at},
		{ObjectMeta: store.ObjectMeta{ID: "b", Labels: map[string]string{"region": "west"}}, Group: "g", Number: &n20, Code: "10", At: at.Add(time.Hour), Enabled: true},
		{ObjectMeta: store.ObjectMeta{ID: "c", Labels: map[string]string{"region": "east"}}, Group: "g", Number: &n20, Code: "01", At: at.Add(2 * time.Hour), Enabled: true},
		{ObjectMeta: store.ObjectMeta{ID: "d", Labels: map[string]string{"region": "east"}}, Group: "g", Number: &n30, Code: "1", At: at.Add(3 * time.Hour)},
		{ObjectMeta: store.ObjectMeta{ID: "e", Labels: map[string]string{"region": "east"}}, Group: "g", Code: "z", At: at.Add(4 * time.Hour), Enabled: true},
		{ObjectMeta: store.ObjectMeta{ID: "x"}, Group: "other", Number: &n100, Code: "1", At: at.Add(5 * time.Hour)},
	}
	stores, _, _ := queryStores(t, objects)
	cases := []struct {
		name   string
		fields store.Requirements
		sort   string
		extra  []store.ListOption
		want   []string
	}{
		{name: "inclusive upper", fields: store.Requirements{selector.RequirementGreaterThan("number", 10), selector.RequirementLessThanOrEqual("number", 30)}, sort: "number+,id+", want: []string{"b", "c", "d"}},
		{name: "reverse bounds", fields: store.Requirements{selector.RequirementGreaterThan("number", 10), selector.RequirementLessThanOrEqual("number", 30)}, sort: "number-,id-", want: []string{"d", "c", "b"}},
		{name: "exclusive upper", fields: store.Requirements{selector.RequirementGreaterThanOrEqual("number", 20), selector.RequirementLessThan("number", 30)}, sort: "number-,id-", want: []string{"c", "b"}},
		{name: "contradiction", fields: store.Requirements{selector.RequirementGreaterThan("number", 30), selector.RequirementLessThan("number", 10)}, sort: "number+,id+", want: []string{}},
		{name: "equal column", fields: store.Requirements{selector.RequirementEqual("number", 20)}, sort: "id-", want: []string{"c", "b"}},
		{name: "numeric operands", fields: store.Requirements{selector.RequirementGreaterThanOrEqual("number", "20"), selector.RequirementLessThanOrEqual("number", "30")}, sort: "number+,id+", want: []string{"b", "c", "d"}},
		{name: "IN residual", fields: store.Requirements{selector.RequirementIn("number", 10, 30)}, sort: "number+,id+", want: []string{"a", "d"}},
		{name: "OR residual", fields: store.Requirements{{Operator: selector.Or, Requirements: store.Requirements{selector.RequirementEqual("number", 10), selector.RequirementEqual("number", 30)}}}, sort: "id+", want: []string{"a", "d"}},
		{name: "NOT residual", fields: store.Requirements{{Operator: selector.Not, Requirements: store.Requirements{selector.RequirementEqual("number", 20)}}}, sort: "id+", want: []string{"a", "d", "e"}},
		{name: "numeric string equivalence", fields: store.Requirements{selector.RequirementEqual("code", 1)}, sort: "code+,id+", want: []string{"c", "d"}},
		{name: "numeric string range", fields: store.Requirements{selector.RequirementGreaterThan("code", "2")}, sort: "code+,id+", want: []string{"b", "e"}},
		{name: "lexical order", sort: "code+,id+", want: []string{"c", "d", "b", "a", "e"}},
		{name: "lexical range", fields: store.Requirements{selector.RequirementGreaterThanOrEqual("code", "a"), selector.RequirementLessThan("code", "zz")}, sort: "code-,id-", want: []string{"e"}},
		{name: "time range", fields: store.Requirements{selector.RequirementGreaterThanOrEqual("at", at.Add(time.Hour).In(time.FixedZone("east", 3600))), selector.RequirementLessThanOrEqual("at", at.Add(3*time.Hour).Format(time.RFC3339Nano))}, sort: "at-,id-", want: []string{"d", "c", "b"}},
		{name: "boolean operand", fields: store.Requirements{selector.RequirementEqual("enabled", "true")}, sort: "id+", want: []string{"b", "c", "e"}},
		{name: "null", fields: store.Requirements{selector.RequirementEqual("number", nil)}, sort: "id+", want: []string{}},
		{name: "null ordering", sort: "number+,id+", want: []string{"e", "a", "b", "c", "d"}},
		{name: "mixed directions", sort: "number+,id-", want: []string{"e", "a", "c", "b", "d"}},
		{name: "label before limit", fields: store.Requirements{selector.RequirementGreaterThanOrEqual("number", 20)}, sort: "number-,id-", extra: []store.ListOption{store.WithLabelRequirements(selector.RequirementEqual("region", "east")), store.WithLimit(1)}, want: []string{"d"}},
		{name: "offset after filter", fields: store.Requirements{selector.RequirementGreaterThan("number", 10)}, sort: "number+,id+", extra: []store.ListOption{store.WithOffset(1), store.WithLimit(1)}, want: []string{"c"}},
	}
	for name, source := range stores {
		t.Run(name, func(t *testing.T) {
			for _, test := range cases {
				t.Run(test.name, func(t *testing.T) {
					fields := append(store.Requirements{selector.RequirementEqual("group", "g")}, test.fields...)
					options := append([]store.ListOption{store.WithFieldRequirements(fields...), store.WithSort(test.sort)}, test.extra...)
					if got := queryIDs(t, source, options...); !reflect.DeepEqual(got, test.want) {
						t.Fatalf("got %v, want %v", got, test.want)
					}
				})
			}
			options := []store.ListOption{store.WithResourceVersion(0), store.WithFieldRequirements(selector.RequirementEqual("group", "g"), selector.RequirementGreaterThan("number", 10)), store.WithSort("number-,id-")}
			result := &store.List[queryRecord]{}
			ids := []string{}
			for {
				if err := source.List(t.Context(), result, append(options, store.WithContinueLimit(result.Continue, 1))...); err != nil {
					t.Fatal(err)
				}
				for _, object := range result.Items {
					ids = append(ids, object.ID)
				}
				if result.Continue == "" {
					break
				}
			}
			if !reflect.DeepEqual(ids, []string{"d", "c", "b"}) {
				t.Fatalf("pagination: %v", ids)
			}
		})
	}
}

func TestIndexTracksDynamicTypes(t *testing.T) {
	stores, source, cache := queryStores(t, []*queryRecord{
		{ObjectMeta: store.ObjectMeta{ID: "a"}, Value: int64(1)},
		{ObjectMeta: store.ObjectMeta{ID: "b"}, Value: "01"},
		{ObjectMeta: store.ObjectMeta{ID: "c"}, Value: int64(2)},
	})
	for name, view := range stores {
		if got := queryIDs(t, view, store.WithFieldRequirements(selector.RequirementEqual("value", 1)), store.WithSort("id+")); !reflect.DeepEqual(got, []string{"a", "b"}) {
			t.Fatalf("%s: %v", name, got)
		}
	}
	if err := source.Patch(t.Context(), &queryRecord{ObjectMeta: store.ObjectMeta{ID: "b"}}, store.FieldPatch{Operations: []store.FieldOperation{{Op: store.FieldOpSet, Path: "value", Value: int64(3)}}}); err != nil {
		t.Fatal(err)
	}
	until(t, func() bool {
		got := queryIDs(t, cache, store.WithFieldRequirements(selector.RequirementEqual("value", 3)))
		return reflect.DeepEqual(got, []string{"b"})
	})
	for _, view := range []store.Store{source, cache} {
		got := queryIDs(t, view, store.WithFieldRequirements(selector.RequirementGreaterThanOrEqual("value", "2")), store.WithSort("value-,id-"))
		if !reflect.DeepEqual(got, []string{"b", "c"}) {
			t.Fatal(got)
		}
	}
	if err := source.Delete(t.Context(), &queryRecord{ObjectMeta: store.ObjectMeta{ID: "b"}}); err != nil {
		t.Fatal(err)
	}
	until(t, func() bool {
		return reflect.DeepEqual(queryIDs(t, cache, store.WithSort("value-,id-")), []string{"c", "a"})
	})
}

func BenchmarkOrderedRange(b *testing.B) {
	for _, indexed := range []bool{false, true} {
		b.Run(fmt.Sprintf("indexed=%v", indexed), func(b *testing.B) {
			source, err := inmemory.New(querySchema(b, indexed))
			if err != nil {
				b.Fatal(err)
			}
			for i := range int64(10000) {
				object := &queryRecord{ObjectMeta: store.ObjectMeta{ID: fmt.Sprint(i)}, Group: "group", Number: &i}
				if err := source.Create(context.Background(), object); err != nil {
					b.Fatal(err)
				}
			}
			options := []store.ListOption{store.WithFieldRequirements(selector.RequirementEqual("group", "group"), selector.RequirementLessThanOrEqual("number", 5000)), store.WithSort("number-,id-"), store.WithLimit(1)}
			b.ReportAllocs()
			for b.Loop() {
				if got := queryIDs(b, source, options...); !reflect.DeepEqual(got, []string{"5000"}) {
					b.Fatal(got)
				}
			}
		})
	}
}

func TestIndexPrecisionAndScope(t *testing.T) {
	at := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	small, large := int64(9007199254740992), int64(9007199254740993)
	stores, source, cache := queryStores(t, []*queryRecord{
		{ObjectMeta: store.ObjectMeta{ID: "a"}, Group: "g", Number: &small, At: at},
		{ObjectMeta: store.ObjectMeta{ID: "b"}, Group: "g", Number: &large, At: at.Add(time.Nanosecond)},
		{ObjectMeta: store.ObjectMeta{ID: "c"}, Group: "g", At: at.Add(-time.Second).In(time.FixedZone("east", 3600))},
	})
	for name, view := range stores {
		for _, sort := range []string{"number+,id+", "at+,id+"} {
			if got := queryIDs(t, view, store.WithSort(sort)); !reflect.DeepEqual(got, []string{"c", "a", "b"}) {
				t.Fatalf("%s/%s: %v", name, sort, got)
			}
		}
		if got := queryIDs(t, view, store.WithFieldRequirements(selector.RequirementGreaterThan("number", small)), store.WithSort("number-,id-"), store.WithLimit(1)); !reflect.DeepEqual(got, []string{"b"}) {
			t.Fatalf("%s precision: %v", name, got)
		}
	}
	for _, name := range []string{"a/b", "other"} {
		scoped := source.Scope(store.Scope{Resource: "tenants", Name: name})
		for _, id := range []string{"a", "b"} {
			number := int64(1)
			if id == "b" {
				number = 2
			}
			object := &queryRecord{ObjectMeta: store.ObjectMeta{ID: id, Labels: map[string]string{"region.zone": "east", "region": "west"}}, Group: "g", Number: &number}
			if err := scoped.Create(t.Context(), object); err != nil {
				t.Fatal(err)
			}
		}
	}
	until(t, func() bool { return len(queryIDs(t, cache, store.WithSubScopes())) == 7 })
	for _, parent := range []store.Store{source, cache} {
		scoped := parent.Scope(store.Scope{Resource: "tenants", Name: "a/b"})
		options := []store.ListOption{store.WithFieldRequirements(selector.RequirementEqual("group", "g"), selector.RequirementGreaterThanOrEqual("number", 1)), store.WithLabelRequirements(selector.RequirementEqual("region.zone", "east")), store.WithSort("number-,id-"), store.WithLimit(1)}
		if got := queryIDs(t, scoped, options...); !reflect.DeepEqual(got, []string{"b"}) {
			t.Fatal(got)
		}
		if got := queryIDs(t, parent, store.WithSubScopes(), store.WithFieldRequirements(selector.RequirementLessThan("number", 10)), store.WithSort("number+,id+")); !reflect.DeepEqual(got, []string{"a", "a", "b", "b"}) {
			t.Fatal(got)
		}
	}
}

func TestCompoundIndexQueries(t *testing.T) {
	cases := []struct {
		name   string
		fields store.Requirements
		want   []string
	}{
		{name: "no prefix", want: []string{"a", "e", "b", "c", "f", "d", "g"}},
		{name: "equality prefix", fields: store.Requirements{selector.RequirementEqual("group", "alpha")}, want: []string{"a", "b", "c", "d"}},
		{name: "full equality", fields: store.Requirements{selector.RequirementEqual("group", "alpha"), selector.RequirementEqual("number", 20), selector.RequirementEqual("id", "c")}, want: []string{"c"}},
		{name: "skip middle column", fields: store.Requirements{selector.RequirementEqual("group", "alpha"), selector.RequirementEqual("id", "c")}, want: []string{"c"}},
		{name: "lower open", fields: store.Requirements{selector.RequirementGreaterThan("number", 20)}, want: []string{"d", "g"}},
		{name: "lower closed", fields: store.Requirements{selector.RequirementGreaterThanOrEqual("number", 20)}, want: []string{"b", "c", "f", "d", "g"}},
		{name: "upper open", fields: store.Requirements{selector.RequirementLessThan("number", 20)}, want: []string{"a", "e"}},
		{name: "upper closed", fields: store.Requirements{selector.RequirementLessThanOrEqual("number", 20)}, want: []string{"a", "e", "b", "c", "f"}},
		{name: "equal endpoints closed", fields: store.Requirements{selector.RequirementGreaterThanOrEqual("number", 20), selector.RequirementLessThanOrEqual("number", 20)}, want: []string{"b", "c", "f"}},
		{name: "equal endpoints open", fields: store.Requirements{selector.RequirementGreaterThan("number", 20), selector.RequirementLessThanOrEqual("number", 20)}, want: []string{}},
		{name: "tightest bounds", fields: store.Requirements{selector.RequirementGreaterThanOrEqual("number", 10), selector.RequirementGreaterThan("number", 10), selector.RequirementGreaterThanOrEqual("number", 20), selector.RequirementLessThanOrEqual("number", 40), selector.RequirementLessThan("number", 30), selector.RequirementLessThanOrEqual("number", 30)}, want: []string{"b", "c", "f"}},
		{name: "equality outside range", fields: store.Requirements{selector.RequirementEqual("number", 20), selector.RequirementGreaterThan("number", 20)}, want: []string{}},
		{name: "contradictory equality", fields: store.Requirements{selector.RequirementEqual("number", 10), selector.RequirementEqual("number", 20)}, want: []string{}},
		{name: "empty IN", fields: store.Requirements{selector.RequirementIn("number")}, want: []string{}},
		{name: "duplicate IN", fields: store.Requirements{selector.RequirementIn("number", 20, "20", 20)}, want: []string{"b", "c", "f"}},
		{name: "IN intersect range", fields: store.Requirements{selector.RequirementIn("number", 10, 30, 40), selector.RequirementLessThan("number", 40)}, want: []string{"a", "e", "d"}},
		{name: "prefix and residual", fields: store.Requirements{selector.RequirementEqual("group", "alpha"), {Operator: selector.Or, Requirements: store.Requirements{selector.RequirementEqual("number", 10), selector.RequirementEqual("code", "y")}}}, want: []string{"a", "c", "d"}},
	}
	for name, indexes := range map[string][]store.Index{
		"scan":        nil,
		"compound":    {{Fields: []string{"group", "number", "id"}}},
		"reordered":   {{Fields: []string{"number", "group", "id"}}},
		"competing":   {{Fields: []string{"group", "number", "id"}}, {Fields: []string{"number", "id"}}},
		"declaration": {{Fields: []string{"number", "id"}}, {Fields: []string{"group", "number", "id"}}},
	} {
		t.Run(name, func(t *testing.T) {
			definition := store.NewSchema()
			if err := definition.Register(&queryRecord{}, store.ResourceSchema{Indexes: indexes}); err != nil {
				t.Fatal(err)
			}
			source, err := inmemory.New(definition)
			if err != nil {
				t.Fatal(err)
			}
			for i, value := range []struct {
				group  string
				number int64
				code   string
			}{{"alpha", 10, "x"}, {"alpha", 20, "x"}, {"alpha", 20, "y"}, {"alpha", 30, "y"}, {"beta", 10, "x"}, {"beta", 20, "y"}, {"gamma", 40, "z"}} {
				object := &queryRecord{ObjectMeta: store.ObjectMeta{ID: string(rune('a' + i))}, Group: value.group, Number: &value.number, Code: value.code}
				if err := source.Create(t.Context(), object); err != nil {
					t.Fatal(err)
				}
			}
			cache := startCache(t, source, &queryRecord{})
			waitSync(t, cache)
			for backend, view := range map[string]store.Store{"memory": source, "cache": cache} {
				t.Run(backend, func(t *testing.T) {
					for _, test := range cases {
						t.Run(test.name, func(t *testing.T) {
							for _, reversed := range []bool{false, true} {
								fields := slices.Clone(test.fields)
								if reversed {
									slices.Reverse(fields)
								}
								for _, descending := range []bool{false, true} {
									sort, want := "number+,id+", slices.Clone(test.want)
									if descending {
										sort = "number-,id-"
										slices.Reverse(want)
									}
									for _, limit := range []int{0, 1, 3} {
										expected := want
										if limit > 0 && len(expected) > limit {
											expected = expected[:limit]
										}
										got := queryIDs(t, view, store.WithFieldRequirements(fields...), store.WithSort(sort), store.WithLimit(limit))
										if !slices.Equal(got, expected) {
											t.Fatalf("reverse predicates=%v sort=%s limit=%d: got %v want %v", reversed, sort, limit, got, expected)
										}
									}
								}
							}
						})
					}
				})
			}
		})
	}
}

func TestFilteredPagesAcrossScopes(t *testing.T) {
	_, source, cache := queryStores(t, nil)
	want := []queryRecord{}
	for _, tenant := range []string{"a/b", "other"} {
		scoped := source.Scope(store.Scope{Resource: "tenants", Name: tenant})
		for i := range 40 {
			number := int64(i % 2)
			object := &queryRecord{
				ObjectMeta: store.ObjectMeta{ID: fmt.Sprintf("%02d", i), Labels: map[string]string{"region": "east"}},
				Group:      "g", Number: &number, Code: "match",
			}
			switch {
			case i < 10:
				object.Group = "excluded"
			case i < 20:
				object.Labels["region"] = "west"
			case i < 37:
				object.Code = "excluded"
			}
			if err := scoped.Create(t.Context(), object); err != nil {
				t.Fatal(err)
			}
			if i >= 37 {
				want = append(want, *object)
			}
		}
	}
	until(t, func() bool { return len(queryIDs(t, cache, store.WithSubScopes())) == 80 })
	for name, view := range map[string]store.Store{"memory": source, "cache": cache} {
		t.Run(name, func(t *testing.T) {
			for _, scope := range []string{"", "a/b"} {
				reader := view
				expected := slices.Clone(want)
				if scope != "" {
					reader = view.Scope(store.Scope{Resource: "tenants", Name: scope})
					expected = expected[:3]
				}
				for _, descending := range []bool{false, true} {
					order := "number+,id+"
					if descending {
						order = "number-,id-"
					}
					// The fixture has only numbers 0 and 1. UID is the public pagination tie break.
					slices.SortFunc(expected, func(a, b queryRecord) int {
						comparison := int(*a.Number - *b.Number)
						if comparison == 0 {
							comparison = strings.Compare(a.ID, b.ID)
						}
						if descending {
							comparison = -comparison
						}
						if comparison == 0 {
							comparison = strings.Compare(a.UID, b.UID)
						}
						return comparison
					})
					wantUIDs := []string{}
					for _, object := range expected {
						wantUIDs = append(wantUIDs, object.UID)
					}
					for _, size := range []int{1, 2, 3, 10} {
						options := []store.ListOption{
							store.WithResourceVersion(0), store.WithSubScopes(),
							store.WithFieldRequirements(selector.RequirementEqual("group", "g")),
							store.WithLabelRequirements(selector.RequirementEqual("region", "east")),
							store.WithSearch("MATCH"), store.WithSearchFields("code"), store.WithSort(order),
						}
						result := &store.List[queryRecord]{}
						got := []string{}
						for {
							if err := reader.List(t.Context(), result, append(options, store.WithContinueLimit(result.Continue, size))...); err != nil {
								t.Fatal(err)
							}
							if len(result.Items) == 0 || len(result.Items) > size {
								t.Fatalf("unexpected page size %d", len(result.Items))
							}
							for _, object := range result.Items {
								got = append(got, object.UID)
							}
							if len(got) > len(wantUIDs) {
								t.Fatalf("pagination repeated objects: %v", got)
							}
							if result.Continue == "" {
								break
							}
						}
						if !slices.Equal(got, wantUIDs) {
							t.Fatalf("scope=%q sort=%s page=%d: got %v want %v", scope, order, size, got, wantUIDs)
						}
					}
				}
			}
		})
	}
}

func TestIndexedMutationSequence(t *testing.T) {
	_, source, cache := queryStores(t, nil)
	// A fixed seed reproduces operation order; this model contains only business fields.
	random := rand.New(rand.NewPCG(42, 73))
	model := map[string]queryRecord{}
	deletedUIDs := map[string]string{}
	for step := range 100 {
		id := fmt.Sprintf("%02d", random.IntN(12))
		number := int64(random.IntN(5))
		group := []string{"alpha", "beta"}[random.IntN(2)]
		object := &queryRecord{ObjectMeta: store.ObjectMeta{ID: id}, Group: group, Number: &number}
		_, exists := model[id]
		deleted := false
		if !exists {
			if err := source.Create(t.Context(), object); err != nil {
				t.Fatal(err)
			}
			if old, ok := deletedUIDs[id]; ok && old == object.UID {
				t.Fatalf("recreated %s retained UID %s", id, old)
			}
		} else {
			switch random.IntN(3) {
			case 0:
				if err := source.Update(t.Context(), object); err != nil {
					t.Fatal(err)
				}
			case 1:
				patch := store.FieldPatch{Operations: []store.FieldOperation{
					{Op: store.FieldOpSet, Path: "group", Value: group},
					{Op: store.FieldOpSet, Path: "number", Value: number},
				}}
				if err := source.Patch(t.Context(), object, patch); err != nil {
					t.Fatal(err)
				}
			case 2:
				if err := source.Get(t.Context(), id, object); err != nil {
					t.Fatal(err)
				}
				deletedUIDs[id] = object.UID
				if err := source.Delete(t.Context(), object); err != nil {
					t.Fatal(err)
				}
				deleted = true
			}
		}
		if deleted {
			delete(model, id)
		} else {
			model[id] = queryRecord{ObjectMeta: store.ObjectMeta{ID: id}, Group: group, Number: &number}
			if err := source.Get(t.Context(), id, object); err != nil {
				t.Fatal(err)
			}
		}
		until(t, func() bool {
			current := &queryRecord{}
			err := cache.Get(t.Context(), id, current, store.WithResourceVersion(0))
			if deleted {
				if commonerrors.IsNotFound(err) {
					return true
				}
				if err != nil {
					t.Fatal(err)
				}
				return false
			}
			if err != nil {
				if commonerrors.IsNotFound(err) {
					return false
				}
				t.Fatal(err)
			}
			return current.ResourceVersion == object.ResourceVersion && current.UID == object.UID
		})
		for _, group := range []string{"alpha", "beta"} {
			for _, threshold := range []int64{0, 2, 4} {
				rows := []queryRecord{}
				for _, row := range model {
					if row.Group == group && *row.Number >= threshold {
						rows = append(rows, row)
					}
				}
				slices.SortFunc(rows, func(a, b queryRecord) int {
					if *a.Number != *b.Number {
						return int(*b.Number - *a.Number)
					}
					return strings.Compare(b.ID, a.ID)
				})
				want := []string{}
				for _, row := range rows {
					want = append(want, row.ID)
				}
				for _, limit := range []int{0, 1} {
					expected := want
					if limit > 0 && len(expected) > limit {
						expected = expected[:limit]
					}
					for name, view := range map[string]store.Store{"memory": source, "cache": cache} {
						got := queryIDs(t, view, store.WithFieldRequirements(selector.RequirementEqual("group", group), selector.RequirementGreaterThanOrEqual("number", threshold)), store.WithSort("number-,id-"), store.WithLimit(limit))
						if !slices.Equal(got, expected) {
							t.Fatalf("step=%d view=%s group=%s threshold=%d limit=%d: got %v want %v", step, name, group, threshold, limit, got, expected)
						}
					}
				}
			}
		}
	}
}
