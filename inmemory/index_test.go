package inmemory_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	commonerrors "xiaoshiai.cn/common/errors"

	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/inmemory"
	"xiaoshiai.cn/store/selector"
)

func TestIndexedQueriesMatchScans(t *testing.T) {
	objects := []store.Object{}
	for index, code := range []string{"01", "1", "2", "false", "true", "ordinary"} {
		objects = append(objects, &item{ObjectMeta: store.ObjectMeta{ID: fmt.Sprint(index)}, Group: "g", Code: code})
	}
	indexed, scan := newItemStore(t, true), newItemStore(t, false)
	for _, c := range []*inmemory.InMemory{indexed, scan} {
		for _, object := range objects {
			if err := c.Create(t.Context(), object); err != nil {
				t.Fatal(err)
			}
		}
	}
	for _, requirements := range []store.Requirements{
		{selector.RequirementEqual("group", "g"), selector.RequirementEqual("code", 1)},
		{selector.RequirementEqual("group", "g"), selector.RequirementEqual("code", false)},
		{{Operator: selector.Or, Requirements: store.Requirements{selector.RequirementEqual("group", "missing"), selector.RequirementEqual("code", "ordinary")}}},
		{{Operator: selector.And, Requirements: store.Requirements{selector.RequirementEqual("group", "g"), selector.RequirementEqual("code", "01")}}},
	} {
		want, got := &store.List[item]{}, &store.List[item]{}
		opts := []store.ListOption{store.WithFieldRequirements(requirements...), store.WithSort("id+")}
		if err := scan.List(t.Context(), want, opts...); err != nil {
			t.Fatal(err)
		}
		if err := indexed.List(t.Context(), got, opts...); err != nil {
			t.Fatal(err)
		}
		gotIDs, wantIDs := []string{}, []string{}
		for _, object := range got.Items {
			gotIDs = append(gotIDs, object.ID)
		}
		for _, object := range want.Items {
			wantIDs = append(wantIDs, object.ID)
		}
		if !reflect.DeepEqual(gotIDs, wantIDs) {
			t.Fatalf("%v: got %+v want %+v", requirements, got, want)
		}
	}
}

func TestIndexUsesCommittedVersion(t *testing.T) {
	schema := store.NewSchema()
	if err := schema.Register(&item{}, store.ResourceSchema{Indexes: []store.Index{{Fields: []string{"resourceVersion"}}}}); err != nil {
		t.Fatal(err)
	}
	source, err := inmemory.New(schema)
	if err != nil {
		t.Fatal(err)
	}
	object := &item{ObjectMeta: store.ObjectMeta{ID: "versioned"}}
	if err := source.Create(t.Context(), object); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		condition := store.WithFieldRequirements(selector.RequirementEqual("resourceVersion", object.ResourceVersion))
		result := &store.List[item]{}
		if err := source.List(t.Context(), result, condition); err != nil {
			t.Fatal(err)
		}
		if len(result.Items) != 1 || result.Items[0].ResourceVersion != object.ResourceVersion {
			t.Fatalf("committed version %d: %+v", object.ResourceVersion, result.Items)
		}
		count, err := source.Count(t.Context(), &item{}, condition)
		if err != nil || count != 1 {
			t.Fatalf("count=%d err=%v", count, err)
		}
		object.Group = "updated"
		if err := source.Update(t.Context(), object); err != nil {
			t.Fatal(err)
		}
		if err := source.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
	}
}

func BenchmarkIndexedList(b *testing.B) {
	for _, indexed := range []bool{false, true} {
		b.Run(fmt.Sprintf("indexed=%v", indexed), func(b *testing.B) {
			c := newItemStore(b, indexed)
			objects := make([]store.Object, 10000)
			for index := range objects {
				objects[index] = &item{ObjectMeta: store.ObjectMeta{ID: fmt.Sprint(index)}, Group: fmt.Sprint(index % 1000), Code: "code"}
			}
			for _, object := range objects {
				if err := c.Create(b.Context(), object); err != nil {
					b.Fatal(err)
				}
			}
			options := []store.ListOption{store.WithFieldRequirements(selector.RequirementEqual("group", "23")), store.WithSort("id-"), store.WithLimit(1)}
			b.ReportAllocs()
			for b.Loop() {
				result := &store.List[item]{}
				if err := c.List(b.Context(), result, options...); err != nil {
					b.Fatal(err)
				}
				if len(result.Items) != 1 {
					b.Fatal(result)
				}
			}
		})
	}
}

func TestUniqueIndexFailedWritesPreserveState(t *testing.T) {
	definitions := store.NewSchema()
	if err := definitions.Register(&item{}, store.ResourceSchema{Indexes: []store.Index{
		{Fields: []string{"group"}, Unique: true},
		{Fields: []string{"code", "id"}},
	}}); err != nil {
		t.Fatal(err)
	}
	source, err := inmemory.New(definitions)
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range []*item{
		{ObjectMeta: store.ObjectMeta{ID: "a"}, Group: "alpha", Code: "old"},
		{ObjectMeta: store.ObjectMeta{ID: "b"}, Group: "beta", Code: "other"},
		{ObjectMeta: store.ObjectMeta{ID: "barrier"}, Group: "barrier"},
	} {
		if err := source.Create(t.Context(), object); err != nil {
			t.Fatal(err)
		}
	}
	cache := startCache(t, source, &item{})
	waitSync(t, cache)
	original := &item{}
	if err := source.Get(t.Context(), "a", original); err != nil {
		t.Fatal(err)
	}
	for _, operation := range []string{"create", "update", "patch", "stale update", "stale patch"} {
		t.Run(operation, func(t *testing.T) {
			desired := *original
			desired.Group, desired.Code = "beta", "changed"
			patch := store.FieldPatch{Operations: []store.FieldOperation{
				{Op: store.FieldOpSet, Path: "group", Value: "beta"},
				{Op: store.FieldOpSet, Path: "code", Value: "changed"},
			}}
			var err error
			switch operation {
			case "create":
				err = cache.Create(t.Context(), &item{ObjectMeta: store.ObjectMeta{ID: "c"}, Group: "beta", Code: "changed"})
			case "update":
				err = cache.Update(t.Context(), &desired)
			case "patch":
				err = cache.Patch(t.Context(), &desired, patch)
			case "stale update":
				desired.ResourceVersion++
				err = cache.Update(t.Context(), &desired)
			case "stale patch":
				version := original.ResourceVersion + 1
				err = cache.Patch(t.Context(), &desired, patch, store.WithPreconditions(store.Preconditions{ResourceVersion: &version}))
			}
			if strings.HasPrefix(operation, "stale") {
				if !commonerrors.IsConflict(err) {
					t.Fatalf("version conflict: %v", err)
				}
			} else if !commonerrors.IsAlreadyExists(err) {
				t.Fatalf("unique conflict: %v", err)
			}
			// Seeing a later event establishes that all preceding mutations reached the cache.
			if err := source.Patch(t.Context(), &item{ObjectMeta: store.ObjectMeta{ID: "barrier"}}, store.FieldPatch{Operations: []store.FieldOperation{{Op: store.FieldOpSet, Path: "code", Value: operation}}}); err != nil {
				t.Fatal(err)
			}
			until(t, func() bool {
				marker := &item{}
				if err := cache.Get(t.Context(), "barrier", marker, store.WithResourceVersion(0)); err != nil {
					t.Fatal(err)
				}
				return marker.Code == operation
			})
			for _, view := range []store.Store{source, cache} {
				got := &item{}
				if err := view.Get(t.Context(), "a", got, store.WithResourceVersion(0)); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, original) {
					t.Fatalf("failed write changed object: got %+v want %+v", got, original)
				}
				for _, predicate := range []selector.Requirement{selector.RequirementEqual("group", "alpha"), selector.RequirementEqual("code", "old")} {
					result := &store.List[item]{}
					if err := view.List(t.Context(), result, store.WithResourceVersion(0), store.WithFieldRequirements(predicate)); err != nil {
						t.Fatal(err)
					}
					if len(result.Items) != 1 || result.Items[0].ID != "a" || result.Items[0].ResourceVersion != original.ResourceVersion {
						t.Fatalf("failed write changed index: %+v", result.Items)
					}
				}
				changed := &store.List[item]{}
				if err := view.List(t.Context(), changed, store.WithResourceVersion(0), store.WithFieldRequirements(selector.RequirementEqual("code", "changed"))); err != nil {
					t.Fatal(err)
				}
				if len(changed.Items) != 0 {
					t.Fatalf("failed write left new index entry: %+v", changed.Items)
				}
			}
		})
	}
}
