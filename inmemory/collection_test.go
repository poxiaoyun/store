package inmemory_test

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	commonerrors "xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/inmemory"
	"xiaoshiai.cn/store/selector"
)

type item struct {
	store.ObjectMeta `json:",inline"`
	Group            string    `json:"group"`
	Code             string    `json:"code"`
	At               time.Time `json:"at"`
	Number           *int64    `json:"number,omitempty"`
}

func itemSchema(t testing.TB, indexed bool) *store.Schema {
	t.Helper()
	s := store.NewSchema()
	definition := store.ResourceSchema{}
	if indexed {
		definition.Indexes = []store.Index{{Name: "lookup", Fields: []string{"group", "code"}}}
	}
	if err := s.Register(&item{}, definition); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestListTimeAndMissingValueOrder(t *testing.T) {
	c := newItemStore(t, false)
	instant := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	big := int64(9007199254740993)
	small := big - 1
	values := []store.Object{
		&item{ObjectMeta: store.ObjectMeta{ID: "fraction"}, At: instant.Add(time.Millisecond), Number: &big},
		&item{ObjectMeta: store.ObjectMeta{ID: "second"}, At: instant, Number: &small},
		&item{ObjectMeta: store.ObjectMeta{ID: "offset"}, At: instant.Add(-time.Second).In(time.FixedZone("east", 3600))},
	}
	for _, value := range values {
		if err := c.Create(t.Context(), value); err != nil {
			t.Fatal(err)
		}
	}
	for _, sort := range []string{"at+", "number+"} {
		result := &store.List[item]{}
		if err := c.List(t.Context(), result, store.WithSort(sort)); err != nil {
			t.Fatal(err)
		}
		ids := []string{}
		for _, value := range result.Items {
			ids = append(ids, value.ID)
		}
		if !reflect.DeepEqual(ids, []string{"offset", "second", "fraction"}) {
			t.Fatalf("%s: %v", sort, ids)
		}
	}
}

func newItemStore(t testing.TB, indexed bool) *inmemory.InMemory {
	t.Helper()
	source, err := inmemory.New(itemSchema(t, indexed))
	if err != nil {
		t.Fatal(err)
	}
	return source
}

type otherItem item

func TestResourceIsolation(t *testing.T) {
	schema := store.NewSchema()
	for _, object := range []store.Object{&item{}, &otherItem{}} {
		if err := schema.Register(object, store.ResourceSchema{Indexes: []store.Index{{Name: "lookup", Fields: []string{"group"}, Unique: true}}}); err != nil {
			t.Fatal(err)
		}
	}
	source, err := inmemory.New(schema)
	if err != nil {
		t.Fatal(err)
	}
	var writers sync.WaitGroup
	for _, prototype := range []store.Object{&item{}, &otherItem{}} {
		writers.Go(func() {
			for i := range 30 {
				object := store.NewObject(prototype)
				value := &item{ObjectMeta: store.ObjectMeta{ID: fmt.Sprint(i)}, Group: fmt.Sprint(i)}
				if err := store.CopyObject(value, object); err != nil {
					t.Error(err)
					return
				}
				if err := source.Create(t.Context(), object); err != nil {
					t.Error(err)
					return
				}
				result := &store.List[store.Unstructured]{Resource: object.GetResource()}
				if err := source.List(t.Context(), result, store.WithFieldRequirements(selector.RequirementEqual("resourceVersion", object.GetResourceVersion()))); err != nil {
					t.Error(err)
					return
				}
				if len(result.Items) != 1 || result.Items[0].GetID() != object.GetID() {
					t.Errorf("committed version selected wrong object: %+v", result.Items)
					return
				}
			}
		})
	}
	writers.Wait()
	if t.Failed() {
		return
	}
	current := &item{}
	if err := source.Get(t.Context(), "1", current); err != nil {
		t.Fatal(err)
	}
	version := current.ResourceVersion
	current.Group = "0"
	if err := source.Update(t.Context(), current); !commonerrors.IsAlreadyExists(err) {
		t.Fatalf("unique conflict: %v", err)
	}
	result := &store.List[item]{}
	if err := source.List(t.Context(), result, store.WithFieldRequirements(selector.RequirementEqual("group", "1"))); err != nil {
		t.Fatal(err)
	}
	if len(result.Items) != 1 || result.Items[0].ResourceVersion != version {
		t.Fatalf("failed update changed object/index: %+v", result.Items)
	}
	if err := source.Delete(t.Context(), &item{ObjectMeta: store.ObjectMeta{ID: "1"}}); err != nil {
		t.Fatal(err)
	}
	peer := &otherItem{}
	if err := source.Get(t.Context(), "1", peer); err != nil {
		t.Fatal(err)
	}
	if peer.Group != "1" {
		t.Fatalf("other resource changed: %+v", peer)
	}
}

func TestConcurrentIndexedSnapshots(t *testing.T) {
	definitions := store.NewSchema()
	if err := definitions.Register(&item{}, store.ResourceSchema{Indexes: []store.Index{{Fields: []string{"number", "id"}}}}); err != nil {
		t.Fatal(err)
	}
	source, err := inmemory.New(definitions)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 16 {
		number := int64(0)
		if err := source.Create(t.Context(), &item{ObjectMeta: store.ObjectMeta{ID: fmt.Sprint(i)}, Number: &number, Code: "0"}); err != nil {
			t.Fatal(err)
		}
	}
	cache := startCache(t, source, &item{})
	waitSync(t, cache)
	ctx, cancel := context.WithCancel(t.Context())
	var readers sync.WaitGroup
	started := make(chan struct{}, 4)
	for _, view := range []store.Store{source, cache, source, cache} {
		readers.Go(func() {
			started <- struct{}{}
			for {
				result := &store.List[item]{}
				if err := view.List(t.Context(), result, store.WithResourceVersion(0), store.WithFieldRequirements(selector.RequirementGreaterThanOrEqual("number", 0)), store.WithSort("number+,id+")); err != nil {
					t.Error(err)
					return
				}
				if len(result.Items) != 16 {
					t.Errorf("partial index snapshot: %d objects", len(result.Items))
					return
				}
				seen := map[string]bool{}
				for i, object := range result.Items {
					if seen[object.ID] || object.Code != fmt.Sprint(*object.Number) {
						t.Errorf("object/index snapshot inconsistent: %+v", object)
						return
					}
					seen[object.ID] = true
					if i > 0 && *result.Items[i-1].Number > *object.Number {
						t.Error("snapshot is not sorted")
						return
					}
				}
				select {
				case <-ctx.Done():
					return
				default:
				}
			}
		})
	}
	defer readers.Wait()
	defer cancel()
	for range 4 {
		<-started
	}
	for round := int64(1); round <= 4; round++ {
		for i := range 16 {
			patch := store.FieldPatch{Operations: []store.FieldOperation{
				{Op: store.FieldOpSet, Path: "number", Value: round},
				{Op: store.FieldOpSet, Path: "code", Value: fmt.Sprint(round)},
			}}
			if err := source.Patch(t.Context(), &item{ObjectMeta: store.ObjectMeta{ID: fmt.Sprint(i)}}, patch); err != nil {
				t.Fatal(err)
			}
		}
	}
	until(t, func() bool {
		result := &store.List[item]{}
		if err := cache.List(t.Context(), result, store.WithResourceVersion(0), store.WithFieldRequirements(selector.RequirementEqual("number", 4))); err != nil {
			t.Fatal(err)
		}
		return len(result.Items) == 16
	})
}
