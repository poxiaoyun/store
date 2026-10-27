package rest

import (
	"context"
	stderrors "errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/selector"
)

type objectAdapterStore struct {
	store.Store
	schema       *store.Schema
	listOptions  store.ListOptions
	watchOptions store.WatchOptions
	updated      store.Object
	watcher      store.Watcher
}

func (s *objectAdapterStore) Schema() *store.Schema {
	return s.schema
}

func newObjectAdapterStore() *objectAdapterStore {
	schema := store.NewSchema()
	if err := schema.Register(&objectAdapterFixture{}, store.ResourceSchema{ResourceName: "object-adapter-fixtures"}); err != nil {
		panic(err)
	}
	return &objectAdapterStore{schema: schema}
}

func (s *objectAdapterStore) List(_ context.Context, _ store.ObjectList, options ...store.ListOption) error {
	s.listOptions = store.ApplyListOptions(options)
	return nil
}

func (s *objectAdapterStore) Watch(_ context.Context, _ store.ObjectList, options ...store.WatchOption) (store.Watcher, error) {
	s.watchOptions = store.ApplyWatchOptions(options)
	return s.watcher, nil
}

func (s *objectAdapterStore) Update(_ context.Context, obj store.Object, _ ...store.UpdateOption) error {
	s.updated = obj
	return nil
}

type objectAdapterWatcher struct {
	events chan store.WatchEvent
}

func (w *objectAdapterWatcher) Stop() {}

func (w *objectAdapterWatcher) Events() <-chan store.WatchEvent {
	return w.events
}

type objectAdapterFixture struct {
	store.ObjectMeta `json:",inline"`
}

func TestListObjectsAppliesCallerOptionsAfterRequestOptions(t *testing.T) {
	storage := newObjectAdapterStore()
	request := httptest.NewRequest(http.MethodGet, "/?page=2&size=25&labelSelector=environment%3Dproduction", nil)
	list := &store.List[objectAdapterFixture]{}
	serverRequirement := selector.RequirementEqual("tenant", "tenant-1")

	if _, err := ListObjects(request, storage, list,
		store.WithOffset(20),
		store.WithLimit(10),
		store.WithLabelRequirements(serverRequirement),
	); err != nil {
		t.Fatal(err)
	}
	if storage.listOptions.Offset != 20 || storage.listOptions.Limit != 10 {
		t.Fatalf("window = %#v", storage.listOptions)
	}
	wantRequirements := store.Requirements{
		selector.RequirementEqual("environment", "production"),
		serverRequirement,
	}
	if !reflect.DeepEqual(storage.listOptions.LabelRequirements, wantRequirements) {
		t.Fatalf("LabelRequirements = %#v, want %#v", storage.listOptions.LabelRequirements, wantRequirements)
	}
}

func TestListObjectsOrWatchPassesRequestFiltersToWatch(t *testing.T) {
	watchError := stderrors.New("watch stopped")
	events := make(chan store.WatchEvent, 1)
	events <- store.WatchEvent{Error: watchError}
	storage := newObjectAdapterStore()
	storage.watcher = &objectAdapterWatcher{events: events}
	request := httptest.NewRequest(http.MethodGet, "/?watch=true&sendInitialEvents=true&labelSelector=environment%3Dproduction&fieldSelector=enabled%3Dtrue", nil)

	_, err := ListObjectsOrWatch(
		httptest.NewRecorder(),
		request,
		storage,
		&store.List[objectAdapterFixture]{},
		store.WithSubScopes(),
		store.WithResourceVersion(7),
	)
	if !stderrors.Is(err, watchError) {
		t.Fatalf("ListObjectsOrWatch() error = %v, want %v", err, watchError)
	}
	if storage.watchOptions.ResourceVersion == nil || *storage.watchOptions.ResourceVersion != 7 || !storage.watchOptions.IncludeSubScopes || !storage.watchOptions.SendInitialEvents {
		t.Fatalf("snapshot options = %#v", storage.watchOptions)
	}
	if !reflect.DeepEqual(storage.watchOptions.LabelRequirements, store.Requirements{selector.RequirementEqual("environment", "production")}) {
		t.Fatalf("LabelRequirements = %#v", storage.watchOptions.LabelRequirements)
	}
	if !reflect.DeepEqual(storage.watchOptions.FieldRequirements, store.Requirements{selector.RequirementEqual("enabled", "true")}) {
		t.Fatalf("FieldRequirements = %#v", storage.watchOptions.FieldRequirements)
	}
}

func TestUpdateObjectPreservesResourceVersion(t *testing.T) {
	storage := newObjectAdapterStore()
	request := httptest.NewRequest(http.MethodPut, "/objects/object-1", strings.NewReader(`{"id":"object-1","resourceVersion":7}`))
	request.Header.Set("Content-Type", "application/json")

	if _, err := UpdateObject(request, storage, &objectAdapterFixture{}, "object-1"); err != nil {
		t.Fatal(err)
	}
	if storage.updated.GetResourceVersion() != 7 {
		t.Fatalf("ResourceVersion = %d, want 7", storage.updated.GetResourceVersion())
	}
}
