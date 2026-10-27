package garbagecollector_test

import (
	"context"
	stderrors "errors"
	"sync"
	"testing"
	"time"

	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/garbagecollector"
	"xiaoshiai.cn/store/inmemory"
)

func TestGarbageCollectorWaitsForEveryInitialSnapshot(t *testing.T) {
	storage := newWatchGateStore()
	collector, err := garbagecollector.NewGarbageCollector(storage, garbagecollector.GarbageCollectorOptions{})
	if err != nil {
		t.Fatalf("NewGarbageCollector() error = %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() {
		done <- collector.Run(ctx)
	}()
	defer func() {
		cancel()
		if err := <-done; err != nil && !stderrors.Is(err, context.Canceled) {
			t.Fatalf("GarbageCollector.Run() error = %v", err)
		}
	}()

	dependentWatch := storage.waitForWatcher(t, "dependents")
	ownerWatch := storage.waitForWatcher(t, "owners")
	dependent := &store.Unstructured{}
	dependent.SetResource("dependents")
	dependent.SetID("dependent")
	dependent.SetUID("dependent-uid")
	dependent.SetOwnerReferences([]store.OwnerReference{{
		Resource: "owners",
		ID:       "missing",
		UID:      "missing-owner-uid",
	}})
	storage.setObject(dependent)
	dependentWatch.send(store.WatchEvent{Type: store.WatchEventCreate, Object: dependent})
	dependentWatch.send(store.WatchEvent{Type: store.WatchEventBookmark})

	select {
	case object := <-storage.deleted:
		t.Fatalf("GC deleted %q before every initial Bookmark", object.GetID())
	case <-time.After(300 * time.Millisecond):
	}

	ownerWatch.send(store.WatchEvent{Type: store.WatchEventBookmark})
	select {
	case object := <-storage.deleted:
		if object.GetID() != "dependent" {
			t.Fatalf("GC deleted %q, want dependent", object.GetID())
		}
		if object.GetUID() != "dependent-uid" {
			t.Fatalf("GC Delete UID = %q, want dependent-uid precondition", object.GetUID())
		}
	case <-time.After(10 * time.Second):
		t.Fatal("GC did not delete dangling dependent after every initial Bookmark")
	}
}

type watchGateStore struct {
	store.Store
	mu       sync.Mutex
	watchers map[string]*watchGateWatcher
	objects  map[string]*store.Unstructured
	deleted  chan store.Object
	schema   *store.Schema
}

func newWatchGateStore() *watchGateStore {
	schema := store.NewSchema()
	for _, resource := range []string{"dependents", "owners"} {
		object := &store.Unstructured{}
		object.SetResource(resource)
		if err := schema.Register(object, store.ResourceSchema{ResourceName: resource}); err != nil {
			panic(err)
		}
	}
	return &watchGateStore{
		watchers: map[string]*watchGateWatcher{},
		objects:  map[string]*store.Unstructured{},
		deleted:  make(chan store.Object, 1),
		schema:   schema,
	}
}

func (s *watchGateStore) Schema() *store.Schema {
	return s.schema.Snapshot()
}

func (s *watchGateStore) Capabilities() store.Capabilities {
	return store.Capabilities{Watch: true}
}

func (s *watchGateStore) Watch(_ context.Context, list store.ObjectList, options ...store.WatchOption) (store.Watcher, error) {
	configured := store.ApplyWatchOptions(options)
	if !configured.SendInitialEvents {
		return nil, stderrors.New("GC watch did not request initial events")
	}
	resource, err := s.schema.ResourceName(list)
	if err != nil {
		return nil, err
	}
	watcher := &watchGateWatcher{events: make(chan store.WatchEvent, 16), stopped: make(chan struct{})}
	s.mu.Lock()
	s.watchers[resource] = watcher
	s.mu.Unlock()
	return watcher, nil
}

func (s *watchGateStore) Scope(...store.Scope) store.Store { return s }

func (s *watchGateStore) Get(_ context.Context, id string, object store.Object, _ ...store.GetOption) error {
	s.mu.Lock()
	current, ok := s.objects[id]
	s.mu.Unlock()
	if !ok {
		return errors.NewNotFound(object.GetResource(), id)
	}
	return store.CopyObject(current, object)
}

func (s *watchGateStore) Delete(_ context.Context, object store.Object, _ ...store.DeleteOption) error {
	s.deleted <- object
	return nil
}

func (s *watchGateStore) setObject(object *store.Unstructured) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[object.GetID()] = object
}

func (s *watchGateStore) waitForWatcher(t testing.TB, resource string) *watchGateWatcher {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		s.mu.Lock()
		watcher := s.watchers[resource]
		s.mu.Unlock()
		if watcher != nil {
			return watcher
		}
		select {
		case <-deadline.C:
			t.Fatalf("timed out waiting for %q watcher", resource)
			return nil
		case <-ticker.C:
		}
	}
}

type watchGateWatcher struct {
	events  chan store.WatchEvent
	stopped chan struct{}
	stop    sync.Once
}

func (w *watchGateWatcher) Events() <-chan store.WatchEvent { return w.events }

func (w *watchGateWatcher) Stop() {
	w.stop.Do(func() {
		close(w.stopped)
	})
}

func (w *watchGateWatcher) send(event store.WatchEvent) {
	w.events <- event
}

func TestNewChildrenGarbageCollector(t *testing.T) {
	ctx, rootCancel := context.WithCancel(context.Background())

	schema := store.NewSchema()
	for _, resource := range []string{"zoos", "area", "employees", "cats", "dogs"} {
		object := &store.Unstructured{}
		object.SetResource(resource)
		if err := schema.Register(object, store.ResourceSchema{ResourceName: resource}); err != nil {
			t.Fatalf("register %q: %v", resource, err)
		}
	}
	storage, err := inmemory.New(schema)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
		return
	}
	t.Cleanup(rootCancel)
	cgc, err := garbagecollector.NewGarbageCollector(storage, garbagecollector.GarbageCollectorOptions{})
	if err != nil {
		t.Fatalf("Failed to create children garbage collector: %v", err)
		return
	}
	runDone := make(chan error, 1)
	go func() {
		runDone <- cgc.Run(ctx)
	}()
	t.Cleanup(func() {
		rootCancel()
		if err := <-runDone; err != nil && !stderrors.Is(err, context.Canceled) {
			t.Errorf("garbage collector stopped: %v", err)
		}
	})

	time.Sleep(1 * time.Second)

	initdatas := []store.ObjectMeta{
		{Name: "main", Resource: "zoos"},
		{Name: "jeff", Resource: "employees", Scopes: []store.Scope{{Resource: "zoos", Name: "main"}}},
		{Name: "lisa", Resource: "employees", Scopes: []store.Scope{{Resource: "zoos", Name: "main"}}},

		{Name: "area1", Resource: "area", Scopes: []store.Scope{{Resource: "zoos", Name: "main"}}},

		{Name: "tom", Resource: "cats", Scopes: []store.Scope{{Resource: "zoos", Name: "main"}, {Resource: "area", Name: "area1"}}},
		{Name: "jerry", Resource: "dogs", Scopes: []store.Scope{{Resource: "zoos", Name: "main"}, {Resource: "area", Name: "area1"}}},

		{Name: "area2", Resource: "area", Scopes: []store.Scope{{Resource: "zoos", Name: "main"}}},
		{Name: "simon", Resource: "cats", Scopes: []store.Scope{{Resource: "zoos", Name: "main"}, {Resource: "area", Name: "area2"}}},

		{Name: "second", Resource: "zoos"},
		{Name: "tony", Resource: "employees", Scopes: []store.Scope{{Resource: "zoos", Name: "second"}}},
		{ID: "lisa-second", Name: "lisa", Resource: "employees", Scopes: []store.Scope{{Resource: "zoos", Name: "second"}}},
	}
	for _, data := range initdatas {
		obj := objfrom(data)
		// set owner reference
		setParentScopeReferences(ctx, storage, obj)
		if err := storage.Scope(data.Scopes...).
			Create(ctx, obj); err != nil {
			t.Fatalf("Failed to create %v: %v", data, err)
		}
	}

	// delete main zoo
	todelete := objfrom(store.ObjectMeta{Name: "main", Resource: "zoos"})
	if err := storage.Scope(todelete.GetScopes()...).
		Delete(ctx, todelete,
			store.WithPropagation(store.DeletePropagationForeground)); err != nil {
		t.Fatalf("Failed to delete main zoo: %v", err)
	}

	// check if cat, dog, bird are deleted
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("Timeout waiting for children to be deleted")
		case <-time.After(5 * time.Second):
			todelete := objfrom(store.ObjectMeta{Name: "main", Resource: "zoos"})
			if err := storage.Scope(todelete.GetScopes()...).
				Get(ctx, todelete.GetID(), todelete); err != nil {
				if errors.IsNotFound(err) {
					t.Log("Main zoo is deleted")
				} else {
					t.Fatalf("Failed to get main zoo: %v", err)
				}
			} else {
				t.Log("Main zoo is not deleted")
			}

			children := store.List[store.Unstructured]{
				Resource: "employees",
			}
			if err := storage.Scope(todelete.GetScopes()...).
				Scope(store.Scope{Resource: todelete.GetResource(), Name: todelete.GetID()}).
				List(ctx, &children); err != nil {
				t.Fatalf("Failed to list employees: %v", err)
			}
			if len(children.Items) == 0 {
				t.Log("All children are deleted")
				return
			}
			t.Logf("Waiting for children to be deleted: %v", children.Items)
		}
	}
}

func objfrom(meta store.ObjectMeta) *store.Unstructured {
	uns := store.Unstructured{}
	uns.SetResource(meta.Resource)
	if meta.ID != "" {
		uns.SetID(meta.ID)
	} else {
		uns.SetID(meta.Name)
	}
	uns.SetName(meta.Name)
	uns.SetScopes(meta.Scopes)
	return &uns
}

func setParentScopeReferences(ctx context.Context, root store.Store, obj *store.Unstructured) {
	scopes := obj.GetScopes()
	if len(scopes) == 0 {
		return
	}
	parentscopes, last := scopes[:len(scopes)-1], scopes[len(scopes)-1]

	parent := &store.Unstructured{}
	parent.SetResource(last.Resource)
	parent.SetID(last.Name)
	if err := root.Scope(parentscopes...).
		Get(ctx, parent.GetID(), parent); err != nil {
		panic(err)
	}
	obj.SetOwnerReferences([]store.OwnerReference{
		{
			UID:      parent.GetUID(),
			ID:       parent.GetID(),
			Resource: parent.GetResource(),
			Scopes:   parent.GetScopes(),
		},
	})
}
