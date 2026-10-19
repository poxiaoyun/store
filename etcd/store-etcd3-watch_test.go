package etcd

import (
	"context"
	"reflect"
	"testing"
	"time"

	"k8s.io/apiserver/pkg/storage/etcd3/testserver"
	"k8s.io/utils/ptr"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/selector"
)

func TestEtcdStore_Watch(t *testing.T) {
	ctx := context.Background()
	etcdStore := SetupEtcdTestEtcdStore(t)

	obj := &TestObject{
		ObjectMeta: store.ObjectMeta{ID: "test", Name: "test", Resource: "test"},
		Spec:       TestObjectSpec{Replicas: ptr.To(int32(1))},
	}
	if err := etcdStore.Create(ctx, obj); err != nil {
		t.Fatalf("failed to create object: %v", err)
	}

	obj2 := &TestObject{
		ObjectMeta: store.ObjectMeta{ID: "test2", Name: "test2", Resource: "test"},
		Spec:       TestObjectSpec{Replicas: ptr.To(int32(1))},
	}
	if err := etcdStore.Create(ctx, obj2); err != nil {
		t.Fatalf("failed to create object: %v", err)
	}

	w, err := etcdStore.Watch(ctx, &store.List[TestObject]{}, store.WithSendInitialEvents())
	if err != nil {
		t.Fatalf("failed to create watcher: %v", err)
	}
	defer w.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// The initial snapshot is established at the bookmark, not at Watch return.
	events := receiveEvents(ctx, 3, w)
	if len(events) != 3 || events[2].Type != store.WatchEventBookmark {
		t.Fatalf("initial events = %#v", events)
	}
	obj2changed := *obj2
	obj2changed.Spec.Replicas = ptr.To(int32(2))
	if err := etcdStore.Update(ctx, &obj2changed); err != nil {
		t.Fatalf("failed to update object: %v", err)
	}
	if err := etcdStore.Get(ctx, obj2changed.ID, &obj2changed); err != nil {
		t.Fatal(err)
	}
	events = append(events, receiveEvents(ctx, 1, w)...)
	if len(events) != 4 {
		t.Fatalf("received %d events, want initial snapshot and one update", len(events))
	}

	expected := []store.WatchEvent{
		{Type: store.WatchEventCreate, Object: obj},
		{Type: store.WatchEventCreate, Object: obj2},
		{Type: store.WatchEventBookmark},
		{Type: store.WatchEventUpdate, Object: &obj2changed},
	}
	for i, event := range events {
		if event.Type != expected[i].Type {
			t.Errorf("expected event type %q, got %q", expected[i].Type, event.Type)
		}
		if event.Type != store.WatchEventBookmark {
			if !reflect.DeepEqual(event.Object, expected[i].Object) {
				t.Errorf("expected object %v, got %v", expected[i].Object, event.Object)
			}
		}
	}
}

func receiveEvents(ctx context.Context, max int, w store.Watcher) []store.WatchEvent {
	events := []store.WatchEvent{}
	for {
		select {
		case event := <-w.Events():
			events = append(events, event)
			if len(events) == max {
				return events
			}
		case <-ctx.Done():
			return events
		}
	}
}

func TestWatchAppliesUnindexedFieldRequirements(t *testing.T) {
	client := testserver.RunEtcd(t, nil)
	schema := store.NewSchema()
	if err := schema.Register(&indexedObject{}, store.ResourceSchema{}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	storage, err := NewEtcdStoreFromClient(client, schema, "/test")
	if err != nil {
		t.Fatalf("NewEtcdStoreFromClient() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, object := range []*indexedObject{
		{ObjectMeta: store.ObjectMeta{ID: "one"}, Email: "one@example.com"},
		{ObjectMeta: store.ObjectMeta{ID: "two"}, Email: "two@example.com"},
	} {
		if err := storage.Create(ctx, object); err != nil {
			t.Fatalf("Create(%s) error = %v", object.ID, err)
		}
	}
	watcher, err := storage.Watch(ctx, &store.List[indexedObject]{},
		store.WithSendInitialEvents(),
		store.WithFieldRequirements(selector.Requirement{
			Key: "email", Operator: selector.Equals, Values: []any{"two@example.com"},
		}),
	)
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	defer watcher.Stop()
	for {
		select {
		case event, ok := <-watcher.Events():
			if !ok {
				t.Fatal("watch events closed before matching event")
			}
			if event.Error != nil {
				t.Fatalf("watch event error = %v", event.Error)
			}
			if event.Object == nil {
				continue
			}
			if event.Object.GetID() != "two" {
				t.Fatalf("watch event ID = %q, want two", event.Object.GetID())
			}
			return
		case <-ctx.Done():
			t.Fatal("timed out waiting for matching watch event")
		}
	}
}
