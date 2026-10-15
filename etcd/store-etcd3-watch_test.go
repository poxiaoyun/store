package etcd

import (
	"context"
	"reflect"
	"testing"
	"time"

	"k8s.io/utils/ptr"
	"xiaoshiai.cn/common/store"
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

	obj2changed := *obj2
	obj2changed.Spec.Replicas = ptr.To(int32(2))
	if err := etcdStore.Update(ctx, &obj2changed); err != nil {
		t.Fatalf("failed to update object: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	events := receiveEvents(ctx, 4, w)

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
