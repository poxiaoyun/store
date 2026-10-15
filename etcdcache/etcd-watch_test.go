package etcdcache

import (
	"context"
	"testing"
	"time"

	"go.etcd.io/etcd/client/v3/kubernetes"
	"xiaoshiai.cn/common/selector"
	"xiaoshiai.cn/common/store"
)

func testEtcdCacherWatch(t *testing.T, client *kubernetes.Client) {
	t.Run("initial bookmark and mutations", func(t *testing.T) {
		ctx := context.Background()
		storage := newTestStore(t, ctx, client, newMyObjectSchema(t))
		existing := newMyObject("existing", "existing", true, "blue")
		if err := storage.Create(ctx, existing); err != nil {
			t.Fatalf("Create(existing) error = %v", err)
		}

		watcher := openTestWatcher(t, ctx, storage, &store.List[MyObject]{}, store.WithSendInitialEvents())
		assertWatchEvent(t, nextWatchEvent(t, watcher), store.WatchEventCreate, "existing")
		assertWatchEvent(t, nextWatchEvent(t, watcher), store.WatchEventBookmark, "")

		created := newMyObject("created", "created", true, "blue")
		if err := storage.Create(ctx, created); err != nil {
			t.Fatalf("Create(created) error = %v", err)
		}
		created.Spec.Value = "updated"
		if err := storage.Update(ctx, created); err != nil {
			t.Fatalf("Update(created) error = %v", err)
		}
		if err := storage.Delete(ctx, created); err != nil {
			t.Fatalf("Delete(created) error = %v", err)
		}

		assertWatchEvent(t, nextWatchEvent(t, watcher), store.WatchEventCreate, "created")
		updated := nextWatchEvent(t, watcher)
		assertWatchEvent(t, updated, store.WatchEventUpdate, "created")
		if object := updated.Object.(*MyObject); object.Spec.Value != "updated" {
			t.Fatalf("update event value = %q, want updated", object.Spec.Value)
		}
		assertWatchEvent(t, nextWatchEvent(t, watcher), store.WatchEventDelete, "created")

		watcher.Stop()
		watcher.Stop()
		assertWatcherClosed(t, watcher)
	})

	t.Run("ID selector and scope filtering", func(t *testing.T) {
		ctx := context.Background()
		storage := newTestStore(t, ctx, client, newMyObjectSchema(t))
		watcher := watchFromCurrent(t, ctx, storage, store.WithID("target"))
		if err := storage.Create(ctx, newMyObject("other", "other", true, "blue")); err != nil {
			t.Fatalf("Create(other) error = %v", err)
		}
		if err := storage.Create(ctx, newMyObject("target", "target", true, "blue")); err != nil {
			t.Fatalf("Create(target) error = %v", err)
		}
		assertWatchEvent(t, nextWatchEvent(t, watcher), store.WatchEventCreate, "target")
		watcher.Stop()
		assertWatcherClosed(t, watcher)

		rootWatcher := watchFromCurrent(t, ctx, storage)
		scoped := storage.Scope(store.Scope{Resource: "organizations", Name: "a"})
		if err := scoped.Create(ctx, newMyObject("scoped", "scoped", true, "blue")); err != nil {
			t.Fatalf("scoped Create() error = %v", err)
		}
		if err := storage.Create(ctx, newMyObject("root", "root", true, "blue")); err != nil {
			t.Fatalf("root Create() error = %v", err)
		}
		assertWatchEvent(t, nextWatchEvent(t, rootWatcher), store.WatchEventCreate, "root")
		rootWatcher.Stop()
		assertWatcherClosed(t, rootWatcher)

		subscopeWatcher := watchFromCurrent(t, ctx, storage, store.WithSubScopes())
		if err := scoped.Create(ctx, newMyObject("scoped-2", "scoped-2", true, "blue")); err != nil {
			t.Fatalf("second scoped Create() error = %v", err)
		}
		assertWatchEvent(t, nextWatchEvent(t, subscopeWatcher), store.WatchEventCreate, "scoped-2")
		subscopeWatcher.Stop()
		assertWatcherClosed(t, subscopeWatcher)
	})

	t.Run("field selector", func(t *testing.T) {
		ctx := context.Background()
		storage := newTestStore(t, ctx, client, newMyObjectSchema(t))
		watcher := watchFromCurrent(
			t,
			ctx,
			storage,
			store.WithFieldRequirements(selector.RequirementEqual("enabled", true)),
		)
		if err := storage.Create(ctx, newMyObject("disabled", "disabled", false, "blue")); err != nil {
			t.Fatalf("Create(disabled) error = %v", err)
		}
		if err := storage.Create(ctx, newMyObject("enabled", "enabled", true, "blue")); err != nil {
			t.Fatalf("Create(enabled) error = %v", err)
		}
		assertWatchEvent(t, nextWatchEvent(t, watcher), store.WatchEventCreate, "enabled")
		watcher.Stop()
		assertWatcherClosed(t, watcher)
	})

	t.Run("request context closes result channel", func(t *testing.T) {
		storage := newTestStore(t, context.Background(), client, newMyObjectSchema(t))
		watchCtx, cancel := context.WithCancel(context.Background())
		watcher := watchFromCurrent(t, watchCtx, storage)
		cancel()
		assertWatcherClosed(t, watcher)
		watcher.Stop()
	})
}

func watchFromCurrent(t *testing.T, ctx context.Context, storage store.Store, opts ...store.WatchOption) store.Watcher {
	t.Helper()
	snapshot := &store.List[MyObject]{}
	if err := storage.List(ctx, snapshot, store.WithSubScopes()); err != nil {
		t.Fatalf("List() before Watch() error = %v", err)
	}
	opts = append(opts, store.WithResourceVersion(snapshot.ResourceVersion))
	return openTestWatcher(t, ctx, storage, &store.List[MyObject]{}, opts...)
}

func openTestWatcher(t *testing.T, ctx context.Context, storage store.Store, list store.ObjectList, opts ...store.WatchOption) store.Watcher {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		watcher, err := storage.Watch(ctx, list, opts...)
		if err == nil {
			return watcher
		}
		lastErr = err
		select {
		case <-ctx.Done():
			t.Fatalf("Watch() context ended while opening: %v", ctx.Err())
		case <-deadline.C:
			t.Fatalf("Watch() did not become ready: %v", lastErr)
		case <-ticker.C:
		}
	}
}

func nextWatchEvent(t *testing.T, watcher store.Watcher) store.WatchEvent {
	t.Helper()
	select {
	case event, ok := <-watcher.Events():
		if !ok {
			t.Fatal("watch result channel closed before the expected event")
		}
		if event.Error != nil {
			t.Fatalf("watch event error = %v", event.Error)
		}
		return event
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for watch event")
		return store.WatchEvent{}
	}
}

func assertWatchEvent(t *testing.T, event store.WatchEvent, wantType store.WatchEventType, wantID string) {
	t.Helper()
	if event.Type != wantType {
		t.Fatalf("watch event type = %q, want %q", event.Type, wantType)
	}
	if wantType == store.WatchEventBookmark {
		if event.Object != nil {
			t.Fatalf("bookmark object = %#v, want nil", event.Object)
		}
		return
	}
	object, ok := event.Object.(*MyObject)
	if !ok {
		t.Fatalf("watch event object type = %T, want *MyObject", event.Object)
	}
	if object.ID != wantID {
		t.Fatalf("watch event object ID = %q, want %q", object.ID, wantID)
	}
}

func assertWatcherClosed(t *testing.T, watcher store.Watcher) {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-watcher.Events():
			if !ok {
				return
			}
		case <-timer.C:
			t.Fatal("watch result channel did not close")
		}
	}
}
