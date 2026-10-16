package reflector

import (
	"context"
	stderrors "errors"
	"sync"
	"testing"
	"time"

	commonerrors "xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
)

func TestReflectorPublishesSnapshotThenLiveEvents(t *testing.T) {
	initial := make(chan store.WatchEvent, 4)
	live := make(chan store.WatchEvent, 1)
	upstream := &reflectorTestStore{watches: []chan store.WatchEvent{initial, live}}
	object := reflectorTestObject("one", 1)
	initial <- store.WatchEvent{Type: store.WatchEventCreate, Object: object}
	initial <- store.WatchEvent{Type: store.WatchEventBookmark, ResourceVersion: 7}
	close(initial)
	updated := reflectorTestObject("one", 2)
	live <- store.WatchEvent{Type: store.WatchEventUpdate, Object: updated, ResourceVersion: 8}

	ctx, cancel := context.WithCancel(t.Context())
	handler := &reflectorTestHandler{applied: make(chan store.WatchEvent, 1)}
	reflector := NewReflector(upstream, &store.List[store.Unstructured]{Resource: "objects"})
	done := make(chan error, 1)
	go func() {
		done <- reflector.Run(ctx, handler)
	}()

	select {
	case <-reflector.Synced():
	case <-time.After(5 * time.Second):
		t.Fatal("Reflector did not publish its initial snapshot")
	}
	if len(handler.replaced) != 1 || handler.replaced[0].GetID() != "one" {
		t.Fatalf("Replace() objects = %#v, want object one", handler.replaced)
	}
	select {
	case event := <-handler.applied:
		if event.Type != store.WatchEventUpdate || event.Object.GetResourceVersion() != 2 {
			t.Fatalf("Apply() event = %#v, want updated object", event)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Reflector did not apply the live event")
	}

	cancel()
	if err := <-done; err != nil && !stderrors.Is(err, context.Canceled) {
		t.Fatalf("Reflector.Run() error = %v", err)
	}
	upstream.mu.Lock()
	defer upstream.mu.Unlock()
	if len(upstream.options) != 2 || !upstream.options[0].SendInitialEvents {
		t.Fatalf("Watch options = %#v, want initial Watch followed by resume", upstream.options)
	}
	if upstream.options[1].ResourceVersion == nil || *upstream.options[1].ResourceVersion != 7 || upstream.options[1].SendInitialEvents {
		t.Fatalf("resume options = %#v, want resourceVersion 7", upstream.options[1])
	}
}

func TestReflectorInvalidatesWhenHistoryIsUnavailable(t *testing.T) {
	initial := make(chan store.WatchEvent, 2)
	initial <- store.WatchEvent{Type: store.WatchEventBookmark, ResourceVersion: 9}
	close(initial)
	rebuilt := make(chan store.WatchEvent, 2)
	rebuilt <- store.WatchEvent{Type: store.WatchEventBookmark}
	upstream := &reflectorTestStore{
		watches: []chan store.WatchEvent{initial, rebuilt},
		errors:  map[int]error{1: commonerrors.NewResourceExpired("objects", "unavailable")},
	}
	ctx, cancel := context.WithCancel(t.Context())
	handler := &reflectorTestHandler{invalidated: make(chan error, 1)}
	reflector := NewReflector(upstream, &store.List[store.Unstructured]{Resource: "objects"})
	done := make(chan error, 1)
	go func() {
		done <- reflector.Run(ctx, handler)
	}()

	select {
	case err := <-handler.invalidated:
		if !commonerrors.IsResourceExpired(err) {
			t.Fatalf("Invalidate() error = %v, want ResourceExpired", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Reflector did not invalidate after ResourceExpired")
	}

	cancel()
	if err := <-done; err != nil && !stderrors.Is(err, context.Canceled) {
		t.Fatalf("Reflector.Run() error = %v", err)
	}
}

type reflectorTestStore struct {
	store.Store
	mu      sync.Mutex
	watches []chan store.WatchEvent
	errors  map[int]error
	options []store.WatchOptions
}

func (*reflectorTestStore) Capabilities() store.Capabilities {
	return store.Capabilities{Watch: true}
}

func (s *reflectorTestStore) Watch(_ context.Context, _ store.ObjectList, options ...store.WatchOption) (store.Watcher, error) {
	configured := store.ApplyWatchOptions(options)
	s.mu.Lock()
	call := len(s.options)
	s.options = append(s.options, configured)
	s.mu.Unlock()
	if err := s.errors[call]; err != nil {
		return nil, err
	}
	return &initialWatcher{events: s.watches[call]}, nil
}

type reflectorTestHandler struct {
	replaced    []*store.Unstructured
	applied     chan store.WatchEvent
	invalidated chan error
}

type initialWatcher struct {
	events chan store.WatchEvent
}

func (w *initialWatcher) Events() <-chan store.WatchEvent { return w.events }

func (*initialWatcher) Stop() {}

func (h *reflectorTestHandler) Replace(_ context.Context, objects []*store.Unstructured) error {
	h.replaced = objects
	return nil
}

func (h *reflectorTestHandler) Apply(_ context.Context, eventType store.WatchEventType, object *store.Unstructured) error {
	h.applied <- store.WatchEvent{Type: eventType, Object: object}
	return nil
}

func (h *reflectorTestHandler) Invalidate(_ context.Context, err error) {
	if h.invalidated != nil {
		h.invalidated <- err
	}
}

func reflectorTestObject(id string, resourceVersion int64) *store.Unstructured {
	object := &store.Unstructured{}
	object.SetResource("objects")
	object.SetID(id)
	object.SetUID("uid-" + id)
	object.SetResourceVersion(resourceVersion)
	return object
}
