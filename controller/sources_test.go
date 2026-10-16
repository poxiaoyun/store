package controller

import (
	"context"
	"testing"

	"xiaoshiai.cn/store"
)

func TestRunListWatchRequestsInitialEventsAndPublishesSnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	object := &store.Unstructured{}
	object.SetResource("objects")
	object.SetID("one")
	object.SetUID("uid-one")
	watches := &sourceTestStore{
		events: []store.WatchEvent{
			{Type: store.WatchEventCreate, Object: object},
			{Type: store.WatchEventBookmark, ResourceVersion: 42},
		},
	}
	var received TypedWatchEvent[*store.Unstructured]
	handler := EventHandlerFunc[*store.Unstructured](func(_ context.Context, event TypedWatchEvent[*store.Unstructured]) error {
		received = event
		cancel()
		return nil
	})
	if err := RunListWatch(ctx, watches, "objects", true, handler); err != nil {
		t.Fatalf("RunListWatch() error = %v", err)
	}
	if !watches.options.SendInitialEvents || !watches.options.IncludeSubScopes {
		t.Fatalf("WatchOptions = %#v, want initial events and subscopes", watches.options)
	}
	if watches.options.ResourceVersion != nil {
		t.Fatalf("WatchOptions.ResourceVersion = %v, want nil", *watches.options.ResourceVersion)
	}
	if received.Type != store.WatchEventCreate || received.Object.GetID() != "one" {
		t.Fatalf("handler event = %#v, want initial Create", received)
	}
}

type sourceTestStore struct {
	store.Store
	options store.WatchOptions
	events  []store.WatchEvent
}

func (*sourceTestStore) Capabilities() store.Capabilities {
	return store.Capabilities{Watch: true}
}

func (s *sourceTestStore) Watch(_ context.Context, _ store.ObjectList, options ...store.WatchOption) (store.Watcher, error) {
	s.options = store.ApplyWatchOptions(options)
	results := make(chan store.WatchEvent, len(s.events))
	for _, event := range s.events {
		results <- event
	}
	return &sourceTestWatcher{events: results}, nil
}

type sourceTestWatcher struct {
	events chan store.WatchEvent
}

func (w *sourceTestWatcher) Stop() {}

func (w *sourceTestWatcher) Events() <-chan store.WatchEvent { return w.events }
