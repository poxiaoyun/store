package mongo

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	commonerrors "xiaoshiai.cn/common/errors"
	testmongodb "xiaoshiai.cn/common/testkit/mongodb"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/selector"
)

type Message struct {
	store.ObjectMeta `json:",inline"`
}

func (*Message) ResourceName() string {
	return "messages"
}

func TestMongoStorageWatchIntegration(t *testing.T) {
	uri := testmongodb.RequireURI(t)
	database := RequireIntegrationDatabase(t, uri)
	storage := NewIntegrationStorage(t, database, &Message{})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	watcher, err := storage.Watch(ctx, &store.List[Message]{},
		store.WithLabelRequirements(selector.NewRequirement("example.com/team", selector.Equals, "platform")),
	)
	if err != nil {
		t.Fatalf("watch messages: %v", err)
	}
	defer watcher.Stop()

	message := &Message{ObjectMeta: store.ObjectMeta{
		ID:   "watch-message",
		Name: "watch integration test",
		Labels: map[string]string{
			"example.com/team": "platform",
		},
	}}
	if err := storage.Create(ctx, message); err != nil {
		t.Fatalf("create message: %v", err)
	}

	for {
		select {
		case event, ok := <-watcher.Events():
			if !ok {
				t.Fatal("watch closed before receiving the create event")
			}
			if event.Error != nil {
				t.Fatalf("watch event: %v", event.Error)
			}
			if event.Type == store.WatchEventCreate &&
				event.Object != nil &&
				event.Object.GetID() == message.GetID() {
				return
			}
		case <-ctx.Done():
			t.Fatalf("wait for create event: %v", ctx.Err())
		}
	}
}

func TestMongoStorageWatchInitialEventsAndSelectorTransitions(t *testing.T) {
	uri := testmongodb.RequireURI(t)
	database := RequireIntegrationDatabase(t, uri)
	storage := NewIntegrationStorage(t, database, &Message{})

	existing := &Message{ObjectMeta: store.ObjectMeta{
		ID:     "existing",
		Labels: map[string]string{"team": "platform"},
	}}
	if err := storage.Create(t.Context(), existing); err != nil {
		t.Fatalf("create existing message: %v", err)
	}

	watcher, err := storage.Watch(
		t.Context(),
		&store.List[Message]{},
		store.WithSendInitialEvents(),
		store.WithLabelRequirements(selector.NewRequirement("team", selector.Equals, "platform")),
	)
	if err != nil {
		t.Fatalf("watch messages: %v", err)
	}
	defer watcher.Stop()

	assertMongoWatchEvent(t, watcher, store.WatchEventCreate, "existing")
	bookmark := nextMongoWatchEvent(t, watcher)
	if bookmark.Type != store.WatchEventBookmark || bookmark.ResourceVersion != 0 || bookmark.Object != nil {
		t.Fatalf("initial bookmark = %#v", bookmark)
	}

	existing.Labels["team"] = "other"
	if err := storage.Update(t.Context(), existing); err != nil {
		t.Fatalf("move existing message out of selector: %v", err)
	}
	deleted := assertMongoWatchEvent(t, watcher, store.WatchEventDelete, "existing")
	if deleted.Object.GetLabels()["team"] != "platform" {
		t.Fatalf("selector delete labels = %v, want previous platform label", deleted.Object.GetLabels())
	}

	existing.Labels["team"] = "platform"
	if err := storage.Update(t.Context(), existing); err != nil {
		t.Fatalf("move existing message into selector: %v", err)
	}
	assertMongoWatchEvent(t, watcher, store.WatchEventCreate, "existing")

	if err := storage.Delete(t.Context(), existing); err != nil {
		t.Fatalf("delete existing message: %v", err)
	}
	deleted = assertMongoWatchEvent(t, watcher, store.WatchEventDelete, "existing")
	if deleted.Object.GetUID() == "" || deleted.Object.GetResource() != "messages" {
		t.Fatalf("delete tombstone = %#v, want complete previous object", deleted.Object)
	}
}

func TestMongoStorageInitialBookmarkIncludesChangesDuringSnapshot(t *testing.T) {
	uri := testmongodb.RequireURI(t)
	database := RequireIntegrationDatabase(t, uri)
	storage := NewIntegrationStorage(t, database, &Message{})

	const objectCount = 80
	objects := make([]*Message, 0, objectCount)
	for index := range objectCount {
		object := &Message{ObjectMeta: store.ObjectMeta{ID: fmt.Sprintf("snapshot-%03d", index)}}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("create initial message %d: %v", index, err)
		}
		objects = append(objects, object)
	}

	watcher, err := storage.Watch(t.Context(), &store.List[Message]{}, store.WithSendInitialEvents())
	if err != nil {
		t.Fatalf("watch messages: %v", err)
	}
	defer watcher.Stop()
	if err := storage.Delete(t.Context(), objects[0]); err != nil {
		t.Fatalf("delete during initial snapshot: %v", err)
	}

	initial := map[string]bool{}
	for {
		event := nextMongoWatchEvent(t, watcher)
		if event.Type == store.WatchEventBookmark {
			break
		}
		switch event.Type {
		case store.WatchEventCreate, store.WatchEventUpdate:
			initial[event.Object.GetID()] = true
		case store.WatchEventDelete:
			delete(initial, event.Object.GetID())
		}
	}
	if initial[objects[0].ID] {
		t.Fatalf("initial state contains %q deleted before Bookmark", objects[0].ID)
	}
}

func TestMongoStorageWatchReturnsResourceExpiredForUnavailableVersion(t *testing.T) {
	storage := &MongoStorage{}

	_, err := storage.Watch(t.Context(), &store.List[Message]{}, store.WithResourceVersion(1))
	if !commonerrors.IsResourceExpired(err) {
		t.Fatalf("Watch() error = %v, want ResourceExpired", err)
	}
}

func TestMongoStorageWatchStopClosesEvents(t *testing.T) {
	uri := testmongodb.RequireURI(t)
	database := RequireIntegrationDatabase(t, uri)
	storage := NewIntegrationStorage(t, database, &Message{})

	watcher, err := storage.Watch(t.Context(), &store.List[Message]{})
	if err != nil {
		t.Fatalf("watch messages: %v", err)
	}
	watcher.Stop()
	watcher.Stop()

	select {
	case event, ok := <-watcher.Events():
		if ok {
			t.Fatalf("watch emitted an event after Stop: %#v", event)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watch event channel did not close after Stop")
	}
}

func nextMongoWatchEvent(t *testing.T, watcher store.Watcher) store.WatchEvent {
	t.Helper()
	select {
	case event, ok := <-watcher.Events():
		if !ok {
			t.Fatal("watch closed before the expected event")
		}
		if event.Error != nil {
			t.Fatalf("watch event error: %v", event.Error)
		}
		return event
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for watch event")
		return store.WatchEvent{}
	}
}

func assertMongoWatchEvent(t *testing.T, watcher store.Watcher, eventType store.WatchEventType, id string) store.WatchEvent {
	t.Helper()
	event := nextMongoWatchEvent(t, watcher)
	if event.Type != eventType || event.Object == nil || event.Object.GetID() != id {
		t.Fatalf("watch event = %#v, want type %q and id %q", event, eventType, id)
	}
	return event
}

func TestMongoStorageWatchScopeIdentity(t *testing.T) {
	for _, test := range []struct {
		name        string
		root        bool
		descendants bool
	}{
		{name: "root exact", root: true},
		{name: "nested exact"},
		{name: "nested descendants", descendants: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			uri := testmongodb.RequireURI(t)
			database := RequireIntegrationDatabase(t, uri)
			root := NewIntegrationStorage(t, database, &Message{})
			scopes := []store.Scope{{Resource: "iam.organization", Name: "one"}, {Resource: "moha.repository", Name: "private"}}
			if test.root {
				scopes = nil
			}
			parent := root.Scope(scopes...)
			childScopes := append(slices.Clone(scopes), store.Scope{Resource: "moha.repository", Name: "nested"})
			child := root.Scope(childScopes...)
			if err := parent.Create(t.Context(), &Message{ObjectMeta: store.ObjectMeta{ID: "parent"}}); err != nil {
				t.Fatal(err)
			}
			if err := child.Create(t.Context(), &Message{ObjectMeta: store.ObjectMeta{ID: "child"}}); err != nil {
				t.Fatal(err)
			}
			options := []store.WatchOption{store.WithSendInitialEvents()}
			if test.descendants {
				options = append(options, store.WithSubScopes())
			}
			watcher, err := parent.Watch(t.Context(), &store.List[Message]{}, options...)
			if err != nil {
				t.Fatal(err)
			}
			defer watcher.Stop()
			seen := map[string][]store.Scope{}
			for {
				event := nextMongoWatchEvent(t, watcher)
				if event.Type == store.WatchEventBookmark {
					break
				}
				seen[event.Object.GetID()] = event.Object.GetScopes()
			}
			expectedCount := 1
			if test.descendants {
				expectedCount++
				if !slices.Equal(seen["child"], childScopes) {
					t.Fatalf("child Watch scope = %#v, want %#v", seen["child"], childScopes)
				}
			}
			if len(seen) != expectedCount || !slices.Equal(seen["parent"], scopes) {
				t.Fatalf("initial scoped Watch = %#v", seen)
			}
			// Stream order makes an incorrectly included sibling event observable
			// before the matching event; no timeout or quiet-window guess is needed.
			sibling := root.Scope(store.Scope{Resource: "iam.organization", Name: "other"})
			if err := sibling.Create(t.Context(), &Message{ObjectMeta: store.ObjectMeta{ID: "sibling"}}); err != nil {
				t.Fatal(err)
			}
			target, targetScopes := parent, scopes
			if test.descendants {
				target, targetScopes = child, childScopes
			}
			live := &Message{ObjectMeta: store.ObjectMeta{ID: "live"}}
			if err := target.Create(t.Context(), live); err != nil {
				t.Fatal(err)
			}
			event := assertMongoWatchEvent(t, watcher, store.WatchEventCreate, "live")
			if !slices.Equal(event.Object.GetScopes(), targetScopes) {
				t.Fatalf("live Watch scope = %#v, want %#v", event.Object.GetScopes(), targetScopes)
			}
			live.Description = "updated"
			if err := target.Update(t.Context(), live); err != nil {
				t.Fatal(err)
			}
			assertMongoWatchEvent(t, watcher, store.WatchEventUpdate, "live")
			if err := target.Delete(t.Context(), live); err != nil {
				t.Fatal(err)
			}
			event = assertMongoWatchEvent(t, watcher, store.WatchEventDelete, "live")
			if !slices.Equal(event.Object.GetScopes(), targetScopes) || event.Object.GetDescription() != "updated" {
				t.Fatalf("delete Watch lost previous scope or object: %#v", event.Object)
			}
		})
	}
}
