package inmemory

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	commonerrors "xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/selector"
	"xiaoshiai.cn/store/storetest"
)

func TestStoreConformance(t *testing.T) {
	capabilities := store.Capabilities{Finalizers: true, OptimisticLock: true, Watch: true}
	storetest.Run(t, storetest.Fixture{
		Capabilities:     capabilities,
		Continue:         true,
		LabelSelector:    true,
		FieldSelector:    true,
		Search:           true,
		Sort:             true,
		SubScopes:        true,
		DeleteBatch:      true,
		PatchBatch:       true,
		SecondaryIndexes: true,
		UniqueIndexes:    true,

		New: func(t testing.TB, schema *store.Schema) (store.Store, error) {
			return New(schema)
		},
	})
}

type user struct {
	store.ObjectMeta `json:",inline"`
	Email            string `json:"email,omitempty"`
	Phone            string `json:"phone,omitempty"`
	Enabled          bool   `json:"enabled,omitempty"`
	Team             string `json:"team,omitempty"`
}

func newUserStore(t *testing.T) *InMemory {
	t.Helper()
	schema := store.NewSchema()
	if err := schema.Register(&user{}, store.ResourceSchema{
		ScopeKeys: []string{"tenant"},
		Indexes: []store.Index{
			{Name: "email", Fields: []string{"email"}, Unique: true},
			{Name: "phone", Fields: []string{"phone"}, Unique: true, Nullable: true},
		},
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	storage, err := New(schema)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return storage
}

func TestUniqueIndexes(t *testing.T) {
	ctx := context.Background()
	storage := newUserStore(t)
	tenantA := storage.Scope(store.Scope{Resource: "tenants", Name: "a"})
	tenantB := storage.Scope(store.Scope{Resource: "tenants", Name: "b"})

	first := &user{ObjectMeta: store.ObjectMeta{ID: "one"}, Email: "one@example.com"}
	if err := tenantA.Create(ctx, first); err != nil {
		t.Fatalf("Create(first) error = %v", err)
	}
	duplicate := &user{ObjectMeta: store.ObjectMeta{ID: "two"}, Email: first.Email}
	if err := tenantA.Create(ctx, duplicate); !commonerrors.IsAlreadyExists(err) {
		t.Fatalf("Create(duplicate) error = %v, want AlreadyExists", err)
	}
	if err := tenantB.Create(ctx, duplicate); err != nil {
		t.Fatalf("Create(other scope) error = %v", err)
	}
}

func TestUniqueIndexUpdateAndDelete(t *testing.T) {
	ctx := context.Background()
	storage := newUserStore(t)
	tenant := storage.Scope(store.Scope{Resource: "tenants", Name: "a"})
	first := &user{ObjectMeta: store.ObjectMeta{ID: "one"}, Email: "old@example.com"}
	if err := tenant.Create(ctx, first); err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	first.Email = "new@example.com"
	if err := tenant.Update(ctx, first); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	second := &user{ObjectMeta: store.ObjectMeta{ID: "two"}, Email: "old@example.com"}
	if err := tenant.Create(ctx, second); err != nil {
		t.Fatalf("Create(reused index) error = %v", err)
	}
	if err := tenant.Delete(ctx, first); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	third := &user{ObjectMeta: store.ObjectMeta{ID: "three"}, Email: "new@example.com"}
	if err := tenant.Create(ctx, third); err != nil {
		t.Fatalf("Create(after delete) error = %v", err)
	}
}

func TestNullableUniqueIndex(t *testing.T) {
	ctx := context.Background()
	storage := newUserStore(t).
		Scope(store.Scope{Resource: "tenants", Name: "a"})
	for _, id := range []string{"one", "two"} {
		if err := storage.Create(ctx, &user{ObjectMeta: store.ObjectMeta{ID: id}, Email: id + "@example.com"}); err != nil {
			t.Fatalf("Create(%s) error = %v", id, err)
		}
	}
}

func TestConcurrentUniqueIndex(t *testing.T) {
	ctx := context.Background()
	storage := newUserStore(t).
		Scope(store.Scope{Resource: "tenants", Name: "a"})
	var successes atomic.Int32
	var wait sync.WaitGroup
	for i := range 20 {
		wait.Add(1)
		go func(i int) {
			defer wait.Done()
			err := storage.Create(ctx, &user{
				ObjectMeta: store.ObjectMeta{ID: fmt.Sprintf("user-%d", i)},
				Email:      "shared@example.com",
			})
			if err == nil {
				successes.Add(1)
				return
			}
			if !commonerrors.IsAlreadyExists(err) {
				t.Errorf("Create() error = %v, want AlreadyExists", err)
			}
		}(i)
	}
	wait.Wait()
	if got := successes.Load(); got != 1 {
		t.Fatalf("successful creates = %d, want 1", got)
	}
}

func TestGetUsesNameArgument(t *testing.T) {
	ctx := context.Background()
	storage := newUserStore(t)
	created := &user{
		ObjectMeta: store.ObjectMeta{ID: "one"},
		Email:      "one@example.com",
	}
	if err := storage.Create(ctx, created); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got := &user{}
	if err := storage.Get(ctx, created.ID, got); err != nil {
		t.Fatalf("Get(%q) error = %v", created.ID, err)
	}
	if got.ID != created.ID || got.Email != created.Email {
		t.Fatalf("Get(%q) = %#v, want %#v", created.ID, got, created)
	}
}

func TestListFiltersSortsAndPagesScopedObjects(t *testing.T) {
	ctx := context.Background()
	storage := newUserStore(t)
	tenantA := storage.Scope(store.Scope{
		Resource: "tenants",
		Name:     "a",
	})
	tenantB := storage.Scope(store.Scope{
		Resource: "tenants",
		Name:     "b",
	})
	for _, item := range []struct {
		storage store.Store
		user    *user
	}{
		{
			storage: tenantA,
			user: &user{
				ObjectMeta: store.ObjectMeta{
					ID:   "alice",
					Name: "Alice",
				},
				Email:   "alice@example.com",
				Enabled: true,
			},
		},
		{
			storage: tenantA,
			user: &user{
				ObjectMeta: store.ObjectMeta{
					ID:   "bob",
					Name: "Bob",
				},
				Email:   "bob@example.com",
				Enabled: true,
			},
		},
		{
			storage: tenantB,
			user: &user{
				ObjectMeta: store.ObjectMeta{
					ID:   "carol",
					Name: "Carol",
				},
				Email:   "carol@example.com",
				Enabled: false,
			},
		},
	} {
		if err := item.storage.Create(ctx, item.user); err != nil {
			t.Fatal(err)
		}
	}

	tenantUsers := &store.List[user]{}
	if err := tenantA.List(ctx, tenantUsers); err != nil {
		t.Fatal(err)
	}
	if len(tenantUsers.Items) != 2 {
		t.Fatalf("tenant users = %#v, want two", tenantUsers.Items)
	}
	if got := tenantUsers.Items[0].Scopes; len(got) != 1 || got[0].Name != "a" {
		t.Fatalf("tenant scopes = %#v, want tenant a", got)
	}

	allEnabled := &store.List[user]{}
	if err := storage.List(
		ctx,
		allEnabled,
		store.WithSubScopes(),
		store.WithFieldRequirements(selector.RequirementEqual("enabled", true)),
		store.WithSort("name-"),
		store.WithLimit(1),
	); err != nil {
		t.Fatal(err)
	}
	if allEnabled.Continue == "" ||
		len(allEnabled.Items) != 1 || allEnabled.Items[0].ID != "bob" {
		t.Fatalf("enabled users = %#v, continue %q",
			allEnabled.Items,
			allEnabled.Continue,
		)
	}
}

func TestPatchPersistsChangesAndUpdatesIndexes(t *testing.T) {
	ctx := context.Background()
	storage := newUserStore(t).
		Scope(store.Scope{
			Resource: "tenants",
			Name:     "a",
		})
	created := &user{
		ObjectMeta: store.ObjectMeta{ID: "alice"},
		Email:      "old@example.com",
	}
	if err := storage.Create(ctx, created); err != nil {
		t.Fatal(err)
	}
	previousVersion := created.ResourceVersion
	if err := storage.Patch(ctx, created, store.MapMergePatch{
		"email": "new@example.com",
		"phone": "1234",
	}); err != nil {
		t.Fatal(err)
	}
	if created.Email != "new@example.com" || created.Phone != "1234" {
		t.Fatalf("patched user = %#v", created)
	}
	if created.ResourceVersion <= previousVersion {
		t.Fatalf("resource version = %d, want greater than %d", created.ResourceVersion, previousVersion)
	}

	got := &user{}
	if err := storage.Get(ctx, created.ID, got); err != nil {
		t.Fatal(err)
	}
	if got.Email != "new@example.com" || got.Phone != "1234" {
		t.Fatalf("stored user = %#v", got)
	}
	reused := &user{
		ObjectMeta: store.ObjectMeta{ID: "bob"},
		Email:      "old@example.com",
	}
	if err := storage.Create(ctx, reused); err != nil {
		t.Fatalf("old index was not released: %v", err)
	}
}

func TestCountAndBatchOperationsUseSelectors(t *testing.T) {
	ctx := context.Background()
	storage := newUserStore(t).
		Scope(store.Scope{
			Resource: "tenants",
			Name:     "a",
		})
	for _, item := range []*user{
		{
			ObjectMeta: store.ObjectMeta{ID: "alice"},
			Email:      "alice@example.com",
			Enabled:    true,
		},
		{
			ObjectMeta: store.ObjectMeta{ID: "bob"},
			Email:      "bob@example.com",
			Enabled:    true,
		},
		{
			ObjectMeta: store.ObjectMeta{ID: "carol"},
			Email:      "carol@example.com",
			Enabled:    false,
		},
	} {
		if err := storage.Create(ctx, item); err != nil {
			t.Fatal(err)
		}
	}

	count, err := storage.Count(
		ctx,
		&user{},
		store.WithFieldRequirements(selector.RequirementEqual("enabled", true)),
	)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("enabled count = %d, want 2", count)
	}

	if err := storage.PatchBatch(
		ctx,
		&store.List[user]{},
		store.MapMergePatchBacth{"team": "platform"},
		store.WithFieldRequirements(selector.RequirementEqual("enabled", true)),
	); err != nil {
		t.Fatal(err)
	}
	patched := &store.List[user]{}
	if err := storage.List(
		ctx,
		patched,
		store.WithFieldRequirements(selector.RequirementEqual("team", "platform")),
	); err != nil {
		t.Fatal(err)
	}

	if err := storage.DeleteBatch(
		ctx,
		&store.List[user]{},
		store.WithFieldRequirements(selector.RequirementEqual("email", "carol@example.com")),
	); err != nil {
		t.Fatal(err)
	}
	remaining := &store.List[user]{}
	if err := storage.List(ctx, remaining); err != nil {
		t.Fatal(err)
	}

}

func TestWatchSendsInitialAndMutationEvents(t *testing.T) {
	ctx := t.Context()
	storage := newUserStore(t).
		Scope(store.Scope{
			Resource: "tenants",
			Name:     "a",
		})
	existing := &user{
		ObjectMeta: store.ObjectMeta{ID: "alice"},
		Email:      "alice@example.com",
		Enabled:    true,
	}
	if err := storage.Create(ctx, existing); err != nil {
		t.Fatal(err)
	}

	watcher, err := storage.Watch(
		ctx,
		&store.List[user]{},
		store.WithSendInitialEvents(),
		store.WithFieldRequirements(selector.RequirementEqual("enabled", true)),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Stop()
	requireWatchEvent(t, watcher, store.WatchEventCreate, "alice")
	requireWatchEvent(t, watcher, store.WatchEventBookmark, "")

	created := &user{
		ObjectMeta: store.ObjectMeta{ID: "bob"},
		Email:      "bob@example.com",
		Enabled:    true,
	}
	if err := storage.Create(ctx, created); err != nil {
		t.Fatal(err)
	}
	requireWatchEvent(t, watcher, store.WatchEventCreate, "bob")
	if err := storage.Patch(ctx, created, store.MapMergePatch{"phone": "1234"}); err != nil {
		t.Fatal(err)
	}
	requireWatchEvent(t, watcher, store.WatchEventUpdate, "bob")
	if err := storage.Delete(ctx, created); err != nil {
		t.Fatal(err)
	}
	requireWatchEvent(t, watcher, store.WatchEventDelete, "bob")
}

func requireWatchEvent(t *testing.T, watcher store.Watcher, eventType store.WatchEventType, id string) {
	t.Helper()
	select {
	case event := <-watcher.Events():
		if event.Error != nil {
			t.Fatal(event.Error)
		}
		if event.Type != eventType {
			t.Fatalf("event type = %q, want %q", event.Type, eventType)
		}
		if id == "" {
			return
		}
		item, ok := event.Object.(*user)
		if !ok || item.ID != id {
			t.Fatalf("event object = %#v, want user %q", event.Object, id)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %q event", eventType)
	}
}
