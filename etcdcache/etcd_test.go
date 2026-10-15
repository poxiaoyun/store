package etcdcache

import (
	"context"
	stderrors "errors"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"k8s.io/apiserver/pkg/storage/etcd3/testserver"
	commonerrors "xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/selector"
	"xiaoshiai.cn/common/store"
	"xiaoshiai.cn/common/store/storetest"
)

func TestStoreConformance(t *testing.T) {
	client := testserver.RunEtcd(t, nil)
	capabilities := (&generic{}).Capabilities()
	storetest.Run(t, storetest.Fixture{
		Capabilities: capabilities,
		New: func(t testing.TB, schema *store.Schema) (store.Store, error) {
			storage, err := NewEtcdCacherFromClient(t.Context(), client, schema, "/store-conformance/"+uuid.NewString())
			if err == nil {
				t.Cleanup(storage.Close)
			}
			return storage, err
		},
	})
}

func TestCachedReadsWaitForCacheReady(t *testing.T) {
	client := testserver.RunEtcd(t, nil)
	storage := newTestStore(t, t.Context(), client, newMyObjectSchema(t))
	db, err := storage.core.getResource("myobjects")
	if err != nil {
		t.Fatal(err)
	}
	readiness := db.storage.(interface{ ReadinessCheck() error })
	assertEventually(t, 5*time.Second, readiness.ReadinessCheck)
	if err := client.Client.Close(); err != nil {
		t.Fatal(err)
	}
	assertEventually(t, 5*time.Second, func() error {
		if readiness.ReadinessCheck() == nil {
			return stderrors.New("cache is still ready")
		}
		return nil
	})

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	err = storage.List(ctx, &store.List[MyObject]{})
	if !stderrors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("List() error = %v, want context deadline exceeded", err)
	}

	ctx, cancel = context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	_, err = storage.Count(ctx, &MyObject{})
	if !stderrors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Count() error = %v, want context deadline exceeded", err)
	}
}

type MyObject struct {
	store.ObjectMeta `json:",inline"`
	Enabled          bool           `json:"enabled"`
	Spec             MyObjectSpec   `json:"spec"`
	Status           MyObjectStatus `json:"status,omitempty"`
}

func (*MyObject) ResourceName() string { return "myobjects" }

type MyObjectSpec struct {
	Value string `json:"value"`
}

type MyObjectStatus struct {
	Phase string `json:"phase,omitempty"`
}

var testPrefixSequence atomic.Uint64

func TestEtcdCacherStore(t *testing.T) {
	client := testserver.RunEtcd(t, nil)

	t.Run("CRUD metadata status and errors", func(t *testing.T) {
		ctx := context.Background()
		storage := newTestStore(t, ctx, client, newMyObjectSchema(t))

		if err := storage.Ping(ctx); err != nil {
			t.Fatalf("Ping() error = %v", err)
		}
		generated := &MyObject{}
		if err := storage.Create(ctx, generated); err != nil {
			t.Fatalf("Create() without ID error = %v", err)
		}
		if generated.ID == "" {
			t.Fatal("Create() without ID did not generate an ID")
		}

		object := &MyObject{
			ObjectMeta: store.ObjectMeta{
				ID:     "one",
				Name:   "One",
				Labels: map[string]string{"team": "blue"},
			},
			Enabled: true,
			Spec:    MyObjectSpec{Value: "original"},
			Status:  MyObjectStatus{Phase: "Pending"},
		}
		if err := storage.Create(ctx, object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		if object.UID == "" || object.ResourceVersion == 0 || object.Generation != 1 || object.Resource != "myobjects" || object.CreationTimestamp.IsZero() {
			t.Fatalf("Create() metadata = %#v", object.ObjectMeta)
		}
		if err := storage.Create(ctx, &MyObject{ObjectMeta: store.ObjectMeta{ID: object.ID}}); !commonerrors.IsAlreadyExists(err) {
			t.Fatalf("duplicate Create() error = %v, want already exists", err)
		}

		got := &MyObject{ObjectMeta: store.ObjectMeta{ID: "ignored"}}
		if err := storage.Get(ctx, object.ID, got); err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if !reflect.DeepEqual(got, object) {
			t.Fatalf("Get() = %#v, want %#v", got, object)
		}

		stale := *got
		got.Spec.Value = "updated"
		if err := storage.Update(ctx, got); err != nil {
			t.Fatalf("Update() error = %v", err)
		}
		if got.Generation != 2 || got.Status.Phase != "Pending" || got.ResourceVersion <= stale.ResourceVersion {
			t.Fatalf("Update() result = %#v", got)
		}
		stale.Spec.Value = "stale"
		if err := storage.Update(ctx, &stale); !commonerrors.IsConflict(err) {
			t.Fatalf("stale Update() error = %v, want conflict", err)
		}

		jsonPatch := store.RawPatch(store.PatchTypeJSONPatch, []byte(`[{"op":"replace","path":"/spec/value","value":"json-patched"}]`))
		if err := storage.Patch(ctx, got, jsonPatch); err != nil {
			t.Fatalf("JSON Patch() error = %v", err)
		}
		if got.Spec.Value != "json-patched" || got.Status.Phase != "Pending" || got.Generation != 3 {
			t.Fatalf("JSON Patch() result = %#v", got)
		}

		if err := storage.Patch(ctx, got, store.MapMergePatch{"enabled": false}); err != nil {
			t.Fatalf("merge Patch() error = %v", err)
		}
		if got.Enabled || got.Status.Phase != "Pending" || got.Generation != 4 {
			t.Fatalf("merge Patch() result = %#v", got)
		}

		statusUpdate := *got
		statusUpdate.Spec.Value = "must-not-change"
		statusUpdate.Status.Phase = "Running"
		generation := got.Generation
		if err := storage.Status().Update(ctx, &statusUpdate); err != nil {
			t.Fatalf("Status().Update() error = %v", err)
		}
		if err := storage.Get(ctx, got.ID, got); err != nil {
			t.Fatalf("Get() after status update error = %v", err)
		}
		if got.Spec.Value != "json-patched" || got.Status.Phase != "Running" || got.Generation != generation {
			t.Fatalf("Status().Update() persisted %#v", got)
		}

		if err := storage.Status().Patch(ctx, got, store.MapMergePatch{
			"status": map[string]any{"phase": "Ready"},
			"spec":   map[string]any{"value": "must-not-change"},
		}); err != nil {
			t.Fatalf("Status().Patch() error = %v", err)
		}
		if got.Spec.Value != "json-patched" || got.Status.Phase != "Ready" || got.Generation != generation {
			t.Fatalf("Status().Patch() result = %#v", got)
		}

		if err := storage.Delete(ctx, got); err != nil {
			t.Fatalf("Delete() error = %v", err)
		}
		if err := storage.Get(ctx, got.ID, &MyObject{}); !commonerrors.IsNotFound(err) {
			t.Fatalf("Get() after delete error = %v, want not found", err)
		}

		list := &store.List[MyObject]{}
		if err := storage.PatchBatch(ctx, list, store.MapMergePatchBacth{}); !commonerrors.IsCode(err, http.StatusNotImplemented) {
			t.Fatalf("PatchBatch() error = %v, want not implemented", err)
		}
		if err := storage.DeleteBatch(ctx, list); !commonerrors.IsCode(err, http.StatusNotImplemented) {
			t.Fatalf("DeleteBatch() error = %v, want not implemented", err)
		}
	})

	t.Run("scopes selectors count search and local pagination", func(t *testing.T) {
		ctx := context.Background()
		storage := newTestStore(t, ctx, client, newMyObjectSchema(t))
		orgA := storage.Scope(store.Scope{Resource: "organizations", Name: "a"})

		objects := []*MyObject{
			newMyObject("root-blue", "root blue", true, "blue"),
			newMyObject("root-red", "root red", false, "red"),
		}
		for _, object := range objects {
			if err := storage.Create(ctx, object); err != nil {
				t.Fatalf("Create(%q) error = %v", object.ID, err)
			}
		}
		for _, object := range []*MyObject{
			newMyObject("scope-blue", "scope blue", true, "blue"),
			newMyObject("scope-red", "scope red", false, "red"),
		} {
			if err := orgA.Create(ctx, object); err != nil {
				t.Fatalf("scoped Create(%q) error = %v", object.ID, err)
			}
		}

		assertListIDs(t, ctx, storage, []string{"root-blue", "root-red"})
		assertListIDs(t, ctx, orgA, []string{"scope-blue", "scope-red"})
		assertListIDs(t, ctx, storage, []string{"root-blue", "root-red", "scope-blue", "scope-red"}, store.WithSubScopes())
		assertListIDs(t, ctx, storage, []string{"scope-blue", "scope-red"},
			store.WithSubScopes(),
			store.WithFieldRequirements(selector.NewRequirement("organization", selector.Exists)),
		)
		assertListIDs(t, ctx, storage, []string{"root-blue", "root-red"},
			store.WithSubScopes(),
			store.WithFieldRequirements(selector.NewRequirement("organization", selector.DoesNotExist)),
		)
		assertListIDs(t, ctx, storage, []string{"root-blue"}, store.WithLabelRequirementsFromSet(map[string]string{"team": "blue"}))
		assertListIDs(t, ctx, storage, []string{"root-blue"}, store.WithFieldRequirements(selector.RequirementEqual("enabled", true)))
		assertListIDs(t, ctx, storage, []string{"root-blue"}, store.WithSearch("blue"), store.WithSearchFields("name"))

		if count, err := storage.Count(ctx, &MyObject{}); err != nil || count != 2 {
			t.Fatalf("Count() = %d, %v, want 2, nil", count, err)
		}
		if count, err := storage.Count(ctx, &MyObject{}, store.WithSubScopes()); err != nil || count != 4 {
			t.Fatalf("Count(subscopes) = %d, %v, want 4, nil", count, err)
		}
		if count, err := storage.Count(ctx, &MyObject{}, store.WithLabelRequirements(selector.RequirementEqual("team", "red"))); err != nil || count != 1 {
			t.Fatalf("Count(red) = %d, %v, want 1, nil", count, err)
		}
		if err := storage.Get(ctx, "root-red", &MyObject{}, store.WithLabelRequirements(selector.RequirementEqual("team", "blue"))); !commonerrors.IsNotFound(err) {
			t.Fatalf("Get() with non-matching selector error = %v, want not found", err)
		}

		page := &store.List[MyObject]{}
		if err := storage.List(ctx, page, store.WithPage(0, 1), store.WithSort("-name")); err != nil {
			t.Fatalf("List(local page) error = %v", err)
		}
		if len(page.Items) != 1 || page.Items[0].ID != "root-red" || page.Total == nil || *page.Total != 2 ||
			page.Page != 1 || page.Size != 1 || page.Continue != "" || page.Limit != 0 {
			t.Fatalf("List(local page) = %#v", page)
		}
	})

	t.Run("continuation pagination fills filtered pages", func(t *testing.T) {
		ctx := context.Background()
		storage := newTestStore(t, ctx, client, newMyObjectSchema(t))
		for _, id := range []string{
			"aaa-skip-00",
			"aaa-skip-01",
			"zzz-match-00",
			"zzz-match-01",
			"zzz-match-02",
		} {
			if err := storage.Create(ctx, newMyObject(id, id, true, "blue")); err != nil {
				t.Fatalf("Create(%q) error = %v", id, err)
			}
		}

		var (
			continueToken string
			got           []string
		)
		for pageNumber := 0; ; pageNumber++ {
			page := &store.List[MyObject]{}
			if err := storage.List(ctx, page,
				store.WithPage(99, 1),
				store.WithContinuation(continueToken, 2),
				store.WithSearch("match"),
			); err != nil {
				t.Fatalf("List(page %d) error = %v", pageNumber, err)
			}
			if len(page.Items) == 0 || len(page.Items) > 2 || page.Total != nil ||
				page.Page != 0 || page.Size != 0 || page.Limit != 2 {
				t.Fatalf("List(page %d) = %#v", pageNumber, page)
			}
			for _, object := range page.Items {
				got = append(got, object.ID)
			}
			if page.Continue == "" {
				break
			}
			if page.Continue == continueToken {
				t.Fatalf("List(page %d) did not advance continue token", pageNumber)
			}
			continueToken = page.Continue
		}
		if want := []string{"zzz-match-00", "zzz-match-01", "zzz-match-02"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("continuation IDs = %v, want %v", got, want)
		}
	})

	t.Run("restart reads all preexisting objects", func(t *testing.T) {
		ctx := context.Background()
		prefix := nextTestPrefix()
		first := newTestStoreAtPrefix(t, ctx, client, newMyObjectSchema(t), prefix)
		for i := range 20 {
			id := fmt.Sprintf("existing-%02d", i)
			if err := first.Create(ctx, newMyObject(id, id, i%2 == 0, "blue")); err != nil {
				t.Fatalf("Create(%q) error = %v", id, err)
			}
		}
		first.Close()

		second := newTestStoreAtPrefix(t, ctx, client, newMyObjectSchema(t), prefix)
		assertEventually(t, 10*time.Second, func() error {
			list := &store.List[MyObject]{}
			if err := second.List(ctx, list); err != nil {
				return err
			}
			if len(list.Items) != 20 {
				return fmt.Errorf("List() returned %d objects, want 20", len(list.Items))
			}
			return nil
		})
		list := &store.List[MyObject]{}
		if err := second.List(ctx, list, store.WithResourceVersion(0)); err != nil {
			t.Fatalf("cached List() error = %v", err)
		}
		if len(list.Items) != 20 {
			t.Fatalf("cached List() returned %d objects, want 20", len(list.Items))
		}
	})

	t.Run("close and context cancellation release resources", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		storage := newTestStore(t, ctx, client, newMyObjectSchema(t))
		if err := storage.Create(context.Background(), newMyObject("one", "one", true, "blue")); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		cancel()
		assertEventually(t, time.Second, func() error {
			if err := storage.Create(context.Background(), newMyObject("two", "two", true, "blue")); err == nil {
				return fmt.Errorf("Create() after root context cancellation succeeded")
			}
			return nil
		})
		storage.Close()
		storage.Close()
		if err := storage.Ping(context.Background()); err != nil {
			t.Fatalf("client supplied by caller was closed: %v", err)
		}

		owned, err := NewEtcdCacher(context.Background(), newMyObjectSchema(t), &Options{
			Servers:   client.Client.Endpoints(),
			KeyPrefix: nextTestPrefix(),
		})
		if err != nil {
			t.Fatalf("NewEtcdCacher() error = %v", err)
		}
		if err := owned.Ping(context.Background()); err != nil {
			t.Fatalf("owned Ping() error = %v", err)
		}
		owned.Close()
		owned.Close()
		if err := owned.Ping(context.Background()); err == nil {
			t.Fatal("owned Ping() after Close() succeeded")
		}
	})

	t.Run("schema rejects unsupported unique indexes", func(t *testing.T) {
		schema := store.NewSchema()
		if err := schema.Register(&MyObject{}, store.ResourceSchema{
			Indexes: []store.Index{{Name: "unique-name", Fields: []string{"name"}, Unique: true}},
		}); err != nil {
			t.Fatalf("Register() error = %v", err)
		}
		storage, err := NewEtcdCacherFromClient(context.Background(), client, schema, nextTestPrefix())
		if err == nil {
			storage.Close()
			t.Fatal("NewEtcdCacherFromClient() error = nil, want unsupported unique index error")
		}
	})

	t.Run("watch", func(t *testing.T) {
		testEtcdCacherWatch(t, client)
	})
}

func newMyObjectSchema(t *testing.T) *store.Schema {
	t.Helper()
	schema := store.NewSchema()
	if err := schema.Register(&MyObject{}, store.ResourceSchema{
		Indexes: []store.Index{
			{Name: "enabled", Fields: []string{"enabled"}},
			{Name: "organization", Fields: []string{"organization"}},
		},
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	return schema
}

func newTestStore(t *testing.T, ctx context.Context, client *kubernetes.Client, schema *store.Schema) *generic {
	t.Helper()
	return newTestStoreAtPrefix(t, ctx, client, schema, nextTestPrefix())
}

func newTestStoreAtPrefix(t *testing.T, ctx context.Context, client *kubernetes.Client, schema *store.Schema, prefix string) *generic {
	t.Helper()
	storage, err := NewEtcdCacherFromClient(ctx, client, schema, prefix)
	if err != nil {
		t.Fatalf("NewEtcdCacherFromClient() error = %v", err)
	}
	t.Cleanup(storage.Close)
	return storage
}

func nextTestPrefix() string {
	return fmt.Sprintf("/etcdcache-test/%d", testPrefixSequence.Add(1))
}

func newMyObject(id, name string, enabled bool, team string) *MyObject {
	return &MyObject{
		ObjectMeta: store.ObjectMeta{
			ID:     id,
			Name:   name,
			Labels: map[string]string{"team": team},
		},
		Enabled: enabled,
		Spec:    MyObjectSpec{Value: id},
	}
}

func assertListIDs(t *testing.T, ctx context.Context, storage store.Store, want []string, opts ...store.ListOption) {
	t.Helper()
	list := &store.List[MyObject]{}
	if err := storage.List(ctx, list, opts...); err != nil {
		t.Fatalf("List() error = %v", err)
	}
	got := make([]string, 0, len(list.Items))
	for _, object := range list.Items {
		got = append(got, object.ID)
	}
	sort.Strings(got)
	want = append([]string(nil), want...)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("List() IDs = %v, want %v", got, want)
	}
}

func assertEventually(t *testing.T, timeout time.Duration, check func() error) {
	t.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	var lastErr error
	for {
		if lastErr = check(); lastErr == nil {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("condition not met within %s: %v", timeout, lastErr)
		case <-ticker.C:
		}
	}
}
