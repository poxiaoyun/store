package etcd

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"k8s.io/apiserver/pkg/storage/etcd3/testserver"
	"k8s.io/utils/ptr"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/selector"
	"xiaoshiai.cn/store/storetest"
)

func TestStoreConformance(t *testing.T) {
	client := testserver.RunEtcd(t, nil)
	capabilities := store.Capabilities{Finalizers: true, OptimisticLock: true, Watch: true}
	storetest.Run(t, storetest.Fixture{
		Capabilities:  capabilities,
		LabelSelector: true,
		FieldSelector: true,
		Continue:      true,
		SubScopes:     true,
		TTL:           true,
		Sort:          true,

		New: func(t testing.TB, schema *store.Schema) (store.Store, error) {
			return NewEtcdStoreFromClient(client, schema, "/store-conformance/"+uuid.NewString())
		},
	})
}

func SetupEtcdTestEtcdStore(t *testing.T) store.Store {
	client := testserver.RunEtcd(t, nil)
	schema := store.NewSchema()
	if err := schema.Register(&TestObject{}, store.ResourceSchema{}); err != nil {
		t.Fatal(err)
	}
	etcdStore := &EtcdStore{core: newEtcdStoreCore(client, schema, "/test")}
	return etcdStore
}

type TestObject struct {
	store.ObjectMeta `json:",inline"`
	Spec             TestObjectSpec   `json:"spec"`
	Status           TestObjectStatus `json:"status"`
}

type TestObjectSpec struct {
	Replicas *int32 `json:"replicas,omitempty"`
}

type TestObjectStatus struct {
	Phase   string `json:"phase,omitempty"`
	Current int32  `json:"current,omitempty"`
}

func TestListContinue(t *testing.T) {
	ctx := context.Background()
	etcdStore := SetupEtcdTestEtcdStore(t)

	const objectCount = 7
	for i := range objectCount {
		obj := &TestObject{
			ObjectMeta: store.ObjectMeta{
				ID:   fmt.Sprintf("continue-%02d", i),
				Name: fmt.Sprintf("continue-%02d", i),
			},
		}
		if err := etcdStore.Create(ctx, obj); err != nil {
			t.Fatalf("failed to create object %d: %v", i, err)
		}
	}

	got := make(map[string]struct{}, objectCount)
	continueToken := ""
	for page := 0; ; page++ {
		list := &store.List[TestObject]{}
		if err := etcdStore.List(ctx, list,
			store.WithContinue(continueToken, 3),
		); err != nil {
			t.Fatalf("failed to list page %d: %v", page, err)
		}
		if len(list.Items) == 0 {
			t.Fatalf("page %d is unexpectedly empty", page)
		}
		if len(list.Items) > 3 {
			t.Fatalf("page %d returned %d items, want at most 3", page, len(list.Items))
		}

		for _, item := range list.Items {
			if _, exists := got[item.ID]; exists {
				t.Fatalf("item %q was returned more than once", item.ID)
			}
			got[item.ID] = struct{}{}
		}
		if list.Continue == "" {
			break
		}

		if list.Continue == continueToken {
			t.Fatalf("page %d returned the same continue token", page)
		}
		continueToken = list.Continue
	}
	if len(got) != objectCount {
		t.Fatalf("pagination returned %d unique items, want %d", len(got), objectCount)
	}
	for i := range objectCount {
		id := fmt.Sprintf("continue-%02d", i)
		if _, exists := got[id]; !exists {
			t.Errorf("pagination omitted item %q", id)
		}
	}
}

func TestListPagePagination(t *testing.T) {
	ctx := context.Background()
	etcdStore := SetupEtcdTestEtcdStore(t)

	for i := range 5 {
		obj := &TestObject{
			ObjectMeta: store.ObjectMeta{
				ID:   fmt.Sprintf("legacy-%02d", i),
				Name: fmt.Sprintf("legacy-%02d", i),
			},
		}
		if err := etcdStore.Create(ctx, obj); err != nil {
			t.Fatalf("failed to create object %d: %v", i, err)
		}
	}

	first := &store.List[TestObject]{}
	if err := etcdStore.List(ctx, first, store.WithOffset(0), store.WithLimit(2)); err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 2 || first.Continue == "" {
		t.Fatalf("first page = %#v", first)
	}

	second := &store.List[TestObject]{}
	if err := etcdStore.List(ctx, second, store.WithOffset(2), store.WithLimit(2)); err != nil {
		t.Fatal(err)
	}
	if len(second.Items) != 2 ||
		second.Items[0].ID != "legacy-02" ||
		second.Items[1].ID != "legacy-03" ||
		second.Continue == "" {
		t.Fatalf("second page = %#v", second)
	}
}

func TestListWithoutSizeReturnsAllItems(t *testing.T) {
	ctx := context.Background()
	etcdStore := SetupEtcdTestEtcdStore(t)

	const objectCount = 5
	for i := range objectCount {
		obj := &TestObject{
			ObjectMeta: store.ObjectMeta{
				ID:   fmt.Sprintf("all-%02d", i),
				Name: fmt.Sprintf("all-%02d", i),
			},
		}
		if err := etcdStore.Create(ctx, obj); err != nil {
			t.Fatalf("failed to create object %d: %v", i, err)
		}
	}

	list := &store.List[TestObject]{}
	if err := etcdStore.List(ctx, list); err != nil {
		t.Fatalf("failed to list all objects: %v", err)
	}
	if len(list.Items) != objectCount {
		t.Fatalf("list returned %d items, want %d", len(list.Items), objectCount)
	}
	if list.Continue != "" {
		t.Fatalf("unpaginated list returned continue token %q", list.Continue)
	}

}

func TestCacheStore_Get(t *testing.T) {
	ctx := context.Background()
	etcdStore := SetupEtcdTestEtcdStore(t)

	obj := &TestObject{
		ObjectMeta: store.ObjectMeta{
			ID:       "test",
			Name:     "test",
			Resource: "test",
		},
		Spec: TestObjectSpec{
			Replicas: ptr.To(int32(1)),
		},
		Status: TestObjectStatus{
			Phase: "Running",
		},
	}
	scopes := []store.Scope{{Resource: "namespace", Name: "default"}}
	namespaceedStore := etcdStore.Scope(scopes...)

	// create
	if err := namespaceedStore.Create(ctx, obj); err != nil {
		t.Fatalf("failed to create object: %v", err)
	}
	exists := &TestObject{ObjectMeta: store.ObjectMeta{ID: "test", Resource: "test"}}
	if err := namespaceedStore.Get(ctx, "test", exists); err != nil {
		t.Fatalf("failed to get object: %v", err)
	}
	if !reflect.DeepEqual(obj, exists) {
		t.Fatalf("expected %v, got %v", obj, exists)
	}

	// update
	exists.Spec.Replicas = ptr.To(int32(2))
	if err := namespaceedStore.Update(ctx, exists); err != nil {
		t.Fatalf("failed to update object: %v", err)
	}
	if err := namespaceedStore.Get(ctx, "test", exists); err != nil {
		t.Fatalf("failed to get object: %v", err)
	}
	if *exists.Spec.Replicas != 2 {
		t.Fatalf("expected 2, got %v", *exists.Spec.Replicas)
	}

	// patch
	patch := store.RawPatch(store.PatchTypeJSONPatch, []byte(`[{"op": "replace", "path": "/spec/replicas", "value": 3}]`))
	if err := namespaceedStore.Patch(ctx, exists, patch); err != nil {
		t.Fatalf("failed to patch object: %v", err)
	}
	if err := namespaceedStore.Get(ctx, "test", exists); err != nil {
		t.Fatalf("failed to get object: %v", err)
	}
	if *exists.Spec.Replicas != 3 {
		t.Fatalf("expected 3, got %v", *exists.Spec.Replicas)
	}

	// delete
	if err := namespaceedStore.Delete(ctx, exists, store.WithPropagation(store.DeletePropagationForeground)); err != nil {
		t.Fatalf("failed to delete object: %v", err)
	}
	if err := namespaceedStore.Get(ctx, "test", exists); err != nil {
		t.Fatalf("failed to get object: %v", err)
	}
	if exists.DeletionTimestamp == nil {
		t.Fatalf("expected deletion timestamp, got nil")
	}
	if !store.ContainsFinalizer(exists, store.FinalizerDeleteDependents) {
		t.Fatalf("expected finalizer, got none")
	}

	// Complete foreground deletion after the garbage collector removes its finalizer.
	store.RemoveFinalizer(exists, store.FinalizerDeleteDependents)
	if err := namespaceedStore.Update(ctx, exists); err != nil {
		t.Fatalf("failed to remove deletion finalizer: %v", err)
	}
	if err := namespaceedStore.Get(ctx, "test", exists); err != nil {
		if !errors.IsNotFound(err) {
			t.Fatalf("expected not found, got %v", err)
		}
	} else {
		t.Fatalf("expected not found, got %v", exists)
	}
}

type indexedObject struct {
	store.ObjectMeta `json:",inline"`
	Email            string `json:"email,omitempty"`
}

func TestListAppliesUnindexedFieldRequirements(t *testing.T) {
	client := testserver.RunEtcd(t, nil)
	schema := store.NewSchema()
	if err := schema.Register(&indexedObject{}, store.ResourceSchema{}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	storage, err := NewEtcdStoreFromClient(client, schema, "/test")
	if err != nil {
		t.Fatalf("NewEtcdStoreFromClient() error = %v", err)
	}
	ctx := context.Background()
	for _, object := range []*indexedObject{
		{ObjectMeta: store.ObjectMeta{ID: "one"}, Email: "one@example.com"},
		{ObjectMeta: store.ObjectMeta{ID: "two"}, Email: "two@example.com"},
	} {
		if err := storage.Create(ctx, object); err != nil {
			t.Fatalf("Create(%s) error = %v", object.ID, err)
		}
	}
	list := &store.List[indexedObject]{}
	if err := storage.List(ctx, list, store.WithFieldRequirements(selector.Requirement{
		Key: "email", Operator: selector.Equals, Values: []any{"two@example.com"},
	})); err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(list.Items) != 1 || list.Items[0].ID != "two" {
		t.Fatalf("List() items = %#v, want only two", list.Items)
	}
}

func TestValidateSchemaRejectsSecondaryIndex(t *testing.T) {
	schema := store.NewSchema()
	if err := schema.Register(&indexedObject{}, store.ResourceSchema{
		Indexes: []store.Index{{Fields: []string{"email"}}},
	}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := validateSchema(schema); err == nil {
		t.Fatal("validateSchema() error = nil")
	}
}
