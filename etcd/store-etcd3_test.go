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
	"xiaoshiai.cn/common/store"
	"xiaoshiai.cn/common/store/storetest"
)

func TestStoreConformance(t *testing.T) {
	client := testserver.RunEtcd(t, nil)
	capabilities := (&EtcdStore{}).Capabilities()
	storetest.Run(t, storetest.Fixture{
		Capabilities: capabilities,
		New: func(t testing.TB, schema *store.Schema) (store.Store, error) {
			return NewEtcdStoreFromClient(client, schema, "/store-conformance/"+uuid.NewString())
		},
	})
}

func SetupEtcdTestEtcdStore(t *testing.T) store.Store {
	client := testserver.RunEtcd(t, nil)
	etcdStore := &EtcdStore{core: newEtcdStoreCore(client, store.NewSchema(), "/test")}
	return etcdStore
}

type TestObject struct {
	store.ObjectMeta `json:",inline"`
	Spec             TestObjectSpec   `json:"spec,omitempty"`
	Status           TestObjectStatus `json:"status,omitempty"`
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
			store.WithPage(99, 1),
			store.WithContinuation(continueToken, 3),
		); err != nil {
			t.Fatalf("failed to list page %d: %v", page, err)
		}
		if len(list.Items) == 0 {
			t.Fatalf("page %d is unexpectedly empty", page)
		}
		if len(list.Items) > 3 {
			t.Fatalf("page %d returned %d items, want at most 3", page, len(list.Items))
		}
		if list.Total != nil {
			t.Fatalf("page %d total is %v, want omitted for continuation pagination", page, list.Total)
		}
		if list.Page != 0 || list.Size != 0 || list.Limit != 3 {
			t.Fatalf("page %d pagination metadata = page %d, size %d, limit %d", page, list.Page, list.Size, list.Limit)
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
		if page == 0 {
			obj := &TestObject{
				ObjectMeta: store.ObjectMeta{
					ID:   "continue-025",
					Name: "continue-025",
				},
			}
			if err := etcdStore.Create(ctx, obj); err != nil {
				t.Fatalf("failed to create object between pages: %v", err)
			}
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

func TestListContinuationRejectsSort(t *testing.T) {
	etcdStore := SetupEtcdTestEtcdStore(t)
	list := &store.List[TestObject]{}
	err := etcdStore.List(t.Context(), list, store.WithContinuation("", 1), store.WithSort("id+"))
	if !errors.IsUnsupported(err) {
		t.Fatalf("List() error = %v, want Unsupported", err)
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
	if err := etcdStore.List(ctx, first, store.WithPage(0, 2)); err != nil {
		t.Fatal(err)
	}
	if len(first.Items) != 2 || first.Total == nil || *first.Total != 5 ||
		first.Page != 1 || first.Size != 2 || first.Continue != "" || first.Limit != 0 {
		t.Fatalf("first page = %#v", first)
	}

	second := &store.List[TestObject]{}
	if err := etcdStore.List(ctx, second, store.WithPage(2, 2)); err != nil {
		t.Fatal(err)
	}
	if len(second.Items) != 2 ||
		second.Items[0].ID != "legacy-02" ||
		second.Items[1].ID != "legacy-03" ||
		second.Total == nil || *second.Total != 5 ||
		second.Page != 2 || second.Size != 2 ||
		second.Continue != "" || second.Limit != 0 {
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
	if list.Page != 0 || list.Size != 0 || list.Limit != 0 {
		t.Fatalf("unpaginated list returned pagination metadata: page %d, size %d, limit %d", list.Page, list.Size, list.Limit)
	}
	if list.Total == nil || *list.Total != objectCount {
		t.Fatalf("unpaginated list total is %v, want %d", list.Total, objectCount)
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
