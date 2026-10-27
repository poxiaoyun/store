package mongo

import (
	"context"
	"errors"
	"testing"

	commonerrors "xiaoshiai.cn/common/errors"
	testmongodb "xiaoshiai.cn/common/testkit/mongodb"
	"xiaoshiai.cn/store"
)

func TestTransaction(t *testing.T) {
	database := RequireIntegrationDatabase(t, testmongodb.RequireURI(t))
	storage := NewStatusIntegrationStorage(t, database, &TestObject{})
	called := false
	err := storage.Transaction(t.Context(), func(context.Context, store.Store) error { called = true; return nil }, store.WithIsolationLevel(store.IsolationSerializable))
	if !commonerrors.IsUnsupported(err) || called {
		t.Fatalf("unsupported transaction executed: called=%v error=%v", called, err)
	}
	injected := errors.New("abort transaction")
	value := &TestObject{Status: TestObjectStatus{Val: "original"}}
	if err := storage.Create(t.Context(), value); err != nil {
		t.Fatal(err)
	}
	err = storage.Transaction(t.Context(), func(ctx context.Context, tx store.Store) error {
		var current TestObject
		if err := tx.Get(ctx, value.ID, &current); err != nil {
			return err
		}
		current.Status.Val = "rolled back"
		if err := tx.Status().
			Update(ctx, &current); err != nil {
			return err
		}
		return injected
	}, store.WithIsolationLevel(store.IsolationRepeatableRead))
	if !errors.Is(err, injected) {
		t.Fatal(err)
	}
	var current TestObject
	if err := storage.Get(t.Context(), value.ID, &current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Val != "original" {
		t.Fatalf("rollback failed: %+v", current)
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := storage.Get(canceled, value.ID, &current); !errors.Is(err, context.Canceled) {
		t.Fatalf("Get canceled: %v", err)
	}
	if err := storage.List(canceled, &store.List[TestObject]{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("List canceled: %v", err)
	}

}
