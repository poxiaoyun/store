package etcd

import (
	"context"
	"testing"
	"time"

	"k8s.io/apiserver/pkg/storage/etcd3/testserver"
	"xiaoshiai.cn/common/selector"
	"xiaoshiai.cn/common/store"
)

type indexedObject struct {
	store.ObjectMeta `json:",inline"`
	Email            string `json:"email,omitempty"`
}

func TestWatchAppliesUnindexedFieldRequirements(t *testing.T) {
	client := testserver.RunEtcd(t, nil)
	schema := store.NewSchema()
	if err := schema.Register(&indexedObject{}, store.ResourceSchema{}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	storage, err := NewEtcdStoreFromClient(client, schema, "/test")
	if err != nil {
		t.Fatalf("NewEtcdStoreFromClient() error = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, object := range []*indexedObject{
		{ObjectMeta: store.ObjectMeta{ID: "one"}, Email: "one@example.com"},
		{ObjectMeta: store.ObjectMeta{ID: "two"}, Email: "two@example.com"},
	} {
		if err := storage.Create(ctx, object); err != nil {
			t.Fatalf("Create(%s) error = %v", object.ID, err)
		}
	}
	watcher, err := storage.Watch(ctx, &store.List[indexedObject]{},
		store.WithSendInitialEvents(),
		store.WithFieldRequirements(selector.Requirement{
			Key: "email", Operator: selector.Equals, Values: []any{"two@example.com"},
		}),
	)
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	defer watcher.Stop()
	for {
		select {
		case event, ok := <-watcher.Events():
			if !ok {
				t.Fatal("watch events closed before matching event")
			}
			if event.Error != nil {
				t.Fatalf("watch event error = %v", event.Error)
			}
			if event.Object == nil {
				continue
			}
			if event.Object.GetID() != "two" {
				t.Fatalf("watch event ID = %q, want two", event.Object.GetID())
			}
			return
		case <-ctx.Done():
			t.Fatal("timed out waiting for matching watch event")
		}
	}
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

func (*indexedObject) ResourceName() string { return "indexedobjects" }

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

func TestValidateSchemaAcceptsPrimaryIndex(t *testing.T) {
	schema := store.NewSchema()
	if err := schema.Register(&indexedObject{}, store.ResourceSchema{}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := validateSchema(schema); err != nil {
		t.Fatalf("validateSchema() error = %v", err)
	}
}
