package store_test

import (
	"testing"

	"xiaoshiai.cn/store"
)

type resourceNameObject struct {
	store.ObjectMeta
}

func (*resourceNameObject) ResourceName() string { return "custom-resources" }

func TestGetResourceUsesListItemResourceName(t *testing.T) {
	resource, err := store.GetResource(&store.List[resourceNameObject]{})
	if err != nil {
		t.Fatal(err)
	}
	if resource != "custom-resources" {
		t.Fatalf("resource = %q, want %q", resource, "custom-resources")
	}
}
