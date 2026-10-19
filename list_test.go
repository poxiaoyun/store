package store_test

import (
	"reflect"
	"testing"

	"xiaoshiai.cn/common/meta"
	"xiaoshiai.cn/common/selector"
	"xiaoshiai.cn/store"
)

func TestListOptionsFromMeta(t *testing.T) {
	modifiers, err := store.ListOptionsFromMeta(meta.ListOptions{
		Page:          2,
		Size:          25,
		Search:        "worker",
		Sort:          "name-",
		LabelSelector: "environment=production",
		FieldSelector: "enabled=true",
	})
	if err != nil {
		t.Fatal(err)
	}
	options := store.ApplyListOptions(modifiers)
	wantLabels := store.Requirements{selector.RequirementEqual("environment", "production")}
	wantFields := store.Requirements{selector.RequirementEqual("enabled", "true")}
	if options.Offset != 25 || options.Limit != 25 || options.Search != "worker" || options.Sort != "name-" {
		t.Fatalf("scalar options = %#v", options)
	}
	if !reflect.DeepEqual(options.LabelRequirements, wantLabels) {
		t.Fatalf("LabelRequirements = %#v, want %#v", options.LabelRequirements, wantLabels)
	}
	if !reflect.DeepEqual(options.FieldRequirements, wantFields) {
		t.Fatalf("FieldRequirements = %#v, want %#v", options.FieldRequirements, wantFields)
	}
}

func TestListOptionsFromMetaRejectsInvalidSelectors(t *testing.T) {
	tests := []meta.ListOptions{
		{LabelSelector: "environment in ("},
		{FieldSelector: "(enabled=true"},
	}
	for _, options := range tests {
		if _, err := store.ListOptionsFromMeta(options); err == nil {
			t.Fatalf("ListOptionsFromMeta(%#v) returned no error", options)
		}
	}
}

func TestListOptionsFromMetaAppliesModifiers(t *testing.T) {
	tenant := selector.RequirementEqual("tenant", "tenant-1")
	modifiers, err := store.ListOptionsFromMeta(
		meta.ListOptions{
			Page:          2,
			Size:          25,
			LabelSelector: "environment=production",
		},
		store.WithLabelRequirements(tenant),
	)
	if err != nil {
		t.Fatal(err)
	}
	options := store.ApplyListOptions(modifiers)
	if options.Offset != 25 || options.Limit != 25 || options.Continue != "" {
		t.Fatalf("window = %#v", options)
	}
	wantRequirements := store.Requirements{
		selector.RequirementEqual("environment", "production"),
		tenant,
	}
	if !reflect.DeepEqual(options.LabelRequirements, wantRequirements) {
		t.Fatalf("LabelRequirements = %#v, want %#v", options.LabelRequirements, wantRequirements)
	}
}

func TestListOptionsFromMetaPrefersCursorOverPage(t *testing.T) {
	modifiers, err := store.ListOptionsFromMeta(meta.ListOptions{Page: 3, Size: 25, Continue: "next", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	options := store.ApplyListOptions(modifiers)
	if options.Continue != "next" || options.Limit != 10 || options.Offset != 0 {
		t.Fatalf("cursor = %#v", options)
	}
}
