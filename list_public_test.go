package store_test

import (
	"encoding/json"
	"reflect"
	"testing"

	"xiaoshiai.cn/common/meta"
	"xiaoshiai.cn/common/selector"
	"xiaoshiai.cn/store"
)

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
	if options.Offset != 25 || options.Limit != 25 || options.Continue != nil {
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
	if options.Continue == nil || *options.Continue != "next" || options.Limit != 10 || options.Offset != 0 {
		t.Fatalf("cursor = %#v", options)
	}
}

func TestListOptionsFromMetaPreservesCursorPagination(t *testing.T) {
	modifiers, err := store.ListOptionsFromMeta(meta.ListOptions{Continue: "next", Limit: 25})
	if err != nil {
		t.Fatal(err)
	}
	options := store.ApplyListOptions(modifiers)
	if options.Continue == nil || *options.Continue != "next" || options.Limit != 25 {
		t.Fatalf("cursor = %#v", options)
	}
}

func TestPaginationOptionsApplyThroughPublicSeam(t *testing.T) {
	window := store.ApplyListOptions([]store.ListOption{
		store.WithOffset(20),
		store.WithLimit(10),
	})
	if window.Offset != 20 || window.Limit != 10 || window.Continue != nil {
		t.Fatalf("window = %#v", window)
	}

	cursor := store.ApplyListOptions([]store.ListOption{store.WithContinue("next", 10)})
	if cursor.Continue == nil || *cursor.Continue != "next" || cursor.Limit != 10 {
		t.Fatalf("cursor = %#v", cursor)
	}
}

func TestListMetadataJSONCarriesOnlyReadResult(t *testing.T) {
	tests := []struct {
		name string
		set  func(*store.List[int])
		want string
	}{
		{
			name: "window",
			set: func(list *store.List[int]) {
				store.SetWindowListMetadata(list, 0)
			},
			want: `{"items":[],"total":0}`,
		},
		{
			name: "cursor",
			set: func(list *store.List[int]) {
				store.SetCursorListMetadata(list, "next")
			},
			want: `{"items":[],"continue":"next"}`,
		},
		{
			name: "last cursor batch",
			set: func(list *store.List[int]) {
				store.SetCursorListMetadata(list, "")
			},
			want: `{"items":[]}`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			list := &store.List[int]{
				Items:    []int{},
				Total:    meta.Ptr(9),
				Continue: "stale",
			}
			test.set(list)
			encoded, err := json.Marshal(list)
			if err != nil {
				t.Fatal(err)
			}
			if string(encoded) != test.want {
				t.Fatalf("json = %s, want %s", encoded, test.want)
			}
		})
	}
}

func TestListConversionsPreserveReadResult(t *testing.T) {
	total := 7
	list := store.List[int]{
		ResourceVersion: 11,
		Items:           []int{1, 2},
		Total:           &total,
	}

	converted := store.ConvertList(list, func(value int) string { return string(rune('a' + value - 1)) })
	if converted.ResourceVersion != 11 || converted.Total == nil || *converted.Total != 7 {
		t.Fatalf("ConvertList() metadata = %#v", converted)
	}
	if !reflect.DeepEqual(converted.Items, []string{"a", "b"}) {
		t.Fatalf("ConvertList() items = %#v", converted.Items)
	}

	page := store.PageFromList(list, meta.ListOptions{Page: 2, Size: 20})
	if page.ResourceVersion != 11 || page.Total == nil || *page.Total != 7 ||
		page.Page != 2 || page.Size != 20 || page.Continue != "" || page.Limit != 0 {
		t.Fatalf("PageFromList() metadata = %#v", page)
	}
	if !reflect.DeepEqual(page.Items, []int{1, 2}) {
		t.Fatalf("PageFromList() items = %#v", page.Items)
	}

	cursor := store.List[int]{Items: []int{3}, Continue: "next"}
	convertedCursor := store.ConvertList(cursor, func(value int) string { return "c" })
	if convertedCursor.Total != nil || convertedCursor.Continue != "next" {
		t.Fatalf("ConvertList() cursor metadata = %#v", convertedCursor)
	}
	cursorPage := store.PageFromList(cursor, meta.ListOptions{Continue: "next", Limit: 25})
	if cursorPage.Total != nil || cursorPage.Continue != "next" || cursorPage.Limit != 25 ||
		cursorPage.Page != 0 || cursorPage.Size != 0 {
		t.Fatalf("PageFromList() cursor metadata = %#v", cursorPage)
	}
}
