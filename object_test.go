package store_test

import (
	"reflect"
	"testing"

	"xiaoshiai.cn/common/meta"
	"xiaoshiai.cn/store"
)

func TestListConversionsPreserveReadResult(t *testing.T) {
	list := store.List[int]{
		ResourceVersion: 11,
		Items:           []int{1, 2},
	}

	converted := store.ConvertList(list, func(value int) string { return string(rune('a' + value - 1)) })
	if converted.ResourceVersion != 11 {
		t.Fatalf("ConvertList() metadata = %#v", converted)
	}
	if !reflect.DeepEqual(converted.Items, []string{"a", "b"}) {
		t.Fatalf("ConvertList() items = %#v", converted.Items)
	}

	page := store.PageFromList(list, meta.ListOptions{Page: 2, Size: 20})
	if page.ResourceVersion != 11 || page.Page != 2 || page.Size != 20 || page.Continue != "" || page.Limit != 0 {
		t.Fatalf("PageFromList() metadata = %#v", page)
	}
	if !reflect.DeepEqual(page.Items, []int{1, 2}) {
		t.Fatalf("PageFromList() items = %#v", page.Items)
	}

	cursor := store.List[int]{Items: []int{3}, Continue: "next"}
	convertedCursor := store.ConvertList(cursor, func(value int) string { return "c" })
	if convertedCursor.Continue != "next" {
		t.Fatalf("ConvertList() cursor metadata = %#v", convertedCursor)
	}
	cursorPage := store.PageFromList(cursor, meta.ListOptions{Continue: "next", Limit: 25})
	if cursorPage.Total != nil || cursorPage.Continue != "next" || cursorPage.Limit != 25 ||
		cursorPage.Page != 0 || cursorPage.Size != 0 {
		t.Fatalf("PageFromList() cursor metadata = %#v", cursorPage)
	}
}
