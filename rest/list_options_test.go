package rest_test

import (
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"

	commonerrors "xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/meta"
	"xiaoshiai.cn/store"
	storerest "xiaoshiai.cn/store/rest"
	"xiaoshiai.cn/store/selector"
)

func TestListOptionsFromRequestRejectsMalformedRequirements(t *testing.T) {
	request := httptest.NewRequest("GET", "/objects?fieldSelector=%28owner%3Dalice", nil)
	_, err := storerest.ListOptionsFromRequest(request)
	if commonerrors.ReasonForError(err) != commonerrors.StatusReasonBadRequest {
		t.Fatalf("ListOptionsFromRequest() error = %v, want BadRequest", err)
	}
}

func TestListOptionsFromRequestParsesRecursiveRequirements(t *testing.T) {
	requirement := selector.Requirement{
		Operator: selector.Or,
		Requirements: store.Requirements{
			selector.RequirementEqual("visibility", "public"),
			selector.RequirementEqual("owner", "alice"),
		},
	}
	expression := store.Requirements{
		selector.RequirementEqual("enabled", "true"),
		requirement,
	}.String()
	request := httptest.NewRequest(
		"GET",
		"/objects?fieldSelector="+url.QueryEscape(expression),
		nil,
	)
	modifiers, err := storerest.ListOptionsFromRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	options := store.ApplyListOptions(modifiers)
	want := store.Requirements{selector.RequirementEqual("enabled", "true"), requirement}
	if !reflect.DeepEqual(options.FieldRequirements, want) {
		t.Fatalf("FieldRequirements = %#v, want %#v", options.FieldRequirements, want)
	}
}

func TestListOptionsFromRequestAppliesRequestDefaults(t *testing.T) {
	request := httptest.NewRequest("GET", "/objects?includeSubscopes=true", nil)
	modifiers, err := storerest.ListOptionsFromRequest(
		request,
		meta.DefaultPage(1, 20),
		meta.DefaultSort("creationTimestamp-"),
	)
	if err != nil {
		t.Fatal(err)
	}
	options := store.ApplyListOptions(modifiers)
	if options.Offset != 0 || options.Limit != 20 || options.Sort != "creationTimestamp-" {
		t.Fatalf("options = %#v", options)
	}
	if options.IncludeSubScopes {
		t.Fatal("IncludeSubScopes was enabled by a public request")
	}
}

func TestListOptionsFromRequestPreservesExplicitValues(t *testing.T) {
	request := httptest.NewRequest("GET", "/objects?page=2&size=50&sort=name%2B", nil)
	modifiers, err := storerest.ListOptionsFromRequest(
		request,
		meta.DefaultPage(1, 20),
		meta.DefaultSort("creationTimestamp-"),
	)
	if err != nil {
		t.Fatal(err)
	}
	options := store.ApplyListOptions(modifiers)
	if options.Offset != 50 || options.Limit != 50 || options.Sort != "name+" {
		t.Fatalf("options = %#v", options)
	}
}
