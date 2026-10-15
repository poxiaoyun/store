package etcdcache

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"xiaoshiai.cn/common/store"
)

const ScopeIndex = "scope"

func ScopeIndexFunc(obj any) ([]string, error) {
	uns, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("object is not an unstructured.Unstructured")
	}
	scopedlist, ok, err := unstructured.NestedSlice(uns.Object, "scopes")
	if err != nil {
		return nil, fmt.Errorf("error getting scopes: %v", err)
	}
	if !ok {
		return nil, nil
	}
	scopekey := ""
	for _, scope := range scopedlist {
		if scope == nil {
			continue
		}
		mapval, ok := scope.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("scope is not a map")
		}
		resource, ok, _ := unstructured.NestedString(mapval, "resource")
		if !ok {
			return nil, fmt.Errorf("scope has no resource")
		}
		name, ok, _ := unstructured.NestedString(mapval, "name")
		if !ok {
			return nil, fmt.Errorf("scope has no name")
		}
		scopekey += "/" + resource + "/" + name
	}
	return []string{scopekey}, nil
}

const NameIndex = "name"

func NameFunc(obj any) ([]string, error) {
	uns, ok := obj.(*unstructured.Unstructured)
	if !ok {
		return nil, fmt.Errorf("object is not an unstructured.Unstructured")
	}
	name, ok, err := unstructured.NestedString(uns.Object, "name")
	if err != nil {
		return nil, fmt.Errorf("error getting name: %v", err)
	}
	if !ok {
		return nil, nil
	}
	return []string{name}, nil
}

func ParseScopes(uns *StorageObject) ([]store.Scope, error) {
	scopedlist, ok, _ := unstructured.NestedSlice(uns.Object, "scopes")
	if !ok {
		return nil, nil
	}
	scopes := make([]store.Scope, 0, len(scopedlist))
	for _, scope := range scopedlist {
		if scope == nil {
			return nil, fmt.Errorf("one of the scopes is nil")
		}
		mapval, ok := scope.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("scope is not a map")
		}
		resource, ok, _ := unstructured.NestedString(mapval, "resource")
		if !ok {
			return nil, fmt.Errorf("scope has no resource")
		}
		name, ok, _ := unstructured.NestedString(mapval, "name")
		if !ok {
			return nil, fmt.Errorf("scope has no name")
		}
		scopes = append(scopes, store.Scope{Resource: resource, Name: name})
	}
	return scopes, nil
}
