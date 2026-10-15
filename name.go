package store

import (
	"fmt"
	"reflect"
	"strings"
)

type ResourceName interface {
	ResourceName() string
}

func GetResource(obj any) (string, error) {
	// limit only can use resouce field from [Unstructured]'s GetResource() method
	// otherwise, it will cause an injection attack
	t := reflect.TypeOf(obj)
	switch val := obj.(type) {
	case ResourceName:
		return val.ResourceName(), nil
	case *Unstructured:
		if resource := val.GetResource(); resource != "" {
			return resource, nil
		}
		return "", fmt.Errorf("cannot get resource name from Unstructured object")
	case ObjectList:
		if resource := val.GetResource(); resource != "" {
			return resource, nil
		}
		itemsPointer, err := GetItemsPtr(val)
		if err != nil {
			return "", err
		}
		t = reflect.TypeOf(itemsPointer).Elem().Elem()
		for t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		if val, ok := reflect.New(t).Interface().(ResourceName); ok {
			return val.ResourceName(), nil
		}
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	return ReflectResourceName(t)
}

func ReflectResourceName(t reflect.Type) (string, error) {
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	typename := t.Name()
	// remove generic type suffix
	if i := strings.Index(typename, "["); i != -1 {
		typename = typename[:i]
	}
	if typename == "" {
		return "", fmt.Errorf("cannot get resource name from type %v", t)
	}
	return SimpleNameToPlural(strings.ToLower(typename)), nil
}

func SimpleNameToPlural(name string) string {
	if strings.HasSuffix(name, "s") {
		return name
	}
	if strings.HasSuffix(name, "y") {
		return strings.TrimSuffix(name, "y") + "ies"
	}
	return strings.ToLower(name) + "s"
}
