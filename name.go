package store

import (
	"fmt"
	"reflect"
	"strings"
)

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
	if before, ok := strings.CutSuffix(name, "y"); ok {
		return before + "ies"
	}
	return strings.ToLower(name) + "s"
}
