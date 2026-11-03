package inmemory

import (
	"reflect"
	"slices"
	"strings"

	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/selector"
)

// fieldValue reads model field names while retaining their native value types.
// Omitted JSON fields are absent from selectors and nullable indexes as well.
func fieldValue(object any, path []string) (any, bool) {
	if unstructured, ok := object.(*store.Unstructured); ok {
		object = unstructured.Object
	}
	value, found := lookupField(reflect.ValueOf(object), path)
	if !found {
		return nil, false
	}
	return selector.ComparisonValue(value.Interface())
}

func lookupField(value reflect.Value, path []string) (reflect.Value, bool) {
	if len(path) == 0 {
		return value, true
	}
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return reflect.Value{}, false
		}
		value = value.Elem()
	}
	if !value.IsValid() {
		return reflect.Value{}, false
	}
	switch value.Kind() {
	case reflect.Map:
		key := reflect.ValueOf(path[0])
		if !key.Type().ConvertibleTo(value.Type().Key()) {
			return reflect.Value{}, false
		}
		member := value.MapIndex(key.Convert(value.Type().Key()))
		if !member.IsValid() {
			return reflect.Value{}, false
		}
		return lookupField(member, path[1:])
	case reflect.Struct:
		var embedded []reflect.Value
		for i := range value.NumField() {
			field := value.Type().Field(i)
			if !field.IsExported() {
				continue
			}
			tags := strings.Split(field.Tag.Get("json"), ",")
			name := tags[0]
			if name == "-" {
				continue
			}
			member := value.Field(i)
			if slices.Contains(tags[1:], "omitempty") && emptyJSONValue(member) {
				continue
			}
			if slices.Contains(tags[1:], "omitzero") {
				zero := member.IsZero()
				if !zero && member.CanInterface() {
					if test, ok := member.Interface().(interface{ IsZero() bool }); ok {
						zero = test.IsZero()
					}
				}
				if zero {
					continue
				}
			}
			if field.Anonymous && name == "" {
				embedded = append(embedded, member)
				continue
			}
			if name == "" {
				name = field.Name
			}
			if name == path[0] {
				return lookupField(member, path[1:])
			}
		}
		for _, member := range embedded {
			if found, ok := lookupField(member, path); ok {
				return found, true
			}
		}
	}
	return reflect.Value{}, false
}

func emptyJSONValue(value reflect.Value) bool {
	switch value.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return value.Len() == 0
	case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Interface, reflect.Pointer:
		return value.IsZero()
	}
	return false
}
