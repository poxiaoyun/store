package mongo

import (
	"reflect"
	"strings"

	libreflect "xiaoshiai.cn/common/reflect"
	"xiaoshiai.cn/store"
)

func ObjectFields(o store.Object) ([]string, error) {
	t := reflect.TypeOf(o)
	fields := []string{}
	err := flattenTypeFields("", t, 1, func(name string) error {
		fields = append(fields, strings.TrimPrefix(name, "."))
		return nil
	})
	return fields, err
}

func flattenTypeFields(name string, t reflect.Type, maxDepth int, fieldFunc func(string) error) error {
	if maxDepth <= 0 {
		return fieldFunc(name)
	}
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	if t.Kind() != reflect.Struct {
		return fieldFunc(name)
	}
	for i := 0; i < t.NumField(); i++ {
		structField := t.Field(i)
		isEmbedded, isIgnore, fieldName := libreflect.StructFieldInfo(structField)
		if isIgnore {
			continue
		}
		if isEmbedded {
			if err := flattenTypeFields(name, structField.Type, maxDepth, fieldFunc); err != nil {
				return err
			}
			continue
		}
		if err := flattenTypeFields(name+"."+fieldName, structField.Type, maxDepth-1, fieldFunc); err != nil {
			return err
		}
	}
	return nil
}
