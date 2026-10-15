package etcdcache

import (
	stdjson "encoding/json"
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	kjson "sigs.k8s.io/json"
	"xiaoshiai.cn/common/meta"
	"xiaoshiai.cn/common/store"
)

func IndexerFromFields(fields []string) cache.Indexers {
	indexers := cache.Indexers{}
	for _, field := range fields {
		indexers[field] = func(obj any) ([]string, error) {
			uns, ok := obj.(*StorageObject)
			if !ok {
				return nil, fmt.Errorf("object is not an StorageObject")
			}
			val, ok := getFieldIndex(uns, strings.Split(field, ".")...)
			if !ok {
				// it allow field selector select on empty and not exists field
				return []string{""}, nil
			}
			return []string{val}, nil
		}
	}
	return indexers
}

func GetAttrsFunc(indexfields []string) func(obj runtime.Object) (labels.Set, fields.Set, error) {
	return func(obj runtime.Object) (labels.Set, fields.Set, error) {
		uns, ok := obj.(*StorageObject)
		if !ok {
			return nil, nil, fmt.Errorf("unexpected object type: %T", obj)
		}
		sFields := fields.Set{
			"id":   GetNestedString(uns.Object, "id"),
			"name": GetNestedString(uns.Object, "name"),
		}
		for _, fname := range indexfields {
			val, ok := getFieldIndex(uns, strings.Split(fname, ".")...)
			if !ok {
				continue
			}
			sFields[fname] = val
		}
		return uns.GetLabels(), sFields, nil
	}
}

func getFieldIndex(uns *StorageObject, fields ...string) (string, bool) {
	val, ok := NestedFieldNoCopy(uns.Object, fields...)
	if !ok {
		return "", false
	}
	switch v := val.(type) {
	case string:
		return v, true
	case bool:
		return strconv.FormatBool(v), true
	case int64:
		return strconv.FormatInt(v, 10), true
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64), true
	default:
		return fmt.Sprintf("%v", v), true
	}
}

func FlatterMap(m map[string]any) map[string]string {
	ret := make(map[string]string)
	for k, v := range m {
		switch val := v.(type) {
		case map[string]any:
			for kk, vv := range FlatterMap(val) {
				ret[k+"."+kk] = vv
			}
		default:
			ret[k] = store.AnyToString(v)
		}
	}
	return ret
}

func SortUnstructuredList(list []StorageObject, bys []meta.SortField) {
	slices.SortFunc(list, func(a, b StorageObject) int {
		for _, by := range bys {
			switch by.Field {
			case "time":
				by.Field = "creationTimestamp"
			}
			av, _ := NestedFieldNoCopy(a.Object, strings.Split(by.Field, ".")...)
			bv, _ := NestedFieldNoCopy(b.Object, strings.Split(by.Field, ".")...)
			switch by.Direction {
			case meta.SortDirectionAsc:
				if ret := compareSortValues(av, bv); ret != 0 {
					return ret
				}
			case meta.SortDirectionDesc:
				if ret := compareSortValues(bv, av); ret != 0 {
					return ret
				}
			}
		}
		return 0
	})
}

// JSON numbers must retain their type when sorted; index strings are only for
// selector lookup. Missing and null fields sort before populated fields.
func compareSortValues(a, b any) int {
	if a == nil {
		if b == nil {
			return 0
		}
		return -1
	}
	if b == nil {
		return 1
	}
	switch av := a.(type) {
	case int64:
		switch bv := b.(type) {
		case int64:
			return store.CompareField(av, bv)
		case float64:
			return big.NewRat(av, 1).Cmp(new(big.Rat).SetFloat64(bv))
		}
	case float64:
		switch bv := b.(type) {
		case float64:
			return store.CompareField(av, bv)
		case int64:
			return new(big.Rat).SetFloat64(av).Cmp(big.NewRat(bv, 1))
		}
	}
	return strings.Compare(store.AnyToString(a), store.AnyToString(b))
}

func searchObject(uns *StorageObject, fields []string, val string) bool {
	if len(fields) == 0 {
		return true
	}
	val = strings.ToLower(val)
	for _, field := range fields {
		strval, ok := getFieldIndex(uns, strings.Split(field, ".")...)
		if ok && strings.Contains(strings.ToLower(strval), val) {
			return true
		}
	}
	return false
}

type APIObjectVersioner struct{}

// ObjectResourceVersion implements storage.Versioner.
func (a APIObjectVersioner) ObjectResourceVersion(obj runtime.Object) (uint64, error) {
	if cacheable, ok := obj.(runtime.CacheableObject); ok {
		obj = cacheable.GetObject()
	}
	object, ok := obj.(*StorageObject)
	if !ok {
		return 0, fmt.Errorf("object is not a StorageObject")
	}
	return uint64(object.GetRevision()), nil
}

// ParseResourceVersion implements storage.Versioner.
func (a APIObjectVersioner) ParseResourceVersion(resourceVersion string) (uint64, error) {
	if resourceVersion == "" || resourceVersion == "0" {
		return 0, nil
	}
	return strconv.ParseUint(resourceVersion, 10, 64)
}

// PrepareObjectForStorage implements storage.Versioner.
func (a APIObjectVersioner) PrepareObjectForStorage(obj runtime.Object) error {
	object, ok := obj.(*StorageObject)
	if !ok {
		return fmt.Errorf("object is not a StorageObject")
	}
	RemoveNestedField(object.Object, "resourceVersion")
	return nil
}

// UpdateObject implements Versioner
func (a APIObjectVersioner) UpdateObject(obj runtime.Object, resourceVersion uint64) error {
	object, ok := obj.(*StorageObject)
	if !ok {
		return fmt.Errorf("object is not a StorageObject")
	}
	object.SetRevision(int64(resourceVersion))
	return nil
}

// UpdateList implements Versioner
func (a APIObjectVersioner) UpdateList(obj runtime.Object, resourceVersion uint64, nextKey string, count *int64) error {
	object, ok := obj.(*StorageObjectList)
	if !ok {
		return fmt.Errorf("object is not a StorageObjectList")
	}
	object.setNestedField(float64(resourceVersion), "resourceVersion")
	if nextKey != "" {
		SetNestedFieldNoCopy(object.Object, nextKey, "continue")
	}
	if count != nil {
		SetNestedFieldNoCopy(object.Object, count, "remainingItemCount")
	}
	return nil
}

func SetNestedStringMap(obj map[string]any, value map[string]string, fields ...string) {
	m := make(map[string]any, len(value)) // convert map[string]string into map[string]any
	for k, v := range value {
		m[k] = v
	}
	SetNestedFieldNoCopy(obj, m, fields...)
}

func SetNestedFieldNoCopy(obj map[string]any, value any, fields ...string) {
	m := obj
	for _, field := range fields[:len(fields)-1] {
		if val, ok := m[field]; ok {
			if valMap, ok := val.(map[string]any); ok {
				m = valMap
			} else {
				return
			}
		} else {
			newVal := make(map[string]any)
			m[field] = newVal
			m = newVal
		}
	}
	m[fields[len(fields)-1]] = value
}

func GetNestedString(obj map[string]any, fields ...string) string {
	val, _ := NestedFieldString(obj, fields...)
	return val
}

func NestedFieldString(obj map[string]any, fields ...string) (string, bool) {
	val, ok := NestedFieldNoCopy(obj, fields...)
	if !ok {
		return "", false
	}
	s, _ := val.(string)
	return s, true
}

func NestedFieldInt64(obj map[string]any, fields ...string) (int64, bool) {
	val, found := NestedFieldNoCopy(obj, fields...)
	if !found {
		return 0, false
	}
	i, ok := val.(int64)
	if !ok {
		// fallback to float64
		f, ok := val.(float64)
		if !ok {
			return 0, false
		}
		return int64(f), true
	}
	return i, true
}

func NestedFieldStringMap(obj map[string]any, fields ...string) (map[string]string, bool) {
	m, found := NestedFieldMap(obj, fields...)
	if !found {
		return nil, false
	}
	strMap := make(map[string]string, len(m))
	for k, v := range m {
		if str, ok := v.(string); ok {
			strMap[k] = str
		} else {
			return nil, false
		}
	}
	return strMap, true
}

func NestedFieldMap(obj map[string]any, fields ...string) (map[string]any, bool) {
	val, found := NestedFieldNoCopy(obj, fields...)
	if !found {
		return nil, found
	}
	m, ok := val.(map[string]any)
	if !ok {
		return nil, false
	}
	return m, true
}

func NestedFieldStringSlice(obj map[string]any, fields ...string) ([]string, bool) {
	val, found := NestedFieldNoCopy(obj, fields...)
	if !found {
		return nil, found
	}
	m, ok := val.([]any)
	if !ok {
		return nil, false
	}
	strSlice := make([]string, 0, len(m))
	for _, v := range m {
		if str, ok := v.(string); ok {
			strSlice = append(strSlice, str)
		} else {
			return nil, false
		}
	}
	return strSlice, true
}

func NestedFieldBool(obj map[string]any, fields ...string) (bool, bool) {
	val, found := NestedFieldNoCopy(obj, fields...)
	if !found {
		return false, false
	}
	b, ok := val.(bool)
	if !ok {
		return false, false
	}
	return b, true
}

func NestedFieldNoCopy(obj map[string]any, fields ...string) (any, bool) {
	var val any = obj
	for _, field := range fields {
		if val == nil {
			return nil, false
		}
		if m, ok := val.(map[string]any); ok {
			val, ok = m[field]
			if !ok {
				return nil, false
			}
		} else {
			return nil, false
		}
	}
	return val, true
}

func RemoveNestedField(obj map[string]any, fields ...string) {
	m := obj
	for _, field := range fields[:len(fields)-1] {
		if x, ok := m[field].(map[string]any); ok {
			m = x
		} else {
			return
		}
	}
	delete(m, fields[len(fields)-1])
}

// JsonUnmarshal decodes JSON data into the provided value.
// it parse float64 values as int64 if they are whole numbers
// It make [NestedFieldInt64] works with map[string]any
// the std json.Unmarshal will parse number as float64 even if they are whole numbers(int64)
func JsonUnmarshal(data []byte, v any) error {
	return kjson.UnmarshalCaseSensitivePreserveInts(data, v)
}

func JsonMarshal(v any) ([]byte, error) {
	return stdjson.Marshal(v)
}
