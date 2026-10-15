package store

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"k8s.io/apimachinery/pkg/runtime"
	kubejson "sigs.k8s.io/json"
	"xiaoshiai.cn/common/meta"
)

var _ Object = &Unstructured{}

type Unstructured struct {
	Object map[string]any
}

// GetID implements Object.
func (u *Unstructured) GetID() string {
	return GetNestedString(u.Object, "id")
}

// SetID implements Object.
func (u *Unstructured) SetID(id string) {
	SetNestedField(u.Object, id, "id")
}

// GetVersion implements Object.
func (u *Unstructured) GetAPIVersion() string {
	return GetNestedString(u.Object, "apiVersion")
}

// SetVersion implements Object.
func (u *Unstructured) SetAPIVersion(ver string) {
	SetNestedField(u.Object, ver, "apiVersion")
}

// GetDescription implements Object.
func (u *Unstructured) GetDescription() string {
	return GetNestedString(u.Object, "description")
}

// SetDescription implements Object.
func (u *Unstructured) SetDescription(desc string) {
	SetNestedField(u.Object, desc, "description")
}

// GetName implements Object.
func (u *Unstructured) GetName() string {
	return GetNestedString(u.Object, "name")
}

// SetName implements Object.
func (u *Unstructured) SetName(name string) {
	u.setNestedField(name, "name")
}

// GetUID implements Object.
func (u *Unstructured) GetUID() string {
	return GetNestedString(u.Object, "uid")
}

// SetUID implements Object.
func (u *Unstructured) SetUID(uid string) {
	u.setNestedField(uid, "uid")
}

// GetResource implements Object.
func (u *Unstructured) GetResource() string {
	return GetNestedString(u.Object, "resource")
}

// SetResource implements Object.
func (u *Unstructured) SetResource(resource string) {
	u.setNestedField(resource, "resource")
}

// GetAnnotations implements Object.
func (u *Unstructured) GetAnnotations() map[string]string {
	return GetNestedStringMap(u.Object, "annotations")
}

// SetAnnotations implements Object.
func (u *Unstructured) SetAnnotations(annotations map[string]string) {
	if annotations == nil {
		RemoveNestedField(u.Object, "annotations")
		return
	}
	u.setNestedMap(annotations, "annotations")
}

// GetResourceVersion implements Object.
func (u *Unstructured) GetResourceVersion() int64 {
	return GetNestedInt64(u.Object, "resourceVersion")
}

// SetResourceVersion implements Object.
func (u *Unstructured) SetResourceVersion(v int64) {
	u.setNestedField(v, "resourceVersion")
}

// GetGeneration implements Object.
func (u *Unstructured) GetGeneration() int64 {
	return GetNestedInt64(u.Object, "generation")
}

// SetGeneration implements Object.
func (u *Unstructured) SetGeneration(v int64) {
	u.setNestedField(v, "generation")
}

// GetLabels implements Object.
func (u *Unstructured) GetLabels() map[string]string {
	return GetNestedStringMap(u.Object, "labels")
}

// SetLabels implements Object.
func (u *Unstructured) SetLabels(labels map[string]string) {
	if labels == nil {
		RemoveNestedField(u.Object, "labels")
		return
	}
	u.setNestedMap(labels, "labels")
}

// GetCreationTimestamp implements Object.
func (u *Unstructured) GetCreationTimestamp() meta.Time {
	return GetNestedTime(u.Object, "creationTimestamp")
}

// SetCreationTimestamp implements Object.
func (u *Unstructured) SetCreationTimestamp(t meta.Time) {
	str := t.Format(time.RFC3339)
	u.setNestedField(str, "creationTimestamp")
}

// GetDeletionTimestamp implements Object.
func (u *Unstructured) GetDeletionTimestamp() *meta.Time {
	tim := GetNestedTime(u.Object, "deletionTimestamp")
	if tim.IsZero() {
		return nil
	}
	return &tim
}

// SetDeletionTimestamp implements Object.
func (u *Unstructured) SetDeletionTimestamp(t *meta.Time) {
	if t == nil {
		RemoveNestedField(u.Object, "deletionTimestamp")
		return
	}
	str := t.Format(time.RFC3339)
	u.setNestedField(str, "deletionTimestamp")
}

// GetScopes implements Object.
func (u *Unstructured) GetScopes() []Scope {
	return GetNestedScopes(u.Object, "scopes")
}

func GetNestedScopes(obj map[string]any, fields ...string) []Scope {
	val, ok := GetNestedField(obj, fields...)
	if !ok {
		return nil
	}
	if list, ok := val.([]any); ok {
		scopes := make([]Scope, 0, len(list))
		for _, item := range list {
			if m, ok := item.(map[string]any); ok {
				scopes = append(scopes, Scope{
					Resource: GetNestedString(m, "resource"),
					Name:     GetNestedString(m, "name"),
				})
			}
		}
		return scopes
	}
	return nil
}

// SetScopes implements Object.
func (u *Unstructured) SetScopes(scopes []Scope) {
	if scopes == nil {
		RemoveNestedField(u.Object, "scopes")
		return
	}
	list := make([]any, 0, len(scopes))
	for _, scope := range scopes {
		scopeMap := map[string]any{"resource": scope.Resource, "name": scope.Name}
		list = append(list, scopeMap)
	}
	u.setNestedField(list, "scopes")
}

// GetFinalizers implements Object.
func (u *Unstructured) GetFinalizers() []string {
	return GetNestedStringSlice(u.Object, "finalizers")
}

// SetFinalizers implements Object.
func (u *Unstructured) SetFinalizers(finalizers []string) {
	if finalizers == nil {
		RemoveNestedField(u.Object, "finalizers")
		return
	}
	list := make([]any, 0, len(finalizers))
	for _, f := range finalizers {
		list = append(list, f)
	}
	u.setNestedField(list, "finalizers")
}

// GetOwnerReferences implements Object.
func (u *Unstructured) GetOwnerReferences() []OwnerReference {
	val, ok := GetNestedField(u.Object, "ownerReferences")
	if !ok || val == nil {
		return nil
	}
	list, ok := val.([]any)
	if !ok {
		return nil
	}
	ret := make([]OwnerReference, 0, len(list))
	for _, item := range list {
		if m, ok := item.(map[string]any); ok {
			controller, _ := GetNestedBool(m, "controller")
			var blockOwnerDeletionPtr *bool
			if blockOwnerDeletion, found := GetNestedBool(m, "blockOwnerDeletion"); found {
				blockOwnerDeletionPtr = &blockOwnerDeletion
			}
			ref := OwnerReference{
				Resource:           GetNestedString(m, "resource"),
				ID:                 GetNestedString(m, "id"),
				UID:                GetNestedString(m, "uid"),
				Scopes:             GetNestedScopes(m, "scopes"),
				Controller:         controller,
				BlockOwnerDeletion: blockOwnerDeletionPtr,
			}
			ret = append(ret, ref)
		}
	}
	return ret
}

// SetOwnerReferences implements Object.
func (u *Unstructured) SetOwnerReferences(references []OwnerReference) {
	if references == nil {
		RemoveNestedField(u.Object, "ownerReferences")
		return
	}
	newReferences := make([]any, 0, len(references))
	for _, reference := range references {
		out, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&reference)
		if err != nil {
			continue
		}
		newReferences = append(newReferences, out)
	}
	u.setNestedField(newReferences, "ownerReferences")
}

func (u *Unstructured) setNestedField(value any, fields ...string) {
	if u.Object == nil {
		u.Object = make(map[string]any)
	}
	SetNestedField(u.Object, value, fields...)
}

func (u *Unstructured) setNestedMap(value map[string]string, fields ...string) {
	if u.Object == nil {
		u.Object = make(map[string]any)
	}
	SetNestedStringMap(u.Object, value, fields...)
}

func (u *Unstructured) GetNestedString(fields ...string) string {
	return GetNestedString(u.Object, fields...)
}

func (u *Unstructured) SetNestedString(value string, fields ...string) {
	u.setNestedField(value, fields...)
}

func SetNestedStringMap(obj map[string]any, value map[string]string, fields ...string) error {
	m := make(map[string]any, len(value)) // convert map[string]string into map[string]any
	for k, v := range value {
		m[k] = v
	}
	return SetNestedField(obj, m, fields...)
}

func GetNestedStringMap(obj map[string]any, fields ...string) map[string]string {
	val, found := GetNestedField(obj, fields...)
	if !found {
		return nil
	}
	m, ok := val.(map[string]any)
	if !ok {
		return nil
	}
	strMap := make(map[string]string, len(m))
	for k, v := range m {
		if str, ok := v.(string); ok {
			strMap[k] = str
		} else {
			return nil
		}
	}
	return strMap
}

func GetNestedString(obj map[string]any, fields ...string) string {
	val, ok := GetNestedField(obj, fields...)
	if !ok {
		return ""
	}
	if s, ok := val.(string); ok {
		return s
	}
	return ""
}

func GetNestedTime(obj map[string]any, fields ...string) meta.Time {
	val, ok := GetNestedField(obj, fields...)
	if !ok {
		return meta.Time{}
	}
	if val == "null" {
		return meta.Time{}
	}
	if s, ok := val.(string); ok {
		t, _ := time.Parse(time.RFC3339, s)
		return meta.Time{Time: t}
	}
	return meta.Time{}
}

func GetNestedInt64(obj map[string]any, fields ...string) int64 {
	val, ok := GetNestedField(obj, fields...)
	if !ok {
		return 0
	}
	switch typed := val.(type) {
	case int:
		return int64(typed)
	case int64:
		return typed
	case float64:
		return int64(typed)
	case json.Number:
		i, _ := typed.Int64()
		return i
	}
	return 0
}

func GetNestedBool(obj map[string]any, fields ...string) (bool, bool) {
	val, ok := GetNestedField(obj, fields...)
	if !ok {
		return false, false
	}
	if b, ok := val.(bool); ok {
		return b, true
	}
	return false, false
}

func GetNestedStringSlice(obj map[string]any, fields ...string) []string {
	val, ok := GetNestedField(obj, fields...)
	if !ok {
		return nil
	}
	switch slice := val.(type) {
	case []any:
		ss := make([]string, 0, len(slice))
		for _, v := range slice {
			if str, ok := v.(string); ok {
				ss = append(ss, str)
			}
		}
		return ss
	}
	return nil
}

func GetNestedField(obj map[string]any, fields ...string) (any, bool) {
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

func SetNestedField(obj map[string]any, value any, fields ...string) error {
	m := obj
	if len(fields) == 0 {
		return nil
	}
	for i, field := range fields[:len(fields)-1] {
		if val, ok := m[field]; ok {
			if valMap, ok := val.(map[string]any); ok {
				m = valMap
			} else {
				return fmt.Errorf("value cannot be set because %v is not a map[string]any", jsonPath(fields[:i+1]))
			}
		} else {
			newVal := make(map[string]any)
			m[field] = newVal
			m = newVal
		}
	}
	m[fields[len(fields)-1]] = value
	return nil
}

func jsonPath(fields []string) string {
	return "." + strings.Join(fields, ".")
}

func ToUnstructured(obj Object) (*Unstructured, error) {
	m, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, err
	}
	u := &Unstructured{Object: m}
	return u, nil
}

func FromUnstructured(u *Unstructured, obj Object) error {
	return runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, obj)
}

// UnmarshalJSON preserves integer facts as int64 and fractions as float64,
// retaining the numeric types accepted by Kubernetes unstructured conversion.
func (u *Unstructured) UnmarshalJSON(data []byte) error {
	d := map[string]any{}
	if err := kubejson.UnmarshalCaseSensitivePreserveInts(data, &d); err != nil {
		return err
	}
	u.Object = d
	return nil
}

func (u *Unstructured) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.Object)
}

func (u *Unstructured) UnmarshalBSON(data []byte) error {
	d := map[string]any{}
	if err := bson.Unmarshal(data, &d); err != nil {
		return err
	}
	u.Object = d
	return nil
}

func (u *Unstructured) MarshalBSON() ([]byte, error) {
	return bson.Marshal(u.Object)
}

func CompareUnstructuredField(a, b *Unstructured, sorts []meta.SortField) int {
	for _, sort := range sorts {
		if cmp := CompareDataFieldSort(a, b, sort); cmp != 0 {
			return cmp
		}
	}
	return 0
}

func CompareDataFieldSort(a, b *Unstructured, sort meta.SortField) int {
	switch sort.Field {
	case "name":
		switch sort.Direction {
		case meta.SortDirectionAsc:
			return strings.Compare(a.GetName(), b.GetName())
		case meta.SortDirectionDesc:
			return strings.Compare(b.GetName(), a.GetName())
		default:
			return 0
		}
	case "time":
		at, bt := a.GetCreationTimestamp(), b.GetCreationTimestamp()
		switch sort.Direction {
		case meta.SortDirectionAsc:
			return at.Compare(bt.Time)
		case meta.SortDirectionDesc:
			return bt.Compare(at.Time)
		default:
			return 0
		}
	}
	av, ok := GetNestedField(a.Object, strings.Split(sort.Field, ".")...)
	if !ok {
		av = ""
	}
	bv, ok := GetNestedField(b.Object, strings.Split(sort.Field, ".")...)
	if !ok {
		bv = ""
	}
	switch sort.Direction {
	case meta.SortDirectionAsc:
		return CompareField(av, bv)
	case meta.SortDirectionDesc:
		return CompareField(bv, av)
	default:
		return 0
	}
}

func CompareField(a, b any) int {
	switch av := a.(type) {
	case string:
		bv, ok := b.(string)
		if !ok {
			return 0
		}
		return strings.Compare(av, bv)
	case int64:
		bv, ok := b.(int64)
		if !ok {
			return 0
		}
		if av == bv {
			return 0
		}
		if av < bv {
			return -1
		}
		return 1
	case float64:
		bv, ok := b.(float64)
		if !ok {
			return 0
		}
		if av == bv {
			return 0
		}
		if av < bv {
			return -1
		}
		return 1
	default:
		return 0
	}
}

func ParseSorts(sort string) []meta.SortField {
	return meta.ParseSort(sort)
}
