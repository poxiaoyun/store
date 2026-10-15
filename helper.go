package store

import (
	"context"
	stderrors "errors"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/meta"
)

type LocalObjectReference struct {
	ID string `json:"id,omitempty"`
}

type ObjectReference struct {
	ID     string  `json:"id,omitempty"`
	Scopes []Scope `json:"scopes,omitempty"`
}

func ObjectReferenceFrom(obj Object) ObjectReference {
	return ObjectReference{
		ID:     obj.GetID(),
		Scopes: obj.GetScopes(),
	}
}

func (r ObjectReference) String() string {
	key := ""
	for _, scope := range r.Scopes {
		key += "/" + scope.Resource + "/" + scope.Name
	}
	key += "/" + r.ID
	return key
}

func (r ObjectReference) Equals(other ObjectReference) bool {
	return r.ID == other.ID && ScopesEquals(r.Scopes, other.Scopes)
}

type ResourcedObjectReference struct {
	UID      string  `json:"uid,omitempty"`
	ID       string  `json:"id,omitempty"`
	Scopes   []Scope `json:"scopes,omitempty"`
	Resource string  `json:"resource,omitempty"`
}

// Equals checks if two ResourcedObjectReference are equal.
// It compares the Name, Resource, and Scopes fields.
// Note: UID is not compared in this method.
// Use [ResourcedObjectReference.EqualsWithUID] if you need to compare UID as well.
func (r ResourcedObjectReference) Equals(other ResourcedObjectReference) bool {
	return r.ID == other.ID && r.Resource == other.Resource && ScopesEquals(r.Scopes, other.Scopes)
}

// EqualsWithUID same as Equals, but also checks the UID.
func (r ResourcedObjectReference) EqualsWithUID(other ResourcedObjectReference) bool {
	return r.UID == other.UID && r.ID == other.ID && r.Resource == other.Resource && ScopesEquals(r.Scopes, other.Scopes)
}

func (r ResourcedObjectReference) String() string {
	key := ""
	for _, scope := range r.Scopes {
		key += "/" + scope.Resource + "/" + scope.Name
	}
	key += "/" + r.Resource
	key += "/" + r.ID
	return key
}

func ResourcedObjectReferenceFrom(obj Object) ResourcedObjectReference {
	return ResourcedObjectReference{
		UID:      obj.GetUID(),
		ID:       obj.GetID(),
		Scopes:   obj.GetScopes(),
		Resource: obj.GetResource(),
	}
}

func ObjectIdentity(obj Object) string {
	key := ""
	for _, scope := range obj.GetScopes() {
		key += "/" + scope.Resource + "/" + scope.Name
	}
	key += "/" + obj.GetResource()
	key += "/" + obj.GetID()
	return key
}

func ScopesEquals(a, b []Scope) bool {
	if len(a) != len(b) {
		return false
	}
	for i, scope := range a {
		if scope != b[i] {
			return false
		}
	}
	return true
}

// IsSameOrUnderScoped returns true if scope1 is under scope2.
// eg. scope1 = [ { "namespace", "default" } ], scope2 = [ { "namespace", "default" }, { "cluster", "abc" } ].
// scope2 is under scope1.
func ScopesIsSameOrUnder(scope1, scope2 []Scope) bool {
	if len(scope2) == 0 {
		return true
	}
	if len(scope1) < len(scope2) {
		return false
	}
	for i := range scope2 {
		if scope1[i] != scope2[i] {
			return false
		}
	}
	return true
}

func GrowSlice(v reflect.Value, maxCapacity int, sizes ...int) {
	cap := v.Cap()
	max := cap
	for _, size := range sizes {
		if size > max {
			max = size
		}
	}
	if len(sizes) == 0 || max > maxCapacity {
		max = maxCapacity
	}
	if max <= cap {
		return
	}
	if v.Len() > 0 {
		extra := reflect.MakeSlice(v.Type(), v.Len(), max)
		reflect.Copy(extra, v)
		v.Set(extra)
	} else {
		extra := reflect.MakeSlice(v.Type(), 0, max)
		v.Set(extra)
	}
}

func SetFinalizer(obj Object, finalizer string) Object {
	finalizers := obj.GetFinalizers()
	if slices.Contains(finalizers, finalizer) {
		return obj
	}
	obj.SetFinalizers(append(finalizers, finalizer))
	return obj
}

func RemoveFinalizer(o Object, finalizer string) (finalizersUpdated bool) {
	f := o.GetFinalizers()
	length := len(f)

	index := 0
	for i := range length {
		if f[i] == finalizer {
			continue
		}
		f[index] = f[i]
		index++
	}
	o.SetFinalizers(f[:index])
	return length != index
}

func ContainsFinalizer(o Object, finalizer string) bool {
	return slices.Contains(o.GetFinalizers(), finalizer)
}

func IgnoreNotFound(err error) error {
	if errors.IsNotFound(err) {
		return nil
	}
	return err
}

func IgnoreAlreadyExists(err error) error {
	if errors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

var objcType = reflect.TypeOf((*Object)(nil)).Elem()

// NewItemFuncFromList returns the reflect.Value of the Items field of the list and a function to create a new item.
func NewItemFuncFromList(list ObjectList) (reflect.Value, func() Object, error) {
	listPtr, err := GetItemsPtr(list)
	if err != nil {
		return reflect.Value{}, nil, errors.NewBadRequest(fmt.Sprintf("object list must have Items field: %v", err))
	}
	v, err := EnforcePtr(listPtr)
	if err != nil || v.Kind() != reflect.Slice {
		return reflect.Value{}, nil, errors.NewBadRequest(fmt.Sprintf("object Items field must be a slice: %v", err))
	}
	elem := v.Type().Elem()
	if elem.Kind() == reflect.Ptr {
		return reflect.Value{}, nil, errors.NewBadRequest("object Items field must be a slice of non-pointer")
	}
	if !reflect.PointerTo(elem).Implements(objcType) {
		return reflect.Value{}, nil, errors.NewBadRequest("object Items field must be a slice of store.Object type")
	}
	newItemFunc := func() Object {
		return reflect.New(elem).Interface().(Object)
	}
	return v, newItemFunc, nil
}

func GetItemsPtr(list any) (any, error) {
	obj, err := getItemsPtr(list)
	if err != nil {
		return nil, fmt.Errorf("%T is not a list: %v", list, err)
	}
	return obj, nil
}

// getItemsPtr returns a pointer to the list object's Items member or an error.
func getItemsPtr(list any) (any, error) {
	v, err := EnforcePtr(list)
	if err != nil {
		return nil, err
	}
	items := v.FieldByName("Items")
	if !items.IsValid() {
		return nil, stderrors.New("items field not found")
	}
	switch items.Kind() {
	case reflect.Interface, reflect.Pointer:
		target := reflect.TypeOf(items.Interface()).Elem()
		if target.Kind() != reflect.Slice {
			return nil, stderrors.New("items field is not a slice")
		}
		return items.Interface(), nil
	case reflect.Slice:
		return items.Addr().Interface(), nil
	default:
		return nil, stderrors.New("items field is not a slice")
	}
}

func NewObject(obj Object) Object {
	return reflect.New(reflect.TypeOf(obj).Elem()).Interface().(Object)
}

func CreateOrUpdate(ctx context.Context, store Store, obj Object, updatefn func() error) error {
	if err := store.Get(ctx, obj.GetID(), obj); err != nil {
		if !errors.IsNotFound(err) {
			return err
		}
		if err := updatefn(); err != nil {
			return err
		}
		if err := store.Create(ctx, obj); err != nil {
			return err
		}
		return nil
	}
	if err := updatefn(); err != nil {
		return err
	}
	return store.Update(ctx, obj)
}

func EnforcePtr(obj any) (reflect.Value, error) {
	v := reflect.ValueOf(obj)
	if v.Kind() != reflect.Pointer {
		if v.Kind() == reflect.Invalid {
			return reflect.Value{}, fmt.Errorf("expected pointer, but got invalid kind")
		}
		return reflect.Value{}, fmt.Errorf("expected pointer, but got %v type", v.Type())
	}
	if v.IsNil() {
		return reflect.Value{}, fmt.Errorf("expected pointer, but got nil")
	}
	return v.Elem(), nil
}

func ForEachItem(list ObjectList, fn func(Object) error) error {
	items, err := GetItemsPtr(list)
	if err != nil {
		return err
	}
	v := reflect.ValueOf(items)
	v = reflect.Indirect(v)
	for i := range v.Len() {
		itemv := v.Index(i)
		// if item is not a pointer, we need to get its address
		if itemv.Kind() != reflect.Ptr {
			itemv = itemv.Addr()
		}
		item, ok := itemv.Interface().(Object)
		if !ok {
			return fmt.Errorf("item is not a Object: %T", itemv.Interface())
		}
		if err := fn(item); err != nil {
			return err
		}
	}
	return nil
}

func CreateIfNotExists(ctx context.Context, store Store, obj Object) error {
	if err := store.Create(ctx, obj); err != nil {
		if !errors.IsAlreadyExists(err) {
			return err
		}
	}
	return nil
}

func SearchNameFunc[T any](search string, getname func(T) string) func(T) bool {
	if getname == nil || search == "" {
		return nil
	}
	return func(item T) bool {
		return strings.Contains(getname(item), search)
	}
}

func SortByFunc[T any](by string, getname func(T) string, gettime func(T) time.Time) func(a, b T) int {
	switch by {
	case "createTime", "createTimeAsc", "time":
		if gettime == nil {
			return nil
		}
		return func(a, b T) int {
			if timcmp := gettime(a).Compare(gettime(b)); timcmp == 0 && getname != nil {
				return strings.Compare(getname(a), getname(b))
			} else {
				return timcmp
			}
		}
	case "createTimeDesc", "time-", "": // default sort by time desc
		if gettime == nil {
			return nil
		}
		return func(a, b T) int {
			if timcmp := gettime(b).Compare(gettime(a)); timcmp == 0 && getname != nil {
				return strings.Compare(getname(a), getname(b))
			} else {
				return timcmp
			}
		}
	case "name":
		if getname == nil {
			return nil
		}
		return func(a, b T) int {
			return strings.Compare(getname(a), getname(b))
		}
	case "nameDesc", "name-":
		if getname == nil {
			return nil
		}
		return func(a, b T) int {
			return strings.Compare(getname(b), getname(a))
		}
	default:
		return nil
	}
}

func NewTimeAsName() string {
	return time.Now().Format("20060102150405")
}

func StringsToAny(values []string) []any {
	result := make([]any, len(values))
	for i, v := range values {
		result[i] = StringToAny(v)
	}
	return result
}

func AnyToStrings(values []any) []string {
	result := make([]string, len(values))
	for i, v := range values {
		result[i] = AnyToString(v)
	}
	return result
}

var (
	rfc3339regex  = regexp.MustCompile(`^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?(Z|[+-]\d{2}:\d{2})$`)
	isnumberRegex = regexp.MustCompile(`^\d+$`)
	isboolRegex   = regexp.MustCompile(`^(true|false)$`)
)

// autoConvertString convert string to any type
// it usefull for convert string to time.Time and compare
func StringToAny(s string) any {
	if isboolRegex.MatchString(s) {
		if b, err := strconv.ParseBool(s); err == nil {
			return b
		}
	}
	if rfc3339regex.MatchString(s) {
		if tim, err := time.Parse(time.RFC3339, s); err == nil {
			return tim
		}
	}
	if isnumberRegex.MatchString(s) {
		if i, err := strconv.Atoi(s); err == nil {
			return i
		}
	}
	return s
}

func AnyToString(a any) string {
	if a == nil {
		return "null"
	}
	switch v := a.(type) {
	case string:
		return v
	case time.Time:
		return v.Format(time.RFC3339)
	case *time.Time:
		if v != nil {
			return v.Format(time.RFC3339)
		}
		return ""
	case *meta.Time:
		if v != nil {
			return v.Time.Format(time.RFC3339)
		}
		return ""
	case meta.Time:
		return v.Time.Format(time.RFC3339)
	case bool:
		return strconv.FormatBool(v)
	case int:
		return strconv.Itoa(v)
	case int8:
		return strconv.Itoa(int(v))
	case int32:
		return strconv.Itoa(int(v))
	case int64:
		return strconv.FormatInt(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	default:
		return fmt.Sprintf("%v", v)
	}
}

// ScopeResourceToFieldName converts a scope'resource to a field name.
// eg. scope: tenants  => field: tenant
func ScopeResourceToFieldName(resource string) string {
	resource = strings.ToLower(resource)
	if strings.HasSuffix(resource, "ies") {
		return resource[:len(resource)-3] + "y"
	}
	return strings.TrimSuffix(resource, "s")
}

// SetScopesFields sets the scope's as fields in the data map.
// exaple:
//
//	if data = {"foo": "bar"} create/update under scopes [{"resource": "tenants", "name": "default"}]
//	the final data will be:
//	{"foo": "bar", "tenant": "default"}
func SetScopesFields(data map[string]any, scopes []Scope) {
	if len(scopes) == 0 {
		return
	}
	for _, scope := range scopes {
		field := ScopeResourceToFieldName(scope.Resource)
		if field == "" {
			continue
		}
		// overwrite the field
		data[field] = scope.Name
	}
}
