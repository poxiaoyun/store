package etcdcache

import (
	"encoding/json"
	"maps"
	"strconv"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/utils/ptr"
)

var (
	_ runtime.Object          = &StorageObjectList{}
	_ metav1.ListMetaAccessor = &StorageObjectList{}
)

type StorageObjectList struct {
	Object map[string]any  `json:",inline"`
	Items  []StorageObject `json:"items"`
}

// GetListMeta implements v1.ListMetaAccessor.
func (s *StorageObjectList) GetListMeta() metav1.ListInterface {
	return (*StorageObjectListMeta)(s)
}

type StorageObjectListMeta StorageObjectList

// GetSelfLink implements v1.ListInterface.
func (s *StorageObjectListMeta) GetSelfLink() string {
	return GetNestedString(s.Object, "selfLink")
}

// SetSelfLink implements v1.ListInterface.
func (s *StorageObjectListMeta) SetSelfLink(selfLink string) {
	s.setNestedField(selfLink, "selfLink")
}

// GetContinue implements v1.ListInterface.
func (s *StorageObjectListMeta) GetContinue() string {
	return GetNestedString(s.Object, "continue")
}

// SetContinue implements v1.ListInterface.
func (s *StorageObjectListMeta) SetContinue(c string) {
	s.setNestedField(c, "continue")
}

// GetRemainingItemCount implements v1.ListInterface.
func (s *StorageObjectListMeta) GetRemainingItemCount() *int64 {
	val, ok := NestedFieldInt64(s.Object, "remainingItemCount")
	if !ok {
		return nil
	}
	return ptr.To(val)
}

// SetRemainingItemCount implements v1.ListInterface.
func (s *StorageObjectListMeta) SetRemainingItemCount(c *int64) {
	if c == nil {
		RemoveNestedField(s.Object, "remainingItemCount")
	} else {
		s.setNestedField(float64(*c), "remainingItemCount")
	}
}

// GetResourceVersion implements v1.ListInterface.
func (s *StorageObjectListMeta) GetResourceVersion() string {
	intval, ok := NestedFieldInt64(s.Object, "resourceVersion")
	if !ok {
		return ""
	}
	return strconv.FormatInt(intval, 10)
}

// SetResourceVersion implements v1.ListInterface.
func (s *StorageObjectListMeta) SetResourceVersion(version string) {
	if version == "" {
		RemoveNestedField(s.Object, "resourceVersion")
		return
	}
	intval, err := strconv.ParseInt(version, 10, 64)
	if err != nil {
		return
	}
	s.setNestedField(intval, "resourceVersion")
}

func (u *StorageObjectListMeta) setNestedField(value any, fields ...string) {
	if u.Object == nil {
		u.Object = make(map[string]any)
	}
	SetNestedFieldNoCopy(u.Object, value, fields...)
}

// GetContinue implements meta.List.
func (s *StorageObjectList) GetContinue() string {
	return GetNestedString(s.Object, "continue")
}

// SetContinue implements meta.List.
func (s *StorageObjectList) SetContinue(c string) {
	s.setNestedField(c, "continue")
}

// GetRemainingItemCount implements meta.List.
func (s *StorageObjectList) GetRemainingItemCount() *int64 {
	val, ok := NestedFieldInt64(s.Object, "remainingItemCount")
	if !ok {
		return nil
	}
	return ptr.To(val)
}

// SetRemainingItemCount implements meta.List.
func (s *StorageObjectList) SetRemainingItemCount(c *int64) {
	if c == nil {
		RemoveNestedField(s.Object, "remainingItemCount")
	} else {
		s.setNestedField(float64(*c), "remainingItemCount")
	}
}

// GetResourceVersion must implement meta.List
// otherwise it may return an int64
func (s *StorageObjectList) GetResourceVersion() int64 {
	val, _ := NestedFieldInt64(s.Object, "resourceVersion")
	return val
}

// SetResourceVersion implements meta.List.
func (s *StorageObjectList) SetResourceVersion(version int64) {
	s.setNestedField(version, "resourceVersion")
}

// GetSelfLink implements meta.List.
func (s *StorageObjectList) GetSelfLink() string {
	return s.getNestedString("selfLink")
}

// SetSelfLink implements meta.List.
func (s *StorageObjectList) SetSelfLink(selfLink string) {
	s.setNestedField(selfLink, "selfLink")
}

// GetObjectKind implements runtime.Object.
func (s *StorageObjectList) GetObjectKind() schema.ObjectKind {
	return s
}

func (u *StorageObjectList) GetAPIVersion() string {
	return u.getNestedString("apiVersion")
}

func (u *StorageObjectList) SetAPIVersion(version string) {
	u.setNestedField(version, "apiVersion")
}

func (u *StorageObjectList) GetKind() string {
	return u.getNestedString("resource")
}

func (u *StorageObjectList) SetKind(kind string) {
	u.setNestedField(kind, "resource")
}

func (u *StorageObjectList) SetGroupVersionKind(gvk schema.GroupVersionKind) {
	u.SetAPIVersion(gvk.GroupVersion().String())
	u.SetKind(gvk.Kind)
}

func (u *StorageObjectList) GroupVersionKind() schema.GroupVersionKind {
	gv, err := schema.ParseGroupVersion(u.GetAPIVersion())
	if err != nil {
		return schema.GroupVersionKind{}
	}
	gvk := gv.WithKind(u.GetKind())
	return gvk
}

func (u *StorageObjectList) setNestedField(value any, fields ...string) {
	if u.Object == nil {
		u.Object = make(map[string]any)
	}
	SetNestedFieldNoCopy(u.Object, value, fields...)
}

func (u *StorageObjectList) getNestedString(fields ...string) string {
	val, _ := NestedFieldString(u.Object, fields...)
	return val
}

func (u *StorageObjectList) MarshalJSON() ([]byte, error) {
	listObj := make(map[string]any, len(u.Object)+1)
	maps.Copy(listObj, u.Object)
	listObj["items"] = u.Items
	return JsonMarshal(listObj)
}

func (u *StorageObjectList) UnmarshalJSON(data []byte) error {
	type decodeList struct {
		Items []json.RawMessage `json:"items"`
	}
	var dList decodeList
	if err := JsonUnmarshal(data, &dList); err != nil {
		return err
	}
	if err := JsonUnmarshal(data, &u.Object); err != nil {
		return err
	}
	delete(u.Object, "items")

	listAPIVersion := u.GetAPIVersion()
	listKind := u.GetKind()

	inferItemKind := strings.TrimSuffix(listKind, "List")
	u.Items = make([]StorageObject, 0, len(dList.Items))
	for _, i := range dList.Items {
		unstruct := &StorageObject{}
		if err := JsonUnmarshal([]byte(i), unstruct); err != nil {
			return err
		}
		if len(unstruct.GetKind()) == 0 && len(unstruct.GetAPIVersion()) == 0 {
			unstruct.SetKind(inferItemKind)
			unstruct.SetAPIVersion(listAPIVersion)
		}
		u.Items = append(u.Items, *unstruct)
	}
	return nil
}

// DeepCopyInto is an autogenerated deepcopy function, copying the receiver, writing into out. in must be non-nil.
func (in *StorageObjectList) DeepCopyInto(out *StorageObjectList) {
	clone := in.DeepCopy()
	*out = *clone
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *StorageObjectList) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (u *StorageObjectList) DeepCopy() *StorageObjectList {
	if u == nil {
		return nil
	}
	out := new(StorageObjectList)
	*out = *u
	out.Object = runtime.DeepCopyJSON(u.Object)
	out.Items = make([]StorageObject, len(u.Items))
	for i := range u.Items {
		u.Items[i].DeepCopyInto(&out.Items[i])
	}
	return out
}
