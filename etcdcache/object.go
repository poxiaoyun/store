package etcdcache

import (
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

var (
	_ runtime.Object            = &StorageObject{}
	_ metav1.Object             = &StorageObject{}
	_ metav1.ObjectMetaAccessor = &StorageObject{}
)

type StorageObject struct {
	Object map[string]any `json:",inline"`
}

// GetObjectMeta implements v1.ObjectMetaAccessor.
func (s *StorageObject) GetObjectMeta() metav1.Object {
	return (*StorageObjectMeta)(s)
}

type StorageObjectMeta StorageObject

// GetName implements v1.Object.
func (s *StorageObjectMeta) GetName() string {
	return GetNestedString(s.Object, "name")
}

// SetName implements v1.Object.
func (s *StorageObjectMeta) SetName(name string) {
	s.setNestedField(name, "name")
}

// GetNamespace implements v1.Object.
func (s *StorageObjectMeta) GetNamespace() string {
	return GetNestedString(s.Object, "namespace")
}

// SetNamespace implements v1.Object.
func (s *StorageObjectMeta) SetNamespace(namespace string) {
	s.setNestedField(namespace, "namespace")
}

// GetSelfLink implements v1.Object.
func (s *StorageObjectMeta) GetSelfLink() string {
	return GetNestedString(s.Object, "selfLink")
}

// SetSelfLink implements v1.Object.
func (s *StorageObjectMeta) SetSelfLink(selfLink string) {
	s.setNestedField(selfLink, "selfLink")
}

// GetUID implements v1.Object.
func (s *StorageObjectMeta) GetUID() types.UID {
	val, _ := NestedFieldString(s.Object, "uid")
	return types.UID(val)
}

// SetUID implements v1.Object.
func (s *StorageObjectMeta) SetUID(uid types.UID) {
	s.setNestedField(string(uid), "uid")
}

// GetResourceVersion implements v1.Object.
func (s *StorageObjectMeta) GetResourceVersion() string {
	intval, ok := NestedFieldInt64(s.Object, "resourceVersion")
	if !ok {
		return ""
	}
	return strconv.FormatInt(intval, 10)
}

// SetResourceVersion implements v1.Object.
func (s *StorageObjectMeta) SetResourceVersion(version string) {
	if version == "" {
		RemoveNestedField(s.Object, "resourceVersion")
	} else {
		intval, err := strconv.ParseInt(version, 10, 64)
		if err != nil {
			return
		}
		s.setNestedField(intval, "resourceVersion")
	}
}

// GetLabels implements v1.Object.
func (s *StorageObjectMeta) GetLabels() map[string]string {
	labels, _ := NestedFieldStringMap(s.Object, "labels")
	return labels
}

// SetLabels implements v1.Object.
func (s *StorageObjectMeta) SetLabels(labels map[string]string) {
	s.setNestedFieldStringMap(labels, "labels")
}

// GetAnnotations implements v1.Object.
func (s *StorageObjectMeta) GetAnnotations() map[string]string {
	annotations, _ := NestedFieldStringMap(s.Object, "annotations")
	return annotations
}

// SetAnnotations implements v1.Object.
func (s *StorageObjectMeta) SetAnnotations(annotations map[string]string) {
	s.setNestedFieldStringMap(annotations, "annotations")
}

// GetCreationTimestamp implements v1.Object.
func (s *StorageObjectMeta) GetCreationTimestamp() metav1.Time {
	var timestamp metav1.Time
	timestamp.UnmarshalQueryParameter(GetNestedString(s.Object, "creationTimestamp"))
	return timestamp
}

// SetCreationTimestamp implements v1.Object.
func (s *StorageObjectMeta) SetCreationTimestamp(timestamp metav1.Time) {
	ts, _ := timestamp.MarshalQueryParameter()
	if len(ts) == 0 || timestamp.Time.IsZero() {
		RemoveNestedField(s.Object, "creationTimestamp")
		return
	}
	s.setNestedField(ts, "creationTimestamp")
}

// GetDeletionGracePeriodSeconds implements v1.Object.
func (s *StorageObjectMeta) GetDeletionGracePeriodSeconds() *int64 {
	val, ok := NestedFieldInt64(s.Object, "deletionGracePeriodSeconds")
	if !ok {
		return nil
	}
	return &val
}

// SetDeletionGracePeriodSeconds implements v1.Object.
func (s *StorageObjectMeta) SetDeletionGracePeriodSeconds(val *int64) {
	if val == nil {
		RemoveNestedField(s.Object, "deletionGracePeriodSeconds")
	} else {
		s.setNestedField(*val, "deletionGracePeriodSeconds")
	}
}

// GetDeletionTimestamp implements v1.Object.
func (s *StorageObjectMeta) GetDeletionTimestamp() *metav1.Time {
	var timestamp metav1.Time
	timestamp.UnmarshalQueryParameter(GetNestedString(s.Object, "deletionTimestamp"))
	if timestamp.IsZero() {
		return nil
	}
	return &timestamp
}

// SetDeletionTimestamp implements v1.Object.
func (s *StorageObjectMeta) SetDeletionTimestamp(timestamp *metav1.Time) {
	if timestamp == nil {
		RemoveNestedField(s.Object, "deletionTimestamp")
		return
	}
	ts, _ := timestamp.MarshalQueryParameter()
	s.setNestedField(ts, "deletionTimestamp")
}

// GetFinalizers implements v1.Object.
func (s *StorageObjectMeta) GetFinalizers() []string {
	val, ok := NestedFieldStringSlice(s.Object, "finalizers")
	if !ok {
		return nil
	}
	return val
}

// SetFinalizers implements v1.Object.
func (s *StorageObjectMeta) SetFinalizers(finalizers []string) {
	s.setNestedStringSlice(finalizers, "finalizers")
}

// GetGenerateName implements v1.Object.
func (s *StorageObjectMeta) GetGenerateName() string {
	val, _ := NestedFieldString(s.Object, "generateName")
	return val
}

// SetGenerateName implements v1.Object.
func (s *StorageObjectMeta) SetGenerateName(name string) {
	s.setNestedField(name, "generateName")
}

// GetGeneration implements v1.Object.
func (s *StorageObjectMeta) GetGeneration() int64 {
	val, ok := NestedFieldInt64(s.Object, "generation")
	if !ok {
		return 0
	}
	return val
}

// SetGeneration implements v1.Object.
func (s *StorageObjectMeta) SetGeneration(generation int64) {
	s.setNestedField(generation, "generation")
}

// GetManagedFields returns the managed fields associated with the metadata
func (s *StorageObjectMeta) GetManagedFields() []metav1.ManagedFieldsEntry {
	v, found := NestedFieldNoCopy(s.Object, "managedFields")
	if !found {
		return nil
	}
	items, ok := v.([]any)
	if !ok {
		return nil
	}
	managedFields := []metav1.ManagedFieldsEntry{}
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			return nil
		}
		out := metav1.ManagedFieldsEntry{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(m, &out); err != nil {
			continue // Skip invalid entries rather than failing entirely
		}
		managedFields = append(managedFields, out)
	}
	return managedFields
}

func (u *StorageObjectMeta) SetManagedFields(managedFields []metav1.ManagedFieldsEntry) {
	if managedFields == nil {
		RemoveNestedField(u.Object, "managedFields")
		return
	}
	items := []any{}
	for _, managedFieldsEntry := range managedFields {
		out, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&managedFieldsEntry)
		if err != nil {
			return
		}
		items = append(items, out)
	}
	u.setNestedField(items, "managedFields")
}

// GetOwnerReferences implements v1.Object.
func (s *StorageObjectMeta) GetOwnerReferences() []metav1.OwnerReference {
	field, found := NestedFieldNoCopy(s.Object, "ownerReferences")
	if !found {
		return nil
	}
	original, ok := field.([]any)
	if !ok {
		return nil
	}
	ret := make([]metav1.OwnerReference, 0, len(original))
	for _, obj := range original {
		o, ok := obj.(map[string]any)
		if !ok {
			return nil
		}
		ret = append(ret, extractOwnerReference(o))
	}
	return ret
}

func extractOwnerReference(v map[string]any) metav1.OwnerReference {
	// though this field is a *bool, but when decoded from JSON, it's
	// unmarshalled as bool.
	var controllerPtr *bool
	if controller, found := NestedFieldBool(v, "controller"); found {
		controllerPtr = &controller
	}
	var blockOwnerDeletionPtr *bool
	if blockOwnerDeletion, found := NestedFieldBool(v, "blockOwnerDeletion"); found {
		blockOwnerDeletionPtr = &blockOwnerDeletion
	}
	return metav1.OwnerReference{
		Kind:               GetNestedString(v, "kind"),
		Name:               GetNestedString(v, "name"),
		APIVersion:         GetNestedString(v, "apiVersion"),
		UID:                types.UID(GetNestedString(v, "uid")),
		Controller:         controllerPtr,
		BlockOwnerDeletion: blockOwnerDeletionPtr,
	}
}

// SetOwnerReferences implements v1.Object.
func (s *StorageObjectMeta) SetOwnerReferences(references []metav1.OwnerReference) {
	if references == nil {
		RemoveNestedField(s.Object, "ownerReferences")
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
	s.setNestedField(newReferences, "ownerReferences")
}

func (s *StorageObjectMeta) setNestedFieldStringMap(value map[string]string, fields ...string) {
	if s.Object == nil {
		s.Object = make(map[string]any)
	}
	SetNestedStringMap(s.Object, value, fields...)
}

func (s *StorageObjectMeta) setNestedField(value any, fields ...string) {
	if s.Object == nil {
		s.Object = make(map[string]any)
	}
	SetNestedFieldNoCopy(s.Object, value, fields...)
}

func (s *StorageObjectMeta) setNestedStringSlice(value []string, fields ...string) {
	if s.Object == nil {
		s.Object = make(map[string]any)
	}
	SetNestedFieldNoCopy(s.Object, value, fields...)
}

func (s *StorageObject) GetLabels() map[string]string {
	labels, _ := NestedFieldStringMap(s.Object, "labels")
	return labels
}

func (s *StorageObject) GetAnnotations() map[string]string {
	annotations, _ := NestedFieldStringMap(s.Object, "annotations")
	return annotations
}

// GetRevision returns the resource version as int64 (internal storage use only).
func (s *StorageObject) GetRevision() int64 {
	val, _ := NestedFieldInt64(s.Object, "resourceVersion")
	return val
}

// SetRevision sets the resource version from int64 (internal storage use only).
func (s *StorageObject) SetRevision(version int64) {
	s.setNestedField(version, "resourceVersion")
}

// The following methods implement metav1.Object by delegating to StorageObjectMeta.

func (s *StorageObject) GetNamespace() string        { return (*StorageObjectMeta)(s).GetNamespace() }
func (s *StorageObject) SetNamespace(ns string)      { (*StorageObjectMeta)(s).SetNamespace(ns) }
func (s *StorageObject) GetName() string             { return (*StorageObjectMeta)(s).GetName() }
func (s *StorageObject) SetName(name string)         { (*StorageObjectMeta)(s).SetName(name) }
func (s *StorageObject) GetGenerateName() string     { return (*StorageObjectMeta)(s).GetGenerateName() }
func (s *StorageObject) SetGenerateName(name string) { (*StorageObjectMeta)(s).SetGenerateName(name) }
func (s *StorageObject) GetUID() types.UID           { return (*StorageObjectMeta)(s).GetUID() }
func (s *StorageObject) SetUID(uid types.UID)        { (*StorageObjectMeta)(s).SetUID(uid) }
func (s *StorageObject) GetResourceVersion() string {
	return (*StorageObjectMeta)(s).GetResourceVersion()
}
func (s *StorageObject) SetResourceVersion(version string) {
	(*StorageObjectMeta)(s).SetResourceVersion(version)
}
func (s *StorageObject) GetGeneration() int64 { return (*StorageObjectMeta)(s).GetGeneration() }
func (s *StorageObject) SetGeneration(generation int64) {
	(*StorageObjectMeta)(s).SetGeneration(generation)
}
func (s *StorageObject) GetSelfLink() string         { return (*StorageObjectMeta)(s).GetSelfLink() }
func (s *StorageObject) SetSelfLink(selfLink string) { (*StorageObjectMeta)(s).SetSelfLink(selfLink) }
func (s *StorageObject) GetCreationTimestamp() metav1.Time {
	return (*StorageObjectMeta)(s).GetCreationTimestamp()
}
func (s *StorageObject) SetCreationTimestamp(t metav1.Time) {
	(*StorageObjectMeta)(s).SetCreationTimestamp(t)
}
func (s *StorageObject) GetDeletionTimestamp() *metav1.Time {
	return (*StorageObjectMeta)(s).GetDeletionTimestamp()
}
func (s *StorageObject) SetDeletionTimestamp(t *metav1.Time) {
	(*StorageObjectMeta)(s).SetDeletionTimestamp(t)
}
func (s *StorageObject) GetDeletionGracePeriodSeconds() *int64 {
	return (*StorageObjectMeta)(s).GetDeletionGracePeriodSeconds()
}
func (s *StorageObject) SetDeletionGracePeriodSeconds(v *int64) {
	(*StorageObjectMeta)(s).SetDeletionGracePeriodSeconds(v)
}
func (s *StorageObject) SetLabels(labels map[string]string) {
	(*StorageObjectMeta)(s).SetLabels(labels)
}
func (s *StorageObject) SetAnnotations(annotations map[string]string) {
	(*StorageObjectMeta)(s).SetAnnotations(annotations)
}
func (s *StorageObject) GetFinalizers() []string { return (*StorageObjectMeta)(s).GetFinalizers() }
func (s *StorageObject) SetFinalizers(finalizers []string) {
	(*StorageObjectMeta)(s).SetFinalizers(finalizers)
}
func (s *StorageObject) GetOwnerReferences() []metav1.OwnerReference {
	return (*StorageObjectMeta)(s).GetOwnerReferences()
}
func (s *StorageObject) SetOwnerReferences(refs []metav1.OwnerReference) {
	(*StorageObjectMeta)(s).SetOwnerReferences(refs)
}
func (s *StorageObject) GetManagedFields() []metav1.ManagedFieldsEntry {
	return (*StorageObjectMeta)(s).GetManagedFields()
}
func (s *StorageObject) SetManagedFields(f []metav1.ManagedFieldsEntry) {
	(*StorageObjectMeta)(s).SetManagedFields(f)
}

func (s *StorageObject) GetObjectKind() schema.ObjectKind {
	return s
}

func (u *StorageObject) SetGroupVersionKind(gvk schema.GroupVersionKind) {
	u.SetAPIVersion(gvk.GroupVersion().String())
	u.SetKind(gvk.Kind)
}

func (u *StorageObject) GroupVersionKind() schema.GroupVersionKind {
	gv, err := schema.ParseGroupVersion(u.GetAPIVersion())
	if err != nil {
		return schema.GroupVersionKind{}
	}
	gvk := gv.WithKind(u.GetKind())
	return gvk
}

func (u StorageObject) GetAPIVersion() string {
	return GetNestedString(u.Object, "apiVersion")
}

func (u *StorageObject) SetAPIVersion(version string) {
	u.setNestedField(version, "apiVersion")
}

func (u StorageObject) GetKind() string {
	return GetNestedString(u.Object, "resource")
}

func (u *StorageObject) SetKind(kind string) {
	u.setNestedField(kind, "resource")
}

func (u *StorageObject) MarshalJSON() ([]byte, error) {
	return JsonMarshal(u.Object)
}

func (u *StorageObject) UnmarshalJSON(b []byte) error {
	return JsonUnmarshal(b, &u.Object)
}

func (in *StorageObject) DeepCopyInto(out *StorageObject) {
	clone := in.DeepCopy()
	*out = *clone
}

// DeepCopyObject is an autogenerated deepcopy function, copying the receiver, creating a new runtime.Object.
func (in *StorageObject) DeepCopyObject() runtime.Object {
	if c := in.DeepCopy(); c != nil {
		return c
	}
	return nil
}

func (in *StorageObject) DeepCopy() *StorageObject {
	if in == nil {
		return nil
	}
	out := new(StorageObject)
	*out = *in
	out.Object = runtime.DeepCopyJSON(in.Object)
	return out
}

func (u *StorageObject) setNestedField(value any, fields ...string) {
	if u.Object == nil {
		u.Object = make(map[string]any)
	}
	SetNestedFieldNoCopy(u.Object, value, fields...)
}
