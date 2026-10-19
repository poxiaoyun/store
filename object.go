// +k8s:openapi-gen=true
package store

import "xiaoshiai.cn/common/meta"

// Object exposes the metadata managed by Store implementations.
type Object interface {
	GetID() string
	SetID(string)

	GetName() string
	SetName(string)

	GetUID() string
	SetUID(string)

	GetDescription() string
	SetDescription(string)

	GetResource() string
	SetResource(string)

	GetScopes() []Scope
	SetScopes([]Scope)

	GetResourceVersion() int64
	SetResourceVersion(int64)

	GetGeneration() int64
	SetGeneration(int64)

	GetLabels() map[string]string
	SetLabels(map[string]string)

	GetAnnotations() map[string]string
	SetAnnotations(map[string]string)

	GetDeletionTimestamp() *meta.Time
	SetDeletionTimestamp(*meta.Time)

	GetCreationTimestamp() meta.Time
	SetCreationTimestamp(meta.Time)

	GetFinalizers() []string
	SetFinalizers([]string)

	GetOwnerReferences() []OwnerReference
	SetOwnerReferences([]OwnerReference)
}

// ObjectList exposes items, resource identity, read version and an opaque
// continuation. Total counts and protocol request parameters are not included.
type ObjectList interface {
	GetResource() string
	SetResource(string)

	GetScopes() []Scope
	SetScopes([]Scope)

	GetResourceVersion() int64
	SetResourceVersion(int64)

	// GetContinue returns the opaque next-batch token, or empty when a cursor
	// traversal is complete.
	GetContinue() string
	// SetContinue sets the opaque next-batch token.
	SetContinue(token string)
}

// +k8s:openapi-gen=true
type Scope struct {
	Resource string `json:"resource,omitempty"`
	Name     string `json:"name,omitempty"`
}

// +k8s:openapi-gen=true
type OwnerReference struct {
	ID                 string  `json:"id,omitempty"`
	Resource           string  `json:"resource,omitempty"`
	UID                string  `json:"uid,omitempty"`
	Scopes             []Scope `json:"scopes,omitempty"`
	Controller         bool    `json:"controller,omitempty"`
	BlockOwnerDeletion *bool   `json:"blockOwnerDeletion,omitempty"`
}

var _ Object = &ObjectMeta{}

// +k8s:openapi-gen=true
type ObjectMeta struct {
	ID                string            `json:"id,omitempty"  validation:"name"`
	Name              string            `json:"name,omitempty"`
	UID               string            `json:"uid,omitempty"`
	APIVersion        string            `json:"apiVersion,omitempty"`
	Scopes            []Scope           `json:"scopes,omitempty"`
	Resource          string            `json:"resource,omitempty"`
	ResourceVersion   int64             `json:"resourceVersion,omitempty"`
	Generation        int64             `json:"generation,omitempty"`
	CreationTimestamp meta.Time         `json:"creationTimestamp"`
	DeletionTimestamp *meta.Time        `json:"deletionTimestamp,omitempty"`
	Labels            map[string]string `json:"labels,omitempty"`
	Annotations       map[string]string `json:"annotations,omitempty"`
	Finalizers        []string          `json:"finalizers,omitempty"`
	OwnerReferences   []OwnerReference  `json:"ownerReferences,omitempty"`
	Description       string            `json:"description,omitempty"`
}

func (o *ObjectMeta) GetID() string {
	return o.ID
}

func (o *ObjectMeta) SetID(id string) {
	o.ID = id
}

// GetVersion implements Object.
func (o *ObjectMeta) GetAPIVersion() string {
	return o.APIVersion
}

// SetVersion implements Object.
func (o *ObjectMeta) SetAPIVersion(version string) {
	o.APIVersion = version
}

// GetDescription implements Object.
func (o *ObjectMeta) GetDescription() string {
	return o.Description
}

// SetDescription implements Object.
func (o *ObjectMeta) SetDescription(desc string) {
	o.Description = desc
}

// GetAnnotations implements Object.
func (o *ObjectMeta) GetAnnotations() map[string]string {
	return o.Annotations
}

// GetCreationTimestamp implements Object.
func (o *ObjectMeta) GetCreationTimestamp() meta.Time {
	return o.CreationTimestamp
}

// GetDeletionTimestamp implements Object.
func (o *ObjectMeta) GetDeletionTimestamp() *meta.Time {
	return o.DeletionTimestamp
}

// GetFinalizers implements Object.
func (o *ObjectMeta) GetFinalizers() []string {
	return o.Finalizers
}

// GetLabels implements Object.
func (o *ObjectMeta) GetLabels() map[string]string {
	return o.Labels
}

// GetName implements Object.
func (o *ObjectMeta) GetName() string {
	return o.Name
}

// GetOwnerReferences implements Object.
func (o *ObjectMeta) GetOwnerReferences() []OwnerReference {
	return o.OwnerReferences
}

// GetResource implements Object.
func (o *ObjectMeta) GetResource() string {
	return o.Resource
}

// GetResourceVersion implements Object.
func (o *ObjectMeta) GetResourceVersion() int64 {
	return o.ResourceVersion
}

// GetScopes implements Object.
func (o *ObjectMeta) GetScopes() []Scope {
	return o.Scopes
}

// GetUID implements Object.
func (o *ObjectMeta) GetUID() string {
	return o.UID
}

// SetAnnotations implements Object.
func (o *ObjectMeta) SetAnnotations(anotations map[string]string) {
	o.Annotations = anotations
}

// SetCreationTimestamp implements Object.
func (o *ObjectMeta) SetCreationTimestamp(creationTimestamp meta.Time) {
	o.CreationTimestamp = creationTimestamp
}

// SetDeletionTimestamp implements Object.
func (o *ObjectMeta) SetDeletionTimestamp(deletionTimestamp *meta.Time) {
	o.DeletionTimestamp = deletionTimestamp
}

// SetFinalizers implements Object.
func (o *ObjectMeta) SetFinalizers(finalizers []string) {
	o.Finalizers = finalizers
}

// SetLabels implements Object.
func (o *ObjectMeta) SetLabels(labels map[string]string) {
	o.Labels = labels
}

// SetName implements Object.
func (o *ObjectMeta) SetName(name string) {
	o.Name = name
}

// SetOwnerReferences implements Object.
func (o *ObjectMeta) SetOwnerReferences(ownerReferences []OwnerReference) {
	o.OwnerReferences = ownerReferences
}

// SetResource implements Object.
func (o *ObjectMeta) SetResource(resource string) {
	o.Resource = resource
}

// SetResourceVersion implements Object.
func (o *ObjectMeta) SetResourceVersion(resourceVersion int64) {
	o.ResourceVersion = resourceVersion
}

// GetGeneration implements Object.
func (o *ObjectMeta) GetGeneration() int64 {
	return o.Generation
}

// SetGeneration implements Object.
func (o *ObjectMeta) SetGeneration(generation int64) {
	o.Generation = generation
}

// SetScopes implements Object.
func (o *ObjectMeta) SetScopes(scopes []Scope) {
	o.Scopes = scopes
}

// SetUID implements Object.
func (o *ObjectMeta) SetUID(uid string) {
	o.UID = uid
}

var _ ObjectList = &List[Object]{}

// List is the Store wire-compatible list result. It carries the read result
// and the backend-owned metadata only; protocol parameters stay in the
// protocol layer.
type List[T any] struct {
	Resource        string  `json:"resource,omitempty"`
	ResourceVersion int64   `json:"resourceVersion,omitempty"`
	Scopes          []Scope `json:"scopes,omitempty"`
	Items           []T     `json:"items" openapi:"dynamic"`
	Continue        string  `json:"continue,omitempty"` // Used for pagination, if set, indicates that there are more items to list
}

// PageFromList projects the Store read result onto the public list contract.
// The backend result supplies items, continuation token and resource version;
// requestOptions supply the protocol echo of the caller's pagination request.
func PageFromList[T any](list List[T], requestOptions meta.ListOptions) meta.Page[T] {
	page := 0
	if requestOptions.Size > 0 {
		page = max(requestOptions.Page, 1)
	}
	return meta.Page[T]{
		ResourceVersion: list.ResourceVersion,
		Items:           list.Items,
		Page:            page,
		Size:            requestOptions.Size,
		Continue:        list.Continue,
		Limit:           requestOptions.Limit,
	}
}

// GetContinue implements ObjectList.
func (b *List[T]) GetContinue() string {
	return b.Continue
}

// SetContinue implements ObjectList.
func (b *List[T]) SetContinue(continueToken string) {
	b.Continue = continueToken
}

// GetResourceVersion implements ObjectList.
func (b *List[T]) GetResourceVersion() int64 {
	return b.ResourceVersion
}

// SetResourceVersion implements ObjectList.
func (b *List[T]) SetResourceVersion(resourceVersion int64) {
	b.ResourceVersion = resourceVersion
}

// GetScopes implements ObjectList.
func (b *List[T]) GetScopes() []Scope {
	return b.Scopes
}

// SetScopes implements ObjectList.
func (b *List[T]) SetScopes(scopes []Scope) {
	b.Scopes = scopes
}

// GetResource implements ObjectList.
func (b *List[T]) GetResource() string {
	return b.Resource
}

// SetResource implements ObjectList.
func (b *List[T]) SetResource(resource string) {
	b.Resource = resource
}

// ConvertList maps list items while preserving Store and read metadata.
func ConvertList[T any, F any](list List[T], f func(T) F) List[F] {
	newItems := make([]F, 0, len(list.Items))
	for _, item := range list.Items {
		newItems = append(newItems, f(item))
	}
	return List[F]{
		Resource:        list.Resource,
		ResourceVersion: list.ResourceVersion,
		Scopes:          list.Scopes,
		Items:           newItems,
		Continue:        list.Continue,
	}
}
