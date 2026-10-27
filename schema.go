package store

import (
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
)

var (
	resourceOrIndexNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]*$`)
	fieldPathPattern           = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)
)

// Index describes a resource index. Fields are ordered and are normalized to
// include the resource's scope keys when the resource is registered.
type Index struct {
	Name     string
	Fields   []string
	Unique   bool
	Nullable bool
}

// FieldSpec declares storage attributes of one model field beyond what the
// field type itself expresses. Name identifies the field by its representation
// name, which is the same name used for its column and for index fields.
type FieldSpec struct {
	Name string
	// Size bounds a string column. A backend with a bounded key length uses it
	// for an indexed string and defaults to 255 when it is not declared.
	// An unindexed string uses the backend's unbounded text type.
	Size int
}

// ResourceSchema describes one resource and the indexes required by stores.
type ResourceSchema struct {
	Object Object
	// ResourceName is the resource name. Empty derives a default name from Object.
	ResourceName string
	// StatusResource declares that the resource exposes the status subresource.
	// A typed resource must also declare a status field.
	StatusResource bool
	ScopeKeys      []string
	Indexes        []Index
	// FieldSpecs declare per-field storage attributes. A name must be unique
	// and must match a field of Object, so a declared attribute can never be
	// silently ignored.
	FieldSpecs []FieldSpec
}

// IsPrimaryIndex reports whether index is the implicit scoped ID index.
func (r ResourceSchema) IsPrimaryIndex(index Index) bool {
	fields := append([]string{"id"}, r.ScopeKeys...)
	return index.Unique && !index.Nullable && slices.Equal(index.Fields, fields)
}

// Schema is the common resource and index definition consumed by stores.
type Schema struct {
	mu        sync.RWMutex
	resources map[string]ResourceSchema
	types     map[reflect.Type]string
}

func NewSchema() *Schema {
	return &Schema{
		resources: map[string]ResourceSchema{},
		types:     map[reflect.Type]string{},
	}
}

func (s *Schema) Register(obj Object, definition ResourceSchema) error {
	if s == nil {
		return fmt.Errorf("schema is nil")
	}
	if obj == nil || reflect.ValueOf(obj).
		Kind() == reflect.Ptr && reflect.ValueOf(obj).
		IsNil() {
		return fmt.Errorf("object is nil")
	}
	resource := definition.ResourceName
	var err error
	if resource == "" {
		resource, err = ReflectResourceName(reflect.TypeOf(obj))
		if err != nil {
			return err
		}
	}
	if !resourceOrIndexNamePattern.MatchString(resource) {
		return fmt.Errorf("resource name %q is invalid", resource)
	}
	definition.ResourceName = resource
	definition.Object = obj
	definition, err = normalizeResourceSchema(resource, definition)
	if err != nil {
		return err
	}
	key := resourceType(obj)
	dynamic := false
	if _, ok := obj.(*Unstructured); ok {
		dynamic = true
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.resources[resource]; exists {
		return fmt.Errorf("resource %q already registered", resource)
	}
	if !dynamic {
		if existing, exists := s.types[key]; exists {
			return fmt.Errorf("type %v already registered as %q", key, existing)
		}
		s.types[key] = resource
	}
	s.resources[resource] = definition
	return nil
}

// ResourceName resolves the registered resource name for obj. Unstructured
// objects and object lists use their own resource field. Types that are not
// registered fall back to the default name derived from their Go type.
func (s *Schema) ResourceName(obj any) (string, error) {
	if s == nil {
		return "", fmt.Errorf("schema is nil")
	}
	switch value := obj.(type) {
	case *Unstructured:
		if resource := value.GetResource(); resource != "" {
			return resource, nil
		}
		return "", fmt.Errorf("cannot get resource name from Unstructured object")
	case ObjectList:
		if resource := value.GetResource(); resource != "" {
			return resource, nil
		}
		itemsPointer, err := GetItemsPtr(value)
		if err != nil {
			return "", err
		}
		key := reflect.TypeOf(itemsPointer).
			Elem().
			Elem()
		for key.Kind() == reflect.Pointer {
			key = key.Elem()
		}
		s.mu.RLock()
		resource, exists := s.types[key]
		s.mu.RUnlock()
		if !exists {
			return ReflectResourceName(key)
		}
		return resource, nil
	}
	key := resourceType(obj)
	if key == nil {
		return "", fmt.Errorf("object is nil")
	}
	s.mu.RLock()
	resource, exists := s.types[key]
	s.mu.RUnlock()
	if !exists {
		return ReflectResourceName(key)
	}
	return resource, nil
}

func resourceType(obj any) reflect.Type {
	t := reflect.TypeOf(obj)
	if t == nil {
		return nil
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

func (s *Schema) Resource(resource string) (ResourceSchema, error) {
	if s == nil {
		return ResourceSchema{}, fmt.Errorf("schema is nil")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	definition, exists := s.resources[resource]
	if !exists {
		return ResourceSchema{}, fmt.Errorf("resource %q is not registered", resource)
	}
	return cloneResourceSchema(definition), nil
}

func (s *Schema) Resources() []string {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	resources := make([]string, 0, len(s.resources))
	for resource := range s.resources {
		resources = append(resources, resource)
	}
	sort.Strings(resources)
	return resources
}

func (s *Schema) NewObject(resource string) (Object, error) {
	definition, err := s.Resource(resource)
	if err != nil {
		return nil, err
	}
	typeOf := reflect.TypeOf(definition.Object)
	var object Object
	if typeOf.Kind() == reflect.Ptr {
		object = reflect.New(typeOf.Elem()).
			Interface().(Object)
	} else {
		object = reflect.New(typeOf).
			Elem().
			Interface().(Object)
	}
	object.SetResource(resource)
	return object, nil
}

// Snapshot returns an independent copy of the schema.
func (s *Schema) Snapshot() *Schema {
	clone := NewSchema()
	s.mu.RLock()
	defer s.mu.RUnlock()
	for resource, definition := range s.resources {
		clone.resources[resource] = cloneResourceSchema(definition)
	}
	for key, resource := range s.types {
		clone.types[key] = resource
	}
	return clone

}

// Clone returns an independent snapshot suitable for store construction.
func (s *Schema) Clone() (*Schema, error) {
	if s == nil {
		return nil, fmt.Errorf("schema is nil")
	}
	return s.Snapshot(), nil
}

func normalizeResourceSchema(resource string, definition ResourceSchema) (ResourceSchema, error) {
	if definition.StatusResource {
		if _, dynamic := definition.Object.(*Unstructured); !dynamic {
			if !hasSchemaField(reflect.TypeOf(definition.Object), []string{"status"}) {
				return ResourceSchema{}, fmt.Errorf("resource %q declares status but has no status field", resource)
			}
		}
	}
	definition.ScopeKeys = append([]string(nil), definition.ScopeKeys...)
	seenScopeKeys := map[string]struct{}{}
	for i, field := range definition.ScopeKeys {
		field = strings.TrimSpace(field)
		if field == "" {
			return ResourceSchema{}, fmt.Errorf("resource %q has an empty scope key", resource)
		}
		if !fieldPathPattern.MatchString(field) {
			return ResourceSchema{}, fmt.Errorf("resource %q has invalid scope key %q", resource, field)
		}
		if _, exists := seenScopeKeys[field]; exists {
			return ResourceSchema{}, fmt.Errorf("resource %q has duplicate scope key %q", resource, field)
		}
		seenScopeKeys[field] = struct{}{}
		definition.ScopeKeys[i] = field
	}

	definition.FieldSpecs = append([]FieldSpec(nil), definition.FieldSpecs...)
	seenFieldSpecs := map[string]struct{}{}
	for i, spec := range definition.FieldSpecs {
		spec.Name = strings.TrimSpace(spec.Name)
		if !fieldPathPattern.MatchString(spec.Name) {
			return ResourceSchema{}, fmt.Errorf("resource %q has invalid field spec name %q", resource, spec.Name)
		}
		if spec.Size < 0 {
			return ResourceSchema{}, fmt.Errorf("resource %q field spec %q has a negative size", resource, spec.Name)
		}
		if _, exists := seenFieldSpecs[spec.Name]; exists {
			return ResourceSchema{}, fmt.Errorf("resource %q has duplicate field spec %q", resource, spec.Name)
		}
		seenFieldSpecs[spec.Name] = struct{}{}
		definition.FieldSpecs[i] = spec
	}

	indexes := make([]Index, 0, len(definition.Indexes)+1)
	indexes = append(indexes, Index{Fields: []string{"id"}, Unique: true})
	indexes = append(indexes, definition.Indexes...)
	seenNames := map[string]struct{}{}
	normalized := make([]Index, 0, len(indexes))
	for _, index := range indexes {
		index.Fields = append([]string(nil), index.Fields...)
		if index.Nullable && !index.Unique {
			return ResourceSchema{}, fmt.Errorf("resource %q index %q is nullable but not unique", resource, index.Name)
		}
		if len(index.Fields) == 0 {
			return ResourceSchema{}, fmt.Errorf("resource %q index %q has no fields", resource, index.Name)
		}
		seenFields := map[string]struct{}{}
		for i, field := range index.Fields {
			field = strings.TrimSpace(field)
			if field == "" {
				return ResourceSchema{}, fmt.Errorf("resource %q index %q has an empty field", resource, index.Name)
			}
			if !fieldPathPattern.MatchString(field) {
				return ResourceSchema{}, fmt.Errorf("resource %q index %q has invalid field %q", resource, index.Name, field)
			}
			if _, exists := seenFields[field]; exists {
				return ResourceSchema{}, fmt.Errorf("resource %q index %q has duplicate field %q", resource, index.Name, field)
			}
			seenFields[field] = struct{}{}
			index.Fields[i] = field
		}
		for _, scopeKey := range definition.ScopeKeys {
			if _, exists := seenFields[scopeKey]; !exists {
				index.Fields = append(index.Fields, scopeKey)
				seenFields[scopeKey] = struct{}{}
			}
		}
		if index.Name == "" {
			index.Name = strings.ReplaceAll(strings.Join(index.Fields, "_"), ".", "_")
		}
		if !resourceOrIndexNamePattern.MatchString(index.Name) {
			return ResourceSchema{}, fmt.Errorf("resource %q has invalid index name %q", resource, index.Name)
		}
		if _, exists := seenNames[index.Name]; exists {
			// An explicit primary index replaces the implicit one.
			replaced := false
			for i := range normalized {
				if slices.Equal(normalized[i].Fields, index.Fields) && index.Unique && !index.Nullable && definition.IsPrimaryIndex(normalized[i]) {
					normalized[i] = index
					replaced = true
					break
				}
			}
			if replaced {
				continue
			}
			return ResourceSchema{}, fmt.Errorf("resource %q has duplicate index name %q", resource, index.Name)
		}
		seenNames[index.Name] = struct{}{}
		normalized = append(normalized, index)
	}
	definition.Indexes = normalized
	return definition, nil
}

func hasSchemaField(t reflect.Type, path []string) bool {
	if len(path) == 0 {
		return true
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t.Kind() == reflect.Map {
		return hasSchemaField(t.Elem(), path[1:])
	}
	if t.Kind() == reflect.Interface {
		return true
	}
	if t.Kind() != reflect.Struct {
		return false
	}
	for i := range t.NumField() {
		field := t.Field(i)
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if name == "-" || !field.IsExported() {
			continue
		}
		if field.Anonymous && name == "" {
			if hasSchemaField(field.Type, path) {
				return true
			}
			continue
		}
		if name == "" {
			name = field.Name
		}
		if name == path[0] {
			return hasSchemaField(field.Type, path[1:])
		}
	}
	return false
}

func cloneResourceSchema(definition ResourceSchema) ResourceSchema {
	clone := ResourceSchema{
		Object:         definition.Object,
		ResourceName:   definition.ResourceName,
		StatusResource: definition.StatusResource,
		ScopeKeys:      append([]string(nil), definition.ScopeKeys...),
		FieldSpecs:     append([]FieldSpec(nil), definition.FieldSpecs...),
		Indexes:        make([]Index, len(definition.Indexes)),
	}
	for i, index := range definition.Indexes {
		clone.Indexes[i] = index
		clone.Indexes[i].Fields = append([]string(nil), index.Fields...)
	}
	return clone
}
