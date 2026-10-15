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
	Object    Object
	ScopeKeys []string
	Indexes   []Index
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
}

func NewSchema() *Schema {
	return &Schema{resources: map[string]ResourceSchema{}}
}

func (s *Schema) Register(obj Object, definition ResourceSchema) error {
	if s == nil {
		return fmt.Errorf("schema is nil")
	}
	if obj == nil || reflect.ValueOf(obj).Kind() == reflect.Ptr && reflect.ValueOf(obj).IsNil() {
		return fmt.Errorf("object is nil")
	}
	resource, err := GetResource(obj)
	if err != nil {
		return err
	}
	if !resourceOrIndexNamePattern.MatchString(resource) {
		return fmt.Errorf("resource name %q is invalid", resource)
	}
	definition.Object = obj
	definition, err = normalizeResourceSchema(resource, definition)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.resources[resource]; exists {
		return fmt.Errorf("resource %q already registered", resource)
	}
	s.resources[resource] = definition
	return nil
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
		object = reflect.New(typeOf.Elem()).Interface().(Object)
	} else {
		object = reflect.New(typeOf).Elem().Interface().(Object)
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

func cloneResourceSchema(definition ResourceSchema) ResourceSchema {
	clone := ResourceSchema{
		Object:     definition.Object,
		ScopeKeys:  append([]string(nil), definition.ScopeKeys...),
		FieldSpecs: append([]FieldSpec(nil), definition.FieldSpecs...),
		Indexes:    make([]Index, len(definition.Indexes)),
	}
	for i, index := range definition.Indexes {
		clone.Indexes[i] = index
		clone.Indexes[i].Fields = append([]string(nil), index.Fields...)
	}
	return clone
}
