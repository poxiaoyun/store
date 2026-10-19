package sql

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"

	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
)

// mergeValues preserves complete document patches so empty objects and null
// retain their RFC 7396 meaning. SQL supports flat document merges; nested
// object merges and JSON Patch are explicitly outside this adapter's contract.
func mergeValues(patch store.Patch, obj store.Object) (map[string]any, error) {
	if patch.Type() != store.PatchTypeMergePatch {
		return nil, errors.NewUnsupported("SQL store supports merge patches only")
	}
	data, err := patch.Data(obj)
	if err != nil {
		return nil, err
	}
	var values map[string]any
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&values); err != nil {
		return nil, errors.NewBadRequest(err.Error())
	}
	if values == nil {
		return nil, errors.NewUnsupported("SQL merge patch must be an object")
	}
	// Decode a second time to reject trailing JSON rather than executing a prefix.
	if !json.Valid(data) {
		return nil, errors.NewBadRequest("invalid merge patch")
	}
	for _, value := range values {
		if document, ok := value.(map[string]any); ok {
			for _, child := range document {
				if _, nested := child.(map[string]any); nested {
					return nil, errors.NewUnsupported("SQL store does not support nested object merges")
				}
			}
		}
	}
	return values, nil
}

func documentType(t reflect.Type) bool {
	t = DerefType(t)
	switch t.Kind() {
	case reflect.Map, reflect.Struct, reflect.Interface, reflect.Slice, reflect.Array:
		return !IsTimeType(t)
	default:
		return false
	}
}

// columnValue is normalized once before SQL rendering. Each occurrence binds
// its own parameters because MySQL uses positional placeholders.
type columnValue struct {
	value    any
	document bool
	merge    map[string]any
}

func (s *Storage) writeValue(q *QueryState, name string, value columnValue) string {
	if value.merge != nil {
		return s.core.dialect.MergeDocument(q, s.core.dialect.Quote(name), value.merge)
	}
	if value.value == nil {
		return "NULL"
	}
	bound := q.Bind(value.value)
	if value.document {
		return s.core.dialect.JSONValue(bound)
	}
	return bound
}

type writePlan struct {
	assignments       []string
	finalizers        []string
	changesFinalizers bool
}

func (s *Storage) planWrite(q *QueryState, obj store.Object, values map[string]any, merge, status bool) (writePlan, error) {
	resource, err := store.GetResource(obj)
	if err != nil {
		return writePlan{}, err
	}
	definition, err := s.core.schema.Resource(resource)
	if err != nil {
		return writePlan{}, err
	}
	fields, err := s.core.codec.ModelFields(definition.Object)
	if err != nil {
		return writePlan{}, err
	}
	types := map[string]ModelField{}
	for _, field := range fields {
		types[field.Name] = field
	}
	if _, ok := types["status"]; status && !ok {
		return writePlan{}, errors.NewUnsupported("resource has no status")
	}
	owned := append(store.ServerOwnedUpdateFields(), "generation")
	for _, scope := range s.scopes {
		owned = append(owned, store.ScopeResourceToFieldName(scope.Resource))
	}
	plan := writePlan{}
	writable := map[string]columnValue{}
	for name, value := range values {
		if slices.Contains(owned, name) || status && name != "status" || !status && name == "status" {
			continue
		}
		if _, ok := types[name]; !ok {
			return writePlan{}, errors.NewBadRequest(fmt.Sprintf("unknown field %q", name))
		}
		field := types[name]
		typed := reflect.ValueOf(value)
		var data []byte
		if merge {
			// Patch values arrive as JSON. Update already decoded the registered
			// model, so only Patch needs field-level type conversion here.
			var err error
			data, err = json.Marshal(value)
			if err != nil {
				return writePlan{}, err
			}
			target := reflect.New(field.Type)
			if err := json.Unmarshal(data, target.Interface()); err != nil {
				return writePlan{}, errors.NewBadRequest(err.Error())
			}
			typed = target.Elem()
		}
		if name == "finalizers" {
			plan.finalizers = typed.Interface().([]string)
			plan.changesFinalizers = true
		}
		normalized := columnValue{document: documentType(field.Type)}
		switch {
		case value == nil:
			// Removing a non-nullable scalar restores its zero value.
			if field.Type.Kind() != reflect.Pointer && !normalized.document && !IsTimeType(field.Type) {
				normalized.value = ToDriverValue(reflect.Zero(field.Type))
			}
		case normalized.document:
			if !merge {
				data, err = json.Marshal(value)
				if err != nil {
					return writePlan{}, err
				}
			}
			normalized.value = string(data)
			if document, ok := value.(map[string]any); merge && ok {
				normalized.merge = document
			}
		default:
			normalized.value = ToDriverValue(typed)
		}
		writable[name] = normalized
	}
	changes := []string{}
	if !status {
		for _, name := range definition.GenerationFields {
			value, exists := writable[name]
			if !exists {
				continue
			}
			left := s.core.dialect.Quote(name)
			if types[name].Type.Kind() != reflect.Pointer && !IsTimeType(types[name].Type) {
				left = "COALESCE(" + left + ", " + q.Bind(ToDriverValue(reflect.Zero(types[name].Type))) + ")"
			}
			rendered := s.writeValue(q, name, value)
			changes = append(changes, s.core.dialect.Distinct(left, rendered))
		}
	}
	assignments := []string{}
	if len(changes) > 0 {
		generation := s.core.dialect.Quote("generation")
		assignments = append(assignments, generation+" = "+generation+" + CASE WHEN "+strings.Join(changes, " OR ")+" THEN 1 ELSE 0 END")
	}
	for _, name := range sortedKeys(writable) {
		rendered := s.writeValue(q, name, writable[name])
		assignments = append(assignments, s.core.dialect.Quote(name)+" = "+rendered)
	}
	version := s.core.dialect.Quote("resourceVersion")
	plan.assignments = append(assignments, version+" = "+version+" + 1")
	return plan, nil
}
