package sql

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
)

// PatchOperation is one field edit decoded from a batch patch. Merge patches and
// JSON patches both reduce to this form.
type PatchOperation struct {
	path  []string
	value any
	// remove marks a path removal, which writes NULL.
	remove bool
}

// DecodeBatchPatch converts a batch patch into ordered field edits.
func DecodeBatchPatch(patch store.PatchBatch) ([]PatchOperation, error) {
	switch patch.Type() {
	case store.PatchTypeMergePatch:
		var merged map[string]any
		if err := json.Unmarshal(patch.Data(), &merged); err != nil {
			return nil, errors.NewBadRequest("invalid merge patch data")
		}
		operations := make([]PatchOperation, 0, len(merged))
		for _, name := range sortedKeys(merged) {
			operations = append(operations, PatchOperation{path: []string{name}, value: merged[name]})
		}
		return operations, nil
	case store.PatchTypeJSONPatch:
		var patchlist []map[string]any
		if err := json.Unmarshal(patch.Data(), &patchlist); err != nil {
			return nil, errors.NewBadRequest("invalid JSON patch data")
		}
		operations := make([]PatchOperation, 0, len(patchlist))
		for _, item := range patchlist {
			operation, err := DecodeJSONPatchOperation(item)
			if err != nil {
				return nil, err
			}
			operations = append(operations, operation)
		}
		return operations, nil
	default:
		return nil, errors.NewBadRequest(fmt.Sprintf("invalid patch type: %s", patch.Type()))
	}
}

func DecodeJSONPatchOperation(item map[string]any) (PatchOperation, error) {
	rawpath, ok := item["path"].(string)
	if !ok || rawpath == "" {
		return PatchOperation{}, errors.NewBadRequest("patch operation has no path")
	}
	op, ok := item["op"].(string)
	if !ok || op == "" {
		return PatchOperation{}, errors.NewBadRequest("patch operation has no op")
	}
	path, err := JSONPointerPath(rawpath)
	if err != nil {
		return PatchOperation{}, err
	}
	for _, segment := range path {
		// Array positions have no fixed column and no document edit that both
		// engines express the same way, so they stay unsupported.
		if segment == "-" {
			return PatchOperation{}, errors.NewUnsupported("SQL store does not support array append patches")
		}
		if _, err := strconv.Atoi(segment); err == nil {
			return PatchOperation{}, errors.NewUnsupported("SQL store does not support array index patches")
		}
	}
	switch op {
	case "add", "replace":
		return PatchOperation{path: path, value: item["value"]}, nil
	case "remove":
		return PatchOperation{path: path, remove: true}, nil
	default:
		return PatchOperation{}, errors.NewBadRequest(fmt.Sprintf("invalid patch op: %s", op))
	}
}

// JSONPointerPath splits a JSON Pointer into unescaped path segments.
func JSONPointerPath(pointer string) ([]string, error) {
	trimmed := strings.TrimPrefix(pointer, "/")
	if trimmed == "" {
		return nil, errors.NewBadRequest("patch path selects the whole object")
	}
	segments := strings.Split(trimmed, "/")
	for i, segment := range segments {
		segments[i] = strings.ReplaceAll(strings.ReplaceAll(segment, "~1", "/"), "~0", "~")
	}
	return segments, nil
}

// patchAssignments renders a batch patch as SQL assignments. A top-level path
// writes its column; a nested path edits the containing JSON document column.
func (s *Storage) patchAssignments(q *QueryState, list store.ObjectList, patch store.PatchBatch, excluded map[string]struct{}) ([]string, error) {
	operations, err := DecodeBatchPatch(patch)
	if err != nil {
		return nil, err
	}
	items, err := store.GetItemsPtr(list)
	if err != nil {
		return nil, err
	}
	fields, err := s.core.codec.ModelFields(items)
	if err != nil {
		return nil, err
	}
	types := make(map[string]reflect.Type, len(fields))
	for _, field := range fields {
		types[field.Name] = field.Type
	}
	assignments := make([]string, 0, len(operations))
	for _, operation := range operations {
		head := operation.path[0]
		if _, skip := excluded[head]; skip {
			continue
		}
		columnType, ok := types[head]
		if !ok {
			return nil, errors.NewBadRequest(fmt.Sprintf("object has no field %q", head))
		}
		column := s.core.dialect.Quote(head)
		if len(operation.path) == 1 {
			// A removed path and a merge-patch null both write NULL.
			if operation.remove || operation.value == nil {
				assignments = append(assignments, column+" = NULL")
				continue
			}
			assignments = append(assignments, column+" = "+q.Bind(driverValue(operation.value)))
			continue
		}
		if !IsDocumentType(columnType) {
			return nil, errors.NewBadRequest(fmt.Sprintf("field %q is not a document", head))
		}
		if operation.remove {
			assignments = append(assignments, column+" = "+s.core.dialect.JSONRemove(column, operation.path[1:]))
			continue
		}
		assignments = append(assignments, column+" = "+s.core.dialect.JSONSet(column, operation.path[1:], q.Bind(driverValue(operation.value))))
	}
	return assignments, nil
}

// isDocumentType reports whether a field is stored as a JSON document, which is
// the only field a nested patch path can edit.
func IsDocumentType(t reflect.Type) bool {
	base := DerefType(t)
	switch base.Kind() {
	case reflect.Map, reflect.Struct, reflect.Interface:
		return !IsTimeType(base)
	case reflect.Slice, reflect.Array:
		return base.Elem().Kind() != reflect.Uint8
	default:
		return false
	}
}
