package store

import (
	"encoding/json"
	"strings"

	jsonpatch "github.com/evanphx/json-patch"
	"xiaoshiai.cn/common/errors"
)

type MapMergePatch map[string]any

func (m MapMergePatch) Data(obj Object) ([]byte, error) {
	return json.Marshal(m)
}

func (m MapMergePatch) Type() PatchType {
	return PatchTypeMergePatch
}

// JSONPointerPath splits a JSON Pointer into unescaped path segments.
func JSONPointerPath(pointer string) ([]string, error) {
	trimmed := strings.TrimPrefix(pointer, "/")
	if trimmed == "" {
		return nil, errors.NewBadRequest("patch path selects the whole object")
	}
	segments := strings.Split(trimmed, "/")
	for i, segment := range segments {
		segments[i] = JSONPointerUnescape(segment)
	}
	return segments, nil
}

func JSONPointerUnescape(s string) string {
	return strings.NewReplacer("~1", "/", "~0", "~").Replace(s)
}

func JSONPointerEscape(s string) string {
	return strings.NewReplacer("~", "~0", "/", "~1").Replace(s)
}

var _ Patch = &rawPatch{}

func RawPatch(typ PatchType, data []byte) Patch {
	return &rawPatch{typ: typ, data: data}
}

type rawPatch struct {
	typ  PatchType
	data []byte
}

// Data implements Patch.
func (r *rawPatch) Data(obj Object) ([]byte, error) {
	return r.data, nil
}

// Type implements Patch.
func (r *rawPatch) Type() PatchType {
	return r.typ
}

func RawPatchBatch(typ PatchType, data []byte) PatchBatch {
	return rawBatchPatch{typ: typ, data: data}
}

type rawBatchPatch struct {
	typ  PatchType
	data []byte
}

func (p rawBatchPatch) Type() PatchType {
	return p.typ
}

func (p rawBatchPatch) Data() []byte {
	return p.data
}

type MapMergePatchBacth map[string]any

func (m MapMergePatchBacth) Data() []byte {
	val, _ := json.Marshal(m)
	return val
}

func (m MapMergePatchBacth) Type() PatchType {
	return PatchTypeMergePatch
}

// MergePatchFrom creates a merge patch that turns obj into the later state.
func MergePatchFrom(obj Object) Patch {
	mergePatchFunc := func(originalJSON, modifiedJSON []byte, dataStruct any) ([]byte, error) {
		return jsonpatch.CreateMergePatch(originalJSON, modifiedJSON)
	}
	return &mergeFromPatch{createPatch: mergePatchFunc, from: obj}
}

type mergeFromPatch struct {
	createPatch func(originalJSON, modifiedJSON []byte, dataStruct any) ([]byte, error)
	from        Object
}

func (s *mergeFromPatch) Type() PatchType {
	return PatchTypeMergePatch
}

func (s *mergeFromPatch) Data(obj Object) ([]byte, error) {
	originalJSON, err := json.Marshal(s.from)
	if err != nil {
		return nil, err
	}
	modifiedJSON, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	return s.createPatch(originalJSON, modifiedJSON, obj)
}

// FieldOperator identifies one relative field mutation carried by a field
// patch.
type FieldOperator string

const (
	// FieldOpSet replaces the value at Path.
	FieldOpSet FieldOperator = "set"
	// FieldOpUnset removes the value at Path. A missing path is a no-op.
	FieldOpUnset FieldOperator = "unset"
	// FieldOpInc adds Value to the numeric value at Path. A missing path takes
	// the increment as its initial value.
	FieldOpInc FieldOperator = "inc"
	// FieldOpExpr sets Path to Expr evaluated against the stored object. The
	// expression may reference the current values of other fields.
	FieldOpExpr FieldOperator = "expr"
)

// FieldOperation is one relative mutation in a FieldPatch.
type FieldOperation struct {
	Op FieldOperator `json:"op"`
	// Path is a dot-separated business field path.
	Path string `json:"path"`
	// Value is the new value for set and the increment for inc. It is ignored
	// by unset and expr and may be null for set.
	Value any `json:"value,omitempty"`
	// Expr is evaluated against the stored object to produce the new value at
	// Path when Op is expr. It is ignored by other operations.
	Expr *Expr `json:"expr,omitempty"`
}

// FieldPatch applies relative field operations to the latest stored object.
// Operations are applied in order, so a later operation observes the result of
// earlier ones. Because the operations carry deltas or expressions instead of
// values computed from a prior read, a backend can apply them in one native
// write.
type FieldPatch struct {
	// Operations are applied in order.
	Operations []FieldOperation `json:"operations"`
}

func (p FieldPatch) Type() PatchType {
	return PatchTypeFieldPatch
}

func (p FieldPatch) Data(Object) ([]byte, error) {
	return json.Marshal(p)
}
