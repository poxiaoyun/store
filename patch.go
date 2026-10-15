package store

import (
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"sort"
	"strings"

	jsonpatch "github.com/evanphx/json-patch"
	"xiaoshiai.cn/common/errors"
)

const MaxJSONPatchOperations = 10000

var _ Patch = &rawPatch{}

func RawPatch(typ PatchType, data []byte) Patch {
	return &rawPatch{typ: typ, data: data}
}

type MapMergePatch map[string]any

func (m MapMergePatch) Data(obj Object) ([]byte, error) {
	return json.Marshal(m)
}

func (m MapMergePatch) Type() PatchType {
	return PatchTypeMergePatch
}

// PatchField is one decoded field edit of a patch. An empty Path writes the
// column itself; a Path edits that path inside the document column it belongs
// to. FromPath references another field of the same object, which turns the edit
// into a copy or a move.
type PatchField struct {
	Column   string
	Path     []string
	Value    any
	Remove   bool
	FromPath []string
}

// EmptiesValue reports whether the edit leaves its field without a value: a
// removal, or an explicitly empty collection.
func (f PatchField) EmptiesValue() bool {
	if f.Remove {
		return true
	}
	values, ok := f.Value.([]any)
	return ok && len(values) == 0
}

// PatchTest is one JSON Patch "test" operation: it asserts that the stored
// object's value at Path equals Value. A test is a condition, not an edit; a
// backend checks it as part of the write, and a failing test rejects the whole
// patch instead of applying it.
type PatchTest struct {
	// Path is the stored field path the test observes, one segment per level.
	Path []string
	// Value is the value Path must equal, as decoded JSON.
	Value any
}

// DecodedPatch is the backend-independent form of one patch request.
type DecodedPatch struct {
	// Fields are the field edits in a stable order.
	Fields []PatchField
	// Tests are the JSON Patch "test" operations the write must satisfy. A merge
	// patch never produces any; a JSON Patch may produce several. A test is a
	// condition, not an edit: a failed test means the patch was computed against
	// a different object state, and is reported as invalid input rather than
	// applied.
	Tests []PatchTest
	// Version is the patch's own resourceVersion condition; a patch
	// resourceVersion is a condition, never a written value. Zero means the
	// patch carries no version condition.
	Version int64
}

// DecodePatch converts one patch into field edits, conditions and the patch's
// own version condition. A merge patch merges into its document paths, which
// keeps the keys it does not mention.
func DecodePatch(patch Patch, request Object) (DecodedPatch, error) {
	data, err := patch.Data(request)
	if err != nil {
		return DecodedPatch{}, err
	}
	var decoded DecodedPatch
	switch patch.Type() {
	case PatchTypeMergePatch:
		decoded.Fields, err = decodeMergePatchFields(data)
	case PatchTypeJSONPatch:
		decoded.Fields, decoded.Tests, err = decodeJSONPatchFields(data)
	default:
		return DecodedPatch{}, fmt.Errorf("unsupported patch type: %s", patch.Type())
	}
	if err != nil {
		return DecodedPatch{}, err
	}
	fields := make([]PatchField, 0, len(decoded.Fields))
	for _, field := range decoded.Fields {
		if field.Column == "resourceVersion" && len(field.Path) == 0 {
			version, ok := patchVersionNumber(field.Value)
			if !ok {
				return DecodedPatch{}, errors.NewBadRequest("resourceVersion must be an integer")
			}
			decoded.Version = version
			continue
		}
		fields = append(fields, field)
	}
	decoded.Fields = fields
	return decoded, nil
}

// patchVersionNumber reads an integer patch version.
func patchVersionNumber(value any) (int64, bool) {
	switch typed := value.(type) {
	case float64:
		return int64(typed), true
	case int64:
		return typed, true
	case int:
		return int64(typed), true
	case json.Number:
		number, err := typed.Int64()
		return number, err == nil
	default:
		return 0, false
	}
}

// decodeMergePatchFields decodes an RFC 7386 merge patch. A nested object merges
// into its document path, an array or scalar replaces its value, and null
// removes it.
func decodeMergePatchFields(data []byte) ([]PatchField, error) {
	var merged map[string]any
	if err := json.Unmarshal(data, &merged); err != nil {
		return nil, errors.NewBadRequest("invalid merge patch data")
	}
	fields := make([]PatchField, 0, len(merged))
	for _, name := range sortedMapKeys(merged) {
		decodeMergePatchField([]string{name}, merged[name], &fields)
	}
	return fields, nil
}

func decodeMergePatchField(path []string, value any, fields *[]PatchField) {
	if object, ok := value.(map[string]any); ok && len(object) > 0 {
		for _, name := range sortedMapKeys(object) {
			decodeMergePatchField(append(slices.Clone(path), name), object[name], fields)
		}
		return
	}
	field := PatchField{Column: path[0], Path: path[1:], Value: value}
	if value == nil {
		field.Remove = true
	}
	*fields = append(*fields, field)
}

// decodeJSONPatchFields decodes an RFC 6902 JSON patch.
func decodeJSONPatchFields(data []byte) ([]PatchField, []PatchTest, error) {
	var operations []map[string]any
	if err := json.Unmarshal(data, &operations); err != nil {
		return nil, nil, errors.NewBadRequest("invalid JSON patch data")
	}
	fields := make([]PatchField, 0, len(operations))
	tests := make([]PatchTest, 0, len(operations))
	for _, operation := range operations {
		op, _ := operation["op"].(string)
		rawpath, ok := operation["path"].(string)
		if !ok || rawpath == "" {
			return nil, nil, errors.NewBadRequest("patch operation has no path")
		}
		path, err := JSONPointerPath(rawpath)
		if err != nil {
			return nil, nil, err
		}
		if op == "test" {
			value, exists := operation["value"]
			if !exists {
				return nil, nil, errors.NewBadRequest("test operation has no value")
			}
			tests = append(tests, PatchTest{Path: path, Value: value})
			continue
		}
		switch op {
		case "add", "replace":
			fields = append(fields, PatchField{Column: path[0], Path: path[1:], Value: operation["value"]})
		case "remove":
			fields = append(fields, PatchField{Column: path[0], Path: path[1:], Remove: true})
		case "copy", "move":
			rawfrom, ok := operation["from"].(string)
			if !ok || rawfrom == "" {
				return nil, nil, errors.NewBadRequest("patch operation has no from")
			}
			from, err := JSONPointerPath(rawfrom)
			if err != nil {
				return nil, nil, err
			}
			fields = append(fields, PatchField{Column: path[0], Path: path[1:], FromPath: from})
			if op == "move" {
				fields = append(fields, PatchField{Column: from[0], Path: from[1:], Remove: true})
			}
		default:
			return nil, nil, errors.NewBadRequest(fmt.Sprintf("invalid patch op: %s", op))
		}
	}
	return fields, tests, nil
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

// sortedMapKeys returns map keys in a stable order.
func sortedMapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func JSONPointerUnescape(s string) string {
	return strings.NewReplacer("~1", "/", "~0", "~").Replace(s)
}

func JSONPointerEscape(s string) string {
	return strings.NewReplacer("~", "~0", "/", "~1").Replace(s)
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

func MergePatchFrom(obj Object) Patch {
	mergePatchFunc := func(originalJSON, modifiedJSON []byte, dataStruct any) ([]byte, error) {
		return jsonpatch.CreateMergePatch(originalJSON, modifiedJSON)
	}
	return &mergeFromPatch{patchType: PatchTypeMergePatch, createPatch: mergePatchFunc, from: obj}
}

type mergeFromPatch struct {
	patchType   PatchType
	createPatch func(originalJSON, modifiedJSON []byte, dataStruct any) ([]byte, error)
	from        Object
}

func (s *mergeFromPatch) Type() PatchType {
	return s.patchType
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

// ApplyPatch applies patch to the current object. A non-zero resourceVersion
// in a merge patch is a precondition and must match the current version.
func ApplyPatch(current Object, request Object, patch Patch) error {
	patchData, err := patch.Data(request)
	if err != nil {
		return errors.NewBadRequest(err.Error())
	}
	switch patch.Type() {
	case PatchTypeJSONPatch:
		return JsonPatchObject(current, patchData)
	case PatchTypeMergePatch:
		condition := struct {
			ResourceVersion *int64 `json:"resourceVersion"`
		}{}
		if err := json.Unmarshal(patchData, &condition); err != nil {
			return errors.NewBadRequest(err.Error())
		}
		if condition.ResourceVersion != nil && *condition.ResourceVersion != 0 && *condition.ResourceVersion != current.GetResourceVersion() {
			return errors.NewConflict(current.GetResource(), current.GetID(),
				fmt.Errorf("resourceVersion %d does not match", *condition.ResourceVersion))
		}
		return JsonMergePatchObject(current, patchData)
	default:
		return fmt.Errorf("unsupported patch type: %s", patch.Type())
	}
}

func JsonMergePatchObject(obj any, patch []byte) error {
	olddata, err := json.Marshal(obj)
	if err != nil {
		return errors.NewBadRequest(err.Error())
	}
	patchedData, err := jsonpatch.MergePatch(olddata, patch)
	if err != nil {
		return errors.NewBadRequest(err.Error())
	}
	target := reflect.ValueOf(obj).Elem()
	replacement := reflect.New(target.Type())
	if err := json.Unmarshal(patchedData, replacement.Interface()); err != nil {
		return errors.NewBadRequest(err.Error())
	}
	target.Set(replacement.Elem())
	return nil
}

func JsonPatchObject(obj any, patch []byte) error {
	olddata, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	patchObj, err := jsonpatch.DecodePatch(patch)
	if err != nil {
		return errors.NewBadRequest(err.Error())
	}
	if len(patchObj) > MaxJSONPatchOperations {
		return errors.NewRequestEntityTooLargeError(
			fmt.Sprintf("The allowed maximum operations in a JSON patch is %d, got %d",
				MaxJSONPatchOperations, len(patchObj)))
	}
	patchedData, err := patchObj.Apply(olddata)
	if err != nil {
		if stderrors.Is(err, jsonpatch.ErrTestFailed) {
			return errors.NewCustomError(http.StatusUnprocessableEntity, errors.StatusReasonInvalid, err.Error())
		}
		return err
	}
	target := reflect.ValueOf(obj).Elem()
	replacement := reflect.New(target.Type())
	if err := json.Unmarshal(patchedData, replacement.Interface()); err != nil {
		return errors.NewBadRequest(err.Error())
	}
	target.Set(replacement.Elem())
	return nil
}
