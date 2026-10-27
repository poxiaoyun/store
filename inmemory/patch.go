package inmemory

import (
	"bytes"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"math"
	"net/http"
	"reflect"
	"strings"

	jsonpatch "github.com/evanphx/json-patch"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
)

// MaxJSONPatchOperations bounds one JSON Patch request.
const MaxJSONPatchOperations = 10000

// ApplyPatch applies patch to the current object. A non-zero resourceVersion
// in a merge patch is a precondition and must match the current version.
func ApplyPatch(current store.Object, request store.Object, patch store.Patch) error {
	patchData, err := patch.Data(request)
	if err != nil {
		return errors.NewBadRequest(err.Error())
	}
	switch patch.Type() {
	case store.PatchTypeJSONPatch:
		return JsonPatchObject(current, patchData)
	case store.PatchTypeMergePatch:
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
	case store.PatchTypeFieldPatch:
		return FieldPatchObject(current, patchData)
	default:
		return fmt.Errorf("unsupported patch type: %s", patch.Type())
	}
}

// FieldPatchObject applies a field patch to obj.
func FieldPatchObject(obj store.Object, patch []byte) error {
	operations, err := DecodeFieldPatch(patch)
	if err != nil {
		return err
	}
	return applyFieldPatchObject(obj, operations)
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

// MergePatchFrom creates a merge patch that turns obj into the later state.
// DecodeFieldPatch decodes and validates one field patch.
func DecodeFieldPatch(data []byte) ([]store.FieldOperation, error) {
	if !json.Valid(data) {
		return nil, errors.NewBadRequest("invalid field patch data")
	}
	var patch store.FieldPatch
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&patch); err != nil {
		return nil, errors.NewBadRequest("invalid field patch data")
	}
	if len(patch.Operations) == 0 {
		return nil, errors.NewBadRequest("field patch has no operations")
	}
	for _, operation := range patch.Operations {
		switch operation.Op {
		case store.FieldOpSet, store.FieldOpUnset:
		case store.FieldOpInc:
			if _, _, _, ok := parseFieldNumber(operation.Value); !ok {
				return nil, errors.NewBadRequest("inc value must be a number")
			}
		case store.FieldOpExpr:
			return nil, errors.NewUnsupported("field patch expression is not supported")
		default:
			return nil, errors.NewBadRequest(fmt.Sprintf("invalid field patch op: %s", operation.Op))
		}
		if !validFieldPath(operation.Path) {
			return nil, errors.NewBadRequest(fmt.Sprintf("invalid field patch path: %s", operation.Path))
		}
	}
	return patch.Operations, nil
}

func validFieldPath(path string) bool {
	if path == "" {
		return false
	}
	for _, segment := range strings.Split(path, ".") {
		if segment == "" || strings.HasPrefix(segment, "$") {
			return false
		}
	}
	return true
}

func applyFieldPatchObject(current store.Object, operations []store.FieldOperation) error {
	object, err := store.ObjectToMap(current)
	if err != nil {
		return err
	}
	for _, operation := range operations {
		path := strings.Split(operation.Path, ".")
		switch operation.Op {
		case store.FieldOpSet:
			err = setFieldPath(object, path, operation.Value)
		case store.FieldOpUnset:
			err = unsetFieldPath(object, path)
		case store.FieldOpInc:
			err = incrementFieldPath(object, path, operation.Value)
		}
		if err != nil {
			return err
		}
	}
	data, err := json.Marshal(object)
	if err != nil {
		return err
	}
	store.ResetObject(current)
	if err := json.Unmarshal(data, current); err != nil {
		return errors.NewBadRequest(err.Error())
	}
	return nil
}

func setFieldPath(object map[string]any, path []string, value any) error {
	parent, _, err := fieldPathParent(object, path, true)
	if err != nil {
		return err
	}
	parent[path[len(path)-1]] = value
	return nil
}

func unsetFieldPath(object map[string]any, path []string) error {
	parent, found, err := fieldPathParent(object, path, false)
	if err != nil || !found {
		return err
	}
	delete(parent, path[len(path)-1])
	return nil
}

func incrementFieldPath(object map[string]any, path []string, delta any) error {
	parent, _, err := fieldPathParent(object, path, true)
	if err != nil {
		return err
	}
	key := path[len(path)-1]
	current, exists := parent[key]
	if !exists {
		parent[key] = delta
		return nil
	}
	incremented, err := addFieldNumbers(current, delta)
	if err != nil {
		return err
	}
	parent[key] = incremented
	return nil
}

func fieldPathParent(object map[string]any, path []string, create bool) (map[string]any, bool, error) {
	current := object
	for _, segment := range path[:len(path)-1] {
		next, exists := current[segment]
		if !exists {
			if !create {
				return nil, false, nil
			}
			child := map[string]any{}
			current[segment] = child
			current = child
			continue
		}
		child, ok := next.(map[string]any)
		if !ok {
			if !create {
				return nil, false, nil
			}
			return nil, false, errors.NewBadRequest(fmt.Sprintf("field %q is not an object", segment))
		}
		current = child
	}
	return current, true, nil
}

func addFieldNumbers(current, delta any) (any, error) {
	currentInteger, currentFloat, currentIsInteger, ok := parseFieldNumber(current)
	if !ok {
		return nil, errors.NewBadRequest("cannot increment a non-numeric field")
	}
	deltaInteger, deltaFloat, deltaIsInteger, ok := parseFieldNumber(delta)
	if !ok {
		return nil, errors.NewBadRequest("increment value must be a number")
	}
	if currentIsInteger && deltaIsInteger {
		return currentInteger + deltaInteger, nil
	}
	if currentIsInteger {
		return float64(currentInteger) + deltaFloat, nil
	}
	if deltaIsInteger {
		return currentFloat + float64(deltaInteger), nil
	}
	return currentFloat + deltaFloat, nil
}

func parseFieldNumber(value any) (int64, float64, bool, bool) {
	if value == nil {
		return 0, 0, false, false
	}
	if number, ok := value.(json.Number); ok {
		if integer, err := number.Int64(); err == nil {
			return integer, 0, true, true
		}
		floating, err := number.Float64()
		if err != nil || math.IsNaN(floating) || math.IsInf(floating, 0) {
			return 0, 0, false, false
		}
		return 0, floating, false, true
	}
	ref := reflect.ValueOf(value)
	switch ref.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return ref.Int(), 0, true, true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		unsigned := ref.Uint()
		if unsigned > math.MaxInt64 {
			return 0, 0, false, false
		}
		return int64(unsigned), 0, true, true
	case reflect.Float32, reflect.Float64:
		floating := ref.Float()
		if math.IsNaN(floating) || math.IsInf(floating, 0) {
			return 0, 0, false, false
		}
		return 0, floating, false, true
	default:
		return 0, 0, false, false
	}
}
