package store

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/meta"
)

// PrepareObjectForCreate applies the server-owned metadata shared by Store
// implementations. The backend sets ResourceVersion after persistence.
func PrepareObjectForCreate(obj Object, resource string, scopes []Scope) {
	if obj.GetID() == "" {
		obj.SetID(uuid.NewString())
	}
	obj.SetUID(uuid.NewString())
	obj.SetResourceVersion(0)
	obj.SetGeneration(1)
	obj.SetCreationTimestamp(ServerTimestampNow())
	obj.SetDeletionTimestamp(nil)
	obj.SetResource(resource)
	obj.SetScopes(scopes)
}

// ValidatePreconditions checks exact object identity and version for Get, Patch or Delete.
func ValidatePreconditions(obj Object, preconditions *Preconditions) error {
	if preconditions == nil {
		return nil
	}
	if preconditions.UID != nil && obj.GetUID() != *preconditions.UID {
		return errors.NewConflict(obj.GetResource(), obj.GetID(), fmt.Errorf("UID does not match"))
	}
	if preconditions.ResourceVersion != nil && obj.GetResourceVersion() != *preconditions.ResourceVersion {
		return errors.NewConflict(obj.GetResource(), obj.GetID(), fmt.Errorf("resourceVersion does not match"))
	}
	return nil
}

// WriteError maps an unmatched conditional mutation to Conflict. Read operations
// retain NotFound; adapters call this only at a single-object write boundary.
func WriteError(err error, obj Object, conditional bool) error {
	if !conditional || !errors.IsNotFound(err) {
		return err
	}
	resource, resourceErr := GetResource(obj)
	if resourceErr != nil {
		return resourceErr
	}
	return errors.NewConflict(resource, obj.GetID(), fmt.Errorf("write conditions did not match"))
}

// PatchWriteError includes conditions embedded in a patch when classifying an
// unmatched write. Successful writes never decode the patch again.
func PatchWriteError(err error, obj Object, patch Patch, options PatchOptions) error {
	if !errors.IsNotFound(err) {
		return err
	}
	decoded, decodeErr := DecodePatch(patch, obj)
	if decodeErr != nil {
		return decodeErr
	}
	conditional := decoded.Version != 0 || len(decoded.Tests) > 0 || len(options.LabelRequirements)+len(options.FieldRequirements) > 0 || options.Preconditions != nil && (options.Preconditions.UID != nil || options.Preconditions.ResourceVersion != nil)
	return WriteError(err, obj, conditional)
}

// ValidateDeleteRequirements checks caller-supplied object selectors.
func ValidateDeleteRequirements(obj Object, labelRequirements, fieldRequirements Requirements) error {
	if err := ValidateSelectorRequirements(labelRequirements, fieldRequirements); err != nil {
		return errors.NewBadRequest(err.Error())
	}
	unstructured, err := ToUnstructured(obj)
	if err != nil {
		return err
	}
	if !MatchLabelReqirements(obj, labelRequirements) ||
		!MatchUnstructuredFieldRequirments(unstructured, fieldRequirements) {
		return errors.NewNotFound(obj.GetResource(), obj.GetID())
	}
	return nil
}

// PrepareObjectForUpdate applies replacement or status-subresource semantics
// to desired. It returns true when removing the final finalizer completes a
// previously requested deletion.
func (s *Schema) PrepareObjectForUpdate(current, desired Object, status bool) (bool, error) {
	resource, err := GetResource(current)
	if err != nil {
		return false, err
	}
	definition, err := s.Resource(resource)
	if err != nil {
		return false, err
	}
	if !status && current.GetDeletionTimestamp() != nil {
		for _, finalizer := range desired.GetFinalizers() {
			if !slices.Contains(current.GetFinalizers(), finalizer) {
				return false, errors.NewBadRequest("cannot add finalizers to a terminating object")
			}
		}
	}
	currentMap, err := ObjectToMap(current)
	if err != nil {
		return false, err
	}
	desiredMap, err := ObjectToMap(desired)
	if err != nil {
		return false, err
	}
	result := desiredMap
	if status {
		result = currentMap
		if value, exists := desiredMap["status"]; exists {
			result["status"] = value
		} else {
			delete(result, "status")
		}
	} else {
		if value, exists := currentMap["status"]; exists {
			result["status"] = value
		} else {
			delete(result, "status")
		}
		for _, field := range serverOwnedUpdateFields {
			if value, exists := currentMap[field]; exists {
				result[field] = value
			} else {
				delete(result, field)
			}
		}
		generation := current.GetGeneration()
		for _, field := range definition.GenerationFields {
			if !objectJSONValuesEqual(generationField(currentMap, field), generationField(desiredMap, field)) {
				generation++
				break
			}
		}
		result["generation"] = generation
	}
	data, err := json.Marshal(result)
	if err != nil {
		return false, err
	}
	ResetObject(desired)
	if err := json.Unmarshal(data, desired); err != nil {
		return false, err
	}
	return current.GetDeletionTimestamp() != nil && len(desired.GetFinalizers()) == 0, nil
}

func generationField(object map[string]any, field string) any {
	var value any = object
	for part := range strings.SplitSeq(field, ".") {
		fields, ok := value.(map[string]any)
		if !ok {
			return nil
		}
		value = fields[part]
	}
	return value
}

// PrepareObjectForDelete applies propagation finalizers and the immutable
// deletion timestamp. It returns true when the object can be deleted now.
func PrepareObjectForDelete(obj Object, policy DeletionPropagation) bool {
	if obj.GetDeletionTimestamp() != nil {
		return len(obj.GetFinalizers()) == 0
	}
	finalizers := obj.GetFinalizers()
	switch policy {
	case DeletePropagationForeground:
		if !slices.Contains(finalizers, FinalizerDeleteDependents) {
			finalizers = append(finalizers, FinalizerDeleteDependents)
		}
	case DeletePropagationOrphan:
		if !slices.Contains(finalizers, FinalizerOrphanDependents) {
			finalizers = append(finalizers, FinalizerOrphanDependents)
		}
	}
	obj.SetFinalizers(finalizers)
	if len(finalizers) == 0 {
		return true
	}
	if obj.GetDeletionTimestamp() == nil {
		deletionTimestamp := ServerTimestampNow()
		obj.SetDeletionTimestamp(&deletionTimestamp)
	}
	return false
}

// CopyObject copies the JSON representation of source into target.
func CopyObject(source, target Object) error {
	data, err := json.Marshal(source)
	if err != nil {
		return err
	}
	ResetObject(target)
	return json.Unmarshal(data, target)
}

// ResetObject clears target before decoding a complete replacement into it.
func ResetObject(target Object) {
	value := reflect.ValueOf(target).
		Elem()
	value.Set(reflect.Zero(value.Type()))
}

// ServerTimestampNow returns the JSON precision shared by Store backends.
func ServerTimestampNow() meta.Time {
	now := time.Now().
		UTC().
		Truncate(time.Second)
	return meta.Time{Time: now}
}

// ObjectToMap returns the JSON object representation of obj. JSON numbers use
// json.Number so subsequent merging and decoding cannot round integer values.
func ObjectToMap(obj Object) (map[string]any, error) {
	data, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	result := map[string]any{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

// ServerOwnedUpdateFields returns the fields a write keeps from the stored
// object instead of taking from the caller. A backend that renders a write
// without reading the object omits exactly these fields from the write.
func ServerOwnedUpdateFields() []string {
	return slices.Clone(serverOwnedUpdateFields)
}

func objectJSONValuesEqual(left, right any) bool {
	switch left := left.(type) {
	case json.Number:
		right, ok := right.(json.Number)
		if !ok {
			return false
		}
		leftValue, _ := new(big.Rat).
			SetString(string(left))
		rightValue, _ := new(big.Rat).
			SetString(string(right))
		return leftValue.Cmp(rightValue) == 0
	case map[string]any:
		right, ok := right.(map[string]any)
		if !ok || len(left) != len(right) {
			return false
		}
		for key, leftValue := range left {
			rightValue, exists := right[key]
			if !exists || !objectJSONValuesEqual(leftValue, rightValue) {
				return false
			}
		}
		return true
	case []any:
		right, ok := right.([]any)
		if !ok || len(left) != len(right) {
			return false
		}
		for index := range left {
			if !objectJSONValuesEqual(left[index], right[index]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(left, right)
	}
}

var serverOwnedUpdateFields = []string{
	"id",
	"uid",
	"resource",
	"scopes",
	"resourceVersion",
	"creationTimestamp",
	"deletionTimestamp",
}

var objectMetaFields = []string{
	"id",
	"name",
	"uid",
	"apiVersion",
	"scopes",
	"resource",
	"resourceVersion",
	"generation",
	"creationTimestamp",
	"deletionTimestamp",
	"labels",
	"annotations",
	"finalizers",
	"ownerReferences",
	"description",
}
