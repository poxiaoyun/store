package etcdcache

import (
	"encoding/json"
	"math/big"
	"reflect"
	"slices"

	"github.com/google/uuid"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
)

func prepareObjectForCreate(obj store.Object, resource string, scopes []store.Scope) {
	if obj.GetID() == "" {
		obj.SetID(uuid.NewString())
	}
	obj.SetUID(uuid.NewString())
	obj.SetResourceVersion(0)
	obj.SetGeneration(1)
	obj.SetCreationTimestamp(store.ServerTimestampNow())
	obj.SetDeletionTimestamp(nil)
	obj.SetResource(resource)
	obj.SetScopes(scopes)
}

func prepareObjectForUpdate(current, desired store.Object, status bool) (bool, error) {
	if !status && current.GetDeletionTimestamp() != nil {
		for _, finalizer := range desired.GetFinalizers() {
			if !slices.Contains(current.GetFinalizers(), finalizer) {
				return false, errors.NewBadRequest("cannot add finalizers to a terminating object")
			}
		}
	}
	currentMap, err := store.ObjectToMap(current)
	if err != nil {
		return false, err
	}
	desiredMap, err := store.ObjectToMap(desired)
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
		if generationChanged(currentMap, desiredMap) {
			generation++
		}
		result["generation"] = generation
	}
	data, err := json.Marshal(result)
	if err != nil {
		return false, err
	}
	store.ResetObject(desired)
	if err := json.Unmarshal(data, desired); err != nil {
		return false, err
	}
	return current.GetDeletionTimestamp() != nil && len(desired.GetFinalizers()) == 0, nil
}

func prepareObjectForDelete(obj store.Object, policy store.DeletionPropagation) bool {
	if obj.GetDeletionTimestamp() != nil {
		return len(obj.GetFinalizers()) == 0
	}
	finalizers := obj.GetFinalizers()
	switch policy {
	case store.DeletePropagationForeground:
		if !slices.Contains(finalizers, store.FinalizerDeleteDependents) {
			finalizers = append(finalizers, store.FinalizerDeleteDependents)
		}
	case store.DeletePropagationOrphan:
		if !slices.Contains(finalizers, store.FinalizerOrphanDependents) {
			finalizers = append(finalizers, store.FinalizerOrphanDependents)
		}
	}
	obj.SetFinalizers(finalizers)
	if len(finalizers) == 0 {
		return true
	}
	if obj.GetDeletionTimestamp() == nil {
		deletionTimestamp := store.ServerTimestampNow()
		obj.SetDeletionTimestamp(&deletionTimestamp)
	}
	return false
}

// generationIgnoredFields are object metadata: changing them does not advance
// Generation. Each store owns its own generation rule.
var generationIgnoredFields = []string{
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
	"status",
}

func generationChanged(current, desired map[string]any) bool {
	for name, value := range current {
		if slices.Contains(generationIgnoredFields, name) {
			continue
		}
		if !objectJSONValuesEqual(value, desired[name]) {
			return true
		}
	}
	for name := range desired {
		if slices.Contains(generationIgnoredFields, name) {
			continue
		}
		if _, exists := current[name]; !exists {
			return true
		}
	}
	return false
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
