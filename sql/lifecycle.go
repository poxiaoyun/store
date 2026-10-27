package sql

import (
	"slices"

	"github.com/google/uuid"
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

var serverOwnedUpdateFields = []string{
	"id",
	"uid",
	"resource",
	"scopes",
	"resourceVersion",
	"creationTimestamp",
	"deletionTimestamp",
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
