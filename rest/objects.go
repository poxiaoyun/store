package rest

import (
	"fmt"
	"net/http"

	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/rest/api"
	"xiaoshiai.cn/store"
)

// ListObjectsOrWatch lists objects or streams Watch events when the request
// contains watch=true.
func ListObjectsOrWatch[T store.ObjectList](w http.ResponseWriter, r *http.Request, storage store.Store, list T, options ...store.ListOption) (any, error) {
	resolved, err := listOptionsFromRequest(r, options...)
	if err != nil {
		return nil, err
	}
	if api.Query(r, "watch", false) {
		listOptions := store.ApplyListOptions(resolved)
		return nil, watchObjects(w, r, storage, list, listOptions)
	}
	return listObjects(r, storage, list, resolved)
}

// ListObjects lists typed Store objects using request query options followed by
// caller-provided options.
func ListObjects[T store.ObjectList](r *http.Request, storage store.Store, list T, options ...store.ListOption) (T, error) {
	resolved, err := listOptionsFromRequest(r, options...)
	if err != nil {
		return *new(T), err
	}
	return listObjects(r, storage, list, resolved)
}

func listOptionsFromRequest(r *http.Request, modifiers ...store.ListOption) ([]store.ListOption, error) {
	options, err := ListOptionsFromRequest(r)
	if err != nil {
		return nil, err
	}
	return append(options, modifiers...), nil
}

func listObjects[T store.ObjectList](r *http.Request, storage store.Store, list T, options []store.ListOption) (T, error) {
	if err := storage.List(r.Context(), list, options...); err != nil {
		return *new(T), err
	}
	return list, nil
}

func watchObjects[T store.ObjectList](w http.ResponseWriter, r *http.Request, storage store.Store, list T, listOptions store.ListOptions) error {
	resource, err := store.GetResource(list)
	if err != nil {
		return err
	}
	encoder, err := api.NewStreamEncoderFromRequest[any](w, r)
	if err != nil {
		return err
	}
	watchOptions := watchOptionsFromListOptions(listOptions)
	if api.Query(r, "sendInitialEvents", false) {
		watchOptions = append(watchOptions, store.WithSendInitialEvents())
	}
	unstructured := &store.List[store.Unstructured]{Resource: resource}
	watcher, err := storage.Watch(r.Context(), unstructured, watchOptions...)
	if err != nil {
		return err
	}
	defer watcher.Stop()

	for {
		select {
		case <-r.Context().Done():
			return nil
		case event, ok := <-watcher.Events():
			if !ok {
				return fmt.Errorf("watcher channel closed")
			}
			if event.Error != nil {
				return event.Error
			}
			if err := encoder.Encode(string(event.Type), event.Object); err != nil {
				return err
			}
		}
	}
}

func watchOptionsFromListOptions(options store.ListOptions) []store.WatchOption {
	var result []store.WatchOption
	if len(options.LabelRequirements) != 0 {
		result = append(result, store.WithLabelRequirements(options.LabelRequirements...))
	}
	if len(options.FieldRequirements) != 0 {
		result = append(result, store.WithFieldRequirements(options.FieldRequirements...))
	}
	if options.ResourceVersion != nil {
		result = append(result, store.WithResourceVersion(*options.ResourceVersion))
	}
	if options.IncludeSubScopes {
		result = append(result, store.WithSubScopes())
	}
	return result
}

// GetObject loads id into obj.
func GetObject(r *http.Request, storage store.Store, obj store.Object, id string, options ...store.GetOption) (any, error) {
	if id == "" {
		return nil, errors.NewBadRequest("id is required")
	}
	if err := storage.Get(r.Context(), id, obj, options...); err != nil {
		return nil, err
	}
	return obj, nil
}

// CreateObject decodes the request body into obj and creates it.
func CreateObject(r *http.Request, storage store.Store, obj store.Object, options ...store.CreateOption) (any, error) {
	if err := api.Body(r, obj); err != nil {
		return nil, err
	}
	if err := storage.Create(r.Context(), obj, options...); err != nil {
		return nil, err
	}
	return obj, nil
}

// PatchObject applies the request body as a JSON merge patch to id.
func PatchObject(r *http.Request, storage store.Store, obj store.Object, id string, options ...store.PatchOption) (any, error) {
	if id == "" {
		return nil, errors.NewBadRequest("id is required")
	}
	if objectID := obj.GetID(); objectID != "" && objectID != id {
		return nil, errors.NewBadRequest(fmt.Sprintf("id in object %s is not equal to id in path %s", objectID, id))
	}
	obj.SetID(id)
	patch := store.MapMergePatch{}
	if err := api.Body(r, &patch); err != nil {
		return nil, err
	}
	if err := storage.Patch(r.Context(), obj, patch, options...); err != nil {
		return nil, err
	}
	return obj, nil
}

// UpdateObject decodes the request body into obj, assigns an omitted ID from
// the path, and preserves its ResourceVersion for Store concurrency control.
func UpdateObject(r *http.Request, storage store.Store, obj store.Object, id string, options ...store.UpdateOption) (any, error) {
	if id == "" {
		return nil, errors.NewBadRequest("id is required")
	}
	if err := api.Body(r, obj); err != nil {
		return nil, err
	}
	if objectID := obj.GetID(); objectID != "" && objectID != id {
		return nil, errors.NewBadRequest(fmt.Sprintf("id in body %s is not equal to id in path %s", objectID, id))
	}
	obj.SetID(id)
	if err := storage.Update(r.Context(), obj, options...); err != nil {
		return nil, err
	}
	return obj, nil
}

// DeleteObject deletes id using obj as the result object.
func DeleteObject(r *http.Request, storage store.Store, obj store.Object, id string, options ...store.DeleteOption) (any, error) {
	if id == "" {
		return nil, errors.NewBadRequest("id is required")
	}
	if objectID := obj.GetID(); objectID != "" && objectID != id {
		return nil, errors.NewBadRequest(fmt.Sprintf("id in object %s is not equal to id in path %s", objectID, id))
	}
	obj.SetID(id)
	if err := storage.Delete(r.Context(), obj, options...); err != nil {
		return nil, err
	}
	return obj, nil
}
