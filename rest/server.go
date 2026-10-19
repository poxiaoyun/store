package rest

import (
	"context"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/log"
	"xiaoshiai.cn/common/rest/api"
	"xiaoshiai.cn/store"
)

type Server struct {
	Store store.Store
}

func NewServer(store store.Store) *Server {
	return &Server{Store: store}
}

func (s *Server) Ping(w http.ResponseWriter, r *http.Request) {
	api.On(w, r, func(ctx context.Context) (any, error) {
		pinger, ok := s.Store.(store.Pinger)
		if !ok {
			return nil, errors.NewNotImplemented("store does not support ping")
		}
		return nil, pinger.Ping(ctx)
	})
}

type CountResponse struct {
	Count int `json:"count"`
}

// listResponse is the List payload: the Store read result plus the protocol
// echo of the caller's pagination request. The Store owns only the read result,
// so the adapter restores the protocol fields from the request.
type listResponse struct {
	store.List[store.Unstructured]
	Page  int `json:"page,omitempty"`
	Size  int `json:"size,omitempty"`
	Limit int `json:"limit,omitempty"`
}

func newListResponse(r *http.Request, list store.List[store.Unstructured]) listResponse {
	response := listResponse{List: list}
	if size := api.Query(r, "size", 0); size > 0 {
		response.Size = size
		response.Page = max(api.Query(r, "page", 0), 1)
	}
	response.Limit = api.Query(r, "limit", 0)
	return response
}

func (s *Server) List(w http.ResponseWriter, r *http.Request) {
	s.on(w, r, func(ctx context.Context, ref store.ResourcedObjectReference) (any, error) {
		log := log.FromContext(ctx)
		if ref.ID == "" {
			listOptions, err := ListOptionsFromRequest(r)
			if err != nil {
				return nil, err
			}
			if api.Query(r, "includeSubscopes", false) {
				listOptions = append(listOptions, store.WithSubScopes())
			}
			// offset is the store protocol's window position; page and size
			// above are the caller-facing form of the same window.
			if offset := api.Query(r, "offset", 0); offset > 0 {
				listOptions = append(listOptions, store.WithOffset(offset))
			}
			if resourceVersion := api.Query(r, "resourceVersion", ""); resourceVersion != "" {
				parsed, err := strconv.ParseInt(resourceVersion, 10, 64)
				if err != nil {
					return nil, errors.NewBadRequest("resourceVersion must be an integer")
				}
				listOptions = append(listOptions, store.WithResourceVersion(parsed))
			}
			if fields := api.Query(r, "fields", ""); fields != "" {
				listOptions = append(listOptions, store.WithFields(strings.Split(fields, ",")...))
			}
			if fields := api.Query(r, "searchFields", ""); fields != "" {
				listOptions = append(listOptions, store.WithSearchFields(strings.Split(fields, ",")...))
			}
			list := store.List[store.Unstructured]{}
			list.Resource = ref.Resource

			// count
			if count := api.Query(r, "count", false); count {
				options := store.ApplyListOptions(listOptions)
				obj := &store.Unstructured{}
				obj.SetResource(ref.Resource)
				countOptions := []store.CountOption{
					store.WithSearch(options.Search), store.WithSearchFields(options.SearchFields...),
					store.WithLabelRequirements(options.LabelRequirements...),
					store.WithFieldRequirements(options.FieldRequirements...),
				}
				if options.IncludeSubScopes {
					countOptions = append(countOptions, store.WithSubScopes())
				}
				count, err := s.Store.Scope(ref.Scopes...).
					Count(ctx, obj, countOptions...)
				if err != nil {
					return nil, err
				}
				return CountResponse{Count: count}, nil
			}
			// watch
			if watch := api.Query(r, "watch", false); watch {
				options := store.ApplyListOptions(listOptions)
				watchOptions := watchOptionsFromListOptions(options)
				if api.Query(r, "sendInitialEvents", false) {
					watchOptions = append(watchOptions, store.WithSendInitialEvents())
				}
				watcher, err := s.Store.Scope(ref.Scopes...).
					Watch(ctx, &list, watchOptions...)
				if err != nil {
					return nil, err
				}
				defer watcher.Stop()

				ssew := api.NewSSEWriter[any](w)
				for {
					select {
					case <-ctx.Done():
						return nil, nil
					case event, ok := <-watcher.Events():
						if !ok {
							ssew.Encode("error", fmt.Errorf("watcher closed"))
							return nil, nil
						}
						if event.Error != nil {
							ssew.Encode("error", event.Error)
							return nil, nil
						}
						if err := ssew.Encode(string(event.Type), event.Object); err != nil {
							log.Error(err, "write event")
							return nil, nil
						}
					}
				}
			}
			// list
			if err := s.Store.Scope(ref.Scopes...).
				List(ctx, &list, listOptions...); err != nil {
				return nil, err
			}
			return newListResponse(r, list), nil

		} else {
			// get
			obj := &store.Unstructured{}
			obj.SetResource(ref.Resource)
			var options []store.GetOption
			labelRequirements, fieldRequirements, err := decodeSelector(r)
			if err != nil {
				return nil, err
			}
			if len(labelRequirements) != 0 {
				options = append(options, store.WithLabelRequirements(labelRequirements...))
			}
			if len(fieldRequirements) != 0 {
				options = append(options, store.WithFieldRequirements(fieldRequirements...))
			}
			if resourceVersion := api.Query(r, "resourceVersion", ""); resourceVersion != "" {
				parsed, err := strconv.ParseInt(resourceVersion, 10, 64)
				if err != nil {
					return nil, errors.NewBadRequest("resourceVersion must be an integer")
				}
				options = append(options, store.WithResourceVersion(parsed))
			}
			preconditions, err := decodePreconditions(r)
			if err != nil {
				return nil, err
			}
			options = append(options, store.WithPreconditions(preconditions))
			if fields := api.Query(r, "fields", ""); fields != "" {
				options = append(options, store.WithFields(strings.Split(fields, ",")...))
			}
			if err := s.Store.Scope(ref.Scopes...).
				Get(ctx, ref.ID, obj, options...); err != nil {
				return nil, err
			}
			return obj, nil

		}
	})
}

func (s *Server) Create(w http.ResponseWriter, r *http.Request) {
	s.on(w, r, func(ctx context.Context, ref store.ResourcedObjectReference) (any, error) {
		obj := &store.Unstructured{}
		if err := api.Body(r, obj); err != nil {
			return nil, err
		}
		obj.SetResource(ref.Resource)

		options := []store.CreateOption{store.WithTTL(api.Query(r, "ttl", time.Duration(0)))}
		if api.Query(r, "dryRun", false) {
			options = append(options, store.WithDryRun())
		}
		if err := s.Store.Scope(ref.Scopes...).
			Create(ctx, obj, options...); err != nil {
			return nil, err
		}
		return obj, nil
	})
}

const PatchDataLimit = 5 * 1024 * 1024 // 5MB

func (s *Server) Patch(w http.ResponseWriter, r *http.Request) {
	s.on(w, r, func(ctx context.Context, ref store.ResourcedObjectReference) (any, error) {
		patchtype, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil {
			return nil, fmt.Errorf("invalid content type: %s", err)
		}
		labelsel, fildsel, err := decodeSelector(r)
		if err != nil {
			return nil, err
		}
		patchdata, err := io.ReadAll(io.LimitReader(r.Body, PatchDataLimit))
		if err != nil {
			return nil, err
		}

		if ref.ID == "" {
			// batch patch
			batchPatch := store.RawPatchBatch(store.PatchType(patchtype), patchdata)
			opts := []store.PatchBatchOption{
				store.WithLabelRequirements(labelsel...),
				store.WithFieldRequirements(fildsel...),
			}
			if api.Query(r, "dryRun", false) {
				opts = append(opts, store.WithDryRun())
			}
			list := store.List[store.Unstructured]{}
			list.Resource = ref.Resource
			if err := s.Store.Scope(ref.Scopes...).
				PatchBatch(ctx, &list, batchPatch, opts...); err != nil {
				return nil, err
			}
			return list, nil
		}

		patch := store.RawPatch(store.PatchType(patchtype), patchdata)

		options := []store.PatchOption{
			store.WithLabelRequirements(labelsel...),
			store.WithFieldRequirements(fildsel...),
		}
		if api.Query(r, "dryRun", false) {
			options = append(options, store.WithDryRun())
		}

		preconditions, err := decodePreconditions(r)
		if err != nil {
			return nil, err
		}
		options = append(options, store.WithPreconditions(preconditions))
		obj := &store.Unstructured{}
		obj.SetResource(ref.Resource)
		obj.SetID(ref.ID)

		if status := api.Query(r, "status", false); status {
			if err := s.Store.Scope(ref.Scopes...).
				Status().
				Patch(ctx, obj, patch, options...); err != nil {
				return nil, err
			}
		} else {
			if err := s.Store.Scope(ref.Scopes...).
				Patch(ctx, obj, patch, options...); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
}

func (s *Server) Update(w http.ResponseWriter, r *http.Request) {
	s.on(w, r, func(ctx context.Context, ref store.ResourcedObjectReference) (any, error) {
		if ref.ID == "" {
			return nil, errors.NewBadRequest("id is required")
		}
		labelsel, fildsel, err := decodeSelector(r)
		if err != nil {
			return nil, err
		}
		options := []store.UpdateOption{
			store.WithTTL(api.Query(r, "ttl", time.Duration(0))),
			store.WithLabelRequirements(labelsel...),
			store.WithFieldRequirements(fildsel...),
		}
		if api.Query(r, "dryRun", false) {
			options = append(options, store.WithDryRun())
		}
		obj := &store.Unstructured{}
		if err := api.Body(r, obj); err != nil {
			return nil, err
		}
		if obj.GetID() != ref.ID {
			return nil, errors.NewBadRequest(fmt.Sprintf("id in body %s is not equal to id in path %s", obj.GetID(), ref.ID))
		}
		obj.SetResource(ref.Resource)

		if status := api.Query(r, "status", false); status {
			if err := s.Store.Scope(ref.Scopes...).
				Status().
				Update(ctx, obj, options...); err != nil {
				return nil, err
			}
		} else {
			if err := s.Store.Scope(ref.Scopes...).
				Update(ctx, obj, options...); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})
}

func (s *Server) Delete(w http.ResponseWriter, r *http.Request) {
	s.on(w, r, func(ctx context.Context, ref store.ResourcedObjectReference) (any, error) {
		if ref.ID == "" {
			// batch delete
			labelsel, fildsel, err := decodeSelector(r)
			if err != nil {
				return nil, err
			}
			list := store.List[store.Unstructured]{}
			list.Resource = ref.Resource
			options := []store.DeleteBatchOption{store.WithLabelRequirements(labelsel...), store.WithFieldRequirements(fildsel...)}
			if api.Query(r, "dryRun", false) {
				options = append(options, store.WithDryRun())
			}
			if err := s.Store.Scope(ref.Scopes...).
				DeleteBatch(ctx, &list, options...); err != nil {
				return nil, err
			}
			return list, nil
		}
		labelRequirements, fieldRequirements, err := decodeSelector(r)
		if err != nil {
			return nil, err
		}
		options := []store.DeleteOption{
			store.WithLabelRequirements(labelRequirements...),
			store.WithFieldRequirements(fieldRequirements...),
		}
		if api.Query(r, "dryRun", false) {
			options = append(options, store.WithDryRun())
		}
		uid, uidProvided := r.URL.Query()["uid"]
		resourceVersion, resourceVersionProvided := r.URL.Query()["resourceVersion"]
		if uidProvided {
			options = append(options, store.WithUID(uid[0]))
		}
		if resourceVersionProvided {
			parsed, err := strconv.ParseInt(resourceVersion[0], 10, 64)
			if err != nil {
				return nil, errors.NewBadRequest("resourceVersion must be an integer")
			}
			options = append(options, store.WithResourceVersion(parsed))
		}
		if propagationPolicy := api.Query(r, "propagationPolicy", ""); propagationPolicy != "" {
			options = append(options, store.WithPropagation(store.DeletionPropagation(propagationPolicy)))
		}
		obj := &store.Unstructured{}
		obj.SetResource(ref.Resource)
		obj.SetID(ref.ID)
		storage := s.Store.Scope(ref.Scopes...)
		if err := storage.Delete(ctx, obj, options...); err != nil {
			return nil, err
		}
		return obj, nil
	})
}

func (s *Server) on(w http.ResponseWriter, r *http.Request,
	fn func(ctx context.Context, ref store.ResourcedObjectReference) (any, error),
) {
	api.On(w, r, func(ctx context.Context) (any, error) {
		return fn(ctx, decodePath(api.Path(r, "path", "")))
	})
}

func decodeSelector(r *http.Request) (store.Requirements, store.Requirements, error) {
	modifiers, err := ListOptionsFromRequest(r)
	if err != nil {
		return nil, nil, err
	}
	options := store.ApplyListOptions(modifiers)
	return options.LabelRequirements, options.FieldRequirements, nil
}

// decodePath
// /scope/name/scope/name/resource/name
// /scope/name/scope/name/resource
// /scope/name/resource
func decodePath(rpath string) store.ResourcedObjectReference {
	rpath = strings.TrimPrefix(rpath, "/")
	rpath = strings.TrimSuffix(rpath, "/")
	parts := strings.Split(rpath, "/")

	scopes := []store.Scope{}
	// every two parts is a scope and name
	for i := 0; i < len(parts); i += 2 {
		if i+1 == len(parts) {
			scopes = append(scopes, store.Scope{Resource: parts[i]})
		} else {
			scopes = append(scopes, store.Scope{Resource: parts[i], Name: parts[i+1]})
		}
	}
	scopes, last := scopes[:len(scopes)-1], scopes[len(scopes)-1]
	return store.ResourcedObjectReference{
		Scopes:   scopes,
		Resource: last.Resource,
		ID:       last.Name,
	}
}

func (s *Server) Group() api.Group {
	return api.NewGroup("/{path...}").
		Route(
			api.HEAD("").
				To(s.Ping),
			api.GET("").
				To(s.List),
			api.POST("").
				To(s.Create),
			api.PUT("").
				To(s.Update),
			api.DELETE("").
				To(s.Delete),
			api.PATCH("").
				To(s.Patch),
		)
}

func decodePreconditions(r *http.Request) (store.Preconditions, error) {
	conditions := store.Preconditions{}
	query := r.URL.Query()
	if values, present := query["preconditionUID"]; present {
		conditions.UID = &values[0]
	}
	if values, present := query["preconditionResourceVersion"]; present {
		version, err := strconv.ParseInt(values[0], 10, 64)
		if err != nil {
			return store.Preconditions{}, errors.NewBadRequest("preconditionResourceVersion must be an integer")
		}
		conditions.ResourceVersion = &version
	}
	return conditions, nil
}
