package etcdcache

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"google.golang.org/grpc"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	k8sschema "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apiserver/pkg/registry/rest"
	"k8s.io/apiserver/pkg/storage"
	storeerr "k8s.io/apiserver/pkg/storage/errors"
	"k8s.io/utils/ptr"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/store"
)

const SetScopeFields = false

type Options struct {
	Servers       []string `json:"servers,omitempty"`
	Username      string   `json:"username,omitempty"`
	Password      string   `json:"password,omitempty" config:"password,sensitive"`
	KeyFile       string   `json:"keyFile,omitempty"`
	CertFile      string   `json:"certFile,omitempty"`
	TrustedCAFile string   `json:"trustedCAFile,omitempty"`
	KeyPrefix     string   `json:"keyPrefix,omitempty"`
}

func NewDefaultOptions() *Options {
	return &Options{
		Servers:   []string{"http://127.0.0.1:2379"},
		KeyPrefix: "/core",
	}
}

const (
	// The short keepalive timeout and interval have been chosen to aggressively
	// detect a failed etcd server without introducing much overhead.
	keepaliveTime    = 30 * time.Second
	keepaliveTimeout = 10 * time.Second

	// dialTimeout is the timeout for failing to establish a connection.
	// It is set to 20 seconds as times shorter than that will cause TLS connections to fail
	// on heavily loaded arm64 CPUs (issue #64649)
	dialTimeout = 20 * time.Second
)

func NewETCD3Client(ctx context.Context, c *Options) (*clientv3.Client, error) {
	tlsInfo := transport.TLSInfo{
		CertFile:      c.CertFile,
		KeyFile:       c.KeyFile,
		TrustedCAFile: c.TrustedCAFile,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		return nil, err
	}
	// NOTE: Client relies on nil tlsConfig
	// for non-secure connections, update the implicit variable
	if len(c.CertFile) == 0 && len(c.KeyFile) == 0 && len(c.TrustedCAFile) == 0 {
		tlsConfig = nil
	}
	dialOptions := []grpc.DialOption{}
	cfg := clientv3.Config{
		Context:              ctx,
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
		DialOptions:          dialOptions,
		Endpoints:            c.Servers,
		TLS:                  tlsConfig,
		Username:             c.Username,
		Password:             c.Password,
	}
	return clientv3.New(cfg)
}

func NewEtcdCacher(ctx context.Context, schema *store.Schema, options *Options) (*generic, error) {
	cli, err := NewETCD3Client(ctx, options)
	if err != nil {
		return nil, err
	}
	kubernetescli := kubernetes.Client{
		Client: cli,
	}
	kubernetescli.Kubernetes = &kubernetescli
	result, err := newEtcdCacherFromClient(ctx, &kubernetescli, schema, options.KeyPrefix, func() {
		_ = cli.Close()
	})
	if err != nil {
		_ = cli.Close()
		return nil, err
	}
	return result, nil
}

func NewEtcdCacherFromClient(ctx context.Context, cli *kubernetes.Client, schema *store.Schema, storagePrefix string) (*generic, error) {
	return newEtcdCacherFromClient(ctx, cli, schema, storagePrefix, nil)
}

func newEtcdCacherFromClient(
	ctx context.Context,
	cli *kubernetes.Client,
	schema *store.Schema,
	storagePrefix string,
	onClose func(),
) (*generic, error) {
	if cli == nil || cli.Client == nil {
		return nil, fmt.Errorf("etcd client is nil")
	}
	schema, err := schema.Clone()
	if err != nil {
		return nil, err
	}
	resourceFields := make(map[string][]string)
	for _, resource := range schema.Resources() {
		definition, err := schema.Resource(resource)
		if err != nil {
			return nil, err
		}
		fieldSet := map[string]struct{}{}
		for _, index := range definition.Indexes {
			if index.Unique && !definition.IsPrimaryIndex(index) {
				return nil, fmt.Errorf("etcd cache store does not support resource %q unique index %q", resource, index.Name)
			}
			if definition.IsPrimaryIndex(index) {
				continue
			}
			for _, field := range index.Fields {
				if slices.Contains(definition.ScopeKeys, field) {
					continue
				}
				fieldSet[field] = struct{}{}
			}
		}
		fields := make([]string, 0, len(fieldSet))
		for field := range fieldSet {
			fields = append(fields, field)
		}
		slices.Sort(fields)
		resourceFields[resource] = fields
	}
	resources := newResourceRegistry(
		ctx,
		resourceFields,
		func(ctx context.Context, resource k8sschema.GroupResource, indexFields []string) (*resourceDB, error) {
			return newResourceStorage(ctx, cli, storagePrefix, resource, indexFields)
		},
		onClose,
	)
	core := &core{
		storagePrefix: storagePrefix,
		cli:           cli,
		resources:     resources,
		schema:        schema,
	}
	return &generic{core: core}, nil
}

var _ store.PingableStore = &generic{}

type generic struct {
	core   *core
	scopes []store.Scope
}

// Schema implements store.Store.
func (c *generic) Schema() *store.Schema {
	return c.core.schema.Snapshot()
}

// Capabilities implements store.Store.
func (c *generic) Capabilities() store.Capabilities {
	return store.Capabilities{
		LabelSelector:    true,
		FieldSelector:    true,
		Search:           true,
		Sort:             true,
		Page:             true,
		Continue:         true,
		SubScopes:        true,
		OptimisticLock:   true,
		Watch:            true,
		TTL:              true,
		SecondaryIndexes: true,
	}
}

// Close stops all resource reflectors and caches. It does not close a client
// supplied to NewEtcdCacherFromClient; NewEtcdCacher closes the client it owns.
func (c *generic) Close() {
	c.core.resources.Close()
}

func (c *generic) Ping(ctx context.Context) error {
	_, err := c.core.cli.Client.Get(ctx, c.core.storagePrefix, clientv3.WithLimit(1))
	return err
}

// PatchBatch implements store.Store.
func (c *generic) PatchBatch(ctx context.Context, obj store.ObjectList, patch store.PatchBatch, opts ...store.PatchBatchOption) error {
	return errors.NewUnsupported("batch patch is not supported")
}

// DeleteBatch implements store.Store.
func (c *generic) DeleteBatch(ctx context.Context, obj store.ObjectList, opts ...store.DeleteBatchOption) error {
	// panic("unimplemented")
	return errors.NewUnsupported("delete batch is not supported")
}

// Count implements store.Store.
func (c *generic) Count(ctx context.Context, obj store.Object, opts ...store.CountOption) (int, error) {
	options := store.ApplyCountOptions(opts)
	predicate, err := ConvertPredicate(options.LabelRequirements, options.FieldRequirements)
	if err != nil {
		return 0, err
	}
	count := 0
	if err := c.core.on(ctx, obj, func(ctx context.Context, db *resourceDB) error {
		if err := db.waitReady(ctx); err != nil {
			return err
		}
		key := getlistkey(c.scopes, db.resource.String())
		listopts := storage.ListOptions{Recursive: true, Predicate: predicate}
		list := &StorageObjectList{}
		if err := db.storage.GetList(ctx, key, listopts, list); err != nil {
			return err
		}
		// filter
		filtered := list.Items
		if !options.IncludeSubScopes {
			filtered = FilterByScopes(filtered, c.scopes)
		}
		count = len(filtered)
		return nil
	}); err != nil {
		return 0, err
	}
	return count, nil
}

// Create implements store.Store.
func (c *generic) Create(ctx context.Context, obj store.Object, opts ...store.CreateOption) error {
	options := store.ApplyCreateOptions(opts)
	return c.core.on(ctx, obj, func(ctx context.Context, db *resourceDB) error {
		store.PrepareObjectForCreate(obj, db.resource.String(), c.scopes)
		uns, err := ConvertToUnstructured(obj)
		if err != nil {
			return err
		}
		key := getObjectKey(c.scopes, db.resource.String(), obj.GetID())
		if err := db.storage.Create(ctx, key, uns, uns, uint64(options.TTL/time.Second)); err != nil {
			err = storeerr.InterpretCreateError(err, db.resource, obj.GetID())
			return err
		}
		_ = ConvertFromUnstructured(uns, obj, db.resource)
		return nil
	})
}

// Delete implements store.Store.
func (c *generic) Delete(ctx context.Context, obj store.Object, opts ...store.DeleteOption) error {
	options := store.ApplyDeleteOptions(opts)
	if err := store.ValidateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return errors.NewBadRequest(err.Error())
	}
	preconditions := &storage.Preconditions{}
	if options.Preconditions != nil {
		if options.Preconditions.UID != nil {
			preconditions.UID = ptr.To(types.UID(*options.Preconditions.UID))
		}
		if options.Preconditions.ResourceVersion != nil {
			preconditions.ResourceVersion = ptr.To(strconv.FormatInt(*options.Preconditions.ResourceVersion, 10))
		}
	}
	predicate, err := ConvertPredicate(nil, nil)
	if err != nil {
		return err
	}
	updatefunc := func(ctx context.Context, current *store.Unstructured) (newObj store.Object, err error) {
		if err := store.ValidateDeleteRequirements(current, options.LabelRequirements, options.FieldRequirements); err != nil {
			return nil, err
		}
		policy := store.DeletePropagationBackground
		if options.PropagationPolicy != nil {
			policy = *options.PropagationPolicy
		}
		if store.PrepareObjectForDelete(current, policy) {
			return current, errShouldDelete
		}
		return current, nil
	}
	return c.update(ctx, obj, preconditions, predicate, updatefunc)
}

// Get implements store.Store.
func (c *generic) Get(ctx context.Context, name string, obj store.Object, opts ...store.GetOption) error {
	options := store.ApplyGetOptions(opts)
	predicate, err := ConvertPredicate(options.LabelRequirements, options.FieldRequirements)
	if err != nil {
		return err
	}
	return c.core.on(ctx, obj, func(ctx context.Context, db *resourceDB) error {
		key := getObjectKey(c.scopes, db.resource.String(), name)
		uns := &StorageObject{}
		storageOptions := storage.GetOptions{
			// if resource version is empty, underlying storage will passthrough to etcd
			// if set to 0, underlying storage will return the cached object
			// if set to a number, underlying storage will return the object with the same resource version
			ResourceVersion: formatResourceVersion(options.ResourceVersion),
		}
		if err := db.storage.Get(ctx, key, storageOptions, uns); err != nil {
			err = storeerr.InterpretGetError(err, db.resource, name)
			return err
		}
		ok, err := predicate.Matches(uns)
		if err != nil {
			return err
		}
		if !ok {
			return apierrors.NewNotFound(db.resource, name)
		}
		if err := ConvertFromUnstructured(uns, obj, db.resource); err != nil {
			return err
		}
		return store.ValidatePreconditions(obj, options.Preconditions)
	})
}

// List implements store.Store.
func (c *generic) List(ctx context.Context, list store.ObjectList, opts ...store.ListOption) error {
	options := store.ApplyListOptions(opts)
	continueMode := options.Limit > 0
	pageMode := !continueMode && options.Size > 0
	page := options.Page
	if pageMode && page < 1 {
		page = 1
	}
	if options.Limit > 0 && options.Sort != "" {
		return errors.NewUnsupported("etcd cache does not support sorting continuation lists")
	}
	preficate, err := ConvertPredicate(options.LabelRequirements, options.FieldRequirements)
	if err != nil {
		return err
	}
	v, newItemFunc, err := store.NewItemFuncFromList(list)
	if err != nil {
		return err
	}
	v.SetZero()
	return c.core.on(ctx, list, func(ctx context.Context, db *resourceDB) error {
		keyprefix := getlistkey(c.scopes, db.resource.String())
		getList := func(predicate storage.SelectionPredicate) (*StorageObjectList, error) {
			if err := db.waitReady(ctx); err != nil {
				return nil, err
			}
			listopts := storage.ListOptions{
				Recursive:       true,
				Predicate:       predicate,
				ResourceVersion: formatResourceVersion(options.ResourceVersion),
			}
			unslist := &StorageObjectList{}
			if err := db.storage.GetList(ctx, keyprefix, listopts, unslist); err != nil {
				return nil, storeerr.InterpretListError(err, db.resource)
			}
			return unslist, nil
		}

		filter := func(items []StorageObject) []StorageObject {
			filtered := items
			if !options.IncludeSubScopes {
				filtered = FilterByScopes(filtered, c.scopes)
			}
			if options.Search != "" {
				searchFields := options.SearchFields
				if len(searchFields) == 0 {
					searchFields = []string{"id", "name"}
				}
				filtered = slices.DeleteFunc(filtered, func(uns StorageObject) bool {
					return !searchObject(&uns, searchFields, options.Search)
				})
			}
			return filtered
		}

		var (
			filtered        []StorageObject
			continueToken   string
			resourceVersion int64
		)
		if continueMode {
			continueToken = options.Continue
			for {
				predicate := preficate
				predicate.Continue = continueToken
				predicate.Limit = int64(options.Limit - len(filtered))
				unslist, err := getList(predicate)
				if err != nil {
					return err
				}
				filtered = append(filtered, filter(unslist.Items)...)
				continueToken = unslist.GetContinue()
				resourceVersion = unslist.GetResourceVersion()
				if continueToken == "" || len(filtered) >= options.Limit {
					break
				}
			}
		} else {
			unslist, err := getList(preficate)
			if err != nil {
				return err
			}
			filtered = filter(unslist.Items)
			resourceVersion = unslist.GetResourceVersion()
			SortUnstructuredList(filtered, store.ParseSorts(options.Sort))
		}

		// pagination
		total := len(filtered)
		if pageMode {
			filtered = PageUnstructuredList(filtered, page, options.Size)
		}

		// convert to result
		for _, uns := range filtered {
			obj := newItemFunc()
			if err := ConvertFromUnstructured(&uns, obj, db.resource); err != nil {
				return err
			}
			v.Set(reflect.Append(v, reflect.ValueOf(obj).Elem()))
		}
		if continueMode {
			store.SetContinuationListMetadata(list, continueToken, options.Limit)
		} else if pageMode {
			store.SetPageListMetadata(list, page, options.Size, total)
		} else {
			store.SetUnpaginatedListMetadata(list, total)
		}
		list.SetResourceVersion(resourceVersion)
		list.SetScopes(c.scopes)
		list.SetResource(db.resource.String())
		return nil
	})
}

func formatResourceVersion(i *int64) string {
	if i == nil {
		return ""
	}
	return strconv.FormatInt(*i, 10)
}

// Patch implements store.Store.
func (c *generic) Patch(ctx context.Context, obj store.Object, patch store.Patch, opts ...store.PatchOption) error {
	options := store.ApplyPatchOptions(opts)
	preconditions := &storage.Preconditions{}
	if obj.GetUID() != "" {
		preconditions.UID = ptr.To(types.UID(obj.GetUID()))
	}
	predicate, err := ConvertPredicate(options.LabelRequirements, options.FieldRequirements)
	if err != nil {
		return err
	}
	updatefunc := func(ctx context.Context, current *store.Unstructured) (newObj store.Object, err error) {
		if err := store.ValidatePreconditions(current, options.Preconditions); err != nil {
			return nil, err
		}
		if err := store.ApplyPatch(current, obj, patch); err != nil {
			return nil, err
		}
		return current, nil
	}
	return c.update(ctx, obj, preconditions, predicate, updatefunc)
}

// Scope implements store.Store.
func (c *generic) Scope(scope ...store.Scope) store.Store {
	return &generic{
		core:   c.core,
		scopes: append(c.scopes, scope...),
	}
}

var errShouldDelete = fmt.Errorf("should delete")

// Update implements store.Store.
func (c *generic) Update(ctx context.Context, obj store.Object, opts ...store.UpdateOption) error {
	options := store.ApplyUpdateOptions(opts)
	updatefunc := func(ctx context.Context, oldObj *store.Unstructured) (store.Object, error) {
		return obj, nil
	}
	preconditions := &storage.Preconditions{}
	if obj.GetUID() != "" {
		preconditions.UID = ptr.To(types.UID(obj.GetUID()))
	}
	if rev := obj.GetResourceVersion(); rev != 0 {
		preconditions.ResourceVersion = ptr.To(strconv.FormatInt(rev, 10))
	}
	predicate, err := ConvertPredicate(options.LabelRequirements, options.FieldRequirements)
	if err != nil {
		return err
	}
	return c.update(ctx, obj, preconditions, predicate, updatefunc)
}

func (c *generic) update(ctx context.Context, obj store.Object, preconditions *storage.Preconditions, predicate storage.SelectionPredicate, updatefunc updateFunc) error {
	return c.core.update(ctx, c.scopes, obj, preconditions, predicate, updatefunc, false)
}

// Status implements store.Store.
func (c *generic) Status() store.StatusStorage {
	return &status{core: c.core, scopes: c.scopes}
}

var _ store.StatusStorage = &status{}

type status struct {
	core   *core
	scopes []store.Scope
}

// Patch implements store.StatusStorage.
func (s *status) Patch(ctx context.Context, obj store.Object, patch store.Patch, opts ...store.PatchOption) error {
	options := store.ApplyPatchOptions(opts)
	preconditions := &storage.Preconditions{}
	if obj.GetUID() != "" {
		preconditions.UID = ptr.To(types.UID(obj.GetUID()))
	}
	predicate, err := ConvertPredicate(options.LabelRequirements, options.FieldRequirements)
	if err != nil {
		return err
	}
	updatefunc := func(ctx context.Context, current *store.Unstructured) (newObj store.Object, err error) {
		if err := store.ValidatePreconditions(current, options.Preconditions); err != nil {
			return nil, err
		}
		if err := store.ApplyPatch(current, obj, patch); err != nil {
			return nil, err
		}
		return current, nil
	}
	return s.update(ctx, obj, preconditions, predicate, updatefunc)
}

func (s *status) update(ctx context.Context, obj store.Object, preconditions *storage.Preconditions, predicate storage.SelectionPredicate, updatefunc updateFunc) error {
	return s.core.update(ctx, s.scopes, obj, preconditions, predicate, updatefunc, true)
}

// Update implements store.StatusStorage.
func (s *status) Update(ctx context.Context, obj store.Object, opts ...store.UpdateOption) error {
	options := store.ApplyUpdateOptions(opts)
	preconditions := &storage.Preconditions{}
	if obj.GetUID() != "" {
		preconditions.UID = ptr.To(types.UID(obj.GetUID()))
	}
	if rev := obj.GetResourceVersion(); rev != 0 {
		preconditions.ResourceVersion = ptr.To(strconv.FormatInt(rev, 10))
	}
	predicate, err := ConvertPredicate(options.LabelRequirements, options.FieldRequirements)
	if err != nil {
		return err
	}
	updatefunc := func(ctx context.Context, oldObj *store.Unstructured) (store.Object, error) {
		return obj, nil
	}
	return s.update(ctx, obj, preconditions, predicate, updatefunc)
}

type core struct {
	resources     *resourceRegistry
	storagePrefix string
	cli           *kubernetes.Client
	schema        *store.Schema
}

func (c *core) on(ctx context.Context, example any, fn func(ctx context.Context, db *resourceDB) error) error {
	if err := c.validateObject(example); err != nil {
		return err
	}
	resource, err := store.GetResource(example)
	if err != nil {
		return err
	}
	db, err := c.getResource(resource)
	if err != nil {
		return err
	}
	return convertError(fn(ctx, db))
}

func convertError(err error) error {
	if err == nil {
		return nil
	}
	if statusErr, ok := err.(*apierrors.StatusError); ok {
		return &errors.Status{
			Status:  statusErr.ErrStatus.Status,
			Code:    statusErr.ErrStatus.Code,
			Message: statusErr.ErrStatus.Message,
			Reason:  errors.StatusReason(statusErr.ErrStatus.Reason),
		}
	}
	return err
}

type updateFunc func(ctx context.Context, current *store.Unstructured) (newObj store.Object, err error)

func (c *core) update(ctx context.Context, scopes []store.Scope, obj store.Object, preconditions *storage.Preconditions, predicate storage.SelectionPredicate, fn updateFunc, statusOnly bool) error {
	if !predicate.Empty() {
		return errors.NewBadRequest("predicate is not supported")
	}
	return c.on(ctx, obj, func(ctx context.Context, db *resourceDB) error {
		out := &StorageObject{}
		key := getObjectKey(scopes, db.resource.String(), obj.GetID())
		err := db.storage.GuaranteedUpdate(ctx, key, out, false, preconditions, func(input runtime.Object, res storage.ResponseMeta) (output runtime.Object, ttl *uint64, err error) {
			current, ok := input.(*StorageObject)
			if !ok {
				return nil, nil, fmt.Errorf("unexpected object type: %T", input)
			}
			// backup fields
			statusfield, _, _ := unstructured.NestedFieldNoCopy(current.Object, "status")
			unsobj := &store.Unstructured{}
			if err := ConvertFromUnstructured(current, unsobj, db.resource); err != nil {
				return nil, nil, err
			}
			currentMap, err := store.ObjectToMap(unsobj)
			if err != nil {
				return nil, nil, err
			}
			scopes, id, uid, creation, deletion, generation := unsobj.GetScopes(), unsobj.GetID(), unsobj.GetUID(), unsobj.GetCreationTimestamp(), unsobj.GetDeletionTimestamp(), unsobj.GetGeneration()
			unsobjchanged, err := fn(ctx, unsobj)
			if err != nil {
				return nil, nil, err
			}
			// do not change scopes
			unsobjchanged.SetScopes(scopes)
			unsobjchanged.SetID(id)
			unsobjchanged.SetUID(uid)
			unsobjchanged.SetResource(db.resource.String())
			unsobjchanged.SetCreationTimestamp(creation)
			// once deletiontime is set, it can not be updated
			if deletion != nil {
				unsobjchanged.SetDeletionTimestamp(deletion)
			}

			newuns, err := ConvertToUnstructured(unsobjchanged)
			if err != nil {
				return nil, nil, err
			}
			// resource can not be changed
			newuns.GetObjectKind().SetGroupVersionKind(current.GetObjectKind().GroupVersionKind())
			// restore ignored fields
			if statusOnly {
				// status-only update: keep only status field from new object, preserve all other fields (including generation)
				status, _, _ := unstructured.NestedFieldNoCopy(newuns.Object, "status")
				newuns.Object = (&unstructured.Unstructured{Object: current.Object}).DeepCopy().Object
				_ = unstructured.SetNestedField(newuns.Object, status, "status")
				// generation is already preserved in the copied object
			} else {
				// Normal updates preserve status and only business-field changes advance generation.
				_ = unstructured.SetNestedField(newuns.Object, statusfield, "status")
				changedMap, err := store.ObjectToMap(unsobjchanged)
				if err != nil {
					return nil, nil, err
				}
				if !store.ObjectBusinessFieldsEqual(currentMap, changedMap) {
					generation++
				}
				_ = unstructured.SetNestedField(newuns.Object, generation, "generation")
			}

			if ShouldDeleteDuringUpdate(ctx, key, newuns, current) {
				return newuns, nil, errShouldDelete
			}
			return newuns, nil, nil
		}, nil)
		if err != nil {
			if err == errShouldDelete {
				// Using the rest.ValidateAllObjectFunc because the request is an UPDATE request and has already passed the admission for the UPDATE verb.
				if err := db.storage.Delete(ctx, key, out, preconditions, rest.ValidateAllObjectFunc, nil, storage.DeleteOptions{}); err != nil {
					// Deletion is racy, i.e., there could be multiple update
					// requests to remove all finalizers from the object, so we
					// ignore the NotFound error.
					if !storage.IsNotFound(err) {
						err = storeerr.InterpretDeleteError(err, db.resource, obj.GetID())
						return err
					}
					// pass
				}
				// pass
			} else {
				err = storeerr.InterpretUpdateError(err, db.resource, obj.GetID())
				return err
			}
		}
		_ = ConvertFromUnstructured(out, obj, db.resource)
		return nil
	})
}

func ShouldDeleteDuringUpdate(ctx context.Context, key string, obj, existing runtime.Object) bool {
	newMeta, err := meta.Accessor(obj)
	if err != nil {
		return false
	}
	if len(newMeta.GetFinalizers()) > 0 {
		return false
	}
	if newMeta.GetDeletionTimestamp() == nil {
		return false
	}
	return true
}

func (e *core) validateObject(obj any) error {
	if obj == nil {
		return errors.NewBadRequest("object is nil")
	}
	if _, err := store.EnforcePtr(obj); err != nil {
		return errors.NewBadRequest(fmt.Sprintf("object must be a pointer: %v", err))
	}
	return nil
}

func (c *core) getResource(resource string) (*resourceDB, error) {
	return c.resources.get(resource)
}

func ScopesObjectKeyFunc(obj runtime.Object) (string, error) {
	uns, ok := obj.(*StorageObject)
	if !ok {
		return "", fmt.Errorf("unexpected object type: %T", obj)
	}
	scopes, err := ParseScopes(uns)
	if err != nil {
		return "", err
	}
	return getObjectKey(scopes, uns.GetKind(), GetNestedString(uns.Object, "id")), nil
}

func ConvertToUnstructured(obj store.Object) (*StorageObject, error) {
	values, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, err
	}
	uns := StorageObject{Object: values}
	uns.SetAPIVersion("v1")
	store.SetScopesFields(values, obj.GetScopes())
	return &uns, nil
}

func ConvertFromUnstructured(uns *StorageObject, obj store.Object, resource k8sschema.GroupResource) error {
	datafield := uns.Object
	if datafield == nil {
		datafield = map[string]any{}
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(datafield, obj); err != nil {
		return err
	}
	return nil
}

func getObjectKey(scopes []store.Scope, resource, name string) string {
	var key strings.Builder
	key.WriteString("/")
	key.WriteString(resource)
	for _, scope := range scopes {
		key.WriteString("/")
		key.WriteString(scope.Resource)
		key.WriteString("/")
		key.WriteString(scope.Name)
	}
	return key.String() + "/" + name
}

func getlistkey(scopes []store.Scope, resource string) string {
	var key strings.Builder
	key.WriteString("/")
	key.WriteString(resource)
	for _, scope := range scopes {
		key.WriteString("/")
		key.WriteString(scope.Resource)
		key.WriteString("/")
		key.WriteString(scope.Name)
	}
	return key.String() + "/"
}

func FilterByScopes(list []StorageObject, scopes []store.Scope) []StorageObject {
	filtered := make([]StorageObject, 0, len(list))
	for _, uns := range list {
		thisscopes, err := ParseScopes(&uns)
		if err != nil {
			continue
		}
		if store.ScopesEquals(thisscopes, scopes) {
			filtered = append(filtered, uns)
		}
	}
	return filtered
}

func PageUnstructuredList(list []StorageObject, page, size int) []StorageObject {
	if size <= 0 {
		return list
	}
	page = max(page, 1)
	total := len(list)
	pageIndex := page - 1
	startIdx := total
	if pageIndex <= total/size {
		startIdx = pageIndex * size
	}
	endIdx := startIdx + min(size, total-startIdx)
	list = list[startIdx:endIdx]
	return list
}
