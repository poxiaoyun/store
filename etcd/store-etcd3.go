package etcd

import (
	"bytes"
	"context"
	stderrors "errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"time"

	grpcprom "github.com/grpc-ecosystem/go-grpc-prometheus"
	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/kubernetes"
	"google.golang.org/grpc"
	"k8s.io/utils/ptr"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/log"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/inmemory"
)

var ErrResourceVersionSetOnCreate = stderrors.New("resourceVersion should not be set on objects to be created")

func NewDefaultOptions() *Options {
	return &Options{
		Servers:   []string{"http://127.0.0.1:2379"},
		KeyPrefix: "/core",
	}
}

type Options struct {
	Servers       []string `json:"servers,omitempty"`
	Username      string   `json:"username,omitempty"`
	Password      string   `json:"password,omitempty"`
	KeyFile       string   `json:"keyFile,omitempty"`
	CertFile      string   `json:"certFile,omitempty"`
	TrustedCAFile string   `json:"trustedCAFile,omitempty"`
	KeyPrefix     string   `json:"keyPrefix,omitempty"`
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

func NewETCD3Client(ctx context.Context, c *Options) (*kubernetes.Client, error) {
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
	dialOptions := []grpc.DialOption{
		// use chained interceptors so that the default (retry and backoff) interceptors are added.
		// otherwise they will be overwritten by the metric interceptor.
		//
		// these optional interceptors will be placed after the default ones.
		// which seems to be what we want as the metrics will be collected on each attempt (retry)
		grpc.WithChainUnaryInterceptor(grpcprom.UnaryClientInterceptor),
		grpc.WithChainStreamInterceptor(grpcprom.StreamClientInterceptor),
	}
	cfg := clientv3.Config{
		DialTimeout:          dialTimeout,
		DialKeepAliveTime:    keepaliveTime,
		DialKeepAliveTimeout: keepaliveTimeout,
		DialOptions:          dialOptions,
		Endpoints:            c.Servers,
		TLS:                  tlsConfig,
		Username:             c.Username,
		Password:             c.Password,
	}
	return kubernetes.New(cfg)
}

func NewEtcdStore(ctx context.Context, schema *store.Schema, c *Options) (*EtcdStore, error) {
	client, err := NewETCD3Client(ctx, c)
	if err != nil {
		return nil, err
	}
	// check if the server is up
	timeouts, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	log := log.FromContext(ctx)
	log.Info("checking etcd server health")
	if _, err := client.Get(timeouts, "health", kubernetes.GetOptions{}); err != nil {
		return nil, fmt.Errorf("etcd server is not reachable: %v", err)
	}
	return NewEtcdStoreFromClient(client, schema, c.KeyPrefix)
}

func NewEtcdStoreFromClient(client *kubernetes.Client, schema *store.Schema, keyPrefix string) (*EtcdStore, error) {
	schema, err := schema.Clone()
	if err != nil {
		return nil, err
	}
	if err := validateSchema(schema); err != nil {
		return nil, err
	}
	return &EtcdStore{core: newEtcdStoreCore(client, schema, keyPrefix)}, nil
}

func newEtcdStoreCore(client *kubernetes.Client, schema *store.Schema, keyPrefix string) *etcdStoreCore {
	return &etcdStoreCore{
		client:     client,
		KeyPrefix:  keyPrefix,
		schema:     schema,
		serializer: &store.JSONSerializer{},
		leases:     newEtcd3LeaseManager(client, 0, 0, 0),
	}
}

func validateSchema(schema *store.Schema) error {
	for _, resource := range schema.Resources() {
		definition, err := schema.Resource(resource)
		if err != nil {
			return err
		}
		for _, index := range definition.Indexes {
			if !definition.IsPrimaryIndex(index) {
				return fmt.Errorf("etcd store does not support resource %q index %q", resource, index.Name)
			}
		}
	}
	return nil
}

var _ store.PingableStore = &EtcdStore{}

type EtcdStore struct {
	scopes []store.Scope
	core   *etcdStoreCore
}

// Schema implements store.Store.
func (e *EtcdStore) Schema() *store.Schema {
	return e.core.schema.Snapshot()
}

// Capabilities implements store.Store.
func (e *EtcdStore) Capabilities() store.Capabilities {
	return store.Capabilities{Finalizers: true, OptimisticLock: true, Watch: true}
}

func (e *EtcdStore) Ping(ctx context.Context) error {
	_, err := e.core.client.KV.Get(ctx, e.core.KeyPrefix, clientv3.WithLimit(1))
	return err
}

// PatchBatch implements store.Store.
func (e *EtcdStore) PatchBatch(ctx context.Context, obj store.ObjectList, patch store.PatchBatch, opts ...store.PatchBatchOption) error {
	return errors.NewUnsupported("etcd does not support batch patch")
}

// DeleteBatch implements store.Store.
func (e *EtcdStore) DeleteBatch(ctx context.Context, obj store.ObjectList, opts ...store.DeleteBatchOption) error {
	return errors.NewUnsupported("etcd does not support delete batch")
}

// Count implements Store.
func (e *EtcdStore) Count(ctx context.Context, obj store.Object, opts ...store.CountOption) (int, error) {
	options := store.ApplyCountOptions(opts)
	if options.Search != "" || len(options.SearchFields) != 0 {
		return 0, errors.NewUnsupported("etcd store does not support search")
	}
	resource, err := e.core.schema.ResourceName(obj)
	if err != nil {
		return 0, err
	}
	list := &store.List[store.Unstructured]{Resource: resource}
	listOptions := []store.ListOption{
		store.WithLabelRequirements(options.LabelRequirements...),
		store.WithFieldRequirements(options.FieldRequirements...),
	}
	if options.IncludeSubScopes {
		listOptions = append(listOptions, store.WithSubScopes())
	}
	if err := e.List(ctx, list, listOptions...); err != nil {
		return 0, err
	}
	return len(list.Items), nil
}

// Create implements Store.
func (e *EtcdStore) Create(ctx context.Context, obj store.Object, opts ...store.CreateOption) error {
	resource, err := e.core.schema.ResourceName(obj)
	if err != nil {
		return err
	}
	creatoptions := store.ApplyCreateOptions(opts)
	if err := e.core.validateObject(obj); err != nil {
		return err
	}
	if creatoptions.DryRun {
		return errors.NewUnsupported("etcd store does not support dry-run")
	}
	if creatoptions.TTL != 0 && len(obj.GetFinalizers()) != 0 {
		return errors.NewBadRequest("TTL cannot be combined with finalizers")
	}
	prepareObjectForCreate(obj, resource, e.scopes)

	preparedKey := e.core.getkey(e.scopes, resource, obj.GetID())

	data, err := e.core.serializer.Encode(obj)
	if err != nil {
		return err
	}
	putopts, err := e.core.ttlOpts(ctx, int64(creatoptions.TTL.Seconds()))
	if err != nil {
		return err
	}
	txnResp, err := e.core.client.KV.Txn(ctx).
		If(
			keyHasRevision(preparedKey, 0), // key does not exist
		).
		Then(
			clientv3.OpPut(preparedKey, string(data), putopts...),
		).
		Commit()
	if err != nil {
		return errors.NewInternalError(err)
	}
	if !txnResp.Succeeded {
		return errors.NewAlreadyExists(resource, obj.GetID())
	}

	putResp := txnResp.Responses[0].GetResponsePut()
	resourceVersion := putResp.Header.Revision
	// decode once
	if err := e.core.serializer.Decode(data, obj); err != nil {
		return errors.NewInternalError(err)
	}
	obj.SetResourceVersion(resourceVersion)
	return nil
}

func keyHasRevision(key string, rev int64) clientv3.Cmp {
	return clientv3.Compare(clientv3.ModRevision(key), "=", rev)
}

// Delete implements Store.
func (e *EtcdStore) Delete(ctx context.Context, obj store.Object, opts ...store.DeleteOption) error {
	deleteoptions := store.ApplyDeleteOptions(opts)
	if deleteoptions.DryRun {
		return errors.NewUnsupported("etcd store does not support dry-run")
	}
	if err := validateSelectorRequirements(deleteoptions.LabelRequirements, deleteoptions.FieldRequirements); err != nil {
		return err
	}
	if err := e.core.validateObject(obj); err != nil {
		return err
	}
	if obj.GetID() == "" {
		return errors.NewBadRequest("name is required")
	}

	policy := ptr.Deref(deleteoptions.PropagationPolicy, store.DeletePropagationBackground)
	for {
		current := store.NewObject(obj)
		if err := e.Get(ctx, obj.GetID(), current); err != nil {
			return err
		}
		if err := store.ValidatePreconditions(current, deleteoptions.Preconditions); err != nil {
			return err
		}
		if err := store.ValidateDeleteRequirements(current, deleteoptions.LabelRequirements, deleteoptions.FieldRequirements); err != nil {
			return err
		}
		if prepareObjectForDelete(current, policy) {
			if err := e.core.directDelete(ctx, e.scopes, current, current.GetResourceVersion()); err != nil {
				if errors.IsConflict(err) {
					continue
				}
				return err
			}
			return store.CopyObject(current, obj)
		}
		updatefunc := func(latest store.Object) (store.Object, error) {
			if err := store.ValidatePreconditions(latest, deleteoptions.Preconditions); err != nil {
				return nil, err
			}
			if err := store.ValidateDeleteRequirements(latest, deleteoptions.LabelRequirements, deleteoptions.FieldRequirements); err != nil {
				return nil, err
			}
			prepareObjectForDelete(latest, policy)
			return latest, nil
		}
		return e.core.tryUpdate(ctx, e.scopes, obj, updatefunc, tryUpdateOptions{})
	}
}

// Get implements Store.
func (e *EtcdStore) Get(ctx context.Context, name string, obj store.Object, opts ...store.GetOption) error {
	resource, err := e.core.schema.ResourceName(obj)
	if err != nil {
		return err
	}
	options := store.ApplyGetOptions(opts)
	if len(options.Fields) != 0 {
		return errors.NewUnsupported("etcd store does not support field projection")
	}
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	if err := e.core.validateObject(obj); err != nil {
		return err
	}
	preparedKey := e.core.getkey(e.scopes, resource, name)
	_, err = e.core.getCurrent(ctx, preparedKey, obj, obj.GetResourceVersion())
	if err != nil {
		return err
	}
	unstructured, err := store.ToUnstructured(obj)
	if err != nil {
		return err
	}
	if !store.MatchLabelReqirements(obj, options.LabelRequirements) ||
		!store.MatchUnstructuredFieldRequirments(unstructured, options.FieldRequirements) {
		return errors.NewNotFound(resource, name)
	}
	return store.ValidatePreconditions(obj, options.Preconditions)
}

const maxLimit = 10000

// List implements Store.
func (e *EtcdStore) List(ctx context.Context, list store.ObjectList, opts ...store.ListOption) error {
	resource, err := e.core.schema.ResourceName(list)
	if err != nil {
		return err
	}
	options := store.ApplyListOptions(opts)
	if len(options.Fields) != 0 {
		return errors.NewUnsupported("etcd store does not support field projection")
	}
	if options.Search != "" || len(options.SearchFields) != 0 {
		return errors.NewUnsupported("etcd store does not support search")
	}
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	if err := e.core.validateObjectList(list); err != nil {
		return err
	}
	page, err := store.PrepareListPage(resource, e.scopes, options)
	if err != nil {
		return err
	}
	options = page.Options
	values, newItem, err := store.NewItemFuncFromList(list)
	if err != nil {
		return err
	}
	values.Set(reflect.MakeSlice(values.Type(), 0, 0))
	key := e.core.getlistkey(e.scopes, resource)
	type item struct {
		object store.Object
		fields *store.Unstructured
	}
	items := []item{}
	startKey := key
	rangeEnd := clientv3.GetPrefixRangeEnd(key)
	requested := store.ApplyListOptions(opts)
	bounded := !options.IncludeSubScopes && (requested.Sort == "" || requested.Sort == "id+") && options.Limit > 0
	var revision int64
	for {
		getOptions := []clientv3.OpOption{clientv3.WithRange(rangeEnd)}
		if revision != 0 {
			getOptions = append(getOptions, clientv3.WithRev(revision))
		} else if options.ResourceVersion != nil {
			getOptions = append(getOptions, clientv3.WithRev(*options.ResourceVersion))
		}
		if bounded {
			getOptions = append(getOptions, clientv3.WithLimit(int64(options.Offset)+int64(options.Limit)-int64(len(items))))
		}
		response, err := e.core.client.KV.Get(ctx, startKey, getOptions...)
		if err != nil {
			return InterpretEtcdReadError(resource, err)
		}
		if revision == 0 {
			revision = response.Header.Revision
			if options.ResourceVersion != nil && *options.ResourceVersion > 0 {
				revision = *options.ResourceVersion
			}
		}
		for _, kv := range response.Kvs {

			if !options.IncludeSubScopes && bytes.Contains(kv.Key[len(key):], []byte("/")) {
				continue
			}
			object := newItem()
			if err := e.core.serializer.Decode(kv.Value, object); err != nil {
				return errors.NewInternalError(err)
			}
			object.SetResourceVersion(kv.ModRevision)
			matches, err := matchFieldRequirements(object, options.FieldRequirements)
			if err != nil {
				return err
			}
			if !matches || !store.MatchLabelReqirements(object, options.LabelRequirements) {
				continue
			}
			unstructured, err := store.ToUnstructured(object)
			if err != nil {
				return err
			}
			items = append(items, item{object: object, fields: unstructured})
		}
		if !response.More || bounded && len(items) >= options.Offset+options.Limit {
			break
		}
		startKey = string(response.Kvs[len(response.Kvs)-1].Key) + "\x00"
	}
	sorts := store.ParseSorts(options.Sort)
	slices.SortStableFunc(items, func(a, b item) int { return store.CompareUnstructuredField(a.fields, b.fields, sorts) })
	start := min(options.Offset, len(items))
	end := len(items)
	if options.Limit > 0 {
		end = start + min(options.Limit, len(items)-start)
	}
	for _, item := range items[start:end] {
		values.Set(reflect.Append(values, reflect.ValueOf(item.object).
			Elem()))
	}
	list.SetResource(resource)
	list.SetScopes(e.scopes)
	list.SetResourceVersion(revision)
	return page.Finish(list)
}

// Patch implements Store.
func (e *EtcdStore) Patch(ctx context.Context, obj store.Object, patch store.Patch, opts ...store.PatchOption) error {
	options := store.ApplyPatchOptions(opts)
	if options.DryRun {
		return errors.NewUnsupported("etcd store does not support dry-run")
	}
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	updatefunc := func(current store.Object) (store.Object, error) {
		if err := store.ValidatePreconditions(current, options.Preconditions); err != nil {
			return nil, err
		}
		desired := store.NewObject(obj)
		if err := store.CopyObject(current, desired); err != nil {
			return nil, err
		}
		if err := inmemory.ApplyPatch(desired, obj, patch); err != nil {
			return nil, err
		}
		_, err := prepareObjectForUpdate(current, desired, false)
		return desired, err
	}
	return e.core.tryUpdate(ctx, e.scopes, obj, updatefunc, tryUpdateOptions{
		LabelRequirements: options.LabelRequirements,
		FieldRequirements: options.FieldRequirements,
	})
}

// Scope implements Store.
func (e *EtcdStore) Scope(scope ...store.Scope) store.Store {
	return &EtcdStore{core: e.core, scopes: append(e.scopes, scope...)}
}

// Status implements Store.
func (e *EtcdStore) Status() store.StatusStorage {
	return &EtcdStatusStore{core: e.core, scopes: e.scopes}
}

// Update implements Store.
func (e *EtcdStore) Update(ctx context.Context, obj store.Object, opts ...store.UpdateOption) error {
	options := store.ApplyUpdateOptions(opts)
	if options.DryRun {
		return errors.NewUnsupported("etcd store does not support dry-run")
	}
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	requested := store.NewObject(obj)
	if err := store.CopyObject(obj, requested); err != nil {
		return err
	}
	updatefunc := func(current store.Object) (store.Object, error) {
		if resourceVersion := requested.GetResourceVersion(); resourceVersion != 0 {
			if resourceVersion != current.GetResourceVersion() {
				return nil, errors.NewConflict(current.GetResource(), requested.GetID(),
					fmt.Errorf("resourceVersion %d does not match", resourceVersion))
			}
		}
		desired := store.NewObject(requested)
		if err := store.CopyObject(requested, desired); err != nil {
			return nil, err
		}
		_, err := prepareObjectForUpdate(current, desired, false)
		return desired, err
	}
	return e.core.tryUpdate(ctx, e.scopes, obj, updatefunc, tryUpdateOptions{
		TTL:               int64(options.TTL.Seconds()),
		LabelRequirements: options.LabelRequirements,
		FieldRequirements: options.FieldRequirements,
	})
}

var _ store.StatusStorage = &EtcdStatusStore{}

type EtcdStatusStore struct {
	core   *etcdStoreCore
	scopes []store.Scope
}

// Patch implements StatusStorage.
func (e *EtcdStatusStore) Patch(ctx context.Context, obj store.Object, patch store.Patch, opts ...store.PatchOption) error {
	options := store.ApplyPatchOptions(opts)
	if options.DryRun {
		return errors.NewUnsupported("etcd store does not support dry-run")
	}
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	updatefunc := func(current store.Object) (store.Object, error) {
		if err := store.ValidatePreconditions(current, options.Preconditions); err != nil {
			return nil, err
		}
		desired := store.NewObject(obj)
		if err := store.CopyObject(current, desired); err != nil {
			return nil, err
		}
		if err := inmemory.ApplyPatch(desired, obj, patch); err != nil {
			return nil, err
		}
		_, err := prepareObjectForUpdate(current, desired, true)
		return desired, err
	}
	return e.core.tryUpdate(ctx, e.scopes, obj, updatefunc, tryUpdateOptions{
		LabelRequirements: options.LabelRequirements,
		FieldRequirements: options.FieldRequirements,
	})
}

// Update implements StatusStorage.
func (e *EtcdStatusStore) Update(ctx context.Context, obj store.Object, opts ...store.UpdateOption) error {
	resource, err := e.core.schema.ResourceName(obj)
	if err != nil {
		return err
	}
	options := store.ApplyUpdateOptions(opts)
	if options.TTL != 0 {
		return errors.NewUnsupported("status updates cannot set TTL")
	}
	if options.DryRun {
		return errors.NewUnsupported("etcd store does not support dry-run")
	}
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	requested := store.NewObject(obj)
	if err := store.CopyObject(obj, requested); err != nil {
		return err
	}
	updatefunc := func(current store.Object) (store.Object, error) {
		if resourceVersion := requested.GetResourceVersion(); resourceVersion != 0 {
			if resourceVersion != current.GetResourceVersion() {
				return nil, errors.NewConflict(resource, requested.GetID(),
					fmt.Errorf("resourceVersion %d does not match", resourceVersion))
			}
		}
		desired := store.NewObject(requested)
		if err := store.CopyObject(requested, desired); err != nil {
			return nil, err
		}
		_, err := prepareObjectForUpdate(current, desired, true)
		return desired, err
	}
	return e.core.tryUpdate(ctx, e.scopes, obj, updatefunc, tryUpdateOptions{
		TTL:               int64(options.TTL.Seconds()),
		LabelRequirements: options.LabelRequirements,
		FieldRequirements: options.FieldRequirements,
	})
}

type etcdStoreCore struct {
	KeyPrefix  string
	client     *kubernetes.Client
	schema     *store.Schema
	serializer store.Serializer
	leases     *leaseManager
}

func (s *etcdStoreCore) ttlOpts(ctx context.Context, ttl int64) ([]clientv3.OpOption, error) {
	if ttl == 0 {
		return nil, nil
	}
	id, err := s.leases.GetLease(ctx, ttl)
	if err != nil {
		return nil, err
	}
	return []clientv3.OpOption{clientv3.WithLease(id)}, nil
}

func (e *etcdStoreCore) validateObjectList(obj store.ObjectList) error {
	if obj == nil {
		return errors.NewBadRequest("object list is nil")
	}
	if _, err := store.EnforcePtr(obj); err != nil {
		return errors.NewBadRequest(fmt.Sprintf("object list must be a pointer: %v", err))
	}
	return nil
}

func (e *etcdStoreCore) validateObject(obj store.Object) error {
	if obj == nil {
		return errors.NewBadRequest("object is nil")
	}
	if _, err := store.EnforcePtr(obj); err != nil {
		return errors.NewBadRequest(fmt.Sprintf("object must be a pointer: %v", err))
	}
	return nil
}

func (e *etcdStoreCore) getlistkey(scopes []store.Scope, resource string) string {
	var key strings.Builder
	key.WriteString(e.KeyPrefix + "/" + resource)
	for _, scope := range scopes {
		key.WriteString("/" + scope.Resource + "/" + scope.Name)
	}
	return key.String() + "/"
}

func (e *etcdStoreCore) getkey(scopes []store.Scope, resource, name string) string {
	// merge all scopes into a single key
	var key strings.Builder
	key.WriteString(e.KeyPrefix + "/" + resource)
	for _, scope := range scopes {
		key.WriteString("/" + scope.Resource + "/" + scope.Name)
	}
	key.WriteString("/" + name)
	return key.String()
}

type tryUpdateOptions struct {
	TTL               int64
	LabelRequirements store.Requirements
	FieldRequirements store.Requirements
}

type tryUpdateFunc func(current store.Object) (store.Object, error)

func (e *etcdStoreCore) tryUpdate(ctx context.Context, scopes []store.Scope, obj store.Object, do tryUpdateFunc, options tryUpdateOptions) error {
	v, err := store.EnforcePtr(obj)
	if err != nil {
		return errors.NewBadRequest(fmt.Sprintf("object must be a pointer: %v", err))
	}
	if obj.GetID() == "" {
		return errors.NewBadRequest("id is required")
	}
	id := obj.GetID()
	resource, err := e.schema.ResourceName(obj)
	if err != nil {
		return err
	}

	currentValue := reflect.New(v.Type())
	current := currentValue.Interface().(store.Object)

	preparedKey := e.getkey(scopes, resource, id)

	response, err := e.client.KV.Get(ctx, preparedKey)
	if err != nil {
		return errors.NewInternalError(err)
	}
	currentdata, err := e.decodeGetResp(response, current, 0)
	if err != nil {
		return err
	}
	lease := response.Kvs[0].Lease
	for {
		currentversion := current.GetResourceVersion()
		unstructured, err := store.ToUnstructured(current)
		if err != nil {
			return err
		}
		if !store.MatchLabelReqirements(current, options.LabelRequirements) ||
			!store.MatchUnstructuredFieldRequirments(unstructured, options.FieldRequirements) {
			return errors.NewConflict(resource, id, fmt.Errorf("write conditions did not match"))
		}
		updated, err := do(current)
		if err != nil {
			return err
		}
		if len(updated.GetFinalizers()) != 0 && (lease != 0 || options.TTL != 0) {
			return errors.NewBadRequest("TTL cannot be combined with finalizers")
		}
		// should set scopes
		updated.SetScopes(scopes)
		updated.SetResource(resource)
		if updated.GetID() != id {
			return errors.NewBadRequest("id cannot be changed")
		}
		completeDeletion := current.GetDeletionTimestamp() != nil && len(updated.GetFinalizers()) == 0
		if completeDeletion {
			txn := e.client.KV.Txn(ctx)
			txnResp, err := txn.
				If(
					keyHasRevision(preparedKey, currentversion),
				).
				Then(
					clientv3.OpDelete(preparedKey),
				).
				Else(
					clientv3.OpGet(preparedKey),
				).
				Commit()
			if err != nil {
				return errors.NewInternalError(err)
			}
			if !txnResp.Succeeded {
				getResp := (*clientv3.GetResponse)(txnResp.Responses[0].GetResponseRange())
				newcurrent, err := e.decodeGetResp(getResp, current, 0)
				if err != nil {
					return err
				}
				currentdata = newcurrent
				lease = getResp.Kvs[0].Lease
				continue
			}
			if err := store.CopyObject(updated, obj); err != nil {
				return errors.NewInternalError(err)
			}
			obj.SetResourceVersion(txnResp.Header.Revision)
			return nil
		}
		updated.SetResourceVersion(0)
		data, err := e.serializer.Encode(updated)
		if err != nil {
			return errors.NewInternalError(err)
		}
		if !bytes.Equal(data, currentdata) || options.TTL != 0 {
			putopts, err := e.ttlOpts(ctx, options.TTL)
			if err != nil {
				return err
			}
			if options.TTL == 0 {
				putopts = []clientv3.OpOption{clientv3.WithIgnoreLease()}
			}
			txn := e.client.KV.Txn(ctx)
			txnResp, err := txn.
				If(
					keyHasRevision(preparedKey, currentversion),
				).
				Then(
					clientv3.OpPut(preparedKey, string(data), putopts...),
				).
				Else(
					clientv3.OpGet(preparedKey),
				).
				Commit()
			if err != nil {
				return errors.NewInternalError(err)
			}
			if !txnResp.Succeeded {
				getResp := (*clientv3.GetResponse)(txnResp.Responses[0].GetResponseRange())
				newcurrent, err := e.decodeGetResp(getResp, current, 0)
				if err != nil {
					return err
				}
				currentdata = newcurrent
				lease = getResp.Kvs[0].Lease
				continue
			}
			currentversion = txnResp.Responses[0].GetResponsePut().
				Header.Revision
		}
		store.ResetObject(obj)
		if err := e.serializer.Decode(data, obj); err != nil {
			return errors.NewInternalError(err)
		}
		obj.SetResourceVersion(currentversion)
		return nil
	}
}

func validateSelectorRequirements(labelRequirements, fieldRequirements store.Requirements) error {
	if err := store.ValidateSelectorRequirements(labelRequirements, fieldRequirements); err != nil {
		return errors.NewBadRequest(err.Error())
	}
	return nil
}

func (s *etcdStoreCore) getCurrent(ctx context.Context, key string, into store.Object, rev int64) ([]byte, error) {
	getResp, err := s.client.KV.Get(ctx, key)
	if err != nil {
		return nil, errors.NewInternalError(err)
	}
	return s.decodeGetResp(getResp, into, rev)
}

func (s *etcdStoreCore) decodeGetResp(getResp *clientv3.GetResponse, into store.Object, rev int64) ([]byte, error) {
	if len(getResp.Kvs) == 0 {
		return nil, errors.NewNotFound(into.GetResource(), into.GetID())
	}
	kv := getResp.Kvs[0]
	if rev != 0 && kv.ModRevision < rev {
		return nil, errors.NewInvalid(into.GetResource(), into.GetID(),
			fmt.Errorf("resourceVersion %d is newer than current %d", rev, kv.ModRevision))
	}
	if into != nil {
		store.ResetObject(into)
		if err := s.serializer.Decode(kv.Value, into); err != nil {
			return nil, errors.NewInternalError(err)
		}
	}
	into.SetResourceVersion(kv.ModRevision)
	return kv.Value, nil
}

func (e *etcdStoreCore) directDelete(ctx context.Context, scopes []store.Scope, obj store.Object, resourceVersion int64) error {
	resource, err := e.schema.ResourceName(obj)
	if err != nil {
		return err
	}

	key := e.getkey(scopes, resource, obj.GetID())

	cmps := []clientv3.Cmp{}
	if resourceVersion != 0 {
		cmps = append(cmps, keyHasRevision(key, resourceVersion))
	}
	txnResp, err := e.client.KV.Txn(ctx).
		If(cmps...).
		Then(clientv3.OpGet(key), clientv3.OpDelete(key)).
		Else(clientv3.OpGet(key)).
		Commit()
	if err != nil {
		return errors.NewInternalError(err)
	}
	if !txnResp.Succeeded {
		getResp := txnResp.Responses[0].GetResponseRange()
		if len(getResp.Kvs) == 0 {
			log.V(4).
				Info("deletion failed because key not found", "key", key)
			return errors.NewNotFound(resource, obj.GetID())
		}
		log.V(4).
			Info("deletion failed because resourceVersion does not match", "key", key)
		return errors.NewConflict(resource, obj.GetID(), fmt.Errorf("resourceVersion %d does not match", resourceVersion))
	}
	// always not be empty
	if getResp := txnResp.Responses[0].GetResponseRange(); len(getResp.Kvs) != 0 {
		data := getResp.Kvs[0].Value
		if err := e.serializer.Decode(data, obj); err != nil {
			return errors.NewInternalError(err)
		}
	}
	// fill resourceVersion
	deleteResp := txnResp.Responses[1].GetResponseDeleteRange()
	if header := deleteResp.Header; header != nil {
		obj.SetResourceVersion(header.Revision)
	}
	return nil
}

func GetObjectField(obj any, field string) (any, bool, error) {
	if uns, ok := obj.(*store.Unstructured); ok {
		val, ok := store.GetNestedField(uns.Object, strings.ToLower(field))
		return val, ok, nil
	}
	v, err := store.EnforcePtr(obj)
	if err != nil {
		return nil, false, err
	}
	if v.Kind() != reflect.Struct {
		return nil, false, fmt.Errorf("expected struct, but got %v type", v.Type())
	}
	f := v.FieldByName(field)
	if !f.IsValid() {
		return nil, false, nil
	}
	return f.Interface(), true, nil
}

func SetObjectField(obj any, field string, value any) (bool, error) {
	if uns, ok := obj.(*store.Unstructured); ok {
		store.SetNestedField(uns.Object, value, strings.ToLower(field))
		return true, nil
	}
	v, err := store.EnforcePtr(obj)
	if err != nil {
		return false, err
	}
	if v.Kind() != reflect.Struct {
		return false, fmt.Errorf("expected struct, but got %v type", v.Type())
	}
	f := v.FieldByName(field)
	if !f.IsValid() {
		return false, nil
	}
	if !f.CanSet() {
		return false, fmt.Errorf("field %s is not settable", field)
	}
	f.Set(reflect.ValueOf(value))
	return true, nil
}

func CopyField(to, from any, field string) error {
	oldStatus, ok, err := GetObjectField(from, field)
	if err != nil {
		return err
	}
	if ok {
		if _, err := SetObjectField(to, field, oldStatus); err != nil {
			return err
		}
	}
	return nil
}
