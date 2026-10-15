package etcdcache

import (
	"context"
	stderrors "errors"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/client/v3/kubernetes"
	"golang.org/x/sync/singleflight"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	cacherstorage "k8s.io/apiserver/pkg/storage/cacher"
	"k8s.io/apiserver/pkg/storage/etcd3"
	etcdfeature "k8s.io/apiserver/pkg/storage/feature"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	"k8s.io/apiserver/pkg/storage/value/encrypt/identity"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
)

var errResourceRegistryClosed = stderrors.New("etcd cache resource registry is closed")

// resourceBackend is the part of the Kubernetes storage contract used by this
// package. Keeping it local prevents unrelated storage.Interface methods from
// becoming part of the resource module's interface.
type resourceBackend interface {
	Create(ctx context.Context, key string, obj, out runtime.Object, ttl uint64) error
	Delete(
		ctx context.Context,
		key string,
		out runtime.Object,
		preconditions *storage.Preconditions,
		validateDeletion storage.ValidateObjectFunc,
		cachedExistingObject runtime.Object,
		opts storage.DeleteOptions,
	) error
	Watch(ctx context.Context, key string, opts storage.ListOptions) (watch.Interface, error)
	Get(ctx context.Context, key string, opts storage.GetOptions, objPtr runtime.Object) error
	GetList(ctx context.Context, key string, opts storage.ListOptions, listObj runtime.Object) error
	GuaranteedUpdate(
		ctx context.Context,
		key string,
		destination runtime.Object,
		ignoreNotFound bool,
		preconditions *storage.Preconditions,
		tryUpdate storage.UpdateFunc,
		cachedExistingObject runtime.Object,
	) error
}

var _ resourceBackend = (*cacherstorage.CacheDelegator)(nil)

// resourceDB owns one resource's backend and all background cache work.
type resourceDB struct {
	storage  resourceBackend
	resource schema.GroupResource
	ready    func(context.Context) error
	destroy  func()
}

func (r *resourceDB) waitReady(ctx context.Context) error {
	return r.ready(ctx)
}

func (r *resourceDB) Close() {
	if r != nil && r.destroy != nil {
		r.destroy()
	}
}

type resourceFactory func(
	ctx context.Context,
	resource schema.GroupResource,
	indexFields []string,
) (*resourceDB, error)

// resourceRegistry serializes construction per resource without blocking
// unrelated resources, and owns the lifetime of every constructed resourceDB.
type resourceRegistry struct {
	ctx         context.Context
	cancel      context.CancelFunc
	indexFields map[string][]string
	factory     resourceFactory
	onClose     func()

	mu        sync.RWMutex
	resources map[string]*resourceDB
	closed    bool
	closeDone chan struct{}
	builds    sync.WaitGroup
	group     singleflight.Group

	stopContextClose func() bool
}

func newResourceRegistry(
	ctx context.Context,
	indexFields map[string][]string,
	factory resourceFactory,
	onClose func(),
) *resourceRegistry {
	if ctx == nil {
		ctx = context.Background()
	}
	lifecycleCtx, cancel := context.WithCancel(ctx)
	fieldsCopy := make(map[string][]string, len(indexFields))
	for resource, fields := range indexFields {
		fieldsCopy[resource] = append([]string(nil), fields...)
	}
	registry := &resourceRegistry{
		ctx:         lifecycleCtx,
		cancel:      cancel,
		indexFields: fieldsCopy,
		factory:     factory,
		onClose:     onClose,
		resources:   make(map[string]*resourceDB),
		closeDone:   make(chan struct{}),
	}
	registry.stopContextClose = context.AfterFunc(lifecycleCtx, registry.Close)
	return registry
}

func (r *resourceRegistry) get(resource string) (*resourceDB, error) {
	r.mu.RLock()
	if r.closed {
		r.mu.RUnlock()
		return nil, errResourceRegistryClosed
	}
	if db, ok := r.resources[resource]; ok {
		r.mu.RUnlock()
		return db, nil
	}
	r.mu.RUnlock()

	result, err, _ := r.group.Do(resource, func() (any, error) {
		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			return nil, errResourceRegistryClosed
		}
		if db, ok := r.resources[resource]; ok {
			r.mu.Unlock()
			return db, nil
		}
		fields := append([]string(nil), r.indexFields[resource]...)
		r.builds.Add(1)
		r.mu.Unlock()
		defer r.builds.Done()

		groupResource := schema.GroupResource{Resource: resource}
		db, err := r.factory(r.ctx, groupResource, fields)
		if err != nil {
			return nil, fmt.Errorf("open resource %q: %w", resource, err)
		}
		if db == nil {
			return nil, fmt.Errorf("open resource %q: factory returned nil", resource)
		}

		r.mu.Lock()
		if r.closed {
			r.mu.Unlock()
			db.Close()
			return nil, errResourceRegistryClosed
		}
		r.resources[resource] = db
		r.mu.Unlock()
		return db, nil
	})
	if err != nil {
		return nil, err
	}
	return result.(*resourceDB), nil
}

func (r *resourceRegistry) Close() {
	r.mu.Lock()
	if r.closed {
		done := r.closeDone
		r.mu.Unlock()
		<-done
		return
	}
	r.closed = true
	stopContextClose := r.stopContextClose
	cancel := r.cancel
	r.mu.Unlock()

	cancel()
	if stopContextClose != nil {
		stopContextClose()
	}
	r.builds.Wait()

	r.mu.Lock()
	resources := make([]*resourceDB, 0, len(r.resources))
	for _, db := range r.resources {
		resources = append(resources, db)
	}
	r.resources = nil
	onClose := r.onClose
	r.onClose = nil
	r.mu.Unlock()

	for _, db := range resources {
		db.Close()
	}
	if onClose != nil {
		onClose()
	}
	close(r.closeDone)
}

func (r *resourceRegistry) Done() <-chan struct{} {
	return r.closeDone
}

func newResourceStorage(
	ctx context.Context,
	cli *kubernetes.Client,
	prefix string,
	groupResource schema.GroupResource,
	indexFields []string,
) (*resourceDB, error) {
	transformer := identity.NewEncryptCheckTransformer()
	leaseConfig := etcd3.NewDefaultLeaseManagerConfig()
	newFunc := func() runtime.Object { return &StorageObject{} }
	newListFunc := func() runtime.Object { return &StorageObjectList{} }
	codec := SimpleJsonCodec{}
	versioner := APIObjectVersioner{}
	resourcePrefix := "/" + groupResource.String()

	decoder := etcd3.NewDefaultDecoder(codec, versioner)
	compactor := etcd3.NewCompactor(cli.Client, time.Hour, clock.RealClock{}, nil)
	rawStorage, err := etcd3.New(
		cli,
		compactor,
		codec,
		newFunc,
		newListFunc,
		prefix,
		resourcePrefix,
		groupResource,
		transformer,
		leaseConfig,
		decoder,
		versioner,
	)
	if err != nil {
		return nil, fmt.Errorf("create etcd storage: %w", err)
	}
	// The internal reflector uses List+Watch because the streaming WatchList
	// mode can terminate before all initial events have reached the cache.
	reflectorStorage := &noWatchListStorage{Interface: rawStorage}
	cacher, err := cacherstorage.NewCacherFromConfig(cacherstorage.Config{
		Storage:             reflectorStorage,
		Versioner:           versioner,
		GroupResource:       groupResource,
		ResourcePrefix:      resourcePrefix,
		KeyFunc:             ScopesObjectKeyFunc,
		NewFunc:             newFunc,
		NewListFunc:         newListFunc,
		GetAttrsFunc:        GetAttrsFunc(indexFields),
		Codec:               codec,
		Indexers:            ptr.To(IndexerFromFields(indexFields)),
		EventsHistoryWindow: storagebackend.DefaultEventsHistoryWindow,
	})
	if err != nil {
		return nil, fmt.Errorf("create resource cache: %w", err)
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.ConsistentListFromCache) ||
		utilfeature.DefaultFeatureGate.Enabled(features.WatchList) {
		etcdfeature.DefaultFeatureSupportChecker.CheckClient(ctx, cli, storage.RequestWatchProgress)
	}
	delegator := cacherstorage.NewCacheDelegator(cacher, rawStorage)
	return &resourceDB{
		storage:  delegator,
		resource: groupResource,
		ready:    cacher.Wait,
		destroy: func() {
			delegator.Stop()
			cacher.Stop()
		},
	}, nil
}

// noWatchListStorage opts the internal reflector out of WatchList streaming.
type noWatchListStorage struct {
	storage.Interface
}

func (*noWatchListStorage) IsWatchListSemanticsUnSupported() bool {
	return true
}
