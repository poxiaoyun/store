package inmemory

import (
	"context"
	"io"
	"slices"
	"sync"

	"golang.org/x/sync/errgroup"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/reflector"
)

// CacheStoreOptions configures the resource collections synchronized by a CacheStore.
type CacheStoreOptions struct {
	// Resources selects registered resources to cache across all scopes.
	// Empty delegates only authoritative operations.
	Resources []store.Object
}

// CacheStore serves explicitly cached Get/List requests from indexed local collections.
// Other operations delegate to the borrowed source. Construct with NewCacheStore and run
// the root once, then call WaitSync before serving cached reads. Scoped views
// share its lifetime. Close preserves the source.
type CacheStore struct {
	// Store delegates authoritative operations to the borrowed source.
	store.Store
	shared *cacheState
	scopes []store.Scope
}

type cacheState struct {
	resources map[string]*collection
	schema    *store.Schema
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.RWMutex
	started   bool
	done      chan struct{}
	changed   chan struct{}
}

var _ store.ClosableStore = (*CacheStore)(nil)

// NewCacheStore constructs a cache over an unscoped source without starting watches.
// Only the selected resources accept cached reads.
func NewCacheStore(source store.Store, options CacheStoreOptions) (*CacheStore, error) {
	if !source.Capabilities().Watch {
		return nil, errors.NewUnsupported("object cache requires initial Watch")
	}
	schema := source.Schema()
	ctx, cancel := context.WithCancel(context.Background())
	c := &cacheState{
		schema:    schema,
		resources: map[string]*collection{},
		ctx:       ctx,
		cancel:    cancel,
		done:      make(chan struct{}),
		changed:   make(chan struct{}),
	}
	for _, selected := range options.Resources {
		name, err := schema.ResourceName(selected)
		if err != nil {
			cancel()
			return nil, err
		}
		_, err = schema.Resource(name)
		if err != nil {
			cancel()
			return nil, err
		}
		if _, exists := c.resources[name]; exists {
			cancel()
			return nil, errors.NewBadRequest("duplicate cached resource: " + name)
		}
		c.resources[name] = nil
	}
	return &CacheStore{Store: source, shared: c}, nil
}

// Run synchronizes registered resources until ctx ends or Close is called.
// The root may run once. Each resource becomes readable after its initial Bookmark.
func (s *CacheStore) Run(ctx context.Context) error {
	s.shared.mu.Lock()
	if s.shared.started {
		s.shared.mu.Unlock()
		return errors.NewBadRequest("cache already started")
	}
	if s.shared.ctx.Err() != nil {
		s.shared.mu.Unlock()
		return io.ErrClosedPipe
	}
	s.shared.started = true
	defer close(s.shared.done)
	stop := context.AfterFunc(ctx, s.shared.cancel)
	defer stop()
	group, runctx := errgroup.WithContext(s.shared.ctx)
	for name := range s.shared.resources {
		group.Go(func() error {
			return s.syncResource(runctx, name)
		})
	}
	s.shared.mu.Unlock()
	err := group.Wait()
	s.shared.cancel()
	return err
}

func (s *CacheStore) syncResource(ctx context.Context, name string) error {
	observer, err := reflector.NewResourceReflector(s.Store, name, store.WithSubScopes())
	if err != nil {
		return err
	}
	definition, _ := s.shared.schema.Resource(name)
	var current *collection
	return observer.RunWithRebuild(ctx, reflector.ReflectorHandlerFuncs[store.Unstructured]{
		ReplaceFunc: func(ctx context.Context, objects []*store.Unstructured) error {
			next := newCollection(definition)
			for _, object := range objects {
				if err := next.apply(store.WatchEventCreate, object); err != nil {
					return err
				}
			}
			current = next
			s.shared.mu.Lock()
			s.shared.resources[name] = next
			close(s.shared.changed)
			s.shared.changed = make(chan struct{})
			s.shared.mu.Unlock()
			return nil
		},
		ApplyFunc: func(ctx context.Context, kind store.WatchEventType, object *store.Unstructured) error {
			return current.apply(kind, object)
		},
		InvalidateFunc: func(ctx context.Context, cause error) {
			s.shared.mu.Lock()
			s.shared.resources[name] = nil
			s.shared.mu.Unlock()
		},
	})
}

// WaitSync waits until every selected resource has a complete current snapshot.
// Call it after starting Run. It can be called again after a read returns
// ServiceUnavailable during rebuilding. Cancellation ends this wait only;
// closing the cache returns io.ErrClosedPipe.
func (s *CacheStore) WaitSync(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if s.shared.ctx.Err() != nil {
			return io.ErrClosedPipe
		}
		s.shared.mu.RLock()
		ready := true
		for _, resource := range s.shared.resources {
			if resource == nil {
				ready = false
				break
			}
		}
		changed := s.shared.changed
		s.shared.mu.RUnlock()
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.shared.ctx.Done():
			return io.ErrClosedPipe
		case <-changed:
		}
	}
}

// Close cancels and joins cache-owned tasks. It is idempotent, including before Run.
func (s *CacheStore) Close() error {
	s.shared.mu.Lock()
	s.shared.cancel()
	started := s.shared.started
	s.shared.mu.Unlock()
	if started {
		<-s.shared.done
	}
	return nil
}

// Scope derives a scoped view sharing the root cache and its tasks.
func (s *CacheStore) Scope(scopes ...store.Scope) store.Store {
	return &CacheStore{Store: s.Store.Scope(scopes...), shared: s.shared, scopes: append(slices.Clone(s.scopes), scopes...)}
}

// Get delegates authoritative reads and serves zero from the cache. Positive
// freshness requirements are unsupported because the cache has no read revision.
// An unsynchronized resource returns ServiceUnavailable without waiting.
func (s *CacheStore) Get(ctx context.Context, id string, object store.Object, options ...store.GetOption) error {
	request := store.ApplyGetOptions(options)
	if request.ResourceVersion == nil {
		return s.Store.Get(ctx, id, object, options...)
	}
	if *request.ResourceVersion > 0 {
		return errors.NewUnsupported("cache has no collection freshness version")
	}
	if *request.ResourceVersion < 0 {
		return errors.NewBadRequest("resourceVersion must not be negative")
	}
	resource, err := s.cachedResource(object)
	if err != nil {
		return err
	}
	return resource.getObject(ctx, s.scopes, id, object, request)
}

// List applies the complete Store query before pagination on one cached snapshot.
// An unsynchronized resource returns ServiceUnavailable without waiting.
func (s *CacheStore) List(ctx context.Context, list store.ObjectList, options ...store.ListOption) error {
	request := store.ApplyListOptions(options)
	if request.ResourceVersion == nil {
		return s.Store.List(ctx, list, options...)
	}
	if *request.ResourceVersion > 0 {
		return errors.NewUnsupported("cache has no collection freshness version")
	}
	if *request.ResourceVersion < 0 {
		return errors.NewBadRequest("resourceVersion must not be negative")
	}
	resource, err := s.cachedResource(list)
	if err != nil {
		return err
	}
	return resource.listObjects(ctx, s.scopes, list, request)
}

func (s *CacheStore) cachedResource(object any) (*collection, error) {
	name, err := s.shared.schema.ResourceName(object)
	if err != nil {
		return nil, err
	}
	if s.shared.ctx.Err() != nil {
		return nil, io.ErrClosedPipe
	}
	s.shared.mu.RLock()
	resource, exists := s.shared.resources[name]
	s.shared.mu.RUnlock()
	if !exists {
		return nil, errors.NewUnsupported("resource is not cached: " + name)
	}
	if resource == nil {
		return nil, errors.NewServiceUnavailable("cache is not synchronized: " + name)
	}
	return resource, nil
}
