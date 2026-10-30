package controller

import (
	"context"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"k8s.io/client-go/util/workqueue"
	"xiaoshiai.cn/common/log"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/lease"
)

type ScopedKey struct {
	ID     string
	Prefix string
}

func ScopedKeyFromObject(obj store.Object) ScopedKey {
	return NewScopedKey(obj.GetScopes(), obj.GetID())
}

func NewScopedKey(scopes []store.Scope, id string) ScopedKey {
	return ScopedKey{ID: id, Prefix: EncodeScopes(scopes)}
}

func (s ScopedKey) Scopes() []store.Scope {
	return DecodeScopes(s.Prefix)
}

type ResourceScopedKey struct {
	Resource string
	Name     string
	Prefix   string
}

func EncodeScopes(scopes []store.Scope) string {
	var ret strings.Builder
	for _, scope := range scopes {
		ret.WriteString("/")
		ret.WriteString(scope.Resource)
		ret.WriteString("/")
		ret.WriteString(scope.Name)
	}
	return ret.String()
}

func EncodeReference(ref store.ObjectReference) string {
	sb := strings.Builder{}
	for _, scope := range ref.Scopes {
		sb.WriteString("/")
		sb.WriteString(scope.Resource)
		sb.WriteString("/")
		sb.WriteString(scope.Name)
	}
	if ref.ID != "" {
		sb.WriteString("/")
		sb.WriteString(ref.ID)
	}
	return store.JSONPointerEscape(sb.String())
}

func DecodeReference(ref string) store.ObjectReference {
	if len(ref) == 0 {
		return store.ObjectReference{}
	}
	ref = store.JSONPointerUnescape(ref)
	parts := strings.Split(ref, "/")
	if parts[0] == "" {
		parts = parts[1:]
	}
	ret := store.ObjectReference{}
	for i := 0; i < len(parts)-1; i += 2 {
		ret.Scopes = append(ret.Scopes, store.Scope{
			Resource: parts[i],
			Name:     parts[i+1],
		})
	}
	if len(parts)%2 == 1 {
		ret.ID = parts[len(parts)-1]
	}
	return ret
}

func DecodeScopes(scopes string) []store.Scope {
	if len(scopes) == 0 {
		return nil
	}
	ret := []store.Scope{}
	parts := strings.Split(scopes, "/")
	if parts[0] == "" {
		parts = parts[1:]
	}
	for i := 0; i < len(parts); i += 2 {
		ret = append(ret, store.Scope{
			Resource: parts[i],
			Name:     parts[i+1],
		})
	}
	return ret
}

type TypedReconciler[T any] interface {
	Reconcile(ctx context.Context, key T) (Result, error)
}

type Controller = TypedController[ScopedKey]

// InitializeReconciler is an optional interface that can be implemented by a Reconciler to
type InitializeReconciler interface {
	Initialize(ctx context.Context) error
}

var _ TypedReconciler[any] = TypedReconcilerFunc[any](nil)

type TypedReconcilerFunc[T any] func(ctx context.Context, key T) (Result, error)

func (f TypedReconcilerFunc[T]) Reconcile(ctx context.Context, key T) (Result, error) {
	return f(ctx, key)
}

type Result struct {
	Requeue      bool
	RequeueAfter time.Duration
}

// ControllerOptions controls worker concurrency, leadership and scheduling.
type ControllerOptions[T comparable] struct {
	// Concurrent bounds active workers; values below one use one worker.
	Concurrent int
	// LeaderElection optionally restricts execution to the current leader.
	LeaderElection lease.LeaderElection
	// RateLimiter computes failed-item retry delays; nil uses the default policy.
	RateLimiter workqueue.TypedRateLimiter[T]
	// RateLimit optionally limits reconciliation attempts across all workers.
	// Nil allows ready work to run as soon as a worker is available.
	RateLimit *rate.Limiter
}

// ControllerOption configures a newly constructed controller.
type ControllerOption[T comparable] func(*ControllerOptions[T])

// WithConcurrent sets the worker count; values below one use one worker.
func WithConcurrent[T comparable](concurrent int) ControllerOption[T] {
	return func(o *ControllerOptions[T]) {
		o.Concurrent = concurrent
	}
}

// WithRateLimit installs a token bucket with the given attempts per second and
// burst capacity. Each controller initialized with this option gets its own bucket.
func WithRateLimit[T comparable](qps rate.Limit, burst int) ControllerOption[T] {
	return func(o *ControllerOptions[T]) {
		o.RateLimit = rate.NewLimiter(qps, burst)
	}
}

func WithLeaderElection[T comparable](leader lease.LeaderElection) ControllerOption[T] {
	return func(o *ControllerOptions[T]) {
		o.LeaderElection = leader
	}
}

func NewController(name string, sync TypedReconciler[ScopedKey], options ...ControllerOption[ScopedKey]) *TypedController[ScopedKey] {
	return NewTypedController(name, sync, options...)
}

// NewTypedController constructs a keyed controller with one worker and no rate cap by default.
func NewTypedController[T comparable](name string, sync TypedReconciler[T], options ...ControllerOption[T]) *TypedController[T] {
	opts := ControllerOptions[T]{}
	for _, opt := range options {
		opt(&opts)
	}
	opts.Concurrent = max(opts.Concurrent, 1)
	if sync == nil {
		panic("sync function is required")
	}
	c := &TypedController[T]{
		name:     name,
		options:  opts,
		queue:    NewDefaultTypedQueue(name, opts.RateLimiter),
		syncFunc: sync,
	}
	return c
}

type TypedController[T comparable] struct {
	name     string
	options  ControllerOptions[T]
	sources  []Source[T]
	queue    TypedQueue[T]
	syncFunc TypedReconciler[T]
}

func (h *TypedController[T]) Watch(source ...Source[T]) *TypedController[T] {
	h.sources = append(h.sources, source...)
	return h
}

func (h *TypedController[T]) Name() string {
	return h.name
}

func (h *TypedController[T]) Run(ctx context.Context) error {
	logger := log.FromContext(ctx).
		WithValues("controller", h.name)

	ctx = log.NewContext(ctx, logger)
	if h.options.LeaderElection != nil {
		return h.options.LeaderElection.OnLeader(ctx, func(ctx context.Context) error {
			logger.Info("starting controller on leader")
			return h.run(ctx)
		})
	} else {
		logger.Info("starting controller")
		return h.run(ctx)
	}
}

func (h *TypedController[T]) run(ctx context.Context) error {
	if init, ok := h.syncFunc.(InitializeReconciler); ok {
		if err := init.Initialize(ctx); err != nil {
			return err
		}
	}
	eg, ctx := errgroup.WithContext(ctx)
	// watch sources
	for _, source := range h.sources {
		eg.Go(func() error {
			return source.Run(ctx, h.queue)
		})
	}
	// run queue consumer
	eg.Go(func() error {
		return RunQueueConsumer(ctx, h.queue, h.reconcile, h.options.Concurrent)
	})
	return eg.Wait()
}

func (h *TypedController[T]) reconcile(ctx context.Context, key T) (Result, error) {
	if h.options.RateLimit != nil {
		if err := h.options.RateLimit.Wait(ctx); err != nil {
			return Result{}, err
		}
	}
	return h.syncFunc.Reconcile(ctx, key)
}

// RunQueueConsumer processes ready keys with bounded concurrency and no rate cap.
// Result and error handling determine whether a key is forgotten or requeued.
func RunQueueConsumer[T comparable](ctx context.Context, queue TypedQueue[T], syncfunc func(ctx context.Context, key T) (Result, error), concurrent int) error {
	go func() {
		<-ctx.Done()
		queue.ShutDown()
	}()

	log := log.FromContext(ctx)

	// get item from queue and process
	wg := sync.WaitGroup{}
	wg.Add(concurrent)
	for range concurrent {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					val, stop := queue.Get()
					if stop {
						queue.Done(val)
						return
					}
					result, err := syncfunc(ctx, val)
					if err != nil {
						log.Error(err, "sync", "key", val)
						if result.Requeue {
							// if retry wait is longer than requeueAfter, use requeueAfter
							// the case it when a resource is expiring but the retry wait is longer than time to expire
							// leads to a resource not being processed when expired immediately
							if nexttry := min(queue.When(val), result.RequeueAfter); nexttry > 0 {
								queue.AddAfter(val, nexttry)
							} else {
								queue.AddRateLimited(val)
							}
						} else {
							queue.AddRateLimited(val)
						}
					} else {
						if result.Requeue {
							queue.AddAfter(val, result.RequeueAfter)
						} else {
							queue.Forget(val)
						}
					}
					queue.Done(val)
				}
			}
		}()
	}
	wg.Wait()
	return nil
}
