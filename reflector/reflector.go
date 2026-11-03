package reflector

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	commonerrors "xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/retry"
	"xiaoshiai.cn/store"
)

// ReflectorHandler owns the authoritative state populated by a Reflector.
type ReflectorHandler[T any] interface {
	// Replace atomically publishes one complete initial snapshot.
	Replace(ctx context.Context, objects []*T) error
	// Apply publishes one ordered live object mutation.
	Apply(ctx context.Context, eventType store.WatchEventType, object *T) error
	// Invalidate marks the published state unavailable after Watch continuity is lost.
	Invalidate(ctx context.Context, cause error)
}

// ReflectorHandlerFuncs adapts functions to ReflectorHandler.
type ReflectorHandlerFuncs[T any] struct {
	ReplaceFunc    func(ctx context.Context, objects []*T) error
	ApplyFunc      func(ctx context.Context, eventType store.WatchEventType, object *T) error
	InvalidateFunc func(ctx context.Context, cause error)
}

// Replace implements ReflectorHandler.
func (f ReflectorHandlerFuncs[T]) Replace(ctx context.Context, objects []*T) error {
	return f.ReplaceFunc(ctx, objects)
}

// Apply implements ReflectorHandler.
func (f ReflectorHandlerFuncs[T]) Apply(ctx context.Context, eventType store.WatchEventType, object *T) error {
	return f.ApplyFunc(ctx, eventType, object)
}

// Invalidate implements ReflectorHandler.
func (f ReflectorHandlerFuncs[T]) Invalidate(ctx context.Context, cause error) {
	f.InvalidateFunc(ctx, cause)
}

// Reflector turns the Store initial-Watch protocol into authoritative Replace
// and ordered Apply calls. A Reflector may be run once.
type Reflector[T any] struct {
	storage store.Store
	list    store.ObjectList
	decode  func(store.Object) (*T, error)
	options []store.WatchOption

	synced chan struct{}
}

// NewReflector constructs a Reflector for list's resource and selectors.
func NewReflector[T any](storage store.Store, list *store.List[T], options ...store.WatchOption) *Reflector[T] {
	return &Reflector[T]{
		storage: storage,
		list:    list,
		options: options,
		synced:  make(chan struct{}),
		decode: func(object store.Object) (*T, error) {
			typed, ok := any(object).(*T)
			if !ok {
				return nil, fmt.Errorf("watch event object has type %T, want *T", object)
			}
			return typed, nil
		},
	}
}

// Synced closes after the first authoritative snapshot has been published.
func (r *Reflector[T]) Synced() <-chan struct{} {
	return r.synced
}

// Run watches until ctx is canceled, resuming from a positive global
// checkpoint when possible and rebuilding from an initial Watch otherwise.
func (r *Reflector[T]) Run(ctx context.Context, handler ReflectorHandler[T]) error {
	return r.run(ctx, handler, false)
}

// RunWithRebuild invalidates on every stream interruption and publishes a new
// initial snapshot before making state available again. It runs once until canceled.
func (r *Reflector[T]) RunWithRebuild(ctx context.Context, handler ReflectorHandler[T]) error {
	return r.run(ctx, handler, true)
}

func (r *Reflector[T]) run(ctx context.Context, handler ReflectorHandler[T], rebuild bool) error {
	if !r.storage.Capabilities().Watch {
		return commonerrors.NewUnsupported("reflector requires Store Watch support")
	}

	checkpoint := int64(0)
	valid := false
	firstSync := true
	var terminal error
	err := retry.OnError(ctx, func(ctx context.Context) error {
		err := r.watch(ctx, handler, &checkpoint, &valid, &firstSync)
		if ctx.Err() != nil {
			return nil
		}
		if rebuild || commonerrors.IsResourceExpired(err) || checkpoint == 0 {
			checkpoint = 0
			if valid {
				handler.Invalidate(ctx, err)
				valid = false
			}
		}
		if commonerrors.IsUnsupported(err) || commonerrors.IsCode(err, 400) {
			terminal = err
			return nil
		}
		return err
	})
	if terminal != nil {
		return terminal
	}
	if ctx.Err() != nil {
		return nil
	}
	return err
}

func (r *Reflector[T]) watch(
	ctx context.Context,
	handler ReflectorHandler[T],
	checkpoint *int64,
	valid *bool,
	firstSync *bool,
) error {
	initial := *checkpoint == 0
	options := append([]store.WatchOption{}, r.options...)
	if initial {
		options = append(options, store.WithSendInitialEvents())
	} else {
		options = append(options, store.WithResourceVersion(*checkpoint))
	}
	watcher, err := r.storage.Watch(ctx, r.list, options...)
	if err != nil {
		return err
	}
	defer watcher.Stop()

	staging := map[string]*T{}
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.Events():
			if !ok {
				return fmt.Errorf("watcher channel closed")
			}
			if event.Error != nil {
				return event.Error
			}
			if event.Type == store.WatchEventBookmark {
				if initial {
					objects := make([]*T, 0, len(staging))
					for _, object := range staging {
						objects = append(objects, object)
					}
					if err := handler.Replace(ctx, objects); err != nil {
						return err
					}
					initial = false
					*valid = true
					if *firstSync {
						close(r.synced)
						*firstSync = false
					}
				}
				if event.ResourceVersion > 0 {
					*checkpoint = event.ResourceVersion
				}
				continue
			}

			object, err := r.decode(event.Object)
			if err != nil {
				return err
			}
			if initial {
				if err := applyReflectorSnapshotEvent(staging, event.Type, object); err != nil {
					return err
				}
				continue
			}
			if err := handler.Apply(ctx, event.Type, object); err != nil {
				return err
			}
			if event.ResourceVersion > 0 {
				*checkpoint = event.ResourceVersion
			}
		}
	}
}

func applyReflectorSnapshotEvent[T any](objects map[string]*T, eventType store.WatchEventType, object *T) error {
	stored, ok := any(object).(store.Object)
	if !ok {
		return fmt.Errorf("watch event object has type %T, want store.Object", object)
	}
	key := ReflectorObjectKey(stored)
	switch eventType {
	case store.WatchEventCreate, store.WatchEventUpdate:
		objects[key] = object
	case store.WatchEventDelete:
		delete(objects, key)
	default:
		return fmt.Errorf("initial Watch event type %q is not an object mutation", eventType)
	}
	return nil
}

// ReflectorObjectKey identifies one object by resource, scopes, and ID.
func ReflectorObjectKey(object store.Object) string {
	var key strings.Builder
	key.WriteString(strconv.Quote(object.GetResource()))
	for _, scope := range object.GetScopes() {
		key.WriteByte('/')
		key.WriteString(strconv.Quote(scope.Resource))
		key.WriteByte('/')
		key.WriteString(strconv.Quote(scope.Name))
	}
	key.WriteByte('/')
	key.WriteString(strconv.Quote(object.GetID()))
	return key.String()
}

// NewResourceReflector observes a registered resource using its schema's Go type
// for backend decoding, then publishes its portable object representation.
func NewResourceReflector(storage store.Store, resource string, options ...store.WatchOption) (*Reflector[store.Unstructured], error) {
	object, err := storage.Schema().NewObject(resource)
	if err != nil {
		return nil, err
	}
	items := reflect.New(reflect.SliceOf(reflect.TypeOf(object).Elem())).Interface()
	list := &resourceList{List: store.List[store.Unstructured]{Resource: resource}, Items: items}
	return &Reflector[store.Unstructured]{
		storage: storage,
		list:    list,
		options: options,
		synced:  make(chan struct{}),
		decode:  store.ToUnstructured,
	}, nil
}

type resourceList struct {
	store.List[store.Unstructured]
	Items any
}
