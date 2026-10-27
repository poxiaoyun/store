package controller

import (
	"context"
	"errors"
	"fmt"

	"xiaoshiai.cn/common/log"
	"xiaoshiai.cn/store"
	storecache "xiaoshiai.cn/store/reflector"
)

type Source[K comparable] interface {
	Run(ctx context.Context, queue TypedQueue[K]) error
}

type SourceFunc[K comparable] func(ctx context.Context, queue TypedQueue[K]) error

func (f SourceFunc[K]) Run(ctx context.Context, queue TypedQueue[K]) error {
	return f(ctx, queue)
}

func NewStoreSource(storage store.Store, example store.Object, predicate ...Predicate[store.Object]) StoreSource[ScopedKey] {
	resource, err := storage.Schema().ResourceName(example)
	if err != nil {
		panic(err)
	}
	return NewCustomStoreSource(storage, resource, func(ctx context.Context, kind store.WatchEventType, obj store.Object) ([]ScopedKey, error) {
		return []ScopedKey{ScopedKeyFromObject(obj)}, nil
	}, predicate...)
}

type Predicate[T store.Object] func(kind store.WatchEventType, obj T) bool

func NewCustomStoreSource[T comparable](storage store.Store, resource string, keyfunc KeyFunc[T], predicate ...Predicate[store.Object]) StoreSource[T] {
	return StoreSource[T]{
		Store:     storage,
		Predicate: predicate,
		Resource:  resource,
		KeyFunc:   keyfunc,
	}
}

type KeyFunc[T comparable] func(ctx context.Context, kind store.WatchEventType, obj store.Object) ([]T, error)

type StoreSource[T comparable] struct {
	store.Store
	Resource  string
	Predicate []Predicate[store.Object]
	KeyFunc   KeyFunc[T]
}

func (s StoreSource[T]) Run(ctx context.Context, queue TypedQueue[T]) error {
	logger := log.FromContext(ctx).
		WithValues("resource", s.Resource)
	logger.Info("source start")
	ctx = log.NewContext(ctx, logger)
	return RunListWatchContext(ctx, s.Store, s.Resource, EventHandlerFunc[*store.Unstructured](func(ctx context.Context, event TypedWatchEvent[*store.Unstructured]) error {
		if event.Type == store.WatchEventBookmark {
			return nil
		}
		logger.Info("event", "kind", event.Type, "id", event.Object.GetID())

		for _, predicate := range s.Predicate {
			if !predicate(event.Type, event.Object) {
				return nil
			}
		}

		keys, err := s.KeyFunc(ctx, event.Type, event.Object)
		if err != nil {
			logger.Error(err, "key error")
			return nil
		}
		for i := range keys {
			queue.Add(keys[i])
		}
		return nil
	}))
}

// TypedWatchEvent is the ordered Store Watch event delivered to a typed handler.
// Object is zero for Bookmark events; ResourceVersion is the optional global
// checkpoint from the Store event.
type TypedWatchEvent[T any] struct {
	Type            store.WatchEventType
	Object          T
	ResourceVersion int64
}

// EventHandler consumes ordered Watch events.
type EventHandler[T any] interface {
	OnEvent(ctx context.Context, event TypedWatchEvent[T]) error
}

var _ EventHandler[any] = EventHandlerFunc[any](nil)

// EventHandlerFunc adapts a function to EventHandler.
type EventHandlerFunc[T any] func(ctx context.Context, event TypedWatchEvent[T]) error

// OnEvent implements EventHandler.
func (f EventHandlerFunc[T]) OnEvent(ctx context.Context, event TypedWatchEvent[T]) error {
	return f(ctx, event)
}

func RunListWatchContext(ctx context.Context, storage store.Store, resource string, handler EventHandler[*store.Unstructured]) error {
	return RunListWatch(ctx, storage, resource, true, handler)
}

func RunListWatch(ctx context.Context, storage store.Store, resource string, subScope bool, handler EventHandler[*store.Unstructured]) error {
	list := &store.List[store.Unstructured]{Resource: resource}
	options := []store.WatchOption{}
	if subScope {
		options = append(options, store.WithSubScopes())
	}
	reflector := storecache.NewReflector(storage, list, options...)
	return reflector.Run(ctx, NewReflectorEventHandler(handler))
}

func RunWatch(ctx context.Context, storage store.Store, resource string, handler EventHandler[*store.Unstructured], options ...store.WatchOption) error {
	log := log.FromContext(ctx)
	list := &store.List[store.Unstructured]{}
	list.SetResource(resource)

	watcher, err := storage.Watch(ctx, list, options...)
	if err != nil {
		return err
	}
	defer func() {
		watcher.Stop()
		log.Info("watcher stoped")
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.Events():
			if !ok {
				return fmt.Errorf("watcher channel closed")
			}
			if event.Error != nil {
				log.Error(event.Error, "watch error")
				return event.Error
			}
			typed := TypedWatchEvent[*store.Unstructured]{
				Type:            event.Type,
				ResourceVersion: event.ResourceVersion,
			}
			switch event.Type {
			case store.WatchEventCreate, store.WatchEventUpdate, store.WatchEventDelete:
				obj, ok := event.Object.(*store.Unstructured)
				if !ok {
					log.Error(errors.New("watch event value is not T"), "watch error")
					return errors.New("watch event value is not T")
				}
				log.V(5).
					Info("watch event", "type", event.Type, "id", obj.GetID(), "resource", obj.GetResource())
				typed.Object = obj
			case store.WatchEventBookmark:
				log.V(5).
					Info("watch bookmark", "resourceVersion", event.ResourceVersion)
			default:
				log.Info("unknown event type", "type", event.Type)
				continue
			}
			if err := handler.OnEvent(ctx, typed); err != nil {
				log.Error(err, "handle error")
				return err
			}
		}
	}
}

type WatchFuncSource[S any, T comparable] struct {
	WatchOptions []store.WatchOption
	Store        store.Store
	KeyFunc      func(ctx context.Context, kind store.WatchEventType, obj *S) ([]T, error)
}

func (s WatchFuncSource[S, T]) Run(ctx context.Context, queue TypedQueue[T]) error {
	log := log.FromContext(ctx)
	fn := EventHandlerFunc[*S](func(ctx context.Context, event TypedWatchEvent[*S]) error {
		if event.Type == store.WatchEventBookmark {
			return nil
		}
		keys, err := s.KeyFunc(ctx, event.Type, event.Object)
		if err != nil {
			log.Error(err, "key error")
			return nil
		}
		for i := range keys {
			log.Info("add key", "key", keys[i])
			queue.Add(keys[i])
		}
		return nil
	})
	return RunTypedListWatchContext(ctx, s.Store, fn, s.WatchOptions...)
}

func RunTypedListWatchContext[T any](ctx context.Context, storage store.Store, handler EventHandler[*T], options ...store.WatchOption) error {
	list := &store.List[T]{}
	reflector := storecache.NewReflector(storage, list, options...)
	return reflector.Run(ctx, NewReflectorEventHandler(handler))
}

func RunTypedWatch[T any](ctx context.Context, storage store.Store, handler EventHandler[*T], options ...store.WatchOption) error {
	log := log.FromContext(ctx)
	list := &store.List[T]{}
	watcher, err := storage.Watch(ctx, list, options...)
	if err != nil {
		return err
	}
	defer func() {
		watcher.Stop()
		log.Info("watcher stoped")
	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.Events():
			if !ok {
				return fmt.Errorf("watcher channel closed")
			}
			if event.Error != nil {
				log.Error(event.Error, "watch error")
				return event.Error
			}
			typed := TypedWatchEvent[*T]{
				Type:            event.Type,
				ResourceVersion: event.ResourceVersion,
			}
			switch event.Type {
			case store.WatchEventCreate, store.WatchEventUpdate, store.WatchEventDelete:
				obj, ok := any(event.Object).(*T)
				if !ok {
					log.Error(errors.New("watch event value is not *T"), "watch error")
					return errors.New("watch event value is not *T")
				}
				log.V(5).
					Info("watch event", "type", event.Type, "data", event.Object)
				typed.Object = obj
			case store.WatchEventBookmark:
				log.V(5).
					Info("watch bookmark", "resourceVersion", event.ResourceVersion)
			default:
				log.Info("unknown event type", "type", event.Type)
				continue
			}
			if err := handler.OnEvent(ctx, typed); err != nil {
				log.Error(err, "handle error")
				return err
			}
		}
	}
}

// ReflectorEventHandler turns authoritative Replace calls into object deltas
// for controller handlers while retaining the last published snapshot.
type ReflectorEventHandler[T any] struct {
	Handler EventHandler[*T]
	Objects map[string]*T
}

// NewReflectorEventHandler constructs a Reflector handler for ordered controller events.
func NewReflectorEventHandler[T any](handler EventHandler[*T]) *ReflectorEventHandler[T] {
	return &ReflectorEventHandler[T]{Handler: handler, Objects: map[string]*T{}}
}

// Replace implements cache.ReflectorHandler.
func (h *ReflectorEventHandler[T]) Replace(ctx context.Context, objects []*T) error {
	next := make(map[string]*T, len(objects))
	for _, object := range objects {
		stored := any(object).(store.Object)
		next[storecache.ReflectorObjectKey(stored)] = object
	}
	for key, old := range h.Objects {
		if _, exists := next[key]; exists {
			continue
		}
		if err := h.Handler.OnEvent(ctx, TypedWatchEvent[*T]{Type: store.WatchEventDelete, Object: old}); err != nil {
			return err
		}
	}
	for key, current := range next {
		old, exists := h.Objects[key]
		if !exists {
			if err := h.Handler.OnEvent(ctx, TypedWatchEvent[*T]{Type: store.WatchEventCreate, Object: current}); err != nil {
				return err
			}
			continue
		}
		oldObject := any(old).(store.Object)
		currentObject := any(current).(store.Object)
		if oldObject.GetUID() == currentObject.GetUID() && oldObject.GetResourceVersion() == currentObject.GetResourceVersion() {
			continue
		}
		if err := h.Handler.OnEvent(ctx, TypedWatchEvent[*T]{Type: store.WatchEventUpdate, Object: current}); err != nil {
			return err
		}
	}
	h.Objects = next
	return nil
}

// Apply implements cache.ReflectorHandler.
func (h *ReflectorEventHandler[T]) Apply(ctx context.Context, eventType store.WatchEventType, object *T) error {
	stored := any(object).(store.Object)
	if err := h.Handler.OnEvent(ctx, TypedWatchEvent[*T]{Type: eventType, Object: object}); err != nil {
		return err
	}
	key := storecache.ReflectorObjectKey(stored)
	if eventType == store.WatchEventDelete {
		delete(h.Objects, key)
	} else {
		h.Objects[key] = object
	}
	return nil
}

// Invalidate implements cache.ReflectorHandler. The last authoritative state
// remains active until the replacement snapshot is published.
func (*ReflectorEventHandler[T]) Invalidate(context.Context, error) {}
