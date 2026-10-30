package controller

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"slices"
	"time"

	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/log"
	"xiaoshiai.cn/store"
)

type Reconciler[T store.Object] interface {
	Sync(ctx context.Context, obj T) (Result, error)
	Remove(ctx context.Context, obj T) (Result, error)
}

type SimpleResourceReconciler[T store.Object] interface {
	Sync(ctx context.Context, obj T) error
	Remove(ctx context.Context, obj T) error
}

// BetterReconciler wraps the reconciler with some features.
// - reversion check,see below.
// - auto update the status after sync if the object is changed.
// - auto add/remove finalizer.
type BetterReconciler[T store.Object] struct {
	Options    BetterReconcilerOptions
	Client     store.Store
	Reconciler Reconciler[T]
}

type BetterReconcilerOptions struct {
	finalizer string
}

// WithFinalizer auto add finalizer to the object if it is not exist.
// it also remove the finalizer when the object is deleted and function Remove() success.
func WithFinalizer(finalizer string) BetterReconcilerOption {
	return func(o *BetterReconcilerOptions) {
		o.finalizer = finalizer
	}
}

type BetterReconcilerOption func(*BetterReconcilerOptions)

func NewBetterReconciler[T store.Object](r Reconciler[T], cli store.Store, options ...BetterReconcilerOption) *BetterReconciler[T] {
	opts := &BetterReconcilerOptions{}
	for _, opt := range options {
		opt(opts)
	}
	return &BetterReconciler[T]{Options: *opts, Reconciler: r, Client: cli}
}

func (r *BetterReconciler[T]) Initialize(ctx context.Context) error {
	if init, ok := r.Reconciler.(InitializeReconciler); ok {
		return init.Initialize(ctx)
	}
	return nil
}

// nolint: funlen,gocognit
func (r *BetterReconciler[T]) Reconcile(ctx context.Context, key ScopedKey) (Result, error) {
	log := log.FromContext(ctx).
		WithValues("key", key)

	log.Info("start reconcile")
	defer log.Info("finish reconcile")

	obj, _ := reflect.TypeAssert[T](reflect.New(reflect.TypeFor[T]().
		Elem()))

	condStorage := r.Client.Scope(key.Scopes()...)
	if err := condStorage.Get(ctx, key.ID, obj); err != nil {
		// if object not found , just ignore it
		// if a post handler needed after object deleted, consider to add a finalizer instead
		if errors.IsNotFound(err) {
			return Result{}, nil
		}
		return Result{}, err
	}

	finalizer := r.Options.finalizer
	if obj.GetDeletionTimestamp() != nil {
		log.Info("is being deleted")
		if finalizer != "" && !ContainsFinalizer(obj, finalizer) {
			return Result{}, nil
		}

		// remove object
		result, err := r.Reconciler.Remove(ctx, obj)
		if err != nil {
			return result, err
		}

		// remove finalizer
		if finalizer != "" && RemoveFinalizer(obj, finalizer) {
			if err := condStorage.Update(ctx, obj); err != nil {
				return Result{}, err
			}
		}
		return result, nil
	}

	// add finalizer
	if finalizer != "" && AddFinalizer(obj, finalizer) {
		if err := condStorage.Update(ctx, obj); err != nil {
			return Result{}, err
		}
	}

	// sync
	result, funcerr := r.Reconciler.Sync(ctx, obj)
	return result, funcerr
}

// ContainsFinalizer checks an Object that the provided finalizer is present.
func ContainsFinalizer(o store.Object, finalizer string) bool {
	return slices.Contains(o.GetFinalizers(), finalizer)
}

// RemoveFinalizer accepts an Object and removes the provided finalizer if present.
// It returns an indication of whether it updated the object's list of finalizers.
func RemoveFinalizer(o store.Object, finalizer string) (finalizersUpdated bool) {
	f := o.GetFinalizers()
	length := len(f)

	index := 0
	for i := range length {
		if f[i] == finalizer {
			continue
		}
		f[index] = f[i]
		index++
	}
	o.SetFinalizers(f[:index])
	return length != index
}

func AddFinalizer(o store.Object, finalizer string) (finalizersUpdated bool) {
	f := o.GetFinalizers()
	if slices.Contains(f, finalizer) {
		return false
	}
	o.SetFinalizers(append(f, finalizer))
	return true
}

var netErrRegexp = regexp.MustCompile(`(?i)(.*?)(read|write) (tcp|udp) [^ ]+->[^ ]+: (.+)`)

func CensorErrorStr(errStr string) string {
	if matches := netErrRegexp.FindStringSubmatch(errStr); matches != nil {
		return fmt.Sprintf("%s%s %s: %s", matches[1], matches[2], matches[3], matches[4])
	}
	return errStr
}

type ReQueueError struct {
	After time.Duration
}

func (r ReQueueError) Error() string {
	return fmt.Sprintf("retry after %s", r.After)
}

func ReQueue(after time.Duration) error {
	return ReQueueError{After: after}
}

func UnwrapReQueueError(err error) (Result, error) {
	if err == nil {
		return Result{}, nil
	}
	if requeue, ok := err.(ReQueueError); ok {
		return Result{Requeue: true, RequeueAfter: requeue.After}, nil
	}
	return Result{}, err
}
