package strategy

import (
	"context"

	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
)

var _ store.PingableStore = &StrategyStore{}

type StrategyStore struct {
	Store    store.Store
	Stratage Strategy
}

// Schema implements store.Store.
func (s *StrategyStore) Schema() *store.Schema {
	return s.Store.Schema()
}

// Capabilities implements store.Store. Wrapper capabilities are intentionally
// conservative until the wrapper has its own conformance coverage.
func (s *StrategyStore) Capabilities() store.Capabilities {
	return store.Capabilities{}
}

func (s *StrategyStore) Ping(ctx context.Context) error {
	pinger, ok := s.Store.(store.Pinger)
	if !ok {
		return errors.NewNotImplemented("underlying store does not support ping")
	}
	return pinger.Ping(ctx)
}

// PatchBatch implements store.Store.
func (s *StrategyStore) PatchBatch(ctx context.Context, obj store.ObjectList, patch store.PatchBatch, opts ...store.PatchBatchOption) error {
	return s.Store.PatchBatch(ctx, obj, patch, opts...)
}

// DeleteBatch implements store.Store.
func (s *StrategyStore) DeleteBatch(ctx context.Context, obj store.ObjectList, opts ...store.DeleteBatchOption) error {
	return s.Store.DeleteBatch(ctx, obj, opts...)
}

type FinishFunc func(ctx context.Context, success bool)

type Strategy struct {
	BeforeCreate        func(ctx context.Context, obj store.Object) (FinishFunc, error)
	BeforeUpdate        func(ctx context.Context, obj, old store.Object) (FinishFunc, error)
	AfterDelete         func(obj store.Object)
	DeletionPropagation store.DeletionPropagation
}

// Count implements store.Store.
func (s *StrategyStore) Count(ctx context.Context, obj store.Object, opts ...store.CountOption) (int, error) {
	return s.Store.Count(ctx, obj, opts...)
}

// Create implements store.Store.
func (s *StrategyStore) Create(ctx context.Context, obj store.Object, opts ...store.CreateOption) error {
	if s.Stratage.BeforeCreate == nil {
		return s.Store.Create(ctx, obj, opts...)
	}
	finish, err := s.Stratage.BeforeCreate(ctx, obj)
	if err != nil {
		return err
	}
	if err := s.Store.Create(ctx, obj, opts...); err != nil {
		finish(ctx, false)
		return err
	}
	finish(ctx, true)
	return nil
}

// Delete implements store.Store.
func (s *StrategyStore) Delete(ctx context.Context, obj store.Object, opts ...store.DeleteOption) error {
	if s.Stratage.DeletionPropagation != "" {
		opts = append(opts, store.WithPropagation(s.Stratage.DeletionPropagation))
	}
	if err := s.Store.Delete(ctx, obj, opts...); err != nil {
		return err
	}
	if s.Stratage.AfterDelete != nil {
		s.Stratage.AfterDelete(obj)
	}
	return nil
}

// Get implements store.Store.
func (s *StrategyStore) Get(ctx context.Context, name string, obj store.Object, opts ...store.GetOption) error {
	return s.Store.Get(ctx, name, obj, opts...)
}

// List implements store.Store.
func (s *StrategyStore) List(ctx context.Context, list store.ObjectList, opts ...store.ListOption) error {
	return s.Store.List(ctx, list, opts...)
}

// Patch implements store.Store.
func (s *StrategyStore) Patch(ctx context.Context, obj store.Object, patch store.Patch, opts ...store.PatchOption) error {
	return s.Store.Patch(ctx, obj, patch, opts...)
}

// Scope implements store.Store.
func (s *StrategyStore) Scope(scope ...store.Scope) store.Store {
	return &StrategyStore{Store: s.Store.Scope(scope...), Stratage: s.Stratage}
}

// Status implements store.Store.
func (s *StrategyStore) Status() store.StatusStorage {
	return s.Store.Status()
}

// Update implements store.Store.
func (s *StrategyStore) Update(ctx context.Context, obj store.Object, opts ...store.UpdateOption) error {
	if s.Stratage.BeforeUpdate == nil {
		return s.Store.Update(ctx, obj, opts...)
	}
	old := store.NewObject(obj)
	if err := s.Store.Get(ctx, obj.GetID(), old); err != nil {
		return err
	}
	finish, err := s.Stratage.BeforeUpdate(ctx, obj, old)
	if err != nil {
		return err
	}
	if err := s.Store.Update(ctx, obj, opts...); err != nil {
		finish(ctx, false)
		return err
	}
	finish(ctx, true)
	return nil
}

// Watch implements store.Store.
func (s *StrategyStore) Watch(ctx context.Context, obj store.ObjectList, opts ...store.WatchOption) (store.Watcher, error) {
	return s.Store.Watch(ctx, obj, opts...)
}
