package lease

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	commonerrors "xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/meta"
	"xiaoshiai.cn/store"
)

// AddToStoreSchema registers the Lease resource.
func AddToStoreSchema(schema *store.Schema) error {
	return schema.Register(&Lease{}, store.ResourceSchema{})
}

// NewStoreLocker returns a distributed Locker backed by any Store.
func NewStoreLocker(storage store.Store, options Options) Locker {
	if options.Identity == "" {
		options.Identity = uuid.NewString()
	}
	if options.LeaseDuration == 0 {
		options.LeaseDuration = 30 * time.Second
	}
	if options.RetryPeriod == 0 {
		options.RetryPeriod = options.LeaseDuration / 3
	}
	return &storeLocker{storage: storage, options: options}
}

type storeLocker struct {
	storage store.Store
	options Options
}

func (l *storeLocker) WithLock(ctx context.Context, name string, operation func(context.Context) error) error {
	current, err := l.acquire(ctx, name)
	if err != nil {
		return err
	}
	operationContext, cancel := context.WithCancel(ctx)
	defer cancel()
	operationResult := make(chan error, 1)
	go func() {
		operationResult <- operation(operationContext)
	}()

	ticker := time.NewTicker(l.options.LeaseDuration / 3)
	defer ticker.Stop()
	for {
		select {
		case err := <-operationResult:
			cancel()
			releaseErr := l.release(current)
			if err != nil {
				return err
			}
			return releaseErr
		case <-ctx.Done():
			cancel()
			<-operationResult
			_ = l.release(current)
			return ctx.Err()
		case <-ticker.C:
			current, err = l.renew(ctx, current)
			if err != nil {
				cancel()
				<-operationResult
				if commonerrors.IsConflict(err) || commonerrors.IsNotFound(err) {
					return ErrLockLost
				}
				return fmt.Errorf("renew lease: %w", err)
			}
		}
	}
}

func (l *storeLocker) acquire(ctx context.Context, name string) (*Lease, error) {
	for {
		current := &Lease{}
		err := l.storage.Get(ctx, name, current)
		if commonerrors.IsNotFound(err) {
			created := l.newLease(name, nil)
			if createErr := l.storage.Create(ctx, created); createErr == nil {
				return created, nil
			} else if !commonerrors.IsAlreadyExists(createErr) && !commonerrors.IsConflict(createErr) {
				return nil, createErr
			}
		} else if err != nil {
			return nil, err
		} else if current.HolderIdentity == l.options.Identity || !leaseValid(current, time.Now()) {
			updated := l.newLease(name, current)
			if updateErr := l.storage.Update(ctx, updated); updateErr == nil {
				return updated, nil
			} else if !commonerrors.IsConflict(updateErr) && !commonerrors.IsNotFound(updateErr) {
				return nil, updateErr
			}
		}

		timer := time.NewTimer(l.options.RetryPeriod)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (l *storeLocker) renew(ctx context.Context, current *Lease) (*Lease, error) {
	updated := l.newLease(current.ID, current)
	updated.AcquireTime = current.AcquireTime
	updated.LeaderTransitions = current.LeaderTransitions
	if err := l.storage.Update(ctx, updated); err != nil {
		return nil, err
	}
	return updated, nil
}

func (l *storeLocker) release(current *Lease) error {
	ctx, cancel := context.WithTimeout(context.Background(), l.options.RetryPeriod)
	defer cancel()
	current.LeaseDurationSeconds = 1
	current.RenewTime = meta.Time{Time: time.Now().
		Add(-time.Second)}
	if err := l.storage.Update(ctx, current); err != nil {
		if commonerrors.IsConflict(err) || commonerrors.IsNotFound(err) {
			return ErrLockLost
		}
		return err
	}
	return nil
}

func (l *storeLocker) newLease(name string, current *Lease) *Lease {
	now := meta.Now()
	result := &Lease{
		ObjectMeta:           store.ObjectMeta{ID: name, Name: name},
		HolderIdentity:       l.options.Identity,
		LeaseDurationSeconds: int(l.options.LeaseDuration / time.Second),
		AcquireTime:          now,
		RenewTime:            now,
	}
	if current != nil {
		result.ResourceVersion = current.ResourceVersion
		result.LeaderTransitions = current.LeaderTransitions
		if current.HolderIdentity == l.options.Identity {
			result.AcquireTime = current.AcquireTime
		} else {
			result.LeaderTransitions++
		}
	}
	return result
}

func leaseValid(current *Lease, now time.Time) bool {
	expiresAt := current.RenewTime.Add(time.Duration(current.LeaseDurationSeconds) * time.Second)
	return expiresAt.After(now)
}
