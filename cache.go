package store

import (
	"context"
	"errors"
	"time"

	"golang.org/x/sync/singleflight"
	"xiaoshiai.cn/common/log"
)

var (
	// ErrInvalidTTL indicates that an operation received a TTL outside its
	// supported range.
	ErrInvalidTTL = errors.New("cache: invalid TTL")
	// ErrInvalidWindow indicates that a windowed capability was configured
	// without a positive window.
	ErrInvalidWindow = errors.New("cache: window must be positive")
	// ErrInvalidCapacity indicates that a budget was configured without a
	// positive capacity.
	ErrInvalidCapacity = errors.New("cache: capacity must be positive")
	// ErrInvalidAmount indicates that a counter operation received a
	// non-positive amount.
	ErrInvalidAmount = errors.New("cache: amount must be positive")
	// ErrCounterOverflow indicates that an addition cannot be represented by
	// the counter value type.
	ErrCounterOverflow = errors.New("cache: counter overflow")
)

// Cache stores best-effort expiring values. Implementations may evict values
// before their TTL, so callers must remain correct on every cache miss.
//
// Values are immutable across this interface. Callers must not mutate a value
// after passing it to Set or mutate a value returned by Get.
type Cache[T any] interface {
	// Get returns the current value for key. A miss returns the zero value,
	// false, and a nil error.
	Get(ctx context.Context, key string) (value T, found bool, err error)

	// Set replaces the value for key. A zero TTL means the value does not
	// expire; a negative TTL returns ErrInvalidTTL.
	Set(ctx context.Context, key string, value T, ttl time.Duration) error

	// Delete removes key. Deleting a missing key succeeds.
	Delete(ctx context.Context, key string) error
}

// WindowedAccumulator atomically accumulates positive weighted amounts during
// a configured window. Unlike Cache values, live state must not be evicted
// before its expiration.
type WindowedAccumulator interface {
	// Add creates a missing or expired counter at amount, or adds amount to the
	// current value. Creation fixes the expiration using the window bound to the
	// adapter; later additions do not extend it.
	Add(ctx context.Context, key string, amount int64) (Count, error)
}

// Count is the state of a windowed accumulator after an addition.
type Count struct {
	// Value is the accumulated value.
	Value int64
	// ExpiresAt is the authoritative expiration time chosen by the adapter.
	ExpiresAt time.Time
}

// Consumption is the result of consuming a windowed budget.
type Consumption struct {
	// Granted reports whether the amount was consumed.
	Granted bool
	// Used is the amount consumed in the current window.
	Used int64
	// Remaining is the amount still available in the current window.
	Remaining int64
	// ExpiresAt is the current window's authoritative expiration time. It is
	// zero when a rejected operation did not create a window.
	ExpiresAt time.Time
}

// WindowedBudget atomically consumes positive weighted amounts without
// exceeding the capacity bound to the adapter. Unlike Cache values, live state
// must not be evicted before its expiration.
type WindowedBudget interface {
	// TryConsume consumes amount only when the resulting usage does not exceed
	// the configured capacity. A rejected operation does not create or change
	// state. The configured window is fixed when state is first created and is
	// not extended by later operations.
	TryConsume(ctx context.Context, key string, amount int64) (Consumption, error)
}

// LoadingCache caches immutable values and merges concurrent loads of one key.
// Construct it with NewLoadingCache. Failed loads are never cached. The first
// caller's context controls a shared load; callers share its result and error.
type LoadingCache[T any] struct {
	// Cache provides the underlying Get, Set and Delete operations.
	Cache[T]
	ttl    time.Duration
	loader func(context.Context, string) (T, error)
	group  singleflight.Group
}

// NewLoadingCache binds a cache backend, nonnegative TTL and loader.
// A zero TTL disables expiration; the backend owns capacity eviction.
func NewLoadingCache[T any](values Cache[T], ttl time.Duration, loader func(context.Context, string) (T, error)) (*LoadingCache[T], error) {
	if ttl < 0 {
		return nil, ErrInvalidTTL
	}
	return &LoadingCache[T]{Cache: values, ttl: ttl, loader: loader}, nil
}

// GetOrLoad returns a cached value or shares one load for the key. Values, including
// referenced maps and slices, must remain immutable after loading. Each caller
// may cancel its own wait; the first caller controls the shared loader context.
// Cache write failures are logged; a successfully loaded value is still returned.
func (c *LoadingCache[T]) GetOrLoad(ctx context.Context, key string) (T, error) {
	value, found, err := c.Cache.Get(ctx, key)
	if err != nil {
		return value, err
	}
	if found {
		return value, nil
	}
	pending := c.group.DoChan(key, func() (any, error) {
		value, found, err := c.Cache.Get(ctx, key)
		if err != nil {
			return value, err
		}
		if found {
			return struct{ Value T }{value}, nil
		}
		value, err = c.loader(ctx, key)
		if err != nil {
			return value, err
		}
		if err := c.Cache.Set(ctx, key, value, c.ttl); err != nil {
			log.FromContext(ctx).Error(err, "cache loaded value")
		}
		return struct{ Value T }{value}, nil
	})
	select {
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	case result := <-pending:
		if result.Err != nil {
			var zero T
			return zero, result.Err
		}
		return result.Val.(struct{ Value T }).Value, nil
	}
}
