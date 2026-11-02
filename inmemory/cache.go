// Package inmemory provides process-local cache and counter adapters.
package inmemory

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/v2/simplelru"
	"xiaoshiai.cn/store"
)

var _ store.Cache[any] = &InMemoryCache[any]{}

// NewCache returns a process-local cache with the given maximum number of entries.
func NewCache[T any](capacity int) (*InMemoryCache[T], error) {
	values, err := simplelru.NewLRU[string, item[T]](capacity, nil)
	if err != nil {
		return nil, fmt.Errorf("create in-memory cache: %w", err)
	}
	return &InMemoryCache[T]{values: values}, nil
}

// InMemoryCache is a bounded least-recently-used store. It is safe for
// concurrent use and must be constructed with NewCache.
type InMemoryCache[T any] struct {
	mu     sync.Mutex
	values *simplelru.LRU[string, item[T]]
}

// Get implements store.Cache.
func (c *InMemoryCache[T]) Get(ctx context.Context, key string) (T, bool, error) {
	c.mu.Lock()
	current, found := c.values.Get(key)
	if found && !current.expiresAt.IsZero() && !time.Now().Before(current.expiresAt) {
		c.values.Remove(key)
		found = false
	}
	c.mu.Unlock()
	if !found {
		var zero T
		return zero, false, nil
	}
	return current.value, true, nil
}

// Set implements store.Cache.
func (c *InMemoryCache[T]) Set(ctx context.Context, key string, value T, ttl time.Duration) error {
	if ttl < 0 {
		return store.ErrInvalidTTL
	}
	c.mu.Lock()
	expiresAt := time.Time{}
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	c.values.Add(key, item[T]{value: value, expiresAt: expiresAt})
	c.mu.Unlock()
	return nil
}

// Delete implements store.Cache.
func (c *InMemoryCache[T]) Delete(ctx context.Context, key string) error {
	c.mu.Lock()
	c.values.Remove(key)
	c.mu.Unlock()
	return nil
}

type item[T any] struct {
	value     T
	expiresAt time.Time
}
