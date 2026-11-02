package store_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/inmemory"
)

func TestConcurrentLoading(t *testing.T) {
	var calls atomic.Int64
	c, err := store.NewLoadingCache(newBackend[string](t), 0, func(ctx context.Context, key string) (string, error) { calls.Add(1); return key, nil })
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			value, err := c.GetOrLoad(t.Context(), "a")
			if err != nil || value != "a" {
				t.Errorf("get: %q %v", value, err)
			}
		})
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("loads: %d", calls.Load())
	}
}

func TestFailuresAreNotCached(t *testing.T) {
	var calls int
	failure := errors.New("load failed")
	c, err := store.NewLoadingCache(newBackend[int](t), time.Minute, func(context.Context, string) (int, error) { calls++; return 0, failure })
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := c.GetOrLoad(t.Context(), "a"); !errors.Is(err, failure) {
			t.Fatal(err)
		}
	}
	if calls != 2 {
		t.Fatalf("cached error: %d", calls)
	}
}

func TestExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		c, err := store.NewLoadingCache(newBackend[int](t), time.Minute, func(context.Context, string) (int, error) { calls++; return calls, nil })
		if err != nil {
			t.Fatal(err)
		}
		first, err := c.GetOrLoad(t.Context(), "a")
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Minute)
		next, err := c.GetOrLoad(t.Context(), "a")
		if err != nil {
			t.Fatal(err)
		}
		if first != 1 || next != 2 {
			t.Fatalf("expiry: %d -> %d", first, next)
		}
	})
}

func newBackend[T any](t *testing.T) *inmemory.InMemoryCache[T] {
	t.Helper()
	backend, err := inmemory.NewCache[T](2)
	if err != nil {
		t.Fatal(err)
	}
	return backend
}

func TestNilValue(t *testing.T) {
	c, err := store.NewLoadingCache(newBackend[any](t), 0, func(context.Context, string) (any, error) { return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		value, err := c.GetOrLoad(t.Context(), "nil")
		if err != nil || value != nil {
			t.Fatalf("nil value: %v %v", value, err)
		}
	}
}

func TestCanceledWait(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	c, err := store.NewLoadingCache(newBackend[string](t), 0, func(context.Context, string) (string, error) { close(started); <-release; return "loaded", nil })
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := c.GetOrLoad(t.Context(), "key"); done <- err }()
	<-started
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = c.GetOrLoad(ctx, "key")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("canceled wait: %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestLoadingCacheRetainsCacheOperations(t *testing.T) {
	var cacheCalls atomic.Int64
	c, err := store.NewLoadingCache(newBackend[string](t), 0, func(context.Context, string) (string, error) { cacheCalls.Add(1); return "loaded", nil })
	if err != nil {
		t.Fatal(err)
	}
	var backend store.Cache[string] = c
	if err := backend.Set(t.Context(), "key", "seeded", 0); err != nil {
		t.Fatal(err)
	}
	value, err := c.GetOrLoad(t.Context(), "key")
	if err != nil || value != "seeded" || cacheCalls.Load() != 0 {
		t.Fatalf("seeded: %q %v calls=%d", value, err, cacheCalls.Load())
	}
	if err := backend.Delete(t.Context(), "key"); err != nil {
		t.Fatal(err)
	}
	value, err = c.GetOrLoad(t.Context(), "key")
	if err != nil || value != "loaded" || cacheCalls.Load() != 1 {
		t.Fatalf("reloaded: %q %v calls=%d", value, err, cacheCalls.Load())
	}
}

type failingCacheWrite struct{ store.Cache[string] }

func (f failingCacheWrite) Set(context.Context, string, string, time.Duration) error {
	return errors.New("cache unavailable")
}

func TestLoadedValueSurvivesCacheWriteFailure(t *testing.T) {
	c, err := store.NewLoadingCache[string](failingCacheWrite{newBackend[string](t)}, 0, func(context.Context, string) (string, error) { return "accepted", nil })
	if err != nil {
		t.Fatal(err)
	}
	value, err := c.GetOrLoad(t.Context(), "key")
	if err != nil || value != "accepted" {
		t.Fatalf("loaded value: %q %v", value, err)
	}
}
