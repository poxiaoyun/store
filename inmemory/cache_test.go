package inmemory_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/inmemory"
)

func TestCacheValueLifecycle(t *testing.T) {
	values := newCache[int](t, 2)

	value, found, err := values.Get(t.Context(), "missing")
	if err != nil {
		t.Fatalf("get missing value: %v", err)
	}
	if found || value != 0 {
		t.Fatalf("missing value = (%d, %t), want (0, false)", value, found)
	}

	if err := values.Set(t.Context(), "key", 0, 0); err != nil {
		t.Fatalf("set zero value: %v", err)
	}
	value, found, err = values.Get(t.Context(), "key")
	if err != nil {
		t.Fatalf("get zero value: %v", err)
	}
	if !found || value != 0 {
		t.Fatalf("cached zero value = (%d, %t), want (0, true)", value, found)
	}

	if err := values.Set(t.Context(), "key", 42, 0); err != nil {
		t.Fatalf("replace value: %v", err)
	}
	value, found, err = values.Get(t.Context(), "key")
	if err != nil {
		t.Fatalf("get replacement value: %v", err)
	}
	if !found || value != 42 {
		t.Fatalf("replacement value = (%d, %t), want (42, true)", value, found)
	}

	if err := values.Delete(t.Context(), "key"); err != nil {
		t.Fatalf("delete value: %v", err)
	}
	if err := values.Delete(t.Context(), "key"); err != nil {
		t.Fatalf("delete missing value: %v", err)
	}
	_, found, err = values.Get(t.Context(), "key")
	if err != nil {
		t.Fatalf("get deleted value: %v", err)
	}
	if found {
		t.Fatal("deleted value remains cached")
	}
}

func TestCacheExpiresValues(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		values := newCache[string](t, 2)
		if err := values.Set(t.Context(), "expiring", "value", time.Minute); err != nil {
			t.Fatalf("set expiring value: %v", err)
		}
		if err := values.Set(t.Context(), "permanent", "value", 0); err != nil {
			t.Fatalf("set permanent value: %v", err)
		}

		time.Sleep(time.Minute)
		_, found, err := values.Get(t.Context(), "expiring")
		if err != nil {
			t.Fatalf("get expired value: %v", err)
		}
		if found {
			t.Fatal("expired value remains cached")
		}
		_, found, err = values.Get(t.Context(), "permanent")
		if err != nil {
			t.Fatalf("get permanent value: %v", err)
		}
		if !found {
			t.Fatal("zero-TTL value expired")
		}
	})
}

func TestCacheEvictsLeastRecentlyUsedValue(t *testing.T) {
	values := newCache[string](t, 2)
	for _, key := range []string{"oldest", "recent"} {
		if err := values.Set(t.Context(), key, key, 0); err != nil {
			t.Fatalf("set %q: %v", key, err)
		}
	}
	if _, _, err := values.Get(t.Context(), "oldest"); err != nil {
		t.Fatalf("refresh oldest value: %v", err)
	}
	if err := values.Set(t.Context(), "new", "new", 0); err != nil {
		t.Fatalf("set new value: %v", err)
	}

	_, found, err := values.Get(t.Context(), "recent")
	if err != nil {
		t.Fatalf("get evicted value: %v", err)
	}
	if found {
		t.Fatal("least recently used value remains cached")
	}
	for _, key := range []string{"oldest", "new"} {
		_, found, err := values.Get(t.Context(), key)
		if err != nil {
			t.Fatalf("get retained value %q: %v", key, err)
		}
		if !found {
			t.Fatalf("value %q was unexpectedly evicted", key)
		}
	}
}

func TestCacheRejectsInvalidConfiguration(t *testing.T) {
	if _, err := inmemory.NewCache[string](0); err == nil {
		t.Fatal("zero capacity succeeded")
	}

	values := newCache[string](t, 1)
	err := values.Set(t.Context(), "key", "value", -time.Second)
	if !errors.Is(err, store.ErrInvalidTTL) {
		t.Fatalf("set error = %v, want ErrInvalidTTL", err)
	}
	_, found, getErr := values.Get(t.Context(), "key")
	if getErr != nil {
		t.Fatalf("get rejected value: %v", getErr)
	}
	if found {
		t.Fatal("value with invalid TTL was cached")
	}
}

func TestCacheConcurrentAccess(t *testing.T) {
	const size = 32
	values := newCache[int](t, size)
	ctx := t.Context()
	errs := make(chan error, size)
	var group sync.WaitGroup
	for index := range size {
		group.Go(func() {
			key := fmt.Sprintf("key-%d", index)
			if err := values.Set(ctx, key, index, 0); err != nil {
				errs <- err
				return
			}
			value, found, err := values.Get(ctx, key)
			if err != nil {
				errs <- err
				return
			}
			if !found || value != index {
				errs <- fmt.Errorf("value %q = (%d, %t)", key, value, found)
			}
		})
	}
	group.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
}

func newCache[T any](t *testing.T, capacity int) *inmemory.InMemoryCache[T] {
	t.Helper()
	values, err := inmemory.NewCache[T](capacity)
	if err != nil {
		t.Fatalf("create cache: %v", err)
	}
	return values
}
