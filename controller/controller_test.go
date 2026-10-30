package controller_test

import (
	"context"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/controller"
)

func TestScopesRoundTrip(t *testing.T) {
	tests := [][]store.Scope{
		{
			{Resource: "foo", Name: "bar"},
			{Resource: "a", Name: "b"},
		},
		{},
		{
			{Name: "bar"},
		},
		{
			{Name: "bar"},
			{Name: "bar"},
		},
	}

	for _, tt := range tests {
		encoded := controller.EncodeScopes(tt)
		decoded := controller.DecodeScopes(encoded)
		if !slices.Equal(tt, decoded) {
			t.Errorf("DecodeScopes(%q) = %v, want %v", encoded, decoded, tt)
		}
	}
}

// TestControllerProcessesReadyWork verifies that an idle worker needs no token.
func TestControllerProcessesReadyWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		processed, _ := startTestController(t, 256, controller.WithConcurrent[int](2))
		synctest.Wait()
		if got := processed.Load(); got != 256 {
			t.Fatalf("processed %d ready items without advancing time, want 256", got)
		}
	})
}

// TestControllerRateLimit checks shared worker limits and independent instances.
func TestControllerRateLimit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limited := controller.WithRateLimit[int](2, 1)
		first, _ := startTestController(t, 3, limited, controller.WithConcurrent[int](2))
		second, _ := startTestController(t, 3, limited)
		for _, want := range []int64{1, 2, 3} {
			synctest.Wait()
			if first.Load() != want || second.Load() != want {
				t.Fatalf("processed %d and %d items, want %d each", first.Load(), second.Load(), want)
			}
			if want < 3 {
				// Two permits per second replenish one token every half second.
				time.Sleep(500 * time.Millisecond)
			}
		}
	})
}

// TestControllerRateLimitCancellation checks that canceled waits do not reconcile.
func TestControllerRateLimitCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		processed, cancel := startTestController(t, 3, controller.WithRateLimit[int](1, 1), controller.WithConcurrent[int](2))
		synctest.Wait()
		if got := processed.Load(); got != 1 {
			t.Fatalf("processed %d items before cancellation, want one", got)
		}
		cancel()
		synctest.Wait()
		if got := processed.Load(); got != 1 {
			t.Fatalf("processed %d items after cancellation, want one", got)
		}
	})
}

func startTestController(t *testing.T, items int, options ...controller.ControllerOption[int]) (*atomic.Int64, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	var processed atomic.Int64
	worker := controller.NewTypedController("test", controller.TypedReconcilerFunc[int](func(context.Context, int) (controller.Result, error) {
		processed.Add(1)
		return controller.Result{}, nil
	}), options...).
		Watch(controller.SourceFunc[int](func(ctx context.Context, queue controller.TypedQueue[int]) error {
			for i := range items {
				queue.Add(i)
			}
			<-ctx.Done()
			return nil
		}))
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return &processed, cancel
}
