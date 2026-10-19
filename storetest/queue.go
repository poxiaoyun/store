package storetest

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
	"xiaoshiai.cn/store"
)

// RunQueue verifies Queue behavior. newQueue must return isolated storage for
// each subtest, with idle polling short enough for the test's five-second deadline.
func RunQueue(t *testing.T, newQueue func(*testing.T) store.Queue) {
	t.Helper()
	t.Run("ReplacementDelayAndPayloadOwnership", func(t *testing.T) {
		q := newQueue(t)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		if err := q.Enqueue(ctx, "item", []byte("old"), store.EnqueueOptions{After: time.Hour}); err != nil {
			t.Fatal(err)
		}
		payload := []byte("new")
		earliest := time.Now().
			Add(30 * time.Millisecond)
		if err := q.Enqueue(ctx, "item", payload, store.EnqueueOptions{After: 30 * time.Millisecond}); err != nil {
			t.Fatal(err)
		}
		payload[0] = 'X'
		err := q.Consume(ctx, func(_ context.Context, id string, data []byte) error {
			if id != "item" || string(data) != "new" {
				return fmt.Errorf("received %q: %q", id, data)
			}
			if time.Now().
				Before(earliest) {
				return errors.New("consumed before delay elapsed")
			}
			cancel()
			return nil
		}, store.ConsumeOptions{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Consume = %v", err)
		}

		ctx, cancel = context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		if err := q.Enqueue(ctx, "item", []byte("again"), store.EnqueueOptions{}); err != nil {
			t.Fatal(err)
		}
		err = q.Consume(ctx, func(context.Context, string, []byte) error { return errors.New("completed ID was redelivered") }, store.ConsumeOptions{})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("completed ID: %v", err)
		}
	})
	t.Run("FailurePreservesWorkAndStopsConsumption", func(t *testing.T) {
		q := newQueue(t)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		if err := q.Enqueue(ctx, "item", []byte("original"), store.EnqueueOptions{}); err != nil {
			t.Fatal(err)
		}
		failure := errors.New("handler failed")
		err := q.Consume(ctx, func(ctx context.Context, id string, data []byte) error {
			// Replacing running work must not overwrite the payload retained for a retry.
			if err := q.Enqueue(ctx, id, []byte("replacement"), store.EnqueueOptions{}); err != nil {
				return err
			}
			data[0] = 'X'
			// Pending work cannot fill a fixed buffer and cause the failed item to be dropped.
			for i := range 32 {
				if err := q.Enqueue(ctx, fmt.Sprint(i), nil, store.EnqueueOptions{}); err != nil {
					return err
				}
			}
			return failure
		}, store.ConsumeOptions{})
		if !errors.Is(err, failure) {
			t.Fatalf("Consume = %v", err)
		}
		seen := map[string]bool{}
		err = q.Consume(ctx, func(_ context.Context, id string, data []byte) error {
			if seen[id] {
				return fmt.Errorf("duplicate delivery: %s", id)
			}
			seen[id] = true
			if id == "item" && string(data) != "original" {
				return fmt.Errorf("retry payload = %q", data)
			}
			if len(seen) == 33 {
				cancel()
			}
			return nil
		}, store.ConsumeOptions{})
		if !errors.Is(err, context.Canceled) || len(seen) != 33 {
			t.Fatalf("Consume = %v, delivered %d items", err, len(seen))
		}
	})
	t.Run("ConcurrentWorkers", func(t *testing.T) {
		q := newQueue(t)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		for i := range 12 {
			if err := q.Enqueue(ctx, fmt.Sprint(i), nil, store.EnqueueOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		entered := make(chan struct{}, 12)
		release := make(chan struct{})
		var active, completed atomic.Int32
		result := make(chan error, 1)
		go func() {
			result <- q.Consume(ctx, func(ctx context.Context, _ string, _ []byte) error {
				if active.Add(1) > 3 {
					active.Add(-1)
					return errors.New("MaxWorkers exceeded")
				}
				defer active.Add(-1)
				entered <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
				if completed.Add(1) == 12 {
					cancel()
				}
				return nil
			}, store.ConsumeOptions{MaxWorkers: 3})
		}()
		for range 3 {
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("MaxWorkers did not allow three simultaneous handlers")
			}
		}
		close(release)
		if err := <-result; !errors.Is(err, context.Canceled) || completed.Load() != 12 {
			t.Fatalf("Consume = %v, completed %d", err, completed.Load())
		}
	})
	t.Run("CancellationReleasesClaim", func(t *testing.T) {
		q := newQueue(t)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		if err := q.Enqueue(ctx, "item", []byte("payload"), store.EnqueueOptions{}); err != nil {
			t.Fatal(err)
		}
		consumeCtx, stop := context.WithCancel(ctx)
		err := q.Consume(consumeCtx, func(ctx context.Context, _ string, _ []byte) error {
			stop()
			<-ctx.Done()
			return ctx.Err()
		}, store.ConsumeOptions{})
		stop()
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel = %v", err)
		}
		err = q.Consume(ctx, func(_ context.Context, id string, data []byte) error {
			if id != "item" || string(data) != "payload" {
				return fmt.Errorf("received %q: %q", id, data)
			}
			cancel()
			return nil
		}, store.ConsumeOptions{})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("consume released item = %v", err)
		}
	})
	t.Run("WorkerFailureKeepsOriginalError", func(t *testing.T) {
		q := newQueue(t)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		for _, id := range []string{"fail", "wait"} {
			if err := q.Enqueue(ctx, id, nil, store.EnqueueOptions{}); err != nil {
				t.Fatal(err)
			}
		}
		waiting := make(chan struct{})
		failure := errors.New("original failure")
		err := q.Consume(ctx, func(ctx context.Context, id string, _ []byte) error {
			if id == "wait" {
				close(waiting)
				<-ctx.Done()
				return ctx.Err()
			}
			select {
			case <-waiting:
				return failure
			case <-ctx.Done():
				return ctx.Err()
			}
		}, store.ConsumeOptions{MaxWorkers: 2})
		if !errors.Is(err, failure) {
			t.Fatalf("Consume = %v", err)
		}
	})
	t.Run("GeneratedIDsAndConcurrentProducers", func(t *testing.T) {
		q := newQueue(t)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		var producers errgroup.Group
		for range 32 {
			producers.Go(func() error { return q.Enqueue(ctx, "", []byte("data"), store.EnqueueOptions{}) })
		}
		if err := producers.Wait(); err != nil {
			t.Fatal(err)
		}
		seen := map[string]bool{}
		err := q.Consume(ctx, func(_ context.Context, id string, _ []byte) error {
			if id == "" || seen[id] {
				return fmt.Errorf("invalid or duplicate generated ID %q", id)
			}
			seen[id] = true
			if len(seen) == 32 {
				cancel()
			}
			return nil
		}, store.ConsumeOptions{})
		if !errors.Is(err, context.Canceled) || len(seen) != 32 {
			t.Fatalf("Consume = %v, received %d IDs", err, len(seen))
		}
	})
	t.Run("ConcurrentDuplicateEnqueue", func(t *testing.T) {
		q := newQueue(t)
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		var producers errgroup.Group
		for range 16 {
			producers.Go(func() error { return q.Enqueue(ctx, "same", []byte("payload"), store.EnqueueOptions{}) })
		}
		if err := producers.Wait(); err != nil {
			t.Fatal(err)
		}
		failure := errors.New("stop after first delivery")
		err := q.Consume(ctx, func(_ context.Context, id string, data []byte) error {
			if id != "same" || string(data) != "payload" {
				return fmt.Errorf("received %q: %q", id, data)
			}
			return failure
		}, store.ConsumeOptions{})
		if !errors.Is(err, failure) {
			t.Fatalf("Consume = %v", err)
		}
	})
	t.Run("InvalidWorkers", func(t *testing.T) {
		q := newQueue(t)
		if err := q.Consume(t.Context(), func(context.Context, string, []byte) error { t.Error("handler called"); return nil }, store.ConsumeOptions{MaxWorkers: -1}); err == nil {
			t.Fatal("negative MaxWorkers accepted")
		}
	})
}
