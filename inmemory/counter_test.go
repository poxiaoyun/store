package inmemory_test

import (
	"errors"
	"math"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/inmemory"
)

func TestWindowedAccumulatorLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		accumulator, err := inmemory.NewWindowedAccumulator(time.Minute)
		if err != nil {
			t.Fatalf("create accumulator: %v", err)
		}

		start := time.Now()
		first, err := accumulator.Add(t.Context(), "key", 3)
		if err != nil {
			t.Fatalf("first addition: %v", err)
		}
		wantExpiration := start.Add(time.Minute)
		if first != (store.Count{Value: 3, ExpiresAt: wantExpiration}) {
			t.Fatalf("first count = %#v, want value 3 expiring at %s", first, wantExpiration)
		}

		second, err := accumulator.Add(t.Context(), "key", 4)
		if err != nil {
			t.Fatalf("second addition: %v", err)
		}
		if second != (store.Count{Value: 7, ExpiresAt: wantExpiration}) {
			t.Fatalf("second count = %#v, want value 7 with unchanged expiration", second)
		}

		other, err := accumulator.Add(t.Context(), "other", 2)
		if err != nil {
			t.Fatalf("add other key: %v", err)
		}
		if other.Value != 2 {
			t.Fatalf("other key value = %d, want 2", other.Value)
		}

		time.Sleep(time.Minute)
		reset, err := accumulator.Add(t.Context(), "key", 5)
		if err != nil {
			t.Fatalf("add expired counter: %v", err)
		}
		wantExpiration = time.Now().Add(time.Minute)
		if reset != (store.Count{Value: 5, ExpiresAt: wantExpiration}) {
			t.Fatalf("reset count = %#v, want value 5 expiring at %s", reset, wantExpiration)
		}
	})
}

func TestNewWindowedAccumulatorValidatesWindow(t *testing.T) {
	for _, window := range []time.Duration{0, -time.Second} {
		_, err := inmemory.NewWindowedAccumulator(window)
		if !errors.Is(err, store.ErrInvalidWindow) {
			t.Fatalf("create with window %s returned %v, want ErrInvalidWindow", window, err)
		}
	}
}

func TestWindowedAccumulatorValidatesAmountAndOverflow(t *testing.T) {
	accumulator, err := inmemory.NewWindowedAccumulator(time.Minute)
	if err != nil {
		t.Fatalf("create accumulator: %v", err)
	}
	for _, amount := range []int64{0, -1} {
		_, err := accumulator.Add(t.Context(), "invalid", amount)
		if !errors.Is(err, store.ErrInvalidAmount) {
			t.Fatalf("add amount %d returned %v, want ErrInvalidAmount", amount, err)
		}
	}
	if _, err := accumulator.Add(t.Context(), "overflow", math.MaxInt64-1); err != nil {
		t.Fatalf("prepare overflow counter: %v", err)
	}
	if _, err := accumulator.Add(t.Context(), "overflow", 2); !errors.Is(err, store.ErrCounterOverflow) {
		t.Fatalf("overflow addition returned %v, want ErrCounterOverflow", err)
	}
	count, err := accumulator.Add(t.Context(), "overflow", 1)
	if err != nil {
		t.Fatalf("add after overflow: %v", err)
	}
	if count.Value != math.MaxInt64 {
		t.Fatalf("count after overflow = %d, want %d", count.Value, int64(math.MaxInt64))
	}
}

func TestWindowedAccumulatorConcurrentAdditions(t *testing.T) {
	const additions = 128
	accumulator, err := inmemory.NewWindowedAccumulator(time.Hour)
	if err != nil {
		t.Fatalf("create accumulator: %v", err)
	}
	ctx := t.Context()
	results := make(chan store.Count, additions)
	var group sync.WaitGroup
	for range additions {
		group.Go(func() {
			count, err := accumulator.Add(ctx, "key", 1)
			if err != nil {
				t.Error(err)
				return
			}
			results <- count
		})
	}
	group.Wait()
	close(results)

	seen := make(map[int64]bool, additions)
	var expiresAt time.Time
	for count := range results {
		if seen[count.Value] {
			t.Fatalf("duplicate counter value %d", count.Value)
		}
		seen[count.Value] = true
		if expiresAt.IsZero() {
			expiresAt = count.ExpiresAt
		} else if count.ExpiresAt != expiresAt {
			t.Fatalf("expiration changed from %s to %s", expiresAt, count.ExpiresAt)
		}
	}
	if len(seen) != additions || !seen[1] || !seen[additions] {
		t.Fatalf("observed values = %d spanning 1=%t to %d=%t", len(seen), seen[1], additions, seen[additions])
	}
}

func TestWindowedBudgetLifecycle(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		budget, err := inmemory.NewWindowedBudget(5, time.Minute)
		if err != nil {
			t.Fatalf("create budget: %v", err)
		}

		start := time.Now()
		first, err := budget.TryConsume(t.Context(), "key", 3)
		if err != nil {
			t.Fatalf("first consumption: %v", err)
		}
		wantExpiration := start.Add(time.Minute)
		want := store.Consumption{Granted: true, Used: 3, Remaining: 2, ExpiresAt: wantExpiration}
		if first != want {
			t.Fatalf("first consumption = %#v, want %#v", first, want)
		}

		second, err := budget.TryConsume(t.Context(), "key", 2)
		if err != nil {
			t.Fatalf("second consumption: %v", err)
		}
		want = store.Consumption{Granted: true, Used: 5, ExpiresAt: wantExpiration}
		if second != want {
			t.Fatalf("second consumption = %#v, want %#v", second, want)
		}

		rejected, err := budget.TryConsume(t.Context(), "key", 1)
		if err != nil {
			t.Fatalf("rejected consumption: %v", err)
		}
		want = store.Consumption{Used: 5, ExpiresAt: wantExpiration}
		if rejected != want {
			t.Fatalf("rejected consumption = %#v, want %#v", rejected, want)
		}

		time.Sleep(time.Minute)
		reset, err := budget.TryConsume(t.Context(), "key", 4)
		if err != nil {
			t.Fatalf("consume after expiration: %v", err)
		}
		want = store.Consumption{
			Granted:   true,
			Used:      4,
			Remaining: 1,
			ExpiresAt: time.Now().Add(time.Minute),
		}
		if reset != want {
			t.Fatalf("reset consumption = %#v, want %#v", reset, want)
		}
	})
}

func TestWindowedBudgetRejectsWithoutCreatingState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		budget, err := inmemory.NewWindowedBudget(5, time.Minute)
		if err != nil {
			t.Fatalf("create budget: %v", err)
		}

		rejected, err := budget.TryConsume(t.Context(), "key", 6)
		if err != nil {
			t.Fatalf("reject oversized consumption: %v", err)
		}
		if rejected != (store.Consumption{Remaining: 5}) {
			t.Fatalf("oversized consumption = %#v, want rejected with all capacity remaining", rejected)
		}

		start := time.Now()
		accepted, err := budget.TryConsume(t.Context(), "key", 1)
		if err != nil {
			t.Fatalf("consume after rejection: %v", err)
		}
		want := store.Consumption{
			Granted:   true,
			Used:      1,
			Remaining: 4,
			ExpiresAt: start.Add(time.Minute),
		}
		if accepted != want {
			t.Fatalf("consumption after rejection = %#v, want %#v", accepted, want)
		}
	})
}

func TestNewWindowedBudgetValidatesPolicy(t *testing.T) {
	tests := []struct {
		name     string
		capacity int64
		window   time.Duration
		wantErr  error
	}{
		{name: "zero capacity", capacity: 0, window: time.Minute, wantErr: store.ErrInvalidCapacity},
		{name: "negative capacity", capacity: -1, window: time.Minute, wantErr: store.ErrInvalidCapacity},
		{name: "zero window", capacity: 1, window: 0, wantErr: store.ErrInvalidWindow},
		{name: "negative window", capacity: 1, window: -time.Minute, wantErr: store.ErrInvalidWindow},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := inmemory.NewWindowedBudget(test.capacity, test.window)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("constructor error = %v, want %v", err, test.wantErr)
			}
		})
	}
}

func TestWindowedBudgetRejectsInvalidAmount(t *testing.T) {
	budget, err := inmemory.NewWindowedBudget(2, time.Minute)
	if err != nil {
		t.Fatalf("create budget: %v", err)
	}
	for _, amount := range []int64{0, -1} {
		_, err := budget.TryConsume(t.Context(), "key", amount)
		if !errors.Is(err, store.ErrInvalidAmount) {
			t.Fatalf("consume amount %d returned %v, want ErrInvalidAmount", amount, err)
		}
	}
}

func TestWindowedBudgetConcurrentCapacity(t *testing.T) {
	const (
		attempts = 128
		capacity = 16
	)
	budget, err := inmemory.NewWindowedBudget(capacity, time.Hour)
	if err != nil {
		t.Fatalf("create budget: %v", err)
	}
	ctx := t.Context()
	results := make(chan bool, attempts)
	var group sync.WaitGroup
	for range attempts {
		group.Go(func() {
			result, err := budget.TryConsume(ctx, "key", 1)
			if err != nil {
				t.Error(err)
				return
			}
			results <- result.Granted
		})
	}
	group.Wait()
	close(results)

	granted := 0
	for accepted := range results {
		if accepted {
			granted++
		}
	}
	if granted != capacity {
		t.Fatalf("granted consumptions = %d, want %d", granted, capacity)
	}
}

func TestFixedWindowLimiterWithWindowedAccumulator(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		accumulator, err := inmemory.NewWindowedAccumulator(time.Minute)
		if err != nil {
			t.Fatalf("create accumulator: %v", err)
		}
		limiter, err := store.NewFixedWindowLimiter(accumulator, 2)
		if err != nil {
			t.Fatalf("create limiter: %v", err)
		}

		for attempt, allowed := range []bool{true, true, false} {
			decision, err := limiter.Allow(t.Context(), "subject")
			if err != nil {
				t.Fatalf("allow attempt %d: %v", attempt+1, err)
			}
			if decision.Allowed != allowed {
				t.Fatalf("attempt %d allowed = %t, want %t", attempt+1, decision.Allowed, allowed)
			}
		}

		time.Sleep(time.Minute)
		decision, err := limiter.Allow(t.Context(), "subject")
		if err != nil {
			t.Fatalf("allow after reset: %v", err)
		}
		if !decision.Allowed || decision.Remaining != 1 {
			t.Fatalf("decision after reset = %#v, want allowed with one remaining", decision)
		}
	})
}
