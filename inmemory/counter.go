package inmemory

import (
	"container/heap"
	"context"
	"math"
	"sync"
	"time"

	"xiaoshiai.cn/store"
)

var (
	_ store.WindowedAccumulator = &WindowedAccumulator{}
	_ store.WindowedBudget      = &WindowedBudget{}
)

// WindowedAccumulator is a process-local atomic accumulator configured with a
// fixed window.
type WindowedAccumulator struct {
	window time.Duration
	state  counterState
}

// NewWindowedAccumulator returns an accumulator whose windows begin with the
// first addition for each missing key.
func NewWindowedAccumulator(window time.Duration) (*WindowedAccumulator, error) {
	if window <= 0 {
		return nil, store.ErrInvalidWindow
	}
	return &WindowedAccumulator{window: window}, nil
}

// Add implements store.WindowedAccumulator.
func (a *WindowedAccumulator) Add(ctx context.Context, key string, amount int64) (store.Count, error) {
	if amount <= 0 {
		return store.Count{}, store.ErrInvalidAmount
	}
	a.state.mu.Lock()
	defer a.state.mu.Unlock()

	now := time.Now()
	a.state.removeExpired(now)
	current, found := a.state.counters[key]
	if found {
		if current.value > math.MaxInt64-amount {
			return store.Count{}, store.ErrCounterOverflow
		}
		current.value += amount
	} else {
		current = a.state.create(key, amount, now.Add(a.window))
	}
	return store.Count{Value: current.value, ExpiresAt: current.expiresAt}, nil
}

// WindowedBudget is a process-local atomic budget configured with a fixed
// capacity and window.
type WindowedBudget struct {
	capacity int64
	window   time.Duration
	state    counterState
}

// NewWindowedBudget returns a budget whose windows begin with the first
// granted consumption for each missing key.
func NewWindowedBudget(capacity int64, window time.Duration) (*WindowedBudget, error) {
	if capacity <= 0 {
		return nil, store.ErrInvalidCapacity
	}
	if window <= 0 {
		return nil, store.ErrInvalidWindow
	}
	return &WindowedBudget{capacity: capacity, window: window}, nil
}

// TryConsume implements store.WindowedBudget.
func (b *WindowedBudget) TryConsume(ctx context.Context, key string, amount int64) (store.Consumption, error) {
	if amount <= 0 {
		return store.Consumption{}, store.ErrInvalidAmount
	}
	b.state.mu.Lock()
	defer b.state.mu.Unlock()

	now := time.Now()
	b.state.removeExpired(now)
	current, found := b.state.counters[key]
	if !found {
		if amount > b.capacity {
			return store.Consumption{Remaining: b.capacity}, nil
		}
		current = b.state.create(key, amount, now.Add(b.window))
		return store.Consumption{
			Granted:   true,
			Used:      amount,
			Remaining: b.capacity - amount,
			ExpiresAt: current.expiresAt,
		}, nil
	}

	remaining := b.capacity - current.value
	if amount > remaining {
		return store.Consumption{
			Used:      current.value,
			Remaining: remaining,
			ExpiresAt: current.expiresAt,
		}, nil
	}
	current.value += amount
	return store.Consumption{
		Granted:   true,
		Used:      current.value,
		Remaining: remaining - amount,
		ExpiresAt: current.expiresAt,
	}, nil
}

type counterState struct {
	mu          sync.Mutex
	counters    map[string]*counterEntry
	expirations counterExpiryHeap
}

func (s *counterState) removeExpired(now time.Time) {
	for len(s.expirations) > 0 && !now.Before(s.expirations[0].expiresAt) {
		expired := heap.Pop(&s.expirations).(*counterEntry)
		delete(s.counters, expired.key)
	}
	if s.counters == nil {
		s.counters = make(map[string]*counterEntry)
	}
}

func (s *counterState) create(key string, value int64, expiresAt time.Time) *counterEntry {
	current := &counterEntry{key: key, value: value, expiresAt: expiresAt}
	s.counters[key] = current
	heap.Push(&s.expirations, current)
	return current
}

type counterEntry struct {
	key       string
	value     int64
	expiresAt time.Time
}

type counterExpiryHeap []*counterEntry

func (h counterExpiryHeap) Len() int {
	return len(h)
}

func (h counterExpiryHeap) Less(left, right int) bool {
	return h[left].expiresAt.Before(h[right].expiresAt)
}

func (h counterExpiryHeap) Swap(left, right int) {
	h[left], h[right] = h[right], h[left]
}

func (h *counterExpiryHeap) Push(value any) {
	*h = append(*h, value.(*counterEntry))
}

func (h *counterExpiryHeap) Pop() any {
	old := *h
	last := len(old) - 1
	result := old[last]
	old[last] = nil
	*h = old[:last]
	return result
}
