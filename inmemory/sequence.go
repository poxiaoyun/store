package inmemory

import (
	"context"
	"sync"

	"xiaoshiai.cn/store"
)

var _ store.Sequence = (*Sequence)(nil)

// Sequence allocates sequences in process memory.
type Sequence struct {
	mu     sync.Mutex
	values map[string]uint64
}

// NewSequence creates an empty in-memory allocator.
func NewSequence() *Sequence {
	return &Sequence{values: map[string]uint64{}}
}

// Next implements store.Sequence.
func (a *Sequence) Next(_ context.Context, namespace string) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.values[namespace]++
	return a.values[namespace], nil
}
