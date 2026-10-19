package inmemory_test

import (
	"sync"
	"testing"

	"xiaoshiai.cn/store/inmemory"
)

func TestSequenceUsesIndependentNamespaces(t *testing.T) {
	allocator := inmemory.NewSequence()
	first, err := allocator.Next(t.Context(), "issues/one")
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	second, err := allocator.Next(t.Context(), "issues/one")
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	other, err := allocator.Next(t.Context(), "issues/two")
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if first != 1 || second != 2 || other != 1 {
		t.Fatalf("sequences = (%d, %d, %d), want (1, 2, 1)", first, second, other)
	}
}

func TestSequenceIsConcurrent(t *testing.T) {
	allocator := inmemory.NewSequence()
	const count = 100
	results := make(chan uint64, count)
	var group sync.WaitGroup
	for range count {
		group.Go(func() {
			value, err := allocator.Next(t.Context(), "jobs")
			if err != nil {
				t.Errorf("Next() error = %v", err)
				return
			}
			results <- value
		})
	}
	group.Wait()
	close(results)
	seen := map[uint64]bool{}
	for value := range results {
		seen[value] = true
	}
	for value := uint64(1); value <= count; value++ {
		if !seen[value] {
			t.Fatalf("sequence does not contain %d: %v", value, seen)
		}
	}
}
