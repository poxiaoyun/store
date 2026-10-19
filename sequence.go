package store

import "context"

// Sequence allocates numbers independently for each stable namespace. Allocated
// numbers are unique and increasing, but gaps are allowed. A namespace is an
// opaque string; it has no implicit hierarchy or relationship to Store scopes.
type Sequence interface {
	// Next returns the next positive number for namespace.
	Next(ctx context.Context, namespace string) (uint64, error)
}
