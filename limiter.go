package store

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	// ErrInvalidLimit indicates that a limiter was configured without a
	// positive request limit.
	ErrInvalidLimit = errors.New("cache: limit must be positive")
)

// Decision describes the result of consuming one limited attempt.
type Decision struct {
	// Allowed reports whether the attempt is within the configured limit.
	Allowed bool
	// Remaining is the number of additional attempts allowed in this window.
	Remaining int64
	// ResetAt is the counter's authoritative expiration time.
	ResetAt time.Time
}

// Limiter consumes attempts independently for each stable key.
type Limiter interface {
	// Allow consumes one attempt and returns its decision. Rejected attempts are
	// also counted. Counter failures are returned without a synthetic decision.
	Allow(ctx context.Context, key string) (Decision, error)
}

var _ Limiter = &FixedWindowLimiter{}

// FixedWindowLimiter limits attempts during a window that begins with the
// first attempt for a missing key.
type FixedWindowLimiter struct {
	counter WindowedAccumulator
	limit   int64
}

// NewFixedWindowLimiter returns a limiter backed by a window-configured
// counter.
func NewFixedWindowLimiter(counter WindowedAccumulator, limit int64) (*FixedWindowLimiter, error) {
	if limit <= 0 {
		return nil, ErrInvalidLimit
	}
	return &FixedWindowLimiter{counter: counter, limit: limit}, nil
}

// Allow implements Limiter.
func (l *FixedWindowLimiter) Allow(ctx context.Context, key string) (Decision, error) {
	count, err := l.counter.Add(ctx, key, 1)
	if err != nil {
		return Decision{}, fmt.Errorf("add fixed-window counter: %w", err)
	}
	remaining := int64(0)
	if count.Value < l.limit {
		remaining = l.limit - count.Value
	}
	return Decision{
		Allowed:   count.Value <= l.limit,
		Remaining: remaining,
		ResetAt:   count.ExpiresAt,
	}, nil
}
