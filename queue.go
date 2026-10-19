package store

import (
	"context"
	"time"
)

// EnqueueOptions controls scheduling of enqueued work.
type EnqueueOptions struct {
	// After delays eligibility for consumption. Non-positive values mean immediately.
	After time.Duration
}

// QueueHandleFunc handles one queued item. Returning nil completes it; returning
// an error leaves it pending and stops this Consume call. Handlers must honor ctx.
// The handler owns data and may modify it without changing a subsequent attempt.
type QueueHandleFunc func(ctx context.Context, id string, data []byte) error

// ConsumeOptions controls queue consumption.
type ConsumeOptions struct {
	// MaxWorkers bounds concurrent handlers in this Consume call. Zero means one;
	// negative values are invalid. Separate Consume calls have independent limits.
	MaxWorkers int
}

// Queue accepts and consumes work independently of object storage. It does not
// guarantee FIFO ordering or recovery of work interrupted by a process crash.
type Queue interface {
	// Enqueue copies data and schedules it after options.After. An empty id is
	// generated automatically. Reusing a pending id replaces its payload and delay;
	// reusing a running or completed id is a successful no-op. Completed ids remain
	// reserved for the lifetime of the queue's storage.
	Enqueue(ctx context.Context, id string, data []byte, options EnqueueOptions) error

	// Consume runs handlers until ctx is canceled or a handler/backend fails. It
	// cancels and waits for its workers before returning the cancellation or
	// failure error. Handler failures leave messages pending for another Consume
	// call; successful handlers complete even during shutdown. Backends return
	// acknowledgement errors rather than claiming completion. No automatic retry
	// policy or exactly-once execution guarantee is provided.
	Consume(ctx context.Context, handler QueueHandleFunc, options ConsumeOptions) error
}
