package task

import (
	"context"
	"errors"
	"time"

	"xiaoshiai.cn/common/meta"
)

// State describes the execution state of a task.
type State string

const (
	// StatePending means the task is waiting for its first attempt or a retry.
	// A pending task with NotBefore in the future is a delayed task.
	StatePending State = "Pending"
	// StateRunning means an implementation has leased the task to a worker.
	StateRunning State = "Running"
	// StateSucceeded means the handler completed successfully.
	StateSucceeded State = "Succeeded"
	// StateDead means the task permanently failed or exhausted its attempts.
	StateDead State = "Dead"
	// StateCanceled means the task was canceled while pending.
	StateCanceled State = "Canceled"
)

var (
	// ErrInvalidArgument is returned when a task type, handler, or option is
	// invalid.
	ErrInvalidArgument = errors.New("invalid task argument")
	// ErrNotFound is returned when a task does not exist.
	ErrNotFound = errors.New("task not found")
	// ErrConflict is returned when an idempotency key is reused for a different
	// submission.
	ErrConflict = errors.New("task submission conflicts with an existing task")
	// ErrInvalidState is returned when an operation is not valid for the current
	// task state.
	ErrInvalidState = errors.New("operation is not valid for the task state")
	// ErrAlreadyRegistered is returned when a task type already has a handler.
	ErrAlreadyRegistered = errors.New("task handler already registered")
)

// Task describes work to be accepted by a Submitter.
//
// Type identifies the handler. When IdempotencyKey is non-empty, repeated
// equivalent submissions must return the original ID without creating another
// execution. Reusing the key with a different task or execution-affecting
// option must return ErrConflict. Submit must copy Payload, Labels, and
// Annotations before returning; ownership of the caller's values is not
// transferred.
type Task struct {
	Type           string            `json:"type"`
	Payload        []byte            `json:"payload,omitempty"`
	IdempotencyKey string            `json:"idempotencyKey,omitempty"`
	Labels         map[string]string `json:"labels,omitempty"`
	Annotations    map[string]string `json:"annotations,omitempty"`
}

// SubmitOptions controls how a task is submitted. Its zero value requests
// immediate execution.
type SubmitOptions struct {
	// NotBefore is the earliest time at which the first attempt may begin.
	NotBefore time.Time
}

// TaskStatus describes the execution lifecycle of an accepted task. Attempt
// starts at one and increases for every new lease, including recovery after a
// worker loses its lease.
type TaskStatus struct {
	State          State      `json:"state"`
	Attempt        int        `json:"attempt"`
	NotBefore      time.Time  `json:"notBefore"`
	StartTime      *time.Time `json:"startTime,omitempty"`
	CompletionTime *time.Time `json:"completionTime,omitempty"`
	LastError      string     `json:"lastError,omitempty"`
}

// TaskInfo is the observable snapshot of an accepted Task. It is also the
// immutable snapshot delivered to a Handler.
type TaskInfo struct {
	ID                string    `json:"id"`
	CreationTimestamp time.Time `json:"creationTimestamp"`
	Task              `json:",inline"`

	Status TaskStatus `json:"status"`
}

// Submitter accepts tasks for asynchronous execution.
//
// A successful return means the adapter has accepted responsibility for the
// task. Cancellation of ctx after a successful return does not cancel it.
type Submitter interface {
	// Submit accepts work and returns the stable ID assigned to it.
	Submit(ctx context.Context, task Task, options SubmitOptions) (string, error)
}

// Manager exposes task state and optional operational controls.
type Manager interface {
	// List returns tasks matching options. A positive Limit selects continuation
	// pagination; otherwise a positive Size selects page pagination, and fields
	// outside the selected behavior are ignored. Search, sort, and selector
	// capabilities are implementation-dependent and documented by each backend.
	List(ctx context.Context, options meta.ListOptions) (meta.Page[TaskInfo], error)

	// Get returns the current snapshot of one task.
	Get(ctx context.Context, id string) (TaskInfo, error)

	// Cancel moves a pending task to StateCanceled and guarantees that no further
	// Handler attempt will start for it. Cancel is idempotent for StateCanceled.
	// Running, succeeded, and dead tasks return ErrInvalidState.
	Cancel(ctx context.Context, id string) error

	// Retry moves a dead or canceled task back to pending. NotBefore is the
	// earliest time for the new attempt; a zero value means immediately. It
	// starts a new execution cycle with its attempt counter reset. Other states
	// return ErrInvalidState.
	Retry(ctx context.Context, id string, notBefore time.Time) error
}

// Handler executes one attempt of a task. Implementations may call it more than
// once for the same task. The context is canceled when the worker shuts down,
// the configured timeout expires, or the worker loses its lease. A nil error
// completes the task. A regular error is retried according to the registered
// policy; use NoRetry or RetryAfter to override that behavior.
type Handler interface {
	// Handle executes one attempt using an immutable task snapshot.
	Handle(ctx context.Context, task TaskInfo) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, task TaskInfo) error

// Handle implements Handler.
func (f HandlerFunc) Handle(ctx context.Context, task TaskInfo) error {
	return f(ctx, task)
}

// NoRetry marks err as a failure that must not be retried. A worker records the
// failure and moves the task to StateDead. A nil error remains nil.
func NoRetry(err error) error {
	if err == nil {
		return nil
	}
	return &Error{
		Err:     err,
		NoRetry: true,
	}
}

// IsNoRetry reports whether err or an error it wraps was marked by NoRetry.
func IsNoRetry(err error) bool {
	var target *Error
	return errors.As(err, &target) && target.NoRetry
}

// RetryAfter marks err for retry after at least the given delay instead of the
// worker's default backoff. A nil error remains nil. NoRetry takes precedence
// if both markers occur in the same error chain. A non-positive delay leaves
// err unchanged, selecting the default backoff.
func RetryAfter(err error, delay time.Duration) error {
	if err == nil {
		return nil
	}
	if delay <= 0 || IsNoRetry(err) {
		return err
	}
	return &Error{
		Err:        err,
		RetryAfter: delay,
	}
}

// RetryDelay returns the delay requested by RetryAfter. The boolean is false
// when err has no explicit retry delay.
func RetryDelay(err error) (time.Duration, bool) {
	var target *Error
	if !errors.As(err, &target) || target.NoRetry || target.RetryAfter <= 0 {
		return 0, false
	}
	return target.RetryAfter, true
}

// Error describes an explicit retry decision returned by a Handler. Ordinary
// errors use the worker's default retry policy. Error values should normally be
// created with NoRetry or RetryAfter.
type Error struct {
	Err        error
	NoRetry    bool
	RetryAfter time.Duration
}

func (e *Error) Error() string {
	if e == nil || e.Err == nil {
		return "task failed"
	}
	return e.Err.Error()
}

// Unwrap returns the underlying Handler error.
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// HandlerOptions defines execution policy for a registered task type. Zero
// values select implementation defaults.
type HandlerOptions struct {
	MaxAttempts int
	Timeout     time.Duration
}

// Worker registers handlers and runs task execution. Register must be called
// before Run. Registering the same task type twice returns ErrAlreadyRegistered.
// A worker only claims its registered task types; tasks for other types remain
// pending for a compatible worker. Run blocks until ctx is canceled or the
// worker encounters a fatal infrastructure error; individual Handler errors
// are task outcomes and must not stop Run.
type Worker interface {
	// Register associates one task type with its handler and execution policy.
	Register(taskType string, handler Handler, options HandlerOptions) error

	// Run executes registered task types until ctx is canceled or the backend fails.
	Run(ctx context.Context) error
}
