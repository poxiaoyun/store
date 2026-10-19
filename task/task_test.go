package task

import (
	"errors"
	"testing"
	"time"
)

func TestNoRetryPreservesCause(t *testing.T) {
	cause := errors.New("invalid payload")
	err := NoRetry(cause)
	var taskErr *Error
	if !errors.As(err, &taskErr) {
		t.Fatal("NoRetry() did not return *Error")
	}
	if !taskErr.NoRetry {
		t.Fatal("NoRetry() returned NoRetry = false")
	}
	if !IsNoRetry(err) {
		t.Fatal("IsNoRetry() = false, want true")
	}
	if !errors.Is(err, cause) {
		t.Fatal("NoRetry() did not preserve its cause")
	}
	if NoRetry(nil) != nil {
		t.Fatal("NoRetry(nil) must return nil")
	}
}

func TestRetryAfter(t *testing.T) {
	cause := errors.New("temporarily unavailable")
	delay := 5 * time.Minute
	err := RetryAfter(cause, delay)
	var taskErr *Error
	if !errors.As(err, &taskErr) {
		t.Fatal("RetryAfter() did not return *Error")
	}
	if taskErr.NoRetry || taskErr.RetryAfter != delay {
		t.Fatalf("RetryAfter() error = %#v", taskErr)
	}

	got, ok := RetryDelay(err)
	if !ok || got != delay {
		t.Fatalf("RetryDelay() = (%s, %t), want (%s, true)", got, ok, delay)
	}
	if !errors.Is(err, cause) {
		t.Fatal("RetryAfter() did not preserve its cause")
	}
	if got := RetryAfter(cause, 0); got != cause {
		t.Fatal("RetryAfter() with a non-positive delay must leave the error unchanged")
	}
	if RetryAfter(nil, delay) != nil {
		t.Fatal("RetryAfter(nil, delay) must return nil")
	}
	if got := RetryAfter(NoRetry(cause), delay); !IsNoRetry(got) {
		t.Fatal("NoRetry must take precedence over RetryAfter")
	}
}
