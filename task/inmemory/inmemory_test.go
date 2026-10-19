package inmemory_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"xiaoshiai.cn/common/meta"
	"xiaoshiai.cn/store/task"
	"xiaoshiai.cn/store/task/inmemory"
)

func TestSubmitIsIdempotentAndCopiesTask(t *testing.T) {
	manager := inmemory.New(inmemory.Options{})
	payload := []byte("original")
	work := task.Task{
		Type:           "example",
		Payload:        payload,
		IdempotencyKey: "operation/1",
		Labels:         map[string]string{"tenant": "one"},
		Annotations:    map[string]string{"trace": "original"},
	}

	id, err := manager.Submit(t.Context(), work, task.SubmitOptions{})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	payload[0] = 'X'
	work.Labels["tenant"] = "changed"
	work.Annotations["trace"] = "changed"

	info, err := manager.Get(t.Context(), id)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got := string(info.Payload); got != "original" {
		t.Fatalf("Payload = %q, want original", got)
	}
	if info.Labels["tenant"] != "one" || info.Annotations["trace"] != "original" {
		t.Fatalf("metadata = (%v, %v), want original values", info.Labels, info.Annotations)
	}
	info.Payload[0] = 'Y'
	info.Labels["tenant"] = "changed"
	info.Annotations["trace"] = "changed"
	info, _ = manager.Get(t.Context(), id)
	if got := string(info.Payload); got != "original" {
		t.Fatalf("stored Payload changed through Get(): %q", got)
	}
	if info.Labels["tenant"] != "one" || info.Annotations["trace"] != "original" {
		t.Fatalf("stored metadata changed through Get(): (%v, %v)", info.Labels, info.Annotations)
	}

	duplicateID, err := manager.Submit(t.Context(), task.Task{
		Type:           "example",
		Payload:        []byte("original"),
		IdempotencyKey: "operation/1",
		Labels:         map[string]string{"tenant": "one"},
		Annotations:    map[string]string{"trace": "original"},
	}, task.SubmitOptions{})
	if err != nil || duplicateID != id {
		t.Fatalf("duplicate Submit() = (%q, %v), want (%q, nil)", duplicateID, err, id)
	}

	_, err = manager.Submit(t.Context(), task.Task{
		Type:           "example",
		Payload:        []byte("different"),
		IdempotencyKey: "operation/1",
	}, task.SubmitOptions{})
	if !errors.Is(err, task.ErrConflict) {
		t.Fatalf("conflicting Submit() error = %v, want ErrConflict", err)
	}
}

func TestWorkerRetriesAndSucceeds(t *testing.T) {
	manager := inmemory.New(inmemory.Options{})
	var calls atomic.Int32
	if err := manager.Register("retry", task.HandlerFunc(func(context.Context, task.TaskInfo) error {
		if calls.Add(1) < 3 {
			return task.RetryAfter(errors.New("temporary"), time.Millisecond)
		}
		return nil
	}), task.HandlerOptions{MaxAttempts: 5}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	id, err := manager.Submit(t.Context(), task.Task{Type: "retry"}, task.SubmitOptions{})
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}

	stop := runManager(t, manager)
	defer stop()
	info := waitForState(t, manager, id, task.StateSucceeded)
	if info.Status.Attempt != 3 {
		t.Fatalf("Attempt = %d, want 3", info.Status.Attempt)
	}
	if info.Status.LastError != "" {
		t.Fatalf("LastError = %q, want empty", info.Status.LastError)
	}
}

func TestNoRetryAndTimeoutBecomeDead(t *testing.T) {
	t.Run("no retry", func(t *testing.T) {
		manager := inmemory.New(inmemory.Options{})
		if err := manager.Register("invalid", task.HandlerFunc(func(context.Context, task.TaskInfo) error {
			return task.NoRetry(errors.New("invalid payload"))
		}), task.HandlerOptions{MaxAttempts: 5}); err != nil {
			t.Fatalf("Register() error = %v", err)
		}
		id, _ := manager.Submit(t.Context(), task.Task{Type: "invalid"}, task.SubmitOptions{})
		stop := runManager(t, manager)
		defer stop()

		info := waitForState(t, manager, id, task.StateDead)
		if info.Status.Attempt != 1 || info.Status.LastError != "invalid payload" {
			t.Fatalf("Status = %#v", info.Status)
		}
	})

	t.Run("timeout", func(t *testing.T) {
		manager := inmemory.New(inmemory.Options{})
		if err := manager.Register("slow", task.HandlerFunc(func(ctx context.Context, _ task.TaskInfo) error {
			<-ctx.Done()
			return ctx.Err()
		}), task.HandlerOptions{
			MaxAttempts: 1,
			Timeout:     5 * time.Millisecond,
		}); err != nil {
			t.Fatalf("Register() error = %v", err)
		}
		id, _ := manager.Submit(t.Context(), task.Task{Type: "slow"}, task.SubmitOptions{})
		stop := runManager(t, manager)
		defer stop()

		info := waitForState(t, manager, id, task.StateDead)
		if info.Status.Attempt != 1 || info.Status.LastError != context.DeadlineExceeded.Error() {
			t.Fatalf("Status = %#v", info.Status)
		}
	})
}

func TestCancelAndRetryPendingTask(t *testing.T) {
	manager := inmemory.New(inmemory.Options{})
	if err := manager.Register("cancel", task.HandlerFunc(func(context.Context, task.TaskInfo) error {
		return nil
	}), task.HandlerOptions{}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	id, _ := manager.Submit(t.Context(), task.Task{Type: "cancel"}, task.SubmitOptions{})

	if err := manager.Cancel(t.Context(), id); err != nil {
		t.Fatalf("Cancel() error = %v", err)
	}
	if err := manager.Cancel(t.Context(), id); err != nil {
		t.Fatalf("second Cancel() error = %v", err)
	}
	info, err := manager.Get(t.Context(), id)
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if info.Status.State != task.StateCanceled {
		t.Fatalf("State = %s, want Canceled", info.Status.State)
	}
	if err := manager.Retry(t.Context(), id, time.Time{}); err != nil {
		t.Fatalf("Retry() error = %v", err)
	}

	stop := runManager(t, manager)
	defer stop()
	info = waitForState(t, manager, id, task.StateSucceeded)
	if info.Status.Attempt != 1 {
		t.Fatalf("Attempt after Retry() = %d, want 1", info.Status.Attempt)
	}
}

func TestCancelRunningTaskReturnsInvalidState(t *testing.T) {
	manager := inmemory.New(inmemory.Options{})
	started := make(chan struct{})
	release := make(chan struct{})
	if err := manager.Register("running", task.HandlerFunc(func(context.Context, task.TaskInfo) error {
		close(started)
		<-release
		return nil
	}), task.HandlerOptions{}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	id, _ := manager.Submit(t.Context(), task.Task{Type: "running"}, task.SubmitOptions{})
	stop := runManager(t, manager)
	defer stop()

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	err := manager.Cancel(t.Context(), id)
	close(release)
	if !errors.Is(err, task.ErrInvalidState) {
		t.Fatalf("Cancel() error = %v, want ErrInvalidState", err)
	}
	waitForState(t, manager, id, task.StateSucceeded)
}

func TestNotBeforeAndList(t *testing.T) {
	manager := inmemory.New(inmemory.Options{})
	executed := make(chan time.Time, 1)
	if err := manager.Register("delayed", task.HandlerFunc(func(context.Context, task.TaskInfo) error {
		executed <- time.Now()
		return nil
	}), task.HandlerOptions{}); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	notBefore := time.Now().
		Add(30 * time.Millisecond)
	id, _ := manager.Submit(t.Context(), task.Task{
		Type:   "delayed",
		Labels: map[string]string{"tenant": "one"},
	}, task.SubmitOptions{NotBefore: notBefore})
	_, _ = manager.Submit(t.Context(), task.Task{Type: "unregistered"}, task.SubmitOptions{})
	stop := runManager(t, manager)
	defer stop()

	select {
	case actual := <-executed:
		if actual.Before(notBefore) {
			t.Fatalf("task executed at %s before %s", actual, notBefore)
		}
	case <-time.After(time.Second):
		t.Fatal("delayed task did not execute")
	}
	waitForState(t, manager, id, task.StateSucceeded)

	page, err := manager.List(t.Context(), meta.ListOptions{
		Page:          1,
		Size:          1,
		Sort:          "-time",
		FieldSelector: "status.state=Succeeded",
		LabelSelector: "tenant=one",
	})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if page.Total == nil || *page.Total != 1 || len(page.Items) != 1 || page.Items[0].ID != id {
		t.Fatalf("List() = %#v", page)
	}
	page, err = manager.List(t.Context(), meta.ListOptions{
		Page:          0,
		Size:          1,
		Continue:      "ignored",
		FieldSelector: "status.state=Succeeded",
		LabelSelector: "tenant=one",
	})
	if err != nil {
		t.Fatalf("List() trusted pagination error = %v", err)
	}
	if page.Page != 1 || page.Size != 1 {
		t.Fatalf("List() trusted pagination = %#v", page)
	}

	_, err = manager.List(t.Context(), meta.ListOptions{Search: "delayed"})
	if !errors.Is(err, task.ErrInvalidArgument) {
		t.Fatalf("List() search error = %v, want ErrInvalidArgument", err)
	}
	_, err = manager.List(t.Context(), meta.ListOptions{FieldSelector: "payload=example"})
	if !errors.Is(err, task.ErrInvalidArgument) {
		t.Fatalf("List() field selector error = %v, want ErrInvalidArgument", err)
	}
}

func runManager(t *testing.T, manager *inmemory.Manager) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	return func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Error("Run() did not stop")
		}
	}
}

func waitForState(t *testing.T, manager task.Manager, id string, state task.State) task.TaskInfo {
	t.Helper()
	deadline := time.Now().
		Add(time.Second)
	for time.Now().
		Before(deadline) {
		info, err := manager.Get(t.Context(), id)
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if info.Status.State == state {
			return info
		}
		time.Sleep(time.Millisecond)
	}
	info, _ := manager.Get(t.Context(), id)
	t.Fatalf("task state = %s, want %s", info.Status.State, state)
	return task.TaskInfo{}
}
