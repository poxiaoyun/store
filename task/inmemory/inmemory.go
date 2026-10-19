// Package inmemory provides an ephemeral task implementation for tests and
// local development. Tasks are lost when the process exits.
package inmemory

import (
	"bytes"
	"context"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"xiaoshiai.cn/common/meta"
	"xiaoshiai.cn/store/task"
)

const (
	defaultMaxWorkers  = 1
	defaultMaxAttempts = 3
)

// Options configures a Manager. Zero values select defaults.
type Options struct {
	MaxWorkers int
}

// New creates an empty in-memory task manager.
func New(options Options) *Manager {
	if options.MaxWorkers == 0 {
		options.MaxWorkers = defaultMaxWorkers
	}
	return &Manager{
		maxWorkers:  options.MaxWorkers,
		tasks:       make(map[string]*task.TaskInfo),
		idempotency: make(map[string]string),
		handlers:    make(map[string]registration),
		wakeup:      make(chan struct{}, options.MaxWorkers),
	}
}

var (
	_ task.Submitter = (*Manager)(nil)
	_ task.Manager   = (*Manager)(nil)
	_ task.Worker    = (*Manager)(nil)
)

// Manager stores and executes tasks for the lifetime of the process.
type Manager struct {
	mu          sync.Mutex
	maxWorkers  int
	tasks       map[string]*task.TaskInfo
	order       []string
	idempotency map[string]string
	handlers    map[string]registration
	wakeup      chan struct{}
}

type registration struct {
	handler task.Handler
	options task.HandlerOptions
}

// Submit accepts a task for execution. Submitted payload and metadata are copied.
func (m *Manager) Submit(ctx context.Context, work task.Task, options task.SubmitOptions) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if work.Type == "" {
		return "", fmt.Errorf("%w: task type is required", task.ErrInvalidArgument)
	}
	work.Payload = bytes.Clone(work.Payload)
	work.Labels = maps.Clone(work.Labels)
	work.Annotations = maps.Clone(work.Annotations)

	m.mu.Lock()
	if work.IdempotencyKey != "" {
		if id, exists := m.idempotency[work.IdempotencyKey]; exists {
			existing := m.tasks[id]
			if existing.Type == work.Type &&
				bytes.Equal(existing.Payload, work.Payload) &&
				maps.Equal(existing.Labels, work.Labels) &&
				maps.Equal(existing.Annotations, work.Annotations) &&
				existing.Status.NotBefore.Equal(options.NotBefore) {
				m.mu.Unlock()
				return id, nil
			}
			m.mu.Unlock()
			return "", task.ErrConflict
		}
	}

	now := time.Now()
	id := uuid.NewString()
	m.tasks[id] = &task.TaskInfo{
		ID:                id,
		CreationTimestamp: now,
		Task:              work,
		Status: task.TaskStatus{
			State:     task.StatePending,
			NotBefore: options.NotBefore,
		},
	}
	m.order = append(m.order, id)
	if work.IdempotencyKey != "" {
		m.idempotency[work.IdempotencyKey] = id
	}
	m.mu.Unlock()

	m.notify()
	return id, nil
}

// List returns an isolated snapshot of matching tasks. It supports page
// pagination, Sort, FieldSelector, and LabelSelector. Search and continuation
// pagination are unsupported.
func (m *Manager) List(ctx context.Context, options meta.ListOptions) (meta.Page[task.TaskInfo], error) {
	if err := ctx.Err(); err != nil {
		return meta.Page[task.TaskInfo]{}, err
	}
	if options.Search != "" {
		return meta.Page[task.TaskInfo]{}, fmt.Errorf("%w: search is unsupported", task.ErrInvalidArgument)
	}
	if options.Limit > 0 {
		return meta.Page[task.TaskInfo]{}, fmt.Errorf("%w: continue is unsupported", task.ErrInvalidArgument)
	}
	fieldSelector, err := fields.ParseSelector(options.FieldSelector)
	if err != nil {
		return meta.Page[task.TaskInfo]{}, fmt.Errorf("%w: field selector: %v", task.ErrInvalidArgument, err)
	}
	for _, requirement := range fieldSelector.Requirements() {
		switch requirement.Field {
		case "id", "type", "idempotencyKey", "status.state", "status.attempt",
			"status.notBefore", "creationTimestamp":
		default:
			return meta.Page[task.TaskInfo]{}, fmt.Errorf(
				"%w: unsupported field selector %q",
				task.ErrInvalidArgument,
				requirement.Field,
			)
		}
	}
	labelSelector, err := labels.Parse(options.LabelSelector)
	if err != nil {
		return meta.Page[task.TaskInfo]{}, fmt.Errorf("%w: label selector: %v", task.ErrInvalidArgument, err)
	}

	m.mu.Lock()
	items := make([]task.TaskInfo, 0, len(m.order))
	for _, id := range m.order {
		info := cloneTaskInfo(*m.tasks[id])
		if !fieldSelector.Matches(taskFields(info)) || !labelSelector.Matches(labels.Set(info.Labels)) {
			continue
		}
		items = append(items, info)
	}
	m.mu.Unlock()

	if err := sortTaskInfo(items, options.Sort); err != nil {
		return meta.Page[task.TaskInfo]{}, err
	}
	return paginate(items, options), nil
}

// Get returns an isolated snapshot of a task.
func (m *Manager) Get(ctx context.Context, id string) (task.TaskInfo, error) {
	if err := ctx.Err(); err != nil {
		return task.TaskInfo{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	info, exists := m.tasks[id]
	if !exists {
		return task.TaskInfo{}, task.ErrNotFound
	}
	return cloneTaskInfo(*info), nil
}

// Cancel prevents a pending task from starting.
func (m *Manager) Cancel(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	info, exists := m.tasks[id]
	if !exists {
		return task.ErrNotFound
	}
	switch info.Status.State {
	case task.StateCanceled:
		return nil
	case task.StatePending:
		now := time.Now()
		info.Status.State = task.StateCanceled
		info.Status.CompletionTime = &now
		return nil
	default:
		return task.ErrInvalidState
	}
}

// Retry starts a new execution cycle for a dead or canceled task.
func (m *Manager) Retry(ctx context.Context, id string, notBefore time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	info, exists := m.tasks[id]
	if !exists {
		return task.ErrNotFound
	}
	if info.Status.State != task.StateDead && info.Status.State != task.StateCanceled {
		return task.ErrInvalidState
	}
	info.Status = task.TaskStatus{
		State:     task.StatePending,
		NotBefore: notBefore,
	}
	m.notify()
	return nil
}

// Register associates a Handler with a task type. Callers register before Run.
func (m *Manager) Register(taskType string, handler task.Handler, options task.HandlerOptions) error {
	if taskType == "" || handler == nil || options.MaxAttempts < 0 || options.Timeout < 0 {
		return task.ErrInvalidArgument
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.handlers[taskType]; exists {
		return task.ErrAlreadyRegistered
	}
	if options.MaxAttempts == 0 {
		options.MaxAttempts = defaultMaxAttempts
	}
	m.handlers[taskType] = registration{
		handler: handler,
		options: options,
	}
	return nil
}

// Run executes registered tasks until ctx is canceled.
func (m *Manager) Run(ctx context.Context) error {
	var workers sync.WaitGroup
	workers.Add(m.maxWorkers)
	for range m.maxWorkers {
		go func() {
			defer workers.Done()
			m.runWorker(ctx)
		}()
	}
	workers.Wait()
	return nil
}

type claimedTask struct {
	id      string
	info    task.TaskInfo
	handler task.Handler
	ctx     context.Context
	cancel  context.CancelFunc
}

func (m *Manager) runWorker(ctx context.Context) {
	for {
		claimed, wait := m.claim(ctx, time.Now())
		if claimed != nil {
			err := claimed.handler.Handle(claimed.ctx, claimed.info)
			claimed.cancel()
			m.finish(ctx, claimed, err, time.Now())
			continue
		}
		if !waitForWork(ctx, m.wakeup, wait) {
			return
		}
	}
}

func (m *Manager) claim(ctx context.Context, now time.Time) (*claimedTask, time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var earliest time.Time
	for _, id := range m.order {
		info := m.tasks[id]
		if info.Status.State != task.StatePending {
			continue
		}
		registration, exists := m.handlers[info.Type]
		if !exists {
			continue
		}
		if info.Status.NotBefore.After(now) {
			if earliest.IsZero() || info.Status.NotBefore.Before(earliest) {
				earliest = info.Status.NotBefore
			}
			continue
		}

		info.Status.State = task.StateRunning
		info.Status.Attempt++
		if info.Status.StartTime == nil {
			started := now
			info.Status.StartTime = &started
		}
		handlerCtx, cancel := context.WithCancel(ctx)
		if registration.options.Timeout > 0 {
			handlerCtx, cancel = context.WithTimeout(ctx, registration.options.Timeout)
		}
		return &claimedTask{
			id:      id,
			info:    cloneTaskInfo(*info),
			handler: registration.handler,
			ctx:     handlerCtx,
			cancel:  cancel,
		}, 0
	}
	if earliest.IsZero() {
		return nil, 0
	}
	return nil, time.Until(earliest)
}

func (m *Manager) finish(runCtx context.Context, claimed *claimedTask, handlerErr error, now time.Time) {
	m.mu.Lock()
	info := m.tasks[claimed.id]

	if runCtx.Err() != nil {
		info.Status.State = task.StatePending
		info.Status.NotBefore = time.Time{}
		m.mu.Unlock()
		return
	}
	if handlerErr == nil {
		info.Status.State = task.StateSucceeded
		info.Status.LastError = ""
		info.Status.CompletionTime = timePointer(now)
		m.mu.Unlock()
		return
	}

	info.Status.LastError = handlerErr.Error()
	registration := m.handlers[info.Type]
	if task.IsNoRetry(handlerErr) || info.Status.Attempt >= registration.options.MaxAttempts {
		info.Status.State = task.StateDead
		info.Status.CompletionTime = timePointer(now)
		m.mu.Unlock()
		return
	}

	delay, specified := task.RetryDelay(handlerErr)
	if !specified {
		delay = defaultRetryBackoff(info.Status.Attempt)
	}
	info.Status.State = task.StatePending
	info.Status.NotBefore = now.Add(delay)
	m.mu.Unlock()
	m.notify()
}

func (m *Manager) notify() {
	select {
	case m.wakeup <- struct{}{}:
	default:
	}
}

func cloneTaskInfo(info task.TaskInfo) task.TaskInfo {
	cloned := info
	cloned.Payload = bytes.Clone(info.Payload)
	cloned.Labels = maps.Clone(info.Labels)
	cloned.Annotations = maps.Clone(info.Annotations)
	if info.Status.StartTime != nil {
		cloned.Status.StartTime = timePointer(*info.Status.StartTime)
	}
	if info.Status.CompletionTime != nil {
		cloned.Status.CompletionTime = timePointer(*info.Status.CompletionTime)
	}
	return cloned
}

func timePointer(value time.Time) *time.Time {
	return &value
}

func waitForWork(ctx context.Context, wakeup <-chan struct{}, wait time.Duration) bool {
	if wait <= 0 {
		select {
		case <-ctx.Done():
			return false
		case <-wakeup:
			return true
		}
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-wakeup:
		return true
	case <-timer.C:
		return true
	}
}

func defaultRetryBackoff(attempt int) time.Duration {
	delay := 100 * time.Millisecond * time.Duration(1<<min(attempt-1, 8))
	return min(delay, 30*time.Second)
}

func taskFields(info task.TaskInfo) fields.Set {
	return fields.Set{
		"id":                info.ID,
		"type":              info.Type,
		"idempotencyKey":    info.IdempotencyKey,
		"status.state":      string(info.Status.State),
		"status.attempt":    strconv.Itoa(info.Status.Attempt),
		"status.notBefore":  info.Status.NotBefore.Format(time.RFC3339Nano),
		"creationTimestamp": info.CreationTimestamp.Format(time.RFC3339Nano),
	}
}

func sortTaskInfo(items []task.TaskInfo, expression string) error {
	fields := meta.ParseSort(expression)
	if len(fields) == 0 {
		fields = []meta.SortField{
			{
				Field:     "creationTimestamp",
				Direction: meta.SortDirectionAsc,
			},
		}
	}
	for _, field := range fields {
		switch field.Field {
		case "id", "type", "status.state", "status.attempt", "status.notBefore",
			"time", "creationTimestamp":
		default:
			return fmt.Errorf("%w: unsupported sort field %q", task.ErrInvalidArgument, field.Field)
		}
	}
	slices.SortStableFunc(items, func(left, right task.TaskInfo) int {
		for _, field := range fields {
			comparison := compareField(left, right, field.Field)
			if comparison == 0 {
				continue
			}
			if field.Direction == meta.SortDirectionDesc {
				return -comparison
			}
			return comparison
		}
		return strings.Compare(left.ID, right.ID)
	})
	return nil
}

func compareField(left, right task.TaskInfo, field string) int {
	switch field {
	case "id":
		return strings.Compare(left.ID, right.ID)
	case "type":
		return strings.Compare(left.Type, right.Type)
	case "status.state":
		return strings.Compare(string(left.Status.State), string(right.Status.State))
	case "status.attempt":
		return left.Status.Attempt - right.Status.Attempt
	case "status.notBefore":
		return left.Status.NotBefore.Compare(right.Status.NotBefore)
	case "time", "creationTimestamp":
		return left.CreationTimestamp.Compare(right.CreationTimestamp)
	default:
		return 0
	}
}

func paginate(items []task.TaskInfo, options meta.ListOptions) meta.Page[task.TaskInfo] {
	total := len(items)
	totalResult := total
	if options.Size <= 0 {
		return meta.Page[task.TaskInfo]{Total: &totalResult, Items: items}
	}
	page := max(options.Page, 1)
	pageIndex := page - 1
	offset := total
	if pageIndex <= total/options.Size {
		offset = pageIndex * options.Size
	}
	end := offset + min(options.Size, total-offset)
	return meta.Page[task.TaskInfo]{
		Total: &totalResult,
		Items: items[offset:end],
		Page:  page,
		Size:  options.Size,
	}
}
