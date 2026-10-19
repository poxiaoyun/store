// Package mongodb provides a persistent task implementation backed by MongoDB
// 5.0 or newer.
package mongodb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"maps"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"xiaoshiai.cn/common/meta"
	"xiaoshiai.cn/store/task"
)

const (
	defaultMaxWorkers    = 1
	defaultMaxAttempts   = 3
	defaultPollInterval  = time.Second
	defaultLeaseDuration = 30 * time.Second
	leaseExpiredError    = "execution lease expired"
)

// Options configures a Manager. Zero values select defaults.
type Options struct {
	MaxWorkers    int
	PollInterval  time.Duration
	LeaseDuration time.Duration
}

// New creates a MongoDB task manager and ensures its indexes exist.
func New(ctx context.Context, collection *mongo.Collection, options Options) (*Manager, error) {
	if options.MaxWorkers == 0 {
		options.MaxWorkers = defaultMaxWorkers
	}
	if options.PollInterval == 0 {
		options.PollInterval = defaultPollInterval
	}
	if options.LeaseDuration == 0 {
		options.LeaseDuration = defaultLeaseDuration
	}
	indexes := []mongo.IndexModel{
		{
			Keys: bson.D{
				{Key: "idempotencyKey", Value: 1},
			},
			Options: mongooptions.Index().
				SetName("task_idempotency").
				SetUnique(true).
				SetPartialFilterExpression(bson.D{
					{Key: "idempotencyKey", Value: bson.D{
						{Key: "$exists", Value: true},
					}},
				}),
		},
		{
			Keys: bson.D{
				{Key: "type", Value: 1},
				{Key: "status.state", Value: 1},
				{Key: "status.notBefore", Value: 1},
				{Key: "creationTimestamp", Value: 1},
			},
			Options: mongooptions.Index().
				SetName("task_claim"),
		},
		{
			Keys: bson.D{
				{Key: "status.state", Value: 1},
				{Key: "lease.deadline", Value: 1},
			},
			Options: mongooptions.Index().
				SetName("task_lease"),
		},
	}
	if _, err := collection.Indexes().
		CreateMany(ctx, indexes); err != nil {
		return nil, fmt.Errorf("create task indexes: %w", err)
	}
	return &Manager{
		collection: collection,
		options:    options,
		handlers:   make(map[string]registration),
	}, nil
}

var (
	_ task.Submitter = (*Manager)(nil)
	_ task.Manager   = (*Manager)(nil)
	_ task.Worker    = (*Manager)(nil)
)

// Manager stores and executes tasks in MongoDB. It must be constructed by New
// so the collection indexes and execution defaults are installed.
type Manager struct {
	collection *mongo.Collection
	options    Options

	mu       sync.Mutex
	handlers map[string]registration
}

type registration struct {
	taskType string
	handler  task.Handler
	options  task.HandlerOptions
}

type document struct {
	ID                string            `bson:"_id"`
	Type              string            `bson:"type"`
	Payload           []byte            `bson:"payload,omitempty"`
	IdempotencyKey    string            `bson:"idempotencyKey,omitempty"`
	Labels            map[string]string `bson:"labels,omitempty"`
	Annotations       map[string]string `bson:"annotations,omitempty"`
	CreationTimestamp time.Time         `bson:"creationTimestamp"`
	Status            documentStatus    `bson:"status"`
	Lease             *lease            `bson:"lease,omitempty"`
}

type documentStatus struct {
	State          task.State `bson:"state"`
	Attempt        int        `bson:"attempt"`
	NotBefore      time.Time  `bson:"notBefore"`
	StartTime      *time.Time `bson:"startTime,omitempty"`
	CompletionTime *time.Time `bson:"completionTime,omitempty"`
	LastError      string     `bson:"lastError,omitempty"`
}

type lease struct {
	Token       string    `bson:"token"`
	Deadline    time.Time `bson:"deadline"`
	MaxAttempts int       `bson:"maxAttempts"`
}

// Submit stores a task for execution.
func (m *Manager) Submit(ctx context.Context, work task.Task, submitOptions task.SubmitOptions) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if work.Type == "" {
		return "", fmt.Errorf("%w: task type is required", task.ErrInvalidArgument)
	}
	now := mongoTime(time.Now())
	doc := document{
		ID:                uuid.NewString(),
		Type:              work.Type,
		Payload:           bytes.Clone(work.Payload),
		IdempotencyKey:    work.IdempotencyKey,
		Labels:            maps.Clone(work.Labels),
		Annotations:       maps.Clone(work.Annotations),
		CreationTimestamp: now,
		Status: documentStatus{
			State:     task.StatePending,
			NotBefore: mongoTime(submitOptions.NotBefore),
		},
	}
	if _, err := m.collection.InsertOne(ctx, doc); err != nil {
		if !mongo.IsDuplicateKeyError(err) || work.IdempotencyKey == "" {
			return "", fmt.Errorf("submit task: %w", err)
		}
		var existing document
		if err := m.collection.FindOne(ctx, bson.D{
			{Key: "idempotencyKey", Value: work.IdempotencyKey},
		}).
			Decode(&existing); err != nil {
			return "", fmt.Errorf("get task by idempotency key: %w", err)
		}
		if existing.Type != doc.Type ||
			!bytes.Equal(existing.Payload, doc.Payload) ||
			!maps.Equal(existing.Labels, doc.Labels) ||
			!maps.Equal(existing.Annotations, doc.Annotations) ||
			!existing.Status.NotBefore.Equal(doc.Status.NotBefore) {
			return "", task.ErrConflict
		}
		return existing.ID, nil
	}
	return doc.ID, nil
}

// List returns an isolated snapshot of matching tasks. It supports page
// pagination, Sort, FieldSelector, and LabelSelector. Search and continuation
// pagination are unsupported.
func (m *Manager) List(ctx context.Context, listOptions meta.ListOptions) (meta.Page[task.TaskInfo], error) {
	filter, sort, err := listQuery(listOptions)
	if err != nil {
		return meta.Page[task.TaskInfo]{}, err
	}
	total, err := m.collection.CountDocuments(ctx, filter)
	if err != nil {
		return meta.Page[task.TaskInfo]{}, fmt.Errorf("count tasks: %w", err)
	}
	findOptions := mongooptions.Find().
		SetSort(sort)
	page := max(listOptions.Page, 1)
	if listOptions.Size > 0 {
		pageIndex := int64(page - 1)
		size := int64(listOptions.Size)
		skip := total
		if pageIndex <= total/size {
			skip = pageIndex * size
		}
		findOptions.SetSkip(skip)
		findOptions.SetLimit(size)
	}
	cursor, err := m.collection.Find(ctx, filter, findOptions)
	if err != nil {
		return meta.Page[task.TaskInfo]{}, fmt.Errorf("list tasks: %w", err)
	}
	defer cursor.Close(ctx)
	documents := []document{}
	if err := cursor.All(ctx, &documents); err != nil {
		return meta.Page[task.TaskInfo]{}, fmt.Errorf("decode tasks: %w", err)
	}
	items := make([]task.TaskInfo, 0, len(documents))
	for _, doc := range documents {
		items = append(items, doc.info())
	}
	totalResult := int(total)
	result := meta.Page[task.TaskInfo]{Total: &totalResult, Items: items}
	if listOptions.Size > 0 {
		result.Page = page
		result.Size = listOptions.Size
	}
	return result, nil
}

// Get returns a task snapshot.
func (m *Manager) Get(ctx context.Context, id string) (task.TaskInfo, error) {
	var doc document
	if err := m.collection.FindOne(ctx, bson.D{
		{Key: "_id", Value: id},
	}).
		Decode(&doc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return task.TaskInfo{}, task.ErrNotFound
		}
		return task.TaskInfo{}, fmt.Errorf("get task: %w", err)
	}
	return doc.info(), nil
}

// Cancel prevents a pending task from starting.
func (m *Manager) Cancel(ctx context.Context, id string) error {
	now := mongoTime(time.Now())
	result, err := m.collection.UpdateOne(ctx, bson.D{
		{Key: "_id", Value: id},
		{Key: "status.state", Value: task.StatePending},
	}, bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "status.state", Value: task.StateCanceled},
			{Key: "status.completionTime", Value: now},
		}},
	})
	if err != nil {
		return fmt.Errorf("cancel task: %w", err)
	}
	if result.MatchedCount == 1 {
		return nil
	}
	var current struct {
		Status documentStatus `bson:"status"`
	}
	if err := m.collection.FindOne(ctx, bson.D{
		{Key: "_id", Value: id},
	}, mongooptions.FindOne().
		SetProjection(bson.D{
			{Key: "status", Value: 1},
		})).
		Decode(&current); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return task.ErrNotFound
		}
		return fmt.Errorf("get task state: %w", err)
	}
	if current.Status.State == task.StateCanceled {
		return nil
	}
	return task.ErrInvalidState
}

// Retry starts a new execution cycle for a dead or canceled task.
func (m *Manager) Retry(ctx context.Context, id string, notBefore time.Time) error {
	result, err := m.collection.UpdateOne(ctx, bson.D{
		{Key: "_id", Value: id},
		{Key: "status.state", Value: bson.D{
			{Key: "$in", Value: bson.A{task.StateDead, task.StateCanceled}},
		}},
	}, bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "status", Value: documentStatus{
				State:     task.StatePending,
				NotBefore: mongoTime(notBefore),
			}},
		}},
		{Key: "$unset", Value: bson.D{
			{Key: "lease", Value: ""},
		}},
	})
	if err != nil {
		return fmt.Errorf("retry task: %w", err)
	}
	if result.MatchedCount == 1 {
		return nil
	}
	if err := m.collection.FindOne(ctx, bson.D{
		{Key: "_id", Value: id},
	}, mongooptions.FindOne().
		SetProjection(bson.D{
			{Key: "_id", Value: 1},
		})).
		Err(); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return task.ErrNotFound
		}
		return fmt.Errorf("get task state: %w", err)
	}
	return task.ErrInvalidState
}

// Register associates a Handler with a task type. Callers register before Run.
func (m *Manager) Register(taskType string, handler task.Handler, handlerOptions task.HandlerOptions) error {
	if taskType == "" || handler == nil || handlerOptions.MaxAttempts < 0 || handlerOptions.Timeout < 0 {
		return task.ErrInvalidArgument
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.handlers[taskType]; exists {
		return task.ErrAlreadyRegistered
	}
	if handlerOptions.MaxAttempts == 0 {
		handlerOptions.MaxAttempts = defaultMaxAttempts
	}
	m.handlers[taskType] = registration{
		taskType: taskType,
		handler:  handler,
		options:  handlerOptions,
	}
	return nil
}

// Run executes registered tasks until ctx is canceled or MongoDB fails.
func (m *Manager) Run(ctx context.Context) error {
	m.mu.Lock()
	registrations := make([]registration, 0, len(m.handlers))
	for _, registered := range m.handlers {
		registrations = append(registrations, registered)
	}
	m.mu.Unlock()

	if err := m.recoverExpired(ctx, mongoTime(time.Now())); err != nil {
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return nil
		}
		return err
	}
	group, runCtx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return m.recoverLoop(runCtx)
	})
	for range m.options.MaxWorkers {
		group.Go(func() error {
			return m.workerLoop(runCtx, registrations)
		})
	}
	if err := group.Wait(); err != nil {
		if ctx.Err() != nil && errors.Is(err, ctx.Err()) {
			return nil
		}
		return err
	}
	return nil
}

type claimedTask struct {
	token        string
	info         task.TaskInfo
	registration registration
}

func (m *Manager) workerLoop(ctx context.Context, registrations []registration) error {
	ticker := time.NewTicker(m.options.PollInterval)
	defer ticker.Stop()
	for {
		claimed, err := m.claim(ctx, registrations, mongoTime(time.Now()))
		if err != nil {
			return err
		}
		if claimed != nil {
			if err := m.execute(ctx, claimed); err != nil {
				return err
			}
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (m *Manager) claim(ctx context.Context, registrations []registration, now time.Time) (*claimedTask, error) {
	for _, registered := range registrations {
		token := uuid.NewString()
		deadline := mongoTime(now.Add(m.options.LeaseDuration))
		update := mongo.Pipeline{
			bson.D{
				{Key: "$set", Value: bson.D{
					{Key: "status.state", Value: task.StateRunning},
					{Key: "status.attempt", Value: bson.D{
						{Key: "$add", Value: bson.A{"$status.attempt", 1}},
					}},
					{Key: "status.startTime", Value: bson.D{
						{Key: "$ifNull", Value: bson.A{"$status.startTime", now}},
					}},
					{Key: "lease", Value: lease{
						Token:       token,
						Deadline:    deadline,
						MaxAttempts: registered.options.MaxAttempts,
					}},
				}},
			},
		}
		findOptions := mongooptions.FindOneAndUpdate().
			SetReturnDocument(mongooptions.After).
			SetSort(bson.D{
				{Key: "status.notBefore", Value: 1},
				{Key: "creationTimestamp", Value: 1},
				{Key: "_id", Value: 1},
			})
		var doc document
		if err := m.collection.FindOneAndUpdate(ctx, bson.D{
			{Key: "type", Value: registered.taskType},
			{Key: "status.state", Value: task.StatePending},
			{Key: "status.notBefore", Value: bson.D{
				{Key: "$lte", Value: now},
			}},
		}, update, findOptions).
			Decode(&doc); err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				continue
			}
			return nil, fmt.Errorf("claim task: %w", err)
		}
		return &claimedTask{
			token:        token,
			info:         doc.info(),
			registration: registered,
		}, nil
	}
	return nil, nil
}

type renewal struct {
	lost bool
	err  error
}

func (m *Manager) execute(ctx context.Context, claimed *claimedTask) error {
	handlerCtx, cancel := context.WithCancel(ctx)
	if claimed.registration.options.Timeout > 0 {
		handlerCtx, cancel = context.WithTimeout(ctx, claimed.registration.options.Timeout)
	}
	done := make(chan struct{})
	renewed := make(chan renewal, 1)
	go func() {
		renewed <- m.renew(handlerCtx, cancel, claimed, done)
	}()
	handlerErr := claimed.registration.handler.Handle(handlerCtx, claimed.info)
	close(done)
	result := <-renewed
	cancel()
	if result.err != nil {
		return result.err
	}
	if ctx.Err() != nil || result.lost {
		return nil
	}
	return m.finish(ctx, claimed, handlerErr, mongoTime(time.Now()))
}

func (m *Manager) renew(ctx context.Context, cancel context.CancelFunc, claimed *claimedTask, done <-chan struct{}) renewal {
	ticker := time.NewTicker(m.options.LeaseDuration / 3)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return renewal{}
		case <-done:
			return renewal{}
		case <-ticker.C:
			deadline := mongoTime(time.Now().
				Add(m.options.LeaseDuration))
			result, err := m.collection.UpdateOne(ctx, bson.D{
				{Key: "_id", Value: claimed.info.ID},
				{Key: "status.state", Value: task.StateRunning},
				{Key: "lease.token", Value: claimed.token},
			}, bson.D{
				{Key: "$set", Value: bson.D{
					{Key: "lease.deadline", Value: deadline},
				}},
			})
			if err != nil {
				if ctx.Err() != nil {
					return renewal{}
				}
				cancel()
				return renewal{err: fmt.Errorf("renew task lease: %w", err)}
			}
			if result.MatchedCount == 0 {
				cancel()
				return renewal{lost: true}
			}
		}
	}
}

func (m *Manager) finish(ctx context.Context, claimed *claimedTask, handlerErr error, now time.Time) error {
	filter := bson.D{
		{Key: "_id", Value: claimed.info.ID},
		{Key: "status.state", Value: task.StateRunning},
		{Key: "lease.token", Value: claimed.token},
	}
	var set bson.D
	if handlerErr != nil {
		if task.IsNoRetry(handlerErr) || claimed.info.Status.Attempt >= claimed.registration.options.MaxAttempts {
			set = bson.D{
				{Key: "status.state", Value: task.StateDead},
				{Key: "status.lastError", Value: handlerErr.Error()},
				{Key: "status.completionTime", Value: now},
			}
		} else {
			delay, specified := task.RetryDelay(handlerErr)
			if !specified {
				delay = defaultRetryBackoff(claimed.info.Status.Attempt)
			}
			set = bson.D{
				{Key: "status.state", Value: task.StatePending},
				{Key: "status.lastError", Value: handlerErr.Error()},
				{Key: "status.notBefore", Value: mongoTime(now.Add(delay))},
			}
		}
	} else {
		set = bson.D{
			{Key: "status.state", Value: task.StateSucceeded},
			{Key: "status.lastError", Value: ""},
			{Key: "status.completionTime", Value: now},
		}
	}
	if _, err := m.collection.UpdateOne(ctx, filter, bson.D{
		{Key: "$set", Value: set},
		{Key: "$unset", Value: bson.D{
			{Key: "lease", Value: ""},
		}},
	}); err != nil {
		return fmt.Errorf("finish task: %w", err)
	}
	return nil
}

func (m *Manager) recoverLoop(ctx context.Context) error {
	ticker := time.NewTicker(m.options.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
		if err := m.recoverExpired(ctx, mongoTime(time.Now())); err != nil {
			return err
		}
	}
}

func (m *Manager) recoverExpired(ctx context.Context, now time.Time) error {
	expired := bson.D{
		{Key: "status.state", Value: task.StateRunning},
		{Key: "lease.deadline", Value: bson.D{
			{Key: "$lte", Value: now},
		}},
	}
	dead := append(bson.D{}, expired...)
	dead = append(dead, bson.E{Key: "$expr", Value: bson.D{
		{Key: "$gte", Value: bson.A{"$status.attempt", "$lease.maxAttempts"}},
	}})
	if _, err := m.collection.UpdateMany(ctx, dead, bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "status.state", Value: task.StateDead},
			{Key: "status.lastError", Value: leaseExpiredError},
			{Key: "status.completionTime", Value: now},
		}},
		{Key: "$unset", Value: bson.D{
			{Key: "lease", Value: ""},
		}},
	}); err != nil {
		return fmt.Errorf("recover exhausted tasks: %w", err)
	}
	pending := append(bson.D{}, expired...)
	pending = append(pending, bson.E{Key: "$expr", Value: bson.D{
		{Key: "$lt", Value: bson.A{"$status.attempt", "$lease.maxAttempts"}},
	}})
	if _, err := m.collection.UpdateMany(ctx, pending, bson.D{
		{Key: "$set", Value: bson.D{
			{Key: "status.state", Value: task.StatePending},
			{Key: "status.lastError", Value: leaseExpiredError},
			{Key: "status.notBefore", Value: now},
		}},
		{Key: "$unset", Value: bson.D{
			{Key: "lease", Value: ""},
		}},
	}); err != nil {
		return fmt.Errorf("recover tasks: %w", err)
	}
	return nil
}

func (d document) info() task.TaskInfo {
	return task.TaskInfo{
		ID:                d.ID,
		CreationTimestamp: d.CreationTimestamp,
		Task: task.Task{
			Type:           d.Type,
			Payload:        bytes.Clone(d.Payload),
			IdempotencyKey: d.IdempotencyKey,
			Labels:         maps.Clone(d.Labels),
			Annotations:    maps.Clone(d.Annotations),
		},
		Status: task.TaskStatus{
			State:          d.Status.State,
			Attempt:        d.Status.Attempt,
			NotBefore:      d.Status.NotBefore,
			StartTime:      d.Status.StartTime,
			CompletionTime: d.Status.CompletionTime,
			LastError:      d.Status.LastError,
		},
	}
}

func mongoTime(value time.Time) time.Time {
	if value.IsZero() {
		return time.Time{}
	}
	return value.UTC().
		Truncate(time.Millisecond)
}

func defaultRetryBackoff(attempt int) time.Duration {
	delay := 100 * time.Millisecond * time.Duration(1<<min(attempt-1, 8))
	return min(delay, 30*time.Second)
}

func listQuery(listOptions meta.ListOptions) (bson.D, bson.D, error) {
	if listOptions.Search != "" {
		return nil, nil, fmt.Errorf("%w: search is unsupported", task.ErrInvalidArgument)
	}
	if listOptions.Limit > 0 {
		return nil, nil, fmt.Errorf("%w: continue is unsupported", task.ErrInvalidArgument)
	}
	conditions := bson.A{}
	fieldSelector, err := fields.ParseSelector(listOptions.FieldSelector)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: field selector: %v", task.ErrInvalidArgument, err)
	}
	for _, requirement := range fieldSelector.Requirements() {
		field, value, err := fieldRequirement(requirement.Field, requirement.Value)
		if err != nil {
			return nil, nil, err
		}
		switch requirement.Operator {
		case selection.Equals, selection.DoubleEquals:
			if requirement.Field == "idempotencyKey" && requirement.Value == "" {
				conditions = append(conditions, bson.D{
					{Key: "$or", Value: bson.A{
						bson.D{
							{Key: field, Value: ""},
						},
						bson.D{
							{Key: field, Value: bson.D{
								{Key: "$exists", Value: false},
							}},
						},
					}},
				})
				continue
			}
			conditions = append(conditions, bson.D{
				{Key: field, Value: value},
			})
		case selection.NotEquals:
			if requirement.Field == "idempotencyKey" && requirement.Value == "" {
				conditions = append(conditions, bson.D{
					{Key: field, Value: bson.D{
						{Key: "$exists", Value: true},
						{Key: "$ne", Value: ""},
					}},
				})
				continue
			}
			conditions = append(conditions, bson.D{
				{Key: field, Value: bson.D{
					{Key: "$ne", Value: value},
				}},
			})
		default:
			return nil, nil, fmt.Errorf("%w: unsupported field selector operator %q", task.ErrInvalidArgument, requirement.Operator)
		}
	}
	labelSelector, err := labels.Parse(listOptions.LabelSelector)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: label selector: %v", task.ErrInvalidArgument, err)
	}
	requirements, selectable := labelSelector.Requirements()
	if !selectable {
		conditions = append(conditions, bson.D{
			{Key: "$expr", Value: false},
		})
	}
	for _, requirement := range requirements {
		expression, err := labelExpression(requirement)
		if err != nil {
			return nil, nil, err
		}
		conditions = append(conditions, bson.D{
			{Key: "$expr", Value: expression},
		})
	}
	filter := bson.D{}
	if len(conditions) > 0 {
		filter = bson.D{
			{Key: "$and", Value: conditions},
		}
	}
	sort := bson.D{}
	hasID := false
	for _, field := range meta.ParseSort(listOptions.Sort) {
		name := field.Field
		if name == "id" {
			name = "_id"
			hasID = true
		}
		if name == "time" {
			name = "creationTimestamp"
		}
		switch name {
		case "_id", "type", "status.state", "status.attempt", "status.notBefore", "creationTimestamp":
		default:
			return nil, nil, fmt.Errorf("%w: unsupported sort field %q", task.ErrInvalidArgument, field.Field)
		}
		direction := 1
		if field.Direction == meta.SortDirectionDesc {
			direction = -1
		}
		sort = append(sort, bson.E{Key: name, Value: direction})
	}
	if len(sort) == 0 {
		sort = bson.D{
			{Key: "creationTimestamp", Value: 1},
		}
	}
	if !hasID {
		sort = append(sort, bson.E{Key: "_id", Value: 1})
	}
	return filter, sort, nil
}

func fieldRequirement(field, value string) (string, any, error) {
	switch field {
	case "id":
		return "_id", value, nil
	case "type", "idempotencyKey", "status.state":
		return field, value, nil
	case "status.attempt":
		attempt, err := strconv.Atoi(value)
		if err != nil {
			return "", nil, fmt.Errorf("%w: invalid status.attempt %q", task.ErrInvalidArgument, value)
		}
		return field, attempt, nil
	case "status.notBefore", "creationTimestamp":
		parsed, err := time.Parse(time.RFC3339Nano, value)
		if err != nil {
			return "", nil, fmt.Errorf("%w: invalid %s %q", task.ErrInvalidArgument, field, value)
		}
		return field, mongoTime(parsed), nil
	default:
		return "", nil, fmt.Errorf("%w: unsupported field selector %q", task.ErrInvalidArgument, field)
	}
}

func labelExpression(requirement labels.Requirement) (bson.D, error) {
	value := bson.D{
		{Key: "$getField", Value: bson.D{
			{Key: "field", Value: bson.D{
				{Key: "$literal", Value: requirement.Key()},
			}},
			{Key: "input", Value: bson.D{
				{Key: "$ifNull", Value: bson.A{"$labels", bson.D{}}},
			}},
		}},
	}
	typeOf := bson.D{
		{Key: "$type", Value: value},
	}
	exists := bson.D{
		{Key: "$ne", Value: bson.A{typeOf, "missing"}},
	}
	missing := bson.D{
		{Key: "$eq", Value: bson.A{typeOf, "missing"}},
	}
	values := requirement.Values().
		List()
	switch requirement.Operator() {
	case selection.Equals, selection.DoubleEquals:
		return bson.D{
			{Key: "$and", Value: bson.A{
				exists,
				bson.D{
					{Key: "$eq", Value: bson.A{value, values[0]}},
				},
			}},
		}, nil
	case selection.NotEquals:
		return bson.D{
			{Key: "$or", Value: bson.A{
				missing,
				bson.D{
					{Key: "$ne", Value: bson.A{value, values[0]}},
				},
			}},
		}, nil
	case selection.In:
		return bson.D{
			{Key: "$and", Value: bson.A{
				exists,
				bson.D{
					{Key: "$in", Value: bson.A{value, values}},
				},
			}},
		}, nil
	case selection.NotIn:
		return bson.D{
			{Key: "$or", Value: bson.A{
				missing,
				bson.D{
					{Key: "$not", Value: bson.A{
						bson.D{
							{Key: "$in", Value: bson.A{value, values}},
						},
					}},
				},
			}},
		}, nil
	case selection.Exists:
		return exists, nil
	case selection.DoesNotExist:
		return missing, nil
	default:
		return nil, fmt.Errorf("%w: unsupported label selector operator %q", task.ErrInvalidArgument, requirement.Operator())
	}
}
