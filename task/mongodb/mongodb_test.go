package mongodb_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo/integration/mtest"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
	"xiaoshiai.cn/common/meta"
	testmongodb "xiaoshiai.cn/common/testkit/mongodb"
	"xiaoshiai.cn/store/task"
	"xiaoshiai.cn/store/task/mongodb"
)

func TestSubmitStoresMetadataKeysLiterally(t *testing.T) {
	test := mtest.New(t, mtest.NewOptions().
		ClientType(mtest.Mock))
	test.Run("submit", func(mt *mtest.T) {
		mt.AddMockResponses(
			mtest.CreateSuccessResponse(),
			mtest.CreateSuccessResponse(bson.E{Key: "n", Value: 1}),
		)
		manager, err := mongodb.New(mt.Context(), mt.Coll, mongodb.Options{})
		if err != nil {
			mt.Fatalf("New() error = %v", err)
		}
		_, err = manager.Submit(mt.Context(), task.Task{
			Type: "example",
			Labels: map[string]string{
				"example.com/team": "iam",
			},
			Annotations: map[string]string{
				"example.com/trace": "one",
			},
		}, task.SubmitOptions{})
		if err != nil {
			mt.Fatalf("Submit() error = %v", err)
		}

		var insert bson.Raw
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName == "insert" {
				insert = event.Command
			}
		}
		if insert == nil {
			mt.Fatal("insert command was not sent")
		}
		documents, err := insert.LookupErr("documents")
		if err != nil {
			mt.Fatalf("insert documents error = %v", err)
		}
		values, err := documents.Array().
			Values()
		if err != nil {
			mt.Fatalf("insert documents values error = %v", err)
		}
		stored := values[0].Document()
		if got := stored.Lookup("labels").
			Document().
			Lookup("example.com/team").
			StringValue(); got != "iam" {
			mt.Fatalf("stored label = %q, want iam", got)
		}
		if got := stored.Lookup("annotations").
			Document().
			Lookup("example.com/trace").
			StringValue(); got != "one" {
			mt.Fatalf("stored annotation = %q, want one", got)
		}
	})
}

func TestSubmitReusesIdempotencyKey(t *testing.T) {
	test := mtest.New(t, mtest.NewOptions().
		ClientType(mtest.Mock))
	test.Run("submit", func(mt *mtest.T) {
		namespace := mt.DB.Name() + "." + mt.Coll.Name()
		mt.AddMockResponses(
			mtest.CreateSuccessResponse(),
			mtest.CreateWriteErrorsResponse(mtest.WriteError{
				Index:   0,
				Code:    11000,
				Message: "duplicate key",
			}),
			mtest.CreateCursorResponse(0, namespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: "task-1"},
				{Key: "type", Value: "example"},
				{Key: "payload", Value: []byte("payload")},
				{Key: "idempotencyKey", Value: "operation-1"},
				{Key: "status", Value: bson.D{
					{Key: "state", Value: task.StatePending},
					{Key: "attempt", Value: 0},
					{Key: "notBefore", Value: time.Time{}},
				}},
			}),
		)
		manager, err := mongodb.New(mt.Context(), mt.Coll, mongodb.Options{})
		if err != nil {
			mt.Fatalf("New() error = %v", err)
		}
		id, err := manager.Submit(mt.Context(), task.Task{
			Type:           "example",
			Payload:        []byte("payload"),
			IdempotencyKey: "operation-1",
		}, task.SubmitOptions{})
		if err != nil {
			mt.Fatalf("Submit() error = %v", err)
		}
		if id != "task-1" {
			mt.Fatalf("Submit() ID = %q, want task-1", id)
		}
	})
}

func TestListUsesGetFieldForLabelSelector(t *testing.T) {
	test := mtest.New(t, mtest.NewOptions().
		ClientType(mtest.Mock))
	test.Run("list", func(mt *mtest.T) {
		namespace := mt.DB.Name() + "." + mt.Coll.Name()
		now := time.Now().
			UTC().
			Truncate(time.Millisecond)
		mt.AddMockResponses(
			mtest.CreateSuccessResponse(),
			mtest.CreateCursorResponse(0, namespace, mtest.FirstBatch, bson.D{
				{Key: "n", Value: 1},
			}),
			mtest.CreateCursorResponse(0, namespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: "task-1"},
				{Key: "type", Value: "example"},
				{Key: "labels", Value: bson.D{
					{Key: "example.com/team", Value: "iam"},
				}},
				{Key: "creationTimestamp", Value: now},
				{Key: "status", Value: bson.D{
					{Key: "state", Value: task.StatePending},
					{Key: "attempt", Value: 0},
					{Key: "notBefore", Value: time.Time{}},
				}},
			}),
		)
		manager, err := mongodb.New(mt.Context(), mt.Coll, mongodb.Options{})
		if err != nil {
			mt.Fatalf("New() error = %v", err)
		}
		page, err := manager.List(mt.Context(), meta.ListOptions{
			LabelSelector: "example.com/team=iam",
		})
		if err != nil {
			mt.Fatalf("List() error = %v", err)
		}
		if len(page.Items) != 1 || page.Items[0].Labels["example.com/team"] != "iam" {
			mt.Fatalf("List() = %#v", page)
		}

		var aggregate bson.Raw
		for _, event := range mt.GetAllStartedEvents() {
			if event.CommandName == "aggregate" {
				aggregate = event.Command
			}
		}
		encoded, err := bson.MarshalExtJSON(aggregate, false, false)
		if err != nil {
			mt.Fatalf("MarshalExtJSON() error = %v", err)
		}
		query := string(encoded)
		for _, expected := range []string{"$getField", "$literal", "$ifNull", "example.com/team"} {
			if !strings.Contains(query, expected) {
				mt.Fatalf("aggregate command %s does not contain %q", query, expected)
			}
		}
	})
}

func TestCancelRunningTaskReturnsInvalidState(t *testing.T) {
	test := mtest.New(t, mtest.NewOptions().
		ClientType(mtest.Mock))
	test.Run("cancel", func(mt *mtest.T) {
		namespace := mt.DB.Name() + "." + mt.Coll.Name()
		mt.AddMockResponses(
			mtest.CreateSuccessResponse(),
			mtest.CreateSuccessResponse(
				bson.E{Key: "n", Value: 0},
				bson.E{Key: "nModified", Value: 0},
			),
			mtest.CreateCursorResponse(0, namespace, mtest.FirstBatch, bson.D{
				{Key: "_id", Value: "task-1"},
				{Key: "status", Value: bson.D{
					{Key: "state", Value: task.StateRunning},
				}},
			}),
		)
		manager, err := mongodb.New(mt.Context(), mt.Coll, mongodb.Options{})
		if err != nil {
			mt.Fatalf("New() error = %v", err)
		}
		if err := manager.Cancel(mt.Context(), "task-1"); err != task.ErrInvalidState {
			mt.Fatalf("Cancel() error = %v, want ErrInvalidState", err)
		}
	})
}

func TestWorkerClaimsAndCompletesTask(t *testing.T) {
	updates := make(chan struct{}, 3)
	clientOptions := mongooptions.Client().
		SetMonitor(&event.CommandMonitor{
			Succeeded: func(_ context.Context, commandEvent *event.CommandSucceededEvent) {
				if commandEvent.CommandName == "update" {
					updates <- struct{}{}
				}
			},
		})
	testOptions := mtest.NewOptions().
		ClientType(mtest.Mock).
		ClientOptions(clientOptions)
	test := mtest.New(t, testOptions)
	test.Run("worker", func(mt *mtest.T) {
		now := time.Now().
			UTC().
			Truncate(time.Millisecond)
		mt.AddMockResponses(
			mtest.CreateSuccessResponse(),
			mtest.CreateSuccessResponse(
				bson.E{Key: "n", Value: 0},
				bson.E{Key: "nModified", Value: 0},
			),
			mtest.CreateSuccessResponse(
				bson.E{Key: "n", Value: 0},
				bson.E{Key: "nModified", Value: 0},
			),
			mtest.CreateSuccessResponse(bson.E{Key: "value", Value: bson.D{
				{Key: "_id", Value: "task-1"},
				{Key: "type", Value: "example"},
				{Key: "creationTimestamp", Value: now},
				{Key: "status", Value: bson.D{
					{Key: "state", Value: task.StateRunning},
					{Key: "attempt", Value: 1},
					{Key: "notBefore", Value: time.Time{}},
					{Key: "startTime", Value: now},
				}},
			}}),
			mtest.CreateSuccessResponse(
				bson.E{Key: "n", Value: 1},
				bson.E{Key: "nModified", Value: 1},
			),
			mtest.CreateSuccessResponse(bson.E{Key: "value", Value: nil}),
		)
		manager, err := mongodb.New(mt.Context(), mt.Coll, mongodb.Options{
			PollInterval:  time.Hour,
			LeaseDuration: time.Hour,
		})
		if err != nil {
			mt.Fatalf("New() error = %v", err)
		}
		handled := make(chan task.TaskInfo, 1)
		if err := manager.Register("example", task.HandlerFunc(func(_ context.Context, info task.TaskInfo) error {
			handled <- info
			return nil
		}), task.HandlerOptions{}); err != nil {
			mt.Fatalf("Register() error = %v", err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- manager.Run(ctx)
		}()
		select {
		case info := <-handled:
			if info.ID != "task-1" || info.Status.Attempt != 1 {
				mt.Fatalf("handled task = %#v", info)
			}
		case <-time.After(time.Second):
			mt.Fatal("handler was not called")
		}
		for range 3 {
			select {
			case <-updates:
			case <-time.After(time.Second):
				mt.Fatal("task completion update was not sent")
			}
		}
		cancel()
		select {
		case err := <-done:
			if err != nil {
				mt.Fatalf("Run() error = %v", err)
			}
		case <-time.After(time.Second):
			mt.Fatal("Run() did not stop")
		}

		var completion bson.Raw
		for _, commandEvent := range mt.GetAllStartedEvents() {
			if commandEvent.CommandName == "update" {
				completion = commandEvent.Command
			}
		}
		encoded, err := bson.MarshalExtJSON(completion, false, false)
		if err != nil {
			mt.Fatalf("MarshalExtJSON() error = %v", err)
		}
		if !strings.Contains(string(encoded), string(task.StateSucceeded)) {
			mt.Fatalf("completion update %s does not set Succeeded", encoded)
		}
	})
}

func TestIntegrationMongoDB(t *testing.T) {
	uri := testmongodb.RequireURI(t)
	t.Run("worker completes submitted task", func(t *testing.T) {
		manager := newIntegrationManager(t, uri, mongodb.Options{
			MaxWorkers:    2,
			PollInterval:  10 * time.Millisecond,
			LeaseDuration: time.Second,
		})
		handled := make(chan task.TaskInfo, 1)
		if err := manager.Register("example", task.HandlerFunc(func(_ context.Context, info task.TaskInfo) error {
			handled <- info
			return nil
		}), task.HandlerOptions{}); err != nil {
			t.Fatalf("Register() error = %v", err)
		}
		id, err := manager.Submit(t.Context(), task.Task{
			Type:    "example",
			Payload: []byte("payload"),
		}, task.SubmitOptions{})
		if err != nil {
			t.Fatalf("Submit() error = %v", err)
		}

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			done <- manager.Run(ctx)
		}()
		select {
		case info := <-handled:
			if info.ID != id || string(info.Payload) != "payload" || info.Status.Attempt != 1 {
				t.Fatalf("handled task = %#v", info)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("handler was not called")
		}

		info := requireIntegrationTaskState(t, manager, id, task.StateSucceeded)
		if info.Status.StartTime == nil || info.Status.CompletionTime == nil {
			t.Fatalf("completed task timestamps = %#v", info.Status)
		}
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Run() did not stop")
		}
	})
	t.Run("submission idempotency is enforced", func(t *testing.T) {
		manager := newIntegrationManager(t, uri, mongodb.Options{})
		work := task.Task{
			Type:           "example",
			Payload:        []byte("payload"),
			IdempotencyKey: "operation-1",
		}
		firstID, err := manager.Submit(t.Context(), work, task.SubmitOptions{})
		if err != nil {
			t.Fatalf("first Submit() error = %v", err)
		}
		secondID, err := manager.Submit(t.Context(), work, task.SubmitOptions{})
		if err != nil {
			t.Fatalf("second Submit() error = %v", err)
		}
		if secondID != firstID {
			t.Fatalf("second Submit() ID = %q, want %q", secondID, firstID)
		}

		conflict := work
		conflict.Payload = []byte("different")
		if _, err := manager.Submit(t.Context(), conflict, task.SubmitOptions{}); !errors.Is(err, task.ErrConflict) {
			t.Fatalf("conflicting Submit() error = %v, want ErrConflict", err)
		}
	})
	t.Run("failed attempt is retried", func(t *testing.T) {
		manager := newIntegrationManager(t, uri, mongodb.Options{
			PollInterval:  10 * time.Millisecond,
			LeaseDuration: time.Second,
		})
		var attempts atomic.Int32
		if err := manager.Register("example", task.HandlerFunc(func(context.Context, task.TaskInfo) error {
			if attempts.Add(1) == 1 {
				return task.RetryAfter(errors.New("temporary"), 10*time.Millisecond)
			}
			return nil
		}), task.HandlerOptions{MaxAttempts: 2}); err != nil {
			t.Fatalf("Register() error = %v", err)
		}
		id, err := manager.Submit(t.Context(), task.Task{Type: "example"}, task.SubmitOptions{})
		if err != nil {
			t.Fatalf("Submit() error = %v", err)
		}

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			done <- manager.Run(ctx)
		}()
		info := requireIntegrationTaskState(t, manager, id, task.StateSucceeded)
		if info.Status.Attempt != 2 || info.Status.LastError != "" || attempts.Load() != 2 {
			t.Fatalf("retried task = %#v, handler attempts = %d", info, attempts.Load())
		}
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Run() did not stop")
		}
	})
	t.Run("multiple workers share one task", func(t *testing.T) {
		database := testmongodb.RequireDatabase(t, uri)
		options := mongodb.Options{
			PollInterval:  10 * time.Millisecond,
			LeaseDuration: time.Second,
		}
		first, err := mongodb.New(t.Context(), database.Collection("tasks"), options)
		if err != nil {
			t.Fatalf("create first MongoDB task manager: %v", err)
		}
		second, err := mongodb.New(t.Context(), database.Collection("tasks"), options)
		if err != nil {
			t.Fatalf("create second MongoDB task manager: %v", err)
		}
		var calls atomic.Int32
		handler := task.HandlerFunc(func(context.Context, task.TaskInfo) error {
			calls.Add(1)
			return nil
		})
		for _, manager := range []*mongodb.Manager{first, second} {
			if err := manager.Register("example", handler, task.HandlerOptions{}); err != nil {
				t.Fatalf("Register() error = %v", err)
			}
		}
		id, err := first.Submit(t.Context(), task.Task{Type: "example"}, task.SubmitOptions{})
		if err != nil {
			t.Fatalf("Submit() error = %v", err)
		}

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 2)
		go func() {
			done <- first.Run(ctx)
		}()
		go func() {
			done <- second.Run(ctx)
		}()
		info := requireIntegrationTaskState(t, first, id, task.StateSucceeded)
		if info.Status.Attempt != 1 || calls.Load() != 1 {
			t.Fatalf("shared task = %#v, handler calls = %d", info, calls.Load())
		}
		cancel()
		for range 2 {
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("Run() error = %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Run() did not stop")
			}
		}
	})
	t.Run("list selects literal label keys", func(t *testing.T) {
		manager := newIntegrationManager(t, uri, mongodb.Options{})
		matchingID, err := manager.Submit(t.Context(), task.Task{
			Type:   "example",
			Labels: map[string]string{"example.com/team": "iam"},
		}, task.SubmitOptions{})
		if err != nil {
			t.Fatalf("submit matching task: %v", err)
		}
		if _, err := manager.Submit(t.Context(), task.Task{
			Type:   "example",
			Labels: map[string]string{"example.com/team": "finance"},
		}, task.SubmitOptions{}); err != nil {
			t.Fatalf("submit non-matching task: %v", err)
		}

		page, err := manager.List(t.Context(), meta.ListOptions{
			LabelSelector: "example.com/team=iam",
		})
		if err != nil {
			t.Fatalf("List() error = %v", err)
		}
		if page.Total == nil || *page.Total != 1 || len(page.Items) != 1 || page.Items[0].ID != matchingID {
			t.Fatalf("List() = %#v, want task %q", page, matchingID)
		}
	})
}

func newIntegrationManager(t *testing.T, uri string, options mongodb.Options) *mongodb.Manager {
	t.Helper()
	database := testmongodb.RequireDatabase(t, uri)
	manager, err := mongodb.New(t.Context(), database.Collection("tasks"), options)
	if err != nil {
		t.Fatalf("create MongoDB task manager: %v", err)
	}
	return manager
}

func requireIntegrationTaskState(
	t *testing.T,
	manager task.Manager,
	id string,
	want task.State,
) task.TaskInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for {
		info, err := manager.Get(ctx, id)
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if info.Status.State == want {
			return info
		}
		select {
		case <-ctx.Done():
			t.Fatalf("task state = %s, want %s", info.Status.State, want)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
