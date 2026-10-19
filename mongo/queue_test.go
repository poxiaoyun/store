package mongo_test

import (
	"context"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/event"
	drivermongo "go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"sync"
	"testing"
	"time"

	testmongodb "xiaoshiai.cn/common/testkit/mongodb"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/mongo"
	"xiaoshiai.cn/store/storetest"
)

func TestQueueConformance(t *testing.T) {
	uri := testmongodb.RequireURI(t)
	storetest.RunQueue(t, func(t *testing.T) store.Queue {
		database := testmongodb.RequireDatabase(t, uri)
		return mongo.NewQueue(database.Collection("queue"), &mongo.QueueOptions{CheckInterval: 10 * time.Millisecond})
	})
}

func TestQueueWakesIdleConsumer(t *testing.T) {
	uri := testmongodb.RequireURI(t)
	idle := make(chan struct{})
	var once sync.Once
	monitor := &event.CommandMonitor{Succeeded: func(_ context.Context, event *event.CommandSucceededEvent) {
		if event.CommandName != "findAndModify" {
			return
		}
		var reply struct {
			Value *bson.M `bson:"value"`
		}
		if err := bson.Unmarshal(event.Reply, &reply); err != nil {
			return
		}
		if reply.Value == nil {
			once.Do(func() { close(idle) })
		}
	}}
	database := testmongodb.RequireDatabase(t, uri, options.Client().
		SetMonitor(monitor))
	q := mongo.NewQueue(database.Collection("queue"), &mongo.QueueOptions{CheckInterval: time.Hour})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := q.Enqueue(ctx, "later", []byte("old"), store.EnqueueOptions{After: time.Hour}); err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		result <- q.Consume(ctx, func(_ context.Context, id string, payload []byte) error {
			if id != "later" || string(payload) != "new" {
				return fmt.Errorf("received %q: %q", id, payload)
			}
			cancel()
			return nil
		}, store.ConsumeOptions{})
	}()
	// Observe an empty claim before enqueueing, so delivery must come from the
	// change notification rather than startup draining or the one-hour poll.
	select {
	case <-idle:
	case <-ctx.Done():
		t.Fatal("consumer did not become idle")
	}
	if err := q.Enqueue(ctx, "later", []byte("new"), store.EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("Consume = %v", err)
	}
}

func TestQueueCoordinatesInstances(t *testing.T) {
	uri := testmongodb.RequireURI(t)
	database := testmongodb.RequireDatabase(t, uri)
	col := database.Collection("queue")
	first := mongo.NewQueue(col, &mongo.QueueOptions{CheckInterval: 10 * time.Millisecond})
	second := mongo.NewQueue(col, &mongo.QueueOptions{CheckInterval: 10 * time.Millisecond})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for i := range 32 {
		if err := first.Enqueue(ctx, fmt.Sprint(i), nil, store.EnqueueOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	var mu sync.Mutex
	seen := map[string]bool{}
	handler := func(_ context.Context, id string, _ []byte) error {
		mu.Lock()
		defer mu.Unlock()
		if seen[id] {
			return fmt.Errorf("concurrent duplicate delivery: %s", id)
		}
		seen[id] = true
		if len(seen) == 32 {
			cancel()
		}
		return nil
	}
	results := make(chan error, 2)
	for _, q := range []*mongo.Queue{first, second} {
		go func() { results <- q.Consume(ctx, handler, store.ConsumeOptions{MaxWorkers: 2}) }()
	}
	for range 2 {
		if err := <-results; !errors.Is(err, context.Canceled) {
			t.Fatalf("Consume = %v", err)
		}
	}
	if len(seen) != 32 {
		t.Fatalf("received %d items", len(seen))
	}
}

func TestQueueCancellationDuringClaim(t *testing.T) {
	uri := testmongodb.RequireURI(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	consumeCtx, stop := context.WithCancel(ctx)
	defer stop()
	var once sync.Once
	monitor := &event.CommandMonitor{Started: func(_ context.Context, event *event.CommandStartedEvent) {
		if event.CommandName == "findAndModify" {
			once.Do(stop)
		}
	}}
	database := testmongodb.RequireDatabase(t, uri, options.Client().
		SetMonitor(monitor))
	q := mongo.NewQueue(database.Collection("queue"), &mongo.QueueOptions{CheckInterval: 10 * time.Millisecond})
	if err := q.Enqueue(ctx, "item", nil, store.EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}
	err := q.Consume(consumeCtx, func(ctx context.Context, _ string, _ []byte) error { return ctx.Err() }, store.ConsumeOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled claim = %v", err)
	}
	err = q.Consume(ctx, func(_ context.Context, id string, _ []byte) error {
		if id != "item" {
			return fmt.Errorf("received %q", id)
		}
		cancel()
		return nil
	}, store.ConsumeOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("consume released item = %v", err)
	}
}

func TestQueueReportsAcknowledgementFailureDuringShutdown(t *testing.T) {
	uri := testmongodb.RequireURI(t)
	database := testmongodb.RequireDatabase(t, uri)
	// Make acknowledgement fail at the database, after the handler has canceled
	// consumption. The returned error must retain the database failure.
	if err := database.CreateCollection(t.Context(), "queue", options.CreateCollection().
		SetValidator(
			bson.M{"state": bson.M{"$ne": "Done"}},
		)); err != nil {
		t.Fatal(err)
	}
	q := mongo.NewQueue(database.Collection("queue"), &mongo.QueueOptions{})
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := q.Enqueue(ctx, "item", nil, store.EnqueueOptions{}); err != nil {
		t.Fatal(err)
	}
	err := q.Consume(ctx, func(context.Context, string, []byte) error { cancel(); return nil }, store.ConsumeOptions{})
	var writeError drivermongo.WriteException
	if !errors.As(err, &writeError) {
		t.Fatalf("acknowledgement failure was hidden: %v", err)
	}
}
