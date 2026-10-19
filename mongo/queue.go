package mongo

import (
	"context"
	stderrors "errors"
	"time"

	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/sync/errgroup"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
)

// QueueOptions configures MongoDB queue consumption.
type QueueOptions struct {
	// CheckInterval bounds idle polling for delayed work and missed notifications.
	// Zero defaults to one minute; negative values are invalid.
	CheckInterval time.Duration
}

// NewQueue constructs a queue backed by col. The caller owns the connection.
func NewQueue(col *mongo.Collection, options *QueueOptions) *Queue {
	interval := options.CheckInterval
	if interval == 0 {
		interval = time.Minute
	}
	return &Queue{col: col, checkInterval: interval}
}

var _ store.Queue = (*Queue)(nil)

// Queue persists work and completed IDs in a MongoDB collection. Construct it
// with NewQueue. Consume requires change stream support (replica set or sharded
// cluster). Processing messages are not automatically recovered after a crash.
type Queue struct {
	col           *mongo.Collection
	checkInterval time.Duration
}

type queueItem struct {
	ID          string    `bson:"_id"`
	Payload     []byte    `bson:"payload"`
	State       string    `bson:"state"`
	AvailableAt time.Time `bson:"availableAt"`
}

const (
	queuePending    = "Pending"
	queueProcessing = "Processing"
	queueDone       = "Done"
)

// Enqueue implements store.Queue.
func (q *Queue) Enqueue(ctx context.Context, id string, data []byte, opt store.EnqueueOptions) error {
	if id == "" {
		id = uuid.NewString()
	}
	availableAt := time.Now()
	if opt.After > 0 {
		// BSON dates have millisecond precision. Round delays up to avoid early delivery.
		availableAt = availableAt.Add(opt.After).
			Add(time.Millisecond - 1).
			Truncate(time.Millisecond)
	}
	_, err := q.col.InsertOne(ctx, queueItem{
		ID:          id,
		Payload:     data,
		State:       queuePending,
		AvailableAt: availableAt,
	})
	if err != nil {
		if !mongo.IsDuplicateKeyError(err) {
			return ConvetMongoError(err, q.col, id)
		}
		// The state predicate serializes replacement against concurrent claiming.
		_, err = q.col.UpdateOne(ctx, bson.M{"_id": id, "state": queuePending}, bson.M{
			"$set": bson.M{"payload": data, "availableAt": availableAt},
		})
		if err != nil {
			return ConvetMongoError(err, q.col, id)
		}
	}
	return nil
}

// Consume implements store.Queue.
func (q *Queue) Consume(ctx context.Context, handler store.QueueHandleFunc, opt store.ConsumeOptions) error {
	if opt.MaxWorkers < 0 {
		return errors.NewBadRequest("MaxWorkers must not be negative")
	}
	if q.checkInterval < 0 {
		return errors.NewBadRequest("CheckInterval must not be negative")
	}
	_, err := q.col.Indexes().
		CreateOne(ctx, mongo.IndexModel{
			Keys: bson.D{{Key: "state", Value: 1}, {Key: "availableAt", Value: 1}, {Key: "_id", Value: 1}},
		})
	if err != nil {
		return err
	}
	stream, err := q.col.Watch(ctx, mongo.Pipeline{{{Key: "$match", Value: bson.M{
		"$or": bson.A{
			bson.M{"operationType": "insert"},
			bson.M{"updateDescription.updatedFields.state": queuePending},
			bson.M{"updateDescription.updatedFields.availableAt": bson.M{"$exists": true}},
		},
	}}}})
	if err != nil {
		return err
	}
	defer stream.Close(context.WithoutCancel(ctx))

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	group, ctx := errgroup.WithContext(ctx)
	workers := max(1, opt.MaxWorkers)
	changed := make(chan struct{}, workers)
	failures := make(chan error, workers)
	group.Go(func() error {
		for stream.Next(ctx) {
			// Notifications only wake workers; claiming always queries authoritative state.
			for range workers {
				select {
				case changed <- struct{}{}:
				default:
				}
			}
		}
		if err := context.Cause(ctx); err != nil {
			return err
		}
		if err := stream.Err(); err != nil {
			return err
		}
		return stderrors.New("queue change stream closed")
	})
	for range workers {
		group.Go(func() error {
			err := q.consume(ctx, handler, changed, cancel)
			failures <- err
			cancel(err)
			return err
		})
	}
	_ = group.Wait()
	result := context.Cause(ctx)
	// Cleanup can fail after a handler or cancellation has already stopped the
	// group. Preserve those errors as well as the original stop cause.
	for range workers {
		err := <-failures
		if !stderrors.Is(result, err) {
			result = stderrors.Join(result, err)
		}
	}
	return result
}

func (q *Queue) consume(ctx context.Context, handler store.QueueHandleFunc, changed <-chan struct{}, cancel context.CancelCauseFunc) error {
	timer := time.NewTimer(q.checkInterval)
	defer timer.Stop()
	for {
		if err := context.Cause(ctx); err != nil {
			return err
		}
		var item queueItem
		// Once a claim is sent, finish reading its result even during shutdown;
		// otherwise cancellation could hide a successful claim that needs releasing.
		err := q.col.FindOneAndUpdate(context.WithoutCancel(ctx),
			bson.M{"state": queuePending, "availableAt": bson.M{"$lte": time.Now()}},
			bson.M{"$set": bson.M{"state": queueProcessing}},
			options.FindOneAndUpdate().
				SetSort(bson.D{{Key: "availableAt", Value: 1}, {Key: "_id", Value: 1}}),
		).
			Decode(&item)
		if err != nil {
			if !stderrors.Is(err, mongo.ErrNoDocuments) {
				return err
			}
			timer.Reset(q.checkInterval)
			select {
			case <-ctx.Done():
				return context.Cause(ctx)
			case <-changed:
			case <-timer.C:
			}
			continue
		}
		var handlerErr error
		select {
		case <-ctx.Done():
			handlerErr = context.Cause(ctx)
		default:
			handlerErr = handler(ctx, item.ID, item.Payload)
		}
		update := bson.M{"$set": bson.M{"state": queueDone}, "$unset": bson.M{"payload": ""}}
		if handlerErr != nil {
			// Cancel siblings before releasing failed work, preventing this call from retrying it.
			cancel(handlerErr)
			update = bson.M{"$set": bson.M{"state": queuePending}}
		}
		// Acknowledge or release claimed work even when the consumer context is canceled.
		_, err = q.col.UpdateOne(context.WithoutCancel(ctx), bson.M{"_id": item.ID, "state": queueProcessing}, update)
		if err != nil {
			return stderrors.Join(handlerErr, err)
		}
		if handlerErr != nil {
			return handlerErr
		}
	}
}
