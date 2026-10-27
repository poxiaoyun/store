package mongo

import (
	"context"
	"reflect"
	"slices"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsoncodec"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readconcern"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/log"
	"xiaoshiai.cn/store"
)

func NewObject[T any](t reflect.Type) T {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		return reflect.New(t).
			Interface().(T)
	}
	return reflect.New(t).
		Elem().
		Interface().(T)
}

// Watch implements Storage.
func (m *MongoStorage) Watch(ctx context.Context, obj store.ObjectList, opts ...store.WatchOption) (store.Watcher, error) {
	options := store.ApplyWatchOptions(opts)
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return nil, err
	}
	resource, err := m.core.schema.ResourceName(obj)
	if err != nil {
		return nil, err
	}
	if options.ResourceVersion != nil && *options.ResourceVersion > 0 {
		return nil, errors.NewResourceExpired(resource, "watch history is unavailable")
	}
	_, newObjFunc, err := store.NewItemFuncFromList(obj)
	if err != nil {
		return nil, err
	}
	var watcher store.Watcher
	err = m.on(ctx, obj, options.IncludeSubScopes, func(ctx context.Context, col *mongo.Collection, filter bson.D) error {
		if options.ID != "" {
			filter = append(filter, bson.E{Key: "id", Value: options.ID})
		}
		findFilter := ConditionsMatch(filter, options.LabelRequirements, options.FieldRequirements, "")
		watchFilter := ToWatchFilter(filter)
		newwatcher, err := NewMongoWatcher(
			ctx,
			col,
			m.core.bsonOptions,
			m.core.bsonRegistry,
			newObjFunc,
			m.scopes,
			options,
			findFilter,
			watchFilter,
		)
		if err != nil {
			return err
		}
		watcher = newwatcher
		return nil
	})
	return watcher, err
}

// ToWatchFilter converts an object filter to a change stream event filter.
func ToWatchFilter(filter bson.D) bson.D {
	ret := bson.D{
		bson.E{Key: "operationType", Value: bson.M{"$in": bson.A{"insert", "update", "replace", "delete"}}},
	}
	current := bson.D{{Key: "fullDocument", Value: bson.D{{Key: "$exists", Value: true}}}}
	previous := bson.D{{Key: "fullDocumentBeforeChange", Value: bson.D{{Key: "$exists", Value: true}}}}
	for _, f := range filter {
		current = append(current, bson.E{Key: "fullDocument." + f.Key, Value: f.Value})
		previous = append(previous, bson.E{Key: "fullDocumentBeforeChange." + f.Key, Value: f.Value})
	}
	if len(filter) > 0 {
		ret = append(ret, bson.E{Key: "$or", Value: bson.A{current, previous}})
	}
	return ret
}

var _ store.Watcher = &MongoWatcher{}

func NewMongoWatcher(ctx context.Context,
	col *mongo.Collection,
	bsonOptions *mongooptions.BSONOptions,
	bsonRegistry *bsoncodec.Registry,
	newobj func() store.Object,
	scopes []store.Scope,
	opts store.WatchOptions,
	findFilter bson.D,
	watchFilter bson.D,
) (*MongoWatcher, error) {
	watchCtx, cancel := context.WithCancel(ctx)
	stream, err := col.Watch(watchCtx,
		mongo.Pipeline{
			bson.D{{Key: "$match", Value: watchFilter}},
		},
		// check support full document on delete
		// https://www.mongodb.com/docs/manual/reference/change-events/delete/#document-pre--and-post-images
		mongooptions.ChangeStream().
			SetBatchSize(0).
			SetFullDocument(mongooptions.Required).
			SetFullDocumentBeforeChange(mongooptions.Required),
	)
	if err != nil {
		cancel()
		return nil, errors.NewInternalError(err)
	}
	var cur *mongo.Cursor
	if opts.SendInitialEvents {
		// batchSize=0 makes the aggregate firstBatch empty. Consume it so the
		// first TryNext after Find is guaranteed to issue getMore.
		stream.TryNext(watchCtx)
		if err := stream.Err(); err != nil {
			_ = stream.Close(watchCtx)
			cancel()
			return nil, errors.NewInternalError(err)
		}
		log.FromContext(watchCtx).
			Info("send initial events", "filter", findFilter)
		// Collection.Clone does not preserve BSONOptions in the current driver.
		snapshot := col.Database().
			Collection(
				col.Name(),
				mongooptions.Collection().
					SetReadConcern(readconcern.Snapshot()).
					SetBSONOptions(bsonOptions).
					SetRegistry(bsonRegistry),
			)
		cur, err = snapshot.Find(watchCtx, findFilter)
		if err != nil {
			_ = stream.Close(watchCtx)
			cancel()
			return nil, errors.NewInternalError(err)
		}
	}
	w := &MongoWatcher{
		col:              col,
		bsonRegistry:     bsonRegistry,
		bsonOptions:      bsonOptions,
		newObjectFunc:    newobj,
		scopes:           scopes,
		includeSubScopes: opts.IncludeSubScopes,
		id:               opts.ID,
		labelSelector:    opts.LabelRequirements,
		fieldSelector:    opts.FieldRequirements,
		results:          make(chan store.WatchEvent, 64),
		cancel:           cancel,
	}
	go w.run(watchCtx, cur, stream)
	return w, nil
}

type MongoWatcher struct {
	col              *mongo.Collection
	bsonRegistry     *bsoncodec.Registry
	bsonOptions      *mongooptions.BSONOptions
	newObjectFunc    func() store.Object
	scopes           []store.Scope
	includeSubScopes bool
	id               string
	labelSelector    store.Requirements
	fieldSelector    store.Requirements
	results          chan store.WatchEvent
	cancel           context.CancelFunc
	stop             sync.Once
}

// Event implements Watcher.
func (w *MongoWatcher) Events() <-chan store.WatchEvent {
	return w.results
}

func (w *MongoWatcher) runlist(ctx context.Context, cur *mongo.Cursor) error {
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		item := w.newObjectFunc()
		if err := cur.Decode(item); err != nil {
			return errors.NewInternalError(err)
		}
		item.SetResource(w.col.Name())
		if !w.send(ctx, store.WatchEvent{Type: store.WatchEventCreate, Object: item}) {
			return ctx.Err()
		}
	}
	if err := cur.Err(); err != nil {
		return errors.NewInternalError(err)
	}
	return nil
}

func (w *MongoWatcher) run(ctx context.Context, cur *mongo.Cursor, stream *mongo.ChangeStream) {
	defer close(w.results)
	defer w.Stop()
	if err := w.consume(ctx, cur, stream); err != nil {
		if ctx.Err() == context.Canceled {
			return
		}
		w.send(ctx, store.WatchEvent{Error: err})
	}
}

func (w *MongoWatcher) consume(ctx context.Context, cur *mongo.Cursor, stream *mongo.ChangeStream) error {
	defer stream.Close(ctx)
	if cur != nil {
		if err := w.runlist(ctx, cur); err != nil {
			return err
		}
		// The initial batch was consumed before Find, so this fetch is one
		// post-Find getMore. Drain its complete batch before publishing Bookmark.
		if err := w.sendNextBatch(ctx, stream); err != nil {
			return err
		}
		if !w.send(ctx, store.WatchEvent{Type: store.WatchEventBookmark}) {
			return ctx.Err()
		}
	}
	for stream.Next(ctx) {
		if err := w.sendCurrentChange(ctx, stream); err != nil {
			return err
		}
	}
	if err := stream.Err(); err != nil {
		return errors.NewInternalError(err)
	}
	return ctx.Err()
}

func (w *MongoWatcher) sendNextBatch(ctx context.Context, stream *mongo.ChangeStream) error {
	if !stream.TryNext(ctx) {
		if err := stream.Err(); err != nil {
			return errors.NewInternalError(err)
		}
		return nil
	}
	for {
		if err := w.sendCurrentChange(ctx, stream); err != nil {
			return err
		}
		if stream.RemainingBatchLength() == 0 {
			return nil
		}
		if !stream.TryNext(ctx) {
			if err := stream.Err(); err != nil {
				return errors.NewInternalError(err)
			}
			return ctx.Err()
		}
	}
}

type rawMongoEvent struct {
	FullDocument             bson.Raw `json:"fullDocument"`
	FullDocumentBeforeChange bson.Raw `json:"fullDocumentBeforeChange"`
}

func (w *MongoWatcher) sendCurrentChange(ctx context.Context, stream *mongo.ChangeStream) error {
	raw := rawMongoEvent{}
	if err := stream.Decode(&raw); err != nil {
		return errors.NewInternalError(err)
	}
	event, err := w.watchEvent(raw)
	if err != nil {
		return errors.NewInternalError(err)
	}
	if event.Type != "" && !w.send(ctx, event) {
		return ctx.Err()
	}
	return nil
}

func (w *MongoWatcher) watchEvent(raw rawMongoEvent) (store.WatchEvent, error) {
	var old, new store.Object
	if len(raw.FullDocumentBeforeChange) > 0 {
		old = w.newObjectFunc()
		if err := bsonUnmarshal(w.bsonRegistry, raw.FullDocumentBeforeChange, old); err != nil {
			return store.WatchEvent{}, err
		}
		old.SetResource(w.col.Name())
	}
	if len(raw.FullDocument) > 0 {
		new = w.newObjectFunc()
		if err := bsonUnmarshal(w.bsonRegistry, raw.FullDocument, new); err != nil {
			return store.WatchEvent{}, err
		}
		new.SetResource(w.col.Name())
	}
	oldMatches, err := w.matches(old)
	if err != nil {
		return store.WatchEvent{}, err
	}
	newMatches, err := w.matches(new)
	if err != nil {
		return store.WatchEvent{}, err
	}
	switch {
	case !oldMatches && newMatches:
		return store.WatchEvent{Type: store.WatchEventCreate, Object: new}, nil
	case oldMatches && newMatches:
		return store.WatchEvent{Type: store.WatchEventUpdate, Object: new}, nil
	case oldMatches && !newMatches:
		return store.WatchEvent{Type: store.WatchEventDelete, Object: old}, nil
	default:
		return store.WatchEvent{}, nil
	}
}

func (w *MongoWatcher) matches(obj store.Object) (bool, error) {
	if obj == nil || !store.MatchLabelReqirements(obj, w.labelSelector) {
		return false, nil
	}
	if w.id != "" && obj.GetID() != w.id {
		return false, nil
	}
	scopes := obj.GetScopes()
	if w.includeSubScopes {
		if len(scopes) < len(w.scopes) || !slices.Equal(scopes[:len(w.scopes)], w.scopes) {
			return false, nil
		}
	} else if !slices.Equal(scopes, w.scopes) {
		return false, nil
	}
	uns, err := store.ToUnstructured(obj)
	if err != nil {
		return false, err
	}
	return store.MatchUnstructuredFieldRequirments(uns, w.fieldSelector), nil
}

func bsonUnmarshal(bsoncodec *bsoncodec.Registry, data []byte, obj store.Object) error {
	dec, err := bson.NewDecoder(bsonrw.NewBSONDocumentReader(data))
	if err != nil {
		return err
	}
	dec.SetRegistry(bsoncodec)
	dec.ZeroStructs()
	dec.UseJSONStructTags()
	return dec.Decode(obj)
}

func (w *MongoWatcher) send(ctx context.Context, e store.WatchEvent) bool {
	select {
	case w.results <- e:
		return true
	case <-ctx.Done():
		return false
	}
}

func (w *MongoWatcher) Stop() {
	w.stop.Do(func() {
		w.cancel()
	})
}
