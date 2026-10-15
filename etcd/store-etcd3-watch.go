package etcd

import (
	"bytes"
	"context"
	stderrors "errors"
	"fmt"
	"slices"

	etcdrpc "go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"golang.org/x/sync/errgroup"
	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/log"
	"xiaoshiai.cn/common/store"
)

const (
	incomingEventChanSize = 100
	outgoingEventChanSize = 100
)

// Watch implements Store.
func (e *EtcdStore) Watch(ctx context.Context, obj store.ObjectList, opts ...store.WatchOption) (store.Watcher, error) {
	resource, err := store.GetResource(obj)
	if err != nil {
		return nil, err
	}
	options := store.ApplyWatchOptions(opts)
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return nil, err
	}

	if err := e.core.validateObjectList(obj); err != nil {
		return nil, err
	}
	_, newItemFunc, err := store.NewItemFuncFromList(obj)
	if err != nil {
		return nil, err
	}
	key := e.core.getlistkey(e.scopes, resource)
	recursive := true
	if options.ID != "" {
		key = e.core.getkey(e.scopes, resource, options.ID)
		recursive = false
	}
	initialRev := int64(0)
	if options.ResourceVersion != nil {
		initialRev = *options.ResourceVersion
	}
	watchCtx := clientv3.WithRequireLeader(ctx)
	if initialRev > 0 || !options.SendInitialEvents {
		getOptions := []clientv3.OpOption{clientv3.WithLimit(1)}
		if initialRev > 0 {
			getOptions = append(getOptions, clientv3.WithRev(initialRev))
		}
		response, err := e.core.client.KV.Get(watchCtx, key, getOptions...)
		if err != nil {
			return nil, InterpretEtcdReadError(resource, err)
		}
		if initialRev == 0 {
			initialRev = response.Header.Revision
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	w := &etcdWatcher{
		core:              e.core,
		labelSelector:     options.LabelRequirements,
		fieldSelector:     options.FieldRequirements,
		newItemFunc:       newItemFunc,
		resource:          resource,
		key:               key,
		recursive:         recursive,
		scopes:            slices.Clone(e.scopes),
		includesubscopes:  options.IncludeSubScopes,
		sendInitialEvents: options.SendInitialEvents,
		initialRev:        initialRev,
		cancel:            cancel,
		resultChan:        make(chan store.WatchEvent, outgoingEventChanSize),
		incomingEventChan: make(chan *etcdEvent, incomingEventChanSize),
	}
	go w.run(clientv3.WithRequireLeader(ctx))
	return w, nil
}

type etcdWatcher struct {
	core          *etcdStoreCore
	labelSelector store.Requirements
	fieldSelector store.Requirements
	newItemFunc   func() store.Object
	cancel        func()

	resource          string
	key               string
	recursive         bool
	scopes            []store.Scope
	includesubscopes  bool
	sendInitialEvents bool
	initialRev        int64
	resultChan        chan store.WatchEvent
	incomingEventChan chan *etcdEvent
}

type etcdEvent struct {
	oldValue        []byte
	newValue        []byte
	oldRevision     int64
	newRevision     int64
	checkpoint      int64
	isBookmark      bool
	bookmarkVersion int64
}

func (w *etcdWatcher) Stop() {
	w.cancel()
}

func (w *etcdWatcher) Events() <-chan store.WatchEvent {
	return w.resultChan
}

func (w *etcdWatcher) sendError(ctx context.Context, err error) {
	if IsCancelError(err) {
		return
	}
	eventError := err
	if errors.ReasonForError(err) == errors.StatusReasonUnknown {
		eventError = errors.NewInternalError(err)
	}
	select {
	case w.resultChan <- store.WatchEvent{Error: eventError}:
	case <-ctx.Done():
	}
}

func (w *etcdWatcher) sendEvent(ctx context.Context, e *etcdEvent) {
	if len(w.incomingEventChan) == cap(w.incomingEventChan) {
		log.V(3).Info("Fast watcher, slow processing. Probably caused by slow decoding, user not receiving fast, or other processing logic",
			"groupResource", w.resource)
	}
	select {
	case w.incomingEventChan <- e:
	case <-ctx.Done():
	}
}

func (w *etcdWatcher) run(ctx context.Context) {
	defer close(w.resultChan)
	eg, egctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return w.processEvent(egctx)
	})
	eg.Go(func() error {
		return w.listwatch(egctx)
	})
	if err := eg.Wait(); err != nil {
		// egctx is already Done at this point
		// ctx is ok to use here to send error
		// if ctx is canceled, the error is not sent
		w.sendError(ctx, err)
	}
	w.cancel()
}

func (w *etcdWatcher) listwatch(ctx context.Context) error {
	if w.sendInitialEvents {
		if err := w.list(ctx); err != nil {
			return err
		}
		w.sendEvent(ctx, &etcdEvent{isBookmark: true, bookmarkVersion: w.initialRev})
	}

	opts := []clientv3.OpOption{
		clientv3.WithRev(w.initialRev + 1),
		clientv3.WithPrevKV(),
	}
	if w.recursive {
		opts = append(opts, clientv3.WithPrefix())
	}
	watchCh := w.core.client.Watch(ctx, w.key, opts...)
	for wres := range watchCh {
		if err := wres.Err(); err != nil {
			return InterpretEtcdReadError(w.resource, err)
		}
		for _, ev := range wres.Events {
			e := &etcdEvent{
				newValue:    ev.Kv.Value,
				newRevision: ev.Kv.ModRevision,
				checkpoint:  ev.Kv.ModRevision,
			}
			if ev.PrevKv != nil {
				e.oldValue = ev.PrevKv.Value
				e.oldRevision = ev.PrevKv.ModRevision
			}
			if ev.Type == clientv3.EventTypeDelete {
				e.newValue = nil
				e.newRevision = 0
			}
			w.sendEvent(ctx, e)
		}
	}
	if ctx.Err() != nil {
		return nil
	}
	return fmt.Errorf("etcd watch channel closed")
}

func (w *etcdWatcher) list(ctx context.Context) error {
	opts := []clientv3.OpOption{clientv3.WithLimit(maxLimit)}
	if w.recursive {
		opts = append(opts, clientv3.WithRange(clientv3.GetPrefixRangeEnd(w.key)))
	}
	if w.initialRev != 0 {
		opts = append(opts, clientv3.WithRev(w.initialRev))
	}

	var err error
	var lastKey []byte
	var getResp *clientv3.GetResponse

	continuekey := w.key
	for {
		getResp, err = w.core.client.KV.Get(ctx, continuekey, opts...)
		if err != nil {
			return InterpretEtcdReadError(w.resource, err)
		}
		if len(getResp.Kvs) == 0 && getResp.More {
			return errors.NewInternalError(fmt.Errorf("no results were found, but etcd indicated there were more values remaining"))
		}
		if w.initialRev == 0 {
			w.initialRev = getResp.Header.Revision
			opts = append(opts, clientv3.WithRev(w.initialRev))
		}
		// send items from the response until no more results
		for i, kv := range getResp.Kvs {
			// has subresources
			if !w.includesubscopes {
				if index := bytes.Index(kv.Key[len(w.key):], []byte("/")); index != -1 {
					continue
				}
			}
			lastKey = kv.Key
			e := &etcdEvent{newValue: kv.Value, newRevision: kv.ModRevision}
			w.sendEvent(ctx, e)
			// free kv early. Long lists can take O(seconds) to decode.
			getResp.Kvs[i] = nil
		}
		// no more results remain
		if !getResp.More {
			return nil
		}
		if !w.recursive {
			return nil
		}
		continuekey = string(lastKey) + "\x00"
	}
}

// InterpretEtcdReadError maps etcd revision failures to the Store error model.
func InterpretEtcdReadError(resource string, err error) error {
	switch {
	case stderrors.Is(err, etcdrpc.ErrCompacted):
		return errors.NewResourceExpired(resource, "watch version is compacted")
	case stderrors.Is(err, etcdrpc.ErrFutureRev):
		return errors.NewResourceExpired(resource, "watch version is in the future")
	}
	return errors.NewInternalError(err)
}

func (w *etcdWatcher) processEvent(ctx context.Context) error {
	for {
		select {
		case e := <-w.incomingEventChan:
			res, err := w.parseEvent(e)
			if err != nil {
				return err
			}
			if res == nil {
				continue
			}
			if len(w.resultChan) == outgoingEventChanSize {
				log.V(3).Info("Fast watcher, slow processing. Probably caused by slow dispatching events to watchers",
					"resource", w.resource)
			}
			// If user couldn't receive results fast enough, we also block incoming events from watcher.
			// Because storing events in local will cause more memory usage.
			// The worst case would be closing the fast watcher.
			select {
			case w.resultChan <- *res:
			case <-ctx.Done():
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func (w *etcdWatcher) parseEvent(e *etcdEvent) (*store.WatchEvent, error) {
	if e.isBookmark {
		return &store.WatchEvent{Type: store.WatchEventBookmark, ResourceVersion: e.bookmarkVersion}, nil
	}
	old, err := w.decodeObject(e.oldValue, e.oldRevision)
	if err != nil {
		return nil, err
	}
	new, err := w.decodeObject(e.newValue, e.newRevision)
	if err != nil {
		return nil, err
	}
	oldMatches, err := w.matches(old)
	if err != nil {
		return nil, err
	}
	newMatches, err := w.matches(new)
	if err != nil {
		return nil, err
	}
	switch {
	case !oldMatches && newMatches:
		return &store.WatchEvent{Type: store.WatchEventCreate, Object: new, ResourceVersion: e.checkpoint}, nil
	case oldMatches && newMatches:
		return &store.WatchEvent{Type: store.WatchEventUpdate, Object: new, ResourceVersion: e.checkpoint}, nil
	case oldMatches && !newMatches:
		return &store.WatchEvent{Type: store.WatchEventDelete, Object: old, ResourceVersion: e.checkpoint}, nil
	default:
		return nil, nil
	}
}

func (w *etcdWatcher) decodeObject(value []byte, revision int64) (store.Object, error) {
	if len(value) == 0 {
		return nil, nil
	}
	object := w.newItemFunc()
	if err := w.core.serializer.Decode(value, object); err != nil {
		return nil, err
	}
	object.SetResourceVersion(revision)
	return object, nil
}

func (w *etcdWatcher) matches(obj store.Object) (bool, error) {
	if obj == nil || !store.MatchLabelReqirements(obj, w.labelSelector) {
		return false, nil
	}
	if w.includesubscopes {
		if !store.ScopesIsSameOrUnder(obj.GetScopes(), w.scopes) {
			return false, nil
		}
	} else if !store.ScopesEquals(obj.GetScopes(), w.scopes) {
		return false, nil
	}
	return matchFieldRequirements(obj, w.fieldSelector)
}

func matchFieldRequirements(obj store.Object, requirements store.Requirements) (bool, error) {
	if len(requirements) == 0 {
		return true, nil
	}
	uns, err := store.ToUnstructured(obj)
	if err != nil {
		return false, err
	}
	return store.MatchUnstructuredFieldRequirments(uns, requirements), nil
}

type etcdError interface {
	Code() grpccodes.Code
	Error() string
}

type grpcError interface {
	GRPCStatus() *grpcstatus.Status
}

func IsCancelError(err error) bool {
	if err == nil {
		return false
	}
	if err == context.Canceled {
		return true
	}
	if etcdErr, ok := err.(etcdError); ok && etcdErr.Code() == grpccodes.Canceled {
		return true
	}
	if grpcErr, ok := err.(grpcError); ok && grpcErr.GRPCStatus().Code() == grpccodes.Canceled {
		return true
	}
	return false
}
