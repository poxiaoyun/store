package inmemory

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
)

var _ store.PingableStore = &InMemory{}

type InMemory struct {
	core   *inmemory
	scopes []store.Scope
	status bool
}

// Schema implements store.Store.
func (i *InMemory) Schema() *store.Schema {
	return i.core.schema.Snapshot()
}

// Capabilities implements store.Store.
func (i *InMemory) Capabilities() store.Capabilities {
	return store.Capabilities{Finalizers: true, OptimisticLock: true, Watch: true}
}

func New(schema *store.Schema) (*InMemory, error) {
	schema, err := schema.Clone()
	if err != nil {
		return nil, err
	}
	return &InMemory{core: &inmemory{
		kvs:      map[string]kv{},
		indexes:  map[string]map[string]map[string]map[string]struct{}{},
		schema:   schema,
		watchers: map[int64]*inmemoryWatcher{},
	}}, nil
}

// Ping implements store.Pinger. An in-memory store has no external backend to
// probe, so it is always available while the process is running.
func (i *InMemory) Ping(context.Context) error {
	return nil
}

// PatchBatch implements store.Store.
func (i *InMemory) PatchBatch(ctx context.Context, obj store.ObjectList, patch store.PatchBatch, opts ...store.PatchBatchOption) error {
	options := store.ApplyPatchBatchOptions(opts)
	if options.DryRun {
		return errors.NewUnsupported("in-memory store does not support dry-run")
	}
	resource, err := i.resourceName(obj)
	if err != nil {
		return err
	}
	single := store.RawPatch(patch.Type(), patch.Data())
	list := &store.List[store.Unstructured]{Resource: resource}
	if err := i.List(
		ctx,
		list,
		store.WithFieldRequirements(options.FieldRequirements...),
		store.WithLabelRequirements(options.LabelRequirements...),
	); err != nil {
		return err
	}
	for index := range list.Items {
		item := &list.Items[index]
		uid := item.GetUID()
		if err := i.Patch(ctx, item, single, store.WithPreconditions(store.Preconditions{UID: &uid}), store.WithLabelRequirements(options.LabelRequirements...), store.WithFieldRequirements(options.FieldRequirements...)); err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			return err
		}
	}
	return nil
}

func (i *InMemory) Count(ctx context.Context, obj store.Object, opts ...store.CountOption) (int, error) {
	options := store.ApplyCountOptions(opts)
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return 0, err
	}
	resource, err := i.resourceName(obj)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, value := range i.core.list(resource, i.scopes, options.IncludeSubScopes) {
		item := store.Unstructured{}
		if err := json.Unmarshal(value.data, &item); err != nil {
			return 0, errors.NewInternalError(err)
		}
		if store.MatchLabelReqirements(&item, options.LabelRequirements) &&
			store.MatchUnstructuredFieldRequirments(&item, options.FieldRequirements) && matchesSearch(&item, options.Search, options.SearchFields) {
			count++
		}
	}
	return count, nil
}

func (i *InMemory) Create(ctx context.Context, obj store.Object, opts ...store.CreateOption) error {
	options := store.ApplyCreateOptions(opts)
	if options.DryRun {
		return errors.NewUnsupported("in-memory store does not support dry-run")
	}
	if options.TTL != 0 {
		return errors.NewUnsupported("in-memory store does not support TTL")
	}
	resource, err := i.resourceName(obj)
	if err != nil {
		return err
	}
	prepareObjectForCreate(obj, resource, i.scopes)
	return i.core.create(resource, i.scopes, obj.GetID(), obj)
}

func (i *InMemory) Delete(ctx context.Context, obj store.Object, opts ...store.DeleteOption) error {
	options := store.ApplyDeleteOptions(opts)
	if options.DryRun {
		return errors.NewUnsupported("in-memory store does not support dry-run")
	}
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	resource, err := i.resourceName(obj)
	if err != nil {
		return err
	}
	_, err = i.core.delete(resource, i.scopes, obj, options)
	return err
}

func (i *InMemory) DeleteBatch(ctx context.Context, obj store.ObjectList, opts ...store.DeleteBatchOption) error {
	options := store.ApplyDeleteBatchOptions(opts)
	if options.DryRun {
		return errors.NewUnsupported("in-memory store does not support dry-run")
	}
	resource, err := i.resourceName(obj)
	if err != nil {
		return err
	}
	list := &store.List[store.Unstructured]{Resource: resource}
	if err := i.List(
		ctx,
		list,
		store.WithFieldRequirements(options.FieldRequirements...),
		store.WithLabelRequirements(options.LabelRequirements...),
	); err != nil {
		return err
	}
	for index := range list.Items {
		item := &list.Items[index]
		if err := i.Delete(ctx, item, store.WithUID(item.GetUID()), store.WithLabelRequirements(options.LabelRequirements...), store.WithFieldRequirements(options.FieldRequirements...)); err != nil {
			if errors.IsNotFound(err) {
				continue
			}
			return err
		}
	}
	return nil
}

func (i *InMemory) Get(ctx context.Context, name string, obj store.Object, opts ...store.GetOption) error {
	options := store.ApplyGetOptions(opts)
	if len(options.Fields) != 0 {
		return errors.NewUnsupported("in-memory store does not support field projection")
	}
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	resource, err := i.resourceName(obj)
	if err != nil {
		return err
	}
	if err := i.core.get(resource, i.scopes, name, obj); err != nil {
		return err
	}
	unstructured, err := store.ToUnstructured(obj)
	if err != nil {
		return err
	}
	if !store.MatchLabelReqirements(obj, options.LabelRequirements) ||
		!store.MatchUnstructuredFieldRequirments(unstructured, options.FieldRequirements) {
		return errors.NewNotFound(resource, name)
	}
	return store.ValidatePreconditions(obj, options.Preconditions)
}

func (i *InMemory) List(ctx context.Context, list store.ObjectList, opts ...store.ListOption) error {
	resource, err := i.resourceName(list)
	if err != nil {
		return err
	}
	options := store.ApplyListOptions(opts)
	if len(options.Fields) != 0 {
		return errors.NewUnsupported("in-memory store does not support field projection")
	}
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}

	page, err := store.PrepareListPage(resource, i.scopes, options)
	if err != nil {
		return err
	}
	options = page.Options
	items, newItem, err := store.NewItemFuncFromList(list)
	if err != nil {
		return err
	}
	values := i.core.list(resource, i.scopes, options.IncludeSubScopes)
	results := make([]listedObject, 0, len(values))
	for _, value := range values {
		item := newItem()
		if err := json.Unmarshal(value.data, item); err != nil {
			return errors.NewInternalError(err)
		}
		item.SetResourceVersion(int64(value.rev))
		unstructured, err := store.ToUnstructured(item)
		if err != nil {
			return err
		}
		if !store.MatchLabelReqirements(item, options.LabelRequirements) ||
			!store.MatchUnstructuredFieldRequirments(unstructured, options.FieldRequirements) ||
			!matchesSearch(unstructured, options.Search, options.SearchFields) {
			continue
		}
		results = append(results, listedObject{
			object:       item,
			unstructured: unstructured,
		})
	}
	sorts := store.ParseSorts(options.Sort)
	slices.SortStableFunc(results, func(a, b listedObject) int {
		return store.CompareUnstructuredField(a.unstructured, b.unstructured, sorts)
	})
	total := len(results)
	start := min(max(options.Offset, 0), total)
	end := total
	if options.Limit > 0 {
		end = min(start+options.Limit, total)
	}
	results = results[start:end]
	items.Set(reflect.MakeSlice(items.Type(), 0, len(results)))
	for _, result := range results {
		value := reflect.ValueOf(result.object)
		items.Set(reflect.Append(items, value.Elem()))
	}
	list.SetResource(resource)
	list.SetScopes(i.scopes)
	list.SetResourceVersion(int64(i.core.rev.Load()))
	return page.Finish(list)
}

func (i *InMemory) Patch(ctx context.Context, obj store.Object, patch store.Patch, opts ...store.PatchOption) error {
	options := store.ApplyPatchOptions(opts)
	if options.DryRun {
		return errors.NewUnsupported("in-memory store does not support dry-run")
	}
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	resource, err := i.resourceName(obj)
	if err != nil {
		return err
	}
	_, err = i.core.patch(resource, i.scopes, obj, patch, i.status, options)
	return err
}

func (i *InMemory) Update(ctx context.Context, obj store.Object, opts ...store.UpdateOption) error {
	options := store.ApplyUpdateOptions(opts)
	if options.DryRun {
		return errors.NewUnsupported("in-memory store does not support dry-run")
	}
	if options.TTL != 0 {
		return errors.NewUnsupported("in-memory store does not support TTL")
	}
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return err
	}
	resource, err := i.resourceName(obj)
	if err != nil {
		return err
	}
	_, err = i.core.put(resource, i.scopes, obj, i.status, options)
	return err
}

func (i *InMemory) Watch(ctx context.Context, obj store.ObjectList, opts ...store.WatchOption) (store.Watcher, error) {
	resource, err := i.resourceName(obj)
	if err != nil {
		return nil, err
	}
	options := store.ApplyWatchOptions(opts)
	if err := validateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return nil, err
	}
	if options.ResourceVersion != nil && *options.ResourceVersion > 0 {
		return nil, errors.NewResourceExpired(resource, "watch history is unavailable")
	}
	_, newItem, err := store.NewItemFuncFromList(obj)
	if err != nil {
		return nil, err
	}
	watcherCtx, cancel := context.WithCancel(ctx)
	watcher := &inmemoryWatcher{
		id:                i.core.watcherID.Add(1),
		core:              i.core,
		ctx:               watcherCtx,
		cancel:            cancel,
		resource:          resource,
		scopes:            slices.Clone(i.scopes),
		includeSubScopes:  options.IncludeSubScopes,
		objectID:          options.ID,
		labelRequirements: options.LabelRequirements,
		fieldRequirements: options.FieldRequirements,
		newItem:           newItem,
		events:            make(chan store.WatchEvent, 100),
	}
	i.core.addWatcher(watcher, options.SendInitialEvents)
	go func() {
		<-watcherCtx.Done()
		i.core.removeWatcher(watcher)
	}()
	return watcher, nil
}

func validateSelectorRequirements(labelRequirements, fieldRequirements store.Requirements) error {
	if err := store.ValidateSelectorRequirements(labelRequirements, fieldRequirements); err != nil {
		return errors.NewBadRequest(err.Error())
	}
	return nil
}

func (i *InMemory) Scope(scope ...store.Scope) store.Store {
	return &InMemory{core: i.core, scopes: append(i.scopes, scope...), status: i.status}
}

func (i *InMemory) Status() store.StatusStorage {
	return &InMemory{core: i.core, scopes: i.scopes, status: true}
}

type inmemory struct {
	mu         sync.RWMutex
	rev        atomic.Uint64
	kvs        map[string]kv
	indexes    map[string]map[string]map[string]map[string]struct{}
	schema     *store.Schema
	watcherMu  sync.RWMutex
	watcherID  atomic.Int64
	watchers   map[int64]*inmemoryWatcher
	dispatcher *eventDispatcher
}

type change struct {
	revision int64
	old      store.Object
	new      store.Object
}

type eventQueue struct {
	mu     sync.Mutex
	cond   *sync.Cond
	items  []change
	head   int
	closed bool
}

func newEventQueue() *eventQueue {
	queue := &eventQueue{}
	queue.cond = sync.NewCond(&queue.mu)
	return queue
}

func (q *eventQueue) push(item change) {
	q.mu.Lock()
	if !q.closed {
		q.items = append(q.items, item)
		q.cond.Signal()
	}
	q.mu.Unlock()
}

func (q *eventQueue) pop() (change, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.head >= len(q.items) && !q.closed {
		q.cond.Wait()
	}
	if q.head >= len(q.items) {
		return change{}, false
	}
	item := q.items[q.head]
	q.head++
	if q.head == len(q.items) {
		q.items = q.items[:0]
		q.head = 0
	}
	return item, true
}

func (q *eventQueue) stop() {
	q.mu.Lock()
	q.closed = true
	q.items = nil
	q.head = 0
	q.cond.Broadcast()
	q.mu.Unlock()
}

type eventDispatcher struct {
	queue *eventQueue
}

func newEventDispatcher() *eventDispatcher {
	return &eventDispatcher{queue: newEventQueue()}
}

func (d *eventDispatcher) run(core *inmemory) {
	for {
		item, ok := d.queue.pop()
		if !ok {
			return
		}
		core.notify(item)
	}
}

func (d *eventDispatcher) stop() {
	d.queue.stop()
}

func cloneObject(obj store.Object) store.Object {
	if obj == nil {
		return nil
	}
	clone := store.NewObject(obj)
	if err := store.CopyObject(obj, clone); err != nil {
		return nil
	}
	return clone
}

// enqueue must be called while the data write lock is held, so queue order
// matches revision order.
func (m *inmemory) enqueue(item change) {
	m.watcherMu.RLock()
	dispatcher := m.dispatcher
	m.watcherMu.RUnlock()
	if dispatcher != nil {
		dispatcher.queue.push(item)
	}
}

type kv struct {
	value []byte
	rev   uint64
}

type listValue struct {
	data []byte
	rev  uint64
}

type listedObject struct {
	object       store.Object
	unstructured *store.Unstructured
}

type inmemoryWatcher struct {
	id                int64
	core              *inmemory
	ctx               context.Context
	cancel            context.CancelFunc
	resource          string
	scopes            []store.Scope
	includeSubScopes  bool
	objectID          string
	startRevision     int64
	labelRequirements store.Requirements
	fieldRequirements store.Requirements
	newItem           func() store.Object
	events            chan store.WatchEvent
	stop              sync.Once
}

func (m *inmemory) create(resource string, scopes []store.Scope, name string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return errors.NewInternalError(err)
	}

	indexValues, err := m.indexValues(resource, scopes, data)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	key := getkey(resource, scopes, name)
	if _, ok := m.kvs[key]; ok {
		return errors.NewAlreadyExists(resource, name)
	}
	if err := m.checkUnique(resource, key, indexValues); err != nil {
		return err
	}
	revision := m.rev.Add(1)
	m.kvs[key] = kv{
		value: data,
		rev:   revision,
	}
	m.addIndexes(resource, key, indexValues)
	if object, ok := value.(store.Object); ok {
		object.SetResourceVersion(int64(revision))
		m.enqueue(change{revision: int64(revision), new: cloneObject(object)})
	}
	return nil
}

func (m *inmemory) get(resource string, scopes []store.Scope, name string, into store.Object) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	key := getkey(resource, scopes, name)
	kv, ok := m.kvs[key]
	if !ok {
		return errors.NewNotFound(resource, name)
	}
	if into != nil {
		store.ResetObject(into)
		if err := json.Unmarshal(kv.value, into); err != nil {
			return errors.NewInternalError(err)
		}
		into.SetResourceVersion(int64(kv.rev))
	}
	return nil
}

func (m *inmemory) put(resource string, scopes []store.Scope, desired store.Object, status bool, options store.UpdateOptions) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := getkey(resource, scopes, desired.GetID())
	kv, ok := m.kvs[key]
	if !ok {
		return false, errors.NewNotFound(resource, desired.GetID())
	}
	current := store.NewObject(desired)
	if err := json.Unmarshal(kv.value, current); err != nil {
		return false, errors.NewInternalError(err)
	}
	current.SetResourceVersion(int64(kv.rev))
	unstructured, err := store.ToUnstructured(current)
	if err != nil {
		return false, err
	}
	if !store.MatchLabelReqirements(current, options.LabelRequirements) ||
		!store.MatchUnstructuredFieldRequirments(unstructured, options.FieldRequirements) {
		return false, errors.NewConflict(resource, desired.GetID(), fmt.Errorf("write conditions did not match"))
	}
	if desired.GetResourceVersion() != 0 && desired.GetResourceVersion() != int64(kv.rev) {
		return false, errors.NewConflict(resource, desired.GetID(), fmt.Errorf("resourceVersion %d does not match", desired.GetResourceVersion()))
	}
	deleted, err := prepareObjectForUpdate(current, desired, status)
	if err != nil {
		return false, err
	}
	if deleted {
		oldIndexValues, err := m.indexValues(resource, scopes, kv.value)
		if err != nil {
			return false, err
		}
		m.removeIndexes(resource, key, oldIndexValues)
		delete(m.kvs, key)
		revision := m.rev.Add(1)
		m.enqueue(change{revision: int64(revision), old: current, new: nil})
		return true, nil
	}
	data, err := json.Marshal(desired)
	if err != nil {
		return false, errors.NewInternalError(err)
	}
	indexValues, err := m.indexValues(resource, scopes, data)
	if err != nil {
		return false, err
	}
	if err := m.checkUnique(resource, key, indexValues); err != nil {
		return false, err
	}
	oldIndexValues, err := m.indexValues(resource, scopes, kv.value)
	if err != nil {
		return false, err
	}
	m.removeIndexes(resource, key, oldIndexValues)
	revision := m.rev.Add(1)
	kv.rev = revision
	kv.value = data
	m.kvs[key] = kv
	m.addIndexes(resource, key, indexValues)
	desired.SetResourceVersion(int64(revision))
	m.enqueue(change{revision: int64(revision), old: current, new: cloneObject(desired)})
	return false, nil
}

func (m *inmemory) list(resource string, scopes []store.Scope, includeSubScopes bool) []listValue {
	m.mu.RLock()
	defer m.mu.RUnlock()

	prefix := getlistkey(resource, scopes)
	values := []listValue{}
	for key, value := range m.kvs {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		if !includeSubScopes && strings.Contains(strings.TrimPrefix(key, prefix), "/") {
			continue
		}
		values = append(values, listValue{
			data: slices.Clone(value.value),
			rev:  value.rev,
		})
	}
	return values
}

func (m *inmemory) addWatcher(watcher *inmemoryWatcher, sendInitialEvents bool) {
	m.mu.RLock()
	m.watcherMu.Lock()
	m.watchers[watcher.id] = watcher
	if sendInitialEvents {
		prefix := getlistkey(watcher.resource, watcher.scopes)
		initial := []kv{}
		for key, value := range m.kvs {
			if !strings.HasPrefix(key, prefix) {
				continue
			}
			if !watcher.includeSubScopes && strings.Contains(strings.TrimPrefix(key, prefix), "/") {
				continue
			}
			initial = append(initial, value)
		}
		watcher.events = make(chan store.WatchEvent, max(100, len(initial)+1))
		for _, value := range initial {
			watcher.sendValue(store.WatchEventCreate, value)
		}
		watcher.send(store.WatchEvent{Type: store.WatchEventBookmark})
	}
	watcher.startRevision = int64(m.rev.Load())
	if m.dispatcher == nil {
		m.dispatcher = newEventDispatcher()
		go m.dispatcher.run(m)
	}
	m.watcherMu.Unlock()
	m.mu.RUnlock()
}

func (m *inmemory) removeWatcher(watcher *inmemoryWatcher) {
	m.watcherMu.Lock()
	delete(m.watchers, watcher.id)
	close(watcher.events)
	if len(m.watchers) == 0 && m.dispatcher != nil {
		m.dispatcher.stop()
		m.dispatcher = nil
	}
	m.watcherMu.Unlock()
}

func (m *inmemory) notify(item change) {
	m.watcherMu.RLock()
	defer m.watcherMu.RUnlock()
	for _, watcher := range m.watchers {
		watcher.sendChange(item.revision, item.old, item.new)
	}
}

func (w *inmemoryWatcher) sendValue(eventType store.WatchEventType, value kv) {
	object := w.newItem()
	if err := json.Unmarshal(value.value, object); err != nil {
		w.send(store.WatchEvent{Error: errors.NewInternalError(err)})
		return
	}
	object.SetResourceVersion(int64(value.rev))
	matches, err := w.matches(object)
	if err != nil {
		w.sendTerminalError(err)
		return
	}
	if matches {
		w.sendObject(eventType, object)
	}
}

func (w *inmemoryWatcher) matches(source store.Object) (bool, error) {
	if source == nil {
		return false, nil
	}
	if source.GetResource() != w.resource || w.objectID != "" && source.GetID() != w.objectID {
		return false, nil
	}
	if w.includeSubScopes {
		if !store.ScopesIsSameOrUnder(source.GetScopes(), w.scopes) {
			return false, nil
		}
	} else if !store.ScopesEquals(source.GetScopes(), w.scopes) {
		return false, nil
	}
	unstructured, err := store.ToUnstructured(source)
	if err != nil {
		return false, err
	}
	return store.MatchLabelReqirements(source, w.labelRequirements) &&
		store.MatchUnstructuredFieldRequirments(unstructured, w.fieldRequirements), nil
}

func (w *inmemoryWatcher) sendChange(revision int64, old, new store.Object) {
	if revision <= w.startRevision {
		return
	}
	oldMatches, err := w.matches(old)
	if err != nil {
		w.sendTerminalError(err)
		return
	}
	newMatches, err := w.matches(new)
	if err != nil {
		w.sendTerminalError(err)
		return
	}
	switch {
	case !oldMatches && newMatches:
		w.sendObject(store.WatchEventCreate, new)
	case oldMatches && newMatches:
		w.sendObject(store.WatchEventUpdate, new)
	case oldMatches && !newMatches:
		w.sendObject(store.WatchEventDelete, old)
	}
}

func (w *inmemoryWatcher) sendObject(eventType store.WatchEventType, source store.Object) {
	data, err := json.Marshal(source)
	if err != nil {
		w.sendTerminalError(errors.NewInternalError(err))
		return
	}
	object := w.newItem()
	if err := json.Unmarshal(data, object); err != nil {
		w.sendTerminalError(errors.NewInternalError(err))
		return
	}
	object.SetResourceVersion(source.GetResourceVersion())
	w.send(store.WatchEvent{
		Type:   eventType,
		Object: object,
	})
}

func (w *inmemoryWatcher) sendTerminalError(err error) {
	w.send(store.WatchEvent{Error: err})
	w.Stop()
}

func (w *inmemoryWatcher) send(event store.WatchEvent) {
	select {
	case w.events <- event:
	case <-w.ctx.Done():
	default:
		w.Stop()
	}
}

func (w *inmemoryWatcher) Stop() {
	w.stop.Do(func() {
		w.cancel()
	})
}

func (w *inmemoryWatcher) Events() <-chan store.WatchEvent {
	return w.events
}

func (m *inmemory) patch(
	resource string,
	scopes []store.Scope,
	into store.Object,
	patch store.Patch,
	status bool,
	options store.PatchOptions,
) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := getkey(resource, scopes, into.GetID())
	value, ok := m.kvs[key]
	if !ok {
		return false, errors.NewNotFound(resource, into.GetID())
	}
	current := store.NewObject(into)
	if err := json.Unmarshal(value.value, current); err != nil {
		return false, errors.NewInternalError(err)
	}
	current.SetResourceVersion(int64(value.rev))
	unstructured, err := store.ToUnstructured(current)
	if err != nil {
		return false, err
	}
	if !store.MatchLabelReqirements(current, options.LabelRequirements) ||
		!store.MatchUnstructuredFieldRequirments(unstructured, options.FieldRequirements) {
		return false, errors.NewConflict(resource, into.GetID(), fmt.Errorf("write conditions did not match"))
	}
	if err := store.ValidatePreconditions(current, options.Preconditions); err != nil {
		return false, err
	}
	desired := store.NewObject(into)
	if err := store.CopyObject(current, desired); err != nil {
		return false, err
	}
	if err := ApplyPatch(desired, into, patch); err != nil {
		return false, err
	}
	deleted, err := prepareObjectForUpdate(current, desired, status)
	if err != nil {
		return false, err
	}
	if deleted {
		oldIndexValues, err := m.indexValues(resource, scopes, value.value)
		if err != nil {
			return false, err
		}
		m.removeIndexes(resource, key, oldIndexValues)
		delete(m.kvs, key)
		if err := store.CopyObject(desired, into); err != nil {
			return false, err
		}
		revision := m.rev.Add(1)
		m.enqueue(change{revision: int64(revision), old: current, new: nil})
		return true, nil
	}
	data, err := json.Marshal(desired)
	if err != nil {
		return false, errors.NewInternalError(err)
	}
	indexValues, err := m.indexValues(resource, scopes, data)
	if err != nil {
		return false, err
	}
	if err := m.checkUnique(resource, key, indexValues); err != nil {
		return false, err
	}
	oldIndexValues, err := m.indexValues(resource, scopes, value.value)
	if err != nil {
		return false, err
	}
	m.removeIndexes(resource, key, oldIndexValues)
	revision := m.rev.Add(1)
	value.value = data
	value.rev = revision
	m.kvs[key] = value
	m.addIndexes(resource, key, indexValues)
	if err := store.CopyObject(desired, into); err != nil {
		return false, errors.NewInternalError(err)
	}
	into.SetResourceVersion(int64(revision))
	m.enqueue(change{revision: int64(revision), old: current, new: cloneObject(desired)})
	return false, nil
}

func (m *inmemory) delete(resource string, scopes []store.Scope, desired store.Object, options store.DeleteOptions) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := getkey(resource, scopes, desired.GetID())
	kv, ok := m.kvs[key]
	if !ok {
		return false, errors.NewNotFound(resource, desired.GetID())
	}
	current := store.NewObject(desired)
	if err := json.Unmarshal(kv.value, current); err != nil {
		return false, errors.NewInternalError(err)
	}
	current.SetResourceVersion(int64(kv.rev))
	if err := store.ValidatePreconditions(current, options.Preconditions); err != nil {
		return false, err
	}
	if err := store.ValidateDeleteRequirements(current, options.LabelRequirements, options.FieldRequirements); err != nil {
		return false, err
	}
	old := cloneObject(current)
	policy := store.DeletePropagationBackground
	if options.PropagationPolicy != nil {
		policy = *options.PropagationPolicy
	}
	if !prepareObjectForDelete(current, policy) {
		data, err := json.Marshal(current)
		if err != nil {
			return false, errors.NewInternalError(err)
		}
		revision := m.rev.Add(1)
		kv.value = data
		kv.rev = revision
		m.kvs[key] = kv
		if err := store.CopyObject(current, desired); err != nil {
			return false, errors.NewInternalError(err)
		}
		desired.SetResourceVersion(int64(revision))
		m.enqueue(change{revision: int64(revision), old: old, new: cloneObject(desired)})
		return false, nil
	}
	indexValues, err := m.indexValues(resource, scopes, kv.value)
	if err != nil {
		return false, err
	}
	revision := m.rev.Add(1)
	m.removeIndexes(resource, key, indexValues)
	delete(m.kvs, key)
	if err := store.CopyObject(current, desired); err != nil {
		return false, errors.NewInternalError(err)
	}
	m.enqueue(change{revision: int64(revision), old: old, new: nil})
	return true, nil
}

func (m *inmemory) indexValues(resource string, scopes []store.Scope, data []byte) (map[string]string, error) {
	definition, err := m.schema.Resource(resource)
	if err != nil {
		return nil, err
	}
	object := map[string]any{}
	if err := json.Unmarshal(data, &object); err != nil {
		return nil, errors.NewInternalError(err)
	}
	values := make(map[string]string, len(definition.Indexes))
	for _, index := range definition.Indexes {
		parts := make([]any, 0, len(index.Fields))
		skip := false
		for _, field := range index.Fields {
			var value any
			found := false
			if scopeIndex := slices.Index(definition.ScopeKeys, field); scopeIndex >= 0 {
				if scopeIndex < len(scopes) {
					value, found = scopes[scopeIndex].Name, true
				}
			} else {
				value, found = store.GetNestedField(object, strings.Split(field, ".")...)
			}
			if (!found || value == nil) && index.Nullable {
				skip = true
				break
			}
			parts = append(parts, value)
		}
		if skip {
			continue
		}
		encoded, err := json.Marshal(parts)
		if err != nil {
			return nil, errors.NewInternalError(err)
		}
		values[index.Name] = string(encoded)
	}
	return values, nil
}

func (m *inmemory) checkUnique(resource, objectKey string, values map[string]string) error {
	definition, err := m.schema.Resource(resource)
	if err != nil {
		return err
	}
	for _, index := range definition.Indexes {
		if !index.Unique {
			continue
		}
		value, exists := values[index.Name]
		if !exists {
			continue
		}
		for key := range m.indexEntries(resource, index.Name, value) {
			if key != objectKey {
				return errors.NewAlreadyExists(resource, fmt.Sprintf("index %s", index.Name))
			}
		}
	}
	return nil
}

func (m *inmemory) indexEntries(resource, indexName, value string) map[string]struct{} {
	resourceIndexes := m.indexes[resource]
	if resourceIndexes == nil {
		resourceIndexes = map[string]map[string]map[string]struct{}{}
		m.indexes[resource] = resourceIndexes
	}
	index := resourceIndexes[indexName]
	if index == nil {
		index = map[string]map[string]struct{}{}
		resourceIndexes[indexName] = index
	}
	entries := index[value]
	if entries == nil {
		entries = map[string]struct{}{}
		index[value] = entries
	}
	return entries
}

func (m *inmemory) addIndexes(resource, objectKey string, values map[string]string) {
	for indexName, value := range values {
		m.indexEntries(resource, indexName, value)[objectKey] = struct{}{}
	}
}

func (m *inmemory) removeIndexes(resource, objectKey string, values map[string]string) {
	for indexName, value := range values {
		entries := m.indexEntries(resource, indexName, value)
		delete(entries, objectKey)
		if len(entries) == 0 {
			delete(m.indexes[resource][indexName], value)
		}
	}
}

func getkey(resource string, scopes []store.Scope, name string) string {
	var key strings.Builder
	key.WriteString("/" + resource)
	for _, scope := range scopes {
		key.WriteString("/" + scope.Resource + "/" + scope.Name)
	}
	key.WriteString("/" + name)
	return key.String()
}

func getlistkey(resource string, scopes []store.Scope) string {
	return getkey(resource, scopes, "")
}

func matchesSearch(object *store.Unstructured, search string, fields []string) bool {
	if search == "" {
		return true
	}
	if len(fields) == 0 {
		fields = []string{"id", "name"}
	}
	search = strings.ToLower(search)
	for _, field := range fields {
		value, found := store.GetNestedField(object.Object, strings.Split(field, ".")...)
		if found && strings.Contains(strings.ToLower(fmt.Sprint(value)), search) {
			return true
		}
	}
	return false
}

func (i *InMemory) resourceName(obj any) (string, error) {
	return i.core.schema.ResourceName(obj)
}
