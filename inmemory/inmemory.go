package inmemory

import (
	"context"
	"fmt"
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
	return i.core.schema.Clone()
}

// Capabilities implements store.Store.
func (i *InMemory) Capabilities() store.Capabilities {
	return store.Capabilities{Finalizers: true, OptimisticLock: true, Watch: true}
}

func New(schema *store.Schema) (*InMemory, error) {
	if schema == nil {
		return nil, fmt.Errorf("schema is nil")
	}
	return &InMemory{core: &inmemory{
		schema:    schema.Clone(),
		resources: map[string]*collection{},
		watchers:  map[int64]*inmemoryWatcher{},
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
	c, err := i.collectionFor(obj)
	if err != nil {
		return 0, err
	}
	return c.count(ctx, i.scopes, opts...)
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
	c, err := i.collectionFor(obj)
	if err != nil {
		return err
	}
	return c.getObject(ctx, i.scopes, name, obj, store.ApplyGetOptions(opts))
}

func (i *InMemory) List(ctx context.Context, list store.ObjectList, opts ...store.ListOption) error {
	c, err := i.collectionFor(list)
	if err != nil {
		return err
	}
	return c.listObjects(ctx, i.scopes, list, store.ApplyListOptions(opts))
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
	return &InMemory{core: i.core, scopes: append(slices.Clone(i.scopes), scope...), status: i.status}
}

func (i *InMemory) Status() store.StatusStorage {
	return &InMemory{core: i.core, scopes: i.scopes, status: true}
}

type inmemory struct {
	mu         sync.Mutex
	schema     *store.Schema
	resources  map[string]*collection
	rev        atomic.Uint64
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

// enqueue runs under the resource write lock so each resource's events retain
// commit order. Watches observe one resource at a time.
func (m *inmemory) enqueue(item change) {
	m.watcherMu.RLock()
	dispatcher := m.dispatcher
	m.watcherMu.RUnlock()
	if dispatcher != nil {
		dispatcher.queue.push(item)
	}
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

func (m *inmemory) create(resource string, scopes []store.Scope, name string, value store.Object) error {

	c := m.collection(resource)
	c.mu.Lock()
	defer c.mu.Unlock()

	key := getkey(scopes, name)
	if _, ok := c.objects[key]; ok {
		return errors.NewAlreadyExists(resource, name)
	}
	revision := m.rev.Add(1)
	prepared, err := c.prepare(value, int64(revision))
	if err != nil {
		return err
	}
	if err := c.checkUnique(key, prepared); err != nil {
		return err
	}
	c.commit(key, prepared)
	value.SetResourceVersion(int64(revision))
	m.enqueue(change{revision: int64(revision), new: prepared})
	return nil
}

func (m *inmemory) put(resource string, scopes []store.Scope, desired store.Object, status bool, options store.UpdateOptions) (bool, error) {
	c := m.collection(resource)
	c.mu.Lock()
	defer c.mu.Unlock()

	key := getkey(scopes, desired.GetID())
	kv, ok := c.objects[key]
	if !ok {
		return false, errors.NewNotFound(resource, desired.GetID())
	}
	current := store.NewObject(desired)
	if err := store.CopyObject(kv, current); err != nil {
		return false, err
	}
	if !matches(current, options.LabelRequirements, options.FieldRequirements) {
		return false, errors.NewConflict(resource, desired.GetID(), fmt.Errorf("write conditions did not match"))
	}
	if desired.GetResourceVersion() != 0 && desired.GetResourceVersion() != kv.GetResourceVersion() {
		return false, errors.NewConflict(resource, desired.GetID(), fmt.Errorf("resourceVersion %d does not match", desired.GetResourceVersion()))
	}
	deleted, err := prepareObjectForUpdate(current, desired, status)
	if err != nil {
		return false, err
	}
	if deleted {
		c.commit(key, nil)
		revision := m.rev.Add(1)
		m.enqueue(change{revision: int64(revision), old: current, new: nil})
		return true, nil
	}
	revision := m.rev.Add(1)
	prepared, err := c.prepare(desired, int64(revision))
	if err != nil {
		return false, err
	}
	if err := c.checkUnique(key, prepared); err != nil {
		return false, err
	}
	c.commit(key, prepared)
	desired.SetResourceVersion(int64(revision))
	m.enqueue(change{revision: int64(revision), old: current, new: prepared})
	return false, nil
}

func (m *inmemory) addWatcher(watcher *inmemoryWatcher, sendInitialEvents bool) {
	c := m.collection(watcher.resource)
	c.mu.RLock()
	m.watcherMu.Lock()
	m.watchers[watcher.id] = watcher
	if sendInitialEvents {
		prefix := getkey(watcher.scopes, "")
		initial := []store.Object{}
		for key, value := range c.objects {
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
	c.mu.RUnlock()
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

func (w *inmemoryWatcher) sendValue(eventType store.WatchEventType, value store.Object) {
	if w.matches(value) {
		w.sendObject(eventType, value)
	}
}

func (w *inmemoryWatcher) matches(source store.Object) bool {
	if source == nil {
		return false
	}
	if source.GetResource() != w.resource || w.objectID != "" && source.GetID() != w.objectID {
		return false
	}
	if w.includeSubScopes {
		if !store.ScopesIsSameOrUnder(source.GetScopes(), w.scopes) {
			return false
		}
	} else if !store.ScopesEquals(source.GetScopes(), w.scopes) {
		return false
	}
	return matches(source, w.labelRequirements, w.fieldRequirements)
}

func (w *inmemoryWatcher) sendChange(revision int64, old, new store.Object) {
	if revision <= w.startRevision {
		return
	}
	oldMatches, newMatches := w.matches(old), w.matches(new)
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
	object := w.newItem()
	if err := store.CopyObject(source, object); err != nil {
		w.sendTerminalError(errors.NewInternalError(err))
		return
	}
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
	c := m.collection(resource)
	c.mu.Lock()
	defer c.mu.Unlock()

	key := getkey(scopes, into.GetID())
	value, ok := c.objects[key]
	if !ok {
		return false, errors.NewNotFound(resource, into.GetID())
	}
	current := store.NewObject(into)
	if err := store.CopyObject(value, current); err != nil {
		return false, err
	}
	if !matches(current, options.LabelRequirements, options.FieldRequirements) {
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
		c.commit(key, nil)
		if err := store.CopyObject(desired, into); err != nil {
			return false, err
		}
		revision := m.rev.Add(1)
		m.enqueue(change{revision: int64(revision), old: current, new: nil})
		return true, nil
	}
	revision := m.rev.Add(1)
	prepared, err := c.prepare(desired, int64(revision))
	if err != nil {
		return false, err
	}
	if err := c.checkUnique(key, prepared); err != nil {
		return false, err
	}
	c.commit(key, prepared)
	if err := store.CopyObject(desired, into); err != nil {
		return false, errors.NewInternalError(err)
	}
	into.SetResourceVersion(int64(revision))
	m.enqueue(change{revision: int64(revision), old: current, new: prepared})
	return false, nil
}

func (m *inmemory) delete(resource string, scopes []store.Scope, desired store.Object, options store.DeleteOptions) (bool, error) {
	c := m.collection(resource)
	c.mu.Lock()
	defer c.mu.Unlock()

	key := getkey(scopes, desired.GetID())
	kv, ok := c.objects[key]
	if !ok {
		return false, errors.NewNotFound(resource, desired.GetID())
	}
	current := store.NewObject(desired)
	if err := store.CopyObject(kv, current); err != nil {
		return false, err
	}
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
		revision := m.rev.Add(1)
		prepared, err := c.prepare(current, int64(revision))
		if err != nil {
			return false, err
		}
		if err := c.checkUnique(key, prepared); err != nil {
			return false, err
		}
		c.commit(key, prepared)
		if err := store.CopyObject(current, desired); err != nil {
			return false, errors.NewInternalError(err)
		}
		desired.SetResourceVersion(int64(revision))
		m.enqueue(change{revision: int64(revision), old: old, new: prepared})
		return false, nil
	}
	revision := m.rev.Add(1)
	c.commit(key, nil)
	if err := store.CopyObject(current, desired); err != nil {
		return false, errors.NewInternalError(err)
	}
	m.enqueue(change{revision: int64(revision), old: old, new: nil})
	return true, nil
}

func (i *InMemory) resourceName(obj any) (string, error) {
	return i.core.schema.ResourceName(obj)
}

func (i *InMemory) collectionFor(object any) (*collection, error) {
	name, err := i.resourceName(object)
	if err != nil {
		return nil, err
	}
	return i.core.collection(name), nil
}

func (m *inmemory) collection(name string) *collection {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, exists := m.resources[name]; exists {
		return c
	}
	definition, err := m.schema.Resource(name)
	if err != nil {
		// Store also accepts unregistered resource types with their derived names.
		definition = store.ResourceSchema{ResourceName: name}
	}
	c := newCollection(definition)
	m.resources[name] = c
	return c
}
