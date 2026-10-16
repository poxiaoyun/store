package garbagecollector

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"time"

	"golang.org/x/sync/errgroup"
	"k8s.io/client-go/util/retry"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/log"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/controller"
	storecache "xiaoshiai.cn/store/reflector"
)

type GarbageCollector struct {
	storage   store.Store
	options   GarbageCollectorOptions
	resources []string
	graph     *graph
}

type GarbageCollectorOptions struct {
	// SetParentAsOwner indicates whether the parent should be add to ownerReferences when object is created.
	SetParentAsOwner bool
}

func NewGarbageCollector(storage store.Store, options GarbageCollectorOptions) (*GarbageCollector, error) {
	if !storage.Capabilities().
		Watch {
		return nil, errors.NewUnsupported("garbage collector requires Store Watch support")
	}
	return &GarbageCollector{
		storage: storage,
		options: options,
		resources: storage.Schema().
			Resources(),
		graph: NewGraph(),
	}, nil
}

func (c *GarbageCollector) Name() string {
	return "garbage-collector"
}

func (c *GarbageCollector) Run(ctx context.Context) error {
	reflectors := make([]*storecache.Reflector[store.Unstructured], 0, len(c.resources))
	handlers := make([]*garbageCollectorReflectorHandler, 0, len(c.resources))
	for _, resource := range c.resources {
		list := &store.List[store.Unstructured]{Resource: resource}
		reflectors = append(reflectors, storecache.NewReflector(c.storage, list, store.WithSubScopes()))
		handlers = append(handlers, &garbageCollectorReflectorHandler{Collector: c, Resource: resource})
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return controller.RunQueueConsumer(ctx, c.graph.changes, c.processGraphChangesResult, 1)
	})
	for index := range reflectors {
		reflector := reflectors[index]
		handler := handlers[index]
		eg.Go(func() error {
			logger := log.FromContext(ctx).
				WithValues("resource", handler.Resource)
			logger.Info("start monitor")
			watchContext := log.NewContext(ctx, logger)
			return reflector.Run(watchContext, handler)
		})
	}
	eg.Go(func() error {
		if err := WaitForReflectors(ctx, reflectors); err != nil {
			return err
		}
		return controller.RunQueueConsumer(ctx, c.graph.attemptToDelete, c.processAttemptToDeleteResult, 1)
	})
	eg.Go(func() error {
		if err := WaitForReflectors(ctx, reflectors); err != nil {
			return err
		}
		return controller.RunQueueConsumer(ctx, c.graph.attemptToOrphan, c.processAttemptToOrphanResult, 1)
	})
	return eg.Wait()
}

// WaitForReflectors waits until every reflector has published its first snapshot.
func WaitForReflectors[T any](ctx context.Context, reflectors []*storecache.Reflector[T]) error {
	for index := range reflectors {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-reflectors[index].Synced():
		}
	}
	return nil
}

type garbageCollectorReflectorHandler struct {
	Collector *GarbageCollector
	Resource  string
}

func (h *garbageCollectorReflectorHandler) Replace(ctx context.Context, objects []*store.Unstructured) error {
	snapshot := make([]*event, 0, len(objects))
	for _, object := range objects {
		if h.Collector.options.SetParentAsOwner {
			if err := h.Collector.injectOwnerReference(ctx, object); err != nil {
				return err
			}
		}
		snapshot = append(snapshot, graphEventFromObject(store.WatchEventCreate, object))
	}
	return h.Collector.EnqueueGraphEvent(ctx, &event{
		eventtype: eventTypeSnapshot,
		resource:  h.Resource,
		snapshot:  snapshot,
	})
}

func (h *garbageCollectorReflectorHandler) Apply(ctx context.Context, eventType store.WatchEventType, object *store.Unstructured) error {
	if h.Collector.options.SetParentAsOwner && eventType == store.WatchEventCreate {
		if err := h.Collector.injectOwnerReference(ctx, object); err != nil {
			return err
		}
	}
	return h.Collector.EnqueueGraphEvent(ctx, graphEventFromObject(eventType, object))
}

func (*garbageCollectorReflectorHandler) Invalidate(context.Context, error) {}

// EnqueueGraphEvent waits until the graph worker has applied the event.
func (c *GarbageCollector) EnqueueGraphEvent(ctx context.Context, change *event) error {
	change.done = make(chan error, 1)
	c.graph.changes.Add(change)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-change.done:
		return err
	}
}

func graphEventFromObject(kind store.WatchEventType, object store.Object) *event {
	return &event{
		identity:   objectIdentityFrom(object),
		eventtype:  eventType(kind),
		owners:     object.GetOwnerReferences(),
		deleting:   object.GetDeletionTimestamp() != nil,
		finalizers: object.GetFinalizers(),
	}
}

func (c *GarbageCollector) injectOwnerReference(ctx context.Context, obj *store.Unstructured) error {
	ownerscopes := obj.GetScopes()
	if len(ownerscopes) == 0 {
		return nil
	}
	ownerscopes, last := ownerscopes[:len(ownerscopes)-1], ownerscopes[len(ownerscopes)-1]

	newown := store.OwnerReference{
		ID:       last.Name,
		Resource: last.Resource,
		Scopes:   ownerscopes,
	}
	ownerRefs := obj.GetOwnerReferences()
	for _, ref := range ownerRefs {
		// if the owner reference already exists, skip
		if IsSameScopes(ref.Scopes, newown.Scopes) && ref.Resource == newown.Resource && ref.ID == newown.ID {
			return nil
		}
	}

	// inject owner reference of current parent
	log.FromContext(ctx).
		V(5).
		Info("inject owner reference", "owner", newown, "resource", obj.GetResource(), "id", obj.GetID())

	storage := c.storage.Scope(ownerscopes...)
	owner := &store.Unstructured{}
	owner.SetResource(last.Resource)
	if err := storage.Get(ctx, last.Name, owner); err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	newown.UID = owner.GetUID()

	for _, ref := range ownerRefs {
		if ref.UID == newown.UID {
			return nil
		}
	}
	ownerRefs = append(ownerRefs, newown)
	patchdata, err := json.Marshal(map[string]any{
		"resourceVersion": obj.GetResourceVersion(),
		"ownerReferences": ownerRefs,
	})
	if err != nil {
		return err
	}
	patch := store.RawPatch(store.PatchTypeMergePatch, patchdata)
	storage = c.storage.Scope(obj.GetScopes()...)
	return storage.Patch(ctx, obj, patch)
}

func eventType(kind store.WatchEventType) eventtype {
	switch kind {
	case store.WatchEventCreate:
		return eventTypeAdd
	case store.WatchEventUpdate:
		return eventTypeUpdate
	case store.WatchEventDelete:
		return eventTypeDelete
	}
	return eventTypeVirtual
}

type eventtype int

const (
	eventTypeAdd eventtype = iota + 1
	eventTypeUpdate
	eventTypeDelete
	eventTypeVirtual
	eventTypeSnapshot
)

type event struct {
	identity   objectIdentity
	owners     []store.OwnerReference
	eventtype  eventtype
	virtual    bool // Virtual means the change is not a real object change
	deleting   bool // Deleting means the object has deletion timestamp set
	finalizers []string

	resource string
	snapshot []*event
	done     chan error
}

type objectIdentity struct {
	store.ResourcedObjectReference
	UID string
}

func (o objectIdentity) Equals(other objectIdentity) bool {
	return o.UID == other.UID && o.ResourcedObjectReference.Equals(other.ResourcedObjectReference)
}

func objectIdentityFrom(obj store.Object) objectIdentity {
	return objectIdentity{
		ResourcedObjectReference: store.ResourcedObjectReference{
			ID:       obj.GetID(),
			Resource: obj.GetResource(),
			Scopes:   obj.GetScopes(),
		},
		UID: obj.GetUID(),
	}
}

func (c *GarbageCollector) processAttemptToOrphanResult(ctx context.Context, node *node) (controller.Result, error) {
	return controller.Result{}, c.processAttemptToOrphan(ctx, node)
}

func (c *GarbageCollector) processAttemptToOrphan(ctx context.Context, node *node) error {
	children := node.getChildren()
	if err := c.orphanDependents(ctx, node.identity, children); err != nil {
		return err
	}
	if err := c.removeFinalizer(ctx, node, store.FinalizerOrphanDependents); err != nil {
		return err
	}
	return nil
}

// dependents are copies of pointers to the owner's dependents, they don't need to be locked.
func (gc *GarbageCollector) orphanDependents(ctx context.Context, owner objectIdentity, dependents []*node) error {
	eg := errgroup.Group{}
	for _, dependent := range dependents {
		dependent := dependent
		eg.Go(func() error {
			return gc.deleteOwnerReferences(ctx, dependent, owner.UID)
		})
	}
	return eg.Wait()
}

func (gc *GarbageCollector) processAttemptToDeleteResult(ctx context.Context, n *node) (controller.Result, error) {
	if err := gc.processAttemptToDeleteInner(ctx, n); err != nil {
		return controller.Result{}, err
	}
	if !n.isObserved() {
		// we need to requeue the node because it's not observed yet
		// retry later
		return controller.Result{Requeue: true, RequeueAfter: 5 * time.Second}, nil
	}
	return controller.Result{}, nil
}

func (gc *GarbageCollector) processAttemptToDeleteInner(ctx context.Context, n *node) error {
	logger := log.FromContext(ctx).
		WithValues("item", n.identity)
	if !n.isObserved() {
		nodeFromGraph, existsInGraph := gc.graph.getNode(n.identity.UID)
		if !existsInGraph {
			// this can happen if attemptToDelete loops on a requeued virtual node because attemptToDeleteItem returned an error,
			// and in the meantime a deletion of the real object associated with that uid was observed
			return nil
		}
		if nodeFromGraph.isObserved() {
			// this can happen if attemptToDelete loops on a requeued virtual node because attemptToDeleteItem returned an error,
			// and in the meantime the real object associated with that uid was observed
			return nil
		}
	}
	if n.isBeingDeleting() && !n.isDeletingChildren() {
		return nil
	}
	latest, err := gc.getObject(ctx, n.identity)
	switch {
	case errors.IsNotFound(err):
		// the GraphBuilder can add "virtual" node for an owner that doesn't
		// exist yet, so we need to enqueue a virtual Delete event to remove
		// the virtual node from GraphBuilder.uidToNode.
		logger.V(5).
			Info("n not found, generating a virtual delete event")
		gc.graph.enqueueVirtualDeleteEvent(n.identity)
		return nil
	case err != nil:
		return err
	}
	if latest.GetUID() != n.identity.UID {
		// the object has been recreated, we need to reevaluate the dependents
		logger.V(5).
			Info("object recreated, generating a virtual delete event")
		gc.graph.enqueueVirtualDeleteEvent(n.identity)
		return nil
	}
	if n.isDeletingChildren() {
		return gc.processDeletingDependentsItem(ctx, n)
	}
	ownerReferences := latest.GetOwnerReferences()

	if len(ownerReferences) == 0 {
		return nil
	}

	solid, dangling, waitingForDependentsDeletion, err := gc.classifyReferences(ctx, n, ownerReferences)
	if err != nil {
		return err
	}
	logger.V(6).
		Info("classify item's references",
			"solid", solid,
			"dangling", dangling,
			"waitingForDependentsDeletion", waitingForDependentsDeletion,
		)
	switch {
	case len(solid) != 0:
		if len(dangling) == 0 && len(waitingForDependentsDeletion) == 0 {
			return nil
		}
		logger.V(2).
			Info("remove dangling references and waiting references for item",
				"dangling", dangling,
				"waitingForDependentsDeletion", waitingForDependentsDeletion,
			)
		// waitingForDependentsDeletion needs to be deleted from the
		// ownerReferences, otherwise the referenced objects will be stuck with
		// the FinalizerDeletingDependents and never get deleted.
		ownerUIDsToDelete := append(ownerRefsToUIDs(dangling), ownerRefsToUIDs(waitingForDependentsDeletion)...)
		return gc.deleteOwnerReferences(ctx, n, ownerUIDsToDelete...)

	case len(waitingForDependentsDeletion) != 0 && n.childrenLength() != 0:
		deps := n.getChildren()
		for _, dep := range deps {
			if dep.isDeletingChildren() {
				// this circle detection has false positives, we need to
				// apply a more rigorous detection if this turns out to be a
				// problem.
				// there are multiple workers run attemptToDeleteItem in
				// parallel, the circle detection can fail in a race condition.
				logger.V(2).
					Info("processing item, some of its owners and its dependent have FinalizerDeletingDependents, to prevent potential cycle, its ownerReferences are going to be modified to be non-blocking, then the item is going to be deleted with Foreground",
						"dependent", dep.identity,
					)
				// change the ownerReferences to be non-blocking
				// set all blocking ownerReferences to non-blocking
				// TODO: patch the ownerReferences to remove the blocking ownerReferences
				break
			}
		}
		logger.V(2).
			Info("at least one owner of item has FinalizerDeletingDependents, and the item itself has dependents, so it is going to be deleted in Foreground",
				"item", n.identity,
			)
		// the deletion event will be observed by the graphBuilder, so the item
		// will be processed again in processDeletingDependentsItem. If it
		// doesn't have dependents, the function will remove the
		// FinalizerDeletingDependents from the item, resulting in the final
		// deletion of the item.
		policy := store.DeletePropagationForeground
		logger.V(2).
			Info("Deleting item", "propagationPolicy", policy)
		return gc.deleteObject(ctx, n.identity, policy)
	default:
		// item doesn't have any solid owner, so it needs to be garbage
		// collected. Also, none of item's owners is waiting for the deletion of
		// the dependents, so set propagationPolicy based on existing finalizers.
		var policy store.DeletionPropagation
		switch {
		case slices.Contains(latest.GetFinalizers(), store.FinalizerOrphanDependents):
			// if an existing orphan finalizer is already on the object, honor it.
			policy = store.DeletePropagationOrphan
		case slices.Contains(latest.GetFinalizers(), store.FinalizerDeleteDependents):
			// if an existing foreground finalizer is already on the object, honor it.
			policy = store.DeletePropagationForeground
		default:
			// otherwise, default to background.
			policy = store.DeletePropagationBackground
		}
		logger.V(2).
			Info("Deleting item", "propagationPolicy", policy)
		return gc.deleteObject(ctx, n.identity, policy)
	}
}

func ownerRefsToUIDs(refs []store.OwnerReference) []string {
	var ret []string
	for _, ref := range refs {
		ret = append(ret, ref.UID)
	}
	return ret
}

func (gc *GarbageCollector) processDeletingDependentsItem(ctx context.Context, item *node) error {
	logger := log.FromContext(ctx)
	blockingChildren := item.blockingChildren()
	if len(blockingChildren) == 0 {
		logger.V(2).
			Info("remove DeleteDependents finalizer for item", "item", item.identity)
		return gc.removeFinalizer(ctx, item, store.FinalizerDeleteDependents)
	}
	for _, child := range blockingChildren {
		if !child.isDeletingChildren() {
			logger.V(2).
				Info("adding dependent to attemptToDelete, because its owner is deletingDependents",
					"item", item.identity,
					"dependent", child.identity,
				)
			gc.graph.attemptToDelete.Add(child)
		}
	}
	return nil
}

// classify the latestReferences to three categories:
// solid: the owner exists, and is not "waitingForDependentsDeletion"
// dangling: the owner does not exist
// waitingForDependentsDeletion: the owner exists, its deletionTimestamp is non-nil, and it has
// FinalizerDeletingDependents
// This function communicates with the server.
func (gc *GarbageCollector) classifyReferences(ctx context.Context, item *node, latestReferences []store.OwnerReference) (
	solid, dangling, waitingForDependentsDeletion []store.OwnerReference, err error,
) {
	for _, reference := range latestReferences {
		isDangling, owner, err := gc.isDangling(ctx, reference, item)
		if err != nil {
			return nil, nil, nil, err
		}
		if isDangling {
			dangling = append(dangling, reference)
			continue
		}
		if owner.GetDeletionTimestamp() != nil && slices.Contains(owner.GetFinalizers(), store.FinalizerDeleteDependents) {
			waitingForDependentsDeletion = append(waitingForDependentsDeletion, reference)
		} else {
			solid = append(solid, reference)
		}
	}
	return solid, dangling, waitingForDependentsDeletion, nil
}

// isDangling check if a reference is pointing to an object that doesn't exist.
// If isDangling looks up the referenced object at the API server, it also
// returns its latest state.
func (gc *GarbageCollector) isDangling(ctx context.Context, reference store.OwnerReference, item *node) (bool, store.Object, error) {
	logger := log.FromContext(ctx)
	// check for recorded absent cluster-scoped parent
	absentOwnerCacheKey := ownerReferenceIdentity(reference)
	if gc.graph.absentOwnerCache.Has(absentOwnerCacheKey) {
		logger.V(5).
			Info("according to the absentOwnerCache, item's owner does not exist",
				"item", item.identity,
				"owner", reference,
			)
		return true, nil, nil
	}
	// TODO: It's only necessary to talk to the API server if the owner node
	// is a "virtual" node. The local graph could lag behind the real
	// status, but in practice, the difference is small.
	desc := &store.Unstructured{}
	desc.SetResource(reference.Resource)
	if err := gc.storage.Scope(absentOwnerCacheKey.Scopes...).
		Get(ctx, absentOwnerCacheKey.ID, desc); err != nil {
		if errors.IsNotFound(err) {
			gc.graph.absentOwnerCache.Add(absentOwnerCacheKey)
			logger.V(5).
				Info("item's owner is not found", "item", item.identity, "owner", reference)
			return true, nil, nil
		}
		return false, nil, err
	}
	if desc.GetUID() != reference.UID {
		logger.V(5).
			Info("item's owner is not found, UID mismatch", "item", item.identity, "owner", reference)
		gc.graph.absentOwnerCache.Add(absentOwnerCacheKey)
		return true, nil, nil
	}
	return false, desc, nil
}

func ownerReferenceIdentity(ref store.OwnerReference) objectIdentity {
	return objectIdentity{
		UID:                      ref.UID,
		ResourcedObjectReference: store.ResourcedObjectReference{Resource: ref.Resource, ID: ref.ID, Scopes: ref.Scopes},
	}
}

func (gc *GarbageCollector) removeFinalizer(ctx context.Context, owner *node, targetFinalizer string) error {
	logger := log.FromContext(ctx)
	err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		ownerObject, err := gc.getObject(ctx, owner.identity)
		if errors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("cannot finalize owner %s, because cannot get it: %v. The garbage collector will retry later", owner.identity, err)
		}
		finalizers := ownerObject.GetFinalizers()
		newfinalizers := []string{}
		found := false
		for _, f := range finalizers {
			if f == targetFinalizer {
				found = true
				continue
			}
			newfinalizers = append(newfinalizers, f)
		}
		if !found {
			logger.V(5).
				Info("finalizer already removed from object", "finalizer", targetFinalizer, "object", owner.identity)
			return nil
		}
		patch, err := json.Marshal(map[string]any{
			"resourceVersion": ownerObject.GetResourceVersion(),
			"finalizers":      newfinalizers,
		})
		if err != nil {
			return fmt.Errorf("unable to finalize %s due to an error serializing patch: %v", owner.identity, err)
		}
		return gc.patchObject(ctx, owner.identity, store.RawPatch(store.PatchTypeMergePatch, patch))
	})
	if errors.IsConflict(err) {
		return fmt.Errorf("updateMaxRetries(%d) has reached. The garbage collector will retry later for owner %v", retry.DefaultBackoff.Steps, owner.identity)
	}
	if errors.IsNotFound(err) {
		return nil
	}
	return err
}

func (gc *GarbageCollector) getObject(ctx context.Context, item objectIdentity) (store.Object, error) {
	storage := gc.storage.Scope(item.Scopes...)
	desc := &store.Unstructured{}
	desc.SetResource(item.Resource)
	if err := storage.Get(ctx, item.ID, desc); err != nil {
		return nil, err
	}
	return desc, nil
}

func (gc *GarbageCollector) patchObject(ctx context.Context, item objectIdentity, patch store.Patch) error {
	storage := gc.storage.Scope(item.Scopes...)
	desc := &store.Unstructured{}
	desc.SetResource(item.Resource)
	desc.SetID(item.ID)

	return storage.Patch(ctx, desc, patch)
}

func (gc *GarbageCollector) deleteObject(ctx context.Context, item objectIdentity, policy store.DeletionPropagation) error {
	options := []store.DeleteOption{}
	if policy != "" {
		options = append(options, store.WithPropagation(policy))
	} else {
		// directly delete the object if no policy is specified
		options = append(options, store.WithPropagation(store.DeletePropagationBackground))
	}
	storage := gc.storage.Scope(item.Scopes...)
	desc := &store.Unstructured{}
	desc.SetResource(item.Resource)
	desc.SetID(item.ID)
	desc.SetUID(item.UID)
	return storage.Delete(ctx, desc, options...)
}

type ObjectMetaForPatch struct {
	ResourceVersion int64                  `json:"resourceVersion"`
	OwnerReferences []store.OwnerReference `json:"ownerReferences"`
}

// Returns JSON merge patch that removes the ownerReferences matching ownerUIDs.
func (gc *GarbageCollector) deleteOwnerReferences(ctx context.Context, item *node, ownerUIDs ...string) error {
	obj, err := gc.getObject(ctx, item.identity)
	if err != nil {
		if errors.IsNotFound(err) {
			return nil
		}
		return err
	}
	expectedObjectMeta := ObjectMetaForPatch{}
	expectedObjectMeta.ResourceVersion = obj.GetResourceVersion()
	refs := obj.GetOwnerReferences()
	for _, ref := range refs {
		var skip bool
		for _, ownerUID := range ownerUIDs {
			if ref.UID == ownerUID {
				skip = true
				break
			}
		}
		if !skip {
			expectedObjectMeta.OwnerReferences = append(expectedObjectMeta.OwnerReferences, ref)
		}
	}
	patchdata, err := json.Marshal(expectedObjectMeta)
	if err != nil {
		return err
	}
	patch := store.RawPatch(store.PatchTypeMergePatch, patchdata)
	return gc.patchObject(ctx, item.identity, patch)
}

func (c *GarbageCollector) processGraphChangesResult(ctx context.Context, event *event) (controller.Result, error) {
	if err := c.processGraphChanges(ctx, event); err != nil {
		return controller.Result{}, err
	}
	if event.done != nil {
		event.done <- nil
	}
	return controller.Result{}, nil
}

func (c *GarbageCollector) processGraphChanges(ctx context.Context, event *event) error {
	if event.eventtype == eventTypeSnapshot {
		return c.processResourceSnapshot(ctx, event)
	}
	existingNode, found := c.graph.getNode(event.identity.UID)
	// If the node exists in the graph and is not observed, mark it as observed.
	if found && !event.virtual && !existingNode.isObserved() {
		// maybe the identity has changed, if so, we need to re-evaluate dependents
		if !existingNode.identity.Equals(event.identity) {
			// find dependents that don't match the identity we observed
			_, potentiallyInvalidDependents := partitionDependents(existingNode.getChildren(), event.identity)
			// add those potentially invalid dependents to the attemptToDelete queue.
			// if their owners are still solid the attemptToDelete will be a no-op.
			// this covers the bad child -> good parent observation sequence.
			// the good parent -> bad child observation sequence is handled in addDependentToOwners
			for _, dep := range potentiallyInvalidDependents {
				c.graph.attemptToDelete.Add(dep)
			}
			existingNode.identity = event.identity
		}
		existingNode.markObserved()
	}

	switch {
	// create a new node
	case !found && (event.eventtype == eventTypeAdd || event.eventtype == eventTypeUpdate):
		node := &node{
			identity:         event.identity,
			children:         map[*node]empty{},
			owners:           event.owners,
			deleting:         event.deleting,
			deletingChildren: event.deleting && slices.Contains(event.finalizers, store.FinalizerDeleteDependents),
		}
		c.graph.addNode(node)
		c.graph.addDependentToOwners(node, node.owners)
		c.graph.processTransitions(event, node)
	case found && (event.eventtype == eventTypeAdd || event.eventtype == eventTypeUpdate):
		added, removed, changed := referencesDiffs(existingNode.owners, event.owners)
		if len(added) != 0 || len(removed) != 0 || len(changed) != 0 {
			existingNode.owners = event.owners
			c.graph.addUnblockedOwnersToDeleteQueue(removed, changed)
			c.graph.addDependentToOwners(existingNode, added)
			c.graph.removeDependentFromOwners(existingNode, removed)
		}
		c.graph.processTransitions(event, existingNode)
		return nil

	// hard delete an existing node
	case event.eventtype == eventTypeDelete:
		if !found {
			return nil
		}
		removeExistingNode := true
		if event.virtual {
			// if the node is virtual, we need to check if it has any dependents that are not being deleted
			if existingNode.virtual {
				matchingDependents, nonmatchingDependents := partitionDependents(existingNode.getChildren(), event.identity)
				if len(nonmatchingDependents) > 0 {
					// some dependents are not agree to delete, so we should not remove the virtual node
					removeExistingNode = false
					if len(matchingDependents) > 0 {
						c.graph.absentOwnerCache.Add(event.identity)
						for _, dep := range matchingDependents {
							c.graph.attemptToDelete.Add(dep)
						}
					}
				}
			} else {
				if !existingNode.identity.Equals(event.identity) {
					// do not remove the existing real node from the graph based on a virtual delete event
					removeExistingNode = false
					matchingDependents, _ := partitionDependents(existingNode.getChildren(), event.identity)
					if len(matchingDependents) > 0 {
						c.graph.absentOwnerCache.Add(event.identity)
						for _, dep := range matchingDependents {
							c.graph.attemptToDelete.Add(dep)
						}
					}
				}
			}
		}
		if removeExistingNode {
			c.graph.removeNode(existingNode.identity.UID)
			c.graph.removeDependentFromOwners(existingNode, existingNode.owners)

			existingNode.childlock.RLock()
			defer existingNode.childlock.RUnlock()
			if len(existingNode.children) > 0 {
				c.graph.absentOwnerCache.Add(event.identity)
				for dep := range existingNode.children {
					c.graph.attemptToDelete.Add(dep)
				}
			}
			for _, owner := range existingNode.owners {
				ownerNode, found := c.graph.getNode(owner.UID)
				if !found || !ownerNode.isDeletingChildren() {
					continue
				}
				// notify the owner that one of its children is being deleted
				c.graph.attemptToDelete.Add(ownerNode)
			}
		}
		// notify the parent
	}
	return nil
}

func (c *GarbageCollector) processResourceSnapshot(ctx context.Context, snapshot *event) error {
	existing := c.graph.NodesForResource(snapshot.resource)
	seen := make(map[string]struct{}, len(snapshot.snapshot))
	for _, change := range snapshot.snapshot {
		seen[change.identity.UID] = struct{}{}
		if err := c.processGraphChanges(ctx, change); err != nil {
			return err
		}
	}
	for _, node := range existing {
		if _, ok := seen[node.identity.UID]; ok {
			continue
		}
		if err := c.processGraphChanges(ctx, &event{
			identity:  node.identity,
			eventtype: eventTypeDelete,
			virtual:   !node.isObserved(),
		}); err != nil {
			return err
		}
	}
	return nil
}

type ownerRefPair struct {
	oldRef store.OwnerReference
	newRef store.OwnerReference
}

func referencesDiffs(olds []store.OwnerReference, news []store.OwnerReference) (added []store.OwnerReference, removed []store.OwnerReference, changed []ownerRefPair) {
	oldrefs := make(map[string]store.OwnerReference)
	for _, value := range olds {
		oldrefs[value.UID] = value
	}
	for _, new := range news {
		if old, ok := oldrefs[new.UID]; ok {
			if !SameOwnerReferences(old, new) {
				changed = append(changed, ownerRefPair{oldRef: old, newRef: new})
			}
			delete(oldrefs, new.UID)
		} else {
			added = append(added, new)
		}
	}
	for _, old := range oldrefs {
		removed = append(removed, old)
	}
	return added, removed, changed
}

func SameOwnerReferences(a, b store.OwnerReference) bool {
	return reflect.DeepEqual(a, b)
}

func partitionDependents(children []*node, owner objectIdentity) (matches, nomatches []*node) {
	for _, child := range children {
		foundMatch, foundMismatch := false, false
		// just allow own same scopes or under scopes
		if !IsSameOrUnderScoped(child.identity.Scopes, owner.Scopes) {
			foundMismatch = true
		} else {
			for _, ownerRef := range child.owners {
				if ownerRef.UID == owner.UID {
					if ownerReferenceMatchesCoordinates(ownerRef, owner) {
						foundMatch = true
					} else {
						foundMismatch = true
					}
				}
			}
		}
		if foundMatch {
			matches = append(matches, child)
		}
		if foundMismatch {
			nomatches = append(nomatches, child)
		}
	}
	return matches, nomatches
}

func ownerReferenceMatchesCoordinates(a store.OwnerReference, b objectIdentity) bool {
	return a.UID == b.UID && a.ID == b.ID && a.Resource == b.Resource && IsSameScopes(a.Scopes, b.Scopes)
}

func IsSameScopes(scope1, scope2 []store.Scope) bool {
	return controller.EncodeScopes(scope1) == controller.EncodeScopes(scope2)
}

// IsSameOrUnderScoped returns true if scope1 is under scope2.
// eg. scope1 = [ { "namespace", "default" } ], scope2 = [ { "namespace", "default" }, { "cluster", "abc" } ].
// scope2 is under scope1.
func IsSameOrUnderScoped(scope1, scope2 []store.Scope) bool {
	if len(scope2) == 0 {
		return true
	}
	if len(scope1) < len(scope2) {
		return false
	}
	for i := range scope2 {
		if scope1[i] != scope2[i] {
			return false
		}
	}
	return true
}
