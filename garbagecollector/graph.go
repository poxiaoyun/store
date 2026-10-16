package garbagecollector

import (
	"slices"
	"sync"

	"k8s.io/utils/lru"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/controller"
)

type empty struct{}

type graph struct {
	nodesmu          sync.RWMutex
	uidtonode        map[string]*node
	changes          controller.TypedQueue[*event]
	attemptToDelete  controller.TypedQueue[*node]
	attemptToOrphan  controller.TypedQueue[*node]
	absentOwnerCache *ReferenceCache
}

func NewGraph() *graph {
	return &graph{
		absentOwnerCache: NewReferenceCache(1000),
		changes:          controller.NewDefaultTypedQueue[*event]("gc-changes", nil),
		attemptToDelete:  controller.NewDefaultTypedQueue[*node]("gc-to-delete", nil),
		attemptToOrphan:  controller.NewDefaultTypedQueue[*node]("gc-to-orphan", nil),
		uidtonode:        make(map[string]*node),
	}
}

func (g *graph) getNode(uid string) (*node, bool) {
	g.nodesmu.RLock()
	defer g.nodesmu.RUnlock()
	node, ok := g.uidtonode[uid]
	return node, ok
}

// NodesForResource returns the graph nodes whose identity belongs to resource.
func (g *graph) NodesForResource(resource string) []*node {
	g.nodesmu.RLock()
	defer g.nodesmu.RUnlock()
	nodes := []*node{}
	for _, node := range g.uidtonode {
		if node.identity.Resource == resource {
			nodes = append(nodes, node)
		}
	}
	return nodes
}

func (g *graph) addNode(node *node) {
	g.nodesmu.Lock()
	defer g.nodesmu.Unlock()

	uid := node.identity.UID
	if _, ok := g.uidtonode[uid]; !ok {
		g.uidtonode[uid] = node
	}
}

func (g *graph) removeNode(uid string) {
	g.nodesmu.Lock()
	defer g.nodesmu.Unlock()
	delete(g.uidtonode, uid)
}

func (g *graph) addDependentToOwners(n *node, owners []store.OwnerReference) {
	hasInvalidOwnerReference := false
	for _, ownerref := range owners {
		ownerNode, ok := g.getNode(ownerref.UID)
		// owner is not exist, need verify the object
		if !ok {
			// Create a "virtual" node in the graph for the owner if it doesn't exist in the graph yet.
			ownerNode = &node{
				identity: ownerReferenceIdentity(ownerref),
				children: make(map[*node]empty),
				virtual:  true,
			}
			g.addNode(ownerNode)
			// verify if the owner is really exist
			g.attemptToDelete.Add(ownerNode)
		}
		ownerNode.addChild(n)
		if !hasInvalidOwnerReference {
			if !IsSameOrUnderScoped(n.identity.Scopes, ownerNode.identity.Scopes) ||
				!ownerReferenceMatchesCoordinates(ownerref, ownerNode.identity) {
				hasInvalidOwnerReference = true
			}
		}
	}
	if hasInvalidOwnerReference {
		// some referenced invalid,need verify the object
		g.attemptToDelete.Add(n)
	}
}

func (g *graph) enqueueVirtualDeleteEvent(ref objectIdentity) {
	g.changes.Add(&event{
		identity:  ref,
		virtual:   true,
		eventtype: eventTypeDelete,
	})
}

func (g *graph) processTransitions(event *event, n *node) {
	if !event.deleting {
		return
	}
	n.markBeingDeleting()

	if slices.Contains(event.finalizers, store.FinalizerOrphanDependents) {
		g.attemptToOrphan.Add(n)
		return
	}
	// If the object is being deleted and has the delete dependents finalizer, mark all children for deletion.
	if slices.Contains(event.finalizers, store.FinalizerDeleteDependents) {
		n.markDeletingChildren()
		for dep := range n.children {
			g.attemptToDelete.Add(dep)
		}
		g.attemptToDelete.Add(n)
		return
	}
	// wait for all finalizers to be removed before deleting the object
	if len(event.finalizers) == 0 {
		g.attemptToDelete.Add(n)
		return
	}
}

func (g *graph) addUnblockedOwnersToDeleteQueue(removed []store.OwnerReference, changed []ownerRefPair) {
	for _, removedref := range removed {
		if removedref.BlockOwnerDeletion != nil && *removedref.BlockOwnerDeletion {
			node, found := g.getNode(removedref.UID)
			if !found {
				continue
			}
			g.attemptToDelete.Add(node)
		}
	}
	for _, c := range changed {
		wasBlocked := c.oldRef.BlockOwnerDeletion != nil && *c.oldRef.BlockOwnerDeletion
		isUnblocked := c.newRef.BlockOwnerDeletion == nil || (c.newRef.BlockOwnerDeletion != nil && !*c.newRef.BlockOwnerDeletion)
		if wasBlocked && isUnblocked {
			node, found := g.getNode(c.newRef.UID)
			if !found {
				continue
			}
			g.attemptToDelete.Add(node)
		}
	}
}

// removeDependentFromOwners remove n from owners' dependents list.
func (g *graph) removeDependentFromOwners(n *node, owners []store.OwnerReference) {
	for _, owner := range owners {
		ownerNode, ok := g.getNode(owner.UID)
		if !ok {
			continue
		}
		ownerNode.removeChild(n)
	}
}

type node struct {
	identity objectIdentity
	// virtual node is not a 'real' node
	virtual     bool
	virtuallock sync.Mutex

	children  map[*node]empty
	childlock sync.RWMutex

	owners []store.OwnerReference

	deleting   bool
	deletelock sync.Mutex

	deletingChildren     bool
	deletingChildrenLock sync.Mutex
}

func (n *node) markObserved() {
	n.virtuallock.Lock()
	defer n.virtuallock.Unlock()
	n.virtual = false
}

func (n *node) isObserved() bool {
	n.virtuallock.Lock()
	defer n.virtuallock.Unlock()
	return !n.virtual
}

func (n *node) getChildren() []*node {
	n.childlock.Lock()
	defer n.childlock.Unlock()
	children := make([]*node, 0, len(n.children))
	for child := range n.children {
		children = append(children, child)
	}
	return children
}

func (n *node) addChild(child *node) {
	n.childlock.Lock()
	defer n.childlock.Unlock()
	n.children[child] = empty{}
}

func (n *node) removeChild(child *node) {
	n.childlock.Lock()
	defer n.childlock.Unlock()
	delete(n.children, child)
}

func (n *node) markBeingDeleting() {
	n.deletelock.Lock()
	defer n.deletelock.Unlock()
	n.deleting = true
}

func (n *node) isBeingDeleting() bool {
	n.deletelock.Lock()
	defer n.deletelock.Unlock()
	return n.deleting
}

func (n *node) markDeletingChildren() {
	n.deletingChildrenLock.Lock()
	defer n.deletingChildrenLock.Unlock()
	n.deletingChildren = true
}

func (n *node) isDeletingChildren() bool {
	n.deletingChildrenLock.Lock()
	defer n.deletingChildrenLock.Unlock()
	return n.deletingChildren
}

func (n *node) blockingChildren() []*node {
	dependents := n.getChildren()
	var ret []*node
	for _, dep := range dependents {
		for _, owner := range dep.owners {
			if owner.UID == n.identity.UID && owner.BlockOwnerDeletion != nil && *owner.BlockOwnerDeletion {
				ret = append(ret, dep)
			}
		}
	}
	return ret
}

func (n *node) childrenLength() int {
	n.childlock.Lock()
	defer n.childlock.Unlock()
	return len(n.children)
}

// ReferenceCache is an LRU cache for uid.
type ReferenceCache struct {
	mutex sync.Mutex
	cache *lru.Cache
}

// NewReferenceCache returns a ReferenceCache.
func NewReferenceCache(maxCacheEntries int) *ReferenceCache {
	return &ReferenceCache{
		cache: lru.New(maxCacheEntries),
	}
}

// Add adds a uid to the cache.
func (c *ReferenceCache) Add(reference objectIdentity) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.cache.Add(reference.UID, nil)
}

// Has returns if a uid is in the cache.
func (c *ReferenceCache) Has(reference objectIdentity) bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	_, found := c.cache.Get(reference.UID)
	return found
}
