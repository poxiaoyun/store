package store

import (
	"context"
	"time"
)

const (
	FinalizerOrphanDependents = "orphan"
	FinalizerDeleteDependents = "foregroundDeletion"
)

type (
	GetOptions struct {
		// Preconditions require an exact UID and/or object version on the selected object.
		Preconditions *Preconditions
		// ResourceVersion set to 0 to get from cache
		// ResourceVersion set to a specific value to get the object not older than that version
		// ResourceVersion set to nil to get from backend
		ResourceVersion *int64
		// FieldRequirements is a list of conditions that must be true for the get to occur.
		// It may not supported by all databases.
		FieldRequirements Requirements
		LabelRequirements Requirements
		// Fields is a list of fields to return.  If empty, all fields are returned.
		Fields []string
	}
	// GetOption configures one Get operation.
	GetOption interface {
		// ApplyToGet applies this option to a Get request.
		ApplyToGet(*GetOptions)
	}

	// ListOptions contains the fully resolved execution intent for one Store
	// List call. Protocol pagination parameters stay in the protocol layer;
	// callers express either a bounded window or a cursor read.
	ListOptions struct {
		// Offset skips that many matching objects. Values below zero are treated
		// as zero. It applies only to window reads.
		Offset int
		// Limit bounds the returned objects in both read modes. A zero limit
		// returns every object from Offset onward in a window read.
		Limit int
		// Continue selects a cursor read. A nil Continue performs a window read;
		// a non-nil Continue requests the batch after the pointed-to token, and
		// a pointed-to empty token requests the first batch. The backend owns
		// the token format and its position.
		Continue *string
		Search   string
		// SearchFields overrides the default search fields, id and name.
		SearchFields []string
		// Sort is the sort order of the list. The format is a comma-separated list of fields, optionally
		// suffixed by "+" or "-". The default is "metadata.name+", which sorts by the object's name.
		// For example, "metadata.name-,metadata.creationTimestamp+" sorts first by descending name, and then by
		// ascending creation timestamp.
		// name is alias for metadata.name
		// time is alias for metadata.creationTimestamp
		Sort string
		// ResourceVersion set to 0 to get from cache
		// ResourceVersion set to a specific value to get the object not older than that version
		// ResourceVersion set to nil to get from backend
		ResourceVersion   *int64
		LabelRequirements Requirements
		FieldRequirements Requirements
		//  IncludeSubScopes is a flag to include resources in subscopes of current scope.
		IncludeSubScopes bool
		// Fields is a list of fields to return.  If empty, all fields are returned.
		Fields []string
	}
	// ListOption configures one List operation.
	ListOption interface {
		// ApplyToList applies this option to a List request.
		ApplyToList(*ListOptions)
	}

	CountOptions struct {
		LabelRequirements Requirements
		FieldRequirements Requirements
		IncludeSubScopes  bool
	}
	// CountOption configures one Count operation.
	CountOption interface {
		// ApplyToCount applies this option to a Count request.
		ApplyToCount(*CountOptions)
	}

	CreateOptions struct {
		TTL    time.Duration
		DryRun bool
	}
	// CreateOption configures one Create operation.
	CreateOption interface {
		// ApplyToCreate applies this option to a Create request.
		ApplyToCreate(*CreateOptions)
	}

	DeleteOptions struct {
		LabelRequirements Requirements
		// FieldRequirements is not supported by all databases on deletion.
		FieldRequirements Requirements
		Preconditions     *Preconditions
		PropagationPolicy *DeletionPropagation
		DryRun            bool
	}
	// DeleteOption configures one Delete operation.
	DeleteOption interface {
		// ApplyToDelete applies this option to a Delete request.
		ApplyToDelete(*DeleteOptions)
	}
	// Preconditions restrict deletion to one caller-observed object identity.
	Preconditions struct {
		// UID prevents deleting a replacement object that reused the same ID.
		UID *string
		// ResourceVersion restricts deletion to the observed object version.
		ResourceVersion *int64
	}

	DeleteBatchOptions struct {
		LabelRequirements Requirements
		FieldRequirements Requirements
		DryRun            bool
	}
	// DeleteBatchOption configures one batch Delete operation.
	DeleteBatchOption interface {
		// ApplyToDeleteBatch applies this option to a batch Delete request.
		ApplyToDeleteBatch(*DeleteBatchOptions)
	}

	UpdateOptions struct {
		TTL time.Duration
		// FieldRequirements is a list of conditions that must be true for the update to occur.
		// it apply to fields.
		FieldRequirements Requirements
		LabelRequirements Requirements
		DryRun            bool
	}
	// UpdateOption configures one Update operation.
	UpdateOption interface {
		// ApplyToUpdate applies this option to an Update request.
		ApplyToUpdate(*UpdateOptions)
	}

	PatchOptions struct {
		// Preconditions require the identity and version used to compute this atomic patch.
		Preconditions     *Preconditions
		FieldRequirements Requirements
		LabelRequirements Requirements
		DryRun            bool
	}
	// PatchOption configures one Patch operation.
	PatchOption interface {
		// ApplyToPatch applies this option to a Patch request.
		ApplyToPatch(*PatchOptions)
	}

	PatchBatchOptions struct {
		// FieldRequirements is a list of conditions that must be true for the update to occur.
		// it apply to fields.
		FieldRequirements Requirements
		LabelRequirements Requirements
		DryRun            bool
	}
	// PatchBatchOption configures one batch Patch operation.
	PatchBatchOption interface {
		// ApplyToPatchBatch applies this option to a batch Patch request.
		ApplyToPatchBatch(*PatchBatchOptions)
	}

	WatchOptions struct {
		ID                string
		LabelRequirements Requirements
		FieldRequirements Requirements
		ResourceVersion   *int64
		IncludeSubScopes  bool
		SendInitialEvents bool
	}
	// WatchOption configures one Watch operation.
	WatchOption interface {
		// ApplyToWatch applies this option to a Watch request.
		ApplyToWatch(*WatchOptions)
	}
)

// DeletionPropagation decides if a deletion will propagate to the dependents of
// the object, and how the garbage collector will handle the propagation.
type DeletionPropagation string

const (
	DeletePropagationBackground DeletionPropagation = "Background"
	DeletePropagationForeground DeletionPropagation = "Foreground"
	DeletePropagationOrphan     DeletionPropagation = "Orphan"
)

type PatchType string

const (
	PatchTypeJSONPatch  PatchType = "application/json-patch+json"
	PatchTypeMergePatch PatchType = "application/merge-patch+json"
)

type Patch interface {
	Type() PatchType
	Data(obj Object) ([]byte, error)
}

type PatchBatch interface {
	Type() PatchType
	Data() []byte
}

type Watcher interface {
	// Stop stops the watch. It is safe to call more than once. Events eventually
	// closes after Stop or cancellation of the context passed to Store.Watch.
	Stop()
	// Events returns the ordered event stream. A runtime failure is delivered as
	// one terminal error event before the channel closes.
	Events() <-chan WatchEvent
}
type WatchEventType string

const (
	WatchEventCreate   WatchEventType = "create"
	WatchEventUpdate   WatchEventType = "update"
	WatchEventDelete   WatchEventType = "delete"
	WatchEventBookmark WatchEventType = "bookmark"
)

// WatchEvent describes one ordered change in a Watch stream. ResourceVersion
// is a global stream checkpoint when positive; Object.ResourceVersion remains
// the version of that individual object.
type WatchEvent struct {
	Type            WatchEventType
	Object          Object
	ResourceVersion int64
	Error           error
}

// Store persists scoped resources and owns their lifecycle metadata.
type Store interface {
	// Schema returns an independent snapshot of the resource schema. Mutating
	// the returned schema does not affect the Store or Stores derived by Scope.
	Schema() *Schema
	// Capabilities reports the optional behavior implemented by this store.
	Capabilities() Capabilities
	// Get loads the object identified by id in the exact current scope into obj.
	// Selectors and exact Preconditions are checked against the same read snapshot.
	// Missing or unselected objects return NotFound; mismatched Preconditions return Conflict.
	Get(ctx context.Context, id string, obj Object, opts ...GetOption) error
	// List loads objects from the current scope into list. Order is unspecified
	// unless a sort is requested; subscopes are excluded by default.
	List(ctx context.Context, list ObjectList, opts ...ListOption) error
	// Count returns the number of objects matching the selectors in the exact
	// current scope, or its subscopes when explicitly requested.
	Count(ctx context.Context, obj Object, opts ...CountOption) (int, error)
	// Create persists obj in the current scope. It generates missing identity
	// and server-owned metadata and returns the persisted metadata through obj.
	Create(ctx context.Context, obj Object, opts ...CreateOption) error
	// Delete requests deletion of obj in the current scope. Background deletion
	// is the default; finalizers can keep the object in a terminating state.
	Delete(ctx context.Context, obj Object, opts ...DeleteOption) error
	// DeleteBatch deletes objects of the list's resource that match the supplied
	// selectors in the current scope.
	DeleteBatch(ctx context.Context, obj ObjectList, opts ...DeleteBatchOption) error
	// Update replaces obj while preserving server-owned metadata and status.
	// ResourceVersion zero is unconditional; a non-zero value is a CAS condition.
	Update(ctx context.Context, obj Object, opts ...UpdateOption) error
	// Patch atomically applies patch to the latest stored object. The object's
	// ResourceVersion is not a precondition; a non-zero resourceVersion in a
	// merge patch or a JSON Patch test operation makes the patch conditional.
	Patch(ctx context.Context, obj Object, patch Patch, opts ...PatchOption) error
	// PatchBatch applies patch to objects of the list's resource that match the
	// supplied selectors in the current scope.
	PatchBatch(ctx context.Context, obj ObjectList, patch PatchBatch, opts ...PatchBatchOption) error
	// Watch observes ordered mutations of the list's resource without silently
	// dropping events. Delete events contain the complete previous object and
	// selector transitions are reported as Create, Update, or Delete according
	// to membership before and after the mutation. WithSendInitialEvents starts
	// an initial Watch: callers apply every event before the first Bookmark to
	// staging state, then atomically publish it; later events are live changes.
	// A positive WatchEvent ResourceVersion is a global checkpoint accepted by
	// WithResourceVersion. Stores that cannot serve a requested position
	// return ResourceExpired so callers can start a new initial Watch.
	Watch(ctx context.Context, obj ObjectList, opts ...WatchOption) (Watcher, error)
	// Status returns the status-subresource writer. Its mutations can only change
	// the top-level status field and ResourceVersion.
	Status() StatusStorage
	// Scope returns a derived Store with scope appended to the current ordered
	// scope; it does not mutate the receiver.
	Scope(scope ...Scope) Store
}

// Capabilities describes optional Store behavior for discovery. Store methods
// validate options from their own backend constraints, not from this value.
// CRUD, Count, exact scopes, server-managed metadata, status isolation,
// finalizers, ID sorting, and offset/Limit window reads are part of the base
// contract and are omitted.
type Capabilities struct {
	LabelSelector    bool
	FieldSelector    bool
	Search           bool
	Sort             bool
	Continue         bool
	ContinueWithSort bool
	Projection       bool
	SubScopes        bool
	OptimisticLock   bool
	// Watch reports support for the complete snapshot-to-live Watch contract.
	Watch            bool
	DeleteBatch      bool
	PatchBatch       bool
	Transaction      bool
	TTL              bool
	DryRun           bool
	SecondaryIndexes bool
	UniqueIndexes    bool
}

// Pinger reports whether a backend can currently serve requests. Applications
// may use it as one input to readiness without making it part of Store's data
// operation contract.
type Pinger interface {
	Ping(ctx context.Context) error
}

type PingableStore interface {
	Store
	Pinger
}

type StatusStorage interface {
	Update(ctx context.Context, obj Object, opts ...UpdateOption) error
	Patch(ctx context.Context, obj Object, patch Patch, opts ...PatchOption) error
}

type TransactionOptions struct {
	Timeout    time.Duration
	MaxRetries int
	// Isolation is the requested isolation level. An empty value keeps the
	// backend default.
	Isolation IsolationLevel
}

// IsolationLevel names a portable transaction isolation level. Backends map it
// to their own level or reject it as Unsupported.
type IsolationLevel string

const (
	// IsolationDefault keeps the backend's own default level.
	IsolationDefault IsolationLevel = ""
	// IsolationReadCommitted prevents dirty reads.
	IsolationReadCommitted IsolationLevel = "read-committed"
	// IsolationRepeatableRead keeps one snapshot for the whole transaction.
	IsolationRepeatableRead IsolationLevel = "repeatable-read"
	// IsolationSerializable serializes conflicting transactions.
	IsolationSerializable IsolationLevel = "serializable"
)

// TransactionOption configures one Store transaction.
type TransactionOption interface {
	// ApplyToTransaction applies this option to a transaction request.
	ApplyToTransaction(*TransactionOptions)
}

type TransactionStore interface {
	Store
	Transaction(ctx context.Context, fn func(ctx context.Context, store Store) error, opts ...TransactionOption) error
}
