package store

import (
	"time"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/utils/ptr"
	"xiaoshiai.cn/common/selector"
)

// SendInitialEventsOption enables the initial snapshot phase of a Watch.
type SendInitialEventsOption struct{}

// ApplyToWatch enables initial events.
func (SendInitialEventsOption) ApplyToWatch(options *WatchOptions) {
	options.SendInitialEvents = true
}

// WithSendInitialEvents requests an initial Watch. All events before the first
// Bookmark build an authoritative snapshot; later events are live changes.
func WithSendInitialEvents() SendInitialEventsOption {
	return SendInitialEventsOption{}
}

// SubScopesOption includes resources below the current scope.
type SubScopesOption struct{}

// ApplyToList includes subscopes in a List.
func (SubScopesOption) ApplyToList(options *ListOptions) {
	options.IncludeSubScopes = true
}

// ApplyToCount includes subscopes in a Count.
func (SubScopesOption) ApplyToCount(options *CountOptions) {
	options.IncludeSubScopes = true
}

// ApplyToWatch includes subscopes in a Watch.
func (SubScopesOption) ApplyToWatch(options *WatchOptions) {
	options.IncludeSubScopes = true
}

// WithSubScopes includes resources below the current scope.
func WithSubScopes() SubScopesOption {
	return SubScopesOption{}
}

// IDOption restricts a Watch to one object ID.
type IDOption string

// ApplyToWatch sets the watched object ID.
func (option IDOption) ApplyToWatch(options *WatchOptions) {
	options.ID = string(option)
}

// WithID restricts a Watch to id.
func WithID(id string) IDOption {
	return IDOption(id)
}

// ResourceVersionOption selects a read version or write precondition.
type ResourceVersionOption int64

// ApplyToGet sets the requested Get version.
func (option ResourceVersionOption) ApplyToGet(options *GetOptions) {
	options.ResourceVersion = ptr.To(int64(option))
}

// ApplyToList sets or clears the requested List version.
func (option ResourceVersionOption) ApplyToList(options *ListOptions) {
	resourceVersion := int64(option)
	if resourceVersion < 0 {
		options.ResourceVersion = nil
		return
	}
	options.ResourceVersion = ptr.To(resourceVersion)
}

// ApplyToWatch sets the Watch checkpoint.
func (option ResourceVersionOption) ApplyToWatch(options *WatchOptions) {
	options.ResourceVersion = ptr.To(int64(option))
}

// ApplyToDelete sets the Delete resource-version precondition.
func (option ResourceVersionOption) ApplyToDelete(options *DeleteOptions) {
	if options.Preconditions == nil {
		options.Preconditions = &Preconditions{}
	}
	options.Preconditions.ResourceVersion = ptr.To(int64(option))
}

// WithResourceVersion selects a read version, a Watch checkpoint, or a Delete
// precondition according to the target operation. For List, a negative value
// clears the version and reads from the backend.
func WithResourceVersion(resourceVersion int64) ResourceVersionOption {
	return ResourceVersionOption(resourceVersion)
}

// FieldRequirementsOption appends field requirements to supported operations.
type FieldRequirementsOption struct {
	// Requirements are appended in declaration order.
	Requirements Requirements
}

// ApplyToGet appends field requirements to a Get.
func (option FieldRequirementsOption) ApplyToGet(options *GetOptions) {
	options.FieldRequirements = append(options.FieldRequirements, option.Requirements...)
}

// ApplyToList appends field requirements to a List.
func (option FieldRequirementsOption) ApplyToList(options *ListOptions) {
	options.FieldRequirements = append(options.FieldRequirements, option.Requirements...)
}

// ApplyToCount appends field requirements to a Count.
func (option FieldRequirementsOption) ApplyToCount(options *CountOptions) {
	options.FieldRequirements = append(options.FieldRequirements, option.Requirements...)
}

// ApplyToDelete appends field requirements to a Delete.
func (option FieldRequirementsOption) ApplyToDelete(options *DeleteOptions) {
	options.FieldRequirements = append(options.FieldRequirements, option.Requirements...)
}

// ApplyToDeleteBatch appends field requirements to a batch Delete.
func (option FieldRequirementsOption) ApplyToDeleteBatch(options *DeleteBatchOptions) {
	options.FieldRequirements = append(options.FieldRequirements, option.Requirements...)
}

// ApplyToUpdate appends field requirements to an Update.
func (option FieldRequirementsOption) ApplyToUpdate(options *UpdateOptions) {
	options.FieldRequirements = append(options.FieldRequirements, option.Requirements...)
}

// ApplyToPatch appends field requirements to a Patch.
func (option FieldRequirementsOption) ApplyToPatch(options *PatchOptions) {
	options.FieldRequirements = append(options.FieldRequirements, option.Requirements...)
}

// ApplyToPatchBatch appends field requirements to a batch Patch.
func (option FieldRequirementsOption) ApplyToPatchBatch(options *PatchBatchOptions) {
	options.FieldRequirements = append(options.FieldRequirements, option.Requirements...)
}

// ApplyToWatch appends field requirements to a Watch.
func (option FieldRequirementsOption) ApplyToWatch(options *WatchOptions) {
	options.FieldRequirements = append(options.FieldRequirements, option.Requirements...)
}

// WithFieldRequirementsFromSelector converts and appends a field selector.
func WithFieldRequirementsFromSelector(selector fields.Selector) FieldRequirementsOption {
	return FieldRequirementsOption{Requirements: FieldsSelectorToReqirements(selector)}
}

// WithFieldRequirementsFromSet converts and appends exact field matches.
func WithFieldRequirementsFromSet(values map[string]string) FieldRequirementsOption {
	return FieldRequirementsOption{Requirements: selector.RequirementsFromMap(values)}
}

// WithFieldRequirements appends field requirements.
func WithFieldRequirements(requirements ...selector.Requirement) FieldRequirementsOption {
	return FieldRequirementsOption{Requirements: requirements}
}

// LabelRequirementsOption appends label requirements to supported operations.
type LabelRequirementsOption struct {
	// Requirements are appended in declaration order.
	Requirements Requirements
}

// ApplyToGet appends label requirements to a Get.
func (option LabelRequirementsOption) ApplyToGet(options *GetOptions) {
	options.LabelRequirements = append(options.LabelRequirements, option.Requirements...)
}

// ApplyToList appends label requirements to a List.
func (option LabelRequirementsOption) ApplyToList(options *ListOptions) {
	options.LabelRequirements = append(options.LabelRequirements, option.Requirements...)
}

// ApplyToCount appends label requirements to a Count.
func (option LabelRequirementsOption) ApplyToCount(options *CountOptions) {
	options.LabelRequirements = append(options.LabelRequirements, option.Requirements...)
}

// ApplyToDelete appends label requirements to a Delete.
func (option LabelRequirementsOption) ApplyToDelete(options *DeleteOptions) {
	options.LabelRequirements = append(options.LabelRequirements, option.Requirements...)
}

// ApplyToDeleteBatch appends label requirements to a batch Delete.
func (option LabelRequirementsOption) ApplyToDeleteBatch(options *DeleteBatchOptions) {
	options.LabelRequirements = append(options.LabelRequirements, option.Requirements...)
}

// ApplyToUpdate appends label requirements to an Update.
func (option LabelRequirementsOption) ApplyToUpdate(options *UpdateOptions) {
	options.LabelRequirements = append(options.LabelRequirements, option.Requirements...)
}

// ApplyToPatch appends label requirements to a Patch.
func (option LabelRequirementsOption) ApplyToPatch(options *PatchOptions) {
	options.LabelRequirements = append(options.LabelRequirements, option.Requirements...)
}

// ApplyToPatchBatch appends label requirements to a batch Patch.
func (option LabelRequirementsOption) ApplyToPatchBatch(options *PatchBatchOptions) {
	options.LabelRequirements = append(options.LabelRequirements, option.Requirements...)
}

// ApplyToWatch appends label requirements to a Watch.
func (option LabelRequirementsOption) ApplyToWatch(options *WatchOptions) {
	options.LabelRequirements = append(options.LabelRequirements, option.Requirements...)
}

// WithLabelRequirementsFromSet converts and appends exact label matches.
func WithLabelRequirementsFromSet(values map[string]string) LabelRequirementsOption {
	return LabelRequirementsOption{Requirements: selector.RequirementsFromMap(values)}
}

// WithLabelRequirementsFromSelector converts and appends a label selector.
func WithLabelRequirementsFromSelector(selector labels.Selector) LabelRequirementsOption {
	return LabelRequirementsOption{Requirements: LabelsSelectorToReqirements(selector)}
}

// WithLabelRequirements appends label requirements.
func WithLabelRequirements(requirements ...selector.Requirement) LabelRequirementsOption {
	return LabelRequirementsOption{Requirements: requirements}
}

// FieldsOption appends projected fields to supported read operations.
type FieldsOption []string

// ApplyToGet appends projected fields to a Get.
func (option FieldsOption) ApplyToGet(options *GetOptions) {
	options.Fields = append(options.Fields, option...)
}

// ApplyToList appends projected fields to a List.
func (option FieldsOption) ApplyToList(options *ListOptions) {
	options.Fields = append(options.Fields, option...)
}

// WithFields appends projected fields.
func WithFields(fields ...string) FieldsOption {
	return FieldsOption(fields)
}

// PreconditionsOption overlays present Get, Patch or Delete preconditions.
type PreconditionsOption Preconditions

// ApplyToGet overlays non-nil precondition fields.
func (option PreconditionsOption) ApplyToGet(options *GetOptions) {
	if option.UID == nil && option.ResourceVersion == nil {
		return
	}
	if options.Preconditions == nil {
		options.Preconditions = &Preconditions{}
	}
	if option.UID != nil {
		options.Preconditions.UID = option.UID
	}
	if option.ResourceVersion != nil {
		options.Preconditions.ResourceVersion = option.ResourceVersion
	}
}

// ApplyToPatch overlays non-nil precondition fields.
func (option PreconditionsOption) ApplyToPatch(options *PatchOptions) {
	if option.UID == nil && option.ResourceVersion == nil {
		return
	}
	if options.Preconditions == nil {
		options.Preconditions = &Preconditions{}
	}
	if option.UID != nil {
		options.Preconditions.UID = option.UID
	}
	if option.ResourceVersion != nil {
		options.Preconditions.ResourceVersion = option.ResourceVersion
	}
}

// ApplyToDelete overlays non-nil precondition fields.
func (option PreconditionsOption) ApplyToDelete(options *DeleteOptions) {
	if option.UID == nil && option.ResourceVersion == nil {
		return
	}
	if options.Preconditions == nil {
		options.Preconditions = &Preconditions{}
	}
	if option.UID != nil {
		options.Preconditions.UID = option.UID
	}
	if option.ResourceVersion != nil {
		options.Preconditions.ResourceVersion = option.ResourceVersion
	}
}

// WithPreconditions overlays the supplied Get, Patch or Delete preconditions.
func WithPreconditions(preconditions Preconditions) PreconditionsOption {
	return PreconditionsOption(preconditions)
}

// UIDOption sets a Delete UID precondition.
type UIDOption string

// ApplyToDelete sets the UID precondition.
func (option UIDOption) ApplyToDelete(options *DeleteOptions) {
	if options.Preconditions == nil {
		options.Preconditions = &Preconditions{}
	}
	options.Preconditions.UID = ptr.To(string(option))
}

// WithUID restricts deletion to an object with uid.
func WithUID(uid string) UIDOption {
	return UIDOption(uid)
}

// TTLOption sets the lifetime of a created or updated object.
type TTLOption time.Duration

// ApplyToCreate sets the Create TTL.
func (option TTLOption) ApplyToCreate(options *CreateOptions) {
	options.TTL = time.Duration(option)
}

// ApplyToUpdate sets the Update TTL.
func (option TTLOption) ApplyToUpdate(options *UpdateOptions) {
	options.TTL = time.Duration(option)
}

// WithTTL sets the object lifetime.
func WithTTL(ttl time.Duration) TTLOption {
	return TTLOption(ttl)
}

// DryRunOption enables validation without persisting a write.
type DryRunOption struct{}

// ApplyToCreate enables a dry-run Create.
func (DryRunOption) ApplyToCreate(options *CreateOptions) {
	options.DryRun = true
}

// ApplyToDelete enables a dry-run Delete.
func (DryRunOption) ApplyToDelete(options *DeleteOptions) {
	options.DryRun = true
}

// ApplyToDeleteBatch enables a dry-run batch Delete.
func (DryRunOption) ApplyToDeleteBatch(options *DeleteBatchOptions) {
	options.DryRun = true
}

// ApplyToUpdate enables a dry-run Update.
func (DryRunOption) ApplyToUpdate(options *UpdateOptions) {
	options.DryRun = true
}

// ApplyToPatch enables a dry-run Patch.
func (DryRunOption) ApplyToPatch(options *PatchOptions) {
	options.DryRun = true
}

// ApplyToPatchBatch enables a dry-run batch Patch.
func (DryRunOption) ApplyToPatchBatch(options *PatchBatchOptions) {
	options.DryRun = true
}

// WithDryRun enables validation without persistence.
func WithDryRun() DryRunOption {
	return DryRunOption{}
}

// OffsetOption sets the initial query offset.
type OffsetOption int

// ApplyToList sets the skipped object count.
func (option OffsetOption) ApplyToList(options *ListOptions) {
	options.Offset = int(option)
}

// WithOffset skips matching objects on the initial request.
func WithOffset(offset int) OffsetOption {
	return OffsetOption(offset)
}

// ListLimitOption bounds the returned objects.
type ListLimitOption int

// ApplyToList sets the returned-object bound.
func (option ListLimitOption) ApplyToList(options *ListOptions) {
	options.Limit = int(option)
}

// WithLimit bounds the returned objects; zero returns all remaining matches.
func WithLimit(limit int) ListLimitOption {
	return ListLimitOption(limit)
}

// ContinueOption selects a cursor read and sets its batch bound.
type ContinueOption struct {
	// Token is the opaque token returned by the previous batch; empty selects
	// the first batch.
	Token string
	// Limit is the maximum number of returned objects for this batch.
	Limit int
}

// ApplyToList selects the cursor read and sets its bound together.
func (option ContinueOption) ApplyToList(options *ListOptions) {
	options.Continue = option.Token
	options.Limit = option.Limit
}

// WithContinue selects a cursor read of at most limit objects after token. An
// empty token selects the first batch. WithLimit also starts a resumable query.
func WithContinue(token string, limit int) ContinueOption {
	return ContinueOption{Token: token, Limit: limit}
}

// SortOption sets the List sort expression.
type SortOption string

// ApplyToList sets the sort expression.
func (option SortOption) ApplyToList(options *ListOptions) {
	options.Sort = string(option)
}

// WithSort sets the List sort expression.
func WithSort(sort string) SortOption {
	return SortOption(sort)
}

// SearchOption sets the List or Count search text.
type SearchOption string

// ApplyToList sets the search text.
func (option SearchOption) ApplyToList(options *ListOptions) {
	options.Search = string(option)
}

// WithSearch sets the List or Count search text.
func WithSearch(search string) SearchOption {
	return SearchOption(search)
}

// SearchFieldsOption appends fields searched by List or Count.
type SearchFieldsOption []string

// ApplyToList appends search fields.
func (option SearchFieldsOption) ApplyToList(options *ListOptions) {
	options.SearchFields = append(options.SearchFields, option...)
}

// WithSearchFields appends fields searched by List or Count.
func WithSearchFields(fields ...string) SearchFieldsOption {
	return SearchFieldsOption(fields)
}

// PropagationOption sets the Delete propagation policy.
type PropagationOption DeletionPropagation

// ApplyToDelete sets the propagation policy.
func (option PropagationOption) ApplyToDelete(options *DeleteOptions) {
	policy := DeletionPropagation(option)
	options.PropagationPolicy = &policy
}

// WithPropagation sets the Delete propagation policy.
func WithPropagation(policy DeletionPropagation) PropagationOption {
	return PropagationOption(policy)
}

// TimeoutOption sets a transaction timeout.
type TimeoutOption time.Duration

// ApplyToTransaction sets the transaction timeout.
func (option TimeoutOption) ApplyToTransaction(options *TransactionOptions) {
	options.Timeout = time.Duration(option)
}

// WithTimeout sets a transaction timeout.
func WithTimeout(timeout time.Duration) TimeoutOption {
	return TimeoutOption(timeout)
}

// MaxRetriesOption sets the maximum transaction retry count.
type MaxRetriesOption int

// ApplyToTransaction sets the maximum retry count.
func (option MaxRetriesOption) ApplyToTransaction(options *TransactionOptions) {
	options.MaxRetries = int(option)
}

// WithMaxRetries sets the maximum transaction retry count.
func WithMaxRetries(retries int) MaxRetriesOption {
	return MaxRetriesOption(retries)
}

// IsolationLevelOption selects the isolation level of a transaction. A backend
// that cannot honor the requested level returns Unsupported.
type IsolationLevelOption IsolationLevel

// ApplyToTransaction sets the requested isolation level.
func (option IsolationLevelOption) ApplyToTransaction(options *TransactionOptions) {
	options.Isolation = IsolationLevel(option)
}

// WithIsolationLevel selects the isolation level of a transaction. An empty
// level keeps the backend default.
func WithIsolationLevel(level IsolationLevel) IsolationLevelOption {
	return IsolationLevelOption(level)
}

// ApplyGetOptions expands Get options in declaration order.
func ApplyGetOptions(options []GetOption) GetOptions {
	if len(options) == 0 {
		return GetOptions{}
	}
	resolved := GetOptions{}
	for _, option := range options {
		option.ApplyToGet(&resolved)
	}
	return resolved
}

// ApplyListOptions expands List options in declaration order.
func ApplyListOptions(options []ListOption) ListOptions {
	if len(options) == 0 {
		return ListOptions{}
	}
	resolved := ListOptions{}
	for _, option := range options {
		option.ApplyToList(&resolved)
	}
	return resolved
}

// ApplyCountOptions expands Count options in declaration order.
func ApplyCountOptions(options []CountOption) CountOptions {
	if len(options) == 0 {
		return CountOptions{}
	}
	resolved := CountOptions{}
	for _, option := range options {
		option.ApplyToCount(&resolved)
	}
	return resolved
}

// ApplyCreateOptions expands Create options in declaration order.
func ApplyCreateOptions(options []CreateOption) CreateOptions {
	if len(options) == 0 {
		return CreateOptions{}
	}
	resolved := CreateOptions{}
	for _, option := range options {
		option.ApplyToCreate(&resolved)
	}
	return resolved
}

// ApplyDeleteOptions expands Delete options in declaration order.
func ApplyDeleteOptions(options []DeleteOption) DeleteOptions {
	if len(options) == 0 {
		return DeleteOptions{}
	}
	resolved := DeleteOptions{}
	for _, option := range options {
		option.ApplyToDelete(&resolved)
	}
	return resolved
}

// ApplyDeleteBatchOptions expands batch Delete options in declaration order.
func ApplyDeleteBatchOptions(options []DeleteBatchOption) DeleteBatchOptions {
	if len(options) == 0 {
		return DeleteBatchOptions{}
	}
	resolved := DeleteBatchOptions{}
	for _, option := range options {
		option.ApplyToDeleteBatch(&resolved)
	}
	return resolved
}

// ApplyUpdateOptions expands Update options in declaration order.
func ApplyUpdateOptions(options []UpdateOption) UpdateOptions {
	if len(options) == 0 {
		return UpdateOptions{}
	}
	resolved := UpdateOptions{}
	for _, option := range options {
		option.ApplyToUpdate(&resolved)
	}
	return resolved
}

// ApplyPatchOptions expands Patch options in declaration order.
func ApplyPatchOptions(options []PatchOption) PatchOptions {
	if len(options) == 0 {
		return PatchOptions{}
	}
	resolved := PatchOptions{}
	for _, option := range options {
		option.ApplyToPatch(&resolved)
	}
	return resolved
}

// ApplyPatchBatchOptions expands batch Patch options in declaration order.
func ApplyPatchBatchOptions(options []PatchBatchOption) PatchBatchOptions {
	if len(options) == 0 {
		return PatchBatchOptions{}
	}
	resolved := PatchBatchOptions{}
	for _, option := range options {
		option.ApplyToPatchBatch(&resolved)
	}
	return resolved
}

// ApplyWatchOptions expands Watch options in declaration order.
func ApplyWatchOptions(options []WatchOption) WatchOptions {
	if len(options) == 0 {
		return WatchOptions{}
	}
	resolved := WatchOptions{}
	for _, option := range options {
		option.ApplyToWatch(&resolved)
	}
	return resolved
}

// ApplyTransactionOptions expands transaction options in declaration order.
func ApplyTransactionOptions(options []TransactionOption) TransactionOptions {
	if len(options) == 0 {
		return TransactionOptions{}
	}
	resolved := TransactionOptions{}
	for _, option := range options {
		option.ApplyToTransaction(&resolved)
	}
	return resolved
}

// ApplyToCount sets the same search predicate used by List.
func (option SearchOption) ApplyToCount(options *CountOptions) { options.Search = string(option) }

// ApplyToCount selects the fields searched by Count.
func (option SearchFieldsOption) ApplyToCount(options *CountOptions) {
	options.SearchFields = append(options.SearchFields, option...)
}
