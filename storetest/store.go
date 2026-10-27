// Package storetest provides a conformance suite for Store implementations.
package storetest

import (
	"context"
	stderrors "errors"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	commonerrors "xiaoshiai.cn/common/errors"
	commonstore "xiaoshiai.cn/store"
	"xiaoshiai.cn/store/selector"
)

// Object is the resource used by the conformance suite.
type Object struct {
	commonstore.ObjectMeta `json:",inline"`
	Value                  string       `json:"value,omitempty"`
	Rank                   int          `json:"rank,omitempty"`
	Tenant                 string       `json:"tenant,omitempty"`
	Status                 ObjectStatus `json:"status"`
}

type schemaMutationObject struct {
	commonstore.ObjectMeta `json:",inline"`
}

// ObjectStatus is the status subresource used by the conformance suite.
type ObjectStatus struct {
	Phase string `json:"phase,omitempty"`
}

// Factory creates an isolated Store for one conformance subtest.
type Factory func(t testing.TB, schema *commonstore.Schema) (commonstore.Store, error)

// Fixture connects one Store implementation to the conformance suite.
// Capabilities and request support are independent expectations, not values
// inferred from the tested Store or its responses. New isolates each subtest,
// including persisted data.
type Fixture struct {
	// ResourceWideIDs requires ID uniqueness across every scope of a resource.
	ResourceWideIDs bool
	// MergePatchOnly declares that JSON Patch must be rejected without effects.
	MergePatchOnly bool
	// JSONPatchSetOnly declares that JSON Patch supports only object-field add,
	// replace and remove. test, copy, move, array paths and lifecycle fields must
	// be rejected with Unsupported.
	JSONPatchSetOnly bool
	// BatchFinalizerRemovalOnly excludes assigning nonempty finalizers in a batch.
	BatchFinalizerRemovalOnly bool

	New          Factory
	Capabilities commonstore.Capabilities

	// These options select supported requests in the suite. Unsupported requests
	// must be rejected; they are not skipped. Index options configure the schema.
	LabelSelector    bool
	FieldSelector    bool
	Search           bool
	Sort             bool
	Continue         bool
	Projection       bool
	SubScopes        bool
	DeleteBatch      bool
	PatchBatch       bool
	Transaction      bool
	TTL              bool
	DryRun           bool
	SecondaryIndexes bool
	UniqueIndexes    bool
}

// Run executes the shared Store contract against fixture.
func Run(t *testing.T, fixture Fixture) {
	t.Helper()
	runIdentity(t, fixture)
	update := func(ctx context.Context, s commonstore.Store, object commonstore.Object) error {
		if !fixture.Capabilities.OptimisticLock {
			object.SetResourceVersion(0)
		}
		return s.Update(ctx, object)
	}
	updateStatus := func(ctx context.Context, s commonstore.Store, object commonstore.Object) error {
		if !fixture.Capabilities.OptimisticLock {
			object.SetResourceVersion(0)
		}
		return s.Status().
			Update(ctx, object)
	}
	runWriteOptions(t, fixture)
	runFinalizers(t, fixture)
	runMutationConditions(t, fixture)
	if !fixture.Capabilities.OptimisticLock {
		for _, operation := range []string{"update", "status update", "patch", "status patch", "merge patch", "status merge patch", "JSON patch", "status JSON patch", "delete"} {
			t.Run("unsupported version condition/"+operation, func(t *testing.T) {
				storage := NewStore(t, fixture)
				object := &Object{Value: "before", Status: ObjectStatus{Phase: "pending"}}
				if err := storage.Create(t.Context(), object); err != nil {
					t.Fatal(err)
				}
				before := &Object{}
				if err := storage.Get(t.Context(), object.ID, before); err != nil {
					t.Fatal(err)
				}
				version := before.ResourceVersion
				condition := commonstore.WithPreconditions(commonstore.Preconditions{ResourceVersion: &version})
				patch := commonstore.MapMergePatch{"value": "changed", "status": map[string]any{"phase": "changed"}}
				merge := commonstore.MapMergePatch{"resourceVersion": version, "value": "changed", "status": map[string]any{"phase": "changed"}}
				jsonPatch := commonstore.RawPatch(commonstore.PatchTypeJSONPatch, fmt.Appendf(nil, `[{"op":"test","path":"/resourceVersion","value":%d},{"op":"replace","path":"/value","value":"changed"}]`, version))
				var err error
				switch operation {
				case "update":
					object.Value = "changed"
					err = storage.Update(t.Context(), object)
				case "status update":
					object.Status.Phase = "changed"
					err = storage.Status().
						Update(t.Context(), object)
				case "patch":
					err = storage.Patch(t.Context(), object, patch, condition)
				case "status patch":
					err = storage.Status().
						Patch(t.Context(), object, patch, condition)
				case "merge patch":
					err = storage.Patch(t.Context(), object, merge)
				case "status merge patch":
					err = storage.Status().
						Patch(t.Context(), object, merge)
				case "JSON patch":
					err = storage.Patch(t.Context(), object, jsonPatch)
				case "status JSON patch":
					err = storage.Status().
						Patch(t.Context(), object, jsonPatch)
				case "delete":
					err = storage.Delete(t.Context(), object, commonstore.WithResourceVersion(version))
				}
				if !commonerrors.IsUnsupported(err) {
					t.Errorf("conditional write error = %v, want Unsupported", err)
				}
				got := &Object{}
				if err := storage.Get(t.Context(), object.ID, got); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, before) {
					t.Fatalf("unsupported condition changed object: %#v", got)
				}
			})
		}
	}
	t.Run("capabilities", func(t *testing.T) {
		storage := NewStore(t, fixture)
		if got := storage.Capabilities(); !reflect.DeepEqual(got, fixture.Capabilities) {
			t.Fatalf("Capabilities() = %#v, want %#v", got, fixture.Capabilities)
		}
		scoped := storage.Scope(commonstore.Scope{Resource: "tenants", Name: "capabilities"})
		if got := scoped.Capabilities(); !reflect.DeepEqual(got, fixture.Capabilities) {
			t.Fatalf("scoped Capabilities() = %#v, want %#v", got, fixture.Capabilities)
		}
	})
	t.Run("schema is an independent scope-stable snapshot", func(t *testing.T) {
		storage := NewStore(t, fixture)
		assertSchemaSnapshot(t, storage)
		assertSchemaSnapshot(t, storage.Scope(commonstore.Scope{Resource: "tenants", Name: "tenant-a"}))
	})
	t.Run("watch sends initial snapshot and closes after stop", func(t *testing.T) {
		if !fixture.Capabilities.Watch {
			t.Skip("watch is not declared")
		}
		storage := NewStore(t, fixture)
		existing := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "existing"}, Value: "initial"}
		if err := storage.Create(t.Context(), existing); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		watcher := OpenWatcher(t, func() (commonstore.Watcher, error) {
			return storage.Watch(
				t.Context(),
				&commonstore.List[Object]{Resource: "storetests"},
				commonstore.WithSendInitialEvents(),
			)
		})
		initial := NextWatchEvent(t, watcher)
		AssertWatchEvent(t, initial, commonstore.WatchEventCreate, "existing")
		bookmark := NextWatchEvent(t, watcher)
		AssertWatchEvent(t, bookmark, commonstore.WatchEventBookmark, "")
		watcher.Stop()
		watcher.Stop()
		AssertWatcherClosed(t, watcher)
	})
	t.Run("watch reports selector membership transitions", func(t *testing.T) {
		if !fixture.Capabilities.Watch {
			t.Skip("watch is not declared")
		}
		storage := NewStore(t, fixture)
		watcher := OpenWatcher(t, func() (commonstore.Watcher, error) {
			return storage.Watch(
				t.Context(),
				&commonstore.List[Object]{Resource: "storetests"},
				commonstore.WithSendInitialEvents(),
				commonstore.WithFieldRequirements(selector.RequirementEqual("rank", 8)),
			)
		})
		defer watcher.Stop()
		AssertWatchEvent(t, NextWatchEvent(t, watcher), commonstore.WatchEventBookmark, "")

		object := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "transition"}, Value: "before", Rank: 7}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		object.Rank = 8
		if err := update(t.Context(), storage, object); err != nil {
			t.Fatalf("entering Update() error = %v", err)
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		AssertWatchEvent(t, NextObjectWatchEvent(t, watcher), commonstore.WatchEventCreate, "transition")

		object.Value = "matching"
		if err := update(t.Context(), storage, object); err != nil {
			t.Fatalf("matching Update() error = %v", err)
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		AssertWatchEvent(t, NextObjectWatchEvent(t, watcher), commonstore.WatchEventUpdate, "transition")

		object.Rank = 9
		if err := update(t.Context(), storage, object); err != nil {
			t.Fatalf("leaving Update() error = %v", err)
		}
		deleted := NextObjectWatchEvent(t, watcher)
		AssertWatchEvent(t, deleted, commonstore.WatchEventDelete, "transition")
		previous := deleted.Object.(*Object)
		if previous.Value != "matching" || previous.Rank != 8 {
			t.Fatalf("Delete transition object = %#v, want complete previous object", previous)
		}
	})
	t.Run("watch reports comparison selector membership transitions", func(t *testing.T) {
		if !fixture.Capabilities.Watch || !fixture.FieldSelector {
			t.Skip("watch with field selectors is not declared")
		}
		storage := NewStore(t, fixture)
		watcher := OpenWatcher(t, func() (commonstore.Watcher, error) {
			return storage.Watch(
				t.Context(),
				&commonstore.List[Object]{Resource: "storetests"},
				commonstore.WithSendInitialEvents(),
				commonstore.WithFieldRequirements(selector.NewRequirement("rank", selector.GreaterThan, 7)),
			)
		})
		defer watcher.Stop()
		AssertWatchEvent(t, NextWatchEvent(t, watcher), commonstore.WatchEventBookmark, "")

		object := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "comparison-transition"}, Rank: 7}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		object.Rank = 8
		if err := update(t.Context(), storage, object); err != nil {
			t.Fatalf("entering Update() error = %v", err)
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		created := NextObjectWatchEvent(t, watcher)
		AssertWatchEvent(t, created, commonstore.WatchEventCreate, "comparison-transition")
		if entered := created.Object.(*Object); entered.Rank != 8 {
			t.Fatalf("Create transition object rank = %d, want 8", entered.Rank)
		}

		object.Rank = 9
		if err := update(t.Context(), storage, object); err != nil {
			t.Fatalf("matching Update() error = %v", err)
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		updated := NextObjectWatchEvent(t, watcher)
		AssertWatchEvent(t, updated, commonstore.WatchEventUpdate, "comparison-transition")
		if current := updated.Object.(*Object); current.Rank != 9 {
			t.Fatalf("Update transition object rank = %d, want 9", current.Rank)
		}

		object.Rank = 7
		if err := update(t.Context(), storage, object); err != nil {
			t.Fatalf("leaving Update() error = %v", err)
		}
		deleted := NextObjectWatchEvent(t, watcher)
		AssertWatchEvent(t, deleted, commonstore.WatchEventDelete, "comparison-transition")
		if previous := deleted.Object.(*Object); previous.Rank != 9 {
			t.Fatalf("Delete transition object rank = %d, want 9", previous.Rank)
		}
	})
	t.Run("field comparisons use typed values before pagination", func(t *testing.T) {
		if !fixture.FieldSelector {
			t.Skip("field selectors are not declared")
		}
		storage := NewStore(t, fixture)
		objects := []*Object{
			{ObjectMeta: commonstore.ObjectMeta{ID: "missing"}},
			{ObjectMeta: commonstore.ObjectMeta{ID: "low"}, Rank: 2},
			{ObjectMeta: commonstore.ObjectMeta{ID: "middle"}, Rank: 3},
			{ObjectMeta: commonstore.ObjectMeta{ID: "high"}, Rank: 10},
		}
		for _, object := range objects {
			if err := storage.Create(t.Context(), object); err != nil {
				t.Fatalf("Create(%q) error = %v", object.ID, err)
			}
		}

		tests := []struct {
			name     string
			operator selector.Operator
			value    int
			wantIDs  []string
		}{
			{name: "greater than", operator: selector.GreaterThan, value: 2, wantIDs: []string{"high", "middle"}},
			{name: "less than", operator: selector.LessThan, value: 3, wantIDs: []string{"low"}},
			{name: "greater than or equal", operator: selector.GreaterThanOrEqual, value: 3, wantIDs: []string{"high", "middle"}},
			{name: "less than or equal", operator: selector.LessThanOrEqual, value: 2, wantIDs: []string{"low"}},
		}
		for _, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				list := &commonstore.List[Object]{Resource: "storetests"}
				if err := storage.List(t.Context(), list, commonstore.WithFieldRequirements(selector.NewRequirement("rank", test.operator, test.value))); err != nil {
					t.Fatalf("List() error = %v", err)
				}
				ids := ObjectIDs(list.Items)
				sort.Strings(ids)
				if !reflect.DeepEqual(ids, test.wantIDs) {
					t.Fatalf("List() IDs = %v, want %v", ids, test.wantIDs)
				}
			})
		}

		greaterThanTwo := commonstore.WithFieldRequirements(
			selector.NewRequirement("rank", selector.GreaterThan, 2),
		)
		count, err := storage.Count(t.Context(), &Object{}, greaterThanTwo)
		if err != nil {
			t.Fatalf("Count() error = %v", err)
		}
		if count != 2 {
			t.Fatalf("Count() = %d, want 2", count)
		}
		{
			list := &commonstore.List[Object]{Resource: "storetests"}
			if err := storage.List(t.Context(), list, greaterThanTwo, commonstore.WithSort("id+"), commonstore.WithLimit(1)); err != nil {
				t.Fatalf("limited List() error = %v", err)
			}
			if ids := ObjectIDs(list.Items); !reflect.DeepEqual(ids, []string{"high"}) {
				t.Fatalf("limited List() IDs = %v, want [high]", ids)
			}

		}
	})
	t.Run("watch checkpoint resumes or expires", func(t *testing.T) {
		if !fixture.Capabilities.Watch {
			t.Skip("watch is not declared")
		}
		storage := NewStore(t, fixture)
		existing := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "before"}, Value: "before"}
		if err := storage.Create(t.Context(), existing); err != nil {
			t.Fatalf("Create(before) error = %v", err)
		}
		initial := OpenWatcher(t, func() (commonstore.Watcher, error) {
			return storage.Watch(
				t.Context(),
				&commonstore.List[Object]{Resource: "storetests"},
				commonstore.WithSendInitialEvents(),
			)
		})
		AssertWatchEvent(t, NextWatchEvent(t, initial), commonstore.WatchEventCreate, "before")
		bookmark := NextWatchEvent(t, initial)
		AssertWatchEvent(t, bookmark, commonstore.WatchEventBookmark, "")
		initial.Stop()
		AssertWatcherClosed(t, initial)

		if bookmark.ResourceVersion == 0 {
			watcher, err := storage.Watch(
				t.Context(),
				&commonstore.List[Object]{Resource: "storetests"},
				commonstore.WithResourceVersion(1),
			)
			if watcher != nil {
				watcher.Stop()
			}
			if !commonerrors.IsResourceExpired(err) {
				t.Fatalf("Watch() error = %v, want ResourceExpired when Bookmark has no checkpoint", err)
			}
			return
		}

		resumed := OpenWatcher(t, func() (commonstore.Watcher, error) {
			return storage.Watch(
				t.Context(),
				&commonstore.List[Object]{Resource: "storetests"},
				commonstore.WithResourceVersion(bookmark.ResourceVersion),
			)
		})
		defer resumed.Stop()
		after := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "after"}, Value: "after"}
		if err := storage.Create(t.Context(), after); err != nil {
			t.Fatalf("Create(after) error = %v", err)
		}
		AssertWatchEvent(t, NextObjectWatchEvent(t, resumed), commonstore.WatchEventCreate, "after")
	})
	t.Run("create generates server metadata", func(t *testing.T) {
		storage := NewStore(t, fixture)
		object := &Object{
			ObjectMeta: commonstore.ObjectMeta{
				UID:               "caller-uid",
				ResourceVersion:   99,
				Generation:        99,
				CreationTimestamp: commonstore.ObjectMeta{}.CreationTimestamp,
			},
			Value: "created",
		}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		if _, err := uuid.Parse(object.ID); err != nil {
			t.Fatalf("Create() ID = %q, want UUID: %v", object.ID, err)
		}
		if _, err := uuid.Parse(object.UID); err != nil {
			t.Fatalf("Create() UID = %q, want UUID: %v", object.UID, err)
		}
		if object.ResourceVersion <= 0 {
			t.Fatalf("Create() ResourceVersion = %d, want positive", object.ResourceVersion)
		}
		if object.Generation != 1 {
			t.Fatalf("Create() Generation = %d, want 1", object.Generation)
		}
		if object.CreationTimestamp.IsZero() {
			t.Fatal("Create() CreationTimestamp is zero")
		}
		if object.Resource != "storetests" {
			t.Fatalf("Create() Resource = %q, want storetests", object.Resource)
		}

		got := &Object{}
		if err := storage.Get(t.Context(), object.ID, got); err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if got.ID != object.ID || got.Value != "created" {
			t.Fatalf("Get() = %#v, want created object %q", got, object.ID)
		}
	})

	t.Run("patch enforces exact preconditions", func(t *testing.T) {
		if !fixture.Capabilities.OptimisticLock {
			t.Skip("conditional writes are checked by the unsupported-version contract")
		}
		storage := NewStore(t, fixture)
		object := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "conditional-patch"}, Value: "original"}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatal(err)
		}
		version := object.ResourceVersion
		patch := commonstore.MapMergePatch{"value": "updated"}
		target := &Object{ObjectMeta: commonstore.ObjectMeta{ID: object.ID}}
		if err := storage.Patch(t.Context(), target, patch, commonstore.WithPreconditions(commonstore.Preconditions{ResourceVersion: &version})); err != nil {
			t.Fatal(err)
		}
		if err := storage.Get(t.Context(), target.ID, target); err != nil {
			t.Fatal(err)
		}
		if target.Value != "updated" || target.ResourceVersion <= version {
			t.Fatalf("patch result: %#v", target)
		}
		for _, expected := range []int64{version, 0} {
			if err := storage.Patch(t.Context(), target, commonstore.MapMergePatch{"value": "rejected"}, commonstore.WithPreconditions(commonstore.Preconditions{ResourceVersion: &expected})); !commonerrors.IsConflict(err) {
				t.Fatalf("stale/zero Patch = %v", err)
			}
		}
		got := &Object{}
		if err := storage.Get(t.Context(), object.ID, got); err != nil {
			t.Fatal(err)
		}
		if got.Value != "updated" {
			t.Fatalf("failed patch persisted: %#v", got)
		}
		uid := "other"
		if err := storage.Status().
			Patch(t.Context(), got, commonstore.MapMergePatch{"status": map[string]any{"phase": "invalid"}}, commonstore.WithPreconditions(commonstore.Preconditions{UID: &uid})); !commonerrors.IsConflict(err) {
			t.Fatalf("wrong identity Status Patch = %v", err)
		}
	})
	t.Run("get enforces exact preconditions", func(t *testing.T) {
		storage := NewStore(t, fixture)
		object := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "observed"}, Value: "original"}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatal(err)
		}
		observedVersion, observedUID := object.ResourceVersion, object.UID
		condition := commonstore.WithPreconditions(commonstore.Preconditions{UID: &observedUID, ResourceVersion: &observedVersion})
		if err := storage.Get(t.Context(), object.ID, &Object{}, condition); err != nil {
			t.Fatal(err)
		}
		object.Value = "updated"
		if err := update(t.Context(), storage, object); err != nil {
			t.Fatal(err)
		}
		if err := storage.Get(t.Context(), object.ID, &Object{}, condition); !commonerrors.IsConflict(err) {
			t.Fatalf("stale Get = %v, want Conflict", err)
		}
		if fixture.Projection {
			if err := storage.Get(t.Context(), object.ID, &Object{}, condition, commonstore.WithFields("value")); !commonerrors.IsConflict(err) {
				t.Fatalf("projected stale Get = %v, want Conflict", err)
			}
			if err := storage.Get(t.Context(), object.ID, object); err != nil {
				t.Fatal(err)
			}
			currentVersion := object.ResourceVersion
			if err := storage.Get(t.Context(), object.ID, &Object{}, commonstore.WithPreconditions(commonstore.Preconditions{UID: &observedUID, ResourceVersion: &currentVersion}), commonstore.WithFields("value")); err != nil {
				t.Fatalf("projected current Get = %v", err)
			}
		}
		if err := storage.Get(t.Context(), "missing", &Object{}, condition); !commonerrors.IsNotFound(err) {
			t.Fatalf("missing Get = %v, want NotFound", err)
		}
		if err := storage.Get(t.Context(), object.ID, &Object{}, condition, commonstore.WithFieldRequirements(selector.RequirementEqual("value", "other"))); !commonerrors.IsNotFound(err) {
			t.Fatalf("unselected Get = %v, want NotFound", err)
		}
		zero := int64(0)
		empty := ""
		for _, preconditions := range []commonstore.Preconditions{
			{ResourceVersion: &zero}, {UID: &empty},
		} {
			if err := storage.Get(t.Context(), object.ID, &Object{}, commonstore.WithPreconditions(preconditions)); !commonerrors.IsConflict(err) {
				t.Fatalf("explicit zero/empty condition = %v, want Conflict", err)
			}
		}
		if err := storage.Get(t.Context(), object.ID, &Object{}); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("create preserves explicit ID", func(t *testing.T) {
		storage := NewStore(t, fixture)
		object := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "explicit"}}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		if object.ID != "explicit" {
			t.Fatalf("Create() ID = %q, want explicit", object.ID)
		}
	})
	t.Run("metadata maps round trip", func(t *testing.T) {
		storage := NewStore(t, fixture)
		object := &Object{ObjectMeta: commonstore.ObjectMeta{
			Labels: map[string]string{
				"example.com/team": "platform",
				"owner.name":       "alice",
				"$owner":           "operations",
			},
			Annotations: map[string]string{
				"example.com/note": "kept",
				"audit.field":      "value",
			},
		}}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		got := &Object{}
		if err := storage.Get(t.Context(), object.ID, got); err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if !reflect.DeepEqual(got.Labels, object.Labels) || !reflect.DeepEqual(got.Annotations, object.Annotations) {
			t.Fatalf("metadata maps = (%v, %v), want (%v, %v)", got.Labels, got.Annotations, object.Labels, object.Annotations)
		}
	})
	t.Run("update owns versions and preserves status", func(t *testing.T) {
		storage := NewStore(t, fixture)
		object := &Object{Value: "one", Status: ObjectStatus{Phase: "current"}}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		uid := object.UID
		created := object.CreationTimestamp
		previousVersion := object.ResourceVersion
		object.Value = "two"
		object.Status.Phase = "must-not-change"
		if err := update(t.Context(), storage, object); err != nil {
			t.Fatalf("Update() error = %v", err)
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		if object.Value != "two" || object.Status.Phase != "current" {
			t.Fatalf("Update() = %#v, want value two and preserved status", object)
		}
		if object.Generation != 2 || object.ResourceVersion <= previousVersion {
			t.Fatalf("Update() generation/version = (%d, %d), want generation 2 and version > %d", object.Generation, object.ResourceVersion, previousVersion)
		}
		if object.UID != uid || !object.CreationTimestamp.Equal(&created) {
			t.Fatalf("Update() changed immutable identity metadata: %#v", object.ObjectMeta)
		}

		generation := object.Generation
		object.Labels = map[string]string{"example.com/team": "platform"}
		if err := update(t.Context(), storage, object); err != nil {
			t.Fatalf("metadata Update() error = %v", err)
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		if object.Generation != generation {
			t.Fatalf("metadata Update() Generation = %d, want %d", object.Generation, generation)
		}
	})
	t.Run("status update only changes status", func(t *testing.T) {
		storage := NewStore(t, fixture)
		object := &Object{
			ObjectMeta: commonstore.ObjectMeta{Labels: map[string]string{"team": "blue"}},
			Value:      "current",
		}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		generation := object.Generation
		version := object.ResourceVersion
		object.Value = "must-not-change"
		object.Labels = map[string]string{"must": "not-change"}
		object.Status.Phase = "ready"
		if err := updateStatus(t.Context(), storage, object); err != nil {
			t.Fatalf("Status().Update() error = %v", err)
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		if object.Value != "current" || !reflect.DeepEqual(object.Labels, map[string]string{"team": "blue"}) || object.Status.Phase != "ready" {
			t.Fatalf("Status().Update() = %#v", object)
		}
		if object.Generation != generation || object.ResourceVersion <= version {
			t.Fatalf("Status().Update() generation/version = (%d, %d)", object.Generation, object.ResourceVersion)
		}
	})
	t.Run("delete honors finalizers", func(t *testing.T) {
		if !fixture.Capabilities.Finalizers {
			t.Skip("finalizers unsupported")
		}
		storage := NewStore(t, fixture)
		object := &Object{ObjectMeta: commonstore.ObjectMeta{Finalizers: []string{"example.com/cleanup"}}}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		if err := storage.Delete(t.Context(), object); err != nil {
			t.Fatalf("Delete() error = %v", err)
		}
		terminating := &Object{}
		if err := storage.Get(t.Context(), object.ID, terminating); err != nil {
			t.Fatalf("Get() terminating object error = %v", err)
		}
		if terminating.DeletionTimestamp == nil {
			t.Fatal("Delete() did not set DeletionTimestamp")
		}
		terminating.Finalizers = nil
		if err := update(t.Context(), storage, terminating); err != nil {
			t.Fatalf("clear finalizers Update() error = %v", err)
		}
		if err := storage.Get(t.Context(), object.ID, &Object{}); !commonerrors.IsNotFound(err) {
			t.Fatalf("Get() after finalizer removal error = %v, want NotFound", err)
		}
	})
	t.Run("delete honors identity and version preconditions", func(t *testing.T) {
		storage := NewStore(t, fixture)
		object := &Object{Value: "current"}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		if err := storage.Delete(t.Context(), object, commonstore.WithUID("stale")); !commonerrors.IsConflict(err) {
			t.Fatalf("stale UID Delete() error = %v, want Conflict", err)
		}
		if fixture.Capabilities.OptimisticLock {
			if err := storage.Delete(t.Context(), object, commonstore.WithResourceVersion(object.ResourceVersion+1)); !commonerrors.IsConflict(err) {
				t.Fatalf("stale ResourceVersion Delete() error = %v, want Conflict", err)
			}
		}
		if err := storage.Delete(
			t.Context(), object,
			commonstore.WithUID("stale"),
			commonstore.WithLabelRequirements(selector.RequirementEqual("team", "red")),
		); !commonerrors.IsConflict(err) {
			t.Fatalf("precondition ordering Delete() error = %v, want Conflict", err)
		}
		if err := storage.Delete(
			t.Context(), object,
			commonstore.WithUID(object.UID),
			commonstore.WithLabelRequirements(selector.RequirementEqual("team", "red")),
		); !commonerrors.IsConflict(err) {
			t.Fatalf("requirement mismatch Delete() error = %v, want Conflict", err)
		}
		deleteOptions := []commonstore.DeleteOption{commonstore.WithUID(object.UID)}
		if fixture.Capabilities.OptimisticLock {
			deleteOptions = append(deleteOptions, commonstore.WithResourceVersion(object.ResourceVersion))
		}
		if err := storage.Delete(t.Context(), object, deleteOptions...); err != nil {
			t.Fatalf("conditional Delete() error = %v", err)
		}
	})
	t.Run("patch follows update boundaries", func(t *testing.T) {
		storage := NewStore(t, fixture)
		object := &Object{Value: "one", Status: ObjectStatus{Phase: "pending"}}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		if err := storage.Patch(t.Context(), object, commonstore.MapMergePatch{
			"value":  "two",
			"status": map[string]any{"phase": "must-not-change"},
		}); err != nil {
			t.Fatalf("Patch() error = %v", err)
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		if object.Value != "two" || object.Status.Phase != "pending" || object.Generation != 2 {
			t.Fatalf("Patch() = %#v", object)
		}
		generation := object.Generation
		if err := storage.Status().
			Patch(t.Context(), object, commonstore.MapMergePatch{
				"value":  "must-not-change",
				"status": map[string]any{"phase": "ready"},
			}); err != nil {
			t.Fatalf("Status().Patch() error = %v", err)
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		if object.Value != "two" || object.Status.Phase != "ready" || object.Generation != generation {
			t.Fatalf("Status().Patch() = %#v", object)
		}
	})
	t.Run("merge patch rejects stale resource version", func(t *testing.T) {
		if !fixture.Capabilities.OptimisticLock {
			t.Skip("conditional writes are checked by the unsupported-version contract")
		}
		storage := NewStore(t, fixture)
		object := &Object{Value: "one"}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		staleVersion := object.ResourceVersion
		object.Value = "two"
		if err := update(t.Context(), storage, object); err != nil {
			t.Fatalf("Update() error = %v", err)
		}
		err := storage.Patch(t.Context(), object, commonstore.MapMergePatch{
			"resourceVersion": staleVersion,
			"value":           "stale",
		})
		if !commonerrors.IsConflict(err) {
			t.Fatalf("stale Patch() error = %v, want Conflict", err)
		}
		got := &Object{}
		if err := storage.Get(t.Context(), object.ID, got); err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if got.Value != "two" {
			t.Fatalf("stale Patch() stored value = %q, want two", got.Value)
		}
	})
	t.Run("merge patch accepts current resource version", func(t *testing.T) {
		if !fixture.Capabilities.OptimisticLock {
			t.Skip("conditional writes are checked by the unsupported-version contract")
		}
		storage := NewStore(t, fixture)
		object := &Object{Value: "one"}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		expectedVersion := object.ResourceVersion
		if err := storage.Patch(t.Context(), object, commonstore.MapMergePatch{
			"resourceVersion": expectedVersion,
			"value":           "two",
		}); err != nil {
			t.Fatalf("Patch() error = %v", err)
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		if object.Value != "two" || object.ResourceVersion <= expectedVersion {
			t.Fatalf("Patch() result = %#v", object)
		}
	})
	t.Run("merge patch without resource version uses latest object", func(t *testing.T) {
		storage := NewStore(t, fixture)
		object := &Object{Value: "one", Rank: 1}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		stale := &Object{}
		if err := commonstore.CopyObject(object, stale); err != nil {
			t.Fatalf("CopyObject() error = %v", err)
		}
		object.Rank = 2
		if err := update(t.Context(), storage, object); err != nil {
			t.Fatalf("Update() error = %v", err)
		}
		if err := storage.Patch(t.Context(), stale, commonstore.MapMergePatch{"value": "two"}); err != nil {
			t.Fatalf("Patch() error = %v", err)
		}
		if err := storage.Get(t.Context(), stale.ID, stale); err != nil {
			t.Fatal(err)
		}
		if stale.Value != "two" || stale.Rank != 2 {
			t.Fatalf("Patch() result = %#v, want latest rank and patched value", stale)
		}
	})
	t.Run("concurrent patches apply to the latest object", func(t *testing.T) {
		storage := NewStore(t, fixture)
		object := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "latest"}, Value: "start", Rank: 1}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		const writers = 8
		errs := make([]error, writers)
		var waiting sync.WaitGroup
		for index := range writers {
			waiting.Add(1)
			go func(index int) {
				defer waiting.Done()
				patched := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "latest"}}
				errs[index] = storage.Patch(t.Context(), patched, commonstore.MapMergePatch{
					"value": fmt.Sprintf("writer-%d", index),
				})
			}(index)
		}
		waiting.Wait()
		for index, err := range errs {
			if err != nil {
				t.Fatalf("concurrent writer %d Patch() error = %v, want no conflict", index, err)
			}
		}
		stored := &Object{}
		if err := storage.Get(t.Context(), "latest", stored); err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if stored.Rank != 1 {
			t.Fatalf("concurrent patch changed an unrelated field: %#v", stored)
		}
		if !strings.HasPrefix(stored.Value, "writer-") {
			t.Fatalf("value = %q, want one writer's value", stored.Value)
		}
	})
	t.Run("JSON patch tests current resource version", func(t *testing.T) {
		if !fixture.Capabilities.OptimisticLock {
			t.Skip("conditional writes are checked by the unsupported-version contract")
		}
		storage := NewStore(t, fixture)
		object := &Object{Value: "one"}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		patch := commonstore.RawPatch(commonstore.PatchTypeJSONPatch, []byte(fmt.Sprintf(`[
            {"op":"test","path":"/resourceVersion","value":%d},
            {"op":"replace","path":"/value","value":"two"}
        ]`, object.ResourceVersion)))
		if fixture.MergePatchOnly || fixture.JSONPatchSetOnly {
			rejectPatch(t, storage, object, patch)
			return
		}
		if err := storage.Patch(t.Context(), object, patch); err != nil {
			t.Fatalf("Patch() error = %v", err)
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		if object.Value != "two" {
			t.Fatalf("Patch() value = %q, want two", object.Value)
		}
	})
	t.Run("JSON patch rejects stale resource version test", func(t *testing.T) {
		if !fixture.Capabilities.OptimisticLock {
			t.Skip("conditional writes are checked by the unsupported-version contract")
		}
		storage := NewStore(t, fixture)
		object := &Object{Value: "one"}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		staleVersion := object.ResourceVersion
		object.Value = "two"
		if err := update(t.Context(), storage, object); err != nil {
			t.Fatalf("Update() error = %v", err)
		}
		patch := commonstore.RawPatch(commonstore.PatchTypeJSONPatch, []byte(fmt.Sprintf(`[
            {"op":"test","path":"/resourceVersion","value":%d},
            {"op":"replace","path":"/value","value":"stale"}
        ]`, staleVersion)))
		if fixture.MergePatchOnly || fixture.JSONPatchSetOnly {
			rejectPatch(t, storage, object, patch)
			return
		}
		err := storage.Patch(t.Context(), object, patch)
		if !commonerrors.IsCode(err, http.StatusUnprocessableEntity) {
			t.Fatalf("stale Patch() error = %v, want 422", err)
		}
		got := &Object{}
		if err := storage.Get(t.Context(), object.ID, got); err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if got.Value != "two" {
			t.Fatalf("stale Patch() stored value = %q, want two", got.Value)
		}
	})
	t.Run("JSON patch replaces object fields", func(t *testing.T) {
		storage := NewStore(t, fixture)
		object := &Object{Value: "one", Rank: 1, Status: ObjectStatus{Phase: "pending"}}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		patch := commonstore.RawPatch(commonstore.PatchTypeJSONPatch, []byte(`[
			{"op":"replace","path":"/value","value":"two"},
			{"op":"add","path":"/rank","value":2}
		]`))
		if fixture.MergePatchOnly {
			rejectPatch(t, storage, object, patch)
			return
		}
		if err := storage.Patch(t.Context(), object, patch); err != nil {
			t.Fatalf("Patch() error = %v", err)
		}
		stored := &Object{}
		if err := storage.Get(t.Context(), object.ID, stored); err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if stored.Value != "two" || stored.Rank != 2 {
			t.Fatalf("JSON patch result = %#v", stored)
		}
		remove := commonstore.RawPatch(commonstore.PatchTypeJSONPatch, []byte(`[{"op":"remove","path":"/rank"}]`))
		if err := storage.Patch(t.Context(), object, remove); err != nil {
			t.Fatalf("JSON patch remove error = %v", err)
		}
		if err := storage.Get(t.Context(), object.ID, stored); err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if stored.Rank != 0 {
			t.Fatalf("JSON patch remove result = %#v", stored)
		}
		statusPatch := commonstore.RawPatch(commonstore.PatchTypeJSONPatch, []byte(`[{"op":"replace","path":"/status/phase","value":"ready"}]`))
		if err := storage.Status().Patch(t.Context(), object, statusPatch); err != nil {
			t.Fatalf("status JSON patch error = %v", err)
		}
		if err := storage.Get(t.Context(), object.ID, stored); err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if stored.Status.Phase != "ready" {
			t.Fatalf("status JSON patch result = %#v", stored)
		}
	})
	t.Run("patch lowers a business field to its zero value", func(t *testing.T) {
		storage := NewStore(t, fixture)
		object := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "zeroed"}, Value: "present", Rank: 3}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		if err := storage.Patch(t.Context(), object, commonstore.MapMergePatch{"value": "", "rank": 0}); err != nil {
			t.Fatalf("Patch() error = %v", err)
		}
		stored := &Object{}
		if err := storage.Get(t.Context(), "zeroed", stored); err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if stored.Value != "" || stored.Rank != 0 {
			t.Fatalf("patch to zero value did not take effect: %#v", stored)
		}
	})

	t.Run("status patch rejects stale resource version", func(t *testing.T) {
		if !fixture.Capabilities.OptimisticLock {
			t.Skip("conditional writes are checked by the unsupported-version contract")
		}
		storage := NewStore(t, fixture)
		object := &Object{Status: ObjectStatus{Phase: "pending"}}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		staleVersion := object.ResourceVersion
		object.Status.Phase = "ready"
		if err := updateStatus(t.Context(), storage, object); err != nil {
			t.Fatalf("Status().Update() error = %v", err)
		}
		err := storage.Status().
			Patch(t.Context(), object, commonstore.MapMergePatch{
				"resourceVersion": staleVersion,
				"status":          map[string]any{"phase": "stale"},
			})
		if !commonerrors.IsConflict(err) {
			t.Fatalf("stale Status().Patch() error = %v, want Conflict", err)
		}
		got := &Object{}
		if err := storage.Get(t.Context(), object.ID, got); err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		if got.Status.Phase != "ready" {
			t.Fatalf("stale Status().Patch() stored phase = %q, want ready", got.Status.Phase)
		}
	})
	t.Run("no-op writes still enforce conditions", func(t *testing.T) {
		storage := NewStore(t, fixture)
		object := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "no-op"}, Value: "same"}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatal(err)
		}
		initialVersion := object.ResourceVersion
		object.ResourceVersion = 0
		if err := update(t.Context(), storage, object); err != nil {
			t.Fatal(err)
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		if object.ResourceVersion < initialVersion || object.Generation != 1 {
			t.Fatalf("no-op metadata = %#v", object.ObjectMeta)
		}
		before := &Object{}
		if err := storage.Get(t.Context(), object.ID, before); err != nil {
			t.Fatal(err)
		}
		wrongVersion := before.ResourceVersion + 1
		for _, operation := range []string{"update", "patch", "status update", "status patch", "delete"} {
			t.Run(operation, func(t *testing.T) {
				target := &Object{}
				if err := commonstore.CopyObject(before, target); err != nil {
					t.Fatal(err)
				}
				var err error
				switch operation {
				case "update":
					target.ResourceVersion = wrongVersion
					err = storage.Update(t.Context(), target)
				case "patch":
					err = storage.Patch(t.Context(), target, commonstore.MapMergePatch{"value": "same"}, commonstore.WithPreconditions(commonstore.Preconditions{ResourceVersion: &wrongVersion}))
				case "status update":
					target.ResourceVersion = wrongVersion
					err = storage.Status().
						Update(t.Context(), target)
				case "status patch":
					err = storage.Status().
						Patch(t.Context(), target, commonstore.MapMergePatch{"status": map[string]any{}}, commonstore.WithPreconditions(commonstore.Preconditions{ResourceVersion: &wrongVersion}))
				case "delete":
					err = storage.Delete(t.Context(), target, commonstore.WithResourceVersion(wrongVersion))
				}
				if fixture.Capabilities.OptimisticLock {
					if !commonerrors.IsConflict(err) {
						t.Errorf("stale no-op error = %v, want Conflict", err)
					}
				} else if !commonerrors.IsUnsupported(err) {
					t.Errorf("conditional write error = %v, want Unsupported", err)
				}
				got := &Object{}
				if err := storage.Get(t.Context(), before.ID, got); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, before) {
					t.Fatalf("failed condition changed persisted state: %#v", got)
				}
			})
		}
	})
	t.Run("optimistic update", func(t *testing.T) {
		storage := NewStore(t, fixture)
		object := &Object{Value: "one"}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		stale := &Object{}
		if err := commonstore.CopyObject(object, stale); err != nil {
			t.Fatalf("CopyObject() error = %v", err)
		}
		object.Value = "two"
		if err := update(t.Context(), storage, object); err != nil {
			t.Fatalf("Update() error = %v", err)
		}
		stale.Value = "stale"
		err := storage.Update(t.Context(), stale)
		if fixture.Capabilities.OptimisticLock {
			if !commonerrors.IsConflict(err) {
				t.Fatalf("stale Update() error = %v, want Conflict", err)
			}
		} else if !commonerrors.IsUnsupported(err) {
			t.Fatalf("stale Update() error = %v, want Unsupported", err)
		}
		unconditional := &Object{}
		if err := storage.Get(t.Context(), object.ID, unconditional); err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		unconditional.ResourceVersion = 0
		unconditional.Value = "unconditional"
		if err := storage.Update(t.Context(), unconditional); err != nil {
			t.Fatalf("unconditional Update() error = %v", err)
		}
	})
	for _, operation := range []string{"get", "list", "count", "update", "patch", "delete", "patch batch", "delete batch"} {
		t.Run("root scope excludes children/"+operation, func(t *testing.T) {
			if operation == "patch batch" && !fixture.PatchBatch || operation == "delete batch" && !fixture.DeleteBatch {
				t.Skip("batch operation is not declared")
			}
			root := NewStore(t, fixture)
			child := root.Scope(commonstore.Scope{Resource: "tenants", Name: "child"})
			obj := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "child-only"}, Value: "protected"}
			if err := child.Create(t.Context(), obj); err != nil {
				t.Fatal(err)
			}
			before := &Object{}
			if err := child.Get(t.Context(), obj.ID, before); err != nil {
				t.Fatal(err)
			}
			target := &Object{ObjectMeta: commonstore.ObjectMeta{ID: obj.ID}, Value: "unexpected"}
			var err error
			switch operation {
			case "get":
				err = root.Get(t.Context(), obj.ID, target)
			case "update":
				err = root.Update(t.Context(), target)
			case "patch":
				err = root.Patch(t.Context(), target, commonstore.MapMergePatch{"value": "unexpected"})
			case "delete":
				err = root.Delete(t.Context(), target)
			case "list":
				list := &commonstore.List[Object]{}
				if err := root.List(t.Context(), list); err != nil {
					t.Fatal(err)
				}
				if len(list.Items) != 0 {
					t.Errorf("root List returned children: %v", ObjectIDs(list.Items))
				}
			case "count":
				count, err := root.Count(t.Context(), &Object{})
				if err != nil {
					t.Fatal(err)
				}
				if count != 0 {
					t.Errorf("root Count = %d, want 0", count)
				}
			case "patch batch":
				if err := root.PatchBatch(t.Context(), &commonstore.List[Object]{}, commonstore.MapMergePatchBacth{"rank": 9}); err != nil {
					t.Fatal(err)
				}
			case "delete batch":
				if err := root.DeleteBatch(t.Context(), &commonstore.List[Object]{}); err != nil {
					t.Fatal(err)
				}
			}
			if operation == "get" || operation == "update" || operation == "patch" || operation == "delete" {
				if !commonerrors.IsNotFound(err) {
					t.Errorf("root operation error = %v, want NotFound", err)
				}
			}
			after := &Object{}
			if err := child.Get(t.Context(), obj.ID, after); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("root operation modified child: %#v", after)
			}
		})
	}
	t.Run("scope is exact by default", func(t *testing.T) {
		storage := NewStore(t, fixture)
		acme := storage.Scope(commonstore.Scope{Resource: "tenants", Name: "acme"})
		other := storage.Scope(commonstore.Scope{Resource: "tenants", Name: "other"})
		object := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "scoped"}}
		if err := acme.Create(t.Context(), object); err != nil {
			t.Fatalf("scoped Create() error = %v", err)
		}
		if err := acme.Get(t.Context(), object.ID, &Object{}); err != nil {
			t.Fatalf("same-scope Get() error = %v", err)
		}
		if err := other.Get(t.Context(), object.ID, &Object{}); !commonerrors.IsNotFound(err) {
			t.Fatalf("sibling-scope Get() error = %v, want NotFound", err)
		}
		if fixture.SubScopes {
			child := acme.Scope(commonstore.Scope{Resource: "projects", Name: "one"})
			childObject := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "child"}}
			if err := child.Create(t.Context(), childObject); err != nil {
				t.Fatalf("child Create() error = %v", err)
			}
		}
		list := &commonstore.List[Object]{Resource: "storetests"}
		if err := acme.List(t.Context(), list); err != nil {
			t.Fatalf("exact List() error = %v", err)
		}
		if len(list.Items) != 1 || list.Items[0].ID != "scoped" {
			t.Fatalf("exact List() IDs = %v, want [scoped]", ObjectIDs(list.Items))
		}
		if fixture.SubScopes {
			list = &commonstore.List[Object]{Resource: "storetests"}
			if err := acme.List(t.Context(), list, commonstore.WithSubScopes()); err != nil {
				t.Fatalf("subscope List() error = %v", err)
			}
			ids := ObjectIDs(list.Items)
			sort.Strings(ids)
			if !reflect.DeepEqual(ids, []string{"child", "scoped"}) {
				t.Fatalf("subscope List() IDs = %v", ids)
			}
		} else {
			if err := acme.List(t.Context(), &commonstore.List[Object]{}, commonstore.WithSubScopes()); !commonerrors.IsUnsupported(err) {
				t.Fatalf("subscope List() error = %v, want Unsupported", err)
			}
			if _, err := acme.Count(t.Context(), &Object{}, commonstore.WithSubScopes()); !commonerrors.IsUnsupported(err) {
				t.Fatalf("subscope Count() error = %v, want Unsupported", err)
			}
		}
	})
	t.Run("list and count", func(t *testing.T) {
		storage := NewStore(t, fixture)
		objects := []*Object{
			{ObjectMeta: commonstore.ObjectMeta{ID: "c", Name: "Gamma", Labels: map[string]string{"example.com/team": "red"}}, Value: "third", Rank: 3},
			{ObjectMeta: commonstore.ObjectMeta{ID: "a", Name: "Alpha Needle", Labels: map[string]string{"example.com/team": "blue", "owner.name": "alice", "$owner": "alice"}}, Value: "first", Rank: 1},
			{ObjectMeta: commonstore.ObjectMeta{ID: "b", Name: "Beta", Labels: map[string]string{"example.com/team": "blue"}}, Value: "second", Rank: 2},
		}
		for _, object := range objects {
			if err := storage.Create(t.Context(), object); err != nil {
				t.Fatalf("Create(%q) error = %v", object.ID, err)
			}
		}
		list := &commonstore.List[Object]{Resource: "storetests"}
		if err := storage.List(t.Context(), list, commonstore.WithSort("id+")); err != nil {
			t.Fatalf("ascending List() error = %v", err)
		}
		if ids := ObjectIDs(list.Items); !reflect.DeepEqual(ids, []string{"a", "b", "c"}) {
			t.Fatalf("ascending IDs = %v", ids)
		}
		list = &commonstore.List[Object]{Resource: "storetests"}
		if err := storage.List(t.Context(), list, commonstore.WithSort("id-")); err != nil {
			t.Fatalf("descending List() error = %v", err)
		}
		if ids := ObjectIDs(list.Items); !reflect.DeepEqual(ids, []string{"c", "b", "a"}) {
			t.Fatalf("descending IDs = %v", ids)
		}
		count, err := storage.Count(t.Context(), &Object{})
		if err != nil || count != 3 {
			t.Fatalf("Count() = (%d, %v), want (3, nil)", count, err)
		}
		RunQueries(t, fixture, storage)
	})
	t.Run("unique indexes", func(t *testing.T) {
		if !fixture.UniqueIndexes {
			schema := commonstore.NewSchema()
			if err := schema.Register(&Object{}, commonstore.ResourceSchema{ResourceName: "storetests", Indexes: []commonstore.Index{
				{Fields: []string{"value"}, Unique: true},
			}}); err != nil {
				t.Fatal(err)
			}
			if _, err := fixture.New(t, schema); err == nil {
				t.Fatal("store construction accepted an unsupported unique constraint")
			}
			return
		}
		storage := NewStore(t, fixture)
		storage = storage.Scope(commonstore.Scope{Resource: "tenants", Name: "unique"})
		first := &Object{Value: "unique"}
		if err := storage.Create(t.Context(), first); err != nil {
			t.Fatalf("first unique Create() error = %v", err)
		}
		if err := storage.Create(t.Context(), &Object{Value: "unique"}); !commonerrors.IsAlreadyExists(err) {
			t.Fatalf("duplicate unique Create() error = %v, want AlreadyExists", err)
		}
	})

	t.Run("transaction commits or rolls back as one unit", func(t *testing.T) {
		if !fixture.Transaction {
			t.Skip("transactions are not declared")
		}
		storage := NewStore(t, fixture)
		transactional, ok := storage.(commonstore.TransactionStore)
		if !ok {
			t.Fatal("fixture expects transactions but store does not implement TransactionStore")
		}
		committed := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "committed"}, Value: "committed"}
		if err := transactional.Transaction(t.Context(), func(ctx context.Context, tx commonstore.Store) error {
			return tx.Create(ctx, committed)
		}); err != nil {
			t.Fatalf("committed Transaction() error = %v", err)
		}
		stored := &Object{}
		if err := storage.Get(t.Context(), "committed", stored); err != nil {
			t.Fatalf("Get(committed) error = %v", err)
		}
		if stored.Value != "committed" {
			t.Fatalf("committed object = %#v", stored)
		}

		rollback := stderrors.New("rollback")
		err := transactional.Transaction(t.Context(), func(ctx context.Context, tx commonstore.Store) error {
			if err := tx.Create(ctx, &Object{ObjectMeta: commonstore.ObjectMeta{ID: "rolled-back"}}); err != nil {
				return err
			}
			return rollback
		})
		if !stderrors.Is(err, rollback) {
			t.Fatalf("rolled back Transaction() error = %v, want the callback error", err)
		}
		if err := storage.Get(t.Context(), "rolled-back", &Object{}); !commonerrors.IsNotFound(err) {
			t.Fatalf("Get(rolled-back) error = %v, want NotFound", err)
		}
	})

	for _, operation := range []struct {
		name      string
		supported bool
		run       func(context.Context, commonstore.Store) error
	}{
		{"patch", fixture.PatchBatch, func(ctx context.Context, storage commonstore.Store) error {
			return storage.PatchBatch(ctx, &commonstore.List[Object]{}, commonstore.MapMergePatchBacth{"rank": 9})
		}},
		{"delete", fixture.DeleteBatch, func(ctx context.Context, storage commonstore.Store) error {
			return storage.DeleteBatch(ctx, &commonstore.List[Object]{})
		}},
	} {
		if operation.supported {
			continue
		}
		t.Run("unsupported batch "+operation.name, func(t *testing.T) {
			storage := NewStore(t, fixture)
			object := &Object{Value: "unchanged", Rank: 1}
			if err := storage.Create(t.Context(), object); err != nil {
				t.Fatal(err)
			}
			before := &Object{}
			if err := storage.Get(t.Context(), object.ID, before); err != nil {
				t.Fatal(err)
			}
			if err := operation.run(t.Context(), storage); !commonerrors.IsUnsupported(err) {
				t.Fatalf("batch %s error = %v, want Unsupported", operation.name, err)
			}
			after := &Object{}
			if err := storage.Get(t.Context(), object.ID, after); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("unsupported batch %s modified object: %#v", operation.name, after)
			}
		})
	}

	t.Run("batch patch and delete apply to matching objects", func(t *testing.T) {
		if !fixture.PatchBatch || !fixture.DeleteBatch {
			t.Skip("batch operations are not declared")
		}
		storage := NewStore(t, fixture)
		storage = storage.Scope(commonstore.Scope{Resource: "tenants", Name: "batch"})
		for index, id := range []string{"a", "b", "c"} {
			object := &Object{ObjectMeta: commonstore.ObjectMeta{ID: id}, Value: id, Rank: index + 1}
			if err := storage.Create(t.Context(), object); err != nil {
				t.Fatalf("Create(%s) error = %v", id, err)
			}
		}
		// The conformance schema declares value unique, so the batch writes a
		// non-unique field and selects its targets by value.
		if err := storage.PatchBatch(
			t.Context(), &commonstore.List[Object]{},
			commonstore.MapMergePatchBacth{"rank": 5},
			commonstore.WithFieldRequirements(selector.NewRequirement("value", selector.In, "b", "c")),
		); err != nil {
			t.Fatalf("PatchBatch() error = %v", err)
		}
		patched := &commonstore.List[Object]{}
		if err := storage.List(t.Context(), patched, commonstore.WithSort("id+")); err != nil {
			t.Fatal(err)
		}
		ranks := map[string]int{}
		for _, item := range patched.Items {
			ranks[item.ID] = item.Rank
		}
		want := map[string]int{"a": 1, "b": 5, "c": 5}
		if !reflect.DeepEqual(ranks, want) {
			t.Fatalf("patched ranks = %#v, want %#v", ranks, want)
		}
		if err := storage.DeleteBatch(
			t.Context(), &commonstore.List[Object]{},
			commonstore.WithFieldRequirements(selector.NewRequirement("rank", selector.GreaterThan, 1)),
		); err != nil {
			t.Fatalf("DeleteBatch() error = %v", err)
		}
		remaining := &commonstore.List[Object]{}
		if err := storage.List(t.Context(), remaining, commonstore.WithSort("id+")); err != nil {
			t.Fatal(err)
		}
		if ids := ObjectIDs(remaining.Items); !reflect.DeepEqual(ids, []string{"a"}) {
			t.Fatalf("remaining IDs = %v, want [a]", ids)
		}
	})

	t.Run("JSON patch keeps operations that read the object's own fields", func(t *testing.T) {
		storage := NewStore(t, fixture)
		object := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "ops"}, Value: "one", Tenant: "tenant-1"}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		// copy and move reference a field of the same object, which is a write
		// expression over the latest row rather than a reason to read it.
		copied := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "ops"}}
		copyPatch := commonstore.RawPatch(commonstore.PatchTypeJSONPatch, []byte(`[{"op":"copy","from":"/value","path":"/tenant"}]`))
		if fixture.MergePatchOnly || fixture.JSONPatchSetOnly {
			rejectPatch(t, storage, copied, copyPatch)
			return
		}
		if err := storage.Patch(t.Context(), copied, copyPatch); err != nil {
			t.Fatalf("JSON patch copy error = %v", err)
		}
		if err := storage.Get(t.Context(), copied.ID, copied); err != nil {
			t.Fatal(err)
		}
		if copied.Tenant != "one" || copied.Value != "one" {
			t.Fatalf("JSON patch copy result = %#v", copied)
		}
		moved := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "ops"}}
		movePatch := commonstore.RawPatch(commonstore.PatchTypeJSONPatch, []byte(`[{"op":"move","from":"/tenant","path":"/value"}]`))
		if err := storage.Patch(t.Context(), moved, movePatch); err != nil {
			t.Fatalf("JSON patch move error = %v", err)
		}
		if err := storage.Get(t.Context(), moved.ID, moved); err != nil {
			t.Fatal(err)
		}
		if moved.Value != "one" || moved.Tenant != "" {
			t.Fatalf("JSON patch move result = %#v", moved)
		}
	})
}

func runIdentity(t *testing.T, fixture Fixture) {
	for _, test := range []struct{ name, first, second string }{
		{"root duplicate", "", ""},
		{"scoped duplicate", "acme", "acme"},
		{"root to child", "", "acme"},
		{"child to root", "acme", ""},
		{"siblings", "acme", "other"},
	} {
		t.Run("identity/"+test.name, func(t *testing.T) {
			root := NewStore(t, fixture)
			first, second := root, root
			if test.first != "" {
				first = root.Scope(commonstore.Scope{Resource: "tenants", Name: test.first})
			}
			if test.second != "" {
				second = root.Scope(commonstore.Scope{Resource: "tenants", Name: test.second})
			}
			original := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "shared"}, Value: "first"}
			if err := first.Create(t.Context(), original); err != nil {
				t.Fatal(err)
			}
			before := &Object{}
			if err := first.Get(t.Context(), original.ID, before); err != nil {
				t.Fatal(err)
			}
			duplicate := &Object{ObjectMeta: commonstore.ObjectMeta{ID: original.ID}, Value: "second"}
			err := second.Create(t.Context(), duplicate)
			if fixture.ResourceWideIDs || test.first == test.second {
				if !commonerrors.IsAlreadyExists(err) {
					t.Fatalf("duplicate Create() = %v, want AlreadyExists", err)
				}
				if test.first != test.second {
					if err := second.Get(t.Context(), original.ID, &Object{}); !commonerrors.IsNotFound(err) {
						t.Fatalf("other-scope Get() = %v, want NotFound", err)
					}
				}
			} else {
				if err != nil {
					t.Fatalf("cross-scope Create() = %v", err)
				}
				stored := &Object{}
				if err := second.Get(t.Context(), original.ID, stored); err != nil {
					t.Fatal(err)
				}
				if stored.Value != "second" || stored.UID == before.UID {
					t.Fatalf("cross-scope identity = %#v", stored)
				}
			}
			after := &Object{}
			if err := first.Get(t.Context(), original.ID, after); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("duplicate Create changed original: %#v", after)
			}
		})
	}
}

func runFinalizers(t *testing.T, fixture Fixture) {
	if !fixture.Capabilities.Finalizers {
		for _, operation := range []string{"create", "update", "patch", "foreground delete"} {
			t.Run("finalizers/unsupported/"+operation, func(t *testing.T) {
				storage := NewStore(t, fixture)
				object := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "unsupported-finalizer"}, Value: "before"}
				if operation == "create" {
					object.Finalizers = []string{"example.com/retain"}
					if err := storage.Create(t.Context(), object); !commonerrors.IsUnsupported(err) {
						t.Fatalf("Create = %v, want Unsupported", err)
					}
					if err := storage.Get(t.Context(), object.ID, &Object{}); !commonerrors.IsNotFound(err) {
						t.Fatalf("rejected Create persisted: %v", err)
					}
					return
				}
				if err := storage.Create(t.Context(), object); err != nil {
					t.Fatal(err)
				}
				before := &Object{}
				if err := storage.Get(t.Context(), object.ID, before); err != nil {
					t.Fatal(err)
				}
				var err error
				switch operation {
				case "update":
					object.Finalizers = []string{"example.com/retain"}
					object.ResourceVersion = 0
					err = storage.Update(t.Context(), object)
				case "patch":
					err = storage.Patch(t.Context(), object, commonstore.MapMergePatch{"finalizers": []string{"example.com/retain"}})
				case "foreground delete":
					err = storage.Delete(t.Context(), object, commonstore.WithPropagation(commonstore.DeletePropagationForeground))
				}
				if !commonerrors.IsUnsupported(err) {
					t.Fatalf("operation = %v, want Unsupported", err)
				}
				after := &Object{}
				if err := storage.Get(t.Context(), object.ID, after); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(before, after) {
					t.Fatalf("unsupported finalizer write changed object: %#v", after)
				}
			})
		}
		return
	}

	t.Run("finalizers/read does not restore removed finalizers", func(t *testing.T) {
		storage := NewStore(t, fixture)
		object := &Object{ObjectMeta: commonstore.ObjectMeta{Finalizers: []string{"example.com/retain"}}}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatal(err)
		}
		target := &Object{ObjectMeta: commonstore.ObjectMeta{ID: object.ID}}
		if err := storage.Patch(t.Context(), target, commonstore.MapMergePatch{"finalizers": nil}); err != nil {
			t.Fatal(err)
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		if len(object.Finalizers) != 0 {
			t.Fatalf("Get restored removed finalizers: %v", object.Finalizers)
		}
		if err := storage.Delete(t.Context(), object); err != nil {
			t.Fatal(err)
		}
		if err := storage.Get(t.Context(), object.ID, &Object{}); !commonerrors.IsNotFound(err) {
			t.Fatalf("Delete without finalizers = %v", err)
		}
	})
	t.Run("finalizers/scoped replacement preserves identity", func(t *testing.T) {
		root := NewStore(t, fixture)
		storage := root.Scope(commonstore.Scope{Resource: "tenants", Name: "lifecycle"})
		object := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "scoped", Finalizers: []string{"example.com/retain"}}}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatal(err)
		}
		// A complete replacement may omit the backend's projected Scope fields.
		desired := &Object{ObjectMeta: commonstore.ObjectMeta{ID: object.ID, Finalizers: []string{"example.com/retain"}}, Value: "changed"}
		if err := storage.Update(t.Context(), desired); err != nil {
			t.Fatal(err)
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		if object.Value != "changed" || !reflect.DeepEqual(object.Scopes, []commonstore.Scope{{Resource: "tenants", Name: "lifecycle"}}) {
			t.Fatalf("replacement lost scope: %#v", object)
		}
		if err := storage.Delete(t.Context(), object); err != nil {
			t.Fatal(err)
		}
		if err := storage.Patch(t.Context(), object, commonstore.MapMergePatch{"finalizers": nil}); err != nil {
			t.Fatal(err)
		}
		if err := storage.Get(t.Context(), object.ID, &Object{}); !commonerrors.IsNotFound(err) {
			t.Fatalf("scoped finalizer completion = %v", err)
		}
	})
	for _, operation := range []string{"update", "merge patch", "JSON patch"} {
		t.Run("finalizers/terminating cannot add/"+operation, func(t *testing.T) {
			storage := NewStore(t, fixture)
			object := &Object{ObjectMeta: commonstore.ObjectMeta{Finalizers: []string{"example.com/retain"}}}
			if err := storage.Create(t.Context(), object); err != nil {
				t.Fatal(err)
			}
			if err := storage.Delete(t.Context(), object); err != nil {
				t.Fatal(err)
			}
			before := &Object{}
			if err := storage.Get(t.Context(), object.ID, before); err != nil {
				t.Fatal(err)
			}
			var err error
			switch operation {
			case "update":
				object.Finalizers = []string{"example.com/retain", "example.com/new"}
				object.ResourceVersion = 0 // Exercise the unconditional write contract.
				err = storage.Update(t.Context(), object)
			case "merge patch":
				err = storage.Patch(t.Context(), object, commonstore.MapMergePatch{"finalizers": []string{"example.com/retain", "example.com/new"}})
			case "JSON patch":
				err = storage.Patch(t.Context(), object, commonstore.RawPatch(commonstore.PatchTypeJSONPatch, []byte(`[{"op":"replace","path":"/finalizers","value":["example.com/new"]}]`)))
			}
			if (fixture.MergePatchOnly || fixture.JSONPatchSetOnly) && operation == "JSON patch" {
				if !commonerrors.IsUnsupported(err) {
					t.Fatalf("JSON patch = %v, want Unsupported", err)
				}
			} else if commonerrors.ReasonForError(err) != commonerrors.StatusReasonBadRequest {
				t.Fatalf("adding a finalizer error = %v, want BadRequest", err)
			}
			after := &Object{}
			if err := storage.Get(t.Context(), object.ID, after); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatalf("rejected write changed object: before %#v, after %#v", before, after)
			}
		})
	}
	if fixture.Capabilities.Watch {
		for _, operation := range []string{"update", "patch"} {
			t.Run("finalizers/completion emits only deletion/"+operation, func(t *testing.T) {
				storage := NewStore(t, fixture)
				object := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "retained", Finalizers: []string{"example.com/retain"}}}
				if err := storage.Create(t.Context(), object); err != nil {
					t.Fatal(err)
				}
				if err := storage.Delete(t.Context(), object); err != nil {
					t.Fatal(err)
				}
				watcher := OpenWatcher(t, func() (commonstore.Watcher, error) {
					return storage.Watch(t.Context(), &commonstore.List[Object]{}, commonstore.WithSendInitialEvents())
				})
				defer watcher.Stop()
				AssertWatchEvent(t, NextWatchEvent(t, watcher), commonstore.WatchEventCreate, object.ID)
				AssertWatchEvent(t, NextWatchEvent(t, watcher), commonstore.WatchEventBookmark, "")
				if operation == "patch" {
					if err := storage.Patch(t.Context(), object, commonstore.MapMergePatch{"finalizers": nil}); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := storage.Get(t.Context(), object.ID, object); err != nil {
						t.Fatal(err)
					}
					object.Finalizers = nil
					object.ResourceVersion = 0 // Verify the unconditional lifecycle transition.
					if err := storage.Update(t.Context(), object); err != nil {
						t.Fatal(err)
					}
				}
				AssertWatchEvent(t, NextObjectWatchEvent(t, watcher), commonstore.WatchEventDelete, object.ID)
				if err := storage.Get(t.Context(), object.ID, &Object{}); !commonerrors.IsNotFound(err) {
					t.Fatalf("completed deletion Get = %v", err)
				}
			})
		}
	}
	if fixture.DeleteBatch && fixture.PatchBatch {
		t.Run("finalizers/batch follows lifecycle", func(t *testing.T) {
			storage := NewStore(t, fixture)
			retained := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "retained", Finalizers: []string{"example.com/retain"}}, Value: "before", Status: ObjectStatus{Phase: "pending"}}
			plain := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "plain"}}
			for _, object := range []*Object{retained, plain} {
				if err := storage.Create(t.Context(), object); err != nil {
					t.Fatal(err)
				}
			}
			if err := storage.DeleteBatch(t.Context(), &commonstore.List[Object]{}); err != nil {
				t.Fatal(err)
			}
			if err := storage.Get(t.Context(), plain.ID, &Object{}); !commonerrors.IsNotFound(err) {
				t.Fatalf("plain Get = %v, want NotFound", err)
			}
			if err := storage.Get(t.Context(), retained.ID, retained); err != nil {
				t.Fatal(err)
			}
			if retained.DeletionTimestamp == nil {
				t.Fatal("DeleteBatch did not begin deletion")
			}
			version, generation := retained.ResourceVersion, retained.Generation
			if err := storage.PatchBatch(t.Context(), &commonstore.List[Object]{}, commonstore.MapMergePatchBacth{"value": "after", "status": map[string]any{"phase": "ignored"}, "deletionTimestamp": nil}); err != nil {
				t.Fatal(err)
			}
			if err := storage.Get(t.Context(), retained.ID, retained); err != nil {
				t.Fatal(err)
			}
			if retained.Value != "after" || retained.Status.Phase != "pending" || retained.DeletionTimestamp == nil || retained.ResourceVersion <= version || retained.Generation != generation+1 {
				t.Fatalf("batch patch violated update contract: %#v", retained)
			}
			if err := storage.PatchBatch(t.Context(), &commonstore.List[Object]{}, commonstore.MapMergePatchBacth{"finalizers": []string{"example.com/new"}}); fixture.BatchFinalizerRemovalOnly && !commonerrors.IsUnsupported(err) || !fixture.BatchFinalizerRemovalOnly && commonerrors.ReasonForError(err) != commonerrors.StatusReasonBadRequest {
				t.Fatalf("batch add finalizer = %v, want BadRequest", err)
			}
			if err := storage.PatchBatch(t.Context(), &commonstore.List[Object]{}, commonstore.MapMergePatchBacth{"finalizers": nil}); err != nil {
				t.Fatal(err)
			}
			if err := storage.Get(t.Context(), retained.ID, &Object{}); !commonerrors.IsNotFound(err) {
				t.Fatalf("retained Get = %v, want NotFound", err)
			}
		})
	}
	if fixture.TTL {
		for _, operation := range []string{"update", "patch", "status update", "status patch"} {
			t.Run("finalizers/writes preserve TTL/"+operation, func(t *testing.T) {
				storage := NewStore(t, fixture)
				object := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "leased"}, Value: "before"}
				if err := storage.Create(t.Context(), object, commonstore.WithTTL(time.Hour)); err != nil {
					t.Fatal(err)
				}
				before := &Object{}
				if err := storage.Get(t.Context(), object.ID, before); err != nil {
					t.Fatal(err)
				}
				var err error
				switch operation {
				case "update":
					object.Value = "after"
					err = storage.Update(t.Context(), object)
				case "patch":
					err = storage.Patch(t.Context(), object, commonstore.MapMergePatch{"value": "after"})
				case "status update":
					object.Status.Phase = "ready"
					err = storage.Status().
						Update(t.Context(), object)
				case "status patch":
					err = storage.Status().
						Patch(t.Context(), object, commonstore.MapMergePatch{"status": map[string]any{"phase": "ready"}})
				}
				if err != nil && !commonerrors.IsUnsupported(err) {
					t.Fatal(err)
				}
				if commonerrors.IsUnsupported(err) {
					after := &Object{}
					if err := storage.Get(t.Context(), object.ID, after); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(before, after) {
						t.Fatal("unsupported write changed leased object")
					}
				}
				err = storage.Patch(t.Context(), object, commonstore.MapMergePatch{"finalizers": []string{"example.com/retain"}})
				if commonerrors.ReasonForError(err) != commonerrors.StatusReasonBadRequest && !commonerrors.IsUnsupported(err) {
					t.Fatalf("write lost TTL: finalizer Patch = %v", err)
				}
			})
		}

		for _, operation := range []string{"create", "update TTL", "update finalizers", "patch finalizers", "delete foreground"} {
			t.Run("finalizers/TTL excludes finalizers/"+operation, func(t *testing.T) {
				storage := NewStore(t, fixture)
				object := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "leased"}}
				if operation == "create" || operation == "update TTL" {
					object.Finalizers = []string{"example.com/retain"}
				}
				if operation == "create" {
					err := storage.Create(t.Context(), object, commonstore.WithTTL(time.Hour))
					if commonerrors.ReasonForError(err) != commonerrors.StatusReasonBadRequest {
						t.Fatalf("TTL with finalizers = %v, want BadRequest", err)
					}
					if err := storage.Get(t.Context(), object.ID, &Object{}); !commonerrors.IsNotFound(err) {
						t.Fatalf("rejected Create persisted: %v", err)
					}
					return
				}
				var createOptions []commonstore.CreateOption
				if operation != "update TTL" {
					createOptions = append(createOptions, commonstore.WithTTL(time.Hour))
				}
				if err := storage.Create(t.Context(), object, createOptions...); err != nil {
					t.Fatal(err)
				}
				before := &Object{}
				if err := storage.Get(t.Context(), object.ID, before); err != nil {
					t.Fatal(err)
				}
				var err error
				switch operation {
				case "update TTL":
					err = storage.Update(t.Context(), object, commonstore.WithTTL(time.Hour))
				case "update finalizers":
					object.Finalizers = []string{"example.com/retain"}
					err = storage.Update(t.Context(), object)
				case "patch finalizers":
					err = storage.Patch(t.Context(), object, commonstore.MapMergePatch{"finalizers": []string{"example.com/retain"}})
				case "delete foreground":
					err = storage.Delete(t.Context(), object, commonstore.WithPropagation(commonstore.DeletePropagationForeground))
				}
				if commonerrors.ReasonForError(err) != commonerrors.StatusReasonBadRequest && !commonerrors.IsUnsupported(err) {
					t.Fatalf("TTL with finalizers = %v, want BadRequest or Unsupported", err)
				}
				after := &Object{}
				if err := storage.Get(t.Context(), object.ID, after); err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(before, after) {
					t.Fatalf("rejected TTL/finalizer write changed object: %#v", after)
				}
			})
		}
	}
	for _, policy := range []commonstore.DeletionPropagation{commonstore.DeletePropagationForeground, commonstore.DeletePropagationOrphan} {
		t.Run("finalizers/repeated delete/"+string(policy), func(t *testing.T) {
			storage := NewStore(t, fixture)
			object := &Object{ObjectMeta: commonstore.ObjectMeta{Finalizers: []string{"example.com/retain"}}}
			if err := storage.Create(t.Context(), object); err != nil {
				t.Fatal(err)
			}
			if err := storage.Delete(t.Context(), object); err != nil {
				t.Fatal(err)
			}
			before := &Object{}
			if err := storage.Get(t.Context(), object.ID, before); err != nil {
				t.Fatal(err)
			}
			if err := storage.Delete(t.Context(), object, commonstore.WithPropagation(policy)); err != nil {
				t.Fatal(err)
			}
			after := &Object{}
			if err := storage.Get(t.Context(), object.ID, after); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(after.Finalizers, []string{"example.com/retain"}) || !reflect.DeepEqual(before.DeletionTimestamp, after.DeletionTimestamp) {
				t.Fatalf("repeated Delete changed deletion intent: %#v", after)
			}
			list := &commonstore.List[Object]{}
			if err := storage.List(t.Context(), list); err != nil {
				t.Fatal(err)
			}
			if len(list.Items) != 1 || list.Items[0].ID != object.ID {
				t.Fatalf("List hid terminating object: %#v", list.Items)
			}
			count, err := storage.Count(t.Context(), &Object{})
			if err != nil || count != 1 {
				t.Fatalf("Count = %d, %v; want 1", count, err)
			}
			if err := storage.Patch(t.Context(), object, commonstore.MapMergePatch{"finalizers": nil}); err != nil {
				t.Fatal(err)
			}
			if err := storage.Get(t.Context(), object.ID, &Object{}); !commonerrors.IsNotFound(err) {
				t.Fatalf("Get after clearing finalizers = %v, want NotFound", err)
			}
		})
	}
}

func assertSchemaSnapshot(t *testing.T, storage commonstore.Store) {
	t.Helper()
	schema := storage.Schema()
	if got := schema.Resources(); !reflect.DeepEqual(got, []string{"storetests"}) {
		t.Fatalf("Schema().Resources() = %v, want [storetests]", got)
	}
	if err := schema.Register(&schemaMutationObject{}, commonstore.ResourceSchema{}); err != nil {
		t.Fatalf("Schema().Register() error = %v", err)
	}
	if got := storage.Schema().
		Resources(); !reflect.DeepEqual(got, []string{"storetests"}) {
		t.Fatalf("Schema() shares mutable state with Store: resources = %v", got)
	}
}

// OpenWatcher opens one subscription. A successful return must already be
// subscribed; initialization waiting belongs to the Store and its Context.
func OpenWatcher(t testing.TB, open func() (commonstore.Watcher, error)) commonstore.Watcher {
	t.Helper()
	watcher, err := open()
	if err != nil {
		t.Fatalf("Watch() error = %v", err)
	}
	return watcher
}

// NextWatchEvent waits for the next successful Watch event.
func NextWatchEvent(t testing.TB, watcher commonstore.Watcher) commonstore.WatchEvent {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case event, ok := <-watcher.Events():
		if !ok {
			t.Fatal("Watch Events() closed before the expected event")
		}
		if event.Error != nil {
			t.Fatalf("Watch event error = %v", event.Error)
		}
		return event
	case <-timer.C:
		t.Fatal("timed out waiting for Watch event")
		return commonstore.WatchEvent{}
	}
}

// NextObjectWatchEvent skips optional progress Bookmarks and waits for an object event.
func NextObjectWatchEvent(t testing.TB, watcher commonstore.Watcher) commonstore.WatchEvent {
	t.Helper()
	for {
		event := NextWatchEvent(t, watcher)
		if event.Type != commonstore.WatchEventBookmark {
			return event
		}
	}
}

// AssertWatchEvent verifies a Watch event's type and object identity.
func AssertWatchEvent(t testing.TB, event commonstore.WatchEvent, wantType commonstore.WatchEventType, wantID string) {
	t.Helper()
	if event.Type != wantType {
		t.Fatalf("Watch event type = %q, want %q", event.Type, wantType)
	}
	if wantType == commonstore.WatchEventBookmark {
		if event.Object != nil {
			t.Fatalf("Bookmark object = %#v, want nil", event.Object)
		}
		return
	}
	object, ok := event.Object.(*Object)
	if !ok {
		t.Fatalf("Watch event object type = %T, want *storetest.Object", event.Object)
	}
	if object.ID != wantID {
		t.Fatalf("Watch event object ID = %q, want %q", object.ID, wantID)
	}
}

// AssertWatcherClosed verifies that a stopped or canceled Watch eventually closes.
func AssertWatcherClosed(t testing.TB, watcher commonstore.Watcher) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case _, ok := <-watcher.Events():
			if !ok {
				return
			}
		case <-timer.C:
			t.Fatal("Watch Events() did not close")
		}
	}
}

// RunQueries verifies supported queries and explicit rejection of unsupported ones.
func RunQueries(t *testing.T, fixture Fixture, storage commonstore.Store) {
	t.Helper()
	fieldComposition := selector.Requirement{
		Operator: selector.Or,
		Requirements: commonstore.Requirements{
			selector.RequirementEqual("rank", 1),
			{
				Operator: selector.And,
				Requirements: commonstore.Requirements{
					selector.NewRequirement("rank", selector.GreaterThan, 1),
					{
						Operator: selector.Not,
						Requirements: commonstore.Requirements{
							selector.RequirementEqual("rank", 2),
						},
					},
				},
			},
		},
	}
	labelComposition := selector.Requirement{
		Operator: selector.Or,
		Requirements: commonstore.Requirements{
			selector.RequirementEqual("example.com/team", "red"),
			selector.RequirementEqual("owner.name", "alice"),
		},
	}
	tests := []struct {
		name    string
		enabled bool
		options []commonstore.ListOption
		wantIDs []string
		ordered bool
	}{
		{name: "label selector", enabled: fixture.LabelSelector, options: []commonstore.ListOption{commonstore.WithLabelRequirements(selector.RequirementEqual("example.com/team", "blue"))}, wantIDs: []string{"a", "b"}},
		{name: "field selector", enabled: fixture.FieldSelector, options: []commonstore.ListOption{commonstore.WithFieldRequirements(selector.RequirementEqual("rank", 2))}, wantIDs: []string{"b"}},
		{name: "composed field selector", enabled: fixture.FieldSelector, options: []commonstore.ListOption{commonstore.WithFieldRequirements(fieldComposition)}, wantIDs: []string{"a", "c"}},
		{name: "composed label selector", enabled: fixture.LabelSelector, options: []commonstore.ListOption{commonstore.WithLabelRequirements(labelComposition)}, wantIDs: []string{"a", "c"}},
		{name: "none selector", enabled: fixture.FieldSelector, options: []commonstore.ListOption{commonstore.WithFieldRequirements(selector.Requirement{})}, wantIDs: []string{}},
		{name: "empty in selector", enabled: fixture.FieldSelector, options: []commonstore.ListOption{commonstore.WithFieldRequirements(selector.NewRequirement("rank", selector.In))}, wantIDs: []string{}},
		{name: "empty not-in selector", enabled: fixture.FieldSelector, options: []commonstore.ListOption{commonstore.WithFieldRequirements(selector.NewRequirement("rank", selector.NotIn))}, wantIDs: []string{}},
		{name: "all selector", enabled: fixture.FieldSelector, options: []commonstore.ListOption{commonstore.WithFieldRequirements(selector.Requirement{Operator: selector.All})}, wantIDs: []string{"a", "b", "c"}},
		{name: "special label keys", enabled: fixture.LabelSelector, options: []commonstore.ListOption{commonstore.WithLabelRequirements(selector.RequirementEqual("owner.name", "alice"), selector.RequirementEqual("$owner", "alice"))}, wantIDs: []string{"a"}},
		{name: "search by name", enabled: fixture.Search, options: []commonstore.ListOption{commonstore.WithSearch("needle")}, wantIDs: []string{"a"}},
		{name: "search by ID", enabled: fixture.Search, options: []commonstore.ListOption{commonstore.WithSearch("c")}, wantIDs: []string{"c"}},
		{name: "explicit search fields", enabled: fixture.Search, options: []commonstore.ListOption{commonstore.WithSearch("c"), commonstore.WithSearchFields("name")}, wantIDs: []string{}},
		{name: "indexed sort", enabled: fixture.Sort, options: []commonstore.ListOption{commonstore.WithSort("rank-")}, wantIDs: []string{"c", "b", "a"}, ordered: true},
		{name: "window limit", enabled: true, options: []commonstore.ListOption{commonstore.WithSort("id+"), commonstore.WithLimit(2)}, wantIDs: []string{"a", "b"}, ordered: true},
		{name: "window offset", enabled: true, options: []commonstore.ListOption{commonstore.WithSort("id+"), commonstore.WithOffset(1), commonstore.WithLimit(1)}, wantIDs: []string{"b"}, ordered: true},
		{name: "window offset past end", enabled: true, options: []commonstore.ListOption{commonstore.WithSort("id+"), commonstore.WithOffset(3)}, wantIDs: []string{}, ordered: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			list := &commonstore.List[Object]{Resource: "storetests"}
			err := storage.List(t.Context(), list, test.options...)
			if !test.enabled {
				if !commonerrors.IsUnsupported(err) {
					t.Fatalf("List() error = %v, want Unsupported", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("List() error = %v", err)
			}
			ids := ObjectIDs(list.Items)
			if !test.ordered {
				sort.Strings(ids)
			}
			if !reflect.DeepEqual(ids, test.wantIDs) {
				t.Fatalf("List() IDs = %v, want %v", ids, test.wantIDs)
			}
		})
	}
	if fixture.FieldSelector {
		invalid := selector.Requirement{Operator: selector.And, Key: "rank"}
		list := &commonstore.List[Object]{Resource: "storetests"}
		err := storage.List(t.Context(), list, commonstore.WithFieldRequirements(invalid))
		if commonerrors.ReasonForError(err) != commonerrors.StatusReasonBadRequest {
			t.Fatalf("invalid requirement List() error = %v, want BadRequest", err)
		}

		count, err := storage.Count(t.Context(), &Object{}, commonstore.WithFieldRequirements(fieldComposition))
		if err != nil {
			t.Fatalf("composed Count() error = %v", err)
		}
		if count != 2 {
			t.Fatalf("composed Count() = %d, want 2", count)
		}
		{
			list := &commonstore.List[Object]{Resource: "storetests"}
			err := storage.List(
				t.Context(),
				list,
				commonstore.WithFieldRequirements(fieldComposition),
				commonstore.WithSort("id+"),
				commonstore.WithLimit(1),
			)
			if err != nil {
				t.Fatalf("composed limited List() error = %v", err)
			}
			if ids := ObjectIDs(list.Items); !reflect.DeepEqual(ids, []string{"a"}) {
				t.Fatalf("composed limited List() IDs = %v, want [a]", ids)
			}

		}
	}
	if !fixture.Projection {
		t.Run("unsupported projection", func(t *testing.T) {
			if err := storage.Get(t.Context(), "a", &Object{}, commonstore.WithFields("value")); !commonerrors.IsUnsupported(err) {
				t.Fatalf("projection Get() error = %v, want Unsupported", err)
			}
			list := &commonstore.List[Object]{}
			if err := storage.List(t.Context(), list, commonstore.WithFields("value")); !commonerrors.IsUnsupported(err) {
				t.Fatalf("projection List() error = %v, want Unsupported", err)
			}
		})
	} else {
		projected := &Object{}
		if err := storage.Get(t.Context(), "a", projected, commonstore.WithFields("id", "value")); err != nil {
			t.Fatalf("projection Get() error = %v", err)
		}
		if projected.ID != "a" || projected.Value != "first" || projected.Name != "" || projected.Rank != 0 {
			t.Fatalf("projection Get() = %#v", projected)
		}
	}
	if fixture.Continue {
		seen := map[string]bool{}
		continueToken := ""
		for {
			list := &commonstore.List[Object]{Resource: "storetests"}
			options := []commonstore.ListOption{commonstore.WithContinue(continueToken, 1)}
			if err := storage.List(t.Context(), list, options...); err != nil {
				t.Fatalf("cursor List() error = %v", err)
			}

			for _, item := range list.Items {
				seen[item.ID] = true
			}
			continueToken = list.Continue
			if continueToken == "" {
				break
			}
		}
		if !reflect.DeepEqual(seen, map[string]bool{"a": true, "b": true, "c": true}) {
			t.Fatalf("cursor List() IDs = %v", seen)
		}
	}
}

// ObjectIDs returns item IDs in list order.
func ObjectIDs(items []Object) []string {
	ids := make([]string, len(items))
	for index := range items {
		ids[index] = items[index].ID
	}
	return ids
}

// NewStore creates one isolated Store with the suite schema.
func NewStore(t testing.TB, fixture Fixture) commonstore.Store {
	t.Helper()
	schema := commonstore.NewSchema()
	definition := commonstore.ResourceSchema{ResourceName: "storetests", ScopeKeys: []string{"tenant"}, StatusResource: true}
	if fixture.SecondaryIndexes {
		definition.Indexes = append(definition.Indexes, commonstore.Index{Name: "rank", Fields: []string{"rank"}})
	}
	if fixture.UniqueIndexes {
		definition.Indexes = append(definition.Indexes, commonstore.Index{Name: "value", Fields: []string{"value"}, Unique: true, Nullable: true})
	}
	err := schema.Register(&Object{}, definition)
	if err != nil {
		t.Fatalf("register conformance schema: %v", err)
	}
	storage, err := fixture.New(t, schema)
	if err != nil {
		t.Fatalf("create conformance store: %v", err)
	}
	return storage
}

// runWriteOptions checks both the result and persisted state through Store.
func runWriteOptions(t *testing.T, fixture Fixture) {
	operations := []struct {
		name      string
		supported bool
		run       func(context.Context, commonstore.Store, *Object) error
	}{
		{"create", true, func(ctx context.Context, s commonstore.Store, obj *Object) error {
			return s.Create(ctx, obj, commonstore.WithDryRun())
		}},
		{"update", true, func(ctx context.Context, s commonstore.Store, obj *Object) error {
			obj.Value = "changed"
			return s.Update(ctx, obj, commonstore.WithDryRun())
		}},
		{"patch", true, func(ctx context.Context, s commonstore.Store, obj *Object) error {
			return s.Patch(ctx, obj, commonstore.MapMergePatch{"value": "changed"}, commonstore.WithDryRun())
		}},
		{"status update", true, func(ctx context.Context, s commonstore.Store, obj *Object) error {
			obj.Status.Phase = "changed"
			return s.Status().
				Update(ctx, obj, commonstore.WithDryRun())
		}},
		{"status patch", true, func(ctx context.Context, s commonstore.Store, obj *Object) error {
			return s.Status().
				Patch(ctx, obj, commonstore.MapMergePatch{"status": map[string]any{"phase": "changed"}}, commonstore.WithDryRun())
		}},
		{"delete", true, func(ctx context.Context, s commonstore.Store, obj *Object) error {
			return s.Delete(ctx, obj, commonstore.WithDryRun())
		}},
		{"patch batch", fixture.PatchBatch, func(ctx context.Context, s commonstore.Store, obj *Object) error {
			return s.PatchBatch(ctx, &commonstore.List[Object]{}, commonstore.MapMergePatchBacth{"value": "changed"}, commonstore.WithDryRun())
		}},
		{"delete batch", fixture.DeleteBatch, func(ctx context.Context, s commonstore.Store, obj *Object) error {
			return s.DeleteBatch(ctx, &commonstore.List[Object]{}, commonstore.WithDryRun())
		}},
	}
	for _, operation := range operations {
		t.Run("dry run/"+operation.name, func(t *testing.T) {
			s := NewStore(t, fixture)
			object := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "dry-run"}, Value: "before", Status: ObjectStatus{Phase: "pending"}}
			if operation.name != "create" {
				if err := s.Create(t.Context(), object); err != nil {
					t.Fatal(err)
				}
			}
			before := &Object{}
			if operation.name != "create" {
				if err := s.Get(t.Context(), object.ID, before); err != nil {
					t.Fatal(err)
				}
			}
			err := operation.run(t.Context(), s, object)
			if !fixture.DryRun || !operation.supported {
				if !commonerrors.IsUnsupported(err) {
					t.Errorf("operation error = %v, want Unsupported", err)
				}
			} else if err != nil {
				t.Errorf("dry-run error = %v", err)
			}
			got := &Object{}
			err = s.Get(t.Context(), "dry-run", got)
			if operation.name == "create" {
				if !commonerrors.IsNotFound(err) {
					t.Fatalf("dry-run Create persisted object: Get error = %v", err)
				}
			} else {
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(got, before) {
					t.Fatalf("dry-run changed persisted state: got %#v, want %#v", got, before)
				}
			}
		})
	}
	if !fixture.TTL {
		for _, operation := range []string{"create", "update", "status update"} {
			t.Run("unsupported TTL/"+operation, func(t *testing.T) {
				s := NewStore(t, fixture)
				obj := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "ttl"}, Value: "before"}
				if operation != "create" {
					if err := s.Create(t.Context(), obj); err != nil {
						t.Fatal(err)
					}
				}
				before := &Object{}
				if operation != "create" {
					if err := s.Get(t.Context(), obj.ID, before); err != nil {
						t.Fatal(err)
					}
				}
				var err error
				switch operation {
				case "create":
					err = s.Create(t.Context(), obj, commonstore.WithTTL(time.Hour))
				case "update":
					obj.Value = "changed"
					err = s.Update(t.Context(), obj, commonstore.WithTTL(time.Hour))
				case "status update":
					obj.Status.Phase = "changed"
					err = s.Status().
						Update(t.Context(), obj, commonstore.WithTTL(time.Hour))
				}
				if !commonerrors.IsUnsupported(err) {
					t.Errorf("TTL error = %v, want Unsupported", err)
				}
				got := &Object{}
				err = s.Get(t.Context(), "ttl", got)
				if operation == "create" {
					if !commonerrors.IsNotFound(err) {
						t.Fatalf("unsupported TTL created object: %v", err)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(got, before) {
						t.Fatalf("unsupported TTL changed object: %#v", got)
					}
				}
			})
		}
	}
}

func rejectPatch(t *testing.T, storage commonstore.Store, obj *Object, patch commonstore.Patch) {
	t.Helper()
	before := &Object{}
	if err := storage.Get(t.Context(), obj.ID, before); err != nil {
		t.Fatal(err)
	}
	if err := storage.Patch(t.Context(), obj, patch); !commonerrors.IsUnsupported(err) {
		t.Fatalf("Patch = %v, want Unsupported", err)
	}
	after := &Object{}
	if err := storage.Get(t.Context(), obj.ID, after); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("unsupported patch changed object: before=%#v after=%#v", before, after)
	}
}

func runMutationConditions(t *testing.T, fixture Fixture) {
	for _, operation := range []string{"update", "patch", "status update", "status patch", "delete"} {
		for _, condition := range []string{"missing versioned object", "unmatched selector"} {
			t.Run("conditional writes/"+operation+"/"+condition, func(t *testing.T) {
				if condition == "missing versioned object" && !fixture.Capabilities.OptimisticLock || condition == "unmatched selector" && !fixture.FieldSelector {
					t.Skip("condition unsupported")
				}
				storage := NewStore(t, fixture)
				object := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "target"}, Value: "before"}
				version := int64(1)
				var requirements commonstore.Requirements
				var preconditions *commonstore.Preconditions
				if condition == "missing versioned object" {
					object.ResourceVersion = version
					preconditions = &commonstore.Preconditions{ResourceVersion: &version}
				} else {
					if err := storage.Create(t.Context(), object); err != nil {
						t.Fatal(err)
					}
					object.ResourceVersion = 0
					requirements = commonstore.Requirements{selector.RequirementEqual("value", "other")}
				}
				patch := commonstore.MapMergePatch{"value": "after", "status": map[string]any{"phase": "ready"}}
				var err error
				switch operation {
				case "update":
					err = storage.Update(t.Context(), object, commonstore.WithFieldRequirements(requirements...))
				case "status update":
					err = storage.Status().
						Update(t.Context(), object, commonstore.WithFieldRequirements(requirements...))
				case "patch", "status patch":
					options := []commonstore.PatchOption{commonstore.WithFieldRequirements(requirements...)}
					if preconditions != nil {
						options = append(options, commonstore.WithPreconditions(*preconditions))
					}
					if operation == "patch" {
						err = storage.Patch(t.Context(), object, patch, options...)
					} else {
						err = storage.Status().
							Patch(t.Context(), object, patch, options...)
					}
				case "delete":
					options := []commonstore.DeleteOption{commonstore.WithFieldRequirements(requirements...)}
					if preconditions != nil {
						options = append(options, commonstore.WithPreconditions(*preconditions))
					}
					err = storage.Delete(t.Context(), object, options...)
				}
				if condition == "missing versioned object" {
					if !commonerrors.IsNotFound(err) {
						t.Fatalf("conditional %s = %v, want NotFound", operation, err)
					}
				} else if !commonerrors.IsConflict(err) {
					t.Fatalf("conditional %s = %v, want Conflict", operation, err)
				}
				after := &Object{}
				err = storage.Get(t.Context(), object.ID, after)
				if condition == "missing versioned object" {
					if !commonerrors.IsNotFound(err) {
						t.Fatalf("failed write created object: %v", err)
					}
				} else if err != nil || after.Value != "before" || after.Status.Phase != "" {
					t.Fatalf("failed write changed object: %#v, %v", after, err)
				}
			})
		}
	}
	t.Run("continuation binds query", func(t *testing.T) {
		if !fixture.Continue {
			t.Skip("pagination unsupported")
		}
		storage := NewStore(t, fixture)
		for _, id := range []string{"a", "b", "c"} {
			if err := storage.Create(t.Context(), &Object{ObjectMeta: commonstore.ObjectMeta{ID: id}, Value: id}); err != nil {
				t.Fatal(err)
			}
		}
		first := &commonstore.List[Object]{}
		if err := storage.List(t.Context(), first, commonstore.WithLimit(1)); err != nil {
			t.Fatal(err)
		}
		if first.Continue == "" || len(first.Items) != 1 {
			t.Fatalf("first page = %#v", first)
		}
		for _, option := range []commonstore.ListOption{commonstore.WithOffset(1), commonstore.WithSort("id-")} {
			if err := storage.List(t.Context(), &commonstore.List[Object]{}, commonstore.WithContinue(first.Continue, 1), option); commonerrors.ReasonForError(err) != commonerrors.StatusReasonBadRequest {
				t.Fatalf("changed query = %v, want BadRequest", err)
			}
		}
		other := storage.Scope(commonstore.Scope{Resource: "tenants", Name: "other"})
		if err := other.List(t.Context(), &commonstore.List[Object]{}, commonstore.WithContinue(first.Continue, 1)); commonerrors.ReasonForError(err) != commonerrors.StatusReasonBadRequest {
			t.Fatalf("changed scope = %v", err)
		}
		second := &commonstore.List[Object]{}
		if err := storage.List(t.Context(), second, commonstore.WithContinue(first.Continue, 2)); err != nil {
			t.Fatal(err)
		}
		if len(second.Items) != 2 || second.Items[0].ID == first.Items[0].ID || second.Continue != "" {
			t.Fatalf("last page = %#v", second)
		}
	})
	t.Run("count uses list search fields", func(t *testing.T) {
		if !fixture.Search {
			t.Skip("search unsupported")
		}
		storage := NewStore(t, fixture)
		for _, value := range []string{"Alpha", "Beta", "alphabet"} {
			if err := storage.Create(t.Context(), &Object{Value: value}); err != nil {
				t.Fatal(err)
			}
		}
		count, err := storage.Count(t.Context(), &Object{}, commonstore.WithSearch("ALPHA"), commonstore.WithSearchFields("value"))
		if err != nil || count != 2 {
			t.Fatalf("Count = %d, %v, want 2", count, err)
		}
		list := &commonstore.List[Object]{}
		if err := storage.List(t.Context(), list, commonstore.WithSearch("ALPHA"), commonstore.WithSearchFields("value")); err != nil {
			t.Fatal(err)
		}
		if len(list.Items) != count {
			t.Fatalf("List and Count differ: %d, %d", len(list.Items), count)
		}
	})
}
