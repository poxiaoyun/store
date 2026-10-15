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
	"xiaoshiai.cn/common/selector"
	commonstore "xiaoshiai.cn/store"
)

// Object is the resource used by the conformance suite.
type Object struct {
	commonstore.ObjectMeta `json:",inline"`
	Value                  string       `json:"value,omitempty"`
	Rank                   int          `json:"rank,omitempty"`
	Tenant                 string       `json:"tenant,omitempty"`
	Status                 ObjectStatus `json:"status,omitempty"`
}

// ResourceName implements commonstore.ResourcedObject.
func (*Object) ResourceName() string { return "storetests" }

type schemaMutationObject struct {
	commonstore.ObjectMeta `json:",inline"`
}

func (*schemaMutationObject) ResourceName() string { return "storetestmutations" }

// ObjectStatus is the status subresource used by the conformance suite.
type ObjectStatus struct {
	Phase string `json:"phase,omitempty"`
}

// Factory creates an isolated Store for one conformance subtest.
type Factory func(t testing.TB, schema *commonstore.Schema) (commonstore.Store, error)

// Fixture connects one Store implementation to the conformance suite.
type Fixture struct {
	New          Factory
	Capabilities commonstore.Capabilities
}

// Run executes the shared Store contract against fixture.
func Run(t *testing.T, fixture Fixture) {
	t.Helper()
	t.Run("capabilities", func(t *testing.T) {
		storage := NewStore(t, fixture)
		if got := storage.Capabilities(); !reflect.DeepEqual(got, fixture.Capabilities) {
			t.Fatalf("Capabilities() = %#v, want %#v", got, fixture.Capabilities)
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
		if err := storage.Update(t.Context(), object); err != nil {
			t.Fatalf("entering Update() error = %v", err)
		}
		AssertWatchEvent(t, NextObjectWatchEvent(t, watcher), commonstore.WatchEventCreate, "transition")

		object.Value = "matching"
		if err := storage.Update(t.Context(), object); err != nil {
			t.Fatalf("matching Update() error = %v", err)
		}
		AssertWatchEvent(t, NextObjectWatchEvent(t, watcher), commonstore.WatchEventUpdate, "transition")

		object.Rank = 9
		if err := storage.Update(t.Context(), object); err != nil {
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
		if !fixture.Capabilities.Watch || !fixture.Capabilities.FieldSelector {
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
		if err := storage.Update(t.Context(), object); err != nil {
			t.Fatalf("entering Update() error = %v", err)
		}
		created := NextObjectWatchEvent(t, watcher)
		AssertWatchEvent(t, created, commonstore.WatchEventCreate, "comparison-transition")
		if entered := created.Object.(*Object); entered.Rank != 8 {
			t.Fatalf("Create transition object rank = %d, want 8", entered.Rank)
		}

		object.Rank = 9
		if err := storage.Update(t.Context(), object); err != nil {
			t.Fatalf("matching Update() error = %v", err)
		}
		updated := NextObjectWatchEvent(t, watcher)
		AssertWatchEvent(t, updated, commonstore.WatchEventUpdate, "comparison-transition")
		if current := updated.Object.(*Object); current.Rank != 9 {
			t.Fatalf("Update transition object rank = %d, want 9", current.Rank)
		}

		object.Rank = 7
		if err := storage.Update(t.Context(), object); err != nil {
			t.Fatalf("leaving Update() error = %v", err)
		}
		deleted := NextObjectWatchEvent(t, watcher)
		AssertWatchEvent(t, deleted, commonstore.WatchEventDelete, "comparison-transition")
		if previous := deleted.Object.(*Object); previous.Rank != 9 {
			t.Fatalf("Delete transition object rank = %d, want 9", previous.Rank)
		}
	})
	t.Run("field comparisons use typed values before pagination", func(t *testing.T) {
		if !fixture.Capabilities.FieldSelector {
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
				if list.Total == nil || *list.Total != len(test.wantIDs) {
					t.Fatalf("List() total = %v, want %d", list.Total, len(test.wantIDs))
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
			if list.Total == nil || *list.Total != 2 {
				t.Fatalf("limited List() metadata = %#v", list)
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
		if err := storage.Status().Patch(t.Context(), got, commonstore.MapMergePatch{"status": map[string]any{"phase": "invalid"}}, commonstore.WithPreconditions(commonstore.Preconditions{UID: &uid})); !commonerrors.IsConflict(err) {
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
		if err := storage.Update(t.Context(), object); err != nil {
			t.Fatal(err)
		}
		if err := storage.Get(t.Context(), object.ID, &Object{}, condition); !commonerrors.IsConflict(err) {
			t.Fatalf("stale Get = %v, want Conflict", err)
		}
		if err := storage.Get(t.Context(), object.ID, &Object{}, condition, commonstore.WithFields("value")); !commonerrors.IsConflict(err) {
			t.Fatalf("projected stale Get = %v, want Conflict", err)
		}
		currentVersion := object.ResourceVersion
		if err := storage.Get(t.Context(), object.ID, &Object{}, commonstore.WithPreconditions(commonstore.Preconditions{UID: &observedUID, ResourceVersion: &currentVersion}), commonstore.WithFields("value")); err != nil {
			t.Fatalf("projected current Get = %v", err)
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
		if err := storage.Update(t.Context(), object); err != nil {
			t.Fatalf("Update() error = %v", err)
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
		if err := storage.Update(t.Context(), object); err != nil {
			t.Fatalf("metadata Update() error = %v", err)
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
		if err := storage.Status().Update(t.Context(), object); err != nil {
			t.Fatalf("Status().Update() error = %v", err)
		}
		if object.Value != "current" || !reflect.DeepEqual(object.Labels, map[string]string{"team": "blue"}) || object.Status.Phase != "ready" {
			t.Fatalf("Status().Update() = %#v", object)
		}
		if object.Generation != generation || object.ResourceVersion <= version {
			t.Fatalf("Status().Update() generation/version = (%d, %d)", object.Generation, object.ResourceVersion)
		}
	})
	t.Run("delete honors finalizers", func(t *testing.T) {
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
		if err := storage.Update(t.Context(), terminating); err != nil {
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
		if err := storage.Delete(t.Context(), object, commonstore.WithResourceVersion(object.ResourceVersion+1)); !commonerrors.IsConflict(err) {
			t.Fatalf("stale ResourceVersion Delete() error = %v, want Conflict", err)
		}
		if err := storage.Delete(t.Context(), object,
			commonstore.WithUID("stale"),
			commonstore.WithLabelRequirements(selector.RequirementEqual("team", "red")),
		); !commonerrors.IsConflict(err) {
			t.Fatalf("precondition ordering Delete() error = %v, want Conflict", err)
		}
		if err := storage.Delete(t.Context(), object,
			commonstore.WithUID(object.UID),
			commonstore.WithLabelRequirements(selector.RequirementEqual("team", "red")),
		); !commonerrors.IsNotFound(err) {
			t.Fatalf("requirement mismatch Delete() error = %v, want NotFound", err)
		}
		if err := storage.Delete(t.Context(), object,
			commonstore.WithUID(object.UID),
			commonstore.WithResourceVersion(object.ResourceVersion),
		); err != nil {
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
		if object.Value != "two" || object.Status.Phase != "pending" || object.Generation != 2 {
			t.Fatalf("Patch() = %#v", object)
		}
		generation := object.Generation
		if err := storage.Status().Patch(t.Context(), object, commonstore.MapMergePatch{
			"value":  "must-not-change",
			"status": map[string]any{"phase": "ready"},
		}); err != nil {
			t.Fatalf("Status().Patch() error = %v", err)
		}
		if object.Value != "two" || object.Status.Phase != "ready" || object.Generation != generation {
			t.Fatalf("Status().Patch() = %#v", object)
		}
	})
	t.Run("merge patch rejects stale resource version", func(t *testing.T) {
		storage := NewStore(t, fixture)
		object := &Object{Value: "one"}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		staleVersion := object.ResourceVersion
		object.Value = "two"
		if err := storage.Update(t.Context(), object); err != nil {
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
		if err := storage.Update(t.Context(), object); err != nil {
			t.Fatalf("Update() error = %v", err)
		}
		if err := storage.Patch(t.Context(), stale, commonstore.MapMergePatch{"value": "two"}); err != nil {
			t.Fatalf("Patch() error = %v", err)
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
		storage := NewStore(t, fixture)
		object := &Object{Value: "one"}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		patch := commonstore.RawPatch(commonstore.PatchTypeJSONPatch, []byte(fmt.Sprintf(`[
            {"op":"test","path":"/resourceVersion","value":%d},
            {"op":"replace","path":"/value","value":"two"}
        ]`, object.ResourceVersion)))
		if err := storage.Patch(t.Context(), object, patch); err != nil {
			t.Fatalf("Patch() error = %v", err)
		}
		if object.Value != "two" {
			t.Fatalf("Patch() value = %q, want two", object.Value)
		}
	})
	t.Run("JSON patch rejects stale resource version test", func(t *testing.T) {
		storage := NewStore(t, fixture)
		object := &Object{Value: "one"}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		staleVersion := object.ResourceVersion
		object.Value = "two"
		if err := storage.Update(t.Context(), object); err != nil {
			t.Fatalf("Update() error = %v", err)
		}
		patch := commonstore.RawPatch(commonstore.PatchTypeJSONPatch, []byte(fmt.Sprintf(`[
            {"op":"test","path":"/resourceVersion","value":%d},
            {"op":"replace","path":"/value","value":"stale"}
        ]`, staleVersion)))
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
		storage := NewStore(t, fixture)
		object := &Object{Status: ObjectStatus{Phase: "pending"}}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatalf("Create() error = %v", err)
		}
		staleVersion := object.ResourceVersion
		object.Status.Phase = "ready"
		if err := storage.Status().Update(t.Context(), object); err != nil {
			t.Fatalf("Status().Update() error = %v", err)
		}
		err := storage.Status().Patch(t.Context(), object, commonstore.MapMergePatch{
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
		if err := storage.Update(t.Context(), object); err != nil {
			t.Fatalf("Update() error = %v", err)
		}
		stale.Value = "stale"
		err := storage.Update(t.Context(), stale)
		if fixture.Capabilities.OptimisticLock {
			if !commonerrors.IsConflict(err) {
				t.Fatalf("stale Update() error = %v, want Conflict", err)
			}
		} else if err != nil {
			t.Fatalf("stale Update() error = %v", err)
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
		if fixture.Capabilities.SubScopes {
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
		if fixture.Capabilities.SubScopes {
			list = &commonstore.List[Object]{Resource: "storetests"}
			if err := acme.List(t.Context(), list, commonstore.WithSubScopes()); err != nil {
				t.Fatalf("subscope List() error = %v", err)
			}
			ids := ObjectIDs(list.Items)
			sort.Strings(ids)
			if !reflect.DeepEqual(ids, []string{"child", "scoped"}) {
				t.Fatalf("subscope List() IDs = %v", ids)
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
		RunQueryCapabilities(t, fixture, storage)
	})
	t.Run("unique indexes", func(t *testing.T) {
		if !fixture.Capabilities.UniqueIndexes {
			t.Skip("unique indexes are not declared")
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
		if !fixture.Capabilities.Transaction {
			t.Skip("transactions are not declared")
		}
		storage := NewStore(t, fixture)
		transactional, ok := storage.(commonstore.TransactionStore)
		if !ok {
			t.Fatal("the Transaction capability is declared without a TransactionStore")
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

	t.Run("batch patch and delete apply to matching objects", func(t *testing.T) {
		if !fixture.Capabilities.PatchBatch || !fixture.Capabilities.DeleteBatch {
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
		if err := storage.PatchBatch(t.Context(), &commonstore.List[Object]{},
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
		if err := storage.DeleteBatch(t.Context(), &commonstore.List[Object]{},
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
		if err := storage.Patch(t.Context(), copied, copyPatch); err != nil {
			t.Fatalf("JSON patch copy error = %v", err)
		}
		if copied.Tenant != "one" || copied.Value != "one" {
			t.Fatalf("JSON patch copy result = %#v", copied)
		}
		moved := &Object{ObjectMeta: commonstore.ObjectMeta{ID: "ops"}}
		movePatch := commonstore.RawPatch(commonstore.PatchTypeJSONPatch, []byte(`[{"op":"move","from":"/tenant","path":"/value"}]`))
		if err := storage.Patch(t.Context(), moved, movePatch); err != nil {
			t.Fatalf("JSON patch move error = %v", err)
		}
		if moved.Value != "one" || moved.Tenant != "" {
			t.Fatalf("JSON patch move result = %#v", moved)
		}
	})
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
	if got := storage.Schema().Resources(); !reflect.DeepEqual(got, []string{"storetests"}) {
		t.Fatalf("Schema() shares mutable state with Store: resources = %v", got)
	}
}

// OpenWatcher waits for a Store adapter's asynchronous backend initialization.
func OpenWatcher(t testing.TB, open func() (commonstore.Watcher, error)) commonstore.Watcher {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var lastErr error
	for {
		watcher, err := open()
		if err == nil {
			return watcher
		}
		lastErr = err
		select {
		case <-deadline.C:
			t.Fatalf("Watch() did not become ready: %v", lastErr)
			return nil
		case <-ticker.C:
		}
	}
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

// RunQueryCapabilities verifies behavior declared by the fixture.
func RunQueryCapabilities(t *testing.T, fixture Fixture, storage commonstore.Store) {
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
		name      string
		enabled   bool
		options   []commonstore.ListOption
		wantIDs   []string
		wantTotal int
		ordered   bool
	}{
		{name: "label selector", enabled: fixture.Capabilities.LabelSelector, options: []commonstore.ListOption{commonstore.WithLabelRequirements(selector.RequirementEqual("example.com/team", "blue"))}, wantIDs: []string{"a", "b"}},
		{name: "field selector", enabled: fixture.Capabilities.FieldSelector, options: []commonstore.ListOption{commonstore.WithFieldRequirements(selector.RequirementEqual("rank", 2))}, wantIDs: []string{"b"}},
		{name: "composed field selector", enabled: fixture.Capabilities.FieldSelector, options: []commonstore.ListOption{commonstore.WithFieldRequirements(fieldComposition)}, wantIDs: []string{"a", "c"}},
		{name: "composed label selector", enabled: fixture.Capabilities.LabelSelector, options: []commonstore.ListOption{commonstore.WithLabelRequirements(labelComposition)}, wantIDs: []string{"a", "c"}},
		{name: "none selector", enabled: fixture.Capabilities.FieldSelector, options: []commonstore.ListOption{commonstore.WithFieldRequirements(selector.Requirement{})}, wantIDs: []string{}},
		{name: "empty in selector", enabled: fixture.Capabilities.FieldSelector, options: []commonstore.ListOption{commonstore.WithFieldRequirements(selector.NewRequirement("rank", selector.In))}, wantIDs: []string{}},
		{name: "empty not-in selector", enabled: fixture.Capabilities.FieldSelector, options: []commonstore.ListOption{commonstore.WithFieldRequirements(selector.NewRequirement("rank", selector.NotIn))}, wantIDs: []string{}},
		{name: "all selector", enabled: fixture.Capabilities.FieldSelector, options: []commonstore.ListOption{commonstore.WithFieldRequirements(selector.Requirement{Operator: selector.All})}, wantIDs: []string{"a", "b", "c"}},
		{name: "special label keys", enabled: fixture.Capabilities.LabelSelector, options: []commonstore.ListOption{commonstore.WithLabelRequirements(selector.RequirementEqual("owner.name", "alice"), selector.RequirementEqual("$owner", "alice"))}, wantIDs: []string{"a"}},
		{name: "search by name", enabled: fixture.Capabilities.Search, options: []commonstore.ListOption{commonstore.WithSearch("needle")}, wantIDs: []string{"a"}},
		{name: "search by ID", enabled: fixture.Capabilities.Search, options: []commonstore.ListOption{commonstore.WithSearch("c")}, wantIDs: []string{"c"}},
		{name: "explicit search fields", enabled: fixture.Capabilities.Search, options: []commonstore.ListOption{commonstore.WithSearch("c"), commonstore.WithSearchFields("name")}, wantIDs: []string{}},
		{name: "indexed sort", enabled: fixture.Capabilities.Sort, options: []commonstore.ListOption{commonstore.WithSort("rank-")}, wantIDs: []string{"c", "b", "a"}, ordered: true},
		{name: "window limit", enabled: true, options: []commonstore.ListOption{commonstore.WithSort("id+"), commonstore.WithLimit(2)}, wantIDs: []string{"a", "b"}, wantTotal: 3, ordered: true},
		{name: "window offset", enabled: true, options: []commonstore.ListOption{commonstore.WithSort("id+"), commonstore.WithOffset(1), commonstore.WithLimit(1)}, wantIDs: []string{"b"}, wantTotal: 3, ordered: true},
		{name: "window offset past end", enabled: true, options: []commonstore.ListOption{commonstore.WithSort("id+"), commonstore.WithOffset(3)}, wantIDs: []string{}, wantTotal: 3, ordered: true},
	}
	for _, test := range tests {
		if !test.enabled {
			continue
		}
		t.Run(test.name, func(t *testing.T) {
			list := &commonstore.List[Object]{Resource: "storetests"}
			err := storage.List(t.Context(), list, test.options...)
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
			wantTotal := test.wantTotal
			if wantTotal == 0 {
				wantTotal = len(test.wantIDs)
			}
			if list.Total == nil || *list.Total != wantTotal || list.Continue != "" {
				t.Fatalf("List() metadata = %#v, want total %d and no cursor", list, wantTotal)
			}
		})
	}
	if fixture.Capabilities.FieldSelector {
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
			if list.Total == nil || *list.Total != 2 {
				t.Fatalf("composed limited List() total = %v, want 2", list.Total)
			}
		}
	}
	if fixture.Capabilities.Projection {
		projected := &Object{}
		if err := storage.Get(t.Context(), "a", projected, commonstore.WithFields("id", "value")); err != nil {
			t.Fatalf("projection Get() error = %v", err)
		}
		if projected.ID != "a" || projected.Value != "first" || projected.Name != "" || projected.Rank != 0 {
			t.Fatalf("projection Get() = %#v", projected)
		}
	}
	if fixture.Capabilities.Continue {
		seen := map[string]bool{}
		continueToken := ""
		for {
			list := &commonstore.List[Object]{Resource: "storetests"}
			options := []commonstore.ListOption{commonstore.WithContinue(continueToken, 1)}
			if err := storage.List(t.Context(), list, options...); err != nil {
				t.Fatalf("cursor List() error = %v", err)
			}
			if list.Total != nil {
				t.Fatalf("cursor List() metadata = %#v", list)
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
		if fixture.Capabilities.ContinueWithSort {
			list := &commonstore.List[Object]{Resource: "storetests"}
			if err := storage.List(t.Context(), list, commonstore.WithContinue("", 1), commonstore.WithSort("id+")); err != nil {
				t.Fatalf("cursor with sort List() error = %v", err)
			}
			if list.Total != nil {
				t.Fatalf("cursor with sort List() metadata = %#v", list)
			}
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
	definition := commonstore.ResourceSchema{ScopeKeys: []string{"tenant"}}
	if fixture.Capabilities.SecondaryIndexes {
		definition.Indexes = append(definition.Indexes, commonstore.Index{Name: "rank", Fields: []string{"rank"}})
	}
	if fixture.Capabilities.UniqueIndexes {
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
