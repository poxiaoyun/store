package mongo

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	commonerrors "xiaoshiai.cn/common/errors"
	testmongodb "xiaoshiai.cn/common/testkit/mongodb"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/selector"
)

func newFieldPatchStorage(t *testing.T, database *mongo.Database) *MongoStorage {
	t.Helper()
	schema := store.NewSchema()
	if err := schema.Register(&fieldPatchObject{}, store.ResourceSchema{
		StatusResource: true,
	}); err != nil {
		t.Fatalf("register schema: %v", err)
	}
	return newIntegrationStorage(t, database, schema)
}

func TestMongoFieldPatch(t *testing.T) {
	database := RequireIntegrationDatabase(t, testmongodb.RequireURI(t))
	storage := newFieldPatchStorage(t, database)
	object := &fieldPatchObject{
		ObjectMeta: store.ObjectMeta{ID: "account"},
		Balance:    100,
		Note:       "old",
		Nested:     &fieldPatchNested{Count: 1, Keep: "keep"},
	}
	if err := storage.Create(t.Context(), object); err != nil {
		t.Fatal(err)
	}
	patch := store.FieldPatch{Operations: []store.FieldOperation{
		{Op: store.FieldOpInc, Path: "balance", Value: json.Number("-30")},
		{Op: store.FieldOpSet, Path: "note", Value: "new"},
		{Op: store.FieldOpInc, Path: "nested.count", Value: json.Number("2")},
	}}
	if err := storage.Patch(t.Context(), object, patch, store.WithFieldRequirements(
		selector.NewRequirement("balance", selector.GreaterThanOrEqual, int64(30)),
	)); err != nil {
		t.Fatal(err)
	}
	got := &fieldPatchObject{}
	if err := storage.Get(t.Context(), object.ID, got); err != nil {
		t.Fatal(err)
	}
	if got.Balance != 70 || got.Note != "new" || got.Nested == nil || got.Nested.Count != 3 || got.Nested.Keep != "keep" {
		t.Fatalf("field patch result: %#v", got)
	}
	if got.Generation != 2 || got.ResourceVersion != 2 {
		t.Fatalf("metadata after field patch: %#v", got)
	}

	// The guard and the increment are one write: an insufficient balance must
	// leave both the value and the version untouched.
	err := storage.Patch(t.Context(), object, store.FieldPatch{Operations: []store.FieldOperation{
		{Op: store.FieldOpInc, Path: "balance", Value: json.Number("-100")},
	}}, store.WithFieldRequirements(
		selector.NewRequirement("balance", selector.GreaterThanOrEqual, int64(100)),
	))
	if !commonerrors.IsConflict(err) {
		t.Fatalf("insufficient balance: %v", err)
	}
	got = &fieldPatchObject{}
	if err := storage.Get(t.Context(), object.ID, got); err != nil {
		t.Fatal(err)
	}
	if got.Balance != 70 || got.Generation != 2 || got.ResourceVersion != 2 {
		t.Fatalf("failed patch changed the object: %#v", got)
	}

	// A set to the same value advances the version but not generation.
	if err := storage.Patch(t.Context(), object, store.FieldPatch{Operations: []store.FieldOperation{
		{Op: store.FieldOpSet, Path: "balance", Value: json.Number("70")},
	}}); err != nil {
		t.Fatal(err)
	}
	got = &fieldPatchObject{}
	if err := storage.Get(t.Context(), object.ID, got); err != nil {
		t.Fatal(err)
	}
	if got.Generation != 2 || got.ResourceVersion != 3 {
		t.Fatalf("no-op set metadata: %#v", got)
	}

	if err := storage.Patch(t.Context(), object, store.FieldPatch{Operations: []store.FieldOperation{
		{Op: store.FieldOpSet, Path: "balance", Value: json.Number("80")},
	}}); err != nil {
		t.Fatal(err)
	}
	got = &fieldPatchObject{}
	if err := storage.Get(t.Context(), object.ID, got); err != nil {
		t.Fatal(err)
	}
	if got.Generation != 3 || got.Balance != 80 {
		t.Fatalf("changed set metadata: %#v", got)
	}

	if err := storage.Patch(t.Context(), object, store.FieldPatch{Operations: []store.FieldOperation{
		{Op: store.FieldOpUnset, Path: "note"},
	}}); err != nil {
		t.Fatal(err)
	}
	got = &fieldPatchObject{}
	if err := storage.Get(t.Context(), object.ID, got); err != nil {
		t.Fatal(err)
	}
	if got.Note != "" || got.Generation != 4 {
		t.Fatalf("unset result: %#v", got)
	}

	stale := got.ResourceVersion - 1
	err = storage.Patch(t.Context(), object, store.FieldPatch{Operations: []store.FieldOperation{
		{Op: store.FieldOpInc, Path: "balance", Value: json.Number("-1")},
	}}, store.WithPreconditions(store.Preconditions{ResourceVersion: &stale}))
	if !commonerrors.IsConflict(err) {
		t.Fatalf("stale precondition: %v", err)
	}

	if err := storage.Patch(t.Context(), object, store.FieldPatch{Operations: []store.FieldOperation{
		{Op: store.FieldOpSet, Path: "status.phase", Value: "ready"},
	}}); err == nil {
		t.Fatal("status field was accepted through the main resource")
	}
	if err := storage.Status().Patch(t.Context(), object, store.FieldPatch{Operations: []store.FieldOperation{
		{Op: store.FieldOpSet, Path: "status.phase", Value: "ready"},
	}}); err != nil {
		t.Fatal(err)
	}
	got = &fieldPatchObject{}
	if err := storage.Get(t.Context(), object.ID, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != "ready" || got.Balance != 80 || got.Generation != 4 {
		t.Fatalf("status patch result: %#v", got)
	}
}

func TestMongoFieldPatchConcurrentDebits(t *testing.T) {
	database := RequireIntegrationDatabase(t, testmongodb.RequireURI(t))
	storage := newFieldPatchStorage(t, database)
	object := &fieldPatchObject{
		ObjectMeta: store.ObjectMeta{ID: "concurrent"},
		Balance:    100,
	}
	if err := storage.Create(t.Context(), object); err != nil {
		t.Fatal(err)
	}
	const attempts = 200
	var succeeded atomic.Int64
	var rejected atomic.Int64
	var wait sync.WaitGroup
	for range attempts {
		wait.Add(1)
		go func() {
			defer wait.Done()
			err := storage.Patch(t.Context(), &fieldPatchObject{
				ObjectMeta: store.ObjectMeta{ID: object.ID},
			}, store.FieldPatch{Operations: []store.FieldOperation{
				{Op: store.FieldOpInc, Path: "balance", Value: json.Number("-1")},
			}}, store.WithFieldRequirements(
				selector.NewRequirement("balance", selector.GreaterThanOrEqual, int64(1)),
			))
			switch {
			case err == nil:
				succeeded.Add(1)
			case commonerrors.IsConflict(err):
				rejected.Add(1)
			default:
				t.Errorf("unexpected concurrent patch error: %v", err)
			}
		}()
	}
	wait.Wait()
	if succeeded.Load() != 100 || rejected.Load() != 100 {
		t.Fatalf("succeeded=%d rejected=%d", succeeded.Load(), rejected.Load())
	}
	got := &fieldPatchObject{}
	if err := storage.Get(t.Context(), object.ID, got); err != nil {
		t.Fatal(err)
	}
	if got.Balance != 0 || got.Generation != 101 || got.ResourceVersion != 101 {
		t.Fatalf("concurrent debit result: %#v", got)
	}
}

func TestMongoFieldPatchOrderedOverlap(t *testing.T) {
	database := RequireIntegrationDatabase(t, testmongodb.RequireURI(t))
	storage := newFieldPatchStorage(t, database)
	object := &fieldPatchObject{
		ObjectMeta: store.ObjectMeta{ID: "ordered"},
		Balance:    100,
		Nested:     &fieldPatchNested{Count: 1, Keep: "keep"},
	}
	if err := storage.Create(t.Context(), object); err != nil {
		t.Fatal(err)
	}
	patch := store.FieldPatch{Operations: []store.FieldOperation{
		{Op: store.FieldOpSet, Path: "balance", Value: json.Number("100")},
		{Op: store.FieldOpInc, Path: "balance", Value: json.Number("-30")},
		{Op: store.FieldOpSet, Path: "nested", Value: map[string]any{"count": int64(5), "keep": "keep"}},
		{Op: store.FieldOpInc, Path: "nested.count", Value: json.Number("2")},
	}}
	if err := storage.Patch(t.Context(), object, patch); err != nil {
		t.Fatal(err)
	}
	got := &fieldPatchObject{}
	if err := storage.Get(t.Context(), object.ID, got); err != nil {
		t.Fatal(err)
	}
	if got.Balance != 70 || got.Nested == nil || got.Nested.Count != 7 || got.Nested.Keep != "keep" {
		t.Fatalf("ordered overlap result: %#v", got)
	}
	if got.Generation != 2 || got.ResourceVersion != 2 {
		t.Fatalf("ordered overlap metadata: %#v", got)
	}

	reversed := store.FieldPatch{Operations: []store.FieldOperation{
		{Op: store.FieldOpInc, Path: "balance", Value: json.Number("5")},
		{Op: store.FieldOpSet, Path: "balance", Value: json.Number("10")},
	}}
	if err := storage.Patch(t.Context(), object, reversed); err != nil {
		t.Fatal(err)
	}
	got = &fieldPatchObject{}
	if err := storage.Get(t.Context(), object.ID, got); err != nil {
		t.Fatal(err)
	}
	if got.Balance != 10 {
		t.Fatalf("later set did not overwrite earlier increment: %#v", got)
	}
}

func TestMongoValidateFieldPatch(t *testing.T) {
	storage := &MongoStorage{}
	definition := store.ResourceSchema{Object: &fieldPatchObject{}, StatusResource: true}
	tests := []struct {
		name    string
		op      store.FieldOperation
		status  bool
		wantErr bool
	}{
		{"business field", store.FieldOperation{Op: store.FieldOpSet, Path: "balance", Value: int64(1)}, false, false},
		{"resource version", store.FieldOperation{Op: store.FieldOpSet, Path: "resourceVersion", Value: int64(1)}, false, true},
		{"generation", store.FieldOperation{Op: store.FieldOpInc, Path: "generation", Value: int64(1)}, false, true},
		{"finalizers", store.FieldOperation{Op: store.FieldOpSet, Path: "finalizers", Value: []any{}}, false, true},
		{"status through resource", store.FieldOperation{Op: store.FieldOpSet, Path: "status.phase", Value: "ready"}, false, true},
		{"status through status", store.FieldOperation{Op: store.FieldOpSet, Path: "status.phase", Value: "ready"}, true, false},
		{"business through status", store.FieldOperation{Op: store.FieldOpSet, Path: "balance", Value: int64(1)}, true, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := storage.validateFieldPatch(definition, []store.FieldOperation{test.op}, test.status)
			if (err != nil) != test.wantErr {
				t.Fatalf("validateFieldPatch() = %v, wantErr %v", err, test.wantErr)
			}
		})
	}
}

func TestBuildFieldPatchUpdate(t *testing.T) {
	classic := buildFieldPatchUpdate([]store.FieldOperation{
		{Op: store.FieldOpInc, Path: "balance", Value: json.Number("-30")},
		{Op: store.FieldOpSet, Path: "note", Value: "new"},
		{Op: store.FieldOpUnset, Path: "old"},
	}, nil)
	document, ok := classic.(bson.D)
	if !ok {
		t.Fatalf("classic update type: %T", classic)
	}
	if len(document) != 3 {
		t.Fatalf("classic update: %#v", document)
	}
	generated := buildFieldPatchUpdate([]store.FieldOperation{
		{Op: store.FieldOpInc, Path: "balance", Value: json.Number("-30")},
		{Op: store.FieldOpSet, Path: "note", Value: "new"},
	}, []string{"balance"})
	pipeline, ok := generated.(mongo.Pipeline)
	if !ok {
		t.Fatalf("generation update type: %T", generated)
	}
	if len(pipeline) != 6 {
		t.Fatalf("generation pipeline: %#v", pipeline)
	}
	if pipeline[0][0].Key != "$set" || pipeline[len(pipeline)-1][0].Key != "$unset" {
		t.Fatalf("generation pipeline stages: %#v", pipeline)
	}
	ordered := buildFieldPatchUpdate([]store.FieldOperation{
		{Op: store.FieldOpSet, Path: "balance", Value: json.Number("10")},
		{Op: store.FieldOpInc, Path: "balance", Value: json.Number("5")},
	}, nil)
	orderedPipeline, ok := ordered.(mongo.Pipeline)
	if !ok {
		t.Fatalf("ordered update type: %T", ordered)
	}
	if len(orderedPipeline) != 3 {
		t.Fatalf("ordered pipeline: %#v", orderedPipeline)
	}
}
