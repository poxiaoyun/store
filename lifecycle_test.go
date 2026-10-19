package store_test

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/google/uuid"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/inmemory"
)

func TestPrepareObjectForCreateOwnsServerMetadata(t *testing.T) {
	object := &store.ObjectMeta{UID: "caller", ResourceVersion: 7, Generation: 9}
	scopes := []store.Scope{{Resource: "tenants", Name: "acme"}}
	store.PrepareObjectForCreate(object, "widgets", scopes)
	if _, err := uuid.Parse(object.ID); err != nil {
		t.Fatalf("ID = %q, want UUID: %v", object.ID, err)
	}
	if _, err := uuid.Parse(object.UID); err != nil {
		t.Fatalf("UID = %q, want UUID: %v", object.UID, err)
	}
	if object.ResourceVersion != 0 || object.Generation != 1 || object.CreationTimestamp.IsZero() {
		t.Fatalf("server metadata = %#v", object)
	}
}

type lifecycleCounter struct {
	store.ObjectMeta
	Count  int64   `json:"count"`
	Ratio  float64 `json:"ratio"`
	Status struct {
		Observed int64 `json:"observed"`
	} `json:"status"`
}

func TestPrepareObjectForUpdatePreservesLargeIntegerChanges(t *testing.T) {
	current := &lifecycleCounter{Count: 9007199254740992, Ratio: 1.5}
	current.Status.Observed = 9007199254740993
	store.PrepareObjectForCreate(current, "lifecyclecounters", nil)
	current.ResourceVersion = 1
	desired := *current
	desired.Count++
	desired.Status.Observed = 0
	schema := store.NewSchema()
	if err := schema.Register(&lifecycleCounter{}, store.ResourceSchema{GenerationFields: []string{"count", "ratio"}}); err != nil {
		t.Fatal(err)
	}
	deleteNow, err := schema.PrepareObjectForUpdate(current, &desired, false)
	if err != nil {
		t.Fatal(err)
	}
	if deleteNow || desired.Generation != 2 || desired.Count != 9007199254740993 || desired.Ratio != 1.5 {
		t.Fatalf("integer update lost value or generation: delete=%v count=%d generation=%d ratio=%v", deleteNow, desired.Count, desired.Generation, desired.Ratio)
	}
	if desired.Status.Observed != current.Status.Observed {
		t.Fatalf("normal update corrupted retained status: got %d, want %d", desired.Status.Observed, current.Status.Observed)
	}
}

func TestGenerationTrackingComparesExactJSONNumbers(t *testing.T) {
	for _, test := range []struct {
		name    string
		current any
		desired any
		equal   bool
	}{
		{"adjacent integers", int64(9007199254740992), int64(9007199254740993), false},
		{"integer widths", int(12), int64(12), true},
		{"decimal spelling", json.Number("1.00"), json.Number("1e0"), true},
		{"fraction spelling", json.Number("0.125"), json.Number("1.25e-1"), true},
		{"negative zero", json.Number("-0.0"), int64(0), true},
		{"string is not a number", "12", int64(12), false},
		{"boolean is not a number", true, int64(1), false},
		{"explicit null differs", nil, int64(0), false},
		{"nested equal", []any{map[string]any{"number": json.Number("1.0")}}, []any{map[string]any{"number": json.Number("1")}}, true},
		{"nested changed", []any{int64(9007199254740993)}, []any{int64(9007199254740992)}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			schema := store.NewSchema()
			model := &store.Unstructured{Object: map[string]any{"resource": "numbers"}}
			if err := schema.Register(model, store.ResourceSchema{GenerationFields: []string{"value"}}); err != nil {
				t.Fatal(err)
			}
			current := &store.Unstructured{Object: map[string]any{"resource": "numbers", "value": test.current, "generation": int64(3), "status": "old"}}
			desired := &store.Unstructured{Object: map[string]any{"resource": "numbers", "value": test.desired, "generation": int64(4), "status": "new"}}
			if _, err := schema.PrepareObjectForUpdate(current, desired, false); err != nil {
				t.Fatal(err)
			}
			if equal := desired.GetGeneration() == 3; equal != test.equal {
				t.Fatalf("tracked equality = %v, want %v", equal, test.equal)
			}

		})
	}
}

func TestStoreCreateUpdatePreservesIntegerFactsAndGeneration(t *testing.T) {
	schema := store.NewSchema()
	if err := schema.Register(&lifecycleCounter{}, store.ResourceSchema{GenerationFields: []string{"count", "ratio"}}); err != nil {
		t.Fatal(err)
	}
	storage, err := inmemory.New(schema)
	if err != nil {
		t.Fatal(err)
	}
	object := &lifecycleCounter{Count: 9007199254740992, Ratio: 1.5}
	object.Status.Observed = math.MaxInt64
	if err := storage.Create(t.Context(), object); err != nil {
		t.Fatal(err)
	}
	if object.Generation != 1 || object.Count != 9007199254740992 || object.Status.Observed != math.MaxInt64 {
		t.Fatalf("create corrupted numeric facts: %#v", object)
	}
	object.Count++
	object.Status.Observed = 0
	if err := storage.Update(t.Context(), object); err != nil {
		t.Fatal(err)
	}
	if object.Generation != 2 || object.Count != 9007199254740993 || object.Status.Observed != math.MaxInt64 {
		t.Fatalf("update corrupted numeric facts: %#v", object)
	}
	object.Labels = map[string]string{"description": "metadata only"}
	if err := storage.Update(t.Context(), object); err != nil {
		t.Fatal(err)
	}
	if object.Generation != 2 || object.Count != 9007199254740993 {
		t.Fatalf("metadata update changed business generation or facts: %#v", object)
	}
	statusStorage := storage.Status()
	object.Count = 0
	object.Status.Observed--
	if err := statusStorage.Update(t.Context(), object); err != nil {
		t.Fatal(err)
	}
	stored := &lifecycleCounter{}
	if err := storage.Get(t.Context(), object.ID, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Generation != 2 || stored.Count != 9007199254740993 || stored.Status.Observed != math.MaxInt64-1 || stored.Ratio != 1.5 {
		t.Fatalf("status update corrupted stored facts: %#v", stored)
	}
}
