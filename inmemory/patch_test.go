package inmemory

import (
	"encoding/json"
	"testing"

	"xiaoshiai.cn/store"
)

type fieldPatchTestObject struct {
	store.ObjectMeta `json:",inline"`
	Balance          int64                `json:"balance"`
	Optional         int64                `json:"optional,omitempty"`
	Note             *string              `json:"note,omitempty"`
	Nested           fieldPatchTestNested `json:"nested"`
}

type fieldPatchTestNested struct {
	Count int    `json:"count"`
	Keep  string `json:"keep"`
}

func TestFieldPatchAppliesRelativeOperations(t *testing.T) {
	note := "old"
	object := &fieldPatchTestObject{
		ObjectMeta: store.ObjectMeta{ResourceVersion: 7},
		Balance:    100,
		Note:       &note,
		Nested:     fieldPatchTestNested{Count: 1, Keep: "keep"},
	}
	patch := store.FieldPatch{Operations: []store.FieldOperation{
		{Op: store.FieldOpInc, Path: "balance", Value: json.Number("-30")},
		{Op: store.FieldOpInc, Path: "optional", Value: json.Number("5")},
		{Op: store.FieldOpSet, Path: "nested.count", Value: json.Number("5")},
		{Op: store.FieldOpUnset, Path: "note"},
	}}
	if err := ApplyPatch(object, object, patch); err != nil {
		t.Fatal(err)
	}
	if object.Balance != 70 || object.Optional != 5 || object.Nested.Count != 5 {
		t.Fatalf("relative operations were not applied: %#v", object)
	}
	if object.Note != nil {
		t.Fatalf("unset left note in place: %#v", object.Note)
	}
	if object.Nested.Keep != "keep" {
		t.Fatalf("unset removed a sibling: %#v", object.Nested)
	}
	if object.ResourceVersion != 7 {
		t.Fatalf("patch changed resourceVersion: %d", object.ResourceVersion)
	}
}

func TestFieldPatchRejectsInvalidOperations(t *testing.T) {
	tests := []struct {
		name string
		data string
	}{
		{"unknown op", `{"operations":[{"op":"move","path":"balance","value":1}]}`},
		{"non numeric inc", `{"operations":[{"op":"inc","path":"balance","value":"1"}]}`},
		{"empty operations", `{"operations":[]}`},
		{"invalid path", `{"operations":[{"op":"set","path":"$balance","value":1}]}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			object := &fieldPatchTestObject{ObjectMeta: store.ObjectMeta{ResourceVersion: 1}}
			patch := store.RawPatch(store.PatchTypeFieldPatch, []byte(test.data))
			if err := ApplyPatch(object, object, patch); err == nil {
				t.Fatal("invalid field patch was accepted")
			}
		})
	}
}

func TestFieldPatchAppliesOverlappingOperationsInOrder(t *testing.T) {
	object := &fieldPatchTestObject{
		ObjectMeta: store.ObjectMeta{ResourceVersion: 1},
		Balance:    100,
		Nested:     fieldPatchTestNested{Count: 1, Keep: "keep"},
	}
	patch := store.FieldPatch{Operations: []store.FieldOperation{
		{Op: store.FieldOpSet, Path: "nested", Value: map[string]any{"count": json.Number("5"), "keep": "keep"}},
		{Op: store.FieldOpInc, Path: "nested.count", Value: json.Number("2")},
		{Op: store.FieldOpSet, Path: "balance", Value: json.Number("10")},
		{Op: store.FieldOpInc, Path: "balance", Value: json.Number("5")},
	}}
	if err := ApplyPatch(object, object, patch); err != nil {
		t.Fatal(err)
	}
	if object.Balance != 15 || object.Nested.Count != 7 || object.Nested.Keep != "keep" {
		t.Fatalf("overlapping operations were not applied in order: %#v", object)
	}

	reversed := &fieldPatchTestObject{
		ObjectMeta: store.ObjectMeta{ResourceVersion: 1},
		Balance:    100,
	}
	reversedPatch := store.FieldPatch{Operations: []store.FieldOperation{
		{Op: store.FieldOpInc, Path: "balance", Value: json.Number("5")},
		{Op: store.FieldOpSet, Path: "balance", Value: json.Number("10")},
	}}
	if err := ApplyPatch(reversed, reversed, reversedPatch); err != nil {
		t.Fatal(err)
	}
	if reversed.Balance != 10 {
		t.Fatalf("later set did not overwrite earlier increment: %#v", reversed)
	}
}

func TestFieldPatchIncrementRejectsNonNumericTarget(t *testing.T) {
	object := &fieldPatchTestObject{
		ObjectMeta: store.ObjectMeta{ResourceVersion: 1},
		Balance:    10,
	}
	note := "text"
	object.Note = &note
	patch := store.FieldPatch{Operations: []store.FieldOperation{
		{Op: store.FieldOpInc, Path: "note", Value: json.Number("1")},
	}}
	if err := ApplyPatch(object, object, patch); err == nil {
		t.Fatal("incrementing a string field was accepted")
	}
}

func TestPatchClearsRemovedFields(t *testing.T) {
	for _, kind := range []string{"merge", "json"} {
		t.Run(kind, func(t *testing.T) {
			value := "reserved"
			object := &struct {
				Key     *string `json:"key,omitempty"`
				Enabled bool    `json:"enabled,omitempty"`
				Name    string  `json:"name"`
			}{Key: &value, Enabled: true, Name: "retained"}
			var err error
			if kind == "merge" {
				err = JsonMergePatchObject(object, []byte(`{"key":null,"enabled":null}`))
			} else {
				err = JsonPatchObject(object, []byte(`[{"op":"remove","path":"/key"},{"op":"remove","path":"/enabled"}]`))
			}
			if err != nil {
				t.Fatal(err)
			}
			if object.Key != nil || object.Enabled || object.Name != "retained" {
				t.Fatalf("patch did not replace removed fields: %#v", object)
			}
		})
	}
}
