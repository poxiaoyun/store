package sql

import (
	"reflect"
	"testing"

	"xiaoshiai.cn/store"
)

type patchObject struct {
	store.ObjectMeta `json:",inline"`
	Value            string            `json:"value"`
	Rank             int64             `json:"rank"`
	Enabled          bool              `json:"enabled"`
	Extra            map[string]string `json:"extra"`
	Retired          *string           `json:"retired"`
}

func (*patchObject) ResourceName() string { return "patchobjects" }

func patchAssignmentsFor(t *testing.T, patch store.PatchBatch) ([]string, []any) {
	t.Helper()
	storage := &Storage{core: &core{dialect: MySQLDialect{}, codec: NewModelCodec()}}
	q := &QueryState{dialect: storage.core.dialect}
	assignments, err := storage.patchAssignments(q, &store.List[patchObject]{}, patch, map[string]struct{}{"id": {}, "resourceVersion": {}})
	if err != nil {
		t.Fatal(err)
	}
	return assignments, q.Args()
}

func TestPatchAssignmentsFromMergePatch(t *testing.T) {
	assignments, args := patchAssignmentsFor(t, store.MapMergePatchBacth{
		"value":   "updated",
		"rank":    float64(7),
		"retired": nil,
		"id":      "ignored",
	})
	want := []string{"`rank` = ?", "`retired` = NULL", "`value` = ?"}
	if !reflect.DeepEqual(assignments, want) {
		t.Fatalf("assignments = %#v, want %#v", assignments, want)
	}
	if !reflect.DeepEqual(args, []any{float64(7), "updated"}) {
		t.Fatalf("args = %#v", args)
	}
}

func TestPatchAssignmentsFromJSONPatch(t *testing.T) {
	patch := store.RawPatchBatch(store.PatchTypeJSONPatch, []byte(`[
		{"op":"replace","path":"/value","value":"updated"},
		{"op":"remove","path":"/retired"},
		{"op":"add","path":"/extra/team","value":"blue"}
	]`))
	assignments, args := patchAssignmentsFor(t, patch)
	want := []string{
		"`value` = ?",
		"`retired` = NULL",
		"`extra` = JSON_SET(`extra`, '$.\"team\"', ?)",
	}
	if !reflect.DeepEqual(assignments, want) {
		t.Fatalf("assignments = %#v, want %#v", assignments, want)
	}
	if !reflect.DeepEqual(args, []any{"updated", "blue"}) {
		t.Fatalf("args = %#v", args)
	}
}

func TestPatchAssignmentsEditNestedDocument(t *testing.T) {
	storage := &Storage{core: &core{dialect: PostgresDialect{}, codec: NewModelCodec()}}
	q := &QueryState{dialect: storage.core.dialect}
	patch := store.RawPatchBatch(store.PatchTypeJSONPatch, []byte(`[
		{"op":"replace","path":"/extra/team","value":"blue"},
		{"op":"remove","path":"/extra/env"}
	]`))
	assignments, err := storage.patchAssignments(q, &store.List[patchObject]{}, patch, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		`"extra" = jsonb_set("extra", ARRAY['team'], $1::jsonb)`,
		`"extra" = "extra" #- ARRAY['env']`,
	}
	if !reflect.DeepEqual(assignments, want) {
		t.Fatalf("assignments = %#v, want %#v", assignments, want)
	}
	if !reflect.DeepEqual(q.Args(), []any{"blue"}) {
		t.Fatalf("args = %#v", q.Args())
	}
}

func TestPatchAssignmentsRejectUnsupportedPaths(t *testing.T) {
	storage := &Storage{core: &core{dialect: MySQLDialect{}, codec: NewModelCodec()}}
	tests := []struct {
		name  string
		patch store.PatchBatch
	}{
		{
			name:  "unknown field",
			patch: store.MapMergePatchBacth{"missing": "x"},
		},
		{
			name:  "nested path into a scalar field",
			patch: store.RawPatchBatch(store.PatchTypeJSONPatch, []byte(`[{"op":"replace","path":"/value/inner","value":"x"}]`)),
		},
		{
			name:  "array index path",
			patch: store.RawPatchBatch(store.PatchTypeJSONPatch, []byte(`[{"op":"replace","path":"/extra/0","value":"x"}]`)),
		},
		{
			name:  "array append path",
			patch: store.RawPatchBatch(store.PatchTypeJSONPatch, []byte(`[{"op":"add","path":"/extra/-","value":"x"}]`)),
		},
		{
			name:  "unknown operation",
			patch: store.RawPatchBatch(store.PatchTypeJSONPatch, []byte(`[{"op":"move","path":"/value","from":"/rank"}]`)),
		},
		{
			name:  "missing path",
			patch: store.RawPatchBatch(store.PatchTypeJSONPatch, []byte(`[{"op":"remove"}]`)),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			q := &QueryState{dialect: storage.core.dialect}
			if _, err := storage.patchAssignments(q, &store.List[patchObject]{}, test.patch, nil); err == nil {
				t.Fatal("patchAssignments() returned no error")
			}
		})
	}
}
