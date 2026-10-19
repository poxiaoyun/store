package sql

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"xiaoshiai.cn/common/meta"
	testpostgresql "xiaoshiai.cn/common/testkit/postgresql"
	"xiaoshiai.cn/store"
)

func TestScanAllReturnsRowStreamError(t *testing.T) {
	db, err := sql.Open("pgx", testpostgresql.RequireURI(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	// PostgreSQL produces one row before evaluating division by zero. Query
	// succeeds; consuming the row stream must still report the execution error.
	rows, err := db.QueryContext(t.Context(), "SELECT 1/(2-x) AS value FROM generate_series(1,2) x")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var result []struct {
		Value int `json:"value"`
	}
	codec := NewModelCodec()
	err = codec.ScanAll(rows, &result)
	var databaseError *pgconn.PgError
	if !errors.As(err, &databaseError) || databaseError.Code != "22012" {
		t.Fatalf("ScanAll = %v, want database division-by-zero error", err)
	}
}

type timestampObject struct {
	store.ObjectMeta `json:",inline"`
	Label            string     `json:"label"`
	UpdatedAt        meta.Time  `json:"updatedAt"`
	DeletedAt        *meta.Time `json:"deletedAt,omitempty"`
	StartedAt        time.Time  `json:"startedAt"`
}

func (*timestampObject) ResourceName() string { return "timestampobjects" }

func TestModelFieldsFollowDeclarationOrder(t *testing.T) {
	codec := NewModelCodec()
	fields, err := codec.ModelFields(&timestampObject{})
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		names = append(names, field.Name)
	}
	want := []string{
		"id", "name", "uid", "apiVersion", "scopes", "resource", "resourceVersion",
		"generation", "creationTimestamp", "deletionTimestamp", "labels",
		"annotations", "finalizers", "ownerReferences", "description",
		"label", "updatedAt", "deletedAt", "startedAt",
	}
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("ModelFields() = %v, want %v", names, want)
	}
}

func TestModelFieldsRejectNonStructModel(t *testing.T) {
	codec := NewModelCodec()
	if _, err := codec.ModelFields("not-a-model"); err == nil {
		t.Fatal("ModelFields(string) returned no error")
	}
}

func TestModelFieldsRejectDuplicateRepresentationName(t *testing.T) {
	codec := NewModelCodec()
	if _, err := codec.ModelFields(&duplicateNameObject{}); err == nil {
		t.Fatal("ModelFields() accepted a duplicate field name")
	}
}

type duplicateNameObject struct {
	store.ObjectMeta `json:",inline"`
	Name             string `json:"name"`
}

func TestToDriverValueWritesTimestampsAsTime(t *testing.T) {
	moment := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	tests := []struct {
		name  string
		value any
		want  any
	}{
		{name: "meta.Time", value: meta.Time{Time: moment}, want: moment},
		{name: "time.Time", value: moment, want: moment},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := ToDriverValue(reflect.ValueOf(test.value))
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("ToDriverValue(%T) = %#v, want %#v", test.value, got, test.want)
			}
		})
	}
}

func TestToDriverValueWritesNilTimestampAsNull(t *testing.T) {
	var missing *meta.Time
	if got := ToDriverValue(reflect.ValueOf(missing)); got != nil {
		t.Fatalf("ToDriverValue(nil *meta.Time) = %#v, want nil", got)
	}
}

func TestToDriverValueKeepsDocumentsForJSONColumns(t *testing.T) {
	value := map[string]string{"team": "blue"}
	got := ToDriverValue(reflect.ValueOf(value))
	valuer, ok := got.(driver.Valuer)
	if !ok {
		t.Fatalf("ToDriverValue(map) = %T, want driver.Valuer", got)
	}
	encoded, err := valuer.Value()
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded.([]byte)) != `{"team":"blue"}` {
		t.Fatalf("encoded document = %s", encoded)
	}
}
