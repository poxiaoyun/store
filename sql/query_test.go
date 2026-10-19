package sql_test

import (
	"context"
	"database/sql"
	stderrors "errors"
	"reflect"
	"strings"
	"testing"

	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/selector"
	storesql "xiaoshiai.cn/store/sql"
	"xiaoshiai.cn/store/sql/mysql"
	"xiaoshiai.cn/store/sql/postgres"
	"xiaoshiai.cn/store/storetest"
)

// queryRecorder stops at the database seam so tests observe the complete query
// and binding order produced by a public Store operation.
type queryRecorder struct {
	storesql.Database
	statement string
	args      []any
	stop      error
}

func (db *queryRecorder) QueryContext(_ context.Context, statement string, args ...any) (*sql.Rows, error) {
	db.statement, db.args = statement, args
	return nil, db.stop
}

func recordList(t *testing.T, dialect storesql.Dialect, options ...store.ListOption) (*queryRecorder, error) {
	t.Helper()
	db := &queryRecorder{stop: stderrors.New("query recorded")}
	storage, err := storesql.NewStorage(t.Context(), store.NewSchema(), db, dialect)
	if err != nil {
		t.Fatal(err)
	}
	return db, storage.List(t.Context(), &store.List[storetest.Object]{}, options...)
}

func TestListBindsRequirementsAndSearch(t *testing.T) {
	tests := []struct {
		name    string
		dialect storesql.Dialect
		options []store.ListOption
		want    string
		args    []any
	}{
		{
			name:    "postgres literal label key",
			dialect: postgres.Dialect{},
			options: []store.ListOption{store.WithLabelRequirements(selector.RequirementEqual("example.com/team", "blue"))},
			want:    `("labels" #>> ARRAY['example.com/team'] IS NOT NULL AND "labels" #>> ARRAY['example.com/team'] = $3)`,
			args:    []any{"blue"},
		},
		{
			name:    "mysql literal label key",
			dialect: mysql.Dialect{},
			options: []store.ListOption{store.WithLabelRequirements(selector.RequirementEqual("example.com/team", "blue"))},
			want:    "(JSON_UNQUOTE(JSON_EXTRACT(`labels`, '$.\"example.com/team\"')) IS NOT NULL AND JSON_UNQUOTE(JSON_EXTRACT(`labels`, '$.\"example.com/team\"')) = ?)",
			args:    []any{"blue"},
		},
		{
			name:    "nested field",
			dialect: postgres.Dialect{},
			options: []store.ListOption{store.WithFieldRequirements(selector.RequirementEqual("extra.team", "blue"))},
			want:    `("extra" #>> ARRAY['team'] IS NOT NULL AND "extra" #>> ARRAY['team'] = $3)`,
			args:    []any{"blue"},
		},
		{
			name:    "lowercase search",
			dialect: mysql.Dialect{},
			options: []store.ListOption{store.WithSearch("Needle"), store.WithSearchFields("value", "extra.team")},
			want:    "(LOWER(`value`) LIKE ? OR LOWER(JSON_UNQUOTE(JSON_EXTRACT(`extra`, '$.\"team\"'))) LIKE ?)",
			args:    []any{"%needle%", "%needle%"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := recordList(t, test.dialect, test.options...)
			if !stderrors.Is(err, db.stop) {
				t.Fatalf("List = %v, want query execution", err)
			}
			if !strings.HasSuffix(db.statement, " AND "+test.want) {
				t.Fatalf("query = %s, want condition %s", db.statement, test.want)
			}
			wantArgs := append([]any{"[]", "null"}, test.args...)
			if !reflect.DeepEqual(db.args, wantArgs) {
				t.Fatalf("args = %#v, want %#v", db.args, wantArgs)
			}
		})
	}
}

func TestEmptyValueListMatchesNothing(t *testing.T) {
	for _, operator := range []selector.Operator{selector.In, selector.NotIn} {
		db, err := recordList(t, mysql.Dialect{}, store.WithFieldRequirements(selector.NewRequirement("rank", operator)))
		if !stderrors.Is(err, db.stop) {
			t.Fatal(err)
		}
		if !strings.HasSuffix(db.statement, " AND 1 = 0") || len(db.args) != 2 {
			t.Fatalf("empty %q query = %s, args = %#v", operator, db.statement, db.args)
		}
	}
}

func TestRequirementRenderingRejectsInvalidStructures(t *testing.T) {
	db, err := recordList(t, mysql.Dialect{}, store.WithFieldRequirements(selector.Requirement{Operator: selector.And, Key: "rank"}))
	if errors.ReasonForError(err) != errors.StatusReasonBadRequest {
		t.Fatalf("invalid requirement error = %v, want BadRequest", err)
	}
	if db.statement != "" {
		t.Fatal("invalid requirement reached database")
	}
}

func TestListWindowPerDialect(t *testing.T) {
	tests := []struct {
		name    string
		dialect storesql.Dialect
		offset  int
		limit   int
		want    string
		args    []any
	}{
		{name: "postgres limit", dialect: postgres.Dialect{}, limit: 10, want: " LIMIT $3", args: []any{11}},
		{name: "postgres offset", dialect: postgres.Dialect{}, offset: 5, want: " OFFSET $3", args: []any{5}},
		{name: "postgres both", dialect: postgres.Dialect{}, offset: 5, limit: 10, want: " LIMIT $3 OFFSET $4", args: []any{11, 5}},
		{name: "mysql limit", dialect: mysql.Dialect{}, limit: 10, want: " LIMIT ?", args: []any{11}},
		{name: "mysql offset", dialect: mysql.Dialect{}, offset: 5, want: " LIMIT 18446744073709551615 OFFSET ?", args: []any{5}},
		{name: "mysql both", dialect: mysql.Dialect{}, offset: 5, limit: 10, want: " LIMIT ? OFFSET ?", args: []any{11, 5}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			db, err := recordList(t, test.dialect, store.WithOffset(test.offset), store.WithLimit(test.limit))
			if !stderrors.Is(err, db.stop) {
				t.Fatal(err)
			}
			if !strings.HasSuffix(db.statement, test.want) {
				t.Fatalf("query = %s, want window %s", db.statement, test.want)
			}
			wantArgs := append([]any{"[]", "null"}, test.args...)
			if !reflect.DeepEqual(db.args, wantArgs) {
				t.Fatalf("args = %#v, want %#v", db.args, wantArgs)
			}
		})
	}
}
