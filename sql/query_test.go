package sql

import (
	"reflect"
	"testing"

	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/selector"
	"xiaoshiai.cn/store"
)

// joinRequirementExpressions renders requirements and returns them joined, so a
// test can compare the generated condition text.
func joinRequirementExpressions(t *testing.T, storage *Storage, q *QueryState, requirements []selector.Requirement) (string, []any) {
	t.Helper()
	builder := &ConditionBuilder{}
	if err := storage.addRequirementConditions(builder, q, nil, store.Requirements(requirements)); err != nil {
		t.Fatal(err)
	}
	return builder.Where(), q.Args()
}

func TestLabelRequirementsReadTheLabelsDocument(t *testing.T) {
	requirement := selector.RequirementEqual("example.com/team", "blue")
	tests := []struct {
		name    string
		dialect Dialect
		want    string
	}{
		{
			name:    "postgres",
			dialect: PostgresDialect{},
			want:    ` WHERE ("labels" #>> ARRAY['example.com/team'] IS NOT NULL AND "labels" #>> ARRAY['example.com/team'] = $1)`,
		},
		{
			name:    "mysql",
			dialect: MySQLDialect{},
			want:    " WHERE (JSON_UNQUOTE(JSON_EXTRACT(`labels`, '$.\"example.com/team\"')) IS NOT NULL AND JSON_UNQUOTE(JSON_EXTRACT(`labels`, '$.\"example.com/team\"')) = ?)",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			storage := &Storage{core: &core{dialect: test.dialect, codec: NewModelCodec()}}
			q := &QueryState{dialect: test.dialect}
			builder := &ConditionBuilder{}
			if err := storage.addRequirementConditions(builder, q, store.Requirements{requirement}, nil); err != nil {
				t.Fatal(err)
			}
			if got := builder.Where(); got != test.want {
				t.Fatalf("label condition = %s, want %s", got, test.want)
			}
			if !reflect.DeepEqual(q.Args(), []any{"blue"}) {
				t.Fatalf("label args = %#v", q.Args())
			}
		})
	}
}

func TestNestedFieldRequirementsCompareJSONText(t *testing.T) {
	storage := &Storage{core: &core{dialect: PostgresDialect{}, codec: NewModelCodec()}}
	where, args := joinRequirementExpressions(t, storage, &QueryState{dialect: PostgresDialect{}},
		[]selector.Requirement{selector.RequirementEqual("extra.team", "blue")},
	)
	want := ` WHERE ("extra" #>> ARRAY['team'] IS NOT NULL AND "extra" #>> ARRAY['team'] = $1)`
	if where != want {
		t.Fatalf("nested condition = %s, want %s", where, want)
	}
	if !reflect.DeepEqual(args, []any{"blue"}) {
		t.Fatalf("nested args = %#v", args)
	}
}

func TestEmptyValueListMatchesNothing(t *testing.T) {
	for _, operator := range []selector.Operator{selector.In, selector.NotIn} {
		requirement := selector.NewRequirement("rank", operator)
		storage := &Storage{core: &core{dialect: MySQLDialect{}, codec: NewModelCodec()}}
		where, args := joinRequirementExpressions(t, storage, &QueryState{dialect: MySQLDialect{}}, []selector.Requirement{requirement})
		if where != " WHERE 1 = 0" {
			t.Fatalf("empty %q condition = %s, want no match", operator, where)
		}
		if len(args) != 0 {
			t.Fatalf("empty %q args = %#v", operator, args)
		}
	}
}

func TestRequirementRenderingRejectsInvalidStructures(t *testing.T) {
	storage := &Storage{core: &core{dialect: MySQLDialect{}, codec: NewModelCodec()}}
	q := &QueryState{dialect: MySQLDialect{}}
	invalid := store.Requirements{{Operator: selector.And, Key: "rank"}}
	if err := storage.addRequirementConditions(&ConditionBuilder{}, q, nil, invalid); errors.ReasonForError(err) != errors.StatusReasonBadRequest {
		t.Fatalf("invalid requirement error = %v, want BadRequest", err)
	}
}

func TestWindowClausePerDialect(t *testing.T) {
	tests := []struct {
		name    string
		dialect Dialect
		offset  int
		limit   int
		want    string
	}{
		{name: "postgres limit", dialect: PostgresDialect{}, limit: 10, want: " LIMIT $1"},
		{name: "postgres offset only", dialect: PostgresDialect{}, offset: 5, want: " OFFSET $1"},
		{name: "postgres both", dialect: PostgresDialect{}, offset: 5, limit: 10, want: " LIMIT $1 OFFSET $2"},
		{name: "mysql limit", dialect: MySQLDialect{}, limit: 10, want: " LIMIT ?"},
		{name: "mysql offset only carries a row limit", dialect: MySQLDialect{}, offset: 5, want: " LIMIT " + mysqlRowLimit + " OFFSET ?"},
		{name: "mysql both", dialect: MySQLDialect{}, offset: 5, limit: 10, want: " LIMIT ? OFFSET ?"},
		{name: "unbounded", dialect: MySQLDialect{}, want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			storage := &Storage{core: &core{dialect: test.dialect, codec: NewModelCodec()}}
			q := &QueryState{dialect: test.dialect}
			if got := storage.window(q, test.offset, test.limit); got != test.want {
				t.Fatalf("window clause = %q, want %q", got, test.want)
			}
		})
	}
}

func TestSearchConditionsMatchLoweredFields(t *testing.T) {
	storage := &Storage{core: &core{dialect: MySQLDialect{}, codec: NewModelCodec()}}
	q := &QueryState{dialect: MySQLDialect{}}
	condition, err := storage.searchConditions(q, "Needle", []string{"value", "extra.team"})
	if err != nil {
		t.Fatal(err)
	}
	want := "(LOWER(`value`) LIKE ? OR LOWER(JSON_UNQUOTE(JSON_EXTRACT(`extra`, '$.\"team\"'))) LIKE ?)"
	if condition != want {
		t.Fatalf("search condition = %s, want %s", condition, want)
	}
	if !reflect.DeepEqual(q.Args(), []any{"%needle%", "%needle%"}) {
		t.Fatalf("search args = %#v", q.Args())
	}
}
