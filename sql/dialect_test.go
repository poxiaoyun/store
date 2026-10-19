package sql

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/common/meta"
	"xiaoshiai.cn/store"
)

func TestDialectQuoteAndPlaceholder(t *testing.T) {
	postgres, err := NewDialect(DBDriverPostgres)
	if err != nil {
		t.Fatal(err)
	}
	MySQLDialect, err := NewDialect(DBDriverMySQL)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := postgres.Quote("user roles"), `"user roles"`; got != want {
		t.Fatalf("postgres quote = %s, want %s", got, want)
	}
	if got, want := MySQLDialect.Quote("user`roles"), "`user``roles`"; got != want {
		t.Fatalf("mysql quote = %s, want %s", got, want)
	}
	if got, want := postgres.Placeholder(3), "$3"; got != want {
		t.Fatalf("postgres placeholder = %s, want %s", got, want)
	}
	if got, want := MySQLDialect.Placeholder(3), "?"; got != want {
		t.Fatalf("mysql placeholder = %s, want %s", got, want)
	}
}

func TestDialectColumnTypes(t *testing.T) {
	tests := []struct {
		name    string
		driver  string
		value   any
		size    int
		indexed bool
		want    string
	}{
		{name: "postgres timestamp", driver: DBDriverPostgres, value: meta.Time{}, want: "timestamptz"},
		{name: "mysql timestamp", driver: DBDriverMySQL, value: meta.Time{}, want: "datetime(6)"},
		{name: "postgres text", driver: DBDriverPostgres, value: "", want: "text"},
		{name: "mysql text", driver: DBDriverMySQL, value: "", want: "text"},
		{name: "postgres indexed string", driver: DBDriverPostgres, value: "", indexed: true, want: "varchar(255)"},
		{name: "mysql indexed string", driver: DBDriverMySQL, value: "", indexed: true, want: "varchar(255)"},
		{name: "declared indexed string", driver: DBDriverMySQL, value: "", size: 64, indexed: true, want: "varchar(64)"},
		{name: "declared unindexed string stays unbounded", driver: DBDriverMySQL, value: "", size: 64, want: "text"},
		{name: "mysql bool", driver: DBDriverMySQL, value: false, want: "tinyint(1)"},
		{name: "postgres bool", driver: DBDriverPostgres, value: false, want: "boolean"},
		{name: "postgres ints", driver: DBDriverPostgres, value: int64(0), want: "bigint"},
		{name: "postgres floats", driver: DBDriverPostgres, value: float64(0), want: "double precision"},
		{name: "postgres json document", driver: DBDriverPostgres, value: map[string]string{}, want: "jsonb"},
		{name: "mysql json document", driver: DBDriverMySQL, value: map[string]string{}, want: "json"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d, err := NewDialect(test.driver)
			if err != nil {
				t.Fatal(err)
			}
			got, err := d.ColumnType("field", reflect.TypeOf(test.value), test.size, test.indexed)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("columnType() = %s, want %s", got, test.want)
			}
		})
	}
}

func TestDialectColumnTypeRejectsUnsupportedField(t *testing.T) {
	d, err := NewDialect(DBDriverPostgres)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := d.ColumnType("field", reflect.TypeFor[chan int](), 0, false); err == nil {
		t.Fatal("columnType(chan) returned no error")
	}
}

func TestDialectNestedIndexDDL(t *testing.T) {
	index := store.Index{
		Name:   "auditevents_event_occurredAt",
		Fields: []string{"event.occurredAt"},
	}
	postgres, err := NewDialect(DBDriverPostgres)
	if err != nil {
		t.Fatal(err)
	}
	statements, err := postgres.IndexDDL("auditevents", index)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{`CREATE INDEX "auditevents_event_occurredAt" ON "auditevents" (("event" #>> ARRAY['occurredAt']))`}
	if !reflect.DeepEqual(statements, want) {
		t.Fatalf("postgres nested index = %#v, want %#v", statements, want)
	}

	MySQLDialect, err := NewDialect(DBDriverMySQL)
	if err != nil {
		t.Fatal(err)
	}
	statements, err = MySQLDialect.IndexDDL("auditevents", index)
	if err != nil {
		t.Fatal(err)
	}
	want = []string{
		"ALTER TABLE `auditevents` ADD COLUMN `idx_event_occurredAt` varchar(255) GENERATED ALWAYS AS (json_unquote(json_extract(`event`,_utf8mb4'$.\"occurredAt\"'))) VIRTUAL",
		"CREATE INDEX `auditevents_event_occurredAt` ON `auditevents` (`idx_event_occurredAt`)",
	}
	if !reflect.DeepEqual(statements, want) {
		t.Fatalf("mysql nested index = %#v, want %#v", statements, want)
	}
}

func TestDialectPlainIndexDDLStaysOnModelColumns(t *testing.T) {
	postgres, err := NewDialect(DBDriverPostgres)
	if err != nil {
		t.Fatal(err)
	}
	statements, err := postgres.IndexDDL("identifiers", store.Index{Name: "identifier_login", Fields: []string{"loginKey"}, Unique: true})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{`CREATE UNIQUE INDEX "identifier_login" ON "identifiers" ("loginKey")`}
	if !reflect.DeepEqual(statements, want) {
		t.Fatalf("postgres plain unique index = %#v, want %#v", statements, want)
	}
}

func TestCanonicalTimeMatchesStoredJSONText(t *testing.T) {
	moment := time.Date(2026, 3, 4, 5, 6, 7, 0, time.FixedZone("UTC+8", 8*3600))
	encoded, err := json.Marshal(meta.Time{Time: moment})
	if err != nil {
		t.Fatal(err)
	}
	want := `"` + CanonicalTime(moment) + `"`
	if string(encoded) != want {
		t.Fatalf("stored JSON time = %s, canonical = %s", encoded, want)
	}
}

func TestTextArgNarrowsTimestampsForNestedComparison(t *testing.T) {
	moment := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	if got := TextArg(moment); got != "2026-03-04T05:06:07Z" {
		t.Fatalf("TextArg(time.Time) = %v", got)
	}
	if got := TextArg(meta.Time{Time: moment}); got != "2026-03-04T05:06:07Z" {
		t.Fatalf("TextArg(meta.Time) = %v", got)
	}
	var missing *meta.Time
	if got := TextArg(missing); got != nil {
		t.Fatalf("TextArg(nil *meta.Time) = %v, want nil", got)
	}
	if got := TextArg("plain"); got != "plain" {
		t.Fatalf("TextArg(string) = %v", got)
	}
}

func TestDialectMapError(t *testing.T) {
	badRequest := func(err error) bool {
		return errors.ReasonForError(err) == errors.StatusReasonBadRequest
	}
	tests := []struct {
		name   string
		driver string
		err    error
		check  func(error) bool
	}{
		{name: "mysql duplicate", driver: DBDriverMySQL, err: &mysql.MySQLError{Number: 1062}, check: errors.IsAlreadyExists},
		{name: "mysql missing required value", driver: DBDriverMySQL, err: &mysql.MySQLError{Number: 1048}, check: badRequest},
		{name: "mysql missing referenced row", driver: DBDriverMySQL, err: &mysql.MySQLError{Number: 1452}, check: errors.IsNotFound},
		{name: "postgres duplicate", driver: DBDriverPostgres, err: &pgconn.PgError{Code: "23505"}, check: errors.IsAlreadyExists},
		{name: "postgres not null", driver: DBDriverPostgres, err: &pgconn.PgError{Code: "23502"}, check: badRequest},
		{name: "postgres missing referenced row", driver: DBDriverPostgres, err: &pgconn.PgError{Code: "23503"}, check: errors.IsNotFound},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d, err := NewDialect(test.driver)
			if err != nil {
				t.Fatal(err)
			}
			mapped := d.MapError(test.err, "widgets", "w1")
			if !test.check(mapped) {
				t.Fatalf("mapError(%v) = %v", test.err, mapped)
			}
		})
	}
}

func TestDialectMapErrorKeepsUnknownErrors(t *testing.T) {
	d, err := NewDialect(DBDriverPostgres)
	if err != nil {
		t.Fatal(err)
	}
	original := &pgconn.PgError{Code: "42P01"}
	if mapped := d.MapError(original, "widgets", "w1"); mapped != error(original) {
		t.Fatalf("mapError(unknown) = %v, want passthrough", mapped)
	}
}

func TestDialectIdentifiesRetryableConflicts(t *testing.T) {
	postgres, err := NewDialect(DBDriverPostgres)
	if err != nil {
		t.Fatal(err)
	}
	MySQLDialect, err := NewDialect(DBDriverMySQL)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		d    Dialect
		err  error
		want bool
	}{
		{name: "postgres deadlock", d: postgres, err: &pgconn.PgError{Code: "40P01"}, want: true},
		{name: "postgres serialization", d: postgres, err: &pgconn.PgError{Code: "40001"}, want: true},
		{name: "postgres unique", d: postgres, err: &pgconn.PgError{Code: "23505"}, want: false},
		{name: "mysql deadlock", d: MySQLDialect, err: &mysql.MySQLError{Number: 1213}, want: true},
		{name: "mysql lock wait timeout", d: MySQLDialect, err: &mysql.MySQLError{Number: 1205}, want: true},
		{name: "mysql duplicate", d: MySQLDialect, err: &mysql.MySQLError{Number: 1062}, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.d.IsRetryableConflict(test.err); got != test.want {
				t.Fatalf("isRetryableConflict(%v) = %t, want %t", test.err, got, test.want)
			}
		})
	}
}

func TestDialectCreateTableAndAddColumn(t *testing.T) {
	columns := []ColumnSpec{
		{Name: "id", Type: reflect.TypeFor[string](), Size: 64, Indexed: true},
		{Name: "content", Type: reflect.TypeFor[map[string]string]()},
	}
	postgres, err := NewDialect(DBDriverPostgres)
	if err != nil {
		t.Fatal(err)
	}
	statement, err := postgres.CreateTable("assets", columns)
	if err != nil {
		t.Fatal(err)
	}
	want := `CREATE TABLE IF NOT EXISTS "assets" ("id" varchar(64), "content" jsonb, PRIMARY KEY ("id"))`
	if statement != want {
		t.Fatalf("postgres create table = %s, want %s", statement, want)
	}
	statement, err = postgres.AddColumn("assets", columns[1])
	if err != nil {
		t.Fatal(err)
	}
	if want := `ALTER TABLE "assets" ADD COLUMN "content" jsonb`; statement != want {
		t.Fatalf("postgres add column = %s, want %s", statement, want)
	}

	MySQLDialect, err := NewDialect(DBDriverMySQL)
	if err != nil {
		t.Fatal(err)
	}
	statement, err = MySQLDialect.CreateTable("assets", columns)
	if err != nil {
		t.Fatal(err)
	}
	want = "CREATE TABLE IF NOT EXISTS `assets` (`id` varchar(64), `content` json, PRIMARY KEY (`id`))"
	if statement != want {
		t.Fatalf("mysql create table = %s, want %s", statement, want)
	}
}
