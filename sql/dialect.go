package sql

import (
	"context"
	"reflect"
	"strings"
	"time"

	"xiaoshiai.cn/common/meta"
	"xiaoshiai.cn/store"
)

// DefaultIndexedStringSize bounds indexed string columns when the resource
// schema does not declare a size.
const DefaultIndexedStringSize = 255

// ColumnInfo describes one existing table column during schema reconciliation.
type ColumnInfo struct {
	Name string
	Type string
}

// IndexInfo describes an existing index's physical key columns, excluding
// covering columns. Expression keys have no physical column name.
type IndexInfo struct {
	Primary bool
	Columns []string
}

// Dialect owns every database-specific decision: identifier quoting, bind
// placeholders, column types, JSON path extraction, schema introspection,
// nested-path indexes, and error mapping. Statements are built once and bound
// through this interface, so the store logic stays dialect-free.
type Dialect interface {
	// Distinct compares two scalar expressions including SQL NULL.
	Distinct(left, right string) string
	// MergeDocument applies a flat RFC 7396 object patch to a JSON column.
	MergeDocument(q *QueryState, column string, patch map[string]any) string
	// FinalizersEmpty tests whether the persisted finalizer array is empty.
	FinalizersEmpty(column string) string

	// Quote renders one identifier.
	Quote(ident string) string
	// Placeholder renders the bind marker of the index-th argument (one-based).
	Placeholder(index int) string
	// ColumnType renders the SQL type of one model field. indexed reports that
	// the field participates in a unique or secondary index, which requires a
	// bounded type on backends with key length limits.
	ColumnType(field string, t reflect.Type, size int, indexed bool) (string, error)
	// JSONText renders a text extraction of remainingPath inside column. A
	// nested field path splits into its first segment as the column and the
	// rest as remainingPath.
	JSONText(column string, remainingPath []string) (string, error)
	// JSONValue wraps a placeholder that carries JSON text so a document column
	// accepts it.
	JSONValue(placeholder string) string
	// Window renders the row window clause. A bound's placeholder is empty when
	// that bound is not requested.
	Window(limitPlaceholder, offsetPlaceholder string) string
	// CreateTable renders CREATE TABLE IF NOT EXISTS for one planned table.
	// ID is always the primary key. Other columns are nullable so omitted
	// optional fields stay absent rather than fail the insert.
	CreateTable(table string, columns []ColumnSpec) (string, error)
	// AddColumn renders ALTER TABLE ADD COLUMN for one planned column.
	AddColumn(table string, column ColumnSpec) (string, error)
	// IndexDDL renders the statements that create one index. An index over a
	// nested path needs an expression index or a generated column.
	IndexDDL(table string, index store.Index) ([]string, error)
	// Columns reads the existing column set of a table.
	Columns(ctx context.Context, db StatementExecutor, table string) (map[string]ColumnInfo, error)
	// Indexes reads the existing index set of a table.
	Indexes(ctx context.Context, db StatementExecutor, table string) (map[string]IndexInfo, error)
	// ValidateIndex confirms that an existing index enforces the declared fields
	// and uniqueness, including expressions and generated columns.
	ValidateIndex(ctx context.Context, db StatementExecutor, table string, index store.Index) error
	// MapError translates a driver error into a domain error.
	MapError(err error, resource, name string) error
	// IsRetryableConflict reports that a statement failed on a retryable
	// transaction conflict, such as a deadlock or a serialization failure.
	IsRetryableConflict(err error) bool
}

// JSONPath splits a nested field path. A field without a dot is a plain column.
func JSONPath(field string) []string {
	return strings.Split(field, ".")
}

// IsNestedField reports whether a field addresses a nested JSON path.
func IsNestedField(field string) bool {
	return strings.Contains(field, ".")
}

// DerefType unwraps every pointer layer, so pointer fields map like their value
// type and stay nullable through the column definition.
func DerefType(t reflect.Type) reflect.Type {
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	return t
}

var (
	timeType     = reflect.TypeFor[time.Time]()
	metaTimeType = reflect.TypeFor[meta.Time]()
)

// IsTimeType reports whether t holds a timestamp rather than a JSON document.
func IsTimeType(t reflect.Type) bool {
	return t == timeType || t == metaTimeType
}

// CanonicalTime renders a timestamp in the same textual form nested JSON
// timestamps use, so nested comparisons stay lexicographically ordered.
func CanonicalTime(value time.Time) string {
	return value.UTC().
		Format(time.RFC3339)
}

// TextArg renders a requirement value for comparison against a nested JSON
// path. Nested values are compared as their JSON text, so timestamps must use
// the canonical form.
func TextArg(value any) any {
	switch typed := value.(type) {
	case time.Time:
		return CanonicalTime(typed)
	case meta.Time:
		return CanonicalTime(typed.Time)
	case *time.Time:
		if typed == nil {
			return nil
		}
		return CanonicalTime(*typed)
	case *meta.Time:
		if typed == nil {
			return nil
		}
		return CanonicalTime(typed.Time)
	default:
		return value
	}
}

// TextArgs renders requirement values for nested JSON comparisons.
func TextArgs(values []any) []any {
	result := make([]any, len(values))
	for i, value := range values {
		result[i] = TextArg(value)
	}
	return result
}

// QuoteLiteral renders one SQL string literal with its quotes doubled, so a
// label key that contains a quote cannot leave the literal.
func QuoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
