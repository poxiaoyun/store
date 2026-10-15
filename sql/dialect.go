package sql

import (
	"context"
	"fmt"
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

// IndexInfo describes one existing index during schema reconciliation. Columns
// is empty when the index is built from an expression or generated column.
type IndexInfo struct {
	Name    string
	Unique  bool
	Columns []string
}

// dialect owns every database-specific decision: identifier quoting, bind
// placeholders, column types, JSON path extraction, schema introspection,
// nested-path indexes, and error mapping. Statements are built once and bound
// through this interface, so the store logic stays dialect-free.
type Dialect interface {
	// Name reports the driver name this dialect serves.
	Name() string
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
	// NotDistinct renders a predicate that is true when left equals right,
	// comparing NULL as one comparable value.
	NotDistinct(left, right string) string
	// JSONExtract renders the document value at remainingPath inside column,
	// keeping its document type.
	JSONExtract(column string, remainingPath []string) (string, error)
	// Distinct renders a predicate that is true when left differs from right,
	// comparing NULL as one comparable value rather than yielding NULL.
	Distinct(left, right string) string
	// JSONValue wraps a placeholder that carries JSON text so a document column
	// accepts it.
	JSONValue(placeholder string) string
	// JSONSet renders an edit that writes value at remainingPath inside a JSON
	// document column. placeholder is an already rendered bind marker.
	JSONSet(column string, remainingPath []string, placeholder string) string
	// JSONRemove renders an edit that removes remainingPath from a JSON
	// document column.
	JSONRemove(column string, remainingPath []string) string
	// UpdateReturning renders the clause that makes an UPDATE return the written
	// row, or the empty string when the backend cannot return it.
	UpdateReturning() string
	// Window renders the row window clause. A bound's placeholder is empty when
	// that bound is not requested.
	Window(limitPlaceholder, offsetPlaceholder string) string
	// CreateTable renders CREATE TABLE IF NOT EXISTS for one planned table.
	// Every non-key column is nullable, because an omitted optional field must
	// stay absent rather than fail the insert.
	CreateTable(table string, columns []ColumnSpec, key []string) (string, error)
	// AddColumn renders ALTER TABLE ADD COLUMN for one planned column.
	AddColumn(table string, column ColumnSpec) (string, error)
	// IndexDDL renders the statements that create one index. An index over a
	// nested path needs an expression index or a generated column.
	IndexDDL(table string, index store.Index) ([]string, error)
	// Columns reads the existing column set of a table.
	Columns(ctx context.Context, db StatementExecutor, table string) (map[string]ColumnInfo, error)
	// Indexes reads the existing index set of a table.
	Indexes(ctx context.Context, db StatementExecutor, table string) (map[string]IndexInfo, error)
	// MapError translates a driver error into a domain error.
	MapError(err error, resource, name string) error
	// IsRetryableConflict reports that a statement failed on a retryable
	// transaction conflict, such as a deadlock or a serialization failure.
	IsRetryableConflict(err error) bool
}

func NewDialect(driver string) (Dialect, error) {
	switch driver {
	case DBDriverMySQL:
		return MySQLDialect{}, nil
	case DBDriverPostgres:
		return PostgresDialect{}, nil
	default:
		return nil, fmt.Errorf("empty or unsupported database driver: [%s]", driver)
	}
}

// jsonPath splits a nested field path. A field without a dot is a plain column.
func JSONPath(field string) []string {
	return strings.Split(field, ".")
}

func IsNestedField(field string) bool {
	return strings.Contains(field, ".")
}

// derefType unwraps every pointer layer, so pointer fields map like their value
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

// isTimeType reports whether t holds a timestamp rather than a JSON document.
func IsTimeType(t reflect.Type) bool {
	return t == timeType || t == metaTimeType
}

// canonicalTime renders a timestamp in the same textual form nested JSON
// timestamps use, so nested comparisons stay lexicographically ordered.
func CanonicalTime(value time.Time) string {
	return value.UTC().Format(time.RFC3339)
}

// textArg renders a requirement value for comparison against a nested JSON
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

func TextArgs(values []any) []any {
	result := make([]any, len(values))
	for i, value := range values {
		result[i] = TextArg(value)
	}
	return result
}

// quotedLiteral renders one SQL string literal with its quotes doubled, so a
// label key that contains a quote cannot leave the literal.
func quotedLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// GeneratedColumnName renders the name of the column that materializes a
// nested index path.
func GeneratedColumnName(table string, path []string) string {
	name := "idx_" + strings.Join(path, "_")
	return strings.ReplaceAll(name, ".", "_")
}

// ---------------------------------------------------------------------------
