package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
)

type PostgresDialect struct{}

func (PostgresDialect) Name() string { return DBDriverPostgres }

func (PostgresDialect) Quote(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
}

func (PostgresDialect) Placeholder(index int) string { return fmt.Sprintf("$%d", index) }

func (PostgresDialect) ColumnType(field string, t reflect.Type, size int, indexed bool) (string, error) {
	base := DerefType(t)
	switch {
	case IsTimeType(base):
		return "timestamptz", nil
	case base.Kind() == reflect.String:
		if indexed {
			if size <= 0 {
				size = DefaultIndexedStringSize
			}
			return fmt.Sprintf("varchar(%d)", size), nil
		}
		return "text", nil
	case base.Kind() == reflect.Bool:
		return "boolean", nil
	case base.Kind() >= reflect.Int && base.Kind() <= reflect.Int64,
		base.Kind() >= reflect.Uint && base.Kind() <= reflect.Uint64:
		return "bigint", nil
	case base.Kind() == reflect.Float32 || base.Kind() == reflect.Float64:
		return "double precision", nil
	case base.Kind() == reflect.Slice && base.Elem().
		Kind() == reflect.Uint8:
		return "bytea", nil
	case base.Kind() == reflect.Map, base.Kind() == reflect.Struct,
		base.Kind() == reflect.Slice, base.Kind() == reflect.Array, base.Kind() == reflect.Interface:
		return "jsonb", nil
	default:
		return "", fmt.Errorf("field %q of type %s has no PostgreSQL column type", field, t)
	}
}

func (d PostgresDialect) CreateTable(table string, columns []ColumnSpec) (string, error) {
	definitions := make([]string, 0, len(columns)+1)
	for _, column := range columns {
		definition, err := d.columnDefinition(column)
		if err != nil {
			return "", err
		}
		definitions = append(definitions, definition)
	}
	definitions = append(definitions, "PRIMARY KEY ("+d.Quote("id")+")")
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", d.Quote(table), strings.Join(definitions, ", ")), nil
}

func (d PostgresDialect) AddColumn(table string, column ColumnSpec) (string, error) {
	definition, err := d.columnDefinition(column)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", d.Quote(table), definition), nil
}

func (d PostgresDialect) columnDefinition(column ColumnSpec) (string, error) {
	columnType, err := d.ColumnType(column.Name, column.Type, column.Size, column.Indexed)
	if err != nil {
		return "", err
	}
	return d.Quote(column.Name) + " " + columnType, nil
}

// Window renders the row window. PostgreSQL accepts OFFSET on its own.
func (PostgresDialect) Window(limitPlaceholder, offsetPlaceholder string) string {
	clause := ""
	if limitPlaceholder != "" {
		clause = " LIMIT " + limitPlaceholder
	}
	if offsetPlaceholder != "" {
		clause += " OFFSET " + offsetPlaceholder
	}
	return clause
}

// JSONValue casts JSON text into a jsonb value.
func (PostgresDialect) JSONValue(placeholder string) string {
	return placeholder + "::jsonb"
}

func (PostgresDialect) JSONText(column string, remainingPath []string) (string, error) {
	return fmt.Sprintf("%s #>> %s", column, postgresJSONPath(remainingPath)), nil
}

// postgresJSONPath renders a text array literal. Each segment is its own
// element, so a label key that contains a dot or comma stays one segment.
func postgresJSONPath(path []string) string {
	segments := make([]string, 0, len(path))
	for _, segment := range path {
		segments = append(segments, quotedLiteral(segment))
	}
	return "ARRAY[" + strings.Join(segments, ", ") + "]"
}

func (d PostgresDialect) IndexDDL(table string, index store.Index) ([]string, error) {
	columns := make([]string, 0, len(index.Fields))
	for _, field := range index.Fields {
		if !IsNestedField(field) {
			columns = append(columns, d.Quote(field))
			continue
		}
		path := JSONPath(field)
		expression, err := d.JSONText(d.Quote(path[0]), path[1:])
		if err != nil {
			return nil, err
		}
		columns = append(columns, "("+expression+")")
	}
	unique := ""
	if index.Unique {
		unique = "UNIQUE "
	}
	// PostgreSQL indexes an expression directly and treats NULL values as
	// distinct in unique indexes, matching a nullable unique index.
	return []string{fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
		unique, d.Quote(index.Name), d.Quote(table), strings.Join(columns, ", "))}, nil
}

func (d PostgresDialect) Columns(ctx context.Context, db StatementExecutor, table string) (map[string]ColumnInfo, error) {
	rows, err := db.QueryContext(ctx, `
SELECT attname, format_type(atttypid, atttypmod)
FROM pg_attribute
WHERE attrelid = to_regclass($1) AND attnum > 0 AND NOT attisdropped`, d.Quote(table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]ColumnInfo{}
	for rows.Next() {
		var info ColumnInfo
		if err := rows.Scan(&info.Name, &info.Type); err != nil {
			return nil, err
		}
		result[info.Name] = info
	}
	return result, rows.Err()
}

func (d PostgresDialect) Indexes(ctx context.Context, db StatementExecutor, table string) (map[string]IndexInfo, error) {
	rows, err := db.QueryContext(ctx, `
SELECT i.relname, ix.indisprimary, a.attname
FROM pg_index ix
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN pg_class t ON t.oid = ix.indrelid
LEFT JOIN unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord) ON k.ord <= ix.indnkeyatts
LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
WHERE t.oid = to_regclass($1)
ORDER BY i.relname, k.ord`, d.Quote(table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]IndexInfo{}
	for rows.Next() {
		var (
			name    string
			primary bool
			column  sql.NullString
		)
		if err := rows.Scan(&name, &primary, &column); err != nil {
			return nil, err
		}
		info := result[name]
		info.Primary = primary
		if column.Valid {
			info.Columns = append(info.Columns, column.String)
		}
		result[name] = info
	}
	return result, rows.Err()
}

func (d PostgresDialect) ValidateIndex(ctx context.Context, db StatementExecutor, table string, index store.Index) error {
	q := &QueryState{dialect: d}
	relation := q.Bind(d.Quote(table))
	name := q.Bind(index.Name)
	conditions := []string{
		"ix.indisunique = " + q.Bind(index.Unique),
		"ix.indisvalid", "ix.indisready", "ix.indpred IS NULL",
		"ix.indnkeyatts = " + q.Bind(len(index.Fields)),
	}
	for i, field := range index.Fields {
		path := JSONPath(field)
		expected := "quote_ident(" + q.Bind(path[0]) + ")"
		if len(path) > 1 {
			encoded, _ := json.Marshal(path[1:])
			// Use PostgreSQL's own identifier and literal quoting to compare its
			// deparsed expression, without interpreting arbitrary SQL in Go.
			expected = "'((' || " + expected + " || ' #>> ARRAY[' || " +
				"(SELECT string_agg(quote_literal(value) || '::text', ', ' ORDER BY ord) FROM jsonb_array_elements_text(" +
				q.Bind(string(encoded)) + "::jsonb) WITH ORDINALITY AS path(value, ord)) || ']))'"
		}
		conditions = append(conditions, fmt.Sprintf("pg_get_indexdef(ix.indexrelid, %d, false) = (%s)", i+1, expected))
	}
	statement := "SELECT " + strings.Join(conditions, " AND ") +
		" FROM pg_index ix JOIN pg_class i ON i.oid = ix.indexrelid WHERE ix.indrelid = to_regclass(" + relation + ") AND i.relname = " + name
	var matches bool
	if err := db.QueryRowContext(ctx, statement, q.args...).
		Scan(&matches); err != nil {
		return err
	}
	if !matches {
		return fmt.Errorf("table %q index %q conflicts with the declared definition", table, index.Name)
	}
	return nil
}

func (PostgresDialect) MapError(err error, resource, name string) error {
	if stderrors.Is(err, sql.ErrNoRows) {
		return errors.NewNotFound(resource, name)
	}
	var pgErr *pgconn.PgError
	if stderrors.As(err, &pgErr) {
		switch pgErr.Code {
		case "23505": // unique_violation
			return errors.NewAlreadyExists(resource, name)
		case "23503": // foreign_key_violation
			return errors.NewNotFound(resource, name)
		case "23502", "23514": // not_null_violation, check_violation
			return errors.NewBadRequest(fmt.Sprintf("missing required value for resource %s", resource))
		}
	}
	return err
}

func (PostgresDialect) IsRetryableConflict(err error) bool {
	var pgErr *pgconn.PgError
	if stderrors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001", "40P01": // serialization_failure, deadlock_detected
			return true
		}
	}
	return false
}

func (PostgresDialect) Distinct(left, right string) string {
	return "(" + left + " IS DISTINCT FROM " + right + ")"
}
func (PostgresDialect) MergeDocument(q *QueryState, column string, patch map[string]any) string {
	expression := "(CASE WHEN jsonb_typeof(" + column + ") = 'object' THEN " + column + " ELSE '{}'::jsonb END)"
	nonnull := map[string]any{}
	for _, key := range sortedKeys(patch) {
		if patch[key] == nil {
			expression = "(" + expression + " - " + q.Bind(key) + "::text)"
		} else {
			nonnull[key] = patch[key]
		}
	}
	data, _ := json.Marshal(nonnull)
	return "(" + expression + " || " + q.Bind(string(data)) + "::jsonb)"
}
func (PostgresDialect) FinalizersEmpty(column string) string {
	return "(" + column + " IS NULL OR " + column + " = 'null'::jsonb OR " + column + " = '[]'::jsonb)"
}
