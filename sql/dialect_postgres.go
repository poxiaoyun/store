package sql

import (
	"context"
	"database/sql"
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
	case base.Kind() == reflect.Slice && base.Elem().Kind() == reflect.Uint8:
		return "bytea", nil
	case base.Kind() == reflect.Map, base.Kind() == reflect.Struct,
		base.Kind() == reflect.Slice, base.Kind() == reflect.Array, base.Kind() == reflect.Interface:
		return "jsonb", nil
	default:
		return "", fmt.Errorf("field %q of type %s has no PostgreSQL column type", field, t)
	}
}

func (d PostgresDialect) CreateTable(table string, columns []ColumnSpec, key []string) (string, error) {
	definitions := make([]string, 0, len(columns)+1)
	for _, column := range columns {
		definition, err := d.columnDefinition(column)
		if err != nil {
			return "", err
		}
		definitions = append(definitions, definition)
	}
	if len(key) > 0 {
		quoted := make([]string, 0, len(key))
		for _, name := range key {
			quoted = append(quoted, d.Quote(name))
		}
		definitions = append(definitions, "PRIMARY KEY ("+strings.Join(quoted, ", ")+")")
	}
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

// UpdateReturning returns the written row, so a write needs no follow-up read.
func (PostgresDialect) UpdateReturning() string { return " RETURNING *" }

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

// NotDistinct renders PostgreSQL's null-safe equality.
func (PostgresDialect) NotDistinct(left, right string) string {
	return left + " IS NOT DISTINCT FROM " + right
}

// JSONExtract renders a jsonb extraction of one nested path.
func (d PostgresDialect) JSONExtract(column string, remainingPath []string) (string, error) {
	return fmt.Sprintf("%s #> %s", column, postgresJSONPath(remainingPath)), nil
}

// Distinct renders PostgreSQL's null-safe inequality.
func (PostgresDialect) Distinct(left, right string) string {
	return left + " IS DISTINCT FROM " + right
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

func (d PostgresDialect) JSONSet(column string, remainingPath []string, placeholder string) string {
	return fmt.Sprintf("jsonb_set(%s, %s, %s::jsonb)", column, postgresJSONPath(remainingPath), placeholder)
}

func (d PostgresDialect) JSONRemove(column string, remainingPath []string) string {
	return fmt.Sprintf("%s #- %s", column, postgresJSONPath(remainingPath))
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

func (PostgresDialect) Columns(ctx context.Context, db StatementExecutor, table string) (map[string]ColumnInfo, error) {
	rows, err := db.QueryContext(ctx, `
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = current_schema() AND table_name = $1`, table)
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

func (PostgresDialect) Indexes(ctx context.Context, db StatementExecutor, table string) (map[string]IndexInfo, error) {
	rows, err := db.QueryContext(ctx, `
SELECT i.relname AS index_name, ix.indisunique AS is_unique,
       array_to_string(array_agg(a.attname ORDER BY k.ord), ',') AS columns
FROM pg_index ix
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN pg_class t ON t.oid = ix.indrelid
LEFT JOIN unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord) ON true
LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
WHERE t.relname = $1 AND a.attname IS NOT NULL
GROUP BY i.relname, ix.indisunique`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]IndexInfo{}
	for rows.Next() {
		var (
			name    string
			unique  bool
			columns sql.NullString
		)
		if err := rows.Scan(&name, &unique, &columns); err != nil {
			return nil, err
		}
		info := IndexInfo{Name: name, Unique: unique}
		if columns.Valid && columns.String != "" {
			info.Columns = strings.Split(columns.String, ",")
		}
		result[name] = info
	}
	return result, rows.Err()
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
