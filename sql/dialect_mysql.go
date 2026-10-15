package sql

import (
	"context"
	"database/sql"
	stderrors "errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-sql-driver/mysql"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
)

type MySQLDialect struct{}

func (MySQLDialect) Name() string { return DBDriverMySQL }

func (MySQLDialect) Quote(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

func (MySQLDialect) Placeholder(int) string { return "?" }

func (MySQLDialect) ColumnType(field string, t reflect.Type, size int, indexed bool) (string, error) {
	base := DerefType(t)
	switch {
	case IsTimeType(base):
		return "datetime(6)", nil
	case base.Kind() == reflect.String:
		if indexed {
			if size <= 0 {
				size = DefaultIndexedStringSize
			}
			return fmt.Sprintf("varchar(%d)", size), nil
		}
		return "text", nil
	case base.Kind() == reflect.Bool:
		return "tinyint(1)", nil
	case base.Kind() >= reflect.Int && base.Kind() <= reflect.Int64,
		base.Kind() >= reflect.Uint && base.Kind() <= reflect.Uint64:
		return "bigint", nil
	case base.Kind() == reflect.Float32 || base.Kind() == reflect.Float64:
		return "double", nil
	case base.Kind() == reflect.Slice && base.Elem().Kind() == reflect.Uint8:
		return "longblob", nil
	case base.Kind() == reflect.Map, base.Kind() == reflect.Struct,
		base.Kind() == reflect.Slice, base.Kind() == reflect.Array, base.Kind() == reflect.Interface:
		return "json", nil
	default:
		return "", fmt.Errorf("field %q of type %s has no MySQL column type", field, t)
	}
}

func (d MySQLDialect) CreateTable(table string, columns []ColumnSpec, key []string) (string, error) {
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

func (d MySQLDialect) AddColumn(table string, column ColumnSpec) (string, error) {
	definition, err := d.columnDefinition(column)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", d.Quote(table), definition), nil
}

func (d MySQLDialect) columnDefinition(column ColumnSpec) (string, error) {
	columnType, err := d.ColumnType(column.Name, column.Type, column.Size, column.Indexed)
	if err != nil {
		return "", err
	}
	return d.Quote(column.Name) + " " + columnType, nil
}

// mysqlRowLimit is MySQL's idiom for "no limit" when only an offset is
// requested, because MySQL rejects OFFSET without LIMIT.
const mysqlRowLimit = "18446744073709551615"

// UpdateReturning returns the empty string: MySQL cannot return the written row.
func (MySQLDialect) UpdateReturning() string { return "" }

// Window renders the row window. MySQL rejects OFFSET without LIMIT.
func (MySQLDialect) Window(limitPlaceholder, offsetPlaceholder string) string {
	switch {
	case offsetPlaceholder == "":
		if limitPlaceholder == "" {
			return ""
		}
		return " LIMIT " + limitPlaceholder
	case limitPlaceholder == "":
		return " LIMIT " + mysqlRowLimit + " OFFSET " + offsetPlaceholder
	default:
		return " LIMIT " + limitPlaceholder + " OFFSET " + offsetPlaceholder
	}
}

// NotDistinct renders MySQL's null-safe equality.
func (MySQLDialect) NotDistinct(left, right string) string {
	return left + " <=> " + right
}

// JSONExtract renders a JSON extraction of one nested path.
func (MySQLDialect) JSONExtract(column string, remainingPath []string) (string, error) {
	return fmt.Sprintf("JSON_EXTRACT(%s, %s)", column, quotedLiteral(mysqlJSONPath(remainingPath))), nil
}

// Distinct renders MySQL's null-safe inequality, which negates the null-safe
// equality operator.
func (MySQLDialect) Distinct(left, right string) string {
	return "NOT (" + left + " <=> " + right + ")"
}

// JSONValue casts JSON text into a JSON value.
func (MySQLDialect) JSONValue(placeholder string) string {
	return "CAST(" + placeholder + " AS JSON)"
}

func (MySQLDialect) JSONText(column string, remainingPath []string) (string, error) {
	return fmt.Sprintf("JSON_UNQUOTE(JSON_EXTRACT(%s, %s))", column, quotedLiteral(mysqlJSONPath(remainingPath))), nil
}

// mysqlJSONPath renders a path whose segments are quoted, so a label key that
// contains a dot stays one segment.
func mysqlJSONPath(path []string) string {
	segments := make([]string, 0, len(path))
	for _, segment := range path {
		escaped := strings.ReplaceAll(segment, "\\", "\\\\")
		escaped = strings.ReplaceAll(escaped, `"`, `\"`)
		segments = append(segments, `"`+escaped+`"`)
	}
	return "$." + strings.Join(segments, ".")
}

func (d MySQLDialect) JSONSet(column string, remainingPath []string, placeholder string) string {
	return fmt.Sprintf("JSON_SET(%s, %s, %s)", column, quotedLiteral(mysqlJSONPath(remainingPath)), placeholder)
}

func (d MySQLDialect) JSONRemove(column string, remainingPath []string) string {
	return fmt.Sprintf("JSON_REMOVE(%s, %s)", column, quotedLiteral(mysqlJSONPath(remainingPath)))
}

func (d MySQLDialect) IndexDDL(table string, index store.Index) ([]string, error) {
	columns := make([]string, 0, len(index.Fields))
	statements := make([]string, 0, len(index.Fields)+1)
	for _, field := range index.Fields {
		if !IsNestedField(field) {
			columns = append(columns, d.Quote(field))
			continue
		}
		path := JSONPath(field)
		column := GeneratedColumnName(table, path)
		expression, err := d.JSONText(d.Quote(path[0]), path[1:])
		if err != nil {
			return nil, err
		}
		// A unique index must materialize the expression. MySQL indexes a
		// stored generated column; plain secondary indexes accept a virtual one.
		kind := "VIRTUAL"
		if index.Unique {
			kind = "STORED"
		}
		statements = append(statements, fmt.Sprintf(
			"ALTER TABLE %s ADD COLUMN %s varchar(%d) GENERATED ALWAYS AS (%s) %s",
			d.Quote(table), d.Quote(column), DefaultIndexedStringSize, expression, kind,
		))
		columns = append(columns, d.Quote(column))
	}
	unique := ""
	if index.Unique {
		unique = "UNIQUE "
	}
	// MySQL has no partial index, so a nullable unique index relies on the
	// engine treating NULL values as distinct, which holds for a generated
	// column whose expression yields NULL for missing paths.
	statements = append(statements, fmt.Sprintf("CREATE %sINDEX %s ON %s (%s)",
		unique, d.Quote(index.Name), d.Quote(table), strings.Join(columns, ", ")))
	return statements, nil
}

func (MySQLDialect) Columns(ctx context.Context, db StatementExecutor, table string) (map[string]ColumnInfo, error) {
	rows, err := db.QueryContext(ctx, `
SELECT COLUMN_NAME, COLUMN_TYPE
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = ?`, table)
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

func (MySQLDialect) Indexes(ctx context.Context, db StatementExecutor, table string) (map[string]IndexInfo, error) {
	rows, err := db.QueryContext(ctx, `
SELECT INDEX_NAME, NON_UNIQUE, COLUMN_NAME
FROM information_schema.statistics
WHERE table_schema = DATABASE() AND table_name = ?
ORDER BY INDEX_NAME, SEQ_IN_INDEX`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]IndexInfo{}
	for rows.Next() {
		var (
			name      string
			nonUnique int
			column    sql.NullString
		)
		if err := rows.Scan(&name, &nonUnique, &column); err != nil {
			return nil, err
		}
		info := result[name]
		info.Name = name
		info.Unique = nonUnique == 0
		if column.Valid {
			info.Columns = append(info.Columns, column.String)
		}
		result[name] = info
	}
	return result, rows.Err()
}

func (MySQLDialect) MapError(err error, resource, name string) error {
	if stderrors.Is(err, sql.ErrNoRows) {
		return errors.NewNotFound(resource, name)
	}
	var mysqlErr *mysql.MySQLError
	if stderrors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1062: // ER_DUP_ENTRY
			return errors.NewAlreadyExists(resource, name)
		case 1452: // ER_NO_REFERENCED_ROW_2
			return errors.NewNotFound(resource, name)
		case 1048, 1364: // column cannot be null, field has no default
			return errors.NewBadRequest(fmt.Sprintf("missing required value for resource %s", resource))
		}
	}
	return err
}

func (MySQLDialect) IsRetryableConflict(err error) bool {
	var mysqlErr *mysql.MySQLError
	if stderrors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1213, 1205: // deadlock, lock wait timeout
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
