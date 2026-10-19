// Package mysql implements the MySQL dialect for SQL Store.
package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
	storesql "xiaoshiai.cn/store/sql"
)

// Open creates the database named by the native MySQL DSN if necessary and
// returns a caller-owned connection pool. Time decoding uses UTC, and affected
// row counts report matched rows so conditional Store writes can detect misses.
func Open(ctx context.Context, dsn string) (*sql.DB, error) {
	config, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	config.ParseTime = true
	config.Loc = time.UTC
	config.ClientFoundRows = true
	adminConfig := config.Clone()
	adminConfig.DBName = ""
	connector, err := mysql.NewConnector(adminConfig)
	if err != nil {
		return nil, err
	}
	admin := sql.OpenDB(connector)
	defer admin.Close()
	if _, err := admin.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+(Dialect{}).Quote(config.DBName)); err != nil {
		return nil, err
	}
	connector, err = mysql.NewConnector(config)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(connector), nil
}

// Dialect implements storesql.Dialect. Its zero value is ready to use.
type Dialect struct{}

var _ storesql.Dialect = Dialect{}

func (Dialect) Quote(ident string) string {
	return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
}

func (Dialect) Placeholder(int) string { return "?" }

func (Dialect) ColumnType(field string, t reflect.Type, size int, indexed bool) (string, error) {
	base := storesql.DerefType(t)
	switch {
	case storesql.IsTimeType(base):
		return "datetime(6)", nil
	case base.Kind() == reflect.String:
		if indexed {
			if size <= 0 {
				size = storesql.DefaultIndexedStringSize
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
	case base.Kind() == reflect.Slice && base.Elem().
		Kind() == reflect.Uint8:
		return "longblob", nil
	case base.Kind() == reflect.Map, base.Kind() == reflect.Struct,
		base.Kind() == reflect.Slice, base.Kind() == reflect.Array, base.Kind() == reflect.Interface:
		return "json", nil
	default:
		return "", fmt.Errorf("field %q of type %s has no MySQL column type", field, t)
	}
}

func (d Dialect) CreateTable(table string, columns []storesql.ColumnSpec) (string, error) {
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

func (d Dialect) AddColumn(table string, column storesql.ColumnSpec) (string, error) {
	definition, err := d.columnDefinition(column)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s", d.Quote(table), definition), nil
}

func (d Dialect) columnDefinition(column storesql.ColumnSpec) (string, error) {
	columnType, err := d.ColumnType(column.Name, column.Type, column.Size, column.Indexed)
	if err != nil {
		return "", err
	}
	return d.Quote(column.Name) + " " + columnType, nil
}

// mysqlRowLimit is MySQL's idiom for "no limit" when only an offset is
// requested, because MySQL rejects OFFSET without LIMIT.
const mysqlRowLimit = "18446744073709551615"

// Window renders the row window. MySQL rejects OFFSET without LIMIT.
func (Dialect) Window(limitPlaceholder, offsetPlaceholder string) string {
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

// JSONValue casts JSON text into a JSON value.
func (Dialect) JSONValue(placeholder string) string {
	return "CAST(" + placeholder + " AS JSON)"
}

func (Dialect) JSONText(column string, remainingPath []string) (string, error) {
	return fmt.Sprintf("JSON_UNQUOTE(JSON_EXTRACT(%s, %s))", column, storesql.QuoteLiteral(mysqlJSONPath(remainingPath))), nil
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

func (d Dialect) IndexDDL(table string, index store.Index) ([]string, error) {
	columns := make([]string, 0, len(index.Fields))
	statements := make([]string, 0, len(index.Fields)+1)
	for _, field := range index.Fields {
		if !storesql.IsNestedField(field) {
			columns = append(columns, d.Quote(field))
			continue
		}
		path := storesql.JSONPath(field)
		column := generatedColumnName(path)
		expression := d.indexExpression(path)
		// A unique index must materialize the expression. MySQL indexes a
		// stored generated column; plain secondary indexes accept a virtual one.
		kind := "VIRTUAL"
		if index.Unique {
			kind = "STORED"
		}
		statements = append(statements, fmt.Sprintf(
			"ALTER TABLE %s ADD COLUMN %s varchar(%d) GENERATED ALWAYS AS (%s) %s",
			d.Quote(table), d.Quote(column), storesql.DefaultIndexedStringSize, expression, kind,
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

func (Dialect) Columns(ctx context.Context, db storesql.StatementExecutor, table string) (map[string]storesql.ColumnInfo, error) {
	rows, err := db.QueryContext(ctx, `
SELECT COLUMN_NAME, COLUMN_TYPE
FROM information_schema.columns
WHERE table_schema = DATABASE() AND table_name = ?`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]storesql.ColumnInfo{}
	for rows.Next() {
		var info storesql.ColumnInfo
		if err := rows.Scan(&info.Name, &info.Type); err != nil {
			return nil, err
		}
		result[info.Name] = info
	}
	return result, rows.Err()
}

// indexExpression uses an explicit character set so catalog comparison does
// not depend on the session's default character set.
func (d Dialect) indexExpression(path []string) string {
	return "json_unquote(json_extract(" + d.Quote(path[0]) + ",_utf8mb4" + storesql.QuoteLiteral(mysqlJSONPath(path[1:])) + "))"
}

func (d Dialect) ValidateIndex(ctx context.Context, db storesql.StatementExecutor, table string, index store.Index) error {
	rows, err := db.QueryContext(ctx, `
SELECT s.NON_UNIQUE, s.COLUMN_NAME, c.GENERATION_EXPRESSION, s.SUB_PART
FROM information_schema.statistics s
LEFT JOIN information_schema.columns c
  ON c.TABLE_SCHEMA = s.TABLE_SCHEMA AND c.TABLE_NAME = s.TABLE_NAME AND c.COLUMN_NAME = s.COLUMN_NAME
WHERE s.TABLE_SCHEMA = DATABASE() AND s.TABLE_NAME = ? AND s.INDEX_NAME = ?
ORDER BY s.SEQ_IN_INDEX`, table, index.Name)
	if err != nil {
		return err
	}
	defer rows.Close()
	position := 0
	for rows.Next() {
		var nonUnique int
		var column, expression sql.NullString
		var prefix sql.NullInt64
		if err := rows.Scan(&nonUnique, &column, &expression, &prefix); err != nil {
			return err
		}
		if position >= len(index.Fields) || (nonUnique == 0) != index.Unique || prefix.Valid {
			return fmt.Errorf("table %q index %q conflicts with the declared definition", table, index.Name)
		}
		field := index.Fields[position]
		matches := column.String == field && expression.String == ""
		if storesql.IsNestedField(field) {
			// MySQL escapes quotes in INFORMATION_SCHEMA's expression text.
			expected := strings.ReplaceAll(d.indexExpression(storesql.JSONPath(field)), "'", `\'`)
			matches = expression.String == expected
		}
		if !matches {
			return fmt.Errorf("table %q index %q conflicts with declared field %q", table, index.Name, field)
		}
		position++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if position != len(index.Fields) {
		return fmt.Errorf("table %q index %q conflicts with the declared fields", table, index.Name)
	}
	return nil
}

func (Dialect) Indexes(ctx context.Context, db storesql.StatementExecutor, table string) (map[string]storesql.IndexInfo, error) {
	rows, err := db.QueryContext(ctx, `
SELECT INDEX_NAME, COLUMN_NAME
FROM information_schema.statistics
WHERE table_schema = DATABASE() AND table_name = ?
ORDER BY INDEX_NAME, SEQ_IN_INDEX`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]storesql.IndexInfo{}
	for rows.Next() {
		var (
			name   string
			column sql.NullString
		)
		if err := rows.Scan(&name, &column); err != nil {
			return nil, err
		}
		info := result[name]
		info.Primary = name == "PRIMARY"
		if column.Valid {
			info.Columns = append(info.Columns, column.String)
		}
		result[name] = info
	}
	return result, rows.Err()
}

func (Dialect) MapError(err error, resource, name string) error {
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

func (Dialect) IsRetryableConflict(err error) bool {
	var mysqlErr *mysql.MySQLError
	if stderrors.As(err, &mysqlErr) {
		switch mysqlErr.Number {
		case 1213, 1205: // deadlock, lock wait timeout
			return true
		}
	}
	return false
}

func (Dialect) Distinct(left, right string) string {
	return "NOT (BINARY " + left + " <=> BINARY " + right + ")"
}
func (Dialect) MergeDocument(q *storesql.QueryState, column string, patch map[string]any) string {
	data, _ := json.Marshal(patch)
	return "JSON_MERGE_PATCH(COALESCE(" + column + ", JSON_OBJECT()), CAST(" + q.Bind(string(data)) + " AS JSON))"
}
func (Dialect) FinalizersEmpty(column string) string {
	return "(" + column + " IS NULL OR JSON_TYPE(" + column + ") = 'NULL' OR JSON_LENGTH(" + column + ") = 0)"
}

// generatedColumnName identifies the materialized nested index path.
func generatedColumnName(path []string) string {
	return "idx_" + strings.Join(path, "_")
}
