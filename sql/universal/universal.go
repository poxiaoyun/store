// Package universal selects the built-in SQL connection and dialect by driver name.
// Applications with a fixed database can use its dialect package directly.
package universal

import (
	"context"
	"database/sql"
	"fmt"

	"xiaoshiai.cn/common/errors"
	storesql "xiaoshiai.cn/store/sql"
	"xiaoshiai.cn/store/sql/mysql"
	"xiaoshiai.cn/store/sql/postgres"
)

const (
	DriverMySQL    = "mysql"
	DriverPostgres = "postgres"
)

// Open provisions and opens the selected database using its native connection
// string, and returns the matching dialect. The caller owns and closes the pool.
// MySQL accepts a go-sql-driver DSN; PostgreSQL accepts a pgx connection string.
// An unknown driver returns Unsupported before opening a connection.
func Open(ctx context.Context, driver, dsn string) (*sql.DB, storesql.Dialect, error) {
	switch driver {
	case DriverMySQL:
		db, err := mysql.Open(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		return db, mysql.Dialect{}, nil
	case DriverPostgres:
		db, err := postgres.Open(ctx, dsn)
		if err != nil {
			return nil, nil, err
		}
		return db, postgres.Dialect{}, nil
	default:
		return nil, nil, errors.NewUnsupported(fmt.Sprintf("unsupported SQL driver %q", driver))
	}
}
