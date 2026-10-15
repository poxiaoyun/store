package sql

import (
	"context"
	"database/sql"
)

// StatementExecutor is the statement surface shared by a *sql.DB and a *sql.Tx. A Store
// holds one StatementExecutor, so a transaction-bound Store is an ordinary Store whose
// StatementExecutor is the open transaction.
type StatementExecutor interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Database additionally opens transactions and reports connectivity. *sql.DB
// implements it; a transaction does not, which is why a transaction-bound
// Store never opens another transaction.
type Database interface {
	StatementExecutor
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
	PingContext(ctx context.Context) error
}
