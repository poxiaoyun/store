package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net/url"
	"reflect"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib"
	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
)

var _ store.PingableStore = &Storage{}

const (
	DBDriverPostgres = "postgres"
	DBDriverMySQL    = "mysql"
)

// defaultMaxTransactionRetries bounds the retries of a transaction aborted by a
// retryable conflict, such as a deadlock or a serialization failure.
const defaultMaxTransactionRetries = 3

func NewDefaultOptions() *Options {
	return &Options{
		Addr:     "postgres:5432",
		Username: "postgres",
		Password: "",
		Driver:   DBDriverPostgres,
		Database: "",
		Params:   map[string]string{},
	}
}

// nolint: tagalign
type Options struct {
	Addr     string            `json:"addr" description:"database host addr"`
	Driver   string            `json:"driver" description:"databse driver, mysql or postgres"`
	Username string            `json:"username" description:"database username"`
	Password string            `json:"password" description:"database password"`
	Database string            `json:"database" description:"database to use"`
	Params   map[string]string `json:"params" description:"additional database connection parameters"`
}

// ConnectionString returns the connection string for the database without the driver schema.
// for mysql, it returns "user:password@tcp(host:port)/database".
// for postgres, it returns "user:password@host:port/database".
func (o *Options) ConnectionString() string {
	switch o.Driver {
	case DBDriverMySQL:
		values := url.Values{}
		values.Add("parseTime", "True")
		values.Add("loc", "UTC")
		// A conditional write decides NotFound or Conflict from the row count,
		// so it needs the matched rows rather than the changed rows.
		values.Add("clientFoundRows", "True")
		return fmt.Sprintf("mysql://%s:%s@tcp(%s)/%s?%s", o.Username, o.Password, o.Addr, o.Database, values.Encode())
	case DBDriverPostgres:
		values := url.Values{}
		values.Add("sslmode", "disable")
		values.Add("timezone", "UTC")
		if o.Database == "" {
			return fmt.Sprintf("postgres://%s:%s@%s?%s", o.Username, o.Password, o.Addr, values.Encode())
		}
		return fmt.Sprintf("postgres://%s:%s@%s/%s?%s", o.Username, o.Password, o.Addr, o.Database, values.Encode())
	default:
		return ""
	}
}

// core is the execution layer shared by every view of one SQL Store: the
// dialect, the model codec, the registered schema and the statement handles.
type core struct {
	dialect Dialect
	codec   *ModelCodec
	schema  *store.Schema
	// database opens transactions. A transaction-bound core has none, so it
	// cannot start another transaction.
	database Database
	// exec runs the statements of this core: the database at the root, the
	// open transaction inside a Transaction callback.
	exec StatementExecutor
}

// Storage is one scoped view of a SQL Store. Construct it with NewStorage. Scope
// derives a view with more scopes, and Transaction derives a view bound to an
// open transaction; both share the execution core.
type Storage struct {
	core   *core
	scopes []store.Scope
}

// NewStorage opens the configured database, reconciles the registered resource
// schema and returns the root Store.
func NewStorage(ctx context.Context, schema *store.Schema, options *Options) (*Storage, error) {
	dialect, err := NewDialect(options.Driver)
	if err != nil {
		return nil, err
	}
	cloned, err := schema.Clone()
	if err != nil {
		return nil, err
	}
	if err := createDatabaseIfNotExists(ctx, options); err != nil {
		return nil, fmt.Errorf("failed to create database: %w", err)
	}
	db, err := sql.Open(driverName(options.Driver), dataSourceName(options.Driver, options.ConnectionString()))
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}
	storage := &Storage{core: &core{
		dialect:  dialect,
		codec:    NewModelCodec(),
		schema:   cloned,
		database: db,
		exec:     db,
	}}
	if err := storage.ensureSchema(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return storage, nil
}

func driverName(driver string) string {
	switch driver {
	case DBDriverMySQL:
		return "mysql"
	case DBDriverPostgres:
		return "pgx"
	default:
		return driver
	}
}

// dataSourceName adapts the connection string to the driver: go-sql-driver
// wants the MySQL DSN without its scheme, while pgx accepts the URL as is.
func dataSourceName(driver, connectionString string) string {
	switch driver {
	case DBDriverMySQL:
		return strings.TrimPrefix(connectionString, "mysql://")
	default:
		return connectionString
	}
}

func createDatabaseIfNotExists(ctx context.Context, options *Options) error {
	nodboptions := *options
	nodboptions.Database = ""
	driver, dsn, dbname := nodboptions.Driver, nodboptions.ConnectionString(), options.Database

	switch driver {
	case DBDriverMySQL:
		db, err := sql.Open("mysql", dataSourceName(driver, dsn))
		if err != nil {
			return fmt.Errorf("failed to open database connection: %w", err)
		}
		defer db.Close()
		_, err = db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS `"+dbname+"`")
		return err
	case DBDriverPostgres:
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			return fmt.Errorf("failed to open database connection: %w", err)
		}
		defer db.Close()
		_, err = db.ExecContext(ctx, `CREATE DATABASE "`+dbname+`"`)
		// 42P04 duplicate_database means another instance created it first.
		var pgErr *pgconn.PgError
		if err != nil && stderrors.As(err, &pgErr) && pgErr.Code == "42P04" {
			return nil
		}
		return err
	default:
		return fmt.Errorf("empty or unsupported database driver: [%s]", driver)
	}
}

// Schema implements store.Store.
func (s *Storage) Schema() *store.Schema {
	return s.core.schema.Snapshot()
}

// Capabilities implements store.Store.
func (s *Storage) Capabilities() store.Capabilities {
	return store.Capabilities{Finalizers: true, OptimisticLock: true}
}

// Ping implements store.PingableStore.
func (s *Storage) Ping(ctx context.Context) error {
	return s.core.database.PingContext(ctx)
}

// Scope implements store.Store.
func (s *Storage) Scope(scopes ...store.Scope) store.Store {
	derived := *s
	derived.scopes = append(slices.Clone(s.scopes), scopes...)
	return &derived
}

// Status implements store.Store.
func (s *Storage) Status() store.StatusStorage {
	return &StatusStorage{storage: s}
}

// Watch implements store.Store. The adapter has no change stream, so it rejects
// the complete snapshot-to-live contract instead of approximating it.
func (s *Storage) Watch(ctx context.Context, obj store.ObjectList, opts ...store.WatchOption) (store.Watcher, error) {
	return nil, errors.NewUnsupported("SQL store does not support watch")
}

// Transaction implements store.TransactionStore. It runs fn with a Store bound
// to one open transaction; that Store must run every statement of the
// transaction. fn may run more than once when the database aborts the
// transaction with a retryable conflict, so it must be re-executable.
func (s *Storage) Transaction(ctx context.Context, fn func(ctx context.Context, tx store.Store) error, options ...store.TransactionOption) error {
	if s.core.database == nil {
		return errors.NewUnsupported("nested transactions are not supported")
	}
	opts := store.ApplyTransactionOptions(options)
	txOptions, err := TransactionLevel(opts.Isolation)
	if err != nil {
		return err
	}
	if opts.Timeout > 0 {
		timeoutCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
		defer cancel()
		ctx = timeoutCtx
	}
	retries := opts.MaxRetries
	if retries <= 0 {
		retries = defaultMaxTransactionRetries
	}
	for attempt := 0; ; attempt++ {
		if err := s.runTransaction(ctx, txOptions, fn); err != nil {
			if !s.core.dialect.IsRetryableConflict(err) || attempt >= retries {
				return err
			}
			if err := waitBeforeRetry(ctx, attempt+1); err != nil {
				return err
			}
			continue
		}
		return nil
	}
}

func (s *Storage) runTransaction(ctx context.Context, options *sql.TxOptions, fn func(ctx context.Context, tx store.Store) error) error {
	tx, err := s.core.database.BeginTx(ctx, options)
	if err != nil {
		return err
	}
	// A transaction-bound core keeps the dialect, codec and schema but has no
	// root handle, so its view cannot start another transaction.
	view := &Storage{
		core: &core{
			dialect: s.core.dialect,
			codec:   s.core.codec,
			schema:  s.core.schema,
			exec:    tx,
		},
		scopes: s.scopes,
	}
	if err := fn(ctx, view); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

// transactionOptions maps a portable isolation level onto the driver level. An
// empty level keeps the backend default, which is READ COMMITTED on PostgreSQL
// and REPEATABLE READ on MySQL.
func TransactionLevel(level store.IsolationLevel) (*sql.TxOptions, error) {
	switch level {
	case store.IsolationDefault:
		return nil, nil
	case store.IsolationReadCommitted:
		return &sql.TxOptions{Isolation: sql.LevelReadCommitted}, nil
	case store.IsolationRepeatableRead:
		return &sql.TxOptions{Isolation: sql.LevelRepeatableRead}, nil
	case store.IsolationSerializable:
		return &sql.TxOptions{Isolation: sql.LevelSerializable}, nil
	default:
		return nil, errors.NewUnsupported(fmt.Sprintf("unsupported isolation level %q", level))
	}
}

// waitBeforeRetry spaces out retries after an aborted transaction. The wait
// only reduces contention; correctness comes from running fn again.
func waitBeforeRetry(ctx context.Context, attempt int) error {
	delay := min(time.Duration(1<<uint(attempt-1))*5*time.Millisecond, 100*time.Millisecond)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type StatusStorage struct {
	storage *Storage
}

// Patch implements store.StatusStorage.
func (s *StatusStorage) Patch(ctx context.Context, obj store.Object, patch store.Patch, opts ...store.PatchOption) error {
	return s.storage.patch(ctx, obj, patch, true, store.ApplyPatchOptions(opts))
}

// Update implements store.StatusStorage.
func (s *StatusStorage) Update(ctx context.Context, obj store.Object, opts ...store.UpdateOption) error {
	return s.storage.update(ctx, obj, true, store.ApplyUpdateOptions(opts))
}

var _ store.StatusStorage = &StatusStorage{}

// Create implements store.Store.
func (s *Storage) Create(ctx context.Context, in store.Object, options ...store.CreateOption) error {
	opts := store.ApplyCreateOptions(options)
	if opts.TTL != 0 {
		return errors.NewUnsupported("SQL store does not support TTL")
	}
	if opts.DryRun {
		return errors.NewUnsupported("SQL store does not support dry-run")
	}
	resource, err := store.GetResource(in)
	if err != nil {
		return err
	}
	store.PrepareObjectForCreate(in, resource, s.scopes)
	in.SetResourceVersion(1)
	values := s.core.codec.ToDriverValueMap(in)
	for _, scope := range s.scopes {
		values[store.ScopeResourceToFieldName(scope.Resource)] = scope.Name
	}
	names := sortedKeys(values)
	quoted := make([]string, 0, len(names))
	placeholders := make([]string, 0, len(names))
	q := &QueryState{dialect: s.core.dialect}
	for _, name := range names {
		quoted = append(quoted, s.core.dialect.Quote(name))
		placeholders = append(placeholders, q.Bind(values[name]))
	}
	statement := "INSERT INTO " + s.core.dialect.Quote(resource) +
		" (" + strings.Join(quoted, ", ") + ") VALUES (" + strings.Join(placeholders, ", ") + ")"
	if _, err := s.core.exec.ExecContext(ctx, statement, q.args...); err != nil {
		return s.core.dialect.MapError(err, resource, in.GetID())
	}
	return nil
}

// Get implements store.Store.
func (s *Storage) Get(ctx context.Context, id string, into store.Object, options ...store.GetOption) error {
	return s.get(ctx, into, id, store.ApplyGetOptions(options))
}

func (s *Storage) get(ctx context.Context, into store.Object, id string, opts store.GetOptions) error {
	resource, err := store.GetResource(into)
	if err != nil {
		return err
	}
	if id == "" {
		return newEmptyIDStorageError(resource)
	}
	columns, err := s.selectColumns(into, opts.Fields, opts.Preconditions != nil)
	if err != nil {
		return err
	}
	q := &QueryState{dialect: s.core.dialect}
	where, err := s.whereClause(q, id, opts.LabelRequirements, opts.FieldRequirements)
	if err != nil {
		return err
	}
	statement := "SELECT " + columns + " FROM " + s.core.dialect.Quote(resource) + where + " LIMIT 1"
	rows, err := s.core.exec.QueryContext(ctx, statement, q.args...)
	if err != nil {
		return s.core.dialect.MapError(err, resource, id)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return s.core.dialect.MapError(err, resource, id)
		}
		return errors.NewNotFound(resource, id)
	}
	if err := s.core.codec.ScanOne(rows, into); err != nil {
		return s.core.dialect.MapError(err, resource, id)
	}
	return store.ValidatePreconditions(into, opts.Preconditions)
}

// Patch implements store.Store.
func (s *Storage) Patch(ctx context.Context, obj store.Object, patch store.Patch, options ...store.PatchOption) error {
	return s.patch(ctx, obj, patch, false, store.ApplyPatchOptions(options))
}

func (s *Storage) patch(ctx context.Context, obj store.Object, patch store.Patch, status bool, opts store.PatchOptions) error {
	if opts.DryRun {
		return errors.NewUnsupported("SQL store does not support dry-run")
	}
	values, err := mergeValues(patch, obj)
	if err != nil {
		return err
	}
	preconditions := opts.Preconditions
	if value, exists := values["resourceVersion"]; exists {
		var version int64
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(data, &version); err != nil {
			return errors.NewBadRequest("resourceVersion must be an integer")
		}
		if version != 0 {
			combined := store.Preconditions{}
			if preconditions != nil {
				combined = *preconditions
			}
			if combined.ResourceVersion != nil && *combined.ResourceVersion != version {
				return errors.NewConflict(obj.GetResource(), obj.GetID(), fmt.Errorf("incompatible version conditions"))
			}
			combined.ResourceVersion = &version
			preconditions = &combined
		}
	}
	return s.write(ctx, obj, values, true, status, false, opts.LabelRequirements, opts.FieldRequirements, preconditions)
}

// Update implements store.Store.
func (s *Storage) Update(ctx context.Context, obj store.Object, options ...store.UpdateOption) error {
	return s.update(ctx, obj, false, store.ApplyUpdateOptions(options))
}

func (s *Storage) update(ctx context.Context, obj store.Object, status bool, opts store.UpdateOptions) error {
	if opts.DryRun {
		return errors.NewUnsupported("SQL store does not support dry-run")
	}
	if opts.TTL != 0 {
		return errors.NewUnsupported("SQL store does not support TTL")
	}
	resource, err := store.GetResource(obj)
	if err != nil {
		return err
	}
	// Decode unstructured requests using the registered persistence model.
	model, err := s.core.schema.NewObject(resource)
	if err != nil {
		return err
	}
	if err := store.CopyObject(obj, model); err != nil {
		return errors.NewBadRequest(err.Error())
	}
	fields := map[string]reflect.Value{}
	s.core.codec.collectFieldValues(reflect.ValueOf(model), fields)
	values := map[string]any{}
	for name, value := range fields {
		values[name] = value.Interface()
	}
	var preconditions *store.Preconditions
	if version := obj.GetResourceVersion(); version != 0 {
		preconditions = &store.Preconditions{ResourceVersion: &version}
	}
	return s.write(ctx, obj, values, false, status, false, opts.LabelRequirements, opts.FieldRequirements, preconditions)
}

func (s *Storage) writeWhere(q *QueryState, id string, labels, fields store.Requirements, preconditions *store.Preconditions) (string, error) {
	where, err := s.whereClause(q, id, labels, fields)
	if err != nil {
		return "", err
	}
	if preconditions != nil {
		if preconditions.ResourceVersion != nil {
			where += " AND " + s.core.dialect.Quote("resourceVersion") + " = " + q.Bind(*preconditions.ResourceVersion)
		}
		if preconditions.UID != nil {
			where += " AND " + s.core.dialect.Quote("uid") + " = " + q.Bind(*preconditions.UID)
		}
	}
	return where, nil
}

func writeMiss(resource, id string, labels, fields store.Requirements, preconditions *store.Preconditions) error {
	if len(labels)+len(fields) > 0 || preconditions != nil && (preconditions.UID != nil || preconditions.ResourceVersion != nil) {
		return errors.NewConflict(resource, id, fmt.Errorf("write conditions did not match"))
	}
	return errors.NewNotFound(resource, id)
}

// atomicMutation uses the caller's transaction, or opens one without retrying
// the operation. It is only needed when completing deletion takes two statements.
func (s *Storage) atomicMutation(ctx context.Context, fn func(*Storage) error) error {
	if s.core.database == nil {
		return fn(s)
	}
	return s.runTransaction(ctx, nil, func(_ context.Context, tx store.Store) error { return fn(tx.(*Storage)) })
}

func (s *Storage) write(ctx context.Context, obj store.Object, values map[string]any, merge, status, batch bool, labels, fields store.Requirements, preconditions *store.Preconditions) error {
	resource, err := store.GetResource(obj)
	if err != nil {
		return err
	}
	id := obj.GetID()
	if !batch && id == "" {
		return newEmptyIDStorageError(resource)
	}
	q := &QueryState{dialect: s.core.dialect}
	plan, err := s.planWrite(q, obj, values, merge, status)
	if err != nil {
		return err
	}
	where, err := s.writeWhere(q, id, labels, fields, preconditions)
	if err != nil {
		return err
	}
	column := s.core.dialect.Quote("finalizers")
	deletion := s.core.dialect.Quote("deletionTimestamp")
	if batch && plan.changesFinalizers && len(plan.finalizers) > 0 {
		return errors.NewUnsupported("SQL batch patch cannot assign nonempty finalizers")
	}
	if batch && plan.changesFinalizers && len(labels)+len(fields) != 0 {
		return errors.NewUnsupported("SQL batch finalizer removal does not support selectors")
	}
	statement := "UPDATE " + s.core.dialect.Quote(resource) + " SET " + strings.Join(plan.assignments, ", ") + where
	run := func(tx *Storage) error {
		if plan.changesFinalizers && len(plan.finalizers) > 0 {
			current, err := tx.lockLifecycle(ctx, resource, id, labels, fields, preconditions)
			if err != nil {
				return err
			}
			if current.DeletionTimestamp != nil {
				for _, finalizer := range plan.finalizers {
					if !slices.Contains(current.Finalizers, finalizer) {
						return errors.NewBadRequest("cannot add finalizers to a terminating object")
					}
				}
			}
		}
		result, err := tx.core.exec.ExecContext(ctx, statement, q.args...)
		if err != nil {
			return tx.core.dialect.MapError(err, resource, id)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected > 0 && plan.changesFinalizers && len(plan.finalizers) == 0 {
			dq := &QueryState{dialect: tx.core.dialect}
			dw, err := tx.whereClause(dq, id, nil, nil)
			if err != nil {
				return err
			}
			_, err = tx.core.exec.ExecContext(ctx, "DELETE FROM "+tx.core.dialect.Quote(resource)+dw+" AND "+deletion+" IS NOT NULL AND "+tx.core.dialect.FinalizersEmpty(column), dq.args...)
			return tx.core.dialect.MapError(err, resource, id)
		}
		if affected > 0 || batch {
			return nil
		}
		return writeMiss(resource, id, labels, fields, preconditions)
	}
	if plan.changesFinalizers {
		return s.atomicMutation(ctx, run)
	}
	return run(s)
}

// Delete reads only lifecycle metadata under a row lock. Ordinary field writes
// remain native SQL, while deletion owns the finalizer decision and its effects.
func (s *Storage) Delete(ctx context.Context, obj store.Object, options ...store.DeleteOption) error {
	opts := store.ApplyDeleteOptions(options)
	if opts.DryRun {
		return errors.NewUnsupported("SQL store does not support dry-run")
	}
	resource, err := store.GetResource(obj)
	if err != nil {
		return err
	}
	if obj.GetID() == "" {
		return newEmptyIDStorageError(resource)
	}
	policy := store.DeletePropagationBackground
	if opts.PropagationPolicy != nil {
		policy = *opts.PropagationPolicy
	}
	return s.atomicMutation(ctx, func(tx *Storage) error {
		q := &QueryState{dialect: tx.core.dialect}
		where, err := tx.writeWhere(q, obj.GetID(), opts.LabelRequirements, opts.FieldRequirements, opts.Preconditions)
		if err != nil {
			return err
		}
		current, err := tx.lockLifecycle(ctx, resource, obj.GetID(), opts.LabelRequirements, opts.FieldRequirements, opts.Preconditions)
		if err != nil {
			return err
		}
		if store.PrepareObjectForDelete(current, policy) {
			_, err := tx.core.exec.ExecContext(ctx, "DELETE FROM "+tx.core.dialect.Quote(resource)+where, q.args...)
			return tx.core.dialect.MapError(err, resource, obj.GetID())
		}
		uq := &QueryState{dialect: tx.core.dialect}
		data, _ := json.Marshal(current.Finalizers)
		assignments := tx.core.dialect.Quote("finalizers") + " = " + tx.core.dialect.JSONValue(uq.Bind(string(data))) + ", " + tx.core.dialect.Quote("deletionTimestamp") + " = " + uq.Bind(current.DeletionTimestamp.Time) + ", " + tx.core.dialect.Quote("resourceVersion") + " = " + tx.core.dialect.Quote("resourceVersion") + " + 1"
		uw, err := tx.writeWhere(uq, obj.GetID(), nil, nil, nil)
		if err != nil {
			return err
		}
		_, err = tx.core.exec.ExecContext(ctx, "UPDATE "+tx.core.dialect.Quote(resource)+" SET "+assignments+uw, uq.args...)
		return tx.core.dialect.MapError(err, resource, obj.GetID())
	})
}

func (s *Storage) DeleteBatch(ctx context.Context, list store.ObjectList, opts ...store.DeleteBatchOption) error {
	options := store.ApplyDeleteBatchOptions(opts)
	if options.DryRun {
		return errors.NewUnsupported("SQL store does not support dry-run")
	}
	resource, err := store.GetResource(list)
	if err != nil {
		return err
	}
	if err := store.ValidateSelectorRequirements(options.LabelRequirements, options.FieldRequirements); err != nil {
		return errors.NewBadRequest(err.Error())
	}
	if deletionChangesPredicate(options.FieldRequirements) {
		return errors.NewUnsupported("SQL batch deletion cannot select by deletionTimestamp or resourceVersion")
	}
	return s.atomicMutation(ctx, func(tx *Storage) error {
		// The UPDATE locks every selected row before deciding which rows can be
		// removed. Its metadata changes must not change the selection predicate.
		q := &QueryState{dialect: tx.core.dialect}
		timestamp := q.Bind(store.ServerTimestampNow().
			Time)
		where, err := tx.whereClause(q, "", options.LabelRequirements, options.FieldRequirements)
		if err != nil {
			return err
		}
		deletion, version := tx.core.dialect.Quote("deletionTimestamp"), tx.core.dialect.Quote("resourceVersion")
		statement := "UPDATE " + tx.core.dialect.Quote(resource) + " SET " + deletion + " = COALESCE(" + deletion + ", " + timestamp + "), " + version + " = " + version + " + 1" + where
		if _, err := tx.core.exec.ExecContext(ctx, statement, q.args...); err != nil {
			return tx.core.dialect.MapError(err, resource, "")
		}
		dq := &QueryState{dialect: tx.core.dialect}
		dw, err := tx.whereClause(dq, "", options.LabelRequirements, options.FieldRequirements)
		if err != nil {
			return err
		}
		empty := tx.core.dialect.FinalizersEmpty(tx.core.dialect.Quote("finalizers"))
		_, err = tx.core.exec.ExecContext(ctx, "DELETE FROM "+tx.core.dialect.Quote(resource)+dw+" AND "+empty, dq.args...)
		return tx.core.dialect.MapError(err, resource, "")
	})
}

func (s *Storage) PatchBatch(ctx context.Context, list store.ObjectList, patch store.PatchBatch, opts ...store.PatchBatchOption) error {
	options := store.ApplyPatchBatchOptions(opts)
	if options.DryRun {
		return errors.NewUnsupported("SQL store does not support dry-run")
	}
	resource, err := store.GetResource(list)
	if err != nil {
		return err
	}
	obj, err := s.core.schema.NewObject(resource)
	if err != nil {
		return err
	}
	values, err := mergeValues(store.RawPatch(patch.Type(), patch.Data()), obj)
	if err != nil {
		return err
	}
	if _, conditional := values["resourceVersion"]; conditional {
		return errors.NewUnsupported("SQL batch patch cannot carry a version condition")
	}
	return s.write(ctx, obj, values, true, false, true, options.LabelRequirements, options.FieldRequirements, nil)
}

// Count implements store.Store.
func (s *Storage) Count(ctx context.Context, obj store.Object, options ...store.CountOption) (int, error) {
	opts := store.ApplyCountOptions(options)
	if opts.IncludeSubScopes {
		return 0, errors.NewUnsupported("SQL store does not support querying descendant scopes")
	}
	resource, err := store.GetResource(obj)
	if err != nil {
		return 0, err
	}
	q := &QueryState{dialect: s.core.dialect}
	where, err := s.whereClause(q, "", opts.LabelRequirements, opts.FieldRequirements)
	if err != nil {
		return 0, err
	}
	search, err := s.searchConditions(q, opts.Search, opts.SearchFields)
	if err != nil {
		return 0, err
	}
	if search != "" {
		where += " AND " + search
	}
	statement := "SELECT COUNT(*) FROM " + s.core.dialect.Quote(resource) + where
	var total int64
	if err := s.core.exec.QueryRowContext(ctx, statement, q.args...).
		Scan(&total); err != nil {
		return 0, s.core.dialect.MapError(err, resource, "")
	}
	return int(total), nil
}

// List implements store.Store. Cursor pagination needs a resumable token that
// this adapter does not produce, so it is rejected rather than degraded.
func (s *Storage) List(ctx context.Context, list store.ObjectList, options ...store.ListOption) error {
	opts := store.ApplyListOptions(options)
	if opts.IncludeSubScopes {
		return errors.NewUnsupported("SQL store does not support querying descendant scopes")
	}

	resource, err := store.GetResource(list)
	if err != nil {
		return err
	}
	page, err := store.PrepareListPage(resource, s.scopes, opts)
	if err != nil {
		return err
	}
	opts = page.Options
	items, err := store.GetItemsPtr(list)
	if err != nil {
		return err
	}
	q := &QueryState{dialect: s.core.dialect}
	builder := &ConditionBuilder{}
	builder.Add(s.scopeCondition(q))
	for _, scope := range s.scopes {
		field := store.ScopeResourceToFieldName(scope.Resource)
		builder.Add(s.core.dialect.Quote(field) + " = " + q.Bind(scope.Name))
	}
	search, err := s.searchConditions(q, opts.Search, opts.SearchFields)
	if err != nil {
		return err
	}
	builder.Add(search)
	if err := s.addRequirementConditions(builder, q, opts.LabelRequirements, opts.FieldRequirements); err != nil {
		return err
	}
	where := builder.Where()
	columns, err := s.selectColumns(items, opts.Fields, false)
	if err != nil {
		return err
	}
	order, err := s.orderBy(opts.Sort)
	if err != nil {
		return err
	}
	statement := "SELECT " + columns + " FROM " + s.core.dialect.Quote(resource) + where
	if order != "" {
		statement += " " + order
	}
	statement += s.window(q, max(opts.Offset, 0), opts.Limit)
	rows, err := s.core.exec.QueryContext(ctx, statement, q.args...)
	if err != nil {
		return s.core.dialect.MapError(err, resource, "")
	}
	defer rows.Close()
	if err := s.core.codec.ScanAll(rows, items); err != nil {
		return s.core.dialect.MapError(err, resource, "")
	}
	list.SetResource(resource)
	return page.Finish(list)
}

// newEmptyIDStorageError reports a request that omitted the object identity.
func newEmptyIDStorageError(resource string) error {
	return errors.NewBadRequest(fmt.Sprintf("empty id for resource %s", resource))
}

// sortedKeys returns map keys in a stable order, so generated statements stay
// comparable and tests stay deterministic.
func sortedKeys[V any](values map[string]V) []string {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func deletionChangesPredicate(requirements store.Requirements) bool {
	for _, requirement := range requirements {
		head, _, _ := strings.Cut(requirement.Key, ".")
		if head == "resourceVersion" || head == "deletionTimestamp" || deletionChangesPredicate(requirement.Requirements) {
			return true
		}
	}
	return false
}

// lockLifecycle reads only the metadata needed to decide a deletion transition.
// The caller owns a transaction so validation and mutation observe one locked row.
func (s *Storage) lockLifecycle(ctx context.Context, resource, id string, labels, fields store.Requirements, preconditions *store.Preconditions) (*store.ObjectMeta, error) {
	q := &QueryState{dialect: s.core.dialect}
	where, err := s.writeWhere(q, id, labels, fields, preconditions)
	if err != nil {
		return nil, err
	}
	rows, err := s.core.exec.QueryContext(ctx, "SELECT "+s.core.dialect.Quote("finalizers")+", "+s.core.dialect.Quote("deletionTimestamp")+" FROM "+s.core.dialect.Quote(resource)+where+" FOR UPDATE", q.args...)
	if err != nil {
		return nil, s.core.dialect.MapError(err, resource, id)
	}
	defer rows.Close()
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, err
		}
		return nil, writeMiss(resource, id, labels, fields, preconditions)
	}
	current := &store.ObjectMeta{ID: id, Resource: resource}
	if err := s.core.codec.ScanOne(rows, current); err != nil {
		return nil, err
	}
	return current, nil
}
