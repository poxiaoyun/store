package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"net/http"
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
	return store.Capabilities{
		LabelSelector:    true,
		FieldSelector:    true,
		Search:           true,
		Sort:             true,
		Projection:       true,
		OptimisticLock:   true,
		DeleteBatch:      true,
		PatchBatch:       true,
		Transaction:      true,
		SecondaryIndexes: true,
		UniqueIndexes:    true,
	}
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
		err := s.runTransaction(ctx, txOptions, fn)
		if err == nil {
			return nil
		}
		if !s.core.dialect.IsRetryableConflict(err) || attempt >= retries {
			return err
		}
		if err := waitBeforeRetry(ctx, attempt+1); err != nil {
			return err
		}
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
	delay := time.Duration(1<<uint(attempt-1)) * 5 * time.Millisecond
	if delay > 100*time.Millisecond {
		delay = 100 * time.Millisecond
	}
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

// patch renders one patch as a conditional write over the latest stored object.
func (s *Storage) patch(ctx context.Context, obj store.Object, patch store.Patch, status bool, opts store.PatchOptions) error {
	decoded, err := store.DecodePatch(patch, obj)
	if err != nil {
		return err
	}
	return s.patchLatest(ctx, obj, decoded, status, opts)
}

// patchLatest writes patch fields to the latest stored object with one
// conditional write. The write never carries the version the caller observed:
// only the patch's own conditions and the caller's preconditions decide whether
// it applies, so a concurrent change is applied instead of reported.
func (s *Storage) patchLatest(ctx context.Context, obj store.Object, decoded store.DecodedPatch, status bool, opts store.PatchOptions) error {
	resource, err := store.GetResource(obj)
	if err != nil {
		return err
	}
	id := obj.GetID()
	if id == "" {
		return newEmptyIDStorageError(resource)
	}
	if err := store.ValidateSelectorRequirements(opts.LabelRequirements, opts.FieldRequirements); err != nil {
		return errors.NewBadRequest(err.Error())
	}
	modelFields, err := s.core.codec.ModelFields(obj)
	if err != nil {
		return err
	}
	types := make(map[string]reflect.Type, len(modelFields))
	for _, field := range modelFields {
		types[field.Name] = field.Type
	}
	q := &QueryState{dialect: s.core.dialect}
	writable, typesOf, emptiesFinalizers, err := s.selectPatchFields(decoded.Fields, types, s.writeOwnedColumns(), status)
	if err != nil {
		return err
	}
	if len(writable) == 0 && len(decoded.Tests) == 0 {
		return errors.NewBadRequest("patch changes no writable field")
	}
	assignments, err := s.patchSetAssignments(q, writable, typesOf, businessColumns(modelFields))
	if err != nil {
		return err
	}
	version := s.core.dialect.Quote("resourceVersion")
	assignments = append(assignments, version+" = "+version+" + 1")
	conditions, err := s.identityConditions(q, id, 0)
	if err != nil {
		return err
	}
	if err := s.addRequirementConditionsTo(&conditions, q, opts.LabelRequirements, opts.FieldRequirements); err != nil {
		return err
	}
	if err := s.addPreconditionConditions(&conditions, q, opts.Preconditions); err != nil {
		return err
	}
	if decoded.Version != 0 {
		conditions = append(conditions, version+" = "+q.Bind(decoded.Version))
	}
	for _, test := range decoded.Tests {
		condition, err := s.patchTestCondition(q, test, types)
		if err != nil {
			return err
		}
		conditions = append(conditions, condition)
	}
	statement := "UPDATE " + s.core.dialect.Quote(resource) +
		" SET " + strings.Join(assignments, ", ") +
		" WHERE " + strings.Join(conditions, " AND ")
	// A miss of a write that carries no condition can only mean the object is
	// gone or unselected, which is NotFound without re-reading.
	miss := func(context.Context) error {
		return errors.NewNotFound(resource, id)
	}
	if len(decoded.Tests) > 0 {
		// A patch test observes the stored object, so a write that matched no
		// row after the tests were checked found a stale test value.
		miss = func(ctx context.Context) error {
			return s.patchTestMissError(ctx, obj, id, opts.LabelRequirements, opts.FieldRequirements, opts.Preconditions)
		}
	} else if decoded.Version != 0 || preconditionsPresent(opts.Preconditions) {
		miss = func(ctx context.Context) error {
			return s.writeMissError(ctx, obj, id, opts.LabelRequirements, opts.FieldRequirements, opts.Preconditions)
		}
	}
	return s.runConditionalWrite(ctx, obj, resource, id, statement, q.args, miss, emptiesFinalizers)
}

// patchTestMissError reports a patch whose test condition did not hold. A stale
// patch test is invalid input rather than a concurrent change.
func (s *Storage) patchTestMissError(ctx context.Context, obj store.Object, id string, labels, fields store.Requirements, preconditions *store.Preconditions) error {
	resource, err := store.GetResource(obj)
	if err != nil {
		return err
	}
	current := store.NewObject(obj)
	if err := s.get(ctx, current, id, store.GetOptions{LabelRequirements: labels, FieldRequirements: fields}); err != nil {
		return err
	}
	if err := store.ValidatePreconditions(current, preconditions); err != nil {
		return err
	}
	// A failed JSON patch test is unprocessable input, matching the mapping the
	// shared patch helpers use.
	return errors.NewCustomError(http.StatusUnprocessableEntity, errors.StatusReasonInvalid,
		fmt.Sprintf("invalid %s %q: patch test failed", resource, id))
}

// Update implements store.Store.
func (s *Storage) Update(ctx context.Context, into store.Object, options ...store.UpdateOption) error {
	return s.update(ctx, into, false, store.ApplyUpdateOptions(options))
}

func (s *Storage) update(ctx context.Context, into store.Object, status bool, opts store.UpdateOptions) error {
	resource, err := store.GetResource(into)
	if err != nil {
		return err
	}
	id := into.GetID()
	if id == "" {
		return newEmptyIDStorageError(resource)
	}
	if err := store.ValidateSelectorRequirements(opts.LabelRequirements, opts.FieldRequirements); err != nil {
		return errors.NewBadRequest(err.Error())
	}
	modelFields, err := s.core.codec.ModelFields(into)
	if err != nil {
		return err
	}
	// An update replaces the object, so it writes every writable column.
	goValues := map[string]reflect.Value{}
	s.core.codec.collectFieldValues(reflect.ValueOf(into), goValues)
	business := businessColumns(modelFields)
	owned := s.writeOwnedColumns()
	q := &QueryState{dialect: s.core.dialect}
	writable := make([]ModelField, 0, len(modelFields))
	for _, field := range modelFields {
		if _, skip := owned[field.Name]; skip {
			continue
		}
		// A normal update keeps the stored status; the status subresource writes
		// nothing else.
		if (status && field.Name != "status") || (!status && field.Name == "status") {
			continue
		}
		if _, err := s.scopeColumn(field.Name); err != nil {
			continue
		}
		writable = append(writable, field)
	}
	if len(writable) == 0 {
		return errors.NewBadRequest("update changes no writable field")
	}
	// The generation predicate renders before the assignments it compares,
	// because MySQL evaluates SET expressions left to right.
	changes := make([]string, 0, len(writable))
	if !status {
		for _, field := range writable {
			if _, ok := business[field.Name]; !ok {
				continue
			}
			value, ok := goValues[field.Name]
			if !ok {
				continue
			}
			rendered, err := s.modelFieldValue(q, field, value)
			if err != nil {
				return err
			}
			changes = append(changes, s.core.dialect.Distinct(s.core.dialect.Quote(field.Name), rendered))
		}
	}
	assignments := make([]string, 0, len(writable)+2)
	if len(changes) > 0 {
		generation := s.core.dialect.Quote("generation")
		assignments = append(assignments, generation+" = "+generation+" + (CASE WHEN "+strings.Join(changes, " OR ")+" THEN 1 ELSE 0 END)")
	}
	for _, field := range writable {
		value, ok := goValues[field.Name]
		if !ok {
			continue
		}
		rendered, err := s.modelFieldValue(q, field, value)
		if err != nil {
			return err
		}
		assignments = append(assignments, s.core.dialect.Quote(field.Name)+" = "+rendered)
	}
	version := s.core.dialect.Quote("resourceVersion")
	assignments = append(assignments, version+" = "+version+" + 1")
	conditions, err := s.identityConditions(q, id, 0)
	if err != nil {
		return err
	}
	if err := s.addRequirementConditionsTo(&conditions, q, opts.LabelRequirements, opts.FieldRequirements); err != nil {
		return err
	}
	// The caller's version is the only precondition an update carries. Without
	// it a miss can only mean the object is gone or unselected, which is
	// NotFound without re-reading.
	miss := func(context.Context) error {
		return errors.NewNotFound(resource, id)
	}
	if requested := into.GetResourceVersion(); requested != 0 {
		conditions = append(conditions, version+" = "+q.Bind(requested))
		miss = func(ctx context.Context) error {
			return s.writeMissError(ctx, into, id, opts.LabelRequirements, opts.FieldRequirements, nil)
		}
	}
	statement := "UPDATE " + s.core.dialect.Quote(resource) +
		" SET " + strings.Join(assignments, ", ") +
		" WHERE " + strings.Join(conditions, " AND ")
	emptiesFinalizers := !status && len(into.GetFinalizers()) == 0
	return s.runConditionalWrite(ctx, into, resource, id, statement, q.args, miss, emptiesFinalizers)
}

// modelFieldValue renders one model field of a replacing write. A document
// column receives JSON text, which the dialect casts into the column type.
func (s *Storage) modelFieldValue(q *QueryState, field ModelField, value reflect.Value) (string, error) {
	if !IsDocumentType(field.Type) {
		return q.Bind(ToDriverValue(value)), nil
	}
	text, err := jsonText(value.Interface())
	if err != nil {
		return "", err
	}
	return s.core.dialect.JSONValue(q.Bind(text)), nil
}

// writeOwnedColumns are the columns a write never takes from the caller: the
// store owns its server fields and computes the generation.
func (s *Storage) writeOwnedColumns() map[string]struct{} {
	owned := map[string]struct{}{"generation": {}}
	for _, name := range store.ServerOwnedUpdateFields() {
		owned[name] = struct{}{}
	}
	return owned
}

// selectPatchFields keeps the writable fields of a patch, reporting the patch's
// own version condition and whether it empties the finalizers.
func (s *Storage) selectPatchFields(fields []store.PatchField, types map[string]reflect.Type, serverOwned map[string]struct{}, status bool) ([]store.PatchField, []reflect.Type, bool, error) {
	writable := make([]store.PatchField, 0, len(fields))
	typesOf := make([]reflect.Type, 0, len(fields))
	emptiesFinalizers := false
	for _, field := range fields {
		if _, skip := serverOwned[field.Column]; skip {
			continue
		}
		// A normal write keeps the stored status; the status subresource writes
		// nothing else.
		if (status && field.Column != "status") || (!status && field.Column == "status") {
			continue
		}
		if _, err := s.scopeColumn(field.Column); err != nil {
			return nil, nil, false, err
		}
		columnType, ok := types[field.Column]
		if !ok {
			return nil, nil, false, errors.NewBadRequest(fmt.Sprintf("object has no field %q", field.Column))
		}
		if field.Column == "finalizers" && field.EmptiesValue() {
			emptiesFinalizers = true
		}
		writable = append(writable, field)
		typesOf = append(typesOf, columnType)
	}
	return writable, typesOf, emptiesFinalizers, nil
}

// patchSetAssignments renders the SET list of one patch write. The generation
// predicate renders before the assignments it compares, because MySQL evaluates
// SET expressions left to right and would otherwise compare a column against the
// value this statement already wrote.
func (s *Storage) patchSetAssignments(q *QueryState, fields []store.PatchField, typesOf []reflect.Type, business map[string]struct{}) ([]string, error) {
	changes := make([]string, 0, len(fields))
	for index, field := range fields {
		if _, ok := business[field.Column]; !ok {
			continue
		}
		changed, err := s.patchFieldValue(q, field, typesOf[index])
		if err != nil {
			return nil, err
		}
		changes = append(changes, s.core.dialect.Distinct(s.core.dialect.Quote(field.Column), changed))
	}
	assignments := make([]string, 0, len(fields)+2)
	if len(changes) > 0 {
		generation := s.core.dialect.Quote("generation")
		assignments = append(assignments, generation+" = "+generation+" + (CASE WHEN "+strings.Join(changes, " OR ")+" THEN 1 ELSE 0 END)")
	}
	for index, field := range fields {
		value, err := s.patchFieldValue(q, field, typesOf[index])
		if err != nil {
			return nil, err
		}
		assignments = append(assignments, s.core.dialect.Quote(field.Column)+" = "+value)
	}
	return assignments, nil
}

// patchTestCondition renders a JSON patch test as a write condition, comparing
// the stored value with the tested value without confusing NULL and absent.
func (s *Storage) patchTestCondition(q *QueryState, test store.PatchTest, types map[string]reflect.Type) (string, error) {
	columnType, ok := types[test.Path[0]]
	if !ok {
		return "", errors.NewBadRequest(fmt.Sprintf("object has no field %q", test.Path[0]))
	}
	column := s.core.dialect.Quote(test.Path[0])
	if len(test.Path) == 1 {
		if !IsDocumentType(columnType) {
			return s.core.dialect.NotDistinct(column, q.Bind(driverValue(test.Value))), nil
		}
		text, err := jsonText(test.Value)
		if err != nil {
			return "", err
		}
		return s.core.dialect.NotDistinct(column, s.core.dialect.JSONValue(q.Bind(text))), nil
	}
	if !IsDocumentType(columnType) {
		return "", errors.NewBadRequest(fmt.Sprintf("field %q is not a document", test.Path[0]))
	}
	extracted, err := s.core.dialect.JSONExtract(column, test.Path[1:])
	if err != nil {
		return "", err
	}
	text, err := jsonText(test.Value)
	if err != nil {
		return "", err
	}
	return s.core.dialect.NotDistinct(extracted, s.core.dialect.JSONValue(q.Bind(text))), nil
}

// runConditionalWrite executes one conditional write and loads the written
// object into obj. A backend that can return the written row needs no follow-up
// read; the others read it back with the same conditions.
func (s *Storage) runConditionalWrite(ctx context.Context, obj store.Object, resource, id, statement string, args []any, miss func(context.Context) error, emptiesFinalizers bool) error {
	if returning := s.core.dialect.UpdateReturning(); returning != "" {
		rows, err := s.core.exec.QueryContext(ctx, statement+returning, args...)
		if err != nil {
			return s.core.dialect.MapError(err, resource, id)
		}
		defer rows.Close()
		if !rows.Next() {
			if err := rows.Err(); err != nil {
				return s.core.dialect.MapError(err, resource, id)
			}
			return miss(ctx)
		}
		if err := s.core.codec.ScanOne(rows, obj); err != nil {
			return s.core.dialect.MapError(err, resource, id)
		}
	} else {
		result, err := s.runWriteStatement(ctx, statement, args)
		if err != nil {
			return s.core.dialect.MapError(err, resource, id)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if affected == 0 {
			return miss(ctx)
		}
		if err := s.readBack(ctx, obj, id); err != nil {
			return err
		}
	}
	if emptiesFinalizers {
		return s.completeDeletion(ctx, resource, id, obj.GetResourceVersion())
	}
	return nil
}

// runWriteStatement runs one write statement, retrying a retryable transaction
// conflict. A statement that the database rolled back had no effect, so running
// it again is safe, and concurrent single-row writes are expected to deadlock on
// some backends.
func (s *Storage) runWriteStatement(ctx context.Context, statement string, args []any) (sql.Result, error) {
	retries := defaultMaxTransactionRetries
	for attempt := 0; ; attempt++ {
		result, err := s.core.exec.ExecContext(ctx, statement, args...)
		if err == nil {
			return result, nil
		}
		if !s.core.dialect.IsRetryableConflict(err) || attempt >= retries {
			return nil, err
		}
		if err := waitBeforeRetry(ctx, attempt+1); err != nil {
			return nil, err
		}
	}
}

// patchFieldValue renders the value one patch field writes. A document column
// receives JSON text, which the dialect casts into the column's document type. A
// field with a source path reads another field of the same row, which keeps a
// copy or move inside the statement.
func (s *Storage) patchFieldValue(q *QueryState, field store.PatchField, columnType reflect.Type) (string, error) {
	document := IsDocumentType(columnType)
	if len(field.FromPath) > 0 {
		return s.sourceFieldValue(field, document)
	}
	if len(field.Path) == 0 {
		if field.Remove {
			return "NULL", nil
		}
		if !document {
			return q.Bind(driverValue(field.Value)), nil
		}
		text, err := jsonText(field.Value)
		if err != nil {
			return "", err
		}
		return s.core.dialect.JSONValue(q.Bind(text)), nil
	}
	if !document {
		return "", errors.NewBadRequest(fmt.Sprintf("field %q is not a document", field.Column))
	}
	column := s.core.dialect.Quote(field.Column)
	if field.Remove {
		return s.core.dialect.JSONRemove(column, field.Path), nil
	}
	text, err := jsonText(field.Value)
	if err != nil {
		return "", err
	}
	return s.core.dialect.JSONSet(column, field.Path, s.core.dialect.JSONValue(q.Bind(text))), nil
}

// sourceFieldValue renders a value read from another field of the same row.
func (s *Storage) sourceFieldValue(field store.PatchField, document bool) (string, error) {
	column := s.core.dialect.Quote(field.FromPath[0])
	if len(field.FromPath) == 1 {
		return column, nil
	}
	if !document {
		return "", errors.NewUnsupported("copying a nested value into a scalar field is not supported")
	}
	return s.core.dialect.JSONExtract(column, field.FromPath[1:])
}

// jsonText encodes a patch value as the JSON text a document column accepts.
func jsonText(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", errors.NewBadRequest(err.Error())
	}
	return string(data), nil
}

// businessColumns returns the columns whose change bumps the generation.
func businessColumns(fields []ModelField) map[string]struct{} {
	names := make(map[string]any, len(fields))
	for _, field := range fields {
		names[field.Name] = nil
	}
	result := make(map[string]struct{})
	for name := range store.ObjectBusinessFields(names) {
		result[name] = struct{}{}
	}
	return result
}

// scopeColumn reports whether a field is a scope column, which is part of an
// object's identity and therefore not patchable.
func (s *Storage) scopeColumn(name string) (struct{}, error) {
	for _, scope := range s.scopes {
		if store.ScopeResourceToFieldName(scope.Resource) == name {
			return struct{}{}, errors.NewBadRequest(fmt.Sprintf("field %q is part of the object identity", name))
		}
	}
	return struct{}{}, nil
}

// addRequirementConditionsTo appends validated label and field requirements to
// an existing condition list.
func (s *Storage) addRequirementConditionsTo(conditions *[]string, q *QueryState, labels, fields store.Requirements) error {
	builder := &ConditionBuilder{}
	if err := s.addRequirementConditions(builder, q, labels, fields); err != nil {
		return err
	}
	*conditions = append(*conditions, builder.conditions...)
	return nil
}

// addPreconditionConditions folds caller preconditions into a write.
// preconditionsPresent reports whether the caller supplied an effective
// precondition. A nil Preconditions, or one whose fields are all nil, adds no
// condition to a write.
func preconditionsPresent(preconditions *store.Preconditions) bool {
	return preconditions != nil && (preconditions.UID != nil || preconditions.ResourceVersion != nil)
}

func (s *Storage) addPreconditionConditions(conditions *[]string, q *QueryState, preconditions *store.Preconditions) error {
	if preconditions == nil {
		return nil
	}
	if preconditions.UID != nil {
		*conditions = append(*conditions, s.core.dialect.Quote("uid")+" = "+q.Bind(*preconditions.UID))
	}
	if preconditions.ResourceVersion != nil {
		*conditions = append(*conditions, s.core.dialect.Quote("resourceVersion")+" = "+q.Bind(*preconditions.ResourceVersion))
	}
	return nil
}

// readBack loads the written object when the backend cannot return it.
func (s *Storage) readBack(ctx context.Context, obj store.Object, id string) error {
	return s.get(ctx, obj, id, store.GetOptions{})
}

// completeDeletion removes an object whose finalizers the write just emptied
// while it was terminating. The condition repeats the version this write
// produced, so a concurrent writer keeps the object.
func (s *Storage) completeDeletion(ctx context.Context, resource, id string, version int64) error {
	q := &QueryState{dialect: s.core.dialect}
	conditions, err := s.identityConditions(q, id, version)
	if err != nil {
		return err
	}
	conditions = append(conditions, s.core.dialect.Quote("deletionTimestamp")+" IS NOT NULL")
	statement := "DELETE FROM " + s.core.dialect.Quote(resource) + " WHERE " + strings.Join(conditions, " AND ")
	if _, err := s.core.exec.ExecContext(ctx, statement, q.args...); err != nil {
		return s.core.dialect.MapError(err, resource, id)
	}
	return nil
}

// writeMissError reports why a conditional write matched no row, using the same
// mapping a read produces: a missing or unselected object is NotFound, and a
// precondition mismatch is Conflict.
func (s *Storage) writeMissError(ctx context.Context, obj store.Object, id string, labels, fields store.Requirements, preconditions *store.Preconditions) error {
	resource, err := store.GetResource(obj)
	if err != nil {
		return err
	}
	current := store.NewObject(obj)
	if err := s.get(ctx, current, id, store.GetOptions{LabelRequirements: labels, FieldRequirements: fields}); err != nil {
		return err
	}
	if err := store.ValidatePreconditions(current, preconditions); err != nil {
		return err
	}
	return errors.NewConflict(resource, id, fmt.Errorf("object changed during the write"))
}

func (s *Storage) saveCurrent(ctx context.Context, resource string, object store.Object, expectedVersion int64) error {
	// A replace write owns the whole row, so it writes every column rather than
	// only the non-zero fields.
	values, err := s.core.codec.ToReplaceDriverValueMap(object)
	if err != nil {
		return err
	}
	names := sortedKeys(values)
	q := &QueryState{dialect: s.core.dialect}
	assignments := make([]string, 0, len(names))
	for _, name := range names {
		assignments = append(assignments, s.core.dialect.Quote(name)+" = "+q.Bind(values[name]))
	}
	conditions, err := s.identityConditions(q, object.GetID(), expectedVersion)
	if err != nil {
		return err
	}
	statement := "UPDATE " + s.core.dialect.Quote(resource) +
		" SET " + strings.Join(assignments, ", ") +
		" WHERE " + strings.Join(conditions, " AND ")
	result, err := s.core.exec.ExecContext(ctx, statement, q.args...)
	if err != nil {
		return s.core.dialect.MapError(err, resource, object.GetID())
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return s.staleWriteError(ctx, resource, object.GetID())
	}
	return nil
}

func (s *Storage) deleteCurrent(ctx context.Context, resource string, object store.Object) error {
	q := &QueryState{dialect: s.core.dialect}
	conditions, err := s.identityConditions(q, object.GetID(), object.GetResourceVersion())
	if err != nil {
		return err
	}
	statement := "DELETE FROM " + s.core.dialect.Quote(resource) +
		" WHERE " + strings.Join(conditions, " AND ")
	result, err := s.core.exec.ExecContext(ctx, statement, q.args...)
	if err != nil {
		return s.core.dialect.MapError(err, resource, object.GetID())
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return s.staleWriteError(ctx, resource, object.GetID())
	}
	return nil
}

// identityConditions render the scope, identity and observed version of one
// conditional write.
func (s *Storage) identityConditions(q *QueryState, id string, expectedVersion int64) ([]string, error) {
	if id == "" {
		return nil, errors.NewBadRequest("empty id for a conditional write")
	}
	conditions := make([]string, 0, len(s.scopes)+2)
	for _, scope := range s.scopes {
		field := store.ScopeResourceToFieldName(scope.Resource)
		conditions = append(conditions, s.core.dialect.Quote(field)+" = "+q.Bind(scope.Name))
	}
	conditions = append(conditions, s.core.dialect.Quote("id")+" = "+q.Bind(id))
	if expectedVersion > 0 {
		conditions = append(conditions, s.core.dialect.Quote("resourceVersion")+" = "+q.Bind(expectedVersion))
	}
	return conditions, nil
}

// staleWriteError reports why a conditional write matched no row: the object is
// gone, or another writer changed its version first.
func (s *Storage) staleWriteError(ctx context.Context, resource, id string) error {
	q := &QueryState{dialect: s.core.dialect}
	where, err := s.whereClause(q, id, nil, nil)
	if err != nil {
		return err
	}
	statement := "SELECT " + s.core.dialect.Quote("resourceVersion") + " FROM " + s.core.dialect.Quote(resource) + where + " LIMIT 1"
	var version int64
	if err := s.core.exec.QueryRowContext(ctx, statement, q.args...).Scan(&version); err != nil {
		if stderrors.Is(err, sql.ErrNoRows) {
			return errors.NewNotFound(resource, id)
		}
		return s.core.dialect.MapError(err, resource, id)
	}
	return errors.NewConflict(resource, id, fmt.Errorf("object changed during the write"))
}

// Delete implements store.Store. It re-reads the object so the finalizer and
// propagation contract is decided from current state, then deletes under that
// observed version; a concurrent change repeats the decision.
func (s *Storage) Delete(ctx context.Context, into store.Object, options ...store.DeleteOption) error {
	opts := store.ApplyDeleteOptions(options)
	resource, err := store.GetResource(into)
	if err != nil {
		return err
	}
	id := into.GetID()
	if err := store.ValidateSelectorRequirements(opts.LabelRequirements, opts.FieldRequirements); err != nil {
		return errors.NewBadRequest(err.Error())
	}
	for {
		current := store.NewObject(into)
		if err := s.get(ctx, current, id, store.GetOptions{}); err != nil {
			return err
		}
		if err := store.ValidatePreconditions(current, opts.Preconditions); err != nil {
			return err
		}
		if err := store.ValidateDeleteRequirements(current, opts.LabelRequirements, opts.FieldRequirements); err != nil {
			return err
		}
		policy := store.DeletePropagationBackground
		if opts.PropagationPolicy != nil {
			policy = *opts.PropagationPolicy
		}
		if store.PrepareObjectForDelete(current, policy) {
			if err := s.deleteCurrent(ctx, resource, current); err != nil {
				if errors.IsConflict(err) {
					continue
				}
				return err
			}
			return store.CopyObject(current, into)
		}
		current.SetResourceVersion(current.GetResourceVersion() + 1)
		if err := s.saveCurrent(ctx, resource, current, current.GetResourceVersion()-1); err != nil {
			if errors.IsConflict(err) {
				continue
			}
			return err
		}
		return store.CopyObject(current, into)
	}
}

// DeleteBatch implements store.Store.
func (s *Storage) DeleteBatch(ctx context.Context, list store.ObjectList, options ...store.DeleteBatchOption) error {
	opts := store.ApplyDeleteBatchOptions(options)
	resource, err := store.GetResource(list)
	if err != nil {
		return err
	}
	q := &QueryState{dialect: s.core.dialect}
	where, err := s.whereClause(q, "", opts.LabelRequirements, opts.FieldRequirements)
	if err != nil {
		return err
	}
	statement := "DELETE FROM " + s.core.dialect.Quote(resource) + where
	if _, err := s.core.exec.ExecContext(ctx, statement, q.args...); err != nil {
		return s.core.dialect.MapError(err, resource, "")
	}
	return nil
}

// PatchBatch implements store.Store. One patch applies to every matching object
// in one statement: a merge patch and a JSON patch both reduce to column
// assignments, and a nested path edits the containing JSON document column.
func (s *Storage) PatchBatch(ctx context.Context, list store.ObjectList, patch store.PatchBatch, options ...store.PatchBatchOption) error {
	opts := store.ApplyPatchBatchOptions(options)
	resource, err := store.GetResource(list)
	if err != nil {
		return err
	}
	q := &QueryState{dialect: s.core.dialect}
	assignments, err := s.patchAssignments(q, list, patch, s.excludedBatchColumns())
	if err != nil {
		return err
	}
	assignments = append(assignments, s.core.dialect.Quote("resourceVersion")+" = "+s.core.dialect.Quote("resourceVersion")+" + 1")
	where, err := s.whereClause(q, "", opts.LabelRequirements, opts.FieldRequirements)
	if err != nil {
		return err
	}
	statement := "UPDATE " + s.core.dialect.Quote(resource) + " SET " + strings.Join(assignments, ", ") + where
	if _, err := s.core.exec.ExecContext(ctx, statement, q.args...); err != nil {
		return s.core.dialect.MapError(err, resource, "")
	}
	return nil
}

// excludedBatchColumns are the model columns a batch patch must not set. The
// identity, version and generation columns stay owned by the Store, a scope
// column is part of an object's identity, and status only changes through the
// status subresource. The list matches the exclusions the MongoDB adapter
// applies to a batch patch.
func (s *Storage) excludedBatchColumns() map[string]struct{} {
	excluded := map[string]struct{}{
		"id":                {},
		"uid":               {},
		"resourceVersion":   {},
		"generation":        {},
		"creationTimestamp": {},
		"status":            {},
	}
	for _, scope := range s.scopes {
		excluded[store.ScopeResourceToFieldName(scope.Resource)] = struct{}{}
	}
	return excluded
}

// Count implements store.Store.
func (s *Storage) Count(ctx context.Context, obj store.Object, options ...store.CountOption) (int, error) {
	opts := store.ApplyCountOptions(options)
	resource, err := store.GetResource(obj)
	if err != nil {
		return 0, err
	}
	q := &QueryState{dialect: s.core.dialect}
	where, err := s.whereClause(q, "", opts.LabelRequirements, opts.FieldRequirements)
	if err != nil {
		return 0, err
	}
	statement := "SELECT COUNT(*) FROM " + s.core.dialect.Quote(resource) + where
	var total int64
	if err := s.core.exec.QueryRowContext(ctx, statement, q.args...).Scan(&total); err != nil {
		return 0, s.core.dialect.MapError(err, resource, "")
	}
	return int(total), nil
}

// List implements store.Store. Cursor pagination needs a resumable token that
// this adapter does not produce, so it is rejected rather than degraded.
func (s *Storage) List(ctx context.Context, list store.ObjectList, options ...store.ListOption) error {
	opts := store.ApplyListOptions(options)
	if opts.Continue != nil {
		return errors.NewUnsupported("SQL store does not support cursor pagination")
	}
	resource, err := store.GetResource(list)
	if err != nil {
		return err
	}
	items, err := store.GetItemsPtr(list)
	if err != nil {
		return err
	}
	q := &QueryState{dialect: s.core.dialect}
	builder := &ConditionBuilder{}
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
	// The count statement binds only the condition values collected so far.
	conditionArgs := slices.Clone(q.args)
	var total int64
	countStatement := "SELECT COUNT(*) FROM " + s.core.dialect.Quote(resource) + where
	if err := s.core.exec.QueryRowContext(ctx, countStatement, conditionArgs...).Scan(&total); err != nil {
		return s.core.dialect.MapError(err, resource, "")
	}
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
	store.SetWindowListMetadata(list, int(total))
	list.SetResource(resource)
	return nil
}

// newEmptyIDStorageError reports a request that omitted the object identity.
func newEmptyIDStorageError(resource string) error {
	return errors.NewBadRequest(fmt.Sprintf("empty id for resource %s", resource))
}

// driverValue converts a decoded patch value into a driver value.
func driverValue(value any) any {
	return ToDriverValue(reflect.ValueOf(value))
}

// sortedKeys returns map keys in a stable order, so generated statements stay
// comparable and tests stay deterministic.
func sortedKeys(values map[string]any) []string {
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
