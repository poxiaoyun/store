package sql_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	commonerrors "xiaoshiai.cn/common/errors"
	testmysql "xiaoshiai.cn/common/testkit/mysql"
	testpostgresql "xiaoshiai.cn/common/testkit/postgresql"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/selector"
	storesql "xiaoshiai.cn/store/sql"
	"xiaoshiai.cn/store/sql/universal"
	"xiaoshiai.cn/store/storetest"
)

func mysqlDSN(t *testing.T) string {
	t.Helper()
	connection, err := url.Parse(testmysql.RequireURI(t))
	if err != nil {
		t.Fatal(err)
	}
	config := mysqldriver.NewConfig()
	config.User = connection.User.Username()
	config.Passwd, _ = connection.User.Password()
	config.Net, config.Addr = "tcp", connection.Host
	config.DBName = strings.TrimPrefix(connection.Path, "/")
	return config.FormatDSN()
}

func TestMySQLStoreConformance(t *testing.T) {
	runSQLStoreConformance(t, universal.DriverMySQL, mysqlDSN(t), false)
}

func TestPostgreSQLStoreConformance(t *testing.T) {
	runSQLStoreConformance(t, universal.DriverPostgres, testpostgresql.RequireURI(t), true)
}

// binaryRecord exercises native binary columns and nullable timestamps together.
type binaryRecord struct {
	Alias            json.RawMessage `json:"alias"`
	store.ObjectMeta `json:",inline"`
	Payload          []byte     `json:"payload"`
	Completed        *time.Time `json:"completed"`
}

func TestPostgreSQLBinaryRoundTrip(t *testing.T) {
	runBinaryRoundTrip(t, universal.DriverPostgres, testpostgresql.RequireURI(t))
}

func TestMySQLBinaryRoundTrip(t *testing.T) {
	runBinaryRoundTrip(t, universal.DriverMySQL, mysqlDSN(t))
}

func runBinaryRoundTrip(t *testing.T, driver, dsn string) {
	t.Helper()
	db, dialect, err := universal.Open(t.Context(), driver, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	schema := store.NewSchema()
	if err := schema.Register(&binaryRecord{}, store.ResourceSchema{}); err != nil {
		t.Fatal(err)
	}
	storage, err := storesql.NewStorage(t.Context(), schema, db, dialect)
	if err != nil {
		t.Fatal(err)
	}
	value := binaryRecord{Payload: []byte{0, 255, 34, 128}}
	if err := storage.Create(t.Context(), &value); err != nil {
		t.Fatal(err)
	}
	var saved binaryRecord
	if err := storage.Get(t.Context(), value.ID, &saved); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(saved.Payload, value.Payload) || saved.Completed != nil {
		t.Fatal(saved)
	}
	now := time.Now().
		UTC().
		Truncate(time.Microsecond)
	saved.Payload = []byte("updated")
	saved.Completed = &now
	if err := storage.Update(t.Context(), &saved); err != nil {
		t.Fatal(err)
	}
	if err := storage.Get(t.Context(), value.ID, &saved); err != nil {
		t.Fatal(err)
	}
	if string(saved.Payload) != "updated" || saved.Completed == nil || !saved.Completed.Equal(now) {
		t.Fatal(saved)
	}
	if err := storage.Patch(t.Context(), &saved, store.RawPatch(store.PatchTypeMergePatch, []byte(`{"payload":"AP8=","completed":null}`))); err != nil {
		t.Fatal(err)
	}
	if err := storage.Get(t.Context(), value.ID, &saved); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(saved.Payload, []byte{0, 255}) || saved.Completed != nil {
		t.Fatal(saved)
	}
}

func runSQLStoreConformance(t *testing.T, driver, dsn string, isPostgres bool) {
	t.Helper()
	// Cases rebuild the same resource with different column types. Each case
	// owns its pool so prepared statements cannot outlive that case's schema.
	open := func(t testing.TB) (*sql.DB, storesql.Dialect) {
		t.Helper()
		db, dialect, err := universal.Open(t.Context(), driver, dsn)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = db.Close() })
		return db, dialect
	}
	t.Run("canceled transaction rolls back and preserves cause", func(t *testing.T) {
		db, dialect := open(t)
		schema := store.NewSchema()
		if err := schema.Register(&binaryRecord{}, store.ResourceSchema{}); err != nil {
			t.Fatal(err)
		}
		storage, err := storesql.NewStorage(t.Context(), schema, db, dialect)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		value := binaryRecord{Payload: []byte("rollback")}
		err = storage.Transaction(ctx, func(ctx context.Context, tx store.Store) error {
			if err := tx.Create(ctx, &value); err != nil {
				return err
			}
			cancel()
			return nil
		})
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled transaction: %v", err)
		}
		if err := storage.Get(t.Context(), value.ID, &binaryRecord{}); !commonerrors.IsNotFound(err) {
			t.Fatalf("canceled transaction persisted a record: %v", err)
		}
	})
	if isPostgres {
		t.Run("identity checks target relation", func(t *testing.T) {
			db, dialect := open(t)
			for _, statement := range []string{
				"CREATE SCHEMA unrelated",
				"CREATE TABLE unrelated.storetests (id varchar(255) PRIMARY KEY)",
				"CREATE TABLE storetests (id varchar(255))",
			} {
				if _, err := db.ExecContext(t.Context(), statement); err != nil {
					t.Fatal(err)
				}
			}
			t.Cleanup(func() {
				_, _ = db.ExecContext(context.Background(), "DROP TABLE storetests")
				_, _ = db.ExecContext(context.Background(), "DROP SCHEMA unrelated CASCADE")
			})
			schema := store.NewSchema()
			if err := schema.Register(&storetest.Object{}, store.ResourceSchema{}); err != nil {
				t.Fatal(err)
			}
			if _, err := storesql.NewStorage(t.Context(), schema, db, dialect); err == nil || !strings.Contains(err.Error(), "primary key") {
				t.Fatalf("constructor borrowed another relation's primary key: %v", err)
			}
		})
	}
	for _, test := range []struct {
		name  string
		key   string
		valid bool
	}{
		{name: "missing", key: ""},
		{name: "composite", key: ", PRIMARY KEY (id, tenant)"},
		{name: "resource wide", key: ", PRIMARY KEY (id)", valid: true},
		{name: "covering", key: ", PRIMARY KEY (id) INCLUDE (tenant)", valid: true},
	} {
		t.Run("existing identity/"+test.name, func(t *testing.T) {
			if test.name == "covering" && !isPostgres {
				t.Skip("covering primary keys are PostgreSQL-specific")
			}
			db, dialect := open(t)
			// Set up an existing database layout independently of the Store schema planner.
			if _, err := db.ExecContext(t.Context(), "CREATE TABLE storetests (id varchar(255), tenant varchar(255)"+test.key+")"); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DROP TABLE storetests") })
			schema := store.NewSchema()
			if err := schema.Register(&storetest.Object{}, store.ResourceSchema{ScopeKeys: []string{"tenant"}}); err != nil {
				t.Fatal(err)
			}
			storage, err := storesql.NewStorage(t.Context(), schema, db, dialect)
			if !test.valid {
				if err == nil || !strings.Contains(err.Error(), "primary key") {
					t.Fatalf("existing identity constructor = %v, want primary key error", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := storage.Create(t.Context(), &storetest.Object{ObjectMeta: store.ObjectMeta{ID: "shared"}}); err != nil {
				t.Fatal(err)
			}
			child := storage.Scope(store.Scope{Resource: "tenants", Name: "acme"})
			if err := child.Create(t.Context(), &storetest.Object{ObjectMeta: store.ObjectMeta{ID: "shared"}}); !commonerrors.IsAlreadyExists(err) {
				t.Fatalf("existing table cross-scope duplicate = %v", err)
			}
		})
	}

	t.Run("explicit root descendant queries", func(t *testing.T) {
		db, dialect := open(t)
		schema := store.NewSchema()
		if err := schema.Register(&storetest.Object{}, store.ResourceSchema{ScopeKeys: []string{"tenant"}}); err != nil {
			t.Fatal(err)
		}
		storage, err := storesql.NewStorage(t.Context(), schema, db, dialect)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _, _ = db.ExecContext(context.Background(), "DROP TABLE storetests") })
		for _, name := range []string{"", "a", "b"} {
			var target store.Store = storage
			if name != "" {
				target = storage.Scope(store.Scope{Resource: "tenants", Name: name})
			}
			if err := target.Create(t.Context(), &storetest.Object{Value: "pending"}); err != nil {
				t.Fatal(err)
			}
		}
		root := &store.List[storetest.Object]{}
		if err := storage.List(t.Context(), root); err != nil {
			t.Fatal(err)
		}
		if len(root.Items) != 1 {
			t.Fatal("ordinary root read escaped scope", root)
		}
		count, err := storage.Count(t.Context(), &storetest.Object{}, store.WithSubScopes(), store.WithFieldRequirementsFromSet(map[string]string{"value": "pending"}))
		if err != nil {
			t.Fatal(err)
		}
		if count != 3 {
			t.Fatalf("descendant count=%d", count)
		}
		all := &store.List[storetest.Object]{}
		if err := storage.List(t.Context(), all, store.WithSubScopes(), store.WithSort("id+"), store.WithContinue("", 2)); err != nil {
			t.Fatal(err)
		}
		if len(all.Items) != 2 || all.Continue == "" {
			t.Fatal(all)
		}
		if err := storage.List(t.Context(), all, store.WithSubScopes(), store.WithSort("id+"), store.WithContinue(all.Continue, 2)); err != nil {
			t.Fatal(err)
		}
		if len(all.Items) != 1 || all.Continue != "" {
			t.Fatal(all)
		}
		if err := storage.Get(t.Context(), all.Items[0].ID, &storetest.Object{}); err != nil && !commonerrors.IsNotFound(err) {
			t.Fatal(err)
		}
	})
	capabilities := store.Capabilities{Finalizers: true, OptimisticLock: true}
	t.Run("nested unique index survives reconstruction", func(t *testing.T) {
		db, dialect := open(t)
		schema := store.NewSchema()
		if err := schema.Register(&storetest.Object{}, store.ResourceSchema{Indexes: []store.Index{
			{Name: "link_key", Fields: []string{"labels.href"}, Unique: true},
		}}); err != nil {
			t.Fatal(err)
		}
		storage, err := storesql.NewStorage(t.Context(), schema, db, dialect)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = db.ExecContext(context.Background(), "DROP TABLE storetests")
		})
		if err := storage.Create(t.Context(), &storetest.Object{ObjectMeta: store.ObjectMeta{Labels: map[string]string{"href": "shared"}}}); err != nil {
			t.Fatal(err)
		}
		reopened, err := storesql.NewStorage(t.Context(), schema, db, dialect)
		if err != nil {
			t.Fatalf("reconstruct store: %v", err)
		}
		if err := reopened.Create(t.Context(), &storetest.Object{ObjectMeta: store.ObjectMeta{Labels: map[string]string{"href": "shared"}}}); !commonerrors.IsAlreadyExists(err) {
			t.Fatalf("nested duplicate after reconstruction = %v", err)
		}
		// An externally changed expression must not be accepted merely because
		// the index name and uniqueness still match the registration.
		statements := []string{
			"DROP INDEX link_key",
			"CREATE UNIQUE INDEX link_key ON storetests ((labels #>> ARRAY['other']))",
		}
		if !isPostgres {
			statements = []string{
				"DROP INDEX link_key ON storetests",
				"ALTER TABLE storetests DROP COLUMN idx_labels_href",
				`ALTER TABLE storetests ADD COLUMN idx_labels_href varchar(255) GENERATED ALWAYS AS (JSON_UNQUOTE(JSON_EXTRACT(labels, '$."other"'))) STORED`,
				"CREATE UNIQUE INDEX link_key ON storetests (idx_labels_href)",
			}
		}
		for _, statement := range statements {
			if _, err := db.ExecContext(t.Context(), statement); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := storesql.NewStorage(t.Context(), schema, db, dialect); err == nil || !strings.Contains(err.Error(), "link_key") {
			t.Fatalf("changed unique expression accepted: %v", err)
		}
	})

	t.Run("native patch round trips", func(t *testing.T) {
		db, dialect := open(t)
		schema := store.NewSchema()
		if err := schema.Register(&storetest.Object{}, store.ResourceSchema{}); err != nil {
			t.Fatal(err)
		}
		measured := &measuredExecutor{Database: db}
		storage, err := storesql.NewStorage(t.Context(), schema, measured, dialect)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_, _ = db.ExecContext(context.Background(), "DROP TABLE "+dialect.Quote("storetests"))
		})
		object := &storetest.Object{Value: "before"}
		if err := storage.Create(t.Context(), object); err != nil {
			t.Fatal(err)
		}
		measured.queries.Store(0)
		measured.writes.Store(0)
		if err := storage.Patch(t.Context(), object, store.MapMergePatch{"value": "after"}); err != nil {
			t.Fatal(err)
		}
		if measured.queries.Load() != 0 || measured.writes.Load() != 1 {
			t.Fatalf("ordinary Patch round trips: queries=%d writes=%d, want 0 and 1", measured.queries.Load(), measured.writes.Load())
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		if object.Value != "after" || object.Generation != 2 {
			t.Fatalf("stored result = %#v", object)
		}
		for _, patch := range []store.MapMergePatch{
			{"value": 42},
			{"value": "rejected", "rank": "invalid"},
			{"value": "rejected", "finalizers": "invalid"},
		} {
			version := object.ResourceVersion
			if err := storage.Patch(t.Context(), object, patch); commonerrors.ReasonForError(err) != commonerrors.StatusReasonBadRequest {
				t.Fatalf("invalid field type = %v, want BadRequest", err)
			}
			if err := storage.Get(t.Context(), object.ID, object); err != nil {
				t.Fatal(err)
			}
			if object.Value != "after" || object.Generation != 2 || object.ResourceVersion != version {
				t.Fatalf("invalid field type changed stored object: %#v", object)
			}
		}
		measured.queries.Store(0)
		list := &store.List[storetest.Object]{}
		if err := storage.List(t.Context(), list, store.WithLimit(1)); err != nil {
			t.Fatal(err)
		}
		if measured.queries.Load() != 1 {
			t.Fatalf("List round trips = %d, want one query without implicit Count", measured.queries.Load())
		}
		for range 12 {
			if err := storage.Create(t.Context(), &storetest.Object{Value: "batch"}); err != nil {
				t.Fatal(err)
			}
		}
		measured.queries.Store(0)
		measured.writes.Store(0)
		if err := storage.PatchBatch(t.Context(), &store.List[storetest.Object]{}, store.MapMergePatchBacth{"rank": 5}); err != nil {
			t.Fatal(err)
		}
		if measured.queries.Load() != 0 || measured.writes.Load() != 1 {
			t.Fatalf("batch Patch round trips: queries=%d writes=%d, want 0 and 1", measured.queries.Load(), measured.writes.Load())
		}
		if err := storage.List(t.Context(), list); err != nil {
			t.Fatal(err)
		}
		if len(list.Items) != 13 {
			t.Fatalf("batch size = %d", len(list.Items))
		}
		for _, item := range list.Items {
			if item.Rank != 5 {
				t.Fatalf("batch skipped %#v", item)
			}
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		if object.Generation != 2 {
			t.Fatalf("untracked rank changed Generation to %d", object.Generation)
		}
		if err := storage.Transaction(t.Context(), func(ctx context.Context, tx store.Store) error {
			object.Value = "After"
			if err := tx.Update(ctx, object); err != nil {
				return err
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		if object.Value != "After" || object.Generation != 3 {
			t.Fatalf("case-sensitive generation = %#v", object)
		}
		for _, patch := range []store.MapMergePatch{
			{"labels": map[string]any{"keep": "yes", "remove": "old"}},
			{"labels": map[string]any{}},
			{"labels": map[string]any{"remove": nil, "new": "added"}},
		} {
			if err := storage.Patch(t.Context(), object, patch); err != nil {
				t.Fatal(err)
			}
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		if object.Labels["keep"] != "yes" || object.Labels["new"] != "added" || len(object.Labels) != 2 {
			t.Fatalf("flat merge semantics = %v", object.Labels)
		}
		version := object.ResourceVersion
		if err := storage.Patch(t.Context(), object, store.MapMergePatch{"value": "rejected", "labels": map[string]any{"nested": map[string]any{"key": "value"}}}); !commonerrors.IsUnsupported(err) {
			t.Fatalf("nested merge = %v, want Unsupported", err)
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		if object.Value != "After" || object.ResourceVersion != version {
			t.Fatalf("unsupported patch had effects: %#v", object)
		}
		if err := storage.Patch(t.Context(), object, store.MapMergePatch{"finalizers": []string{"retain"}}); err != nil {
			t.Fatal(err)
		}
		if err := storage.Delete(t.Context(), object); err != nil {
			t.Fatal(err)
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		version = object.ResourceVersion
		if err := storage.PatchBatch(t.Context(), &store.List[storetest.Object]{}, store.MapMergePatchBacth{"finalizers": nil}, store.WithFieldRequirements(selector.RequirementEqual("id", object.ID))); !commonerrors.IsUnsupported(err) {
			t.Fatalf("filtered finalizer completion = %v", err)
		}
		if err := storage.DeleteBatch(t.Context(), &store.List[storetest.Object]{}, store.WithFieldRequirements(selector.RequirementEqual("resourceVersion", version))); !commonerrors.IsUnsupported(err) {
			t.Fatalf("version-selected batch deletion = %v", err)
		}
		if err := storage.Get(t.Context(), object.ID, object); err != nil {
			t.Fatal(err)
		}
		if object.ResourceVersion != version || len(object.Finalizers) != 1 || object.DeletionTimestamp == nil {
			t.Fatalf("unsupported lifecycle operation had effects: %#v", object)
		}
	})
	storetest.Run(t, storetest.Fixture{
		ResourceWideIDs:           true,
		MergePatchOnly:            true,
		BatchFinalizerRemovalOnly: true,
		Capabilities:              capabilities,
		Continue:                  true,
		LabelSelector:             true,
		FieldSelector:             true,
		Search:                    true,
		Sort:                      true,
		Projection:                true,
		DeleteBatch:               true,
		PatchBatch:                true,
		Transaction:               true,
		SecondaryIndexes:          true,
		UniqueIndexes:             true,

		New: func(t testing.TB, schema *store.Schema) (store.Store, error) {
			db, dialect := open(t)
			storage, err := storesql.NewStorage(t.Context(), schema, db, dialect)
			if err != nil {
				return nil, err
			}
			t.Cleanup(func() {
				statement := "DROP TABLE IF EXISTS " + dialect.Quote("storetests")
				if _, err := db.ExecContext(context.Background(), statement); err != nil {
					t.Errorf("drop conformance table: %v", err)
				}
			})
			return storage, nil
		},
	})
}

// measuredExecutor observes actual database round trips; it never substitutes results.
type measuredExecutor struct {
	storesql.Database
	queries atomic.Int64
	writes  atomic.Int64
}

func (e *measuredExecutor) QueryContext(ctx context.Context, statement string, args ...any) (*sql.Rows, error) {
	e.queries.Add(1)
	return e.Database.QueryContext(ctx, statement, args...)
}
func (e *measuredExecutor) QueryRowContext(ctx context.Context, statement string, args ...any) *sql.Row {
	e.queries.Add(1)
	return e.Database.QueryRowContext(ctx, statement, args...)
}
func (e *measuredExecutor) ExecContext(ctx context.Context, statement string, args ...any) (sql.Result, error) {
	e.writes.Add(1)
	return e.Database.ExecContext(ctx, statement, args...)
}
