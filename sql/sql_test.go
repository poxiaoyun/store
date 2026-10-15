package sql

import (
	"context"
	"net/url"
	"strings"
	"testing"

	commonerrors "xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store"
	"xiaoshiai.cn/store/storetest"
	testmysql "xiaoshiai.cn/common/testkit/mysql"
	testpostgresql "xiaoshiai.cn/common/testkit/postgresql"
)

func TestSQLStoreRejectsCursorPagination(t *testing.T) {
	storage := &Storage{}
	err := storage.List(
		t.Context(),
		&store.List[store.Unstructured]{Resource: "testobjects"},
		store.WithContinue("", 10),
	)
	if !commonerrors.IsUnsupported(err) {
		t.Fatalf("List() error = %v, want Unsupported", err)
	}
}

func TestMySQLStoreConformance(t *testing.T) {
	RunSQLStoreConformance(t, DBDriverMySQL, testmysql.RequireURI(t))
}

func TestPostgreSQLStoreConformance(t *testing.T) {
	RunSQLStoreConformance(t, DBDriverPostgres, testpostgresql.RequireURI(t))
}

// RunSQLStoreConformance runs the shared suite against one SQL driver.
func RunSQLStoreConformance(t *testing.T, driver, uri string) {
	t.Helper()
	connection, err := url.Parse(uri)
	if err != nil {
		t.Fatalf("parse database URI: %v", err)
	}
	password, _ := connection.User.Password()
	options := &Options{
		Addr:     connection.Host,
		Driver:   driver,
		Username: connection.User.Username(),
		Password: password,
		Database: strings.TrimPrefix(connection.Path, "/"),
	}
	capabilities := (&Storage{}).Capabilities()
	storetest.Run(t, storetest.Fixture{
		Capabilities: capabilities,
		New: func(t testing.TB, schema *store.Schema) (store.Store, error) {
			storage, err := NewStorage(t.Context(), schema, options)
			if err != nil {
				return nil, err
			}
			t.Cleanup(func() {
				statement := "DROP TABLE IF EXISTS " + storage.core.dialect.Quote("storetests")
				if _, err := storage.core.exec.ExecContext(context.Background(), statement); err != nil {
					t.Errorf("drop conformance table: %v", err)
				}
			})
			return storage, nil
		},
	})
}
