package universal_test

import (
	"testing"

	"xiaoshiai.cn/common/errors"
	"xiaoshiai.cn/store/sql/universal"
)

func TestOpenRejectsUnknownDriver(t *testing.T) {
	for _, driver := range []string{"", "unknown"} {
		db, dialect, err := universal.Open(t.Context(), driver, "")
		if !errors.IsUnsupported(err) {
			t.Fatalf("Open(%q) = %v, want Unsupported", driver, err)
		}
		if db != nil || dialect != nil {
			t.Fatalf("Open(%q) returned a database or dialect on failure", driver)
		}
	}
}

func TestOpenRejectsInvalidConnectionString(t *testing.T) {
	for _, driver := range []string{universal.DriverMySQL, universal.DriverPostgres} {
		db, dialect, err := universal.Open(t.Context(), driver, "%")
		if err == nil {
			t.Fatalf("Open(%q) accepted invalid connection string", driver)
		}
		if db != nil || dialect != nil {
			t.Fatalf("Open(%q) returned a database or dialect on failure", driver)
		}
	}
}
