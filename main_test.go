package schema

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"

	"github.com/ory/dockertest"
)

// TestMain replaces the normal test runner for this package. It connects to
// Docker running on the local machine and launches testing database
// containers to which we then connect and store the connection in a package
// global variable
//
func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Can't run schema tests. Docker is not running: %s", err)
	}

	var wg sync.WaitGroup
	for name := range TestDBs {
		testDB := TestDBs[name]
		wg.Add(1)
		go func() {
			testDB.Init(pool)
			wg.Done()
		}()
	}
	wg.Wait()

	code := m.Run()

	// Purge all the containers we created
	// You can't defer this because os.Exit doesn't execute defers
	for _, info := range TestDBs {
		info.Cleanup(pool)
	}

	os.Exit(code)
}

func withEachDialect(t *testing.T, f func(t *testing.T, d Dialect)) {
	dialects := []Dialect{Postgres, NewSQLite()}
	for _, dialect := range dialects {
		t.Run(fmt.Sprintf("%T", dialect), func(t *testing.T) {
			f(t, dialect)
		})
	}
}

func withEachTestDB(t *testing.T, f func(t *testing.T, tdb *TestDB)) {
	for dbName, tdb := range TestDBs {
		t.Run(dbName, func(t *testing.T) {
			f(t, tdb)
		})
	}
}

func withTestDB(t *testing.T, name string, f func(t *testing.T, tdb *TestDB)) {
	tdb, exists := TestDBs[name]
	if !exists {
		t.Fatalf("Database '%s' doesn't exist. Add it to TestDBs", name)
	}
	f(t, tdb)
}

func connectDB(t *testing.T, name string) *sql.DB {
	info, exists := TestDBs[name]
	if !exists {
		t.Fatalf("Database '%s' doesn't exist.", name)
	}
	db, err := sql.Open(info.Driver, info.DSN())
	if err != nil {
		t.Fatalf("Failed to connect to %s: %s", name, err)
	}
	return db
}
