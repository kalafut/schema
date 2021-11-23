package schema

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestApplyWithNilDBProvidesHelpfulError(t *testing.T) {
	m := NewMigrator()
	err := m.Apply(nil, []*Migration{
		{
			ID:     "2019-01-01 Test",
			Script: "CREATE TABLE fake_table (id INTEGER)",
		},
	})
	if !errors.Is(err, ErrNilDB) {
		t.Errorf("Expected %v, got %v", ErrNilDB, err)
	}
}

func TestApplyInLexicalOrder(t *testing.T) {
	db := connectDB(t, "postgres11")
	tableName := "lexical_order_migrations"
	migrator := NewMigrator(WithDialect(Postgres), WithTableName(tableName))
	outOfOrderMigrations := []*Migration{
		{
			ID:     "2019-01-01 999 Should Run Last",
			Script: "CREATE TABLE last_table (id INTEGER NOT NULL);",
		},
		{
			ID:     "2019-01-01 001 Should Run First",
			Script: "CREATE TABLE first_table (id INTEGER NOT NULL);",
		},
	}
	err := migrator.Apply(db, outOfOrderMigrations)
	if err != nil {
		t.Error(err)
	}

	applied, err := migrator.GetAppliedMigrations(db)
	if err != nil {
		t.Error(err)
	}
	if len(applied) != 2 {
		t.Errorf("Expected exactly 2 applied migrations. Got %d", len(applied))
	}
	firstMigration := applied["2019-01-01 001 Should Run First"]
	if firstMigration == nil {
		t.Error("Missing first migration")
	} else if firstMigration.Checksum == "" {
		t.Error("Expected checksum to get populated when migration ran")
	}

	secondMigration := applied["2019-01-01 999 Should Run Last"]
	if secondMigration == nil {
		t.Error("Missing second migration")
	} else if secondMigration.Checksum == "" {
		t.Error("Expected checksum to get populated when migration ran")
	}

	if firstMigration.AppliedAt.After(secondMigration.AppliedAt) {
		t.Errorf("Expected migrations to run in lexical order, but first migration ran at %s and second one ran at %s", firstMigration.AppliedAt, secondMigration.AppliedAt)
	}
}

func TestFailedMigration(t *testing.T) {
	db := connectDB(t, "postgres11")
	tableName := time.Now().Format(time.RFC3339Nano)
	migrator := NewMigrator(WithTableName(tableName))
	migrations := []*Migration{
		{
			ID:     "2019-01-01 Bad Migration",
			Script: "CREATE TIBBLE bad_table_name (id INTEGER NOT NULL PRIMARY KEY)",
		},
	}
	err := migrator.Apply(db, migrations)
	if err == nil || !strings.Contains(err.Error(), "TIBBLE") {
		t.Errorf("Expected explanatory error from failed migration. Got %v", err)
	}
	rows, err := db.Query("SELECT * FROM " + migrator.QuotedTableName())
	if err != nil {
		t.Error(err)
	}
	if rows.Next() {
		t.Error("Record was inserted in tracking table even though the migration failed")
	}
	_ = rows.Close()
}

func TestSimultaneousApply(t *testing.T) {
	concurrency := 4
	dataTable := fmt.Sprintf("data%d", rand.Int())
	migrationsTable := fmt.Sprintf("Migrations %s", time.Now().Format(time.RFC3339Nano))
	sharedMigrations := []*Migration{
		{
			ID:     "2020-05-01 Sleep",
			Script: "SELECT pg_sleep(1)",
		},
		{
			ID: "2020-05-02 Create Data Table",
			Script: fmt.Sprintf(`CREATE TABLE %s (
				id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
				created_at TIMESTAMP WITH TIME ZONE NOT NULL
			)`, dataTable),
		},
		{
			ID:     "2020-05-03 Add Initial Record",
			Script: fmt.Sprintf(`INSERT INTO %s (created_at) VALUES (NOW())`, dataTable),
		},
	}

	var wg sync.WaitGroup
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(i int) {
			db := connectDB(t, "postgres11")
			migrator := NewMigrator(WithTableName(migrationsTable))
			err := migrator.Apply(db, sharedMigrations)
			if err != nil {
				t.Error(err)
			}
			_, err = db.Exec(fmt.Sprintf("INSERT INTO %s (created_at) VALUES (NOW())", dataTable))
			if err != nil {
				t.Error(err)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	// We expect concurrency + 1 rows in the data table
	// (1 from the migration, and one each for the
	// goroutines which ran Apply and then did an
	// insert afterwards)
	db := connectDB(t, "postgres11")
	count := 0
	row := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", dataTable))
	err := row.Scan(&count)
	if err != nil {
		t.Error(err)
	}
	if count != concurrency+1 {
		t.Errorf("Expected to get %d rows in %s table. Instead got %d", concurrency+1, dataTable, count)
	}
}

func TestBeginTransactionFailure(t *testing.T) {
	m := makeTestMigrator()
	bt := BadTransactor{}

	m.transaction(bt, func(q Queryer) error {
		return nil
	})
	if !errors.Is(m.err, ErrBeginFailed) {
		t.Errorf("Expected ErrBeginFailed, got %v", m.err)
	}
}
func TestCreateMigrationsTable(t *testing.T) {
	db := connectDB(t, "postgres11")
	migrator := makeTestMigrator()
	migrator.createMigrationsTable(db)
	if migrator.err != nil {
		t.Errorf("Error occurred when creating migrations table: %s", migrator.err)
	}

	// Test that we can re-run it safely
	migrator.createMigrationsTable(db)
	if migrator.err != nil {
		t.Errorf("Calling createMigrationsTable a second time failed: %s", migrator.err)
	}
}

func TestCreateMigrationsTableFailure(t *testing.T) {
	m := makeTestMigrator()
	bt := BadTransactor{}
	m.err = ErrPriorFailure
	m.createMigrationsTable(bt)
	if m.err != ErrPriorFailure {
		t.Errorf("Expected error %v. Got %v.", ErrPriorFailure, m.err)
	}
}

func TestLockFailure(t *testing.T) {
	bc := BadConnection{}
	m := makeTestMigrator()
	m.lock(bc)
	expectedContents := "FAIL: SELECT pg_advisory_lock"
	if m.err == nil || !strings.Contains(m.err.Error(), expectedContents) {
		t.Errorf("Expected error msg with '%s'. Got '%s'", expectedContents, m.err)
	}

	m.err = ErrPriorFailure
	m.lock(bc)
	if m.err != ErrPriorFailure {
		t.Errorf("Expected error %v. Got %v", ErrPriorFailure, m.err)
	}
}

func TestUnlockFailure(t *testing.T) {
	bc := BadConnection{}
	m := makeTestMigrator()
	m.unlock(bc)
	expectedContents := "FAIL: SELECT pg_advisory_unlock"
	if m.err == nil || !strings.Contains(m.err.Error(), expectedContents) {
		t.Errorf("Expected error msg with '%s'. Got '%v'", expectedContents, m.err)
	}

	m.err = ErrPriorFailure
	m.unlock(bc)
	if m.err != ErrPriorFailure {
		t.Errorf("Expected error %v. Got %v.", ErrPriorFailure, m.err)
	}
}

func TestRunFailure(t *testing.T) {
	bc := BadConnection{}
	m := makeTestMigrator()
	m.run(bc, makeValidUnorderedMigrations())
	expectedContents := "FAIL: SELECT id, checksum"
	if m.err == nil || !strings.Contains(m.err.Error(), expectedContents) {
		t.Errorf("Expected error msg with '%s'. Got '%v'.", expectedContents, m.err)
	}

	m.err = ErrPriorFailure
	m.run(bc, makeValidUnorderedMigrations())
	if m.err != ErrPriorFailure {
		t.Errorf("Expected error %v. Got %v.", ErrPriorFailure, m.err)
	}

	m.err = nil
	m.run(nil, makeValidUnorderedMigrations())
	if m.err != ErrNilDB {
		t.Errorf("Expected error '%s'. Got '%v'.", expectedContents, m.err)
	}
}

func TestMigrationWithPriorError(t *testing.T) {
	bc := BadConnection{}
	m := makeTestMigrator()
	m.err = ErrPriorFailure
	m.transaction(bc, func(q Queryer) error {
		return nil
	})
	if m.err != ErrPriorFailure {
		t.Errorf("Expected error %v. Got %v", ErrPriorFailure, m.err)
	}
}

func TestComputeMigrationPlanFailure(t *testing.T) {
	bq := BadQueryer{}
	m := makeTestMigrator()
	_, err := m.computeMigrationPlan(bq, []*Migration{})
	expectedContents := "FAIL: SELECT id, checksum, execution_time_in_millis, applied_at"
	if err == nil || !strings.Contains(err.Error(), expectedContents) {
		t.Errorf("Expected error msg with '%s'. Got '%v'.", expectedContents, err)
	}
}

func TestMigrationRecoversFromPanics(t *testing.T) {
	db := connectDB(t, "postgres11")
	migrator := makeTestMigrator()
	migrator.transaction(db, func(tx Queryer) error {
		panic(errors.New("Panic Error"))
	})
	if migrator.err == nil {
		t.Error("Expected error to be set after panic. Got nil")
	} else if migrator.err.Error() != "Panic Error" {
		t.Errorf("Expected panic to be converted to error=Panic Error. Got %v", migrator.err)
	}

	migrator.err = nil
	migrator.transaction(db, func(tx Queryer) error {
		panic("Panic String")
	})

	if migrator.err == nil {
		t.Error("Expected error to be set after panic. Got nil")
	} else if migrator.err.Error() != "Panic String" {
		t.Errorf("Expected panic to be converted to error=Panic String. Got %v", migrator.err)
	}
}
func TestNilTransaction(t *testing.T) {
	migrator := makeTestMigrator()
	migrator.transaction(nil, func(q Queryer) error {
		return nil
	})
	if !errors.Is(migrator.err, ErrNilDB) {
		t.Errorf("Expected ErrNilDB. Got %v", migrator.err)
	}
}

// makeTestMigrator is a utility function which produces a migrator with an
// isolated environment (isolated due to a unique name for the migration
// tracking table).
func makeTestMigrator(options ...Option) Migrator {
	tableName := time.Now().Format(time.RFC3339Nano)
	options = append(options, WithTableName(tableName))
	return NewMigrator(options...)
}

func makeValidUnorderedMigrations() []*Migration {
	return []*Migration{
		{
			ID: "2021-01-01 002",
			Script: `CREATE TABLE data_table (
				id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
				created_at TIMESTAMP WITH TIME ZONE NOT NULL
			)`,
		},
		{
			ID:     "2021-01-01 001",
			Script: "CREATE TABLE first_table (created_at TIMESTAMP WITH TIME ZONE NOT NULL)",
		},
		{
			ID:     "2021-01-01 003",
			Script: `INSERT INTO data_table (created_at) VALUES (NOW())`,
		},
	}
}
