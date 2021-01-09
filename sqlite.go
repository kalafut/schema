package schema

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mattn/go-sqlite3"
)

// SQLite is the dialect for SQLite databases
var SQLite = &sqliteDialect{}

var _ Locker = SQLite

// SQLite is the SQLite dialect
type sqliteDialect struct {
	lockCode int64
	lock     sync.Mutex
}

func (s sqliteDialect) LockSQL(_ string) string {
	return ""
}

func (s sqliteDialect) UnlockSQL(_ string) string {
	return ""
}

func (s *sqliteDialect) Lock(db *sql.DB) error {
	s.lock.Lock()

	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS migration_lock(
			id INTEGER PRIMARY KEY,
			expiration DATETIME NOT NULL)`)
	if err != nil {
		return err
	}

	for {
		_, err := db.Exec(`DELETE FROM migration_lock WHERE datetime(expiration) < datetime('now')`)
		if err != nil {
			return err
		}

		_, err = db.Exec(`INSERT INTO migration_lock(id, expiration) VALUES(42, ?)`, time.Now().Add(1*time.Minute))

		switch err := err.(type) {
		case sqlite3.Error:
			if err.Code == sqlite3.ErrConstraint {
				time.Sleep(time.Second)
			} else {
				return err
			}
		default:
			return nil
		}
	}
}

func (s *sqliteDialect) Lock2(db *sql.DB) error {
	s.lock.Lock()

	result, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS migration_lock (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			expiration DATETIME NOT NULL
		);
		INSERT INTO migration_lock(expiration) VALUES(?)`, time.Now().Add(1*time.Minute))
	if err != nil {
		return err
	}
	s.lockCode, err = result.LastInsertId()
	if err != nil {
		return err
	}

	var min int64
	for {
		_, err := db.Exec(`DELETE FROM migration_lock WHERE datetime(expiration) < datetime('now')`)
		if err != nil {
			return err
		}

		row := db.QueryRow(`SELECT min(id) FROM migration_lock;`)

		if err := row.Scan(&min); err != nil {
			return err
		}

		if min == s.lockCode {
			return nil
		}

		time.Sleep(time.Second)
	}
}

func (s *sqliteDialect) Unlock(db *sql.DB) error {
	_, err := db.Exec(`DELETE FROM migration_lock`, s.lockCode)
	s.lock.Unlock()
	return err
}

// CreateSQL takes the name of the migration tracking table and
// returns the SQL statement needed to create it
func (s *sqliteDialect) CreateSQL(tableName string) string {
	return fmt.Sprintf(`
				CREATE TABLE IF NOT EXISTS %s (
					id TEXT NOT NULL,
					checksum TEXT NOT NULL DEFAULT '',
					execution_time_in_millis INTEGER NOT NULL DEFAULT 0,
					applied_at DATETIME
				);
			`, tableName)
}

// InsertSQL takes the name of the migration tracking table and
// returns the SQL statement needed to insert a migration into it
func (s sqliteDialect) InsertSQL(tableName string) string {
	return fmt.Sprintf(`
				INSERT INTO %s
				( id, checksum, execution_time_in_millis, applied_at )
				VALUES
				( ?, ?, ?, ? )
				`,
		tableName,
	)
}

// SelectSQL takes the name of the migration tracking table and
// returns trhe SQL statement to retrieve all records from it
//
func (s sqliteDialect) SelectSQL(tableName string) string {
	return fmt.Sprintf(`
		SELECT id, checksum, execution_time_in_millis, applied_at
		FROM %s
		ORDER BY id ASC
	`, tableName)
}

// QuotedTableName returns the string value of the name of the migration
// tracking table after it has been quoted for Postgres
//
func (s sqliteDialect) QuotedTableName(_, tableName string) string {
	return s.quotedIdent(tableName)
}

// quotedIdent wraps the supplied string in the Postgres identifier
// quote character
func (s sqliteDialect) quotedIdent(ident string) string {
	return `"` + strings.ReplaceAll(ident, `"`, "") + `"`
}
