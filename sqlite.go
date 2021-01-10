package schema

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/mattn/go-sqlite3"
)

// SQLite is the dialect for SQLite databases
var SQLite = &sqliteDialect{}
var ErrSQLiteLockTimeout = errors.New("sqlite: timeout requesting lock")

var _ Locker = SQLite

const lockMagicNum = 794774819
const defaultSQLiteLockTable = "schema_lock"

// SQLite is the SQLite dialect
type sqliteDialect struct {
	LockDuration time.Duration
	LockTable    string

	code int64
}

func (s sqliteDialect) LockSQL(_ string) string {
	return ""
}

func (s sqliteDialect) UnlockSQL(_ string) string {
	return ""
}

func (s *sqliteDialect) Lock(db *sql.DB) error {
	_, err := db.Exec(s.addTable(`
		CREATE TABLE IF NOT EXISTS {table} (
			id INTEGER PRIMARY KEY,
			code INTEGER,
			expiration DATETIME NOT NULL)`))
	if err != nil {
		return err
	}

	timeoutDuration := s.LockDuration
	if timeoutDuration == 0 {
		const defaultDuration = 30 * time.Second
		timeoutDuration = defaultDuration
	}
	timeout := time.Now().Add(timeoutDuration)

	for time.Now().Before(timeout) {
		if _, err := db.Exec(s.addTable(`DELETE FROM {table} WHERE datetime(expiration) < datetime('now')`)); err != nil {
			return err
		}

		code := time.Now().UnixNano()
		_, err = db.Exec(
			s.addTable(`INSERT INTO {table} (id, code, expiration) VALUES(?, ?, ?)`),
			lockMagicNum, code, time.Now().Add(timeoutDuration))

		switch err := err.(type) {
		case sqlite3.Error:
			if err.Code == sqlite3.ErrConstraint {
				time.Sleep(time.Second)
			} else {
				return err
			}
		default:
			s.code = code
			return nil
		}
	}

	return ErrSQLiteLockTimeout
}

func (s sqliteDialect) Unlock(db *sql.DB) error {
	// Delete only the lock we created by checking 'code'. This guards against the
	// edge case where another process has deleted our expired lock and grabbed
	// their own just before we process Unlock().
	_, err := db.Exec(s.addTable(`DELETE FROM {table} WHERE id=? AND code=?;`), lockMagicNum, s.code)

	return err
}

// CreateSQL takes the name of the migration tracking table and
// returns the SQL statement needed to create it
func (s sqliteDialect) CreateSQL(tableName string) string {
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

func (s sqliteDialect) addTable(sql string) string {
	table := s.LockTable
	if table == "" {
		table = defaultSQLiteLockTable
	}

	return strings.Replace(sql, "{table}", table, 1)
}
