package schema

import (
	"fmt"
	"strings"
)

// SQLite is the dialect for SQLite databases
var SQLite = sqliteDialect{}

// SQLite is the SQLite dialect
type sqliteDialect struct{}

func (s sqliteDialect) LockSQL(tableName string) string {
	return ""
}

func (s sqliteDialect) UnlockSQL(tableName string) string {
	return ""
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
				)
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
