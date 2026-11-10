package delegation

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
	"io"
	"os"
	"strings"
)

// JSON is accepted only at this upgrade boundary. Native tables, the import
// marker, removal of old tables and the schema version commit together.
func migrateDelegationSQLite(ctx context.Context, db *sql.DB, legacyPath string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var userVersion int
	if err = tx.QueryRowContext(ctx, `PRAGMA user_version`).Scan(&userVersion); err != nil {
		return err
	}
	if userVersion < 0 || userVersion > delegationSQLiteStoreVersion {
		return fmt.Errorf("unsupported delegation user_version %d", userVersion)
	}
	// Acquire the writer before inspecting the version so two openers cannot
	// both plan the same upgrade from stale read snapshots.
	if _, err = tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL)`); err != nil {
		return err
	}
	var version string
	err = tx.QueryRowContext(ctx, `SELECT value FROM meta WHERE key='schema_version'`).Scan(&version)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if version == "2" {
		if userVersion != delegationSQLiteStoreVersion {
			return fmt.Errorf("inconsistent delegation schema versions: meta=%s user=%d", version, userVersion)
		}
		return sqlitestate.CommitSchema(tx, db)
	}
	if userVersion == delegationSQLiteStoreVersion {
		return fmt.Errorf("delegation schema marker is missing or inconsistent")
	}
	if version != "" && version != "1" {
		return fmt.Errorf("unsupported delegation schema version %q", version)
	}
	var hasRecords bool
	if err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='records')`).Scan(&hasRecords); err != nil {
		return err
	}
	imported := false
	if hasRecords {
		for _, table := range delegationLegacyTables {
			if _, err = tx.ExecContext(ctx, `ALTER TABLE `+table+` RENAME TO legacy_`+table); err != nil {
				return err
			}
		}
	}
	for _, statement := range delegationNativeSchema[1:] { // meta exists above
		if _, err = tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	writer, err := prepareDelegationImport(ctx, tx)
	if err != nil {
		return err
	}
	defer writer.Close()
	if hasRecords {
		for _, table := range delegationLegacyTables {
			if err = migrateDelegationBlobTable(ctx, tx, writer, table); err != nil {
				return err
			}
			if _, err = tx.ExecContext(ctx, `DROP TABLE legacy_`+table); err != nil {
				return err
			}
		}
	} else if legacyPath != "" {
		if _, statErr := os.Stat(legacyPath); statErr == nil {
			imported = true
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return statErr
		}
		if err = importDelegationJSON(ctx, writer, legacyPath); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES('schema_version','2'),('legacy_import','complete') ON CONFLICT(key) DO UPDATE SET value=excluded.value`); err != nil {
		return err
	}
	if imported {
		if _, err = tx.ExecContext(ctx, `INSERT INTO meta(key,value) VALUES('legacy_source',?)`, legacyPath); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `PRAGMA user_version=2`); err != nil {
		return err
	}
	if err = sqlitestate.CommitSchema(tx, db); err != nil {
		return err
	}
	if imported {
		// Commit is the authority switch. Even a crash or archive failure
		// here cannot make a subsequent opener import the source again.
		archive := legacyPath + ".migrated"
		if _, err = os.Lstat(archive); err == nil {
			return fmt.Errorf("delegation migration committed; archive already exists: %s", archive)
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("delegation migration committed; inspect archive: %w", err)
		}
		if err = os.Rename(legacyPath, archive); err != nil {
			return fmt.Errorf("delegation migration committed; archive legacy input: %w", err)
		}
	}
	return nil
}

var delegationLegacyTables = []string{"records", "routes", "remote_threads", "executions", "outbox", "inbox_cursors", "inbox_backoffs"}

func migrateDelegationBlobTable(ctx context.Context, tx *sql.Tx, writer *delegationImportStatements, table string) error {
	rows, err := tx.QueryContext(ctx, `SELECT id,json FROM legacy_`+table+` ORDER BY id`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var raw []byte
		if err = rows.Scan(&id, &raw); err != nil {
			return err
		}
		if err = importDelegationRow(ctx, writer, table, id, raw); err != nil {
			return fmt.Errorf("migrate delegation %s %q: %w", table, id, err)
		}
	}
	return rows.Err()
}

func importDelegationJSON(ctx context.Context, writer *delegationImportStatements, path string) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	if err = delegationJSONToken(d, json.Delim('{')); err != nil {
		return err
	}
	seen := map[string]bool{}
	for d.More() {
		keyToken, err := d.Token()
		if err != nil {
			return err
		}
		key, ok := keyToken.(string)
		if !ok || seen[key] {
			return fmt.Errorf("invalid or duplicate delegation field %q", key)
		}
		seen[key] = true
		if key == "schema_version" {
			var version int
			if err = d.Decode(&version); err != nil {
				return err
			}
			if version < 0 || version > 1 {
				return fmt.Errorf("unsupported legacy delegation schema %d", version)
			}
			continue
		}
		known := false
		for _, table := range delegationLegacyTables {
			known = known || key == table
		}
		if !known {
			return fmt.Errorf("unknown legacy delegation field %q", key)
		}
		open, close := json.Delim('{'), json.Delim('}')
		if key == "records" {
			open, close = '[', ']'
		}
		token, err := d.Token()
		if err != nil {
			return err
		}
		if token == nil { // old nil arrays and maps
			continue
		}
		if token != open {
			return fmt.Errorf("invalid legacy delegation %s container", key)
		}
		for d.More() {
			id := ""
			if key != "records" {
				token, err := d.Token()
				if err != nil {
					return err
				}
				id, ok = token.(string)
				if !ok || strings.TrimSpace(id) == "" {
					return fmt.Errorf("invalid legacy delegation %s key", key)
				}
			}
			var raw json.RawMessage
			if err = d.Decode(&raw); err != nil {
				return err
			}
			if err = importDelegationRow(ctx, writer, key, id, raw); err != nil {
				return err
			}
		}
		if err = delegationJSONToken(d, close); err != nil {
			return err
		}
	}
	if err = delegationJSONToken(d, json.Delim('}')); err != nil {
		return err
	}
	var extra any
	if err = d.Decode(&extra); !errors.Is(err, io.EOF) {
		return fmt.Errorf("legacy delegation has trailing data: %v", err)
	}
	return nil
}

func delegationJSONToken(d *json.Decoder, want json.Delim) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	if token != want {
		return fmt.Errorf("legacy delegation: expected %q", want)
	}
	return nil
}

func importDelegationRow(ctx context.Context, writer *delegationImportStatements, table, id string, raw json.RawMessage) error {
	var key string
	var insert func() error
	switch table {
	case "records":
		var v Record
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		key, insert = v.RecordID, func() error { return writer.Record(ctx, v) }
	case "routes":
		var v Route
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		key, insert = v.DelegationID, func() error { _, err := writer.routes.ExecContext(ctx, routesArgs(v)...); return err }
	case "remote_threads":
		var v RemoteThread
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		key, insert = v.ThreadID, func() error { _, err := writer.remote_threads.ExecContext(ctx, remote_threadsArgs(v)...); return err }
	case "executions":
		var v ExecutionFence
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		key, insert = v.DelegationID, func() error { _, err := writer.executions.ExecContext(ctx, executionsArgs(v)...); return err }
	case "outbox":
		var v OutboxRecord
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		key, insert = v.RecordID, func() error { _, err := writer.outbox.ExecContext(ctx, outboxArgs(v)...); return err }
	case "inbox_cursors":
		var v InboxCursor
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		key, insert = v.ChatID, func() error { _, err := writer.inbox_cursors.ExecContext(ctx, inbox_cursorsArgs(v)...); return err }
	case "inbox_backoffs":
		var v InboxBackoff
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		key, insert = v.ChatID, func() error { _, err := writer.inbox_backoffs.ExecContext(ctx, inbox_backoffsArgs(v)...); return err }
	default:
		return fmt.Errorf("unknown delegation migration table %q", table)
	}
	if strings.TrimSpace(key) == "" || key != strings.TrimSpace(key) || (id != "" && id != key) {
		return fmt.Errorf("invalid delegation %s key %q (stored as %q)", table, key, id)
	}
	// Native primary keys reject duplicate map keys and array identities.
	return insert()
}
