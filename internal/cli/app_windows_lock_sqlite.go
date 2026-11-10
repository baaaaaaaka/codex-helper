package cli

import (
	"database/sql"
	"fmt"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
	"os"
	"path/filepath"
	"time"
)

// The mkdir still arbitrates installation ownership. This immutable descriptor
// merely identifies that owner; it never shares the business state's database.
func writeWindowsManagedLockOwner(dir string, o codexWindowsManagedLockMetadata) error {
	return sqlitestate.PublishSnapshot(filepath.Join(dir, codexWindowsManagedLockOwner), nil, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`CREATE TABLE owner(id INTEGER PRIMARY KEY CHECK(id=1),pid INTEGER NOT NULL,created_at TEXT NOT NULL)`); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO owner VALUES(1,?,?)`, o.PID, o.CreatedAt.Format(time.RFC3339Nano))
		return err
	})
}
func readWindowsManagedLockOwner(dir string) (o codexWindowsManagedLockMetadata, err error) {
	db, err := sqlitestate.OpenSnapshot(filepath.Join(dir, codexWindowsManagedLockOwner), 64*1024)
	if os.IsNotExist(err) {
		return readLegacyWindowsManagedLockOwner(dir)
	}
	if err != nil {
		return o, err
	}
	defer db.Close()
	var v int
	if err = db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		return o, err
	}
	if v != 1 {
		return o, fmt.Errorf("unsupported install lock descriptor %d", v)
	}
	var at string
	if err = db.QueryRow(`SELECT pid,created_at FROM owner WHERE id=1`).Scan(&o.PID, &at); err != nil {
		return o, err
	}
	o.CreatedAt, err = time.Parse(time.RFC3339Nano, at)
	return o, err
}
