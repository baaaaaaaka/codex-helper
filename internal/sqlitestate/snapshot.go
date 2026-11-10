package sqlitestate

import (
	"database/sql"
	"os"
	"path/filepath"
)

// PublishSnapshot commits concrete SQL into a private database, closes every
// handle, then uses the existing rename boundary to publish immutable bytes.
// Readers never share WAL or a writable connection with the publisher. The
// callback owns its table definitions; this helper maps no business objects.
func PublishSnapshot(path string, secure func(string) error, fill func(*sql.Tx) error) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".sqlite-snapshot-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if err = f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	defer os.Remove(tmp)
	defer os.Remove(tmp + "-journal")
	db, err := Open(tmp, false)
	if err != nil {
		return err
	}
	defer db.Close()
	// Immutable descriptors are small. A 1 KiB page avoids an 8 KiB floor
	// without changing page/cache/durability policy for any business store.
	if _, err = db.Exec(`PRAGMA page_size=1024`); err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = fill(tx); err != nil {
		return err
	}
	if _, err = tx.Exec(`PRAGMA user_version=1`); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	if err = db.Close(); err != nil {
		return err
	}
	if secure != nil {
		if err = secure(tmp); err != nil {
			return err
		}
	}
	root, err := os.OpenRoot(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer root.Close()
	return root.Rename(filepath.Base(tmp), filepath.Base(path))
}
