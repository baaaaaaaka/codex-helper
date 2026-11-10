package sqlitestate

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const publicationMagic = "CXP-SQLITE-1\n"

// CommitSchema seals a completed migration before its source can be archived.
// It is used only at schema admission, never for ordinary business mutations.
func CommitSchema(tx *sql.Tx, db *sql.DB) error {
	if err := tx.Commit(); err != nil {
		return err
	}
	return Seal(db)
}

// CheckPublishedSchema also serves owners that retain a separate WAL opener.
// A published database cannot re-enter the new-install migration branch.
func CheckPublishedSchema(db *sql.DB) error {
	var seq int
	var name, path string
	if err := db.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
		return err
	}
	sealed, err := publicationExists(path)
	if err != nil || !sealed {
		return err
	}
	var version int
	if err = db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	if version == 0 {
		return fmt.Errorf("published SQLite database has no schema: %s", path)
	}
	return nil
}

// Inspect distinguishes a new location from the loss of published state. The
// fixed marker contains no business fields and is never changed after creation.
// Errors for lost published state intentionally do not match os.ErrNotExist.
func Inspect(path string) (os.FileInfo, error) {
	sealed, err := publicationExists(path)
	if err != nil {
		return nil, err
	}
	info, err := os.Lstat(path)
	if sealed && os.IsNotExist(err) {
		return nil, fmt.Errorf("published SQLite database is missing: %s", path)
	}
	if sealed && err == nil && info.Size() == 0 {
		return nil, fmt.Errorf("published SQLite database is empty: %s", path)
	}
	// SQLite accepts some short, nonempty files as empty databases. They are
	// corrupt native state, not a fresh install or a failed schema transaction.
	if err == nil && info.Mode().IsRegular() && info.Size() > 0 {
		f, openErr := openSharedReadFile(path)
		if openErr != nil {
			return nil, openErr
		}
		var header [16]byte
		_, readErr := io.ReadFull(f, header[:])
		f.Close()
		if readErr != nil || string(header[:]) != "SQLite format 3\x00" {
			return nil, fmt.Errorf("invalid SQLite database header: %s", path)
		}
	}
	return info, err
}
func publicationExists(path string) (bool, error) {
	marker := path + ".published"
	info, err := os.Lstat(marker)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || info.Size() != int64(len(publicationMagic)) {
		return false, fmt.Errorf("invalid SQLite publication marker: %s", marker)
	}
	f, err := os.Open(marker)
	if err != nil {
		return false, err
	}
	defer f.Close()
	var b [len(publicationMagic)]byte
	if _, err = io.ReadFull(f, b[:]); err != nil {
		return false, err
	}
	if string(b[:]) != publicationMagic {
		return false, fmt.Errorf("invalid SQLite publication marker: %s", marker)
	}
	return true, nil
}

// Seal runs after schema/data commit and before legacy archival. Existing
// markers cause no writes. Portable immutable install manifests are published
// with their owning directory and do not use this separate mutable-store seal.
func Seal(db *sql.DB) error {
	var seq int
	var name, path string
	if err := db.QueryRow(`PRAGMA database_list`).Scan(&seq, &name, &path); err != nil {
		return err
	}
	if name != "main" || path == "" {
		return fmt.Errorf("SQLite publication requires a file database")
	}
	sealed, err := publicationExists(path)
	if err != nil {
		return err
	}
	if sealed {
		return nil
	}
	var version int
	if err = db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	if version == 0 {
		return fmt.Errorf("cannot publish uncommitted SQLite schema: %s", path)
	}
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".sqlite-published-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if _, err = f.WriteString(publicationMagic); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Link(tmp, path+".published"); err != nil {
		if !os.IsExist(err) {
			return err
		}
		if _, err = publicationExists(path); err != nil {
			return err
		}
	}
	return syncPublicationDirectory(dir)
}
