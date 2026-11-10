package entry

import (
	"database/sql"
	"os"
	"path/filepath"
	"strconv"

	_ "modernc.org/sqlite"
)

// Frozen bootstrap protocol fixture, independent of the runtime implementation.
// Publish a closed database so the real launcher observes an atomic activation.
func publishBootFixture(root, good, previous string, generation uint64) error {
	f, err := os.CreateTemp(root, ".boot-fixture-*.sqlite")
	if err != nil {
		return err
	}
	path := f.Name()
	if err = f.Close(); err != nil {
		return err
	}
	defer os.Remove(path)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return err
	}
	defer db.Close()
	if _, err = db.Exec(`PRAGMA user_version=1; CREATE TABLE boot(id INTEGER PRIMARY KEY CHECK(id=1),protocol INTEGER NOT NULL,generation TEXT NOT NULL,good TEXT NOT NULL,previous TEXT NOT NULL,candidate_version TEXT,candidate_sha256 TEXT,candidate_attempt TEXT)`); err != nil {
		return err
	}
	if _, err = db.Exec(`INSERT INTO boot VALUES(1,2,?,?,?,NULL,NULL,NULL)`, strconv.FormatUint(generation, 10), good, previous); err != nil {
		return err
	}
	if err = db.Close(); err != nil {
		return err
	}
	if err = os.Rename(path, filepath.Join(root, "boot.sqlite")); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(root, "boot.sqlite.published"), []byte("CXP-SQLITE-1\n"), 0600)
}
