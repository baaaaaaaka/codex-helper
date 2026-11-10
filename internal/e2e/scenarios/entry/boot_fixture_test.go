package entry

import (
	"database/sql"
	"fmt"
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
	// The first publication is completed before the watchdog is started.
	// Later activations must not truncate its immutable publication seal.
	marker := filepath.Join(root, "boot.sqlite.published")
	const magic = "CXP-SQLITE-1\n"
	if raw, err := os.ReadFile(marker); err == nil {
		if string(raw) != magic {
			return fmt.Errorf("invalid fixture publication marker")
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.WriteFile(marker, []byte(magic), 0600)
}
