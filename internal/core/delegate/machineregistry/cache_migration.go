package machineregistry

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
	"io"
	"os"
	"path/filepath"
)

// A legacy cache can be re-observed from Graph, but malformed or future input
// must not silently reset meeting generations. Import and version are atomic.
func migrateRegistryCache(db *sql.DB, legacy string) error {
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version int
	if err = tx.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	if version == 1 {
		return sqlitestate.CommitSchema(tx, db)
	}
	if version != 0 {
		return fmt.Errorf("unsupported registry database version %d", version)
	}
	if _, err = tx.Exec(registryCacheDDL); err != nil {
		return err
	}
	defaultPath, e := appdirs.StatePath("teams", "machine-registry.json")
	if e == nil && filepath.Clean(legacy) == filepath.Clean(defaultPath) {
		if _, e = os.Stat(legacy); errors.Is(e, os.ErrNotExist) {
			old, e := appdirs.LegacyConfigPath("teams", "machine-registry.json")
			if e != nil {
				return e
			}
			legacy = old
		} else if e != nil {
			return e
		}
	}
	if _, err = tx.Exec(`CREATE TABLE registry_imports(source_path TEXT PRIMARY KEY)`); err != nil {
		return err
	}
	f, err := os.Open(legacy)
	imported := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if imported {
		var v *Cache
		d := json.NewDecoder(f)
		err = d.Decode(&v)
		if err == nil && v == nil {
			err = fmt.Errorf("registry cache must be an object")
		}
		if err == nil {
			var extra any
			if e := d.Decode(&extra); !errors.Is(e, io.EOF) {
				err = fmt.Errorf("trailing registry cache data: %v", e)
			}
		}
		f.Close()
		if err != nil {
			return fmt.Errorf("migrate registry cache: %w", err)
		}
		if v.SchemaVersion < 0 || v.SchemaVersion > 1 {
			return fmt.Errorf("unsupported registry cache version %d", v.SchemaVersion)
		}
		if _, err = tx.Exec(registryCachePut, cacheArgs(*v)...); err != nil {
			return err
		}
	}
	if imported {
		if _, err = tx.Exec(`INSERT INTO registry_imports VALUES(?)`, legacy); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`PRAGMA user_version=1`); err != nil {
		return err
	}
	if err = sqlitestate.CommitSchema(tx, db); err != nil {
		return err
	}
	if imported {
		if _, err = os.Lstat(legacy + ".migrated"); err == nil {
			return fmt.Errorf("registry migration committed; archive already exists")
		}
		if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("registry migration committed; inspect archive: %w", err)
		}
		if err = os.Rename(legacy, legacy+".migrated"); err != nil {
			return fmt.Errorf("registry migration committed; archive: %w", err)
		}
	}
	return nil
}
