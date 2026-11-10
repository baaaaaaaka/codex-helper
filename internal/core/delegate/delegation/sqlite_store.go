package delegation

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"

	_ "modernc.org/sqlite"
)

const delegationSQLiteStoreVersion = 2

func storePathUsesSQLite(path string) bool {
	return strings.EqualFold(filepath.Ext(strings.TrimSpace(path)), ".sqlite")
}
func legacyJSONStorePathForSQLite(path string) string {
	if !storePathUsesSQLite(path) {
		return ""
	}
	return strings.TrimSuffix(path, filepath.Ext(path)) + ".json"
}

func openDelegationSQLiteStore(path string, create bool) (*sql.DB, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("delegation sqlite path is required")
	}
	if create {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			return nil, err
		}
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			err = f.Close()
		}
		if err != nil && !os.IsExist(err) {
			return nil, err
		}
	}
	if _, err := sqlitestate.Inspect(path); err != nil {
		return nil, err
	}
	query := url.Values{}
	query.Set("_txlock", "immediate")
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "synchronous(NORMAL)")
	query.Add("_pragma", "temp_store(MEMORY)")
	if create {
		query.Set("mode", "rwc")
	} else {
		query.Set("mode", "rw")
	}
	db, err := sql.Open("sqlite", delegationSQLiteFileURI(path, query))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, stmt := range []string{
		`PRAGMA journal_mode = WAL`,
		`PRAGMA synchronous = NORMAL`,
		`PRAGMA busy_timeout = 5000`,
		`PRAGMA temp_store = MEMORY`,
		`PRAGMA foreign_keys = ON`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	if err := chmodDelegationSQLiteFiles(path); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func delegationSQLiteFileURI(path string, query url.Values) string {
	u := url.URL{Scheme: "file"}
	if runtime.GOOS == "windows" {
		slash := strings.ReplaceAll(path, `\`, `/`)
		if strings.HasPrefix(slash, "//") {
			trimmed := strings.TrimLeft(slash, "/")
			host, rest, ok := strings.Cut(trimmed, "/")
			if ok {
				u.Host = host
				u.Path = "/" + rest
			} else {
				u.Path = slash
			}
		} else {
			if len(slash) >= 2 && slash[1] == ':' {
				slash = "/" + slash
			}
			u.Path = slash
		}
	} else {
		u.Path = path
	}
	if len(query) > 0 {
		u.RawQuery = query.Encode()
	}
	return u.String()
}

func chmodDelegationSQLiteFiles(path string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		info, err := os.Lstat(path + suffix)
		if suffix != "" && os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("delegation SQLite file is not regular: %s", path+suffix)
		}
		if info.Mode().Perm() != 0600 {
			if err := os.Chmod(path+suffix, 0600); err != nil {
				return err
			}
		}
	}
	return nil
}

func ensureDelegationSQLiteSchema(ctx context.Context, db *sql.DB) error {
	return migrateDelegationSQLite(ctx, db, "")
}
