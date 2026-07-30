package teams

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	_ "modernc.org/sqlite"
)

func teamsLedgerSQLitePath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	ext := filepath.Ext(path)
	if ext == "" {
		return path + ".sqlite"
	}
	return strings.TrimSuffix(path, ext) + ".sqlite"
}

func openTeamsLedgerSQLite(path string) (*sql.DB, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("ledger sqlite path is required")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	_, statErr := os.Stat(path)
	newDB := os.IsNotExist(statErr)
	if statErr != nil && !newDB {
		return nil, statErr
	}
	query := url.Values{}
	query.Set("mode", "rwc")
	db, err := sql.Open("sqlite", teamsSQLiteFileURI(path, query))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	stmts := []string{
		`PRAGMA synchronous = NORMAL`,
		`PRAGMA busy_timeout = 5000`,
		`PRAGMA temp_store = MEMORY`,
	}
	if newDB {
		stmts = append([]string{`PRAGMA journal_mode = WAL`}, stmts...)
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			_ = db.Close()
			return nil, err
		}
	}
	chmodTeamsLedgerSQLiteFiles(path)
	return db, nil
}

// openTeamsLedgerSQLiteReadOnly opens an existing ledger without creating its
// directory, database, WAL/SHM sidecars, schema, or chmod writes. Migration
// discovery and replay-fence union use this path so inspecting a retained
// legacy ledger cannot mutate it.
func openTeamsLedgerSQLiteReadOnly(ctx context.Context, path string) (*sql.DB, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("ledger sqlite path is required")
	}
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("ledger sqlite path is not a regular file: %s", path)
	}
	query := url.Values{}
	query.Set("mode", "ro")
	// A cleanly closed WAL database has no WAL or a zero-length WAL. Mark that
	// stable snapshot immutable so SQLite does not create or update WAL/SHM
	// sidecars for a read. A non-empty WAL must remain a normal read-only open
	// so its committed frames are visible, but only when an existing SHM lets
	// SQLite do that without creating a persistent sidecar.
	if walInfo, walErr := os.Stat(path + "-wal"); os.IsNotExist(walErr) || (walErr == nil && walInfo.Size() == 0) {
		query.Set("immutable", "1")
	} else if walErr != nil {
		return nil, walErr
	} else if _, shmErr := os.Stat(path + "-shm"); shmErr != nil {
		if os.IsNotExist(shmErr) {
			return nil, fmt.Errorf("read live ledger WAL without creating SHM: %w", shmErr)
		}
		return nil, shmErr
	}
	db, err := sql.Open("sqlite", teamsSQLiteFileURI(path, query))
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func teamsSQLiteFileURI(path string, query url.Values) string {
	u := url.URL{Scheme: "file"}
	if runtime.GOOS == "windows" {
		u = teamsSQLiteWindowsFileURL(path)
	} else {
		u.Path = path
	}
	if len(query) > 0 {
		u.RawQuery = query.Encode()
	}
	return u.String()
}

func teamsSQLiteWindowsFileURL(path string) url.URL {
	slash := strings.ReplaceAll(path, `\`, `/`)
	if strings.HasPrefix(slash, "//") {
		trimmed := strings.TrimLeft(slash, "/")
		host, rest, ok := strings.Cut(trimmed, "/")
		if ok {
			return url.URL{Scheme: "file", Host: host, Path: "/" + rest}
		}
		return url.URL{Scheme: "file", Path: slash}
	}
	if len(slash) >= 2 && slash[1] == ':' {
		slash = "/" + slash
	}
	return url.URL{Scheme: "file", Path: slash}
}

func chmodTeamsLedgerSQLiteFiles(path string) {
	_ = os.Chmod(path, 0o600)
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(path + suffix); err == nil {
			_ = os.Chmod(path+suffix, 0o600)
		}
	}
}
