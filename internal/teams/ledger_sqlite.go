package teams

import (
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
