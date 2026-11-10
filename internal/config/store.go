package config

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

// ErrStaleReader indicates the on-disk config requires a newer codex-helper
// than this binary provides (a breaking schema change raised the reader floor
// above what this build supports). A caller that hits this MUST NOT write the
// config; a long-lived service should surface it loudly and let a newer binary
// take over rather than crash-loop.
var ErrStaleReader = errors.New("config requires a newer codex-helper version")

// StaleReaderError carries the details behind ErrStaleReader. It satisfies
// errors.Is(err, ErrStaleReader).
type StaleReaderError struct {
	FileMinReader int
	Supported     int
	WriterVersion int
}

func (e *StaleReaderError) Error() string {
	return fmt.Sprintf(
		"config requires reader >= %d but this build supports <= %d (written by config version %d); upgrade codex-helper",
		e.FileMinReader, e.Supported, e.WriterVersion,
	)
}

func (e *StaleReaderError) Is(target error) bool { return target == ErrStaleReader }

type Store struct {
	mu           sync.Mutex
	path, legacy string
	db           *sql.DB
}

func DefaultPath() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("get user config dir: %w", err)
	}
	return filepath.Join(base, "codex-proxy", "config.sqlite"), nil
}

func DefaultPathForHome(home string) (string, error) {
	home = filepath.Clean(strings.TrimSpace(home))
	if home == "" {
		return "", fmt.Errorf("empty home dir")
	}

	base := home
	switch runtime.GOOS {
	case "windows":
		base = filepath.Join(home, "AppData", "Roaming")
	case "darwin":
		base = filepath.Join(home, "Library", "Application Support")
	default:
		base = filepath.Join(home, ".config")
	}
	return filepath.Join(base, "codex-proxy", "config.sqlite"), nil
}

func NewStore(pathOverride string) (*Store, error) {
	path := strings.TrimSpace(pathOverride)
	if path == "" {
		var err error
		path, err = DefaultPath()
		if err != nil {
			return nil, err
		}
	}
	path, legacy := configDatabasePaths(path)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	return &Store{path: path, legacy: legacy}, nil
}

// An old --config filename remains a path alias. It never selects a legacy
// storage backend. Unknown historical suffixes use a separate .sqlite file.
func configDatabasePaths(path string) (string, string) {
	if strings.EqualFold(filepath.Ext(path), ".sqlite") {
		return path, strings.TrimSuffix(path, filepath.Ext(path)) + ".json"
	}
	if strings.EqualFold(filepath.Ext(path), ".json") {
		return strings.TrimSuffix(path, filepath.Ext(path)) + ".sqlite", path
	}
	return path + ".sqlite", path
}
func (s *Store) Path() string { return s.path }
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}
func (s *Store) ensureDB() error {
	if s.db != nil {
		return nil
	}
	db, err := openConfigDatabase(s.path, false)
	if err != nil {
		return err
	}
	if err = migrateConfigDatabase(db, s.legacy); err != nil {
		db.Close()
		return err
	}
	s.db = db
	return nil
}
func (s *Store) Load() (Config, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureDB(); err != nil {
		return Config{}, err
	}
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Config{}, err
	}
	defer tx.Rollback()
	cfg, err := readConfigTx(tx)
	if err != nil {
		return cfg, err
	}
	return cfg, tx.Commit()
}
func LoadPathReadOnly(path string) (Config, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		var err error
		path, err = DefaultPath()
		if err != nil {
			return Config{}, err
		}
	}
	path, legacy := configDatabasePaths(path)
	if _, err := sqlitestate.Inspect(path); errors.Is(err, os.ErrNotExist) {
		if _, legacyErr := os.Lstat(legacy); legacyErr == nil {
			return Config{}, fmt.Errorf("configuration at %s requires SQLite migration before read-only access", legacy)
		} else if !errors.Is(legacyErr, os.ErrNotExist) {
			return Config{}, legacyErr
		}
		return Config{Version: CurrentVersion, MinReader: MinReaderVersion}, nil
	} else if err != nil {
		return Config{}, err
	}
	db, err := openConfigDatabase(path, true)
	if err != nil {
		return Config{}, err
	}
	defer db.Close()
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Config{}, err
	}
	defer tx.Rollback()
	var version int
	if err = tx.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return Config{}, err
	}
	if version != 1 {
		return Config{}, fmt.Errorf("unsupported config database version %d", version)
	}
	cfg, err := readConfigTx(tx)
	if err != nil {
		return cfg, err
	}
	return cfg, tx.Commit()
}
func (s *Store) Save(cfg Config) error {
	return s.write(func(tx *sql.Tx) error { return saveConfigTx(tx, cfg) })
}
func (s *Store) Update(fn func(*Config) error) error {
	return s.write(func(tx *sql.Tx) error {
		cfg, err := readConfigTx(tx)
		if err != nil {
			return err
		}
		if err = fn(&cfg); err != nil {
			return err
		}
		return saveConfigTx(tx, cfg)
	})
}
func (s *Store) write(fn func(*sql.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureDB(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	header, err := readConfigHeader(tx)
	if err != nil {
		return err
	}
	if header.Version > CurrentVersion {
		return fmt.Errorf("refuse to overwrite newer config version %d", header.Version)
	}
	if err = fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}
