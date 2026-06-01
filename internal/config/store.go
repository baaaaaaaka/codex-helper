package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/gofrs/flock"
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
	mu   sync.Mutex
	path string
	lock *flock.Flock
}

func DefaultPath() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("get user config dir: %w", err)
	}
	return filepath.Join(base, "codex-proxy", "config.json"), nil
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
	return filepath.Join(base, "codex-proxy", "config.json"), nil
}

func NewStore(pathOverride string) (*Store, error) {
	path := pathOverride
	if path == "" {
		p, err := DefaultPath()
		if err != nil {
			return nil, err
		}
		path = p
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create config dir: %w", err)
	}

	return &Store{
		path: path,
		lock: flock.New(path + ".lock"),
	}, nil
}

func (s *Store) Path() string { return s.path }

func (s *Store) Load() (Config, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.lock.Lock(); err != nil {
		return Config{}, fmt.Errorf("lock config: %w", err)
	}
	defer func() { _ = s.lock.Unlock() }()

	return s.loadUnlocked()
}

func (s *Store) Save(cfg Config) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.lock.Lock(); err != nil {
		return fmt.Errorf("lock config: %w", err)
	}
	defer func() { _ = s.lock.Unlock() }()

	return s.saveUnlocked(cfg)
}

func (s *Store) Update(fn func(*Config) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.lock.Lock(); err != nil {
		return fmt.Errorf("lock config: %w", err)
	}
	defer func() { _ = s.lock.Unlock() }()

	cfg, err := s.loadUnlocked()
	if err != nil {
		return err
	}

	if err := fn(&cfg); err != nil {
		return err
	}

	return s.saveUnlocked(cfg)
}

func (s *Store) loadUnlocked() (Config, error) {
	b, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Config{Version: CurrentVersion}, nil
		}
		return Config{}, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}

	// Three-state gate. Compatibility is decided by the reader floor, not by
	// version equality, so an additive future config (higher version, same
	// floor) reads fine instead of bricking an older binary.
	if cfg.Version < 0 || cfg.MinReader < 0 {
		return Config{}, fmt.Errorf("corrupt config: negative version (version=%d minReader=%d)", cfg.Version, cfg.MinReader)
	}
	floor := cfg.MinReader
	if floor == 0 {
		// Legacy files (pre-minReader) and version 1 predate breaking changes.
		floor = MinReaderVersion
	}
	if floor > SupportedReaderVersion {
		return Config{}, &StaleReaderError{
			FileMinReader: floor,
			Supported:     SupportedReaderVersion,
			WriterVersion: cfg.Version,
		}
	}

	// Migrate legacy write-generation stamps in memory. Newer additive
	// generations (Version > CurrentVersion) are left as-is and read tolerantly.
	if cfg.Version == 0 || cfg.Version == 1 {
		cfg.Version = CurrentVersion
	}

	return cfg, nil
}

func (s *Store) saveUnlocked(cfg Config) error {
	// Never let an older binary clobber a newer-generation file: that would
	// silently drop fields this build does not know about. Fail loudly instead;
	// the operator/service should upgrade rather than corrupt the config.
	if existing, err := os.ReadFile(s.path); err == nil {
		var onDisk struct {
			Version int `json:"version"`
		}
		if json.Unmarshal(existing, &onDisk) == nil && onDisk.Version > CurrentVersion {
			return fmt.Errorf(
				"refuse to overwrite newer config (on-disk version %d > this build %d); upgrade codex-helper",
				onDisk.Version, CurrentVersion,
			)
		}
	}

	// Stamp this binary's own write generation and reader floor; the caller does
	// not control these.
	cfg.Version = CurrentVersion
	cfg.MinReader = MinReaderVersion

	dir := filepath.Dir(s.path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}

	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}
	b = append(b, '\n')

	if err := atomicWriteFile(s.path, b, 0o600); err != nil {
		return fmt.Errorf("atomic write config: %w", err)
	}

	return nil
}
