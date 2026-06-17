package beacon

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/gofrs/flock"
)

type Store struct {
	mu   sync.Mutex
	path string
}

var storeProcessLocks sync.Map

func DefaultStorePath() (string, error) {
	if override := strings.TrimSpace(os.Getenv("CODEX_HELPER_BEACON_STORE")); override != "" {
		return override, nil
	}
	path, err := appdirs.StatePath("beacon", "state.json")
	if err != nil {
		return "", err
	}
	legacyPath, legacyErr := appdirs.LegacyCachePath("beacon", "state.json")
	if legacyErr != nil {
		return path, nil
	}
	dir, err := appdirs.ResolveMigratedDirWithRequired(filepath.Dir(path), filepath.Dir(legacyPath), "state.json")
	if err != nil {
		return "", err
	}
	resolvedPath := filepath.Join(dir, "state.json")
	if sameBeaconStorePath(resolvedPath, path) && !sameBeaconStorePath(path, legacyPath) && !beaconStateFileValid(path) && beaconStateFileValid(legacyPath) {
		if err := appdirs.CopyFileReplacing(path, legacyPath); err != nil {
			return legacyPath, nil
		}
	}
	return resolvedPath, nil
}

func NewStore(path string) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		var err error
		path, err = DefaultStorePath()
		if err != nil {
			return nil, err
		}
	}
	path = filepath.Clean(path)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create beacon state dir: %w", err)
	}
	return &Store{path: path}, nil
}

func (s *Store) Path() string { return s.path }

func (s *Store) Load() (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var st State
	err := s.withFileLockUnlocked(func() error {
		var err error
		st, err = s.loadUnlocked()
		return err
	})
	return st, err
}

func (s *Store) Save(st State) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.withFileLockUnlocked(func() error {
		return s.saveUnlocked(st)
	})
}

func (s *Store) Update(fn func(*State) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.withFileLockUnlocked(func() error {
		st, err := s.loadUnlocked()
		if err != nil {
			return err
		}
		if err := fn(&st); err != nil {
			return err
		}
		return s.saveUnlocked(st)
	})
}

func (s *Store) withFileLockUnlocked(fn func() error) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("create beacon state dir: %w", err)
	}
	processLock := beaconStoreProcessLock(s.path)
	processLock.Lock()
	defer processLock.Unlock()

	lock := flock.New(s.path + ".lock")
	if err := lock.Lock(); err != nil {
		return fmt.Errorf("lock beacon state: %w", err)
	}
	defer func() { _ = lock.Unlock() }()
	return fn()
}

func beaconStoreProcessLock(path string) *sync.Mutex {
	actual, _ := storeProcessLocks.LoadOrStore(filepath.Clean(path), &sync.Mutex{})
	return actual.(*sync.Mutex)
}

func beaconStateFileValid(path string) bool {
	b, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	var st State
	if err := json.Unmarshal(b, &st); err != nil {
		return false
	}
	if st.Version == 0 {
		st.Version = StateVersion
	}
	return st.Version == StateVersion
}

func sameBeaconStorePath(a string, b string) bool {
	if strings.TrimSpace(a) == "" || strings.TrimSpace(b) == "" {
		return false
	}
	return filepath.Clean(a) == filepath.Clean(b)
}

func (s *Store) loadUnlocked() (State, error) {
	b, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			st := State{Version: StateVersion}
			st.normalize()
			return st, nil
		}
		return State{}, fmt.Errorf("read beacon state: %w", err)
	}
	if len(b) > 16<<20 {
		return State{}, fmt.Errorf("beacon state too large: %d bytes", len(b))
	}
	var st State
	if err := json.Unmarshal(b, &st); err != nil {
		return State{}, fmt.Errorf("parse beacon state: %w", err)
	}
	if st.Version == 0 {
		st.Version = StateVersion
	}
	if st.Version != StateVersion {
		return State{}, fmt.Errorf("unsupported beacon state version %d (expected %d)", st.Version, StateVersion)
	}
	st.normalize()
	return st, nil
}

func (s *Store) saveUnlocked(st State) error {
	st.normalize()
	if st.Version != StateVersion {
		return fmt.Errorf("refuse to write beacon state version %d (expected %d)", st.Version, StateVersion)
	}
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("create beacon state dir: %w", err)
	}
	b, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal beacon state: %w", err)
	}
	b = append(b, '\n')
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".state-*.tmp")
	if err != nil {
		return fmt.Errorf("create beacon temp state: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write beacon temp state: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync beacon temp state: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close beacon temp state: %w", err)
	}
	if err := os.Chmod(tmpName, 0o600); err != nil {
		return fmt.Errorf("chmod beacon temp state: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("replace beacon state: %w", err)
	}
	if err := syncBeaconStateDir(filepath.Dir(s.path)); err != nil {
		return err
	}
	return nil
}

func syncBeaconStateDir(dir string) error {
	if runtime.GOOS == "windows" {
		return nil
	}
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open beacon state dir for sync: %w", err)
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		if runtime.GOOS == "darwin" {
			return nil
		}
		return fmt.Errorf("sync beacon state dir: %w", err)
	}
	return nil
}
