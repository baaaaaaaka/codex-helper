package skills

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/gofrs/flock"
)

type Store struct {
	mu         sync.Mutex
	configPath string
	statePath  string
	lock       *flock.Flock
}

func NewStore(configDir string) (*Store, error) {
	configDir = filepath.Clean(configDir)
	if configDir == "." || configDir == "" {
		return nil, fmt.Errorf("empty config dir")
	}
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		return nil, fmt.Errorf("create skill subscription config dir: %w", err)
	}
	configPath := filepath.Join(configDir, ConfigFilename)
	return &Store{
		configPath: configPath,
		statePath:  filepath.Join(configDir, StateFilename),
		lock:       flock.New(configPath + ".lock"),
	}, nil
}

func (s *Store) ConfigPath() string { return s.configPath }
func (s *Store) StatePath() string  { return s.statePath }

func (s *Store) LoadConfig() (Config, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.lock.Lock(); err != nil {
		return Config{}, fmt.Errorf("lock skill subscriptions: %w", err)
	}
	defer func() { _ = s.lock.Unlock() }()
	return s.loadConfigUnlocked()
}

func (s *Store) LoadState() (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.lock.Lock(); err != nil {
		return State{}, fmt.Errorf("lock skill subscription state: %w", err)
	}
	defer func() { _ = s.lock.Unlock() }()
	return s.loadStateUnlocked()
}

func (s *Store) Update(fn func(*Config, *State) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.lock.Lock(); err != nil {
		return fmt.Errorf("lock skill subscriptions: %w", err)
	}
	defer func() { _ = s.lock.Unlock() }()

	cfg, err := s.loadConfigUnlocked()
	if err != nil {
		return err
	}
	st, err := s.loadStateUnlocked()
	if err != nil {
		return err
	}
	if err := fn(&cfg, &st); err != nil {
		return err
	}
	if err := s.saveConfigUnlocked(cfg); err != nil {
		return err
	}
	return s.saveStateUnlocked(st)
}

func (s *Store) UpdateState(fn func(*State) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.lock.Lock(); err != nil {
		return fmt.Errorf("lock skill subscription state: %w", err)
	}
	defer func() { _ = s.lock.Unlock() }()

	st, err := s.loadStateUnlocked()
	if err != nil {
		return err
	}
	if err := fn(&st); err != nil {
		return err
	}
	return s.saveStateUnlocked(st)
}

func (s *Store) loadConfigUnlocked() (Config, error) {
	var cfg Config
	if err := readJSONFile(s.configPath, &cfg); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return Config{Version: configVersion}, nil
		}
		return Config{}, fmt.Errorf("read skill subscriptions: %w", err)
	}
	if cfg.Version == 0 {
		cfg.Version = configVersion
	}
	if cfg.Version != configVersion {
		return Config{}, fmt.Errorf("unsupported skill subscriptions version %d (expected %d)", cfg.Version, configVersion)
	}
	return cfg, nil
}

func (s *Store) loadStateUnlocked() (State, error) {
	var st State
	if err := readJSONFile(s.statePath, &st); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return State{Version: stateVersion}, nil
		}
		return State{}, fmt.Errorf("read skill subscription state: %w", err)
	}
	if st.Version == 0 {
		st.Version = stateVersion
	}
	if st.Version != stateVersion {
		return State{}, fmt.Errorf("unsupported skill subscription state version %d (expected %d)", st.Version, stateVersion)
	}
	return st, nil
}

func (s *Store) saveConfigUnlocked(cfg Config) error {
	if cfg.Version == 0 {
		cfg.Version = configVersion
	}
	if cfg.Version != configVersion {
		return fmt.Errorf("refuse to write skill subscriptions version %d (expected %d)", cfg.Version, configVersion)
	}
	return writeJSONFile(s.configPath, cfg)
}

func (s *Store) saveStateUnlocked(st State) error {
	if st.Version == 0 {
		st.Version = stateVersion
	}
	if st.Version != stateVersion {
		return fmt.Errorf("refuse to write skill subscription state version %d (expected %d)", st.Version, stateVersion)
	}
	return writeJSONFile(s.statePath, st)
}

func readJSONFile(path string, dest any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, dest); err != nil {
		return fmt.Errorf("parse %s: %w", filepath.Base(path), err)
	}
	return nil
}

func writeJSONFile(path string, value any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create %s dir: %w", filepath.Base(path), err)
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal %s: %w", filepath.Base(path), err)
	}
	data = append(data, '\n')
	return atomicWriteFile(path, data, 0o600)
}
