package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

// PatchHistoryEntry records a single patch operation.
type PatchHistoryEntry struct {
	Path          string    `json:"path"`
	OrigSHA256    string    `json:"origSha256"`
	PatchedSHA256 string    `json:"patchedSha256"`
	ProxyVersion  string    `json:"proxyVersion"`
	PatchedAt     time.Time `json:"patchedAt"`
	Failed        bool      `json:"failed,omitempty"`
	FailureReason string    `json:"failureReason,omitempty"`
}

// PatchHistory is the on-disk structure for patch_history.json.
type PatchHistory struct {
	Version int                 `json:"version"`
	Entries []PatchHistoryEntry `json:"entries"`
}

const patchHistoryVersion = 1

// PatchHistoryStore provides locked read/write access to patch_history.json.
type PatchHistoryStore struct {
	mu   sync.Mutex
	path string
	lock *flock.Flock
}

// NewPatchHistoryStore creates a store for the given config directory.
// The history file is stored at <configDir>/patch_history.json.
func NewPatchHistoryStore(configDir string) (*PatchHistoryStore, error) {
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		return nil, fmt.Errorf("create config dir: %w", err)
	}
	path := filepath.Join(configDir, "patch_history.json")
	return &PatchHistoryStore{
		path: path,
		lock: flock.New(path + ".lock"),
	}, nil
}

// Path returns the file path of the patch history store.
func (s *PatchHistoryStore) Path() string { return s.path }

// Load reads the current patch history from disk.
func (s *PatchHistoryStore) Load() (PatchHistory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.lock.Lock(); err != nil {
		return PatchHistory{}, fmt.Errorf("lock patch history: %w", err)
	}
	defer func() { _ = s.lock.Unlock() }()

	return s.loadUnlocked()
}

// Update applies fn to the current history and writes it back atomically.
func (s *PatchHistoryStore) Update(fn func(*PatchHistory) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.lock.Lock(); err != nil {
		return fmt.Errorf("lock patch history: %w", err)
	}
	defer func() { _ = s.lock.Unlock() }()

	h, err := s.loadUnlocked()
	if err != nil {
		return err
	}
	if err := fn(&h); err != nil {
		return err
	}
	return s.saveUnlocked(h)
}

// IsPatched returns true if the binary at path with the given SHA256 hash
// has already been successfully patched.
func (s *PatchHistoryStore) IsPatched(path, origSHA256 string) (bool, error) {
	h, err := s.Load()
	if err != nil {
		return false, err
	}
	for _, e := range h.Entries {
		if e.Path == path && e.OrigSHA256 == origSHA256 && !e.Failed {
			return true, nil
		}
	}
	return false, nil
}

// IsFailed returns true if a previous patch for this binary was recorded as failed.
func (s *PatchHistoryStore) IsFailed(path, origSHA256 string) (bool, error) {
	h, err := s.Load()
	if err != nil {
		return false, err
	}
	for _, e := range h.Entries {
		if e.Path == path && e.OrigSHA256 == origSHA256 && e.Failed {
			return true, nil
		}
	}
	return false, nil
}

// Find returns the entry for the given path and origSHA256, or nil if not found.
func (s *PatchHistoryStore) Find(path, origSHA256 string) (*PatchHistoryEntry, error) {
	h, err := s.Load()
	if err != nil {
		return nil, err
	}
	for i := range h.Entries {
		if h.Entries[i].Path == path && h.Entries[i].OrigSHA256 == origSHA256 {
			entry := h.Entries[i]
			return &entry, nil
		}
	}
	return nil, nil
}

// Remove deletes the entry for the given path and origSHA256.
func (s *PatchHistoryStore) Remove(path, origSHA256 string) error {
	return s.Update(func(h *PatchHistory) error {
		filtered := h.Entries[:0]
		for _, e := range h.Entries {
			if !(e.Path == path && e.OrigSHA256 == origSHA256) {
				filtered = append(filtered, e)
			}
		}
		h.Entries = filtered
		return nil
	})
}

// Upsert inserts or updates an entry matching (path, origSHA256).
func (s *PatchHistoryStore) Upsert(entry PatchHistoryEntry) error {
	return s.Update(func(h *PatchHistory) error {
		for i := range h.Entries {
			if h.Entries[i].Path == entry.Path && h.Entries[i].OrigSHA256 == entry.OrigSHA256 {
				h.Entries[i] = entry
				return nil
			}
		}
		h.Entries = append(h.Entries, entry)
		return nil
	})
}

func (s *PatchHistoryStore) loadUnlocked() (PatchHistory, error) {
	b, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return PatchHistory{Version: patchHistoryVersion}, nil
		}
		return PatchHistory{}, fmt.Errorf("read patch history: %w", err)
	}

	var h PatchHistory
	if err := json.Unmarshal(b, &h); err != nil {
		return PatchHistory{}, fmt.Errorf("parse patch history: %w", err)
	}

	if h.Version == 0 {
		h.Version = patchHistoryVersion
	}

	return h, nil
}

func (s *PatchHistoryStore) saveUnlocked(h PatchHistory) error {
	if h.Version == 0 {
		h.Version = patchHistoryVersion
	}

	b, err := json.MarshalIndent(h, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal patch history: %w", err)
	}
	b = append(b, '\n')

	if err := atomicWriteFile(s.path, b, 0o600); err != nil {
		return fmt.Errorf("atomic write patch history: %w", err)
	}

	return nil
}
