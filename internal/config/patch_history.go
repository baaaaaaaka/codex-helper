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

// PatchFailureThreshold is the number of consecutive non-transient startup
// failures required before a patched binary is considered broken and patching
// is skipped. Requiring repetition (rather than latching on the first failure)
// prevents a one-off crash from permanently disabling yolo for a codex version.
const PatchFailureThreshold = 3

// PatchHistoryEntry records a single patch operation.
type PatchHistoryEntry struct {
	Path          string    `json:"path"`
	OrigSHA256    string    `json:"origSha256"`
	PatchedSHA256 string    `json:"patchedSha256"`
	ProxyVersion  string    `json:"proxyVersion"`
	PatchedAt     time.Time `json:"patchedAt"`
	Failed        bool      `json:"failed,omitempty"`
	FailureCount  int       `json:"failureCount,omitempty"`
	FailureReason string    `json:"failureReason,omitempty"`
	// CodexVersion is the codex CLI version string for this binary, recorded
	// when known. KnownGood marks a binary that has both patched and run
	// cleanly at least once — a safe target to fall back to if a later codex
	// build misbehaves. KnownGoodAt is when it was last confirmed good.
	CodexVersion string    `json:"codexVersion,omitempty"`
	KnownGood    bool      `json:"knownGood,omitempty"`
	KnownGoodAt  time.Time `json:"knownGoodAt,omitempty"`
}

// effectiveFailureCount returns the consecutive-failure count, treating a legacy
// entry (Failed=true with no count, written before FailureCount existed) as a
// single failure so it never permanently latches on upgrade.
func effectiveFailureCount(e PatchHistoryEntry) int {
	if e.FailureCount > 0 {
		return e.FailureCount
	}
	if e.Failed {
		return 1
	}
	return 0
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

// IsFailed reports whether this binary has reached the consecutive-failure
// threshold and patching should be skipped. A single (or sub-threshold) failure
// does not latch, so a transient crash cannot permanently disable yolo.
func (s *PatchHistoryStore) IsFailed(path, origSHA256 string) (bool, error) {
	h, err := s.Load()
	if err != nil {
		return false, err
	}
	for _, e := range h.Entries {
		if e.Path == path && e.OrigSHA256 == origSHA256 {
			return effectiveFailureCount(e) >= PatchFailureThreshold, nil
		}
	}
	return false, nil
}

// RecordFailure increments the consecutive-failure count for (path, origSHA256),
// preserving any existing patch metadata. Caller must have already filtered out
// transient (non-binary) failures.
func (s *PatchHistoryStore) RecordFailure(path, origSHA256, proxyVersion, reason string) error {
	return s.Update(func(h *PatchHistory) error {
		for i := range h.Entries {
			if h.Entries[i].Path == path && h.Entries[i].OrigSHA256 == origSHA256 {
				h.Entries[i].FailureCount = effectiveFailureCount(h.Entries[i]) + 1
				h.Entries[i].Failed = true
				h.Entries[i].FailureReason = reason
				h.Entries[i].PatchedAt = time.Now()
				if proxyVersion != "" {
					h.Entries[i].ProxyVersion = proxyVersion
				}
				return nil
			}
		}
		h.Entries = append(h.Entries, PatchHistoryEntry{
			Path:          path,
			OrigSHA256:    origSHA256,
			ProxyVersion:  proxyVersion,
			PatchedAt:     time.Now(),
			Failed:        true,
			FailureCount:  1,
			FailureReason: reason,
		})
		return nil
	})
}

// RecordKnownGood marks (path, origSHA256) as a binary that has patched and run
// cleanly: it clears any failure count and records the codex version (when
// known). Such an entry is a safe fallback target if a later codex build
// misbehaves.
func (s *PatchHistoryStore) RecordKnownGood(path, origSHA256, codexVersion string) error {
	now := time.Now()
	return s.Update(func(h *PatchHistory) error {
		for i := range h.Entries {
			if h.Entries[i].Path == path && h.Entries[i].OrigSHA256 == origSHA256 {
				h.Entries[i].FailureCount = 0
				h.Entries[i].Failed = false
				h.Entries[i].FailureReason = ""
				h.Entries[i].KnownGood = true
				h.Entries[i].KnownGoodAt = now
				if codexVersion != "" {
					h.Entries[i].CodexVersion = codexVersion
				}
				return nil
			}
		}
		h.Entries = append(h.Entries, PatchHistoryEntry{
			Path:         path,
			OrigSHA256:   origSHA256,
			CodexVersion: codexVersion,
			KnownGood:    true,
			KnownGoodAt:  now,
			PatchedAt:    now,
		})
		return nil
	})
}

// LastKnownGood returns the most recently confirmed known-good entry, or false
// if none has been recorded.
func (s *PatchHistoryStore) LastKnownGood() (PatchHistoryEntry, bool, error) {
	h, err := s.Load()
	if err != nil {
		return PatchHistoryEntry{}, false, err
	}
	var best PatchHistoryEntry
	found := false
	for _, e := range h.Entries {
		if !e.KnownGood {
			continue
		}
		if effectiveFailureCount(e) >= PatchFailureThreshold {
			continue
		}
		if !found || e.KnownGoodAt.After(best.KnownGoodAt) {
			best = e
			found = true
		}
	}
	return best, found, nil
}

// ClearFailure resets the consecutive-failure count for (path, origSHA256),
// called after the patched binary runs successfully. Patch metadata is kept.
func (s *PatchHistoryStore) ClearFailure(path, origSHA256 string) error {
	return s.Update(func(h *PatchHistory) error {
		for i := range h.Entries {
			if h.Entries[i].Path == path && h.Entries[i].OrigSHA256 == origSHA256 {
				h.Entries[i].FailureCount = 0
				h.Entries[i].Failed = false
				h.Entries[i].FailureReason = ""
				return nil
			}
		}
		return nil
	})
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
