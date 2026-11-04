package fakecodex

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync/atomic"
	"syscall"
	"time"
)

// Turn statuses (app-server v2 wire values).
const (
	StatusInProgress  = "inProgress"
	StatusCompleted   = "completed"
	StatusFailed      = "failed"
	StatusInterrupted = "interrupted"
)

// Thread is one persisted thread.
type Thread struct {
	ID          string  `json:"id"`
	Cwd         string  `json:"cwd"`
	Name        string  `json:"name,omitempty"`
	CreatedAt   int64   `json:"createdAt"`
	UpdatedAt   int64   `json:"updatedAt"`
	RolloutPath string  `json:"rolloutPath,omitempty"`
	ForkedFrom  string  `json:"forkedFromId,omitempty"`
	Turns       []*Turn `json:"turns"`
}

// Turn is one persisted turn. Items use the v2 ThreadItem wire shape.
type Turn struct {
	ID          string           `json:"id"`
	Status      string           `json:"status"`
	OwnerPID    int              `json:"ownerPid,omitempty"`
	Items       []map[string]any `json:"items"`
	Error       *TurnError       `json:"error,omitempty"`
	StartedAt   int64            `json:"startedAt"`
	CompletedAt int64            `json:"completedAt,omitempty"`
}

// TurnError is the v2 TurnError wire shape.
type TurnError struct {
	Message           string  `json:"message"`
	CodexErrorInfo    any     `json:"codexErrorInfo"`
	AdditionalDetails *string `json:"additionalDetails"`
}

// Store persists threads as one JSON file each. Several app-server
// processes may share it (the helper runs one per runner); each thread is
// written only by the process running its turn.
type Store struct {
	Dir string
}

func (s Store) path(id string) string {
	return filepath.Join(s.Dir, "threads", id+".json")
}

// Load reads a thread. Turns left inProgress by a process that no longer
// exists are reported as interrupted (Codex died mid-turn).
func (s Store) Load(id string) (*Thread, error) {
	data, err := os.ReadFile(s.path(id))
	if err != nil {
		return nil, err
	}
	var t Thread
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, fmt.Errorf("decode thread %s: %w", id, err)
	}
	for _, turn := range t.Turns {
		if turn.Status == StatusInProgress && !processAlive(turn.OwnerPID) {
			turn.Status = StatusInterrupted
		}
	}
	return &t, nil
}

// Save writes a thread atomically.
func (s Store) Save(t *Thread) error {
	if err := os.MkdirAll(filepath.Join(s.Dir, "threads"), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(t, "", " ")
	if err != nil {
		return err
	}
	tmp := fmt.Sprintf("%s.%d.%d.tmp", s.path(t.ID), os.Getpid(), tmpSeq.Add(1))
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path(t.ID))
}

var tmpSeq atomic.Int64

// List returns all threads, most recently updated first.
func (s Store) List() ([]*Thread, error) {
	entries, err := os.ReadDir(filepath.Join(s.Dir, "threads"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var out []*Thread
	for _, e := range entries {
		name := e.Name()
		if filepath.Ext(name) != ".json" {
			continue
		}
		t, err := s.Load(name[:len(name)-len(".json")])
		if err != nil {
			continue
		}
		out = append(out, t)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].UpdatedAt > out[j].UpdatedAt })
	return out, nil
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	if pid == os.Getpid() {
		return true
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

// Active reports whether some turn of t is still running in a live process.
func (t *Thread) Active() bool {
	for _, turn := range t.Turns {
		if turn.Status == StatusInProgress {
			return true
		}
	}
	return false
}

// TurnByID returns the turn with id, or nil.
func (t *Thread) TurnByID(id string) *Turn {
	for _, turn := range t.Turns {
		if turn.ID == id {
			return turn
		}
	}
	return nil
}

var idSeq atomic.Int64

// newID returns a UUIDv7-shaped id unique within the process.
func newID() string {
	now := time.Now()
	n := uint64(now.UnixNano()) ^ uint64(os.Getpid())<<20 ^ uint64(idSeq.Add(1))*0x9e3779b97f4a7c15
	ms := uint64(now.UnixMilli())
	return fmt.Sprintf("%08x-%04x-7%03x-8%03x-%012x", ms>>16, ms&0xffff, (n>>52)&0xfff, (n>>40)&0xfff, n&0xffffffffffff)
}
