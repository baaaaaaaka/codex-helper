package teams

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
)

const maxActiveStoreBindingBytes = 16 << 10

// ActiveStoreBinding identifies the canonical store owned by the current
// managed listener generation. It is advisory: consumers must verify every
// field against the store owner before trusting it.
type ActiveStoreBinding struct {
	CanonicalPath   string    `json:"canonical_path"`
	ScopeID         string    `json:"scope_id"`
	PID             int       `json:"pid"`
	StartedAt       time.Time `json:"started_at"`
	LeaseGeneration int64     `json:"lease_generation"`
}

func ActiveStoreBindingPath() (string, error) {
	return appdirs.StatePath("teams", "service", "active-store.json")
}

// LoadActiveStoreBindingReadOnly reads the advisory binding without migration
// fallback or file creation.
func LoadActiveStoreBindingReadOnly() (ActiveStoreBinding, bool, error) {
	path, err := ActiveStoreBindingPath()
	if err != nil {
		return ActiveStoreBinding{}, false, err
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return ActiveStoreBinding{}, false, nil
	}
	if err != nil {
		return ActiveStoreBinding{}, false, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return ActiveStoreBinding{}, false, fmt.Errorf("Teams active-store binding is not a regular file: %s", path)
	}
	if info.Size() <= 0 || info.Size() > maxActiveStoreBindingBytes {
		return ActiveStoreBinding{}, false, fmt.Errorf("Teams active-store binding has invalid size %d", info.Size())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return ActiveStoreBinding{}, false, err
	}
	var binding ActiveStoreBinding
	if err := json.Unmarshal(data, &binding); err != nil {
		return ActiveStoreBinding{}, false, err
	}
	binding.CanonicalPath = filepath.Clean(strings.TrimSpace(binding.CanonicalPath))
	binding.ScopeID = strings.TrimSpace(binding.ScopeID)
	if binding.CanonicalPath == "." || !filepath.IsAbs(binding.CanonicalPath) || binding.ScopeID == "" || binding.PID <= 0 || binding.StartedAt.IsZero() || binding.LeaseGeneration <= 0 {
		return ActiveStoreBinding{}, false, fmt.Errorf("Teams active-store binding is incomplete")
	}
	return binding, true, nil
}

// WriteActiveStoreBinding atomically records a managed listener generation.
// Rewriting an identical generation is a no-op.
func WriteActiveStoreBinding(binding ActiveStoreBinding) error {
	binding.CanonicalPath = filepath.Clean(strings.TrimSpace(binding.CanonicalPath))
	binding.ScopeID = strings.TrimSpace(binding.ScopeID)
	if binding.CanonicalPath == "." || !filepath.IsAbs(binding.CanonicalPath) || binding.ScopeID == "" || binding.PID <= 0 || binding.StartedAt.IsZero() || binding.LeaseGeneration <= 0 {
		return fmt.Errorf("cannot write incomplete Teams active-store binding")
	}
	path, err := ActiveStoreBindingPath()
	if err != nil {
		return err
	}
	data, err := json.MarshalIndent(binding, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	if existing, readErr := os.ReadFile(path); readErr == nil && bytes.Equal(existing, data) {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	return writeActiveStoreBindingAtomic(path, data)
}

func (b *Bridge) writeManagedActiveStoreBinding() {
	child := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_CHILD"))
	if b == nil ||
		b.store == nil ||
		strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_SERVICE")) != "1" ||
		child == "1" ||
		strings.EqualFold(child, "true") ||
		strings.EqualFold(child, "yes") {
		return
	}
	b.ownerMu.Lock()
	owner := b.owner
	b.ownerMu.Unlock()
	binding := ActiveStoreBinding{
		CanonicalPath:   b.store.Path(),
		ScopeID:         b.scope.ID,
		PID:             owner.PID,
		StartedAt:       owner.StartedAt,
		LeaseGeneration: owner.LeaseGeneration,
	}
	if err := WriteActiveStoreBinding(binding); err != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams active-store binding warning: %v\n", err)
	}
}
