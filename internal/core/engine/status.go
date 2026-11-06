package engine

import (
	"encoding/json"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// metaRunningVersion is the helper version of the process that owns the
// store (R-17: status reports the version actually running).
const metaRunningVersion = "engine_running_version"

// StoreStatus is what the engine keeps in the store about the running
// helper and its updates, for `teams service status` (read without owning
// the store).
type StoreStatus struct {
	// RunningVersion is the version of the last process that started on
	// the store.
	RunningVersion string
	// UpdateInstalled is a release installed and waiting for the restart
	// that activates it; UpdateScheduled one waiting for running turns.
	UpdateInstalled, UpdateScheduled string
	// CodexUpgrade is a Codex CLI upgrade waiting for running turns.
	CodexUpgrade bool
}

// ReadStoreStatus reads StoreStatus from a store (read-only use is fine).
func ReadStoreStatus(st *store.Store) (StoreStatus, error) {
	var s StoreStatus
	get := func(key string) (string, error) {
		v, _, err := st.Meta(key)
		return strings.TrimSpace(v), err
	}
	var err error
	if s.RunningVersion, err = get(metaRunningVersion); err != nil {
		return s, err
	}
	tag := func(key string) (string, error) {
		raw, err := get(key)
		if err != nil || raw == "" {
			return "", err
		}
		var r updateRequest
		if json.Unmarshal([]byte(raw), &r) != nil {
			return "", nil
		}
		return r.Tag, nil
	}
	if s.UpdateInstalled, err = tag(metaUpdatePending); err != nil {
		return s, err
	}
	if s.UpdateScheduled, err = tag(metaUpdateRequest); err != nil {
		return s, err
	}
	c, err := get(metaCodexUpgrade)
	s.CodexUpgrade = c != ""
	return s, err
}
