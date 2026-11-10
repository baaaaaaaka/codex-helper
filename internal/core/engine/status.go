package engine

import (
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
	err := st.Read(func(tx *store.Tx) error {
		version, _, err := tx.Meta(metaRunningVersion)
		if err != nil {
			return err
		}
		s.RunningVersion = strings.TrimSpace(version)
		installed, _, err := tx.UpdatePending()
		if err != nil {
			return err
		}
		s.UpdateInstalled = installed.Tag
		scheduled, _, err := tx.UpdateRequest()
		if err != nil {
			return err
		}
		s.UpdateScheduled = scheduled.Tag
		_, s.CodexUpgrade, err = tx.CodexRequest()
		return err
	})
	return s, err
}
