package cli

import (
	"context"

	teamsstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func loadTeamsStoreStateAndClose(ctx context.Context, path string) (state teamsstore.State, err error) {
	st, err := teamsstore.Open(path)
	if err != nil {
		return state, err
	}
	defer func() {
		if closeErr := st.Close(); err == nil && closeErr != nil {
			err = closeErr
		}
	}()
	return st.Load(ctx)
}

func closeTeamsStoreWithPriorError(st *teamsstore.Store, priorErr error) error {
	closeErr := st.Close()
	if priorErr != nil {
		return priorErr
	}
	return closeErr
}

func closeTeamsStoreHandles(handles []teamsStoreHandle) error {
	var firstErr error
	for _, item := range handles {
		if item.Store == nil {
			continue
		}
		if err := item.Store.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
