package store

import (
	"context"
	"errors"
	"testing"
)

func TestTeamsRuntimeSafetyStoreLoadPropagatesContextAfterStateLockCI(t *testing.T) {
	st := newTestStore(t)
	if _, _, err := st.CreateSession(context.Background(), SessionContext{
		ID:          "context-probe",
		Status:      SessionStatusActive,
		TeamsChatID: "context-probe-chat",
	}); err != nil {
		t.Fatalf("CreateSession: %v", err)
	}
	if _, err := st.MigrateLargeStateToSQLite(context.Background(), 0); err != nil {
		t.Fatalf("MigrateLargeStateToSQLite: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	prevHook := loadUnlockedTestHook
	loadUnlockedTestHook = cancel
	t.Cleanup(func() {
		loadUnlockedTestHook = prevHook
	})

	if _, err := st.Load(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Load error = %v, want context.Canceled after cancellation immediately following state-lock acquisition", err)
	}
}
