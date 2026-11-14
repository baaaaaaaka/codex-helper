package codexagent

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/core/codex"
)

func TestRecoveryMissingRegistryDoesNotProveExecutorStopped(t *testing.T) {
	a := New(Config{PIDFile: filepath.Join(t.TempDir(), "missing.sqlite")})
	defer a.Close()
	if err := a.Prepare(context.Background()); err != nil {
		t.Fatal(err)
	}
	if a.RecoveryExecutorStopped(1) {
		t.Fatal("missing process evidence authorized recovery")
	}
}

func TestRecoveryAttachedOrRunningExecutorBlocksContinuation(t *testing.T) {
	a := New(Config{PIDFile: filepath.Join(t.TempDir(), "managed.sqlite")})
	defer a.Close()
	a.prepared = true
	if !a.RecoveryExecutorStopped(1) {
		t.Fatal("fenced idle executor should permit reconciliation")
	}
	a.attached[1] = 1
	if a.RecoveryExecutorStopped(1) {
		t.Fatal("attached native client was ignored")
	}
	delete(a.attached, 1)
	a.running[1] = &runningTurn{}
	if a.RecoveryExecutorStopped(1) {
		t.Fatal("running native execution was ignored")
	}
	delete(a.running, 1)
	a.prepared = false
	if a.RecoveryExecutorStopped(1) {
		t.Fatal("failed preparation authorized recovery")
	}
}

func TestRecoveryUnknownStatusDoesNotBecomeRetryableFailure(t *testing.T) {
	out := outcome("native", "futureStatus", "", &codex.TurnError{Message: "model at capacity"}, nil)
	if out.Status == "failed" {
		t.Fatal("unknown protocol status became failed")
	}
}
