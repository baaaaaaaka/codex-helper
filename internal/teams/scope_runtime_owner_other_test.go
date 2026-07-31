//go:build !linux

package teams

import (
	"os"
	"testing"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestRuntimeStoreOwnerExitConfirmedForMissingLocalPID(t *testing.T) {
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatalf("hostname: %v", err)
	}
	owner := teamstore.OwnerMetadata{PID: 1 << 30, Hostname: hostname}
	if !runtimeStoreOwnerExitConfirmed(owner) {
		t.Fatal("missing local PID should confirm that the legacy writer exited")
	}
	owner.PID = os.Getpid()
	if runtimeStoreOwnerExitConfirmed(owner) {
		t.Fatal("live local PID must not confirm writer exit")
	}
	owner.Hostname = "remote-host.invalid"
	if runtimeStoreOwnerExitConfirmed(owner) {
		t.Fatal("remote owner must not confirm writer exit")
	}
}
