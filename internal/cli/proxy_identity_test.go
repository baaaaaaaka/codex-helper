package cli

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/manager"
)

func TestClaimLegacyProxyInstancePersistsOwnerLease(t *testing.T) {
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	if err := store.Save(config.Config{
		Version:   config.CurrentVersion,
		Instances: []config.Instance{{ID: "legacy-1", ProfileID: "p1"}},
	}); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	identity, err := claimLegacyProxyInstance(store, "legacy-1")
	if err != nil {
		t.Fatalf("claimLegacyProxyInstance: %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("load config: %v", err)
	}
	if len(cfg.Instances) != 1 {
		t.Fatalf("instances = %+v", cfg.Instances)
	}
	got := cfg.Instances[0]
	if got.BrokerID != "legacy-1" || got.BrokerEpoch != identity.BrokerEpoch || got.OwnerToken != identity.OwnerToken {
		t.Fatalf("claimed identity = %+v, want %+v", got, identity)
	}

	if _, err := claimLegacyProxyInstance(store, "legacy-1"); !errors.Is(err, manager.ErrInstanceOwnershipLost) {
		t.Fatalf("second claim error = %v, want %v", err, manager.ErrInstanceOwnershipLost)
	}
}

func TestClaimProxyInstanceReclaimsDeadExpiredOwnerWithoutReusingPorts(t *testing.T) {
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	now := time.Unix(200, 0)
	old := config.Instance{
		ID: "stale-1", ProfileID: "p1", BrokerID: "broker-stable", BrokerEpoch: "epoch-old", OwnerToken: "token-old",
		DaemonPID: 99, HTTPPort: 18080, SocksPort: 19090,
		OwnerLastSeenAt: now.Add(-2 * manager.DefaultProxyOwnerLease), OwnerLeaseExpiresAt: now.Add(-time.Second),
		LastSeenAt: now.Add(-2 * manager.DefaultProxyOwnerLease),
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, Instances: []config.Instance{old}}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	identity, err := claimProxyInstance(store, old.ID, now, func(int) bool { return false })
	if err != nil {
		t.Fatalf("claimProxyInstance: %v", err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := cfg.Instances[0]
	if got.BrokerID != old.BrokerID || got.BrokerEpoch != identity.BrokerEpoch || got.OwnerToken != identity.OwnerToken {
		t.Fatalf("reclaimed identity = %+v, identity=%+v old=%+v", got, identity, old)
	}
	if got.HTTPPort != 0 || got.SocksPort != 0 || got.DaemonPID != 0 {
		t.Fatalf("stale listener details survived takeover: %+v", got)
	}
	if !got.OwnerLeaseExpiresAt.Equal(now.Add(manager.DefaultProxyOwnerLease)) {
		t.Fatalf("lease expiry = %v, want %v", got.OwnerLeaseExpiresAt, now.Add(manager.DefaultProxyOwnerLease))
	}
}

func TestClaimProxyInstanceDoesNotFenceLiveOwner(t *testing.T) {
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	now := time.Unix(300, 0)
	inst := config.Instance{ID: "live-1", OwnerToken: "token-live", OwnerLastSeenAt: now.Add(-time.Hour), OwnerLeaseExpiresAt: now.Add(-time.Hour), DaemonPID: 42}
	if err := store.Save(config.Config{Version: config.CurrentVersion, Instances: []config.Instance{inst}}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, err := claimProxyInstance(store, inst.ID, now, func(pid int) bool { return pid == 42 }); !errors.Is(err, manager.ErrInstanceOwnershipLost) {
		t.Fatalf("claim live owner error = %v, want %v", err, manager.ErrInstanceOwnershipLost)
	}
}
