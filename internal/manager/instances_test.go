package manager

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestInstanceRecordHeartbeatRemove(t *testing.T) {
	dir := t.TempDir()
	store, err := config.NewStore(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	inst := config.Instance{
		ID:         "i1",
		ProfileID:  "p1",
		HTTPPort:   8080,
		SocksPort:  1080,
		DaemonPID:  123,
		StartedAt:  time.Now().Add(-1 * time.Minute),
		LastSeenAt: time.Now().Add(-1 * time.Minute),
	}

	if err := RecordInstance(store, inst); err != nil {
		t.Fatalf("RecordInstance: %v", err)
	}

	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Instances) != 1 || cfg.Instances[0].ID != "i1" {
		t.Fatalf("Instances=%#v", cfg.Instances)
	}

	now := time.Now()
	if err := Heartbeat(store, "i1", now); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	cfg, _ = store.Load()
	if !cfg.Instances[0].LastSeenAt.Equal(now) {
		t.Fatalf("LastSeenAt=%s want %s", cfg.Instances[0].LastSeenAt, now)
	}

	if err := RemoveInstance(store, "i1"); err != nil {
		t.Fatalf("RemoveInstance: %v", err)
	}
	cfg, _ = store.Load()
	if len(cfg.Instances) != 0 {
		t.Fatalf("expected empty instances, got %#v", cfg.Instances)
	}
}

func TestOwnedInstanceOpsFenceStaleDaemon(t *testing.T) {
	dir := t.TempDir()
	store, err := config.NewStore(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	inst := config.Instance{
		ID:          "owned-1",
		ProfileID:   "p1",
		OwnerToken:  "owner-a",
		BrokerEpoch: "epoch-a",
		LastSeenAt:  time.Unix(1, 0),
	}
	if err := RecordInstance(store, inst); err != nil {
		t.Fatalf("RecordInstance: %v", err)
	}

	want := time.Unix(2, 0)
	if err := HeartbeatOwned(store, inst.ID, inst.OwnerToken, inst.BrokerEpoch, want); err != nil {
		t.Fatalf("HeartbeatOwned: %v", err)
	}

	if err := UpdateOwnedInstance(store, inst.ID, "owner-stale", inst.BrokerEpoch, func(live *config.Instance) error {
		live.HTTPPort = 9999
		return nil
	}); !errors.Is(err, ErrInstanceOwnershipLost) {
		t.Fatalf("stale UpdateOwnedInstance error = %v, want %v", err, ErrInstanceOwnershipLost)
	}
	if err := RemoveOwnedInstance(store, inst.ID, "owner-stale", inst.BrokerEpoch); !errors.Is(err, ErrInstanceOwnershipLost) {
		t.Fatalf("stale RemoveOwnedInstance error = %v, want %v", err, ErrInstanceOwnershipLost)
	}

	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load after stale operations: %v", err)
	}
	if len(cfg.Instances) != 1 || !cfg.Instances[0].LastSeenAt.Equal(want) || cfg.Instances[0].HTTPPort != 0 {
		t.Fatalf("stale owner changed instance: %+v", cfg.Instances)
	}

	if err := RemoveOwnedInstance(store, inst.ID, inst.OwnerToken, inst.BrokerEpoch); err != nil {
		t.Fatalf("RemoveOwnedInstance current owner: %v", err)
	}
	cfg, err = store.Load()
	if err != nil {
		t.Fatalf("Load after remove: %v", err)
	}
	if len(cfg.Instances) != 0 {
		t.Fatalf("expected owned instance removed, got %+v", cfg.Instances)
	}
}

func TestIsProxyOwnerLeaseStaleRequiresDeadProcessAndExpiredLease(t *testing.T) {
	now := time.Unix(100, 0)
	inst := config.Instance{
		OwnerToken:          "owner",
		DaemonPID:           123,
		OwnerLastSeenAt:     now.Add(-time.Minute),
		OwnerLeaseExpiresAt: now.Add(-time.Second),
	}
	if IsProxyOwnerLeaseStale(inst, now, time.Second, func(int) bool { return true }) {
		t.Fatal("live daemon was considered stale")
	}
	if !IsProxyOwnerLeaseStale(inst, now, time.Second, func(int) bool { return false }) {
		t.Fatal("dead daemon with expired lease was not considered stale")
	}
	inst.OwnerLeaseExpiresAt = now.Add(time.Minute)
	if IsProxyOwnerLeaseStale(inst, now, time.Second, func(int) bool { return false }) {
		t.Fatal("unexpired lease was considered stale")
	}
}

func TestInstanceOpsErrorPaths(t *testing.T) {
	dir := t.TempDir()
	store, err := config.NewStore(filepath.Join(dir, "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	t.Run("Heartbeat missing instance", func(t *testing.T) {
		if err := Heartbeat(store, "missing", time.Now()); err == nil {
			t.Fatalf("expected heartbeat error for missing instance")
		}
	})

	t.Run("RemoveInstance missing id is no-op", func(t *testing.T) {
		inst := config.Instance{ID: "i1", ProfileID: "p1", HTTPPort: 1, SocksPort: 2}
		if err := RecordInstance(store, inst); err != nil {
			t.Fatalf("RecordInstance: %v", err)
		}
		if err := RemoveInstance(store, "missing"); err != nil {
			t.Fatalf("RemoveInstance error: %v", err)
		}
		cfg, err := store.Load()
		if err != nil {
			t.Fatalf("Load: %v", err)
		}
		if len(cfg.Instances) != 1 || cfg.Instances[0].ID != "i1" {
			t.Fatalf("expected instance to remain, got %#v", cfg.Instances)
		}
	})
}

func TestMergeProxyRecoveryBudgetPreservesConcurrentAttemptsAndBlock(t *testing.T) {
	start := time.Unix(100, 0)
	current := config.ProxyRecoveryBudget{
		RestartWindowStartedAt: start,
		RestartAttempts:        4,
		RequestWindowStartedAt: start,
		RequestAttempts:        7,
	}
	proposed := config.ProxyRecoveryBudget{
		RestartWindowStartedAt: start,
		RestartAttempts:        2,
		RequestWindowStartedAt: start,
		RequestAttempts:        9,
		Blocked:                true,
		BlockedAt:              time.Unix(120, 0),
		LastReason:             "request budget exceeded",
	}

	got := MergeProxyRecoveryBudget(current, proposed)
	if got.RestartWindowStartedAt != start || got.RestartAttempts != 4 {
		t.Fatalf("stale restart snapshot erased attempts: %+v", got)
	}
	if got.RequestWindowStartedAt != start || got.RequestAttempts != 9 {
		t.Fatalf("request attempts were not merged by maximum: %+v", got)
	}
	if !got.Blocked || !got.BlockedAt.Equal(proposed.BlockedAt) || got.LastReason != proposed.LastReason {
		t.Fatalf("blocked state was not durably propagated: %+v", got)
	}

	newWindow := config.ProxyRecoveryBudget{
		RestartWindowStartedAt: time.Unix(200, 0),
		RestartAttempts:        1,
	}
	got = MergeProxyRecoveryBudget(got, newWindow)
	if !got.RestartWindowStartedAt.Equal(newWindow.RestartWindowStartedAt) || got.RestartAttempts != 1 {
		t.Fatalf("new restart window did not replace expired window: %+v", got)
	}
	if !got.Blocked {
		t.Fatal("new snapshot cleared durable blocked fence")
	}
}
