package cli

import (
	"fmt"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/ids"
	"github.com/baaaaaaaka/codex-helper/internal/manager"
)

// proxyBrokerIdentity is persisted with the instance record. BrokerID stays
// stable for the logical instance, while BrokerEpoch and OwnerToken change
// when a new daemon wins ownership.
type proxyBrokerIdentity struct {
	BrokerID    string
	BrokerEpoch string
	OwnerToken  string
}

// claimLegacyProxyInstance upgrades an explicitly invoked legacy daemon to a
// fenced owner. A dead owner may be reclaimed only after both the process and
// the lease are stale; a live process is never fenced merely because its
// heartbeat was delayed.
func claimLegacyProxyInstance(store *config.Store, instanceID string) (proxyBrokerIdentity, error) {
	return claimProxyInstance(store, instanceID, proxyNow(), proxyProcessAlive)
}

func claimProxyInstance(store *config.Store, instanceID string, now time.Time, processAlive func(int) bool) (proxyBrokerIdentity, error) {
	identity, err := newProxyBrokerIdentity(instanceID)
	if err != nil {
		return proxyBrokerIdentity{}, err
	}
	if store == nil {
		return proxyBrokerIdentity{}, fmt.Errorf("claim proxy instance: nil store")
	}
	if err := store.Update(func(cfg *config.Config) error {
		for i := range cfg.Instances {
			inst := &cfg.Instances[i]
			if inst.ID != instanceID {
				continue
			}
			if inst.OwnerToken != "" && !manager.IsProxyOwnerLeaseStale(*inst, now, manager.DefaultProxyOwnerLease, processAlive) {
				return manager.ErrInstanceOwnershipLost
			}
			if inst.BrokerID == "" {
				inst.BrokerID = identity.BrokerID
			}
			inst.BrokerEpoch = identity.BrokerEpoch
			inst.OwnerToken = identity.OwnerToken
			inst.OwnerAcquiredAt = now
			inst.OwnerLastSeenAt = now
			inst.OwnerLeaseExpiresAt = now.Add(manager.DefaultProxyOwnerLease)
			inst.DaemonPID = 0
			// A stale daemon no longer owns its listeners. Do not ask the new
			// owner to bind a port that may have been reused by another process.
			inst.HTTPPort = 0
			inst.SocksPort = 0
			inst.LastSeenAt = now
			return nil
		}
		return fmt.Errorf("instance %q not found", instanceID)
	}); err != nil {
		return proxyBrokerIdentity{}, err
	}
	return identity, nil
}

func newProxyBrokerIdentity(instanceID string) (proxyBrokerIdentity, error) {
	if instanceID == "" {
		return proxyBrokerIdentity{}, fmt.Errorf("proxy instance id is required")
	}
	epoch, err := ids.New()
	if err != nil {
		return proxyBrokerIdentity{}, fmt.Errorf("create proxy broker epoch: %w", err)
	}
	token, err := ids.New()
	if err != nil {
		return proxyBrokerIdentity{}, fmt.Errorf("create proxy owner token: %w", err)
	}
	return proxyBrokerIdentity{
		BrokerID:    instanceID,
		BrokerEpoch: epoch,
		OwnerToken:  token,
	}, nil
}

// ensureProxyInstanceIdentity upgrades a legacy instance record in memory.
// The caller persists the returned record while holding the config store
// lock. Legacy records are deliberately upgraded only when an explicit proxy
// daemon start claims them; normal reuse never silently claims them.
func ensureProxyInstanceIdentity(inst *config.Instance, ownerToken string) (bool, error) {
	if inst == nil || inst.ID == "" {
		return false, fmt.Errorf("proxy instance identity requires an instance id")
	}
	changed := false
	if inst.BrokerID == "" {
		inst.BrokerID = inst.ID
		changed = true
	}
	if inst.BrokerEpoch == "" {
		identity, err := newProxyBrokerIdentity(inst.ID)
		if err != nil {
			return false, err
		}
		inst.BrokerEpoch = identity.BrokerEpoch
		changed = true
	}
	if inst.OwnerToken == "" {
		if ownerToken == "" {
			identity, err := newProxyBrokerIdentity(inst.ID)
			if err != nil {
				return false, err
			}
			ownerToken = identity.OwnerToken
		}
		inst.OwnerToken = ownerToken
		changed = true
	} else if ownerToken != "" && inst.OwnerToken != ownerToken {
		return false, fmt.Errorf("proxy instance %q is owned by another broker epoch", inst.ID)
	}
	return changed, nil
}
