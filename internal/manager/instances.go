package manager

import (
	"errors"
	"fmt"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

// ErrInstanceOwnershipLost means that a daemon attempted to update an
// instance after another daemon had already taken its broker lease.
var ErrInstanceOwnershipLost = errors.New("proxy instance ownership lost")

// DefaultProxyOwnerLease is deliberately longer than the normal heartbeat
// interval. It allows a laptop to spend a short amount of time waking its
// user session without causing a competing starter to fence a live daemon.
const DefaultProxyOwnerLease = 45 * time.Second

// IsProxyOwnerLeaseStale returns true only when the record has missed its
// lease and the recorded process is no longer alive. The process check is
// intentionally part of the predicate: a delayed heartbeat must not permit a
// second daemon to take over a still-running broker.
func IsProxyOwnerLeaseStale(inst config.Instance, now time.Time, lease time.Duration, processAlive func(int) bool) bool {
	if inst.OwnerToken == "" {
		return true
	}
	if processAlive != nil && inst.DaemonPID > 0 && processAlive(inst.DaemonPID) {
		return false
	}
	if lease <= 0 {
		lease = DefaultProxyOwnerLease
	}
	if !inst.OwnerLeaseExpiresAt.IsZero() {
		return !inst.OwnerLeaseExpiresAt.After(now)
	}
	last := inst.OwnerLastSeenAt
	if last.IsZero() {
		last = inst.LastSeenAt
	}
	if last.IsZero() || now.Before(last) {
		return false
	}
	return now.Sub(last) >= lease
}

func RecordInstance(store *config.Store, inst config.Instance) error {
	return store.Update(func(cfg *config.Config) error {
		cfg.UpsertInstance(inst)
		return nil
	})
}

func RemoveInstance(store *config.Store, instanceID string) error {
	return store.Update(func(cfg *config.Config) error {
		cfg.RemoveInstance(instanceID)
		return nil
	})
}

func Heartbeat(store *config.Store, instanceID string, now time.Time) error {
	return store.Update(func(cfg *config.Config) error {
		for i := range cfg.Instances {
			if cfg.Instances[i].ID == instanceID {
				cfg.Instances[i].LastSeenAt = now
				if cfg.Instances[i].OwnerToken != "" {
					if cfg.Instances[i].OwnerAcquiredAt.IsZero() {
						cfg.Instances[i].OwnerAcquiredAt = now
					}
					cfg.Instances[i].OwnerLastSeenAt = now
					cfg.Instances[i].OwnerLeaseExpiresAt = now.Add(DefaultProxyOwnerLease)
				}
				return nil
			}
		}
		return fmt.Errorf("instance %q not found", instanceID)
	})
}

// UpdateOwnedInstance applies an update only when the instance still belongs
// to ownerToken and brokerEpoch. This is the compare-and-swap boundary that
// prevents a stale detached daemon from overwriting a replacement daemon's
// ports, health timestamp, or ownership record.
func UpdateOwnedInstance(store *config.Store, instanceID, ownerToken, brokerEpoch string, fn func(*config.Instance) error) error {
	if store == nil {
		return fmt.Errorf("update owned instance: nil store")
	}
	return store.Update(func(cfg *config.Config) error {
		for i := range cfg.Instances {
			inst := &cfg.Instances[i]
			if inst.ID != instanceID {
				continue
			}
			if ownerToken == "" || brokerEpoch == "" || inst.OwnerToken != ownerToken || inst.BrokerEpoch != brokerEpoch {
				return ErrInstanceOwnershipLost
			}
			if fn != nil {
				return fn(inst)
			}
			return nil
		}
		return fmt.Errorf("instance %q not found", instanceID)
	})
}

func HeartbeatOwned(store *config.Store, instanceID, ownerToken, brokerEpoch string, now time.Time) error {
	return UpdateOwnedInstance(store, instanceID, ownerToken, brokerEpoch, func(inst *config.Instance) error {
		inst.LastSeenAt = now
		if inst.OwnerAcquiredAt.IsZero() {
			inst.OwnerAcquiredAt = now
		}
		inst.OwnerLastSeenAt = now
		inst.OwnerLeaseExpiresAt = now.Add(DefaultProxyOwnerLease)
		return nil
	})
}

// RemoveOwnedInstance removes only the record owned by the supplied lease.
// If a replacement has already taken the lease, the replacement is left
// untouched and ErrInstanceOwnershipLost is returned.
func RemoveOwnedInstance(store *config.Store, instanceID, ownerToken, brokerEpoch string) error {
	if store == nil {
		return fmt.Errorf("remove owned instance: nil store")
	}
	return store.Update(func(cfg *config.Config) error {
		for _, inst := range cfg.Instances {
			if inst.ID != instanceID {
				continue
			}
			if ownerToken == "" || brokerEpoch == "" || inst.OwnerToken != ownerToken || inst.BrokerEpoch != brokerEpoch {
				return ErrInstanceOwnershipLost
			}
			cfg.RemoveInstance(instanceID)
			return nil
		}
		return fmt.Errorf("instance %q not found", instanceID)
	})
}
