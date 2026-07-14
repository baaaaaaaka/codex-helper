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

// ErrInstanceNotFound lets lifecycle callers distinguish a daemon that
// removed its own record during shutdown from an ownership conflict.
var ErrInstanceNotFound = errors.New("proxy instance not found")

// MergeProxyRecoveryBudget merges a snapshot produced by a daemon with the
// value currently persisted by another lifecycle participant (for example a
// native supervisor). Recovery counters are monotonic within a window; a
// stale snapshot must not erase attempts that were recorded concurrently.
// A newer window represents an intentional expiry/reset and replaces the old
// window. Blocked is a durable one-way fence and is therefore never cleared
// by a stale or partially initialized process.
func MergeProxyRecoveryBudget(current, proposed config.ProxyRecoveryBudget) config.ProxyRecoveryBudget {
	merged := current
	merged.RestartWindowStartedAt, merged.RestartAttempts = mergeRecoveryWindow(
		current.RestartWindowStartedAt,
		current.RestartAttempts,
		proposed.RestartWindowStartedAt,
		proposed.RestartAttempts,
	)
	merged.RequestWindowStartedAt, merged.RequestAttempts = mergeRecoveryWindow(
		current.RequestWindowStartedAt,
		current.RequestAttempts,
		proposed.RequestWindowStartedAt,
		proposed.RequestAttempts,
	)
	if !current.Blocked && proposed.Blocked {
		merged.Blocked = true
		merged.BlockedAt = proposed.BlockedAt
		merged.LastReason = proposed.LastReason
	} else if current.Blocked {
		merged.Blocked = true
		if merged.BlockedAt.IsZero() {
			merged.BlockedAt = proposed.BlockedAt
		}
		if merged.LastReason == "" {
			merged.LastReason = proposed.LastReason
		}
	}
	return merged
}

func mergeRecoveryWindow(currentStart time.Time, currentAttempts int, proposedStart time.Time, proposedAttempts int) (time.Time, int) {
	switch {
	case currentStart.IsZero():
		return proposedStart, proposedAttempts
	case proposedStart.IsZero():
		return currentStart, currentAttempts
	case proposedStart.After(currentStart):
		return proposedStart, proposedAttempts
	case currentStart.After(proposedStart):
		return currentStart, currentAttempts
	case proposedAttempts > currentAttempts:
		return proposedStart, proposedAttempts
	default:
		return currentStart, currentAttempts
	}
}

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
		return fmt.Errorf("%w: %q", ErrInstanceNotFound, instanceID)
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
		return fmt.Errorf("%w: %q", ErrInstanceNotFound, instanceID)
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
		return fmt.Errorf("%w: %q", ErrInstanceNotFound, instanceID)
	})
}
