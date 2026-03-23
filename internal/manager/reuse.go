package manager

import (
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/proc"
)

var (
	reuseProcessAlive           = proc.IsAlive
	reuseLooksLikeProxyDaemonFn = proc.LooksLikeProxyDaemon
)

func FindReusableInstance(instances []config.Instance, profileID string, hc HealthClient) *config.Instance {
	var best *config.Instance
	for i := range instances {
		inst := &instances[i]
		if inst.ProfileID != profileID {
			continue
		}
		if !isReusableDaemon(inst) {
			continue
		}
		if inst.DaemonPID <= 0 || !reuseProcessAlive(inst.DaemonPID) {
			continue
		}
		if err := hc.CheckHTTPProxy(inst.HTTPPort, inst.ID); err != nil {
			continue
		}

		if best == nil || inst.LastSeenAt.After(best.LastSeenAt) || best.LastSeenAt.IsZero() {
			copy := *inst
			best = &copy
		}
	}
	return best
}

func isReusableDaemon(inst *config.Instance) bool {
	if inst == nil {
		return false
	}
	if inst.Kind == config.InstanceKindDaemon {
		return true
	}
	if inst.Kind != "" {
		return false
	}
	ok, err := reuseLooksLikeProxyDaemonFn(inst.DaemonPID)
	return err == nil && ok
}

func IsInstanceStale(inst config.Instance, now time.Time, maxAge time.Duration) bool {
	if maxAge <= 0 {
		return false
	}
	if inst.LastSeenAt.IsZero() {
		return false
	}
	return now.Sub(inst.LastSeenAt) > maxAge
}
