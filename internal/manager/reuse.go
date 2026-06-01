package manager

import (
	"context"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/proc"
)

var (
	reuseProcessAlive           = proc.IsAlive
	reuseLooksLikeProxyDaemonFn = proc.LooksLikeProxyDaemon
)

func FindReusableInstance(instances []config.Instance, profileID string, hc HealthClient) *config.Instance {
	inst, _ := FindReusableInstanceContext(context.Background(), instances, profileID, hc)
	return inst
}

func FindReusableInstanceContext(ctx context.Context, instances []config.Instance, profileID string, hc HealthClient) (*config.Instance, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var best *config.Instance
	for i := range instances {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
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
		if err := hc.CheckHTTPProxyContext(ctx, inst.HTTPPort, inst.ID); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			continue
		}

		if best == nil || inst.LastSeenAt.After(best.LastSeenAt) || best.LastSeenAt.IsZero() {
			copy := *inst
			best = &copy
		}
	}
	return best, nil
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
