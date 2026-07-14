package cli

import (
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/manager"
)

// healthClientForProxyProfile is the single policy point for reusable proxy
// daemons. A local /health response proves only that the HTTP listener exists;
// the profile target is also probed so a dead SSH/SOCKS route is not reused.
func healthClientForProxyProfile(profile config.Profile, timeout time.Duration) manager.HealthClient {
	host := profile.RouteTargetHost
	port := profile.RouteTargetPort
	if strings.TrimSpace(host) == "" {
		host = profile.Host
	}
	if port <= 0 {
		port = profile.Port
	}
	return healthClientForProxyTarget(timeout, host, port)
}

func healthClientForProxyTarget(timeout time.Duration, host string, port int) manager.HealthClient {
	hc := manager.HealthClient{Timeout: timeout}
	if strings.TrimSpace(host) != "" && port > 0 && port <= 65535 {
		hc.RouteTarget = net.JoinHostPort(strings.TrimSpace(host), strconv.Itoa(port))
	}
	return hc
}

func healthClientForProxyInstance(cfg config.Config, inst config.Instance, timeout time.Duration) manager.HealthClient {
	for _, profile := range cfg.Profiles {
		if profile.ID == inst.ProfileID {
			return healthClientForProxyProfile(profile, timeout)
		}
	}
	return manager.HealthClient{Timeout: timeout}
}
