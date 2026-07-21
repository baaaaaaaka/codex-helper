package cli

import (
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestHealthClientForProxyProfileUsesExplicitRouteTarget(t *testing.T) {
	hc := healthClientForProxyProfile(config.Profile{
		Host:            "ssh-alias",
		Port:            22,
		RouteTargetHost: "api.example.com",
		RouteTargetPort: 443,
	}, time.Second)
	if hc.RouteTarget != "api.example.com:443" {
		t.Fatalf("reuse route target = %q, want api.example.com:443", hc.RouteTarget)
	}
}

func TestHealthClientForProxyProfileDoesNotProbeSSHConfigAliasWithoutRouteTarget(t *testing.T) {
	hc := healthClientForProxyProfile(config.Profile{
		Host:    "pdx-ssh-session",
		Port:    4081,
		User:    "alice",
		SSHArgs: []string{"-F", "/home/example/.ssh/config"},
	}, time.Second)
	if hc.RouteTarget != "" {
		t.Fatalf("reuse route target = %q, want no implicit SSH alias probe", hc.RouteTarget)
	}
}

func TestHealthClientForProxyProfileDoesNotProbeProxyJumpEndpointWithoutRouteTarget(t *testing.T) {
	hc := healthClientForProxyProfile(config.Profile{
		Host:    "target.internal",
		Port:    22,
		User:    "alice",
		SSHArgs: []string{"-J", "jump-alias"},
	}, time.Second)
	if hc.RouteTarget != "" {
		t.Fatalf("reuse route target = %q, want no implicit ProxyJump endpoint probe", hc.RouteTarget)
	}
}

func TestHealthClientForProxyTargetRejectsIncompleteOrInvalidTargets(t *testing.T) {
	tests := []struct {
		name string
		host string
		port int
		want string
	}{
		{name: "missing host", port: 443},
		{name: "missing port", host: "api.example.com"},
		{name: "negative port", host: "api.example.com", port: -1},
		{name: "port too large", host: "api.example.com", port: 65536},
		{name: "blank host", host: "  ", port: 443},
		{name: "valid ipv4", host: "api.example.com", port: 443, want: "api.example.com:443"},
		{name: "valid ipv6", host: "2001:db8::1", port: 443, want: "[2001:db8::1]:443"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hc := healthClientForProxyTarget(time.Second, tt.host, tt.port)
			if hc.RouteTarget != tt.want {
				t.Fatalf("route target = %q, want %q", hc.RouteTarget, tt.want)
			}
		})
	}
}
