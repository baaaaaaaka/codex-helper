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
