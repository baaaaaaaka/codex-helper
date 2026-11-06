package cli

import (
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
)

// `teams run --interval d` (F-CLI-3) sets the control, scan and warm
// cadence to d without reading the fastest tier slower or idle tiers
// faster than d.
func TestTeamsPollIntervalTiming(t *testing.T) {
	p := engine.ProductionTiming()
	got := teamsPollIntervalTiming(p, 20*time.Second)
	if got.Control != 20*time.Second || got.ScanInterval != 20*time.Second || got.Warm != 20*time.Second {
		t.Fatalf("cadence: %+v", got)
	}
	if got.Hot != p.Hot || got.Cool != 20*time.Second || got.Cold != 30*time.Second {
		t.Fatalf("tiers: hot %v cool %v cold %v", got.Hot, got.Cool, got.Cold)
	}
	fast := teamsPollIntervalTiming(p, time.Millisecond)
	if fast.Control != p.Tick || fast.Hot != p.Tick {
		t.Fatalf("floor at the tick: %+v", fast)
	}
}

func TestTeamsRunOnceRefusedForService(t *testing.T) {
	cmd := newRootCmd()
	cmd.SetArgs([]string{"teams", "run", "--once", "--managed-service-child"})
	cmd.SetOut(new(nopWriter))
	cmd.SetErr(new(nopWriter))
	if err := cmd.Execute(); err == nil || err.Error() != "--once cannot be used by the Teams service" {
		t.Fatalf("err = %v", err)
	}
	cmd = newRootCmd()
	cmd.SetArgs([]string{"teams", "run", "--interval", "0s"})
	cmd.SetOut(new(nopWriter))
	cmd.SetErr(new(nopWriter))
	if err := cmd.Execute(); err == nil || err.Error() != "--interval must be positive" {
		t.Fatalf("err = %v", err)
	}
}

type nopWriter struct{}

func (*nopWriter) Write(p []byte) (int, error) { return len(p), nil }
