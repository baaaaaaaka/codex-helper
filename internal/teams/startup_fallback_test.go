package teams

import (
	"os"
	"path/filepath"
	"testing"
)

func TestTeamsStartupFallbackStopRequested(t *testing.T) {
	t.Setenv(envTeamsStartupFallbackStopFile, "")
	if teamsStartupFallbackStopRequested() {
		t.Fatalf("stop request should be false without a stop file env")
	}

	stopPath := filepath.Join(t.TempDir(), "startup.stop")
	t.Setenv(envTeamsStartupFallbackStopFile, stopPath)
	if teamsStartupFallbackStopRequested() {
		t.Fatalf("stop request should be false before the stop file exists")
	}
	if err := os.WriteFile(stopPath, []byte("stop\n"), 0o600); err != nil {
		t.Fatalf("write stop file: %v", err)
	}
	if !teamsStartupFallbackStopRequested() {
		t.Fatalf("stop request should be true once the stop file exists")
	}
}

func TestTeamsStartupFallbackExitOnStandby(t *testing.T) {
	t.Setenv(envTeamsStartupFallbackExitOnStandby, "")
	if teamsStartupFallbackExitOnStandby() {
		t.Fatalf("exit-on-standby should default to false")
	}
	for _, value := range []string{"1", "true", "yes", "on", " TRUE "} {
		t.Setenv(envTeamsStartupFallbackExitOnStandby, value)
		if !teamsStartupFallbackExitOnStandby() {
			t.Fatalf("exit-on-standby should be true for %q", value)
		}
	}
	for _, value := range []string{"0", "false", "no", "off"} {
		t.Setenv(envTeamsStartupFallbackExitOnStandby, value)
		if teamsStartupFallbackExitOnStandby() {
			t.Fatalf("exit-on-standby should be false for %q", value)
		}
	}
}
