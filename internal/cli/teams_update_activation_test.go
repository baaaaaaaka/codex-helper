package cli

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/update"
)

func TestWindowsTeamsPendingHelperProcessRestartRedirectsOutput(t *testing.T) {
	script := windowsTeamsPendingHelperProcessRestartPowerShell(
		`C:\Users\Alice\AppData\Local\Temp\codex-proxy.pending.exe`,
		`C:\Users\Alice\AppData\Roaming\codex-proxy\codex-proxy.exe`,
		"v1.2.3",
		[]string{"teams", "run"},
	)
	for _, want := range []string{
		"Start-Process -FilePath $dest",
		"-WindowStyle Hidden",
		"-RedirectStandardOutput $stdoutLog",
		"-RedirectStandardError $stderrLog",
		"teams-helper-process-restart.stdout.log",
		"teams-helper-process-restart.stderr.log",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("pending helper restart script missing %q:\n%s", want, script)
		}
	}
}

func TestWindowsTeamsPendingHelperActivationChecksExactParsedVersion(t *testing.T) {
	for name, script := range map[string]string{
		"service activation": windowsTeamsPendingHelperActivationPowerShell(
			`C:\Users\Alice\AppData\Local\Temp\codex-proxy.pending.exe`,
			`C:\Users\Alice\AppData\Roaming\codex-proxy\codex-proxy.exe`,
			"v0.1.0",
		),
		"process restart": windowsTeamsPendingHelperProcessRestartPowerShell(
			`C:\Users\Alice\AppData\Local\Temp\codex-proxy.pending.exe`,
			`C:\Users\Alice\AppData\Roaming\codex-proxy\codex-proxy.exe`,
			"v0.1.0",
			[]string{"teams", "run"},
		),
	} {
		t.Run(name, func(t *testing.T) {
			for _, forbidden := range []string{
				"-like ('*' + $want + '*')",
				"-like",
			} {
				if strings.Contains(script, forbidden) {
					t.Fatalf("activation script still uses wildcard version check %q:\n%s", forbidden, script)
				}
			}
			for _, want := range []string{
				"Version-FromText",
				"$actual -ieq $want",
				"formal entry version ' + $actual + ' did not match target",
				"could not parse formal entry version from empty output",
			} {
				if !strings.Contains(script, want) {
					t.Fatalf("activation script missing exact-version fragment %q:\n%s", want, script)
				}
			}
		})
	}
}

func TestTeamsPendingHelperActivationOwnerSidecarMatchesExactTarget(t *testing.T) {
	pendingPath := filepath.Join(t.TempDir(), ".codex-proxy_0.1.0_windows_amd64.exe.123")
	if err := writeTeamsPendingHelperActivationOwner(pendingPath, "v0.1.0"); err != nil {
		t.Fatalf("writeTeamsPendingHelperActivationOwner error: %v", err)
	}
	if !teamsPendingHelperActivationOwnerMatches(pendingPath, "0.1.0") {
		t.Fatal("owner sidecar should match exact final target")
	}
	if teamsPendingHelperActivationOwnerMatches(pendingPath, "0.1.0-rc.133") {
		t.Fatal("owner sidecar must not match rc when target is final")
	}
	if teamsPendingHelperActivationOwnerMatches(filepath.Join(t.TempDir(), ".codex-proxy_0.1.0_windows_amd64.exe.456"), "0.1.0") {
		t.Fatal("missing owner sidecar should not match")
	}
}

func TestDiscoverTeamsPendingHelperActivationRequiresOwnershipWhenTargetUnknown(t *testing.T) {
	lockCLITestHooks(t)
	prevGOOS := teamsServiceGOOS
	prevFind := teamsUpdateFindPendingReplacementsForPlatform
	prevProbe := teamsUpdateProbeBinaryVersion
	prevOwned := teamsUpdatePendingHelperActivationOwned
	t.Cleanup(func() {
		teamsServiceGOOS = prevGOOS
		teamsUpdateFindPendingReplacementsForPlatform = prevFind
		teamsUpdateProbeBinaryVersion = prevProbe
		teamsUpdatePendingHelperActivationOwned = prevOwned
	})
	teamsServiceGOOS = func() string { return "windows" }
	installPath := filepath.Join(t.TempDir(), "codex-proxy.exe")
	pendingPath := filepath.Join(filepath.Dir(installPath), ".codex-proxy_0.1.0_windows_amd64.exe.123")
	owned := false
	teamsUpdateFindPendingReplacementsForPlatform = func(path string, goos string, goarch string) ([]update.PendingReplacement, error) {
		if path != installPath || goos != "windows" {
			t.Fatalf("FindPendingReplacements path/goos = %q/%q, want %q/windows", path, goos, installPath)
		}
		return []update.PendingReplacement{{Path: pendingPath, Version: "0.1.0", ModTime: time.Now()}}, nil
	}
	teamsUpdateProbeBinaryVersion = func(_ context.Context, path string, _ time.Duration) (update.BinaryVersion, error) {
		switch path {
		case installPath:
			return update.BinaryVersion{Version: "0.1.0-rc.133", Output: "codex-proxy version 0.1.0-rc.133"}, nil
		case pendingPath:
			return update.BinaryVersion{Version: "0.1.0", Output: "codex-proxy version 0.1.0"}, nil
		default:
			t.Fatalf("ProbeBinaryVersion path = %q", path)
			return update.BinaryVersion{}, nil
		}
	}
	teamsUpdatePendingHelperActivationOwned = func(path string, version string) bool {
		if path != pendingPath || version != "0.1.0" {
			t.Fatalf("owner check path/version = %q/%q, want pending final", path, version)
		}
		return owned
	}

	if activation, ok, err := discoverTeamsPendingHelperActivation(context.Background(), installPath, ""); err != nil {
		t.Fatalf("discoverTeamsPendingHelperActivation unowned error: %v", err)
	} else if ok {
		t.Fatalf("unowned pending activation discovered unexpectedly: %#v", activation)
	}
	owned = true
	activation, ok, err := discoverTeamsPendingHelperActivation(context.Background(), installPath, "")
	if err != nil {
		t.Fatalf("discoverTeamsPendingHelperActivation owned error: %v", err)
	}
	if !ok || activation.PendingPath != pendingPath || activation.Version != "0.1.0" {
		t.Fatalf("activation = %#v ok=%v, want owned final pending replacement", activation, ok)
	}
}
