package cli

import (
	"context"
	"fmt"
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

func TestWindowsTeamsPendingHelperActivationTreatsMoveFailuresAsFatal(t *testing.T) {
	for name, script := range map[string]string{
		"service activation": windowsTeamsPendingHelperActivationPowerShell(
			`C:\Users\Alice\AppData\Local\Temp\codex-proxy.pending.exe`,
			`C:\Users\Alice\AppData\Roaming\codex-proxy\codex-proxy.exe`,
			"v1.2.3",
		),
		"process restart": windowsTeamsPendingHelperProcessRestartPowerShell(
			`C:\Users\Alice\AppData\Local\Temp\codex-proxy.pending.exe`,
			`C:\Users\Alice\AppData\Roaming\codex-proxy\codex-proxy.exe`,
			"v1.2.3",
			[]string{"teams", "run"},
		),
	} {
		t.Run(name, func(t *testing.T) {
			for _, want := range []string{
				"Move-Item -Force -LiteralPath $src -Destination $dest -ErrorAction Stop",
				"pending helper still exists after Move-Item",
				"move attempt ' + ($i + 1) + ' failed",
				"formal helper locked by process(es)",
				"Set-Content -LiteralPath $tmp -Encoding UTF8 -ErrorAction Stop",
				"Move-Item -Force -LiteralPath $tmp -Destination $statusPath -ErrorAction Stop",
			} {
				if !strings.Contains(script, want) {
					t.Fatalf("activation script missing fatal-move fragment %q:\n%s", want, script)
				}
			}
		})
	}
}

func TestWindowsTeamsPendingHelperActivationRetiresOldTeamsBlockers(t *testing.T) {
	for name, script := range map[string]string{
		"service activation": windowsTeamsPendingHelperActivationPowerShell(
			`C:\Users\Alice\AppData\Local\Temp\codex-proxy.pending.exe`,
			`C:\Users\Alice\AppData\Roaming\codex-proxy\codex-proxy.exe`,
			"v1.2.3",
		),
		"process restart": windowsTeamsPendingHelperProcessRestartPowerShell(
			`C:\Users\Alice\AppData\Local\Temp\codex-proxy.pending.exe`,
			`C:\Users\Alice\AppData\Roaming\codex-proxy\codex-proxy.exe`,
			"v1.2.3",
			[]string{"teams", "run"},
		),
	} {
		t.Run(name, func(t *testing.T) {
			for _, want := range []string{
				"function Stop-RetirableTeamsHelperBlockers",
				"teams\\s+(run|listen)",
				"teams\\s+service\\s+watchdog",
				"Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop",
				"stopped old Teams helper process pid=",
			} {
				if !strings.Contains(script, want) {
					t.Fatalf("activation script missing blocker-retirement fragment %q:\n%s", want, script)
				}
			}
		})
	}
}

func TestWindowsTeamsPendingHelperActivationSafetyInvariantsStress(t *testing.T) {
	versions := []string{"v0.1.0", "0.1.0-rc.134", "v1.2.3", "2.0.0-beta.1"}
	argSets := [][]string{
		{"teams", "run"},
		{"teams", "run", "--auto-service=false"},
		{"teams", "run", "--registry", `C:\Users\Alice\Teams Registry\registry.json`},
		{"teams", "run", "--label", "quoted ' label"},
	}
	for i := 0; i < 128; i++ {
		pending := fmt.Sprintf(`C:\Users\Alice\AppData\Local\Temp\codex helper stress %03d O'Brien\.codex-proxy_%s_windows_amd64.exe.%d`, i, strings.TrimPrefix(versions[i%len(versions)], "v"), i+1000)
		install := fmt.Sprintf(`C:\Users\Alice\AppData\Roaming\codex helper stress %03d\codex-proxy.exe`, i)
		version := versions[i%len(versions)]
		scripts := map[string]string{
			"service activation": windowsTeamsPendingHelperActivationPowerShell(pending, install, version),
			"process restart":    windowsTeamsPendingHelperProcessRestartPowerShell(pending, install, version, argSets[i%len(argSets)]),
		}
		for name, script := range scripts {
			t.Run(fmt.Sprintf("%s/%03d", name, i), func(t *testing.T) {
				assertWindowsActivationScriptSafetyInvariants(t, script)
			})
		}
	}
}

func assertWindowsActivationScriptSafetyInvariants(t *testing.T, script string) {
	t.Helper()
	commands := windowsPowerShellMoveItemCommands(script)
	if len(commands) < 2 {
		t.Fatalf("activation script Move-Item commands = %#v, want at least source and status moves", commands)
	}
	for _, command := range commands {
		if !strings.Contains(command, "-ErrorAction Stop") {
			t.Fatalf("Move-Item command is not terminating:\n%s\nscript:\n%s", command, script)
		}
	}
	for _, want := range []string{
		"if (Test-Path -LiteralPath $src) { throw 'pending helper still exists after Move-Item' }",
		"if (Test-DestVersion) { $ready=$true } else { $lastErr=$script:lastErr }",
		"Write-Status 'failed' $lastErr",
		"formal helper locked by process(es)",
		"Stop-RetirableTeamsHelperBlockers $procs",
		"Stop-Process -Id $proc.ProcessId -Force -ErrorAction Stop",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("activation script missing safety invariant %q:\n%s", want, script)
		}
	}
	for _, forbidden := range []string{
		"Move-Item -Force -LiteralPath $src -Destination $dest; ",
		"Move-Item -Force -LiteralPath $tmp -Destination $statusPath }",
		"-like ('*' + $want + '*')",
	} {
		if strings.Contains(script, forbidden) {
			t.Fatalf("activation script contains unsafe fragment %q:\n%s", forbidden, script)
		}
	}
}

func windowsPowerShellMoveItemCommands(script string) []string {
	var commands []string
	const marker = "Move-Item "
	rest := script
	for {
		idx := strings.Index(rest, marker)
		if idx < 0 {
			return commands
		}
		command := rest[idx:]
		next := idx + len(marker)
		if end := strings.Index(command, ";"); end >= 0 {
			command = command[:end]
			next = idx + end + 1
		}
		commands = append(commands, strings.TrimSpace(command))
		rest = rest[next:]
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
