package proxysupervisor

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestNativeSupervisorDefinitionLint(t *testing.T) {
	if value := strings.ToLower(strings.TrimSpace(os.Getenv("CODEX_HELPER_PROXY_NATIVE_LINT"))); value != "1" && value != "true" && value != "yes" {
		t.Skip("set CODEX_HELPER_PROXY_NATIVE_LINT=1 to run the native service linter")
	}
	platform := Platform(runtime.GOOS)
	if platform != PlatformLinux && platform != PlatformDarwin && platform != PlatformWindows {
		t.Skipf("native supervisor lint is not defined for %s", runtime.GOOS)
	}
	name, data, err := Render(Spec{
		Platform: platform, Executable: "/bin/true", ConfigPath: filepath.Join(t.TempDir(), "config.json"),
		InstanceID: "native-lint", OwnerToken: "owner", RestartDelay: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	switch platform {
	case PlatformDarwin:
		if _, err := exec.LookPath("plutil"); err != nil {
			t.Fatalf("plutil is required for native macOS lint: %v", err)
		}
		out, err := exec.Command("plutil", "-lint", path).CombinedOutput()
		if err != nil {
			t.Fatalf("plutil -lint: %v\n%s", err, out)
		}
	case PlatformWindows:
		powershell := "powershell.exe"
		if _, err := exec.LookPath(powershell); err != nil {
			t.Skipf("PowerShell not available: %v", err)
		}
		literal := strings.ReplaceAll(path, "'", "''")
		out, err := exec.Command(powershell, "-NoProfile", "-NonInteractive", "-Command", "[xml](Get-Content -Raw -LiteralPath '"+literal+"') | Out-Null").CombinedOutput()
		if err != nil {
			t.Fatalf("PowerShell XML parse: %v\n%s", err, out)
		}
	case PlatformLinux:
		if _, err := exec.LookPath("systemd-analyze"); err != nil {
			t.Skipf("systemd-analyze not available: %v", err)
		}
		out, err := exec.Command("systemd-analyze", "verify", path).CombinedOutput()
		if err != nil {
			if strings.Contains(strings.ToLower(string(out)), "permission denied") {
				t.Skipf("systemd-analyze is installed but not permitted in this runner: %s", out)
			}
			t.Fatalf("systemd-analyze verify: %v\n%s", err, out)
		}
	}
}
