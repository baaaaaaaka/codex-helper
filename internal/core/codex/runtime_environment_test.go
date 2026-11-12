package codex

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
)

var runtimeEnvironmentFixtureNames = []string{
	"CXP_RUNTIME", "CXP_RUNTIME_ROOT", "CXP_RUNTIME_VERSION", "CXP_ENTRY_PATH",
	"CXP_RUNTIME_DISABLE", "CXP_RUNTIME_FORCE", "CXP_BOOT_MANAGED",
	"CXP_RUNTIME_PROBE_PROTOCOL", "CXP_BOOT_ADDRESS", "CXP_BOOT_TOKEN",
}

// The additional startup work is a bounded environment pass, without I/O.
func BenchmarkAppServerRuntimeEnvironment(b *testing.B) {
	for _, size := range []int{64, 512} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			env := make([]string, size)
			for i := range env {
				env[i] = fmt.Sprintf("WORKLOAD_%d=value", i)
			}
			for _, name := range runtimeEnvironmentFixtureNames {
				env = append(env, name+"=internal")
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_ = helperruntime.WithoutRuntimeMarkers(env)
			}
		})
	}
}

// The fixture exits before speaking RPC. Observing its actual environment
// proves the contract at the OS process boundary, on both transport paths.
func writeRuntimeEnvironmentFixture(path string) int {
	var values []string
	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		for _, name := range append(runtimeEnvironmentFixtureNames,
			"PATH", "HOME", "HTTPS_PROXY", "CODEX_HELPER_TEAMS_CHILD",
			"CODEX_HELPER_CLI_PATH", "CODEX_HELPER_CLI_DIR", "CXP_ENV_KEEP") {
			if strings.EqualFold(key, name) {
				values = append(values, value)
				break
			}
		}
	}
	b, err := json.Marshal(values)
	if err != nil || os.WriteFile(path, b, 0600) != nil {
		return 2
	}
	return 0
}

func TestStartRemovesRuntimeMarkers(t *testing.T) {
	exe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	for _, shared := range []bool{false, true} {
		transport := "stdio"
		if shared {
			transport = "shared"
		}
		for _, source := range []string{"inherited", "overlay", "configure", "nil-environment", "empty-environment"} {
			t.Run(transport+"/"+source, func(t *testing.T) {
				dir := t.TempDir()
				output := filepath.Join(dir, "环境 snapshot.json")
				keep := map[string]string{
					"PATH": "parent-path", "HOME": dir, "HTTPS_PROXY": "http://proxy.test:1234",
					"CODEX_HELPER_TEAMS_CHILD": "1", "CODEX_HELPER_CLI_PATH": filepath.Join(dir, "cxp entry"),
					"CODEX_HELPER_CLI_DIR": dir, "CXP_ENV_KEEP": "parent",
				}
				for k, v := range keep {
					t.Setenv(k, v)
				}
				for _, k := range runtimeEnvironmentFixtureNames {
					t.Setenv(k, "inherited")
				}
				t.Setenv("CXP_RUNTIME", "1")
				markers := []string{"cxp_runtime=1", "CXP_RUNTIME=1", "Cxp_Runtime_Version=v9.9.9", "cxp_boot_token=not-a-real-token"}
				cfg := Config{Bin: exe, Args: []string{"--test-runtime-environment", output}, Dir: dir}
				if shared {
					cfg.SharedDir = dir
				}
				switch source {
				case "overlay":
					cfg.Env = append(markers, "CXP_ENV_KEEP=overlay")
					keep["CXP_ENV_KEEP"] = "overlay"
				case "configure":
					cfg.Configure = func(cmd *exec.Cmd) error {
						cmd.Env = append(cmd.Env, append(markers, "CXP_ENV_KEEP=configured")...)
						return nil
					}
					keep["CXP_ENV_KEEP"] = "configured"
				case "nil-environment":
					cfg.Configure = func(cmd *exec.Cmd) error { cmd.Env = nil; return nil }
				case "empty-environment":
					cfg.Configure = func(cmd *exec.Cmd) error { cmd.Env = []string{}; return nil }
					for k := range keep {
						keep[k] = ""
					}
				}
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				if srv, err := Start(ctx, cfg); err == nil {
					srv.Kill()
					t.Fatal("environment fixture unexpectedly spoke the app-server protocol")
				}
				b, err := os.ReadFile(output)
				if err != nil {
					t.Fatalf("fixture did not execute: %v", err)
				}
				var values []string
				if err := json.Unmarshal(b, &values); err != nil {
					t.Fatal(err)
				}
				actual := map[string]string{}
				for _, value := range values {
					key, v, _ := strings.Cut(value, "=")
					for _, blocked := range runtimeEnvironmentFixtureNames {
						if strings.EqualFold(key, blocked) {
							t.Errorf("internal marker reached child: %s", key)
						}
					}
					actual[strings.ToUpper(key)] = v
				}
				for k, want := range keep {
					if actual[k] != want {
						t.Errorf("child %s = %q, want %q", k, actual[k], want)
					}
				}
			})
		}
	}
}
