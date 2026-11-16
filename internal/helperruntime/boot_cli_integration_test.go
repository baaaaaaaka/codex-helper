//go:build cxp_boot_e2e

package helperruntime

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Exercise the installed entry AND production CLI parsing. A fault fixture
// alone cannot distinguish an ordinary profile error from a startup crash.
func TestBootE2ERealCLIErrorNeverChangesConfirmedVersion(t *testing.T) {
	sut := os.Getenv("CXP_TUI_SUT_BINARY")
	if sut == "" {
		sut = filepath.Join(t.TempDir(), BinaryName(runtime.GOOS))
		if out, err := exec.Command("go", "build", "-o", sut, "../../cmd/codex-proxy").CombinedOutput(); err != nil {
			t.Fatalf("build CLI: %v: %s", err, out)
		}
	}
	binary, err := os.ReadFile(sut)
	if err != nil {
		t.Fatal(err)
	}
	c, dir := bootProcessFixture(t)
	for _, path := range []string{c.EntryPath, VersionPath(c.Root, "v2.0.0", runtime.GOOS)} {
		if err := os.WriteFile(path, binary, 0755); err != nil {
			t.Fatal(err)
		}
	}
	want := BootState{Schema: BootProtocol, Generation: 3, Good: "v2.0.0", Previous: "v1.0.0"}
	if err := writeBoot(c.Root, want); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(c.Root, BootFile))
	if err != nil {
		t.Fatal(err)
	}
	cfg := filepath.Join(dir, "config.json")
	badConfig := filepath.Join(dir, "broken.json")
	for path, text := range map[string]string{cfg: `{"proxyEnabled":false,"profiles":[{"id":"existing","name":"existing"}]}`, badConfig: `{broken`} {
		if err := os.WriteFile(path, []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	// Keep an actual schema-10 Teams database throughout the failed command
	// and subsequent service-status read. No host helper or network is used.
	storePath := filepath.Join(dir, "core.sqlite")
	st, err := corestore.Open(storePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Acquire(corestore.Ident{Host: "boot-regression", PID: os.Getpid(), BootID: "isolated"}); err != nil {
		t.Fatal(err)
	}
	if err := st.Update(func(tx *corestore.Tx) error { return tx.SetMeta("engine_running_version", "v2.0.0-regression") }); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) ([]byte, error) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		cmd := bootCommand(t, c, dir, "normal", args...)
		// CommandContext retains the fixture's isolated environment below.
		isolated := exec.CommandContext(ctx, cmd.Path, cmd.Args[1:]...)
		isolated.Dir, isolated.WaitDelay = dir, time.Second
		isolated.Env = append(cmd.Env, "HOME="+dir, "USERPROFILE="+dir,
			"XDG_CONFIG_HOME="+filepath.Join(dir, "config"), "XDG_CACHE_HOME="+filepath.Join(dir, "cache"),
			"CODEX_HOME="+filepath.Join(dir, "codex"), "CODEX_DIR="+filepath.Join(dir, "codex"),
			"CODEX_HELPER_STATE_DIR="+filepath.Join(dir, "state"), "CODEX_HELPER_TEAMS_CORE_STORE="+storePath,
			"CODEX_PROXY_SKIP_BUILTIN_SKILLS=1", "HTTP_PROXY=http://127.0.0.1:1", "HTTPS_PROXY=http://127.0.0.1:1")
		return isolated.CombinedOutput()
	}
	version, err := run("--version")
	if err != nil {
		t.Fatalf("initial version: %v: %s", err, version)
	}
	for _, tc := range []struct {
		name    string
		args    []string
		message string
	}{
		{"version-profile", []string{"--config", cfg, "version"}, `profile "version" not found`},
		{"explicit-profile", []string{"--config", cfg, "tui", "--profile", "does-not-exist"}, `profile "does-not-exist" not found`},
		{"invalid-flag", []string{"--not-a-real-option"}, "unknown flag"},
		{"invalid-config", []string{"--config", badConfig}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			out, runErr := run(tc.args...)
			after, err := os.ReadFile(filepath.Join(c.Root, BootFile))
			if err != nil || !bytes.Equal(before, after) {
				t.Fatalf("ordinary CLI error rewrote boot state: %v; output: %s", err, out)
			}
			if runErr == nil || !strings.Contains(string(out), tc.message) {
				t.Fatalf("expected CLI error %q: %v: %s", tc.message, runErr, out)
			}
			if tc.message != "" && strings.Count(string(out), tc.message) != 1 {
				t.Fatalf("command replayed: %s", out)
			}
			if events := bootEvents(t, dir); events != "" {
				t.Fatalf("executed retained previous runtime: %s", events)
			}
			if next, err := run("--version"); err != nil || !bytes.Equal(next, version) {
				t.Fatalf("next invocation changed version: %v: %s", err, next)
			}
			// Read the business store without requiring an installed OS service.
			if out, err := run("--config", cfg, "teams", "status"); err != nil || !strings.Contains(string(out), storePath) || !strings.Contains(string(out), "Running version: v2.0.0-regression") {
				t.Fatalf("subsequent Teams store read failed: %v: %s", err, out)
			}
		})
	}
	// Exercise runMain's error handling too: a missing/invalid Good must not
	// fall through to the full CLI embedded in the stable entry.
	for _, previous := range []string{"v1.0.0", ""} {
		for _, mode := range []string{"missing", "invalid-executable"} {
			t.Run("unavailable/"+previous+"/"+mode, func(t *testing.T) {
				want.Previous = previous
				if err := writeBoot(c.Root, want); err != nil {
					t.Fatal(err)
				}
				path := VersionPath(c.Root, want.Good, runtime.GOOS)
				if mode == "missing" {
					if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
						t.Fatal(err)
					}
				} else if err := os.WriteFile(path, []byte("invalid executable"), 0755); err != nil {
					t.Fatal(err)
				}
				before, err := os.ReadFile(filepath.Join(c.Root, BootFile))
				if err != nil {
					t.Fatal(err)
				}
				out, err := run("--version")
				if err == nil || !strings.Contains(string(out), "Error: runtime startup:") {
					t.Fatalf("failure ran previous/embedded CLI: %v: %s", err, out)
				}
				after, err := os.ReadFile(filepath.Join(c.Root, BootFile))
				if err != nil || !bytes.Equal(before, after) {
					t.Fatalf("failure rewrote boot state: %v", err)
				}
				if events := bootEvents(t, dir); events != "" {
					t.Fatalf("executed previous: %s", events)
				}
			})
		}
	}
}
