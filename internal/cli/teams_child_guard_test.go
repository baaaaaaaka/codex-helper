package cli

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
)

func TestTeamsCodexChildEnvUsesInstalledEntry(t *testing.T) {
	lockCLITestHooks(t)
	oldExecutable := teamsChildExecutablePath
	t.Cleanup(func() { teamsChildExecutablePath = oldExecutable })
	for _, managed := range []bool{false, true} {
		name := "standalone"
		if managed {
			name = "managed"
		}
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "安装 with spaces")
			entry := filepath.Join(dir, helperruntime.BinaryName(teamsServiceGOOS()))
			root := filepath.Join(dir, ".cxp-runtime")
			immutable := helperruntime.VersionPath(root, "v1.2.3", teamsServiceGOOS())
			for _, p := range []string{entry, immutable} {
				if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(p, []byte("inert fixture"), 0700); err != nil {
					t.Fatal(err)
				}
			}
			teamsChildExecutablePath = func() (string, error) { return entry, nil }
			t.Setenv(helperruntime.EnvRuntime, "")
			if managed {
				teamsChildExecutablePath = func() (string, error) { return immutable, nil }
				t.Setenv(helperruntime.EnvRuntime, "1")
				t.Setenv(helperruntime.EnvRuntimeRoot, root)
				t.Setenv(helperruntime.EnvRuntimeVersion, "v1.2.3")
				t.Setenv(helperruntime.EnvEntryPath, entry)
			}
			env := teamsCodexChildEnv()
			for key, want := range map[string]string{
				envTeamsHelperCLIPath: entry, envTeamsHelperCLIDir: dir, envTeamsCodexChild: "1",
			} {
				if got := envValue(env, key); got != want {
					t.Errorf("%s = %q, want %q", key, got, want)
				}
			}
		})
	}
}

func TestTeamsCodexChildEnvKeepsGuardOnUnresolvedExecutable(t *testing.T) {
	lockCLITestHooks(t)
	oldExecutable := teamsChildExecutablePath
	t.Cleanup(func() { teamsChildExecutablePath = oldExecutable })
	t.Setenv(helperruntime.EnvRuntime, "")
	teamsChildExecutablePath = func() (string, error) { return "", errors.New("no executable") }
	env := teamsCodexChildEnv()
	if envValue(env, envTeamsCodexChild) != "1" || envValue(env, envTeamsCodexParentPID) == "" {
		t.Fatal("Teams child protection was lost")
	}
}

func TestTeamsCodexChildEnvDoesNotFallbackFromMissingManagedEntry(t *testing.T) {
	lockCLITestHooks(t)
	oldExecutable := teamsChildExecutablePath
	t.Cleanup(func() { teamsChildExecutablePath = oldExecutable })
	entry := filepath.Join(t.TempDir(), "removed installation", helperruntime.BinaryName(teamsServiceGOOS()))
	t.Setenv(helperruntime.EnvRuntime, "1")
	t.Setenv(helperruntime.EnvRuntimeRoot, filepath.Join(filepath.Dir(entry), ".cxp-runtime"))
	t.Setenv(helperruntime.EnvRuntimeVersion, "v1.2.3")
	t.Setenv(helperruntime.EnvEntryPath, entry)
	teamsChildExecutablePath = func() (string, error) {
		t.Fatal("managed helper must not resolve an immutable binary as an installation entry")
		return "", nil
	}
	env := teamsCodexChildEnv()
	if got := envValue(env, envTeamsHelperCLIPath); got != entry {
		t.Fatalf("CLI path = %q, want missing owning entry %q", got, entry)
	}
	if envValue(env, envTeamsCodexChild) != "1" || envValue(env, envTeamsCodexParentPID) == "" {
		t.Fatal("Teams child protection was lost")
	}
}
