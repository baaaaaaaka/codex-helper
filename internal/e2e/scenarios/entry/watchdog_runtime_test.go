package entry

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

// Run the release binaries and the real Unix exec path. Mocking restartSelf
// cannot detect a launcher that accidentally keeps the old runtime markers.
func TestEntryWatchdogRuntimeUpgradeAndRollback(t *testing.T) {
	harness.Parallel(t)
	testWatchdogRuntime(t, watchdogRuntimeCase{
		id: "ENTRY-WATCHDOG-RUNTIME", title: "a running watchdog follows runtime activation and rollback through the stable launcher, preserving its PID",
	})
}

func TestEntryWatchdogRuntimeLegacyLauncher(t *testing.T) {
	harness.Parallel(t)
	testWatchdogRuntime(t, watchdogRuntimeCase{
		id: "ENTRY-WATCHDOG-LEGACY", title: "a watchdog entered by an older launcher without a pinned version follows activation and rollback", legacy: true,
	})
}

type watchdogRuntimeCase struct {
	id, title string
	legacy    bool
}

func testWatchdogRuntime(t *testing.T, tc watchdogRuntimeCase) {
	t.Helper()
	s := harness.NewScenario(t, tc.id, harness.Options{
		Guarantees: []string{"entry-point"}, Title: tc.title,
		Budget: 90 * time.Second, NoStart: true,
	})
	defer s.Finish()
	binDir := filepath.Join(s.Env.Home, "bin")
	entry := filepath.Join(binDir, "cxp")
	root := filepath.Join(binDir, ".cxp-runtime")
	for _, v := range []string{"v9.9.0", "v9.9.1"} {
		built, err := sut.VersionedCLI(v)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(built)
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(root, "versions", v, "cxp")
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0o700); err != nil {
			t.Fatal(err)
		}
		if v == "v9.9.0" {
			if err := os.WriteFile(entry, raw, 0o700); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Set the documented runtime pointer atomically as an installer does.
	// Keep this fixture independent of the product runtime implementation.
	activate := func(version string) error {
		tmp := filepath.Join(root, "active.next")
		if err := os.WriteFile(tmp, []byte(version+"\n"), 0o600); err != nil {
			return err
		}
		return os.Rename(tmp, filepath.Join(root, "active"))
	}
	if err := activate("v9.9.0"); err != nil {
		t.Fatal(err)
	}
	log, err := os.Create(filepath.Join(s.Env.Root, "watchdog.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	launch := entry
	if tc.legacy {
		launch = filepath.Join(root, "versions", "v9.9.0", "cxp")
	}
	cmd := exec.Command(launch, "teams", "service", "watchdog", "--loop", "--interval", "50ms", "--quiet")
	for k, v := range s.Env.Vars {
		if !strings.HasPrefix(k, "CXP_RUNTIME") && k != "CXP_ENTRY_PATH" {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
	}
	cmd.Env = append(cmd.Env, "PATH="+os.Getenv("PATH"), "TMPDIR="+os.Getenv("TMPDIR"), "CXP_RUNTIME_FORCE=1")
	if tc.legacy {
		// The old entry launches the runtime with these three markers,
		// without CXP_RUNTIME_VERSION. Use the real versioned executable.
		cmd.Env = append(cmd.Env, "CXP_RUNTIME=1", "CXP_RUNTIME_ROOT="+root, "CXP_ENTRY_PATH="+entry)
	}
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	pid := cmd.Process.Pid
	for _, v := range []string{"v9.9.0", "v9.9.1", "v9.9.0"} {
		if err := activate(v); err != nil {
			t.Fatal(err)
		}
		want := filepath.Join(root, "versions", v, "cxp")
		seen := ""
		ok := s.Poll(15*time.Second, func() bool {
			seen, _ = os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
			return seen == want
		})
		if !s.Check("the same watchdog process enters "+v, ok, v, filepath.Base(filepath.Dir(seen))) {
			raw, _ := os.ReadFile(log.Name())
			t.Logf("watchdog: %s", raw)
			return
		}
	}
	// Ensure it did not merely enter the runtime and immediately exit/loop.
	time.Sleep(200 * time.Millisecond)
	seen, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	s.Check("the watchdog remains in the rolled-back runtime", err == nil && seen == filepath.Join(root, "versions", "v9.9.0", "cxp"), "v9.9.0", fmt.Sprint(err))
}
