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
	s := harness.NewScenario(t, "ENTRY-WATCHDOG-RUNTIME", harness.Options{
		Guarantees: []string{"entry-point"}, Title: "a running watchdog follows runtime activation and rollback through the stable launcher, preserving its PID",
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
	cmd := exec.Command(entry, "teams", "service", "watchdog", "--loop", "--interval", "50ms", "--quiet")
	for k, v := range s.Env.Vars {
		if !strings.HasPrefix(k, "CXP_RUNTIME") && k != "CXP_ENTRY_PATH" {
			cmd.Env = append(cmd.Env, k+"="+v)
		}
	}
	cmd.Env = append(cmd.Env, "PATH="+os.Getenv("PATH"), "TMPDIR="+os.Getenv("TMPDIR"), "CXP_RUNTIME_FORCE=1")
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
