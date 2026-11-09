//go:build linux

package entry

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

// The service manager reports an intentionally stopped watchdog. The main
// helper's startup, lease, migration and chat recovery remain real; the
// historical process handoff is covered by the dedicated systemd gate.
func TestEntryMigrationPreservesChatWithStoppedWatchdog(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	current := filepath.Join(root, "versions", "v9.9.1", "cxp")
	copyExe := func(src, dst string) {
		t.Helper()
		raw, err := os.ReadFile(src)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(dst, raw, 0700); err != nil {
			t.Fatal(err)
		}
	}
	built, err := sut.VersionedCLI("v9.9.1")
	if err != nil {
		t.Fatal(err)
	}
	copyExe(built, current)
	if err := os.WriteFile(filepath.Join(root, "active"), []byte("v9.9.1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	manager := filepath.Join(dir, "systemctl")
	script := `#!/usr/bin/python3
import os,sys,json
from pathlib import Path
d=Path(__file__).parent
a=sys.argv[1:]
with (d/'calls').open('a') as f: f.write(json.dumps(a)+'\n')
if 'show' in a:
    # Report no watchdog process. The live historical process and its restart
    # are covered by the dedicated real-systemd integration gate.
    print(os.getppid() if a[-1]=='codex-helper-teams.service' else 0)
else:
    raise RuntimeError(f'unexpected service-manager call: {a!r}')
`
	if err := os.WriteFile(manager, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	userEnv := map[string]string{
		"PATH": dir + ":" + os.Getenv("PATH"), "CODEX_HELPER_TEAMS_SERVICE": "1",
		"CODEX_HELPER_TEAMS_LINUX_SERVICE_BACKEND": "systemd",
		"CXP_RUNTIME": "1", "CXP_RUNTIME_ROOT": root, "CXP_RUNTIME_VERSION": "v9.9.1",
		"CXP_ENTRY_PATH": filepath.Join(dir, "cxp"),
	}
	s := harness.NewScenario(t, "ENTRY-MIGRATION-STOPPED-WATCHDOG", harness.Options{
		Title:      "startup preserves the original chats during migration when the watchdog is stopped",
		Guarantees: []string{"entry-point", "auto-migrate"}, NoStart: true, Budget: 90 * time.Second,
		UserEnv: userEnv, Factory: sut.Factory(current, sut.FakeCodexBinary()),
		Golden: harness.GoldenOptions{Skip: "chat recovery is asserted explicitly"},
	})
	defer s.Finish()
	scope, raw, ids := migrationRecoveryFixture(s, 0)
	s.StartSUT()
	checkMigrationRecovered(s, s.SUT, scope, raw, ids, "stopped-watchdog")
	calls, _ := os.ReadFile(filepath.Join(dir, "calls"))
	s.Check("stopped watchdog was not started or restarted", !strings.Contains(string(calls), `"restart"`) && !strings.Contains(string(calls), `"start"`), "no start or restart", string(calls))
	s.Check("repair did not fail", !strings.Contains(s.SUT.Output(), "watchdog runtime repair failed"), "successful repair", s.OutputTail(1000))
}
