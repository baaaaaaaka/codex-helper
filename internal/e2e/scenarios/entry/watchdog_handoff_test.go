//go:build linux

package entry

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

// Only the service manager is substituted. The main helper's startup,
// lease, migration, /proc inspection and replacement watchdog are real.
func TestEntryLegacyWatchdogHandoff(t *testing.T) {
	dir := t.TempDir()
	root := filepath.Join(dir, ".cxp-runtime")
	current := filepath.Join(root, "versions", "v9.9.1", "cxp")
	old := filepath.Join(root, "versions", "v0.1.20", "cxp")
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
	copyExe("/bin/sleep", old) // stable executable-identity fixture
	manager := filepath.Join(dir, "systemctl")
	script := `#!/usr/bin/python3
import os,sys,signal,subprocess,json,time
from pathlib import Path
d=Path(__file__).parent
a=sys.argv[1:]
with (d/'calls').open('a') as f: f.write(json.dumps(a)+'\n')
if a[:2] == ['--user','start'] and a[-1]=='codex-helper-teams-watchdog.service':
    binary=d/'.cxp-runtime/versions/v0.1.20/cxp'
    p=subprocess.Popen([str(binary),'3600'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
    (d/'wdpid').write_text(str(p.pid))
    for _ in range(200):
        try:
            if os.readlink(f'/proc/{p.pid}/exe') == str(binary): break
        except FileNotFoundError:
            pass
        time.sleep(0.01)
    else:
        raise RuntimeError('legacy watchdog fixture did not enter its executable')
elif 'show' in a:
    print(os.getppid() if a[-1]=='codex-helper-teams.service' else (d/'wdpid').read_text().strip())
elif 'restart' in a:
    assert a == ['--user','restart','codex-helper-teams-watchdog.service'], a
    os.kill(int((d/'wdpid').read_text()),signal.SIGTERM)
    env=dict(os.environ)
    env['CXP_RUNTIME_DISABLE']='1'
    p=subprocess.Popen([str(d/'.cxp-runtime/versions/v9.9.1/cxp'),'teams','service','watchdog','--loop','--interval','100ms','--quiet'],env=env,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,start_new_session=True)
    (d/'wdpid').write_text(str(p.pid))
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
	s := harness.NewScenario(t, "ENTRY-WATCHDOG-HANDOFF", harness.Options{
		Title:      "startup replaces only a historical watchdog before migrating, preserving the original chats",
		Guarantees: []string{"entry-point", "auto-migrate"}, NoStart: true, Budget: 90 * time.Second,
		UserEnv: userEnv, Factory: sut.Factory(current, sut.FakeCodexBinary()),
		Golden: harness.GoldenOptions{Skip: "chat recovery is asserted explicitly; this scenario also observes OS process identities"},
	})
	defer s.Finish()
	scope, raw, ids := migrationRecoveryFixture(s, 0)
	if out, err := exec.Command(manager, "--user", "start", "codex-helper-teams-watchdog.service").CombinedOutput(); err != nil {
		t.Fatalf("start legacy watchdog fixture: %v: %s", err, out)
	}
	pidfile := filepath.Join(dir, "wdpid")
	defer func() {
		raw, _ := os.ReadFile(pidfile)
		pid, _ := strconv.Atoi(strings.TrimSpace(string(raw)))
		exe, _ := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
		if exe == current || exe == old {
			_ = syscall.Kill(pid, syscall.SIGTERM)
		}
	}()
	s.StartSUT()
	checkMigrationRecovered(s, s.SUT, scope, raw, ids, "watchdog-recovered")
	pidraw, _ := os.ReadFile(pidfile)
	pid, _ := strconv.Atoi(strings.TrimSpace(string(pidraw)))
	exe, _ := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	s.Check("watchdog actually entered the current runtime", exe == current, current, exe)
	calls, _ := os.ReadFile(filepath.Join(dir, "calls"))
	s.Check("one watchdog-only restart", strings.Count(string(calls), `"restart"`) == 1, "one restart", string(calls))
	s.Check("repair did not fail", !strings.Contains(s.SUT.Output(), "watchdog runtime repair failed"), "successful repair", s.OutputTail(1000))
}
