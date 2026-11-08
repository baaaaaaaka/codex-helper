package cli

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
)

const watchdogProcessFixture = "CXP_TEST_WATCHDOG_PROCESS_FIXTURE"

type watchdogProcessRecord struct {
	PID                 int
	Exe, Version, Stage string
}

// The child substitutes only health collection. Runtime dispatch and the
// watchdog's restartSelf are real, including Windows' spawn/exit contract.
func TestTeamsServiceWatchdogRuntimeProcessChild(t *testing.T) {
	if os.Getenv(watchdogProcessFixture) != "1" {
		return
	}
	dir := os.Getenv("CXP_TEST_WATCHDOG_PROCESS_DIR")
	record := func(stage string) {
		exe, err := helperpath.RawExecutable()
		if err != nil {
			t.Fatal(err)
		}
		current, _ := helperruntime.Current()
		raw, _ := json.Marshal(watchdogProcessRecord{os.Getpid(), exe, current.Version, stage})
		f, err := os.OpenFile(filepath.Join(dir, "trace.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		_, err = f.Write(append(raw, '\n'))
		closeErr := f.Close()
		if err != nil || closeErr != nil {
			t.Fatalf("trace: %v, %v", err, closeErr)
		}
	}
	record("start")
	if code, handled, err := helperruntime.Launch("v9.9.0", os.Args); handled || err != nil {
		if err != nil {
			t.Fatal(err)
		}
		os.Exit(code)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	teamsServiceWatchdogStatePath = func() (string, error) { return filepath.Join(dir, "state.json"), nil }
	teamsServiceWatchdogCollectSnapshot = func(context.Context, teamsServiceWatchdogOptions) (teamsServiceWatchdogSnapshot, error) {
		record("check")
		if _, err := os.Stat(filepath.Join(dir, "stop")); err == nil {
			cancel()
		}
		return teamsServiceWatchdogSnapshot{}, nil
	}
	err := runTeamsServiceWatchdogLoop(ctx, teamsServiceWatchdogOptions{}, 20*time.Millisecond, true, io.Discard, os.Stderr)
	if err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestTeamsServiceWatchdogRuntimeProcessUpgradeAndRollback(t *testing.T) {
	dir := t.TempDir()
	dir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatal(err)
	}
	name := helperruntime.BinaryName(runtime.GOOS)
	entry := filepath.Join(dir, "bin", name)
	root := filepath.Join(filepath.Dir(entry), ".cxp-runtime")
	testExe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(testExe)
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{entry, helperruntime.VersionPath(root, "v9.9.0", runtime.GOOS), helperruntime.VersionPath(root, "v9.9.1", runtime.GOOS)} {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, raw, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := helperruntime.Activate(root, "v9.9.0"); err != nil {
		t.Fatal(err)
	}
	log, err := os.Create(filepath.Join(dir, "output.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	cmd := exec.Command(entry, "-test.run=^TestTeamsServiceWatchdogRuntimeProcessChild$")
	for _, env := range os.Environ() {
		key, _, _ := strings.Cut(env, "=")
		key = strings.ToUpper(key)
		if !strings.HasPrefix(key, "CXP_RUNTIME") && key != "CXP_ENTRY_PATH" && key != "TMPDIR" && key != "TMP" && key != "TEMP" {
			cmd.Env = append(cmd.Env, env)
		}
	}
	cmd.Env = append(cmd.Env, watchdogProcessFixture+"=1", "CXP_TEST_WATCHDOG_PROCESS_DIR="+dir, "CXP_RUNTIME_FORCE=1", "TMPDIR="+dir, "TMP="+dir, "TEMP="+dir)
	cmd.Stdout, cmd.Stderr = log, log
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	records := func() []watchdogProcessRecord {
		data, _ := os.ReadFile(filepath.Join(dir, "trace.jsonl"))
		var out []watchdogProcessRecord
		for _, line := range strings.Split(string(data), "\n") {
			var r watchdogProcessRecord
			if json.Unmarshal([]byte(line), &r) == nil {
				out = append(out, r)
			}
		}
		return out
	}
	defer func() {
		_ = os.WriteFile(filepath.Join(dir, "stop"), nil, 0o600)
		// Each launcher records its PID before dispatch. Kill only this
		// fixture's verified executables, including Windows launcher parents.
		time.Sleep(150 * time.Millisecond)
		for _, r := range records() {
			prefix, exe := filepath.Join(dir, "bin")+string(os.PathSeparator), r.Exe
			if runtime.GOOS == "windows" {
				prefix, exe = strings.ToLower(prefix), strings.ToLower(exe)
			}
			if r.Stage != "start" || !strings.HasPrefix(exe, prefix) {
				continue
			}
			identity := &teamsServiceLocalSupervisorProcessIdentity{Executable: r.Exe}
			if teamsServiceLocalSupervisorVerifyRecordedIdentity(r.PID, identity, "watchdog test child") == nil {
				if p, err := os.FindProcess(r.PID); err == nil {
					_ = p.Kill()
				}
			}
		}
		_ = cmd.Process.Kill()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("watchdog child did not exit")
		}
		if t.Failed() {
			output, _ := os.ReadFile(log.Name())
			t.Logf("watchdog child: %s", output)
		}
	}()
	start := 0
	for _, version := range []string{"v9.9.0", "v9.9.1", "v9.9.0"} {
		if err := helperruntime.Activate(root, version); err != nil {
			t.Fatal(err)
		}
		deadline := time.Now().Add(15 * time.Second)
		found := false
		for time.Now().Before(deadline) {
			trace := records()
			for i := start; i < len(trace); i++ {
				r := trace[i]
				want := helperruntime.VersionPath(root, version, runtime.GOOS)
				samePath := r.Exe == want || runtime.GOOS == "windows" && strings.EqualFold(r.Exe, want)
				if r.Stage == "check" && r.Version == version && samePath {
					found = true
					start = i + 1
					break
				}
			}
			if found {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if !found {
			t.Fatalf("watchdog did not collect health from the actual %s executable; trace=%+v", version, records())
		}
		from := len(records())
		checks := 0
		deadline = time.Now().Add(5 * time.Second)
		for checks < 3 && time.Now().Before(deadline) {
			trace := records()
			for _, r := range trace[from:] {
				if r.Stage == "check" {
					checks++
					if r.Version != version {
						t.Fatalf("old runtime %s still collected health after entering %s", r.Version, version)
					}
				}
			}
			from = len(trace)
			time.Sleep(20 * time.Millisecond)
		}
		if checks < 3 {
			t.Fatalf("watchdog stopped checking health after entering %s", version)
		}
	}
}
