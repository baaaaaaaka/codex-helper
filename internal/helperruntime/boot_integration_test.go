//go:build cxp_boot_e2e

package helperruntime

// Run only in a disposable Docker/native CI environment. These tests launch
// native child processes using the production dispatcher, lock and IPC code.
import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

var fixtureOnce sync.Once
var fixtureBinary string
var fixtureError error
var fixtureDirectory string

func TestMain(m *testing.M) {
	code := m.Run()
	if fixtureDirectory != "" {
		_ = os.RemoveAll(fixtureDirectory)
	}
	os.Exit(code)
}

func nativeBootFixture(t *testing.T) string {
	t.Helper()
	fixtureOnce.Do(func() {
		if p := os.Getenv("CXP_BOOT_FIXTURE"); p != "" {
			fixtureBinary = p
			return
		}
		dir, err := os.MkdirTemp("", "cxp-boot-fixture-")
		if err != nil {
			fixtureError = err
			return
		}
		fixtureDirectory = dir
		fixtureBinary = filepath.Join(dir, BinaryName(runtime.GOOS))
		cmd := exec.Command("go", "build", "-o", fixtureBinary, "./testdata/bootfixture")
		if out, err := cmd.CombinedOutput(); err != nil {
			fixtureError = fmt.Errorf("build fixture: %w: %s", err, out)
		}
	})
	if fixtureError != nil {
		t.Fatal(fixtureError)
	}
	return fixtureBinary
}

func bootProcessFixture(t *testing.T) (Context, string) {
	t.Helper()
	dir := t.TempDir()
	entry := filepath.Join(dir, BinaryName(runtime.GOOS))
	bytes, err := os.ReadFile(nativeBootFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(entry, bytes, 0755); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(dir, ".cxp-runtime")
	if _, err := InstallVersion(root, entry, "v1.0.0", runtime.GOOS, false); err != nil {
		t.Fatal(err)
	}
	if err := Activate(root, "v1.0.0"); err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(bytes)
	c := Context{Root: root, EntryPath: entry, Version: "v1.0.0", RuntimePath: VersionPath(root, "v1.0.0", runtime.GOOS)}
	if err := StageBootCandidate(context.Background(), c, "v1.0.0", 1, entry, "v2.0.0", hex.EncodeToString(h[:])); err != nil {
		t.Fatal(err)
	}
	return c, dir
}

func bootCommand(t *testing.T, c Context, dir, mode string, args ...string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(c.EntryPath, args...)
	cmd.Env = append(WithoutRuntimeMarkers(os.Environ()), EnvForce+"=1", "CXP_BOOT_TEST_DIR="+dir, "CXP_BOOT_TEST_MODE="+mode, "CXP_AUTO_UPDATE=1", "CODEX_PROXY_VERSION=latest")
	cmd.WaitDelay = time.Second
	return cmd
}

func bootEvents(t *testing.T, dir string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "events"))
	if os.IsNotExist(err) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func awaitBoot(t *testing.T, why string, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timeout: " + why)
}

func TestBootE2EFailedCandidateFallsBackAndNextStartStaysGood(t *testing.T) {
	for _, mode := range []string{"crash", "hang", "descendant"} {
		t.Run(mode, func(t *testing.T) {
			c, dir := bootProcessFixture(t)
			cmd := bootCommand(t, c, dir, mode, "tui")
			if out, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("fallback: %v: %s", err, out)
			}
			events := bootEvents(t, dir)
			if !strings.Contains(events, "input v1.0.0") || strings.Contains(events, "input v2.0.0") {
				t.Fatal(events)
			}
			s, err := ReadBoot(c.Root)
			if err != nil || s.Good != "v1.0.0" || s.Candidate.Attempt == "" {
				t.Fatalf("%+v %v", s, err)
			}
			if out, err := bootCommand(t, c, dir, "normal", "tui").CombinedOutput(); err != nil {
				t.Fatalf("fresh next start: %v %s", err, out)
			}
			events = bootEvents(t, dir)
			if strings.Count(events, "entered v2.0.0") != 1 || strings.Count(events, "input v1.0.0") != 2 {
				t.Fatal(events)
			}
			assertBootDescendantsStopped(t, events)
		})
	}
}

func TestBootE2EProbeBoundsOutputAndKillsDescendants(t *testing.T) {
	fixture := nativeBootFixture(t)
	if err := ProbeProtocol(context.Background(), fixture); err != nil {
		t.Fatal(err)
	}
	if out, err := ProbeExecutable(context.Background(), fixture, []string{"--probe-flood"}, 5*time.Second); err == nil || len(out) != 16<<10 {
		t.Fatalf("probe output length=%d error=%v", len(out), err)
	}
	dir := t.TempDir()
	t.Setenv("CXP_BOOT_TEST_DIR", dir)
	started := time.Now()
	if _, err := ProbeExecutable(context.Background(), fixture, []string{"--probe-descendant"}, 2*time.Second); err == nil || time.Since(started) > 8*time.Second {
		t.Fatalf("probe deadline failed: %v", err)
	}
	events := bootEvents(t, dir)
	if !strings.Contains(events, "descendant v2.0.0") {
		t.Fatal("probe did not exercise descendant ownership", events)
	}
	assertBootCandidatesStopped(t, events)
	assertBootDescendantsStopped(t, events)
}

func TestBootE2EConfirmedStartupFailureUsesPreviousOnThisAndNextStart(t *testing.T) {
	for _, mode := range []string{"crash", "missing"} {
		t.Run(mode, func(t *testing.T) {
			c, dir := bootProcessFixture(t)
			if err := writeBoot(c.Root, BootState{Schema: BootProtocol, Generation: 3, Good: "v2.0.0", Previous: "v1.0.0"}); err != nil {
				t.Fatal(err)
			}
			if mode == "missing" {
				if err := os.Remove(VersionPath(c.Root, "v2.0.0", runtime.GOOS)); err != nil {
					t.Fatal(err)
				}
			}
			for i := 0; i < 2; i++ {
				if out, err := bootCommand(t, c, dir, mode, "tui").CombinedOutput(); err != nil {
					t.Fatalf("start %d: %s %v", i, out, err)
				}
			}
			s, err := ReadBoot(c.Root)
			if err != nil || s.Good != "v1.0.0" || s.Previous != "" || s.Candidate != nil {
				t.Fatalf("%+v %v", s, err)
			}
			events := bootEvents(t, dir)
			if strings.Count(events, "input v1.0.0") != 2 || strings.Count(events, "entered v2.0.0") > 1 {
				t.Fatal(events)
			}
		})
	}
}

func TestBootE2EHelpCannotPromoteAndSuccessfulTUICommits(t *testing.T) {
	c, dir := bootProcessFixture(t)
	if out, err := bootCommand(t, c, dir, "normal", "--version").CombinedOutput(); err != nil || !strings.Contains(string(out), "1.0.0") {
		t.Fatalf("%s %v", out, err)
	}
	s, _ := ReadBoot(c.Root)
	if s.Candidate.Attempt != "" {
		t.Fatal("version consumed trial")
	}
	if out, err := bootCommand(t, c, dir, "normal", "tui").CombinedOutput(); err != nil {
		t.Fatalf("%s %v", out, err)
	}
	s, _ = ReadBoot(c.Root)
	if s.Good != "v2.0.0" || s.Previous != "v1.0.0" || s.Candidate != nil {
		t.Fatal(s)
	}
	if out, err := bootCommand(t, c, dir, "normal", "--version").CombinedOutput(); err != nil || !strings.Contains(string(out), "2.0.0") {
		t.Fatalf("next start %s %v", out, err)
	}
}

func TestBootE2ECancellationAndPostReadyFailureNeverReplay(t *testing.T) {
	for _, mode := range []string{"cancel", "handled-cancel", "post-ready-crash"} {
		t.Run(mode, func(t *testing.T) {
			c, dir := bootProcessFixture(t)
			err := bootCommand(t, c, dir, mode, "tui").Run()
			if err == nil {
				t.Fatal("expected nonzero exit")
			}
			events := bootEvents(t, dir)
			if !strings.Contains(events, "entered v2.0.0") {
				t.Fatalf("candidate was never executed: %s; %v", events, err)
			}
			if strings.Contains(events, "entered v1.0.0") {
				t.Fatalf("user operation replayed: %s", events)
			}
			s, _ := ReadBoot(c.Root)
			want := "v1.0.0"
			if mode == "post-ready-crash" {
				want = "v2.0.0"
			}
			if s.Good != want {
				t.Fatal(s)
			}
		})
	}
}

func TestBootE2EKilledLauncherCannotPoisonNextStart(t *testing.T) {
	for _, mode := range []string{"hang", "descendant"} {
		t.Run(mode, func(t *testing.T) {
			c, dir := bootProcessFixture(t)
			cmd := bootCommand(t, c, dir, mode, "tui")
			if err := cmd.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = cmd.Process.Kill() })
			awaitBoot(t, "candidate entered", func() bool {
				events := bootEvents(t, dir)
				return strings.Contains(events, "entered v2.0.0") && (mode != "descendant" || strings.Contains(events, "descendant v2.0.0"))
			})
			if err := cmd.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = cmd.Wait()
			assertBootCandidatesStopped(t, bootEvents(t, dir))
			assertBootDescendantsStopped(t, bootEvents(t, dir))
			if out, err := bootCommand(t, c, dir, "normal", "tui").CombinedOutput(); err != nil {
				t.Fatalf("%s %v", out, err)
			}
			if strings.Count(bootEvents(t, dir), "entered v2.0.0") != 1 {
				t.Fatal("repeated failed candidate")
			}
		})
	}
}

func TestBootE2EHumanSetupDoesNotTimeoutOrPromote(t *testing.T) {
	c, dir := bootProcessFixture(t)
	cmd := bootCommand(t, c, dir, "setup", "tui")
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill() })
	awaitBoot(t, "setup owns input", func() bool { return strings.Contains(bootEvents(t, dir), "setup v2.0.0") })
	time.Sleep(bootReadyTimeout + 200*time.Millisecond)
	s, _ := ReadBoot(c.Root)
	if s.Good != "v1.0.0" {
		t.Fatal("setup promoted trial")
	}
	if err := os.WriteFile(filepath.Join(dir, "release"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err != nil {
		t.Fatal(err)
	}
	if events := bootEvents(t, dir); strings.Contains(events, "entered v1.0.0") || !strings.Contains(events, "input v2.0.0") {
		t.Fatal(events)
	}
}

func TestBootE2EReadOnlyStateNeverPreventsGoodStartup(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("POSIX directory permissions require a non-root user; commit faults are covered on all OSes")
	}
	c, dir := bootProcessFixture(t)
	before, err := os.ReadFile(filepath.Join(c.Root, BootFile))
	if err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(c.Root, "runtime.lock")
	if err := os.Chmod(lockPath, 0444); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(c.Root, 0555); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(c.Root, 0755); _ = os.Chmod(lockPath, 0600) })
	for i := 0; i < 2; i++ {
		if out, err := bootCommand(t, c, dir, "normal", "tui").CombinedOutput(); err != nil {
			t.Fatalf("read-only startup: %v %s", err, out)
		}
	}
	after, _ := os.ReadFile(filepath.Join(c.Root, BootFile))
	if string(after) != string(before) {
		t.Fatal("read-only startup rewrote selection")
	}
	events := bootEvents(t, dir)
	if strings.Contains(events, "entered v2.0.0") || strings.Count(events, "input v1.0.0") != 2 {
		t.Fatal(events)
	}
}

func TestBootE2EExistingWindowSurvivesNewVersionConfirmation(t *testing.T) {
	c, dir := bootProcessFixture(t)
	old := exec.Command(c.RuntimePath, "tui")
	old.Env = append(bootEnvironment(c.Root, c.EntryPath, "v1.0.0"), "CXP_BOOT_TEST_DIR="+dir, "CXP_BOOT_TEST_MODE=hold")
	if err := old.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- old.Wait() }()
	t.Cleanup(func() { _ = old.Process.Kill() })
	awaitBoot(t, "old window accepts input", func() bool { return strings.Contains(bootEvents(t, dir), "input v1.0.0") })
	if out, err := bootCommand(t, c, dir, "normal", "tui").CombinedOutput(); err != nil {
		t.Fatalf("new window %v %s", err, out)
	}
	s, _ := ReadBoot(c.Root)
	if s.Good != "v2.0.0" {
		t.Fatal(s)
	}
	select {
	case err := <-done:
		t.Fatalf("existing window exited during update: %v", err)
	default:
	}
	if err := os.WriteFile(filepath.Join(dir, "release"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
