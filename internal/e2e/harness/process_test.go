package harness

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const procChildEnv = "E2E_PROCSUT_CHILD"

// TestProcessSUTChild is the child process of the ProcessSUT tests (it does
// nothing in a normal run).
func TestProcessSUTChild(t *testing.T) {
	mode := os.Getenv(procChildEnv)
	if mode == "" {
		t.Skip("child process only")
	}
	marker := filepath.Join(os.Getenv("E2E_PROCSUT_DIR"), "started-once")
	switch mode {
	case "restart-once":
		if _, err := os.Stat(marker); err != nil {
			_ = os.WriteFile(marker, []byte("1"), 0o600)
			os.Exit(ServiceRestartExitCode)
		}
	case "always-restart":
		os.Exit(ServiceRestartExitCode)
	}
	time.Sleep(time.Minute)
	os.Exit(0)
}

func childSUT(t *testing.T, mode string) *ProcessSUT {
	dir := t.TempDir()
	return NewProcessSUT(SUTConfig{}, ProcessSpec{
		Name: "child", Binary: os.Args[0], Args: []string{"-test.run=^TestProcessSUTChild$"}, Dir: dir,
		Env: func(string) []string {
			return []string{procChildEnv + "=" + mode, "E2E_PROCSUT_DIR=" + dir, "PATH=" + os.Getenv("PATH")}
		},
	})
}

func waitUntil(t *testing.T, d time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return cond()
}

func TestProcessSUTServiceRestart(t *testing.T) {
	s := childSUT(t, "restart-once")
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer s.Crash(context.Background())
	if !waitUntil(t, 10*time.Second, func() bool { return s.Instance() == "child-2" }) {
		t.Fatalf("no service restart: instance %q, output:\n%s", s.Instance(), s.Output())
	}
	time.Sleep(200 * time.Millisecond)
	if !s.Running() || s.ExitErr() != nil || s.ServiceRestarts() != 1 {
		t.Fatalf("running=%v exitErr=%v restarts=%d\n%s", s.Running(), s.ExitErr(), s.ServiceRestarts(), s.Output())
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	if s.Running() {
		t.Fatal("still running after Stop")
	}
}

func TestProcessSUTStopCancelsPendingRestart(t *testing.T) {
	s := childSUT(t, "always-restart")
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !waitUntil(t, 10*time.Second, func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.pending
	}) {
		t.Fatalf("exit 75 not seen:\n%s", s.Output())
	}
	if !s.Running() {
		t.Fatal("a pending service restart must count as running")
	}
	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(ServiceRestartDelay + 500*time.Millisecond)
	if s.Running() || s.Instance() != "child-1" || s.ServiceRestarts() != 0 {
		t.Fatalf("restart not cancelled: running=%v instance=%s restarts=%d\n%s", s.Running(), s.Instance(), s.ServiceRestarts(), s.Output())
	}
}
