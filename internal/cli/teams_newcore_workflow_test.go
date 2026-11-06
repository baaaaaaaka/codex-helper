package cli

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// With the new core selected, `teams workflow enable/disable/status` read
// and write the new store's Workflow configuration (what the helper reads
// at start) and never touch Graph or the old store.
func TestTeamsNewCoreWorkflowConfig(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	t.Setenv("CODEX_HELPER_STATE_DIR", filepath.Join(tmp, "state"))
	t.Setenv(envTeamsCoreStore, "")

	secretDir := filepath.Join(tmp, "secret")
	if err := os.MkdirAll(secretDir, 0o700); err != nil {
		t.Fatal(err)
	}
	secret := filepath.Join(secretDir, "webhook-url")
	if err := os.WriteFile(secret, []byte("https://example.invalid/workflows/hook\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) (string, error) {
		return executeRootForTeamsTestAllowError(t, append([]string{"teams", "workflow"}, args...)...)
	}
	url := func() string {
		t.Helper()
		path := filepath.Join(tmp, "state", "teams", "core.sqlite")
		st, err := corestore.OpenWith(path, corestore.Options{ReadOnly: true})
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		u, err := teamsNewCoreWorkflowURL(st, filepath.Dir(path))
		if err != nil {
			t.Fatal(err)
		}
		return u
	}

	if out, err := run("status"); err != nil || !strings.Contains(out, "Teams workflow notifications: disabled") {
		t.Fatalf("status before enable = %q, %v", out, err)
	}
	out, err := run("enable", "--webhook-url-file", secret)
	if err != nil || !strings.Contains(out, "Teams workflow notifications enabled.") || !strings.Contains(out, "configured (ok)") {
		t.Fatalf("enable = %q, %v", out, err)
	}
	if got := url(); got != "https://example.invalid/workflows/hook" {
		t.Fatalf("helper would read webhook %q", got)
	}
	out, err = run("status")
	if err != nil || !strings.Contains(out, "notifications: enabled") || !strings.Contains(out, "Webhook URL file: configured (ok)") ||
		strings.Contains(out, "example.invalid") {
		t.Fatalf("status = %q, %v", out, err)
	}

	// A live helper owns the store: the command hands the change to it,
	// and the helper applies it under its owner fence without a restart
	// (as the old core did). Its notifier reads the setting at each card.
	path := filepath.Join(tmp, "state", "teams", "core.sqlite")
	host, _ := teamsHostname()
	h, err := lease.Acquire(context.Background(), lease.Config{Anchor: path, Self: lease.Local(host, teamsBootID())})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()
	// In place needs a known boot id (without one the lease copies).
	if teamsBootID() != "" && !h.Plan().InPlace {
		t.Fatalf("plan %+v", h.Plan())
	}
	if err := h.Activate(); err != nil {
		t.Fatal(err)
	}
	st, err := corestore.OpenWith(path, corestore.Options{Lease: h})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Acquire(corestore.Ident{Host: host, PID: os.Getpid(), BootID: teamsBootID()}); err != nil {
		t.Fatal(err)
	}
	// The helper publishes its state for read-only commands.
	newTeamsStatusPublisher(st, filepath.Dir(path), h.Gen(), host, io.Discard).publish(time.Now())
	n := &teamsNewCoreNotifier{st: st, path: path, log: io.Discard}
	if !n.Available() {
		t.Fatal("helper notifier off while enabled")
	}
	prevWait := teamsWorkflowApplyWait
	teamsWorkflowApplyWait = 300 * time.Millisecond
	t.Cleanup(func() { teamsWorkflowApplyWait = prevWait })

	out, err = run("disable")
	if err != nil || !strings.Contains(out, "has not picked the change up yet") {
		t.Fatalf("disable while running, helper not polling = %q, %v", out, err)
	}
	if out, _ := run("status"); !strings.Contains(out, "Pending change: disabled") {
		t.Fatalf("status with a pending change = %q", out)
	}
	if n.Available() {
		t.Fatal("pending disable not applied by the helper's notifier")
	}
	if _, err := os.Stat(teamsWorkflowRequestPath(path)); !os.IsNotExist(err) {
		t.Fatalf("request left after apply: %v", err)
	}

	teamsWorkflowApplyWait = 10 * time.Second
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); n.watch(ctx) }()
	out, err = run("enable", "--webhook-url-file", secret)
	cancel()
	<-done
	if err != nil || !strings.Contains(out, "The running Teams helper applied the change.") {
		t.Fatalf("enable while running = %q, %v", out, err)
	}
	if !n.Available() {
		t.Fatal("enable not in effect in the running helper")
	}
	_ = st.Release()
	_ = st.Close()
	_ = h.Release()

	if out, err := run("disable"); err != nil || !strings.Contains(out, "disabled") {
		t.Fatalf("disable = %q, %v", out, err)
	}
	if got := url(); got != "" {
		t.Fatalf("cards still on after disable: %q", got)
	}
	if out, err := run("status"); err != nil || !strings.Contains(out, "notifications: disabled") {
		t.Fatalf("status after disable = %q, %v", out, err)
	}
}
