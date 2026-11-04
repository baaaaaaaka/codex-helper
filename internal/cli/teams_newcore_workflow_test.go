package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// With the new core selected, `teams workflow enable/disable/status` read
// and write the new store's Workflow configuration (what the helper reads
// at start) and never touch Graph or the old store. A running helper owns
// the store, so the write is refused with the steps to apply it.
func TestTeamsNewCoreWorkflowConfig(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	t.Setenv("CODEX_HELPER_STATE_DIR", filepath.Join(tmp, "state"))
	t.Setenv(envTeamsCore, "new")
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

	// A live helper owns the store: refused, nothing changed.
	st, err := corestore.Open(filepath.Join(tmp, "state", "teams", "core.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Acquire(corestore.Ident{Host: "other-box", PID: 1, BootID: "b"}); err != nil {
		t.Fatal(err)
	}
	if _, err := run("disable"); err == nil || !strings.Contains(err.Error(), "cxp teams service stop") {
		t.Fatalf("disable while running = %v", err)
	}
	_ = st.Release()
	_ = st.Close()
	if url() == "" {
		t.Fatal("refused disable changed the setting")
	}

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
