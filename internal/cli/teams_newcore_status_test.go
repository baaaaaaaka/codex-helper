package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/delegate"
	"github.com/baaaaaaaka/codex-helper/internal/core/migrate"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// `teams service status` reads the new core's state from its store without
// owning it: owner liveness, versions, sessions, requests, unsent messages,
// Workflow mode and the registry machine id.
func TestTeamsNewCoreServiceStatusReadsStore(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CODEX_HELPER_STATE_DIR", filepath.Join(dir, "state"))
	path := filepath.Join(dir, "core.sqlite")
	st, err := corestore.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.Acquire(corestore.Ident{Host: "box", PID: 4242, BootID: "b"}); err != nil {
		t.Fatal(err)
	}
	err = st.Update(func(tx *corestore.Tx) error {
		conv, err := tx.CreateConversation(corestore.NewConversation{Label: "s001", Kind: corestore.KindWork, Channel: "teams", ExtChatID: "c1"})
		if err != nil {
			return err
		}
		if _, err := tx.CreateConversation(corestore.NewConversation{Label: "control", Kind: corestore.KindControl, Channel: "teams", ExtChatID: "c0"}); err != nil {
			return err
		}
		if _, err := tx.AdmitWindow("c1", conv, []corestore.InboundMsg{{ExtMsgID: "m1", ExtTime: 1, Sender: corestore.SenderOwner, Kind: corestore.InPrompt, Body: "hi"}}, "cur"); err != nil {
			return err
		}
		if _, err := tx.Enqueue(conv, []corestore.OutMsg{{DeliveryKey: "notice:1", Kind: corestore.OutNotice, Body: "x"}}); err != nil {
			return err
		}
		for k, v := range map[string]string{
			migrate.MetaWorkflowEnabled: "true",
			delegate.MetaMachineID:      "machine:0011",
			"engine_running_version":    "v9.9.9",
			"engine_update_request":     `{"tag":"v10.0.0"}`,
		} {
			if err := tx.SetMeta(k, v); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	t.Setenv(envTeamsCoreStore, path)
	var out bytes.Buffer
	writeTeamsNewCoreStatus(&out, time.Now())
	got := out.String()
	for _, want := range []string{
		"Teams helper", "Store: " + path,
		"Owner: box pid 4242", "(live)",
		"Running version: v9.9.9",
		"Helper update: v10.0.0 scheduled",
		"Sessions: 1 (1 active, 0 parked)",
		"Requests: 1 queued, 0 running",
		"Unsent messages: 1 queued, 0 sending, 0 unconfirmed",
		"Workflow cards: on",
		"Registry machine: machine:0011",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("status lacks %q:\n%s", want, got)
		}
	}

	ro, err := corestore.OpenWith(path, corestore.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	lines, err := teamsNewCoreStatusLines(ro, teamsStoreView{}, time.Now().Add(5*time.Minute))
	if err != nil || !strings.Contains(strings.Join(lines, "\n"), "(stale)") {
		t.Errorf("stale owner not reported: %v %v", lines, err)
	}
}

// Without a store, status says so instead of creating one.
func TestTeamsNewCoreServiceStatusWithoutStore(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CODEX_HELPER_STATE_DIR", filepath.Join(dir, "state"))
	missing := filepath.Join(dir, "missing", "core.sqlite")
	t.Setenv(envTeamsCoreStore, missing)
	var out bytes.Buffer
	writeTeamsNewCoreStatus(&out, time.Now())
	if !strings.Contains(out.String(), "Store: none yet") {
		t.Errorf("status without a store: %q", out.String())
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Errorf("status created the store: %v", err)
	}
}

// `cxp teams status` reads auth and the store locally.
func TestTeamsStatusCommandReadsStore(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CODEX_HELPER_STATE_DIR", filepath.Join(dir, "state"))
	t.Setenv("HOME", dir)
	t.Setenv(envTeamsCoreStore, filepath.Join(dir, "missing", "core.sqlite"))
	cmd := newRootCmd()
	var out strings.Builder
	cmd.SetArgs([]string{"teams", "status"})
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"Teams helper", "Auth:", "Store: none yet"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("teams status lacks %q:\n%s", want, out.String())
		}
	}
}
