package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
	"github.com/baaaaaaaka/codex-helper/internal/core/migrate"
	corestore "github.com/baaaaaaaka/codex-helper/internal/core/store"
)

func addTeamsWorkChat(t *testing.T, st *corestore.Store, label string) {
	t.Helper()
	err := st.Update(func(tx *corestore.Tx) error {
		_, err := tx.CreateConversation(corestore.NewConversation{Label: label, Kind: corestore.KindWork, Channel: "teams", ExtChatID: "chat-" + label})
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func seedTeamsStateStore(t *testing.T, st *corestore.Store) {
	t.Helper()
	err := st.Update(func(tx *corestore.Tx) error {
		if _, err := tx.CreateConversation(corestore.NewConversation{Label: "control", Kind: corestore.KindControl, Channel: "teams",
			ExtChatID: "c0", Title: "Codex control", ExtURL: "https://teams.example.invalid/control"}); err != nil {
			return err
		}
		if err := tx.SetMeta(migrate.MetaWorkflowEnabled, "true"); err != nil {
			return err
		}
		return tx.SetMeta(migrate.MetaWorkflowSecretFile, "webhook-url")
	})
	if err != nil {
		t.Fatal(err)
	}
	addTeamsWorkChat(t, st, "s001")
}

func readTeamsStatusFileForTest(t *testing.T, dir string) teamsPublishedStatus {
	t.Helper()
	p, ok := readTeamsPublishedStatus(dir)
	if !ok {
		t.Fatalf("no status file in %s", dir)
	}
	return p
}

func teamsStatusTextForTest(now time.Time) string {
	var out bytes.Buffer
	writeTeamsCoreStoreStatus(&out, now)
	return out.String()
}

// While a helper holds the store, every read-only command takes the state
// the helper published (and says so when there is none), never the file.
func TestTeamsCommandsReadThePublishedStateWhileAHelperHoldsTheStore(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	t.Setenv("CODEX_HELPER_STATE_DIR", filepath.Join(tmp, "state"))
	dir := filepath.Join(tmp, "store")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "core.sqlite")
	t.Setenv(envTeamsCoreStore, path)

	host, _ := teamsHostname()
	h, err := lease.Acquire(context.Background(), lease.Config{Anchor: path, Self: lease.Local(host, teamsBootID())})
	if err != nil {
		t.Fatal(err)
	}
	defer h.Release()
	if err := h.Activate(); err != nil {
		t.Fatal(err)
	}
	st, err := corestore.OpenWith(path, corestore.Options{Lease: h})
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.Acquire(corestore.Ident{Host: host, PID: os.Getpid(), BootID: teamsBootID()}); err != nil {
		t.Fatal(err)
	}
	seedTeamsStateStore(t, st)

	// Nothing published yet: each command says the helper has not.
	view, err := teamsReadStoreView()
	if err != nil {
		t.Fatal(err)
	}
	if !view.helperHolds() {
		t.Fatalf("lease not seen as held: %+v", view.Info)
	}
	var unpublished errTeamsStateUnpublished
	if _, _, err := teamsReadStoreState(view, time.Now()); !errors.As(err, &unpublished) || !strings.Contains(err.Error(), "has not published") {
		t.Fatalf("state before publish: %v", err)
	}
	if got := teamsStatusTextForTest(time.Now()); !strings.Contains(got, "Store details: the helper on "+host) || !strings.Contains(got, "has not published") ||
		!strings.Contains(got, "(live)") || strings.Contains(got, "Sessions:") {
		t.Fatalf("status before publish:\n%s", got)
	}
	prevWait := teamsServiceControlChatWait
	teamsServiceControlChatWait = 0
	t.Cleanup(func() { teamsServiceControlChatWait = prevWait })
	if _, err := defaultTeamsServiceBootstrapControlChat(context.Background(), nil, false, io.Discard); err == nil || !strings.Contains(err.Error(), "has not published") {
		t.Fatalf("bootstrap before publish: %v", err)
	}
	if _, _, err := teamsNewCoreWorkflowConfig(path); err == nil || !strings.Contains(err.Error(), "has not published") {
		t.Fatalf("workflow config before publish: %v", err)
	}

	pub := newTeamsStatusPublisher(st, dir, h.Gen(), host, io.Discard)
	t0 := time.UnixMilli(1_800_000_000_000)
	pub.publish(t0)
	// A commit after the publish is not seen until the helper publishes
	// again: the commands read the file, not the store.
	addTeamsWorkChat(t, st, "s002")

	got := teamsStatusTextForTest(time.Now())
	for _, want := range []string{"Store: " + path, "(live)", "Sessions: 1 (1 active, 0 parked)", "Requests: 0 queued, 0 running", "Workflow cards: on"} {
		if !strings.Contains(got, want) {
			t.Errorf("status lacks %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "Store details") {
		t.Errorf("status of a live, published helper has a note:\n%s", got)
	}
	c, ok, err := teamsStoredControlChat()
	if err != nil || !ok || c.ChatID != "c0" || c.Title != "Codex control" || c.URL != "https://teams.example.invalid/control" {
		t.Fatalf("control chat = %+v %v %v", c, ok, err)
	}
	res, err := defaultTeamsServiceBootstrapControlChat(context.Background(), nil, false, io.Discard)
	if err != nil || res.URL != "https://teams.example.invalid/control" || res.Topic != "Codex control" || res.ChatID != "c0" {
		t.Fatalf("bootstrap = %+v, %v", res, err)
	}
	enabled, file, err := teamsNewCoreWorkflowConfig(path)
	if err != nil || !enabled || file != filepath.Join(dir, "webhook-url") {
		t.Fatalf("workflow config = %v %q %v", enabled, file, err)
	}
	if n, err := teamsRunningCodexWork(); err != nil || n != 0 {
		t.Fatalf("running work = %d, %v", n, err)
	}

	// The helper publishes again: the commit shows; an unchanged state is
	// not rewritten.
	pub.publish(t0.Add(time.Second))
	if p := readTeamsStatusFileForTest(t, dir); p.State.Sessions != 2 || p.Gen != h.Gen() || p.At != t0.Add(time.Second).UnixMilli() {
		t.Fatalf("after the second publish: %+v", p)
	}
	pub.publish(t0.Add(time.Minute))
	if p := readTeamsStatusFileForTest(t, dir); p.At != t0.Add(time.Second).UnixMilli() {
		t.Fatalf("an unchanged state was rewritten: %+v", p)
	}

	// The helper stops renewing: the state is shown with a note.
	view, _ = teamsReadStoreView()
	s, note, err := teamsReadStoreState(view, time.Now().Add(time.Hour))
	if err != nil || s.Sessions != 2 || !strings.Contains(note, "is not answering") {
		t.Fatalf("stale helper: %+v %q %v", s, note, err)
	}

	// A file of another generation is not this helper's.
	raw, _ := os.ReadFile(teamsStatusFilePath(dir))
	var p teamsPublishedStatus
	_ = json.Unmarshal(raw, &p)
	p.Gen = h.Gen() + 7
	if err := writeTeamsJSONFile(teamsStatusFilePath(dir), p); err != nil {
		t.Fatal(err)
	}
	if _, _, err := teamsReadStoreState(view, time.Now()); !errors.As(err, &unpublished) {
		t.Fatalf("a file of another generation was used: %v", err)
	}
	if _, _, err := teamsReadStoreState(view, time.Now().Add(time.Hour)); err == nil || !strings.Contains(err.Error(), "is not answering") {
		t.Fatalf("stale helper without a state: %v", err)
	}

	// A helper taking over says so.
	claimed := view
	claimed.Info.Head.State = lease.Claimed
	if _, _, err := teamsReadStoreState(claimed, time.Now()); err == nil || !strings.Contains(err.Error(), "taking over") {
		t.Fatalf("claimed: %v", err)
	}

	// The helper stops: the store is read directly, whatever the file says.
	if err := st.Release(); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}
	if err := h.Release(); err != nil {
		t.Fatal(err)
	}
	view, _ = teamsReadStoreView()
	if view.helperHolds() {
		t.Fatal("released lease seen as held")
	}
	s, note, err = teamsReadStoreState(view, time.Now())
	if err != nil || note != "" || s.Sessions != 2 || s.Control == nil || !s.ownerOK {
		t.Fatalf("direct read after release: %+v %q %v", s, note, err)
	}
	if got := teamsStatusTextForTest(time.Now()); !strings.Contains(got, "Owner: none (released by") || !strings.Contains(got, "Sessions: 2 (2 active") ||
		strings.Contains(got, "Store details") {
		t.Fatalf("status after release:\n%s", got)
	}
}

// exitedPIDForTest is the pid of a process that has exited.
func exitedPIDForTest(t *testing.T) int {
	t.Helper()
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Skipf("cannot run a child process: %v", err)
	}
	return cmd.ProcessState.Pid()
}

// A helper of this host that crashed (its pid is gone) does not hold the
// store: the command reads the file directly.
func TestTeamsStoreHeldOnlyWhileTheHelperIsAlive(t *testing.T) {
	host, _ := teamsHostname()
	self := lease.Local(host, teamsBootID())
	if self.Boot == "" {
		t.Skip("no boot id on this platform")
	}
	head := lease.Record{Gen: 3, State: lease.Active, Host: host, Boot: self.Boot, PIDNS: self.PIDNS, PID: self.PID, Start: self.Start, Wall: time.Now().UnixMilli()}
	v := teamsStoreView{Info: lease.Info{Exists: true, Head: head, HeadGen: 3, HeadOK: true}}
	if !v.helperHolds() {
		t.Fatal("this live process not seen as holding")
	}
	v.Info.Head.PID = exitedPIDForTest(t)
	v.Info.Head.Start = ""
	if v.helperHolds() {
		t.Fatal("an exited helper seen as holding")
	}
	v.Info.Head.Host, v.Info.Head.Boot = "other-host", "other-boot"
	if !v.helperHolds() {
		t.Fatal("another host's helper not seen as holding")
	}
	v.Info.Head.State = lease.Released
	if v.helperHolds() {
		t.Fatal("a released lease seen as held")
	}
	v.Info.HeadOK = false
	if !v.helperHolds() {
		t.Fatal("an unreadable head not assumed to hold")
	}
}

// The store belongs to another host and no helper holds it: the command
// never opens it and shows what that host's helper last published.
func TestTeamsRemoteStoreStateUsesThePublishedFile(t *testing.T) {
	dir := t.TempDir()
	anchor := filepath.Join(dir, "core.sqlite")
	head := lease.Record{Gen: 5, State: lease.Active, Host: "other-host", Boot: "b", PID: 77, Wall: time.Now().UnixMilli()}
	v := teamsStoreView{Anchor: anchor, DB: filepath.Join(dir, "core-5.sqlite"), Remote: "other-host",
		Info: lease.Info{Exists: true, Head: head, HeadGen: 5, HeadOK: true, Current: head, CurrentGen: 5}}

	if _, _, err := teamsReadStoreState(v, time.Now()); err == nil || !strings.Contains(err.Error(), "has not published") {
		t.Fatalf("held, nothing published: %v", err)
	}
	if err := writeTeamsJSONFile(teamsStatusFilePath(dir), teamsPublishedStatus{V: 1, Gen: 5, Host: "other-host", PID: 77, At: time.Now().UnixMilli(),
		State: teamsStoreStatus{Sessions: 4, Active: 3, Control: &teamsControlChat{ChatID: "c9", URL: "https://x.invalid/c9"}}}); err != nil {
		t.Fatal(err)
	}
	s, note, err := teamsReadStoreState(v, time.Now())
	if err != nil || note != "" || s.Sessions != 4 || s.Control == nil || s.Control.ChatID != "c9" {
		t.Fatalf("held, published: %+v %q %v", s, note, err)
	}

	v.Info.Head.State = lease.Released
	s, note, err = teamsReadStoreState(v, time.Now())
	if err != nil || s.Sessions != 4 || !strings.Contains(note, "the helper on other-host (pid 77), which is not running now") {
		t.Fatalf("released, published: %+v %q %v", s, note, err)
	}
	if err := os.Remove(teamsStatusFilePath(dir)); err != nil {
		t.Fatal(err)
	}
	var remote errTeamsStoreOnOtherHost
	if _, _, err := teamsReadStoreState(v, time.Now()); !errors.As(err, &remote) {
		t.Fatalf("released, nothing published: %v", err)
	}
	if _, err := os.Stat(v.DB); !os.IsNotExist(err) {
		t.Fatalf("the remote store file was created: %v", err)
	}
}

// The helper's publisher writes after commits only.
func TestTeamsStatusPublisherFollowsCommits(t *testing.T) {
	dir := t.TempDir()
	st, err := corestore.Open(filepath.Join(dir, "core.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.Acquire(corestore.Ident{Host: "box", PID: os.Getpid(), BootID: "b"}); err != nil {
		t.Fatal(err)
	}
	pub := newTeamsStatusPublisher(st, dir, 9, "box", io.Discard)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); pub.run(ctx) }()
	defer func() { cancel(); <-done }()

	waitFor := func(what string, ok func(teamsPublishedStatus) bool) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if p, found := readTeamsPublishedStatus(dir); found && ok(p) {
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Fatalf("publisher never wrote %s", what)
	}
	waitFor("the initial state", func(p teamsPublishedStatus) bool { return p.Gen == 9 && p.Host == "box" && p.State.Sessions == 0 })
	addTeamsWorkChat(t, st, "s001")
	waitFor("the commit", func(p teamsPublishedStatus) bool { return p.State.Sessions == 1 })
	addTeamsWorkChat(t, st, "s002")
	addTeamsWorkChat(t, st, "s003")
	waitFor("a burst of commits", func(p teamsPublishedStatus) bool { return p.State.Sessions == 3 })
}
