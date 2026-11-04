package store

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOpenCreatesSchemaAndRefusesForeignVersions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "core.sqlite")
	c := newClock()
	s := openAt(t, path, c)
	var mode string
	mustT(t, s.w.QueryRow(`PRAGMA journal_mode`).Scan(&mode))
	if mode != "wal" {
		t.Fatalf("journal_mode %q", mode)
	}
	for _, p := range []struct {
		pragma string
		want   int64
	}{{"user_version", SchemaVersion}, {"application_id", ApplicationID}, {"foreign_keys", 1}, {"synchronous", 1}, {"auto_vacuum", 2}, {"wal_autocheckpoint", 1000}} {
		var v int64
		mustT(t, s.w.QueryRow(`PRAGMA `+p.pragma).Scan(&v))
		if v != p.want {
			t.Errorf("PRAGMA %s = %d, want %d", p.pragma, v, p.want)
		}
	}
	for _, tbl := range Tables {
		if n := countWhere(t, s, `SELECT count(*) FROM sqlite_schema WHERE type='table' AND name=?`, tbl); n != 1 {
			t.Errorf("table %s missing", tbl)
		}
	}
	if n := countWhere(t, s, `SELECT count(*) FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%'`); n != len(Tables) {
		t.Errorf("tables = %d, want %d", n, len(Tables))
	}
	if n := countWhere(t, s, `SELECT count(*) FROM sqlite_schema WHERE type='trigger'`); n != 0 {
		t.Errorf("triggers = %d", n)
	}
	s.Close()

	// Reopen is a no-op.
	s2, err := OpenWith(path, Options{Now: c.now})
	mustT(t, err)
	s2.Close()

	// A newer schema is refused, by writers and readers.
	s3, err := OpenWith(path, Options{Now: c.now})
	mustT(t, err)
	_, err = s3.w.Exec(`PRAGMA user_version = 2`)
	mustT(t, err)
	s3.Close()
	_, err = OpenWith(path, Options{Now: c.now})
	wantErr(t, err, ErrUnknownVersion)
	_, err = OpenWith(path, Options{Now: c.now, ReadOnly: true})
	wantErr(t, err, ErrUnknownVersion)

	// A foreign SQLite file is refused.
	other := filepath.Join(dir, "other.sqlite")
	o, err := OpenWith(other, Options{Now: c.now})
	mustT(t, err)
	_, err = o.w.Exec(`PRAGMA application_id = 7`)
	mustT(t, err)
	o.Close()
	_, err = OpenWith(other, Options{Now: c.now})
	wantErr(t, err, ErrUnknownVersion)

	// A read-only open of a missing file fails without creating it.
	_, err = OpenWith(filepath.Join(dir, "missing.sqlite"), Options{ReadOnly: true})
	if err == nil {
		t.Fatal("read-only open of a missing file succeeded")
	}
	if _, err := os.Stat(filepath.Join(dir, "missing.sqlite")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("read-only open created the file")
	}
}

func TestOwnerAcquireRules(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "core.sqlite")
	c := newClock()
	a := openAt(t, path, c)
	wantErr(t, a.Update(func(*Tx) error { return nil }), ErrNotAcquired)
	g1, err := a.Acquire(testIdent(100))
	mustT(t, err)
	if g1 != 1 {
		t.Fatalf("first generation %d", g1)
	}

	b := openAt(t, path, c)
	// A fresh heartbeat of a live holder blocks.
	_, err = b.Acquire(Ident{Host: "h1", InstallHost: "h1", PID: 200, BootID: "boot-1"})
	wantErr(t, err, ErrOwnerLive)
	// Another host is blocked too while fresh.
	_, err = b.Acquire(Ident{Host: "h2", PID: 200, BootID: "boot-9"})
	wantErr(t, err, ErrOwnerLive)
	// A service installed for another host never acquires.
	_, err = b.Acquire(Ident{Host: "h2", InstallHost: "h1", PID: 200, BootID: "boot-9"})
	wantErr(t, err, ErrWrongHost)

	// Same host and boot with a dead holder pid: immediate takeover.
	b.pid = func(pid int) bool { return pid != 100 }
	g2, err := b.Acquire(Ident{Host: "h1", InstallHost: "h1", PID: 200, BootID: "boot-1"})
	mustT(t, err)
	if g2 != 2 {
		t.Fatalf("takeover generation %d", g2)
	}
	wantErr(t, a.Update(func(*Tx) error { return nil }), ErrFenced)
	wantErr(t, a.Heartbeat(), ErrFenced)

	// Stale heartbeat: anyone may take over.
	c.add(OwnerStaleAfter + time.Second)
	cc := openAt(t, path, c)
	g3, err := cc.Acquire(Ident{Host: "h2", PID: 300, BootID: "boot-9"})
	mustT(t, err)
	if g3 != 3 {
		t.Fatalf("stale takeover generation %d", g3)
	}
	wantErr(t, b.Update(func(*Tx) error { return nil }), ErrFenced)
	mustT(t, cc.Update(func(*Tx) error { return nil }))

	// Release lets the next process in immediately.
	mustT(t, cc.Release())
	d := openAt(t, path, c)
	_, err = d.Acquire(Ident{Host: "h3", PID: 400, BootID: "boot-3"})
	mustT(t, err)

	// Kill switch: the running owner's next heartbeat reports it (and does
	// not renew), and no host acquires while the file exists.
	mustT(t, d.Heartbeat())
	before, _, err := d.OwnerInfo()
	mustT(t, err)
	mustT(t, os.WriteFile(filepath.Join(dir, DisabledFile), nil, 0o600))
	wantErr(t, d.Heartbeat(), ErrDisabled)
	after, _, err := d.OwnerInfo()
	mustT(t, err)
	if after.HeartbeatAt != before.HeartbeatAt {
		t.Fatalf("heartbeat renewed while disabled: %d -> %d", before.HeartbeatAt, after.HeartbeatAt)
	}
	mustT(t, d.Release())
	_, err = d.Acquire(Ident{Host: "h3", PID: 400, BootID: "boot-3"})
	wantErr(t, err, ErrDisabled)
	// Removing the file (un-kill) lets the next start in.
	mustT(t, os.Remove(filepath.Join(dir, DisabledFile)))
	_, err = d.Acquire(Ident{Host: "h3", PID: 400, BootID: "boot-3"})
	mustT(t, err)
	mustT(t, d.Heartbeat())
}

// TestFenceAcrossProcesses: a second OS process acquires the store; every
// write of the first one then fails with ErrFenced.
func TestFenceAcrossProcesses(t *testing.T) {
	if os.Getenv("CXP_STORE_HELPER") != "" {
		return
	}
	path := filepath.Join(t.TempDir(), "core.sqlite")
	s, err := OpenWith(path, Options{})
	mustT(t, err)
	defer s.Close()
	_, err = s.Acquire(Ident{Host: "h1", PID: os.Getpid(), BootID: "b"})
	mustT(t, err)
	var conv int64
	mustT(t, s.Update(func(tx *Tx) error {
		conv, err = tx.CreateConversation(NewConversation{Label: "w", Kind: KindWork, Channel: "teams", ExtChatID: "c"})
		return err
	}))

	// The child pushes its clock past the stale window and acquires.
	runHelper(t, path, "acquire", "120s")
	err = s.Update(func(tx *Tx) error { _, err := tx.Enqueue(conv, []OutMsg{out("k", OutNotice)}); return err })
	wantErr(t, err, ErrFenced)
	wantErr(t, s.Heartbeat(), ErrFenced)
	if n := countWhere(t, s, `SELECT count(*) FROM outbox`); n != 0 {
		t.Fatalf("fenced write committed %d rows", n)
	}
	o, _, err := s.OwnerInfo()
	mustT(t, err)
	if o.Generation != 2 || o.PID == os.Getpid() {
		t.Fatalf("owner after child acquire: %+v", o)
	}

	// A child that does not wait out the heartbeat is refused while we are
	// fresh (after we re-acquire past the child's heartbeat).
	c2, err := OpenWith(path, Options{Now: func() time.Time { return time.Now().Add(4 * time.Minute) }})
	mustT(t, err)
	defer c2.Close()
	_, err = c2.Acquire(Ident{Host: "h1", PID: os.Getpid(), BootID: "b"})
	mustT(t, err)
	out := runHelperErr(t, path, "acquire", "4m")
	if !strings.Contains(out, ErrOwnerLive.Error()) {
		t.Fatalf("child acquire output %q, want owner-live refusal", out)
	}
}

// TestStoreHelperProcess is the child side of TestFenceAcrossProcesses.
func TestStoreHelperProcess(t *testing.T) {
	path := os.Getenv("CXP_STORE_HELPER")
	if path == "" {
		t.Skip("helper process only")
	}
	skew, err := time.ParseDuration(os.Getenv("CXP_STORE_SKEW"))
	if err != nil {
		fmt.Println("ERR", err)
		os.Exit(3)
	}
	s, err := OpenWith(path, Options{Now: func() time.Time { return time.Now().Add(skew) }})
	if err != nil {
		fmt.Println("ERR", err)
		os.Exit(3)
	}
	defer s.Close()
	if _, err := s.Acquire(Ident{Host: "h2", PID: os.Getpid(), BootID: "child"}); err != nil {
		fmt.Println("ERR", err)
		os.Exit(4)
	}
	fmt.Println("OK")
	os.Exit(0)
}

func helperCmdOutput(t *testing.T, path, skew string) (string, error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestStoreHelperProcess$", "-test.count=1")
	cmd.Env = append(os.Environ(), "CXP_STORE_HELPER="+path, "CXP_STORE_SKEW="+skew)
	b, err := cmd.CombinedOutput()
	return string(b), err
}

func runHelper(t *testing.T, path, _ string, skew string) {
	t.Helper()
	outp, err := helperCmdOutput(t, path, skew)
	if err != nil || !strings.Contains(outp, "OK") {
		t.Fatalf("helper: %v: %s", err, outp)
	}
}

func runHelperErr(t *testing.T, path, _ string, skew string) string {
	t.Helper()
	outp, err := helperCmdOutput(t, path, skew)
	if err == nil {
		t.Fatalf("helper acquire unexpectedly succeeded: %s", outp)
	}
	return outp
}

func TestDeliveryKeyIdempotency(t *testing.T) {
	s, _ := openTest(t)
	a := newWork(t, s, "a")
	var n1, n2 int
	mustT(t, s.Update(func(tx *Tx) (err error) {
		n1, err = tx.Enqueue(a, []OutMsg{out("k1", OutFinal), out("k2", OutFinal), out("k1", OutFinal)})
		return
	}))
	mustT(t, s.Update(func(tx *Tx) (err error) {
		n2, err = tx.Enqueue(a, []OutMsg{out("k2", OutFinal), out("k3", OutFinal)})
		return
	}))
	if n1 != 2 || n2 != 1 {
		t.Fatalf("enqueued %d then %d, want 2 then 1", n1, n2)
	}
	// Keys are global: another conversation cannot reuse one.
	b := newWork(t, s, "b")
	mustT(t, s.Update(func(tx *Tx) (err error) { n1, err = tx.Enqueue(b, []OutMsg{out("k1", OutFinal)}); return }))
	if n1 != 0 {
		t.Fatal("duplicate key enqueued in another conversation")
	}
	// A sent key is never re-enqueued either; seqs have no gaps.
	mustT(t, s.Update(func(tx *Tx) error {
		tok, err := tx.BeginSend(1)
		if err != nil || tok == "" {
			return fmt.Errorf("begin: %q %v", tok, err)
		}
		return tx.MarkSent(1, "m1")
	}))
	mustT(t, s.Update(func(tx *Tx) (err error) { n1, err = tx.Enqueue(a, []OutMsg{out("k1", OutFinal)}); return }))
	if n1 != 0 {
		t.Fatal("sent key re-enqueued")
	}
	var seqs string
	mustT(t, s.r.QueryRow(`SELECT group_concat(seq, ',') FROM (SELECT seq FROM outbox WHERE conv_id=? ORDER BY seq)`, a).Scan(&seqs))
	if seqs != "1,2,3" {
		t.Fatalf("seqs %q", seqs)
	}
	if err := s.Update(func(tx *Tx) error { _, err := tx.Enqueue(a, []OutMsg{{Kind: OutFinal}}); return err }); err == nil {
		t.Fatal("enqueue without a delivery key succeeded")
	}
}
