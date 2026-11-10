package updatestate

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestUpdateSQLiteMigrationAndQuiet(t *testing.T) {
	root := t.TempDir()
	pending := filepath.Join(root, "pending.exe")
	sources := map[string]string{
		"auto-check.json":                   `{"next":"2026-11-10T01:02:03.123456789+08:00","failures":6,"last_error":"中文"}`,
		"pending-update.json":               `{"schema":1,"target_version":"v2.0.0","target_sha256":"hash","previous_active":"v1.0.0","target_active":"/root/2","runtime_root":"/root","entry_path":"/entry","record_path":"/record","request_id":"request"}`,
		"pending.exe.teams-activation.json": `{"version":1,"kind":"teams-helper-update","target_version":"2.0.0","created_at":"2026-11-10T01:02:03.000000001Z"}`,
		"pending.exe.activation.json":       `{"version":1,"status":"failed","message":"retry boundary","source":"src","dest":"dest","want":"2.0.0","updated_at":"2026-11-10T01:02:03.000000002Z"}`,
	}
	for name, body := range sources {
		if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	check, err := s.Check()
	if err != nil || check.Failures != 6 || check.Next.Nanosecond() != 123456789 || check.LastError != "中文" {
		t.Fatalf("check %#v/%v", check, err)
	}
	p, err := s.Pending()
	if err != nil || p.RequestID != "request" || p.PreviousActive != "v1.0.0" {
		t.Fatalf("pending %#v/%v", p, err)
	}
	owner, err := s.Owner(pending)
	if err != nil || owner.CreatedAt.Nanosecond() != 1 || owner.Kind != "teams-helper-update" {
		t.Fatalf("owner %#v/%v", owner, err)
	}
	result, err := s.Result(pending)
	if err != nil || result.Status != "failed" || result.UpdatedAt.Nanosecond() != 2 {
		t.Fatalf("result %#v/%v", result, err)
	}
	var before, after int
	if err = s.db.QueryRow(`SELECT total_changes()`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		if err = s.SaveCheck(check); err != nil {
			t.Fatal(err)
		}
		if err = s.SavePending(p); err != nil {
			t.Fatal(err)
		}
		if err = s.SaveOwner(pending, owner); err != nil {
			t.Fatal(err)
		}
		if err = s.SaveResult(pending, result); err != nil {
			t.Fatal(err)
		}
	}
	if err = s.db.QueryRow(`SELECT total_changes()`).Scan(&after); err != nil || before != after {
		t.Fatal("unchanged update records wrote")
	}
	if err = s.DeletePending("wrong-request"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Pending(); err != nil {
		t.Fatal("stale completion removed pending")
	}
	if err = s.DeletePending(p.RequestID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Pending(); !os.IsNotExist(err) {
		t.Fatal("pending not deleted")
	}
	if _, err = s.Owner(pending); err != nil {
		t.Fatal("pending cleanup affected owner")
	}
	for name, body := range sources {
		if actual, err := os.ReadFile(filepath.Join(root, name) + ".migrated"); err != nil || string(actual) != body {
			t.Fatalf("archive %s/%v", name, err)
		}
		if err = os.WriteFile(filepath.Join(root, name), []byte("poison"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	other, err := OpenRead(root)
	if err != nil {
		t.Fatal(err)
	}
	other.Close()
}

func TestUpdateSQLiteMigrationRollsBackAndReadOnly(t *testing.T) {
	root := t.TempDir()
	if s, err := OpenRead(root); !os.IsNotExist(err) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("fresh read %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, Filename)); !os.IsNotExist(err) {
		t.Fatal("fresh read created database")
	}
	good := filepath.Join(root, "auto-check.json")
	bad := filepath.Join(root, "pending-update.json")
	if err := os.WriteFile(good, []byte(`{"failures":4}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(bad, []byte(`{"schema":999}`), 0600); err != nil {
		t.Fatal(err)
	}
	if s, err := Open(root); err == nil {
		s.Close()
		t.Fatal("future source accepted")
	}
	if _, err := os.Stat(good); err != nil {
		t.Fatal("source archived before commit")
	}
	if err := os.WriteFile(bad, []byte(`{"schema":1,"request_id":"fixed"}`), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := OpenRead(root)
	if err != nil {
		t.Fatal(err)
	}
	p, err := s.Pending()
	if err != nil || p.RequestID != "fixed" {
		t.Fatal("retry", err)
	}
	s.Close()
	s, err = Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`PRAGMA user_version=999`); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if s, err := Open(root); err == nil {
		s.Close()
		t.Fatal("future native accepted")
	}
	if s, err := OpenRead(root); err == nil {
		s.Close()
		t.Fatal("future native read")
	}
}

func TestUpdateSQLiteInvocationAndActivationEntry(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	request := Invocation{Schema: 1, RequestID: "one", CandidatePath: "/candidate", CandidateSHA256: "hash", TargetVersion: "v2.0.0", RuntimeRoot: root, EntryPath: "/entry"}
	if err = s.SaveInvocation(request); err != nil {
		t.Fatal(err)
	}
	other := request
	other.CandidatePath = "changed"
	if err = s.SaveInvocation(other); err == nil {
		t.Fatal("same request input replaced")
	}
	r, err := s.Invocation("one")
	if err != nil || r.CandidatePath != "/candidate" {
		t.Fatal("immutable invocation", err)
	}
	if _, err = s.Invocation("missing"); !os.IsNotExist(err) {
		t.Fatal("wrong request read")
	}
	path := filepath.Join(root, "candidate.exe")
	var stderr bytes.Buffer
	code, handled := HandleActivationCommand([]string{"cxp", "--cxp-record-activation", "--source", path, "--dest", "/installed.exe", "--want", "2.0.0", "--status", "failed", "--message", "中文"}, &stderr)
	if !handled || code != 0 {
		t.Fatalf("entry %d/%v: %s", code, handled, stderr.String())
	}
	result, err := s.Result(path)
	if err != nil || result.Message != "中文" || result.Status != "failed" || time.Since(result.UpdatedAt) > time.Minute {
		t.Fatalf("result %#v/%v", result, err)
	}
	if err = s.DeleteInvocation("one"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Invocation("one"); !os.IsNotExist(err) {
		t.Fatal("invocation cleanup")
	}
	if _, err = s.Result(path); err != nil {
		t.Fatal("invocation cleanup affected activation")
	}
}

func TestUpdateSQLiteInvocationCrashRetention(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := Invocation{Schema: 1, RequestID: "old", CandidatePath: "/candidate"}
	if err := s.SaveInvocation(r); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE update_invocations SET created_at=? WHERE request_id='old'`, time.Now().Add(-8*24*time.Hour).Unix()); err != nil {
		t.Fatal(err)
	}
	// Even an expired ID cannot replace immutable input or trigger a replay.
	if err := s.SaveInvocation(r); err == nil {
		t.Fatal("reused expired input")
	}
	if _, err := s.Invocation("old"); err != nil {
		t.Fatal(err)
	}
	r.RequestID = "live"
	if err := s.SaveInvocation(r); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Invocation("old"); !os.IsNotExist(err) {
		t.Fatal("expired crash input retained", err)
	}
	if _, err := s.Invocation("live"); err != nil {
		t.Fatal(err)
	}
}

func TestUpdateSQLiteMissingPublishedDatabase(t *testing.T) {
	root := t.TempDir()
	s, err := Open(root)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.SavePending(Pending{Schema: 1, RequestID: "unknown"}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	path := filepath.Join(root, Filename)
	if err = os.Rename(path, path+".lost"); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(root, "pending-update.json"), []byte(`{"schema":1,"request_id":"old"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if s, err := OpenRead(root); err == nil || os.IsNotExist(err) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("read lost published state: %v", err)
	}
	if s, err := Open(root); err == nil || os.IsNotExist(err) {
		if s != nil {
			s.Close()
		}
		t.Fatalf("recreated published state: %v", err)
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("replacement update state created")
	}
}
