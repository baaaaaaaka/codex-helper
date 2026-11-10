package cli

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/engine"
)

func TestTeamsExchangeSQLiteAtomicCompletionAndConditionalAck(t *testing.T) {
	anchor := filepath.Join(t.TempDir(), "core.sqlite")
	s, err := openTeamsExchange(anchor, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r := engine.CLIRequest{ID: "one", Op: "send-file", ChatID: "chat", File: "路径", Message: "plain JSON {}", Force: true}
	if err = s.Submit(r); err != nil {
		t.Fatal(err)
	}
	if err = s.Submit(r); err == nil {
		t.Fatal("request ID admitted twice")
	}
	if _, err = s.db.Exec(`CREATE TRIGGER fail_line BEFORE INSERT ON result_lines BEGIN SELECT RAISE(ABORT,'fail'); END`); err != nil {
		t.Fatal(err)
	}
	result := engine.CLIResult{Lines: []string{"first", "第二行"}}
	if err = s.Complete(r.ID, result, time.Now()); err == nil {
		t.Fatal("injected child failure accepted")
	}
	if got, ok, err := s.Request(r.ID); err != nil || !ok || got != r {
		t.Fatalf("request lost after rollback: %+v %v", got, err)
	}
	if _, ok, err := s.Result(r.ID); err != nil || ok {
		t.Fatalf("partial result published: %v %v", ok, err)
	}
	s.db.Exec(`DROP TRIGGER fail_line`)
	if err = s.Complete(r.ID, result, time.Now()); err != nil {
		t.Fatal(err)
	}
	if pending, err := s.Pending(); err != nil || pending {
		t.Fatalf("completed request pending: %v %v", pending, err)
	}
	if got, ok, err := s.Result(r.ID); err != nil || !ok || !reflect.DeepEqual(got, result) {
		t.Fatalf("result order: %+v %v", got, err)
	}
	if err = s.SetWorkflow("older", teamsWorkflowRequest{Enabled: true, WebhookURLFile: "old"}); err != nil {
		t.Fatal(err)
	}
	if err = s.SetWorkflow("newer", teamsWorkflowRequest{Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if err = s.ClearWorkflow("older"); err != nil {
		t.Fatal(err)
	}
	if _, rev, ok, err := s.Workflow(); err != nil || !ok || rev != "newer" {
		t.Fatalf("older acknowledgement erased new request: %q %v", rev, err)
	}
	before, _ := os.Stat(teamsExchangePath(anchor))
	for i := 0; i < 10; i++ {
		if _, err = s.Expire(time.Unix(0, 0)); err != nil {
			t.Fatal(err)
		}
		if _, _, err = s.Result(r.ID); err != nil {
			t.Fatal(err)
		}
	}
	after, _ := os.Stat(teamsExchangePath(anchor))
	if !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("quiet reads/cleanup wrote state")
	}
	if err = s.Ack(r.ID); err != nil {
		t.Fatal(err)
	}
	var children int
	s.db.QueryRow(`SELECT COUNT(*) FROM result_lines`).Scan(&children)
	if children != 0 {
		t.Fatal("result acknowledgement left children", children)
	}
}

func TestTeamsExchangeSQLiteLegacyAtomicMigration(t *testing.T) {
	anchor := filepath.Join(t.TempDir(), "core.sqlite")
	dir := teamsCLIRequestDir(anchor)
	os.MkdirAll(dir, 0700)
	req := filepath.Join(dir, "old.json")
	res := filepath.Join(dir, "old.result.json")
	os.WriteFile(req, []byte(`{"id":"old","op":"send-file","message":"keep"}`), 0600)
	os.WriteFile(res, []byte(`{"lines":["second","first"],"error":""}`), 0600)
	bad := filepath.Join(filepath.Dir(anchor), "workflow-request.json")
	os.WriteFile(bad, []byte(`{"enabled":`), 0600)
	if s, err := openTeamsExchange(anchor, false); err == nil {
		s.Close()
		t.Fatal("accepted malformed final source")
	}
	if _, err := os.Stat(req); err != nil {
		t.Fatal("failed import archived an input", err)
	}
	if _, err := os.Stat(teamsExchangePath(anchor) + ".published"); !os.IsNotExist(err) {
		t.Fatal("failed import published a seal", err)
	}
	os.WriteFile(bad, []byte(`{"enabled":true,"webhook_url_file":"keep-path"}`), 0600)
	s, err := openTeamsExchange(anchor, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if pending, err := s.Pending(); err != nil || pending {
		t.Fatalf("legacy acknowledged request replayed: %v %v", pending, err)
	}
	if got, ok, err := s.Result("old"); err != nil || !ok || !reflect.DeepEqual(got.Lines, []string{"second", "first"}) {
		t.Fatalf("legacy result lost: %+v %v", got, err)
	}
	if got, rev, ok, err := s.Workflow(); err != nil || !ok || rev != "legacy" || !got.Enabled || got.WebhookURLFile != "keep-path" {
		t.Fatalf("legacy workflow: %+v %v", got, err)
	}
	for _, path := range []string{req, res, bad} {
		if _, err := os.Stat(path + ".migrated"); err != nil {
			t.Fatal("missing archive", err)
		}
	}
	// A leftover source after commit cannot overwrite native progress.
	os.WriteFile(req, []byte(`{"id":"old","op":"send-file","message":"poison"}`), 0600)
	s.Close()
	s, err = openTeamsExchange(anchor, false)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if pending, _ := s.Pending(); pending {
		t.Fatal("residual JSON reimported")
	}
	s.Close()
	if err = os.Remove(teamsExchangePath(anchor)); err != nil {
		t.Fatal(err)
	}
	if s, err := openTeamsExchange(anchor, false); err == nil {
		s.Close()
		t.Fatal("lost published exchange reset from JSON")
	}
}
