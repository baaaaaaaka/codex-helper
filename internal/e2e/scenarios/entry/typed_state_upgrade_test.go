package entry

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
	_ "modernc.org/sqlite"
)

// Unlike rc.59 conversion, this exercises the actual CLI's in-place upgrade
// of a frozen core v5 database, then runs without any legacy authority.
func TestEntryTypedSQLiteUpgradePreservesAttemptedNotice(t *testing.T) {
	harness.Parallel(t)
	path := filepath.Join(t.TempDir(), "core.sqlite")
	s := harness.NewScenario(t, "ENTRY-TYPED-SQLITE-UPGRADE", harness.Options{
		Guarantees: []string{"entry-point", "R-1", "R-49"},
		Title:      "upgrade a frozen core v5 database; card fence and completed CLI result survive restart",
		Budget:     120 * time.Second, NoStart: true,
		UserEnv: map[string]string{"CODEX_HELPER_TEAMS_CORE_STORE": path},
		Golden:  harness.GoldenOptions{Skip: "SQL upgrade and exact delivery counts are asserted here; message rendering has existing entry/notify goldens"},
	})
	defer s.Finish()
	schema, err := os.ReadFile(filepath.Join("..", "..", "..", "core", "store", "testdata", "engine-v5.sql"))
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := db.Exec(query, args...); err != nil {
			t.Fatal(err)
		}
	}
	exec(string(schema))
	exec(`INSERT INTO conversations(id,label,kind,channel,ext_chat_id,title,title_source,state,created_at,updated_at) VALUES(1,'control','control','teams','19:control@thread.v2','Control','user','active',1,1),(2,'s001','work','teams',?,'Original work','user','active',1,1)`, s001)
	exec(`INSERT INTO threads(conv_id,thread_id,cwd,updated_at) VALUES(2,?,?,1)`, migrationRecoveryThread, s.Env.WorkRoot)
	exec(`INSERT INTO meta(key,value) VALUES('engine_notify','[{"conv":2,"key":"already-attempted:","kind":"ready","attempted":true}]'),('engine_cli:completed','{"stage":"sending","conv":2,"result":{"lines":["original result"]}}')`)
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// Configure through the real CLI only after the old database exists.
	// Workflow configuration itself opens the core store and must exercise
	// the same upgrade as helper startup, rather than create a fresh v6 DB.
	s.WorkflowWebhookURL = s.Graph.NewWorkflowWebhook()
	s.EnableWorkflow(s.WorkflowWebhookURL)
	s.Graph.SeedChat("19:control@thread.v2", "Control")
	s.Graph.SeedChat(s001, "Original work")
	fake := fakecodex.Store{Dir: filepath.Join(s.Env.CodexHome, "e2e-fakecodex")}
	if err := fake.Save(&fakecodex.Thread{ID: migrationRecoveryThread, Cwd: s.Env.WorkRoot, Turns: []*fakecodex.Turn{}}); err != nil {
		t.Fatal(err)
	}
	s.StartSUT()
	if !s.WaitFor("the upgraded process is serving", 30*time.Second, func() (bool, string) { return strings.Contains(s.SUT.Output(), runningLine), s.OutputTail(500) }) {
		return
	}
	_, fallback := s.WaitSUTMessage("19:control@thread.v2", "Codex chat ready", steps.CommandTimeout)
	s.Check("attempted card uses its one legal fallback", fallback && len(s.Graph.WorkflowCards()) == 0, "one mention, zero card attempts", fmt.Sprintf("fallback=%v cards=%d", fallback, len(s.Graph.WorkflowCards())))
	token := s.Token("after-upgrade")
	steps.MustPost(s, s001, "Continue original thread "+token)
	_, answered := steps.WaitAnswered(s, s001, token, steps.AnswerTimeout)
	calls := s.Codex.CallsWithToken(token)
	s.Check("the migrated binding answers once", answered && len(calls) == 1 && calls[0].ThreadID == migrationRecoveryThread, "one original-thread execution", fmt.Sprint(calls))
	s.StopSUT()
	db, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	var version, oldKeys int
	var result string
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT count(*) FROM meta WHERE key IN ('engine_notify','engine_cli:completed')`).Scan(&oldKeys); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT text FROM cli_result_lines WHERE request_id='completed' AND position=0`).Scan(&result); err != nil {
		t.Fatal(err)
	}
	s.Check("upgrade committed typed state and removed old keys", version == 6 && oldKeys == 0 && result == "original result", "v6; no old keys; original result", fmt.Sprintf("version=%d old=%d result=%q", version, oldKeys, result))
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	// A leftover legacy file must not override the published database.
	if err := os.WriteFile(filepath.Join(filepath.Dir(path), "state.json"), []byte("poisoned legacy input"), 0600); err != nil {
		t.Fatal(err)
	}
	s.StartSUT()
	if !s.WaitFor("SQLite-only restart serves", 30*time.Second, func() (bool, string) { return strings.Contains(s.SUT.Output(), runningLine), s.OutputTail(500) }) {
		return
	}
	again := s.Token("after-sqlite-only-restart")
	steps.MustPost(s, s001, "Continue original thread "+again)
	_, answered = steps.WaitAnswered(s, s001, again, steps.AnswerTimeout)
	notices := s.SUTMessagesContaining("19:control@thread.v2", "Codex chat ready")
	s.Check("restart neither replays the notice nor reads legacy state", answered && len(notices) == 1, "answers; original fallback once", fmt.Sprintf("answered=%v notices=%d", answered, len(notices)))
}
