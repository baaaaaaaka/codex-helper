package entry

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakecodex"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
	"github.com/gofrs/flock"
)

// A real first-start process acquires its lease but cannot migrate while
// the legacy writer holds its lock. Kill it there and start again: the
// leftover lease must not turn an old installation into an empty new one.
func TestEntryMigrationResumesAfterInterruptedStart(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "ENTRY-MIGRATE-RESTART", harness.Options{
		Guarantees: []string{"entry-point", "auto-migrate", "R-1"},
		Title:      "migration interrupted before publication: restart keeps the old chats and serves their messages",
		Budget:     120 * time.Second,
		NoStart:    true,
		Seam:       harness.Seam{Knobs: map[string]string{"Takeover": "3s"}},
	})
	defer s.Finish()
	const threadID = migrationRecoveryThread
	scope, raw, ids := migrationRecoveryFixture(s, 0)
	lock := flock.New(filepath.Join(scope, "state.json.lock"))
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Unlock()
	s.StartSUT()
	if !s.WaitFor("the first migration is blocked by the legacy writer", 30*time.Second, func() (bool, string) {
		return strings.Contains(s.SUT.Output(), "Teams store conversion failed:"), s.OutputTail(500)
	}) {
		return
	}
	s.CrashSUT()
	_, err := os.Stat(filepath.Join(scope, "core.lease"))
	s.Check("the interrupted start left a lease", err == nil, "lease exists", fmt.Sprint(err))
	_, err = os.Stat(filepath.Join(scope, "core.sqlite"))
	s.Check("no database was published", os.IsNotExist(err), "no core.sqlite", fmt.Sprint(err))
	s.Check("the failed migration did not contact Teams", len(s.Graph.Requests(fakegraph.RequestFilter{})) == 0,
		"no Graph request", fmt.Sprint(len(s.Graph.Requests(fakegraph.RequestFilter{}))))
	if err := lock.Unlock(); err != nil {
		t.Fatal(err)
	}
	s.StartSUT()
	if !s.WaitFor("the restart converts the old store", 45*time.Second, func() (bool, string) {
		return strings.Contains(s.SUT.Output(), runningLine+filepath.Join(scope, "core.sqlite")), s.OutputTail(500)
	}) {
		return
	}
	token := s.Token("after-restart")
	steps.MustPost(s, s001, "Continue in the original chat "+token)
	_, answered := steps.WaitAnswered(s, s001, token, steps.AnswerTimeout)
	s.Check("the original work chat answers after restart", answered, "answered", fmt.Sprint(answered), s.Transcript(s001, 8)...)
	calls := s.Codex.CallsWithToken(token)
	s.Check("the answer resumed the original Codex thread", len(calls) == 1 && calls[0].ThreadID == threadID,
		threadID+" once", fmt.Sprint(calls))
	if answered {
		_, alerted := s.WaitSUTMessage("19:control@thread.v2", "Request: Continue in the original chat "+token, steps.CommandTimeout)
		s.Check("the original control chat gets the completion notice", alerted, "completion notice", fmt.Sprint(alerted))
	}
	for _, chat := range s.Graph.Chats() {
		s.Check("no replacement control chat was created", ids[chat.ID] || !strings.Contains(chat.Topic, harness.ControlChatTopicMarker),
			"original chats only", chat.Topic)
	}
	s.StopSUT()
	now, err := os.ReadFile(filepath.Join(scope, "state.json"))
	s.Check("the legacy store is unchanged", err == nil && string(now) == string(raw), "byte-identical", fmt.Sprint(err))
	s.Check("migration completed once", strings.Count(s.SUT.Output(), "Teams store converted in") == 1,
		"one conversion", s.OutputTail(500))
}

const migrationRecoveryThread = "11111111-1111-4111-8111-000000000001"

func migrationRecoveryFixture(s *harness.Scenario, padding int) (string, []byte, map[string]bool) {
	t := s.T
	// A migrated thread already exists in Codex. Seed the fake app-server's
	// persistence too, so resuming this exact original thread is observable.
	const threadID = migrationRecoveryThread
	st := fakecodex.Store{Dir: filepath.Join(s.Env.CodexHome, "e2e-fakecodex")}
	if err := st.Save(&fakecodex.Thread{ID: threadID, Cwd: s.Env.WorkRoot, Turns: []*fakecodex.Turn{}}); err != nil {
		t.Fatal(err)
	}
	scope, raw, ids := rc59Store(s, func(state map[string]json.RawMessage) {
		// Keep the real fixture's original bindings and receive cursor, but
		// give its work chat a directory that exists in this scenario. Its
		// unrelated queued work and old deliveries are covered elsewhere.
		for key := range state {
			switch key {
			case "schema_version", "scope", "control_chat", "sessions", "chat_polls":
			default:
				delete(state, key)
			}
		}
		var sessions map[string]map[string]any
		if err := json.Unmarshal(state["sessions"], &sessions); err != nil {
			t.Fatal(err)
		}
		sessions["s001"]["cwd"] = s.Env.WorkRoot
		state["sessions"], _ = json.Marshal(map[string]any{"s001": sessions["s001"]})
		var polls map[string]json.RawMessage
		if err := json.Unmarshal(state["chat_polls"], &polls); err != nil {
			t.Fatal(err)
		}
		state["chat_polls"], _ = json.Marshal(map[string]json.RawMessage{s001: polls[s001]})
		if padding > 0 {
			rows := map[string]any{}
			for i := 0; i < padding; i++ {
				id := fmt.Sprintf("padding-%06d", i)
				rows[id] = map[string]any{"id": id, "session_id": "s001", "teams_chat_id": s001,
					"teams_message_id": id, "text": "already handled", "status": "persisted",
					"received_at": "2026-08-01T00:00:00Z", "created_at": "2026-08-01T00:00:00Z"}
			}
			state["inbound_events"], _ = json.Marshal(rows)
		}
	})
	return scope, raw, ids
}
