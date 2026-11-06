package entry

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

const runningLine = "Teams helper (new core) running on "

// teamsDir is the helper's Teams state directory in the scenario sandbox.
func teamsDir(s *harness.Scenario) string {
	return filepath.Join(s.Env.StateDir, "codex-helper", "teams")
}

// ENTRY-FRESH: on a machine where no older helper ever ran, `cxp teams run`
// creates its store in the Teams state directory and serves the owner; a
// restart runs on the same store.
func TestEntryFreshInstall(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "ENTRY-FRESH", harness.Options{
		Guarantees: []string{"entry-point"},
		Title:      "`cxp teams run` on a fresh machine: new store, work served, same store after a restart",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	chat, _ := steps.NewWorkChat(s, control, "entry")
	tok := s.Token("entry")
	steps.MustPost(s, chat.ID, "Entry task "+tok)
	_, ok := steps.WaitAnswered(s, chat.ID, tok, steps.AnswerTimeout)
	s.Check("work is served through the production entry point", ok, "answered", fmt.Sprint(ok), s.Transcript(chat.ID, 6)...)

	s.StopSUT()
	s.StartSUT()
	next := s.Token("again")
	steps.MustPost(s, chat.ID, "After the restart "+next)
	_, ok = steps.WaitAnswered(s, chat.ID, next, steps.AnswerTimeout)
	s.Check("work continues after a restart", ok, "answered", fmt.Sprint(ok), s.Transcript(chat.ID, 6)...)

	want := filepath.Join(teamsDir(s), "core.sqlite")
	var paths []string
	for _, line := range strings.Split(s.SUT.Output(), "\n") {
		if i := strings.Index(line, runningLine); i >= 0 {
			paths = append(paths, strings.TrimSpace(line[i+len(runningLine):]))
		}
	}
	same := len(paths) == 2 && paths[0] == want && paths[1] == want
	s.Check("both starts run on the store in the Teams state directory", same, want+" twice", fmt.Sprintf("%q", paths))
	_, err := os.Stat(want)
	s.Check("the store exists there", err == nil, "core.sqlite present", fmt.Sprint(err))
	var controls int
	for _, c := range s.Graph.Chats() {
		if c.ID == control.ID || strings.EqualFold(c.Topic, control.Topic) {
			controls++
		}
	}
	s.Check("one control chat across the restart", controls == 1, "1", fmt.Sprint(controls))
}

// ENTRY-MIGRATE: a store of an intermediate release (neither v0.1.20 nor
// rc.59) stops the new core before it reads Teams or creates anything: the
// owner is told to run rc.59 once first.
func TestEntryRefusesUnconvertedStore(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "ENTRY-MIGRATE", harness.Options{
		Guarantees: []string{"entry-point", "unsupported-store-refused"},
		Title:      "an intermediate-release store is refused with the rc.59 instruction; nothing is touched",
		Budget:     60 * time.Second,
		NoStart:    true,
		Invariants: harness.Invariants{Except: []harness.Exception{{Invariant: harness.InvCleanStop, Match: "exited on its own with \"exit status 1\"",
			Reason: "the point of the scenario: an unsupported store stops the helper with an error"}}},
	})
	defer s.Finish()
	dir := teamsDir(s)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(`{"schema_version":1}`), 0o600); err != nil {
		t.Fatal(err)
	}
	s.StartSUT()
	s.WaitFor("the helper exits", 30*time.Second, func() (bool, string) {
		return !s.SUT.Running(), s.OutputTail(300)
	})
	out := s.SUT.Output()
	s.Check("the owner is told to run rc.59 once first", strings.Contains(out, "run v0.1.22-rc.59 once"), "rc.59 instruction", s.OutputTail(400))
	s.Check("the process exits with an error (not a restart request)", strings.Contains(out, "exited: exit status 1"), "exit status 1", s.OutputTail(200))
	_, err := os.Stat(filepath.Join(dir, "core.sqlite"))
	s.Check("no store is created", os.IsNotExist(err), "no core.sqlite", fmt.Sprint(err))
	reqs := s.Graph.Requests(fakegraph.RequestFilter{})
	s.Check("Teams is not contacted", len(reqs) == 0, "0 Graph requests", fmt.Sprint(len(reqs)), s.RequestTail(5)...)
	raw, _ := os.ReadFile(filepath.Join(dir, "state.json"))
	s.Check("the old store is untouched", string(raw) == `{"schema_version":1}`, "unchanged", string(raw))
}

var chatIDRE = regexp.MustCompile(`19:[A-Za-z0-9_.-]+@thread\.v2`)

// ENTRY-AUTOMIGRATE: the first start of this version on an rc.59 store
// (written by the rc.59 code: internal/core/migrate/testdata) converts it
// into core.sqlite next to it, keeps the old files unchanged, posts the
// conversion summary to the control chat once and the interrupted-request
// notice once, also across a restart, which runs on the converted store
// without converting again.
func TestEntryConvertsOldStoreAtFirstStart(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "ENTRY-AUTOMIGRATE", harness.Options{
		Guarantees: []string{"entry-point", "auto-migrate"},
		Title:      "first start on an rc.59 store: converted once, summary and interrupted notices once, old store kept",
		Budget:     120 * time.Second,
		NoStart:    true,
	})
	defer s.Finish()
	scope, raw, ids := rc59Store(s, nil)
	before := map[string][]byte{"state.json": raw}
	const control = "19:control@thread.v2"
	s.Check("the control chat is among the old store's chats", ids[control], control, fmt.Sprint(len(ids)))

	own := func(chat, sub string) int {
		n := 0
		for _, m := range s.Graph.Messages(chat) {
			if m.Origin == fakegraph.OriginSUT && strings.Contains(m.PlainText(), sub) {
				n++
			}
		}
		return n
	}
	const summary = "Teams helper data converted from v0.1.22-rc.59"
	const interrupted = "The previous request was interrupted when the helper stopped"
	interruptedAll := func() int {
		n := 0
		for id := range ids {
			n += own(id, interrupted)
		}
		return n
	}
	s.StartSUT()
	s.WaitFor("the conversion summary in the control chat", 60*time.Second, func() (bool, string) {
		return own(control, summary) >= 1, s.OutputTail(400)
	})
	s.WaitFor("the interrupted-request notice", 30*time.Second, func() (bool, string) {
		return interruptedAll() >= 1, s.OutputTail(200)
	})
	time.Sleep(3 * time.Second)
	s.StopSUT()
	s.StartSUT()
	s.WaitFor("the restarted helper polls the control chat", 30*time.Second, func() (bool, string) {
		return strings.Count(s.SUT.Output(), runningLine) == 2, s.OutputTail(200)
	})
	time.Sleep(3 * time.Second)

	out := s.SUT.Output()
	s.Check("the store is converted once", strings.Count(out, "Converting the Teams store") == 1 && strings.Count(out, "Teams store converted in") == 1,
		"one conversion", s.OutputTail(600))
	want := filepath.Join(scope, "core.sqlite")
	s.Check("both starts run on the converted store next to the old one", strings.Count(out, runningLine+want) == 2, want+" twice", s.OutputTail(600))
	s.Check("the summary is posted once", own(control, summary) == 1, "1", fmt.Sprint(own(control, summary)), s.Transcript(control, 6)...)
	if msgs := s.Graph.Messages(control); len(msgs) > 0 {
		for _, m := range msgs {
			if m.Origin == fakegraph.OriginSUT && strings.Contains(m.PlainText(), summary) {
				text := m.PlainText()
				s.Check("the summary shows no paths", !strings.Contains(text, scope) && !strings.Contains(text, "scope_"), "no paths", text)
			}
		}
	}
	var transcripts []string
	for id := range ids {
		transcripts = append(transcripts, s.Transcript(id, 8)...)
	}
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Method: "POST"}) {
		transcripts = append(transcripts, r.String())
	}
	if raw, err := os.ReadFile(filepath.Join(scope, "migration-report.txt")); err == nil {
		transcripts = append(transcripts, string(raw))
	}
	s.Check("each interrupted request gets one notice", interruptedAll() == 1, "1", fmt.Sprint(interruptedAll()), transcripts...)
	// Owner ruling 2026-11-05 (Q1): the answer whose send the old helper
	// could not confirm is not in the chat's history, so it is sent once,
	// also across the restart.
	s.Check("the unconfirmed old answer, absent from the chat, is sent once", own(s001, "maybe posted") == 1,
		"1", fmt.Sprint(own(s001, "maybe posted")), s.Transcript(s001, 6)...)
	s.Check("the history check is decided once", strings.Count(out, "absent from the chat's history: sent now") == 1,
		"one decision", s.OutputTail(600))
	for name, raw := range before {
		now, err := os.ReadFile(filepath.Join(scope, name))
		s.Check("the old store file "+name+" is unchanged", err == nil && string(now) == string(raw), "byte-identical", fmt.Sprint(err))
	}
	_, err := os.Stat(filepath.Join(scope, "migration-report.txt"))
	s.Check("the conversion report is next to the store", err == nil, "migration-report.txt", fmt.Sprint(err))
}

// rc59Store writes the full-JSON form of an rc.59 store (the SQLite form is
// covered by the cli unit test and the real-store acceptance run) into the
// scenario's scope directory, after edit, and seeds its chats in the fake
// Graph. The fixture's chat gate until 2099 is removed: gates are honoured
// by design and would hold that chat's messages.
func rc59Store(s *harness.Scenario, edit func(state map[string]json.RawMessage)) (scope string, raw []byte, ids map[string]bool) {
	t := s.T
	scope = filepath.Join(teamsDir(s), "scopes", "scope_rc59")
	if err := os.MkdirAll(scope, 0o700); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join("..", "..", "..", "core", "migrate", "testdata", "rc59", "json", "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	var state map[string]json.RawMessage
	if err := json.Unmarshal(raw, &state); err != nil {
		t.Fatal(err)
	}
	delete(state, "chat_rate_limits")
	if edit != nil {
		edit(state)
	}
	if raw, err = json.Marshal(state); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(scope, "state.json"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	ids = map[string]bool{}
	for _, id := range chatIDRE.FindAll(raw, -1) {
		ids[string(id)] = true
	}
	// The chats the older helper created exist in Teams. Seeded in id
	// order: the transcript lists chats with equal (empty) titles in
	// creation order.
	seed := make([]string, 0, len(ids))
	for id := range ids {
		seed = append(seed, id)
	}
	sort.Strings(seed)
	for _, id := range seed {
		s.Graph.SeedChat(id, "")
	}
	return scope, raw, ids
}

// s001 is the rc.59 fixture's work chat whose answer o2 ("maybe posted",
// 2026-09-01T10:00:20Z) the old helper was still sending when it stopped.
const s001 = "19:chat-s001@thread.v2"

// ENTRY-OLDANSWER (owner ruling 2026-11-05, Q1): the answer whose send the
// old helper could not confirm is already in its chat, posted the way
// v0.1.20 posts an answer whose outbox id carries an '@' (as the real
// store's does): without the hidden marker, so only its text identifies it.
// The first start reads the chat's history, finds it and sends nothing; a
// restart does not check again.
func TestEntryOldAnswerAlreadyInChat(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "ENTRY-OLDANSWER", harness.Options{
		Guarantees: []string{"auto-migrate", "R-1"},
		Title:      "first start on an rc.59 store whose unconfirmed answer is in its chat: found in the history, not sent again",
		Budget:     120 * time.Second,
		NoStart:    true,
	})
	defer s.Finish()
	// The old helper polled s001 after its answer posted: its cursor is past
	// it, so the new core does not read the answer as a prompt either.
	scope, _, _ := rc59Store(s, func(state map[string]json.RawMessage) {
		var polls map[string]map[string]any
		if err := json.Unmarshal(state["chat_polls"], &polls); err != nil {
			t.Fatal(err)
		}
		polls[s001]["last_modified_cursor"] = "2026-09-01T10:00:30Z"
		raw, err := json.Marshal(polls)
		if err != nil {
			t.Fatal(err)
		}
		state["chat_polls"] = raw
	})
	// o1, sent with its Teams id, then o2 as v0.1.20 renders a final.
	if err := s.Graph.SeedMessage(s001, "1788256809000", time.Date(2026, 9, 1, 10, 0, 9, 0, time.UTC),
		`<p><strong>🤖 ✅ Codex answer:</strong></p><p>answer one</p><p><strong>🔧 Helper:</strong> ✅ Codex finished responding.</p>`); err != nil {
		t.Fatal(err)
	}
	if err := s.Graph.SeedMessage(s001, "1788256821000", time.Date(2026, 9, 1, 10, 0, 21, 0, time.UTC),
		`<p><strong>🤖 ✅ Codex answer:</strong></p><p>maybe posted</p><p><strong>🔧 Helper:</strong> ✅ Codex finished responding.</p>`); err != nil {
		t.Fatal(err)
	}
	posted := func() int {
		n := 0
		for _, m := range s.Graph.Messages(s001) {
			if m.Origin == fakegraph.OriginSUT && strings.Contains(m.PlainText(), "maybe posted") {
				n++
			}
		}
		return n
	}
	const found = "found in the chat's history (1788256821000): not sent"
	s.StartSUT()
	s.WaitFor("the history check of the old answer", 60*time.Second, func() (bool, string) {
		return strings.Contains(s.SUT.Output(), "answer of the previous helper from"), s.OutputTail(400)
	})
	time.Sleep(2 * time.Second)
	s.StopSUT()
	s.StartSUT()
	s.WaitFor("the restarted helper polls the control chat", 30*time.Second, func() (bool, string) {
		return strings.Count(s.SUT.Output(), runningLine) == 2, s.OutputTail(200)
	})
	time.Sleep(3 * time.Second)

	out := s.SUT.Output()
	s.Check("the old answer is found in the chat's history by its text", strings.Count(out, found) == 1, found+" once", s.OutputTail(600))
	s.Check("the old answer is not sent again", posted() == 0, "0", fmt.Sprint(posted()), s.Transcript(s001, 8)...)
	// It is newer than s001's cursor minus the read overlap and carries no
	// marker: only its listing tells it from an owner's prompt.
	accepted := 0
	for _, m := range s.Graph.Messages(s001) {
		if m.Origin == fakegraph.OriginSUT && strings.Contains(m.PlainText(), "Request accepted") {
			accepted++
		}
	}
	s.Check("the old answer is not taken for the owner's prompt", accepted == 0, "no request accepted in its chat", fmt.Sprint(accepted), s.Transcript(s001, 8)...)
	s.Check("a restart does not check again", strings.Count(out, "answer of the previous helper from") == 1, "1 decision", s.OutputTail(600))
	_, err := os.Stat(filepath.Join(scope, "core.sqlite"))
	s.Check("the store is converted", err == nil, "core.sqlite", fmt.Sprint(err))
}
