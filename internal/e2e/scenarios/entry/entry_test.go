package entry

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/sut"
)

const runningLine = "Teams helper (new core) running on "

func cxpOnly(t *testing.T) {
	if os.Getenv(sut.EnvSUT) != "cxp" {
		t.Skip("production entry point scenarios run with E2E_SUT=cxp")
	}
}

// teamsDir is the helper's Teams state directory in the scenario sandbox.
func teamsDir(s *harness.Scenario) string {
	return filepath.Join(s.Env.StateDir, "codex-helper", "teams")
}

// ENTRY-FRESH: on a machine where no older helper ever ran, `cxp teams run`
// creates its store in the Teams state directory and serves the owner; a
// restart runs on the same store.
func TestEntryFreshInstall(t *testing.T) {
	cxpOnly(t)
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

// ENTRY-MIGRATE: an older helper's store that was never converted stops
// the new core before it reads Teams or creates anything: the owner is told
// to run `cxp teams migrate`; nothing is converted automatically.
func TestEntryRefusesUnconvertedStore(t *testing.T) {
	cxpOnly(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "ENTRY-MIGRATE", harness.Options{
		Guarantees: []string{"entry-point", "no-auto-migrate"},
		Title:      "an unconverted old store is refused with the migrate instruction; nothing is touched",
		Budget:     60 * time.Second,
		NoStart:    true,
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
	s.Check("the owner is told to run the migration", strings.Contains(out, "`cxp teams migrate --source "+dir+"`"), "migrate instruction naming the store", s.OutputTail(400))
	s.Check("the process exits with an error (not a restart request)", strings.Contains(out, "exited: exit status 1"), "exit status 1", s.OutputTail(200))
	_, err := os.Stat(filepath.Join(dir, "core.sqlite"))
	s.Check("no store is created", os.IsNotExist(err), "no core.sqlite", fmt.Sprint(err))
	reqs := s.Graph.Requests(fakegraph.RequestFilter{})
	s.Check("Teams is not contacted", len(reqs) == 0, "0 Graph requests", fmt.Sprint(len(reqs)), s.RequestTail(5)...)
	raw, _ := os.ReadFile(filepath.Join(dir, "state.json"))
	s.Check("the old store is untouched", string(raw) == `{"schema_version":1}`, "unchanged", string(raw))
}
