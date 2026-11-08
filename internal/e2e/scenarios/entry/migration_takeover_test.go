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
	"github.com/gofrs/flock"
)

func TestEntryMigrationResumesAfterCrashDuringImport(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "ENTRY-MIGRATE-IMPORT-CRASH", harness.Options{
		Guarantees: []string{"entry-point", "auto-migrate", "R-1"},
		Title:      "a crash with a partially written conversion never publishes it; restart preserves the original chat and thread",
		Budget:     120 * time.Second, NoStart: true,
		Seam: harness.Seam{Knobs: map[string]string{"Takeover": "3s"}},
	})
	defer s.Finish()
	// Real historical rows keep the import transaction busy long enough to
	// stop the process after SQLite has started, without a product test hook.
	scope, raw, ids := migrationRecoveryFixture(s, 12000)
	s.StartSUT()
	if !freezeDuringMigration(s, scope) {
		return
	}
	defer harness.Thaw(s.SUT)
	s.CrashSUT()
	s.Check("the incomplete conversion contacted no chat", len(s.Graph.Requests(fakegraph.RequestFilter{})) == 0, "no Graph requests", fmt.Sprint(len(s.Graph.Requests(fakegraph.RequestFilter{}))))
	s.StartSUT()
	checkMigrationRecovered(s, s.SUT, scope, raw, ids, "after-import-crash")
}

func TestEntryMigrationTakeoverFencesWaitingOwner(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "ENTRY-MIGRATE-TAKEOVER", harness.Options{
		Guarantees: []string{"entry-point", "auto-migrate", "R-1"},
		Title:      "another host completes migration while the first is frozen; the resumed first owner exits without touching Teams",
		Budget:     120 * time.Second, NoStart: true,
		Seam: harness.Seam{Hostname: "host-a", Knobs: map[string]string{"Takeover": "3s"}},
		Invariants: harness.Invariants{Except: []harness.Exception{{Invariant: harness.InvCleanStop,
			Match: `cxp-1 exited on its own with "exit status 1"`, Reason: "the resumed superseded startup must exit with a fence error"}}},
	})
	defer s.Finish()
	scope, raw, ids := migrationRecoveryFixture(s, 0)
	lock := flock.New(filepath.Join(scope, "state.json.lock"))
	if err := lock.Lock(); err != nil {
		t.Fatal(err)
	}
	defer lock.Unlock()
	s.StartSUT()
	if !s.WaitFor("A is waiting to retry the blocked migration", 30*time.Second, func() (bool, string) {
		return strings.Contains(s.SUT.Output(), "conversion failed:"), s.OutputTail(500)
	}) {
		return
	}
	if !harness.Freeze(s.SUT) {
		t.Fatal("cannot freeze A")
	}
	defer harness.Thaw(s.SUT)
	b := s.NewPeer("B", harness.PeerOptions{Seam: harness.Seam{Hostname: "host-b"}, Args: []string{"--managed-service-child"}})
	if err := b.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	if !s.WaitFor("B claims the stale startup", 20*time.Second, func() (bool, string) {
		return strings.Contains(b.Output(), "taking over generation"), b.Output()
	}) {
		return
	}
	if err := lock.Unlock(); err != nil {
		t.Fatal(err)
	}
	checkMigrationRecovered(s, b, scope, raw, ids, "taken-over")
	seq := s.Graph.LastSeq()
	harness.Thaw(s.SUT)
	exited := s.Poll(5*time.Second, func() bool { return !s.SUT.Running() })
	s.Check("A cancels its retry immediately when fenced", exited && strings.Contains(s.SUT.Output(), "fenced"), "fenced exit within 5s", s.OutputTail(500))
	var requests int
	for _, r := range s.Graph.Requests(fakegraph.RequestFilter{Since: seq}) {
		if r.Instance == s.SUT.Instance() {
			requests++
		}
	}
	s.Check("the resumed stale startup never contacts Teams", requests == 0, "no requests from A", fmt.Sprint(requests))
	checkOriginalMigrationAnswer(s, "after-thaw")
}

func TestEntryMigrationTakeoverFencesImporter(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "ENTRY-MIGRATE-FENCED-IMPORT", harness.Options{
		Guarantees: []string{"entry-point", "auto-migrate", "R-1"},
		Title:      "an importer frozen while building its temporary store loses ownership; it cannot publish when it resumes and the next host recovers",
		Budget:     120 * time.Second, NoStart: true,
		Seam: harness.Seam{Hostname: "host-a", Knobs: map[string]string{"Takeover": "3s"}},
		Invariants: harness.Invariants{Except: []harness.Exception{{Invariant: harness.InvCleanStop,
			Match: `cxp-1 exited on its own with "exit status 1"`, Reason: "the superseded importer exits with a fence error"}}},
	})
	defer s.Finish()
	scope, raw, ids := migrationRecoveryFixture(s, 12000)
	s.StartSUT()
	if !freezeDuringMigration(s, scope) {
		return
	}
	defer harness.Thaw(s.SUT)
	b := s.NewPeer("B", harness.PeerOptions{Seam: harness.Seam{Hostname: "host-b"}, Args: []string{"--managed-service-child"}})
	if err := b.Start(s.Ctx()); err != nil {
		t.Fatal(err)
	}
	if !s.WaitFor("the successor owns the lease but waits for the source lock", 25*time.Second, func() (bool, string) {
		return strings.Contains(b.Output(), "conversion failed:"), b.Output()
	}) {
		return
	}
	harness.Thaw(s.SUT)
	exited := s.Poll(10*time.Second, func() bool { return !s.SUT.Running() })
	s.Check("the resumed importer exits with the fence error", exited && strings.Contains(s.SUT.Output(), "fenced"), "fenced exit", s.OutputTail(500))
	for _, name := range []string{"core.sqlite", "MIGRATED-TO-core.sqlite"} {
		_, err := os.Stat(filepath.Join(scope, name))
		s.Check("the fenced importer did not publish "+name, os.IsNotExist(err), "absent", fmt.Sprint(err))
	}
	s.Check("the fenced importer never contacted a chat", len(s.Graph.Requests(fakegraph.RequestFilter{})) == 0, "no Graph requests", fmt.Sprint(len(s.Graph.Requests(fakegraph.RequestFilter{}))))
	checkMigrationRecovered(s, b, scope, raw, ids, "after-fenced-import")
}

func TestEntryMigrationRefusesMissingPublishedDatabase(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "ENTRY-MIGRATE-MISSING-DB", harness.Options{
		Guarantees: []string{"entry-point"},
		Title:      "loss of an already published database stops startup; the old backup is never mistaken for current data or replaced by an empty store",
		Budget:     90 * time.Second, NoStart: true,
		Invariants: harness.Invariants{Except: []harness.Exception{{Invariant: harness.InvCleanStop,
			Match: `cxp-2 exited on its own with "exit status 1"`, Reason: "missing published data must fail closed"}}},
	})
	defer s.Finish()
	scope, raw, ids := migrationRecoveryFixture(s, 0)
	s.StartSUT()
	checkMigrationRecovered(s, s.SUT, scope, raw, ids, "before-loss")
	s.StopSUT()
	marker, err := os.ReadFile(filepath.Join(scope, "MIGRATED-TO-core.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(scope, "core.sqlite")); err != nil {
		t.Fatal(err)
	}
	seq := s.Graph.LastSeq()
	s.StartSUT()
	exited := s.Poll(15*time.Second, func() bool { return !s.SUT.Running() })
	s.Check("startup refuses an empty replacement", exited && strings.Contains(s.SUT.Output(), "refusing to create an empty store"), "exits with missing-store error", s.OutputTail(500))
	_, err = os.Stat(filepath.Join(scope, "core.sqlite"))
	s.Check("the missing current database is not recreated", os.IsNotExist(err), "no database", fmt.Sprint(err))
	requests := s.Graph.Requests(fakegraph.RequestFilter{Since: seq})
	s.Check("the failed restart never contacts Teams", len(requests) == 0, "no Graph requests", fmt.Sprint(len(requests)))
	after, err := os.ReadFile(filepath.Join(scope, "MIGRATED-TO-core.sqlite"))
	s.Check("the migration marker is unchanged", err == nil && string(marker) == string(after), "unchanged", fmt.Sprint(err))
	after, err = os.ReadFile(filepath.Join(scope, "state.json"))
	s.Check("the old backup is unchanged", err == nil && string(raw) == string(after), "unchanged", fmt.Sprint(err))
}

func checkMigrationRecovered(s *harness.Scenario, owner harness.SUT, scope string, raw []byte, ids map[string]bool, token string) {
	s.T.Helper()
	ready := s.Poll(60*time.Second, func() bool { return strings.Contains(owner.Output(), runningLine) })
	if !s.Check("the current owner serves the converted store", ready, "serving", owner.Output()) {
		return
	}
	checkOriginalMigrationAnswer(s, token)
	for _, chat := range s.Graph.Chats() {
		s.Check("no replacement control chat", ids[chat.ID] || !strings.Contains(chat.Topic, harness.ControlChatTopicMarker), "original chats only", chat.Topic)
	}
	now, err := os.ReadFile(filepath.Join(scope, "state.json"))
	s.Check("the legacy state remains byte-identical", err == nil && string(now) == string(raw), "unchanged", fmt.Sprint(err))
	s.Check("only the current process completed migration", strings.Count(owner.Output(), "Teams store converted in") == 1, "one conversion", owner.Output())
}

func checkOriginalMigrationAnswer(s *harness.Scenario, suffix string) {
	s.T.Helper()
	token := s.Token(suffix)
	steps.MustPost(s, s001, "Keep the original conversation "+token)
	ok := s.Poll(steps.AnswerTimeout, func() bool { return len(steps.AnswerPosts(s, s001, token)) > 0 })
	calls := s.Codex.CallsWithToken(token)
	s.Check("the original chat answers once on the original thread", ok && len(steps.AnswerPosts(s, s001, token)) == 1 && len(calls) == 1 && calls[0].ThreadID == migrationRecoveryThread,
		"one answer on the original thread", fmt.Sprint(calls), s.Transcript(s001, 8)...)
	if ok {
		alerted := s.Poll(steps.CommandTimeout, func() bool {
			return len(s.SUTMessagesContaining("19:control@thread.v2", "Request: Keep the original conversation "+token)) > 0
		})
		s.Check("the original control receives the completion notice", alerted, "completion notice", fmt.Sprint(alerted))
	}
}

func freezeDuringMigration(s *harness.Scenario, scope string) bool {
	s.T.Helper()
	deadline := time.Now().Add(30 * time.Second)
	frozen := false
	for time.Now().Before(deadline) && s.SUT.Running() {
		if _, err := os.Stat(filepath.Join(scope, "core.sqlite.tmp-wal")); err == nil {
			frozen = harness.Freeze(s.SUT)
			break
		}
		time.Sleep(time.Millisecond)
	}
	_, finalErr := os.Stat(filepath.Join(scope, "core.sqlite"))
	if !s.Check("the first process stopped during the temporary import", frozen && os.IsNotExist(finalErr), "temporary database only", fmt.Sprintf("frozen=%v final=%v", frozen, finalErr)) {
		if frozen {
			harness.Thaw(s.SUT)
		}
		return false
	}
	return true
}
