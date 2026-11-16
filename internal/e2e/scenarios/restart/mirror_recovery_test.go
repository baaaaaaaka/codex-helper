package restart

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

func TestMirrorReplacementRecoversWithoutNotificationStorm(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "MIR-RECOVERY-BATCH", harness.Options{Guarantees: []string{"F-MIR-4", "R-1", "R-2"}, Title: "replacement with missing delivery evidence preserves content in bounded recovery batches", Budget: 150 * time.Second,
		Golden: harness.GoldenOptions{Skip: "Recovery batch boundaries depend on sender availability. Exact content/counts, bounded posts, continued live delivery, dedupe, restart and all transport invariants are asserted explicitly."}})
	defer s.Finish()
	steps.ReadyControlChat(s)
	if !steps.HistoryWatchReady(s) {
		t.Fatal("history watcher not ready")
	}
	r := harness.NewRollout(t, s.Env, s.Env.WorkDir(t, "recovery"))
	original := s.Token("original")
	r.AppendTurn(t, "original question", "", "original answer "+original)
	chat := ""
	s.Require("local session mirrored", 25*time.Second, func() (bool, string) {
		cs := steps.ChatsWithSUTText(s, original)
		if len(cs) == 1 {
			chat = cs[0].ID
		}
		return chat != "", fmt.Sprint(len(cs))
	})
	if err := r.AppendRecord("event_msg", map[string]any{"type": "context_compacted"}); err != nil {
		t.Fatal(err)
	}
	s.Require("original compaction delivered", 20*time.Second, func() (bool, string) { return len(s.SUTMessagesContaining(chat, "Context compacted")) == 1, "waiting" })
	waitMirrorOutbox(t, s)
	path := s.StorePath()
	if path == "" {
		t.Fatal("no store path")
	}
	s.StopSUT()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	result, err := db.Exec(`DELETE FROM outbox WHERE kind='mirror' AND body LIKE '%Context compacted%'`)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	removed, err := result.RowsAffected()
	db.Close()
	if err != nil || removed != 1 {
		t.Fatalf("deleted %d rows, err=%v", removed, err)
	}
	for i := 0; i < 701; i++ {
		if err := r.AppendRecord("event_msg", map[string]any{"type": "context_compacted"}); err != nil {
			t.Fatal(err)
		}
	}
	recovered := s.Token("recoveredanswer")
	r.AppendTurn(t, "offline question", "offline full progress", "offline full answer "+recovered)
	// Writer.Replace updates its own integrity evidence as well as changing inode.
	if err := r.Replace(func(lines [][]byte) [][]byte { return lines }); err != nil {
		t.Fatal(err)
	}
	s.StartSUT()
	s.Require("offline answer fully recovered", 40*time.Second, func() (bool, string) { return len(s.SUTMessagesContaining(chat, recovered)) == 1, "waiting" })
	waitMirrorOutbox(t, s)
	count, posts := 0, 0
	repeated := regexp.MustCompile(`Repeated ([0-9]+) times:`)
	for _, m := range s.SUTMessagesContaining(chat, "Recovered history") {
		posts++
		text := m.PlainText()
		count += strings.Count(text, "Context compacted")
		for _, match := range repeated.FindAllStringSubmatch(text, -1) {
			n, _ := strconv.Atoi(match[1])
			count += n - 1
		}
	}
	s.Check("all uncertain and offline compactions delivered", count == 702, "702", fmt.Sprint(count), s.Transcript(chat, 12)...)
	for _, text := range []string{"offline question", "offline full progress", "offline full answer " + recovered} {
		s.Check("recovery retains "+text, len(s.SUTMessagesContaining(chat, text)) == 1, "1 complete occurrence", fmt.Sprint(len(s.SUTMessagesContaining(chat, text))))
	}
	s.Check("recovery has bounded transport posts", posts > 0 && posts <= 12, "1..12", fmt.Sprint(posts))
	fresh := s.Token("freshanswer")
	r.AppendTurn(t, "fresh question", "", "fresh answer "+fresh)
	s.Require("live synchronization continues", 25*time.Second, func() (bool, string) { return len(s.SUTMessagesContaining(chat, fresh)) == 1, "waiting" })
	waitMirrorOutbox(t, s)
	s.StopSUT()
	s.StartSUT()
	s.Settle(2*time.Second, 8*time.Second)
	s.Check("restart does not replay recovery", len(s.SUTMessagesContaining(chat, "Recovered history")) == posts, fmt.Sprint(posts), fmt.Sprint(len(s.SUTMessagesContaining(chat, "Recovered history"))))
	s.Check("original answer not duplicated", len(s.SUTMessagesContaining(chat, original)) == 1, "1", fmt.Sprint(len(s.SUTMessagesContaining(chat, original))))
}
