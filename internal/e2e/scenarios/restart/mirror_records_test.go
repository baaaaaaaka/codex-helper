package restart

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// Big, broken and half-written records in a live local session never stop
// its mirroring (F-MIR-9, R-1, R-44).
//
// History: chats s512/s514/s519 stopped mirroring after a 10.8 MB pasted
// screenshot or a 2.25 MB tool output (U-I09, INC-05; b3ac75a…ca2f1a5);
// one malformed JSONL line poisoned the stream (b9c8a07); long sessions
// failed with "bufio.Scanner: token too long" at the 16 MiB line limit
// four times (U-I24, INC-21; 40b6949, f6ab0f2, c001e1c); a record read
// before Codex finished writing it must not be posted half (the tail
// windows of L-I4).
//
// A live local TUI session is mirrored, then: a prompt with a 10.8 MB
// pasted screenshot, a 3 MB tool output, a 17 MiB tool output (over the
// 16 MiB line limit), a malformed line and a record type nobody knows,
// then the turn's commentary and answer. The prompt text, commentary and
// answer each arrive once and nothing of the bulk reaches Teams. The next
// turn's answer records are written in pieces that end inside each
// record, with pauses in between: no answer is posted without its end,
// and the whole answer arrives once.
func TestMirrorSurvivesBadRecords(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "MIR-BAD-RECORDS", harness.Options{
		Guarantees: []string{"F-MIR-9", "R-1", "R-44"},
		Title:      "a live TUI session with a 10.8 MB screenshot, 3 MB and 17 MiB tool outputs, a malformed line and a half-written record keeps mirroring",
		Budget:     120 * time.Second,
		// The bulk records cost the reader real CPU (about 31 MB parsed
		// or skipped once).
		Invariants: harness.Invariants{CPUBudget: 8 * time.Second},
	})
	defer s.Finish()

	steps.ReadyControlChat(s)
	if !steps.HistoryWatchReady(s) {
		s.Check("history watcher initialised", false, "probe session mirrored", "never mirrored")
		return
	}
	r := harness.NewRollout(t, s.Env, s.Env.WorkDir(t, "bad-records"))
	p1, a1 := s.Token("firstprompt"), s.Token("firstanswer")
	r.AppendTurn(t, "Earlier question "+p1, "", "Earlier answer "+a1)
	var chat fakegraph.ChatInfo
	s.Require("the session is mirrored into a work chat", 30*time.Second, func() (bool, string) {
		cs := steps.ChatsWithSUTText(s, a1)
		if len(cs) == 1 {
			chat = cs[0]
			return true, ""
		}
		return false, fmt.Sprintf("%d chats with the answer", len(cs))
	})
	s.Settle(2*time.Second, 10*time.Second)

	// Turn 2, live: the bulk.
	const imageMark = "iVBORw0KGgoAAAANSUhEUgAA"
	image := "data:image/png;base64," + strings.Repeat(imageMark, (10_800_000)/len(imageMark))
	p2, c2, a2 := s.Token("shotprompt"), s.Token("shotcommentary"), s.Token("shotanswer")
	turn2 := rollout.Turn{Prompt: "What is wrong in this screenshot? " + p2, Images: []string{image},
		Commentary: "Reading the logs " + c2, Answer: "The screenshot shows a crash " + a2}
	open, err := r.BeginTurn(turn2)
	if err != nil {
		t.Fatal(err)
	}
	const toolMark, hugeMark = "E2E-TOOL-OUTPUT-LINE ", "E2E-HUGE-OUTPUT-LINE "
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	must(r.AppendRecord("response_item", map[string]any{"type": "function_call", "name": "exec_command", "call_id": "call_big",
		"arguments": `{"cmd":"cat build.log"}`}))
	must(r.AppendRecord("response_item", map[string]any{"type": "function_call_output", "call_id": "call_big",
		"output": strings.Repeat(toolMark, 3_000_000/len(toolMark))}))
	must(r.AppendRecord("event_msg", map[string]any{"type": "exec_command_end", "call_id": "call_huge", "turn_id": open.ID,
		"aggregated_output": strings.Repeat(hugeMark, (17<<20)/len(hugeMark)), "exit_code": 0}))
	must(r.AppendRaw([]byte(`{"timestamp":"2026-11-06T00:00:00.000Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"E2E-MALFORMED-RECORD` + "\n")))
	must(r.AppendRecord("future_record_kind", map[string]any{"type": "something_new", "text": "E2E-UNKNOWN-RECORD"}))
	must(r.FinishTurn(open, turn2))

	s.WaitFor("the answer after the bulk records is mirrored", 30*time.Second, func() (bool, string) {
		return len(s.SUTMessagesContaining(chat.ID, a2)) > 0, "not yet"
	})
	s.Settle(2*time.Second, 10*time.Second)

	// Turn 3, live: the answer record arrives in two writes.
	p3, a3 := s.Token("splitprompt"), s.Token("splitanswer")
	turn3 := rollout.Turn{Prompt: "And now? " + p3, Answer: "First half of the answer, " + strings.Repeat("then more text, ", 40) + "and the end " + a3}
	open3, err := r.BeginTurn(turn3)
	must(err)
	rest, err := r.RenderFinish(open3, turn3)
	must(err)
	// Codex writes the answer in three records (item_completed, the
	// response item, task_complete): each is cut in the middle, so a
	// reader that consumes a half-written tail loses all of them.
	var cuts []int
	for off := 0; off < len(rest); {
		end := bytes.IndexByte(rest[off:], '\n')
		if end < 0 {
			end = len(rest) - off
		}
		if i := bytes.Index(rest[off:off+end], []byte("then more text")); i > 0 {
			cuts = append(cuts, off+i)
		}
		off += end + 1
	}
	if len(cuts) == 0 {
		t.Fatal("answer text not found in the rendered records")
	}
	s.Note("the answer's records are written in %d pieces", len(cuts)+1)
	prev := 0
	half := 0
	for i, cut := range cuts {
		must(r.AppendRaw(rest[prev:cut]))
		prev = cut
		if i == 0 {
			s.WaitFor("the prompt of the turn being written is mirrored", 20*time.Second, func() (bool, string) {
				return steps.UserRecords(s, chat.ID, "And now? "+p3) > 0, "not yet"
			})
		}
		// Several mirror passes over the half-written record.
		time.Sleep(2 * time.Second)
		// A completed record may be mirrored already; a cut one may not.
		half += len(s.SUTMessagesContaining(chat.ID, "First half of the answer")) - len(s.SUTMessagesContaining(chat.ID, a3))
	}
	s.Check("nothing of a half-written record is posted", half == 0, "no answer without its end", fmt.Sprint(half), s.Transcript(chat.ID, 6)...)
	must(r.AppendRaw(rest[prev:]))
	s.WaitFor("the completed answer is mirrored", 20*time.Second, func() (bool, string) {
		return len(s.SUTMessagesContaining(chat.ID, a3)) > 0, "not yet"
	})
	s.Settle(3*time.Second, 15*time.Second)

	tr := s.Transcript(chat.ID, 20)
	once := func(name, text string, n int) {
		s.Check(name, n == 1, "1", fmt.Sprint(n), tr...)
	}
	once("the screenshot prompt's text is mirrored once", "", steps.UserRecords(s, chat.ID, "What is wrong in this screenshot? "+p2))
	once("the commentary after the bulk is mirrored once", "", len(s.SUTMessagesContaining(chat.ID, c2)))
	once("the answer after the bulk is mirrored once", "", len(s.SUTMessagesContaining(chat.ID, a2)))
	once("the prompt of the split turn is mirrored once", "", steps.UserRecords(s, chat.ID, "And now? "+p3))
	once("the split answer is mirrored once, whole", "", len(s.SUTMessagesContaining(chat.ID, turn3.Answer)))
	once("the first answer is still there once", "", len(s.SUTMessagesContaining(chat.ID, a1)))

	var leaks []string
	maxBody := 0
	for _, q := range s.Graph.Requests(fakegraph.RequestFilter{}) {
		if q.Method == "GET" {
			continue
		}
		if len(q.Body) > maxBody {
			maxBody = len(q.Body)
		}
		for _, mark := range []string{imageMark + imageMark, toolMark, hugeMark, "E2E-MALFORMED-RECORD", "E2E-UNKNOWN-RECORD"} {
			if strings.Contains(q.Body, mark) {
				leaks = append(leaks, fmt.Sprintf("%s %s carries %q", q.Method, q.Route, mark))
			}
		}
	}
	s.Check("no screenshot data, tool output, malformed or unknown record reaches Teams", len(leaks) == 0, "none", fmt.Sprint(leaks))
	s.Check("every Graph write stays small", maxBody < 100_000, "< 100 KB", fmt.Sprintf("largest body %d bytes", maxBody))
	s.Check("mirroring never runs Codex", len(s.Codex.Calls()) == 0, "0", fmt.Sprint(len(s.Codex.Calls())))
}

// A rollout Codex replaces with a new file (a legacy rollout rewritten as
// a paginated one) keeps mirroring from where it was, with nothing
// re-posted (F-MIR-7, R-2, R-44).
//
// History: sync stopped for good when Codex atomically replaced a legacy
// rollout with a paginated one (v0.1.22-rc.19, 7da6f8b, "rebase
// checkpoints onto paginated history"); new TUI sessions and turns were
// "never detected" while that fence held (U-I11, INC-20 SC-20b).
//
// Two replacements: first a shorter file that keeps only the newest turn
// (the cursor is now past its end), then a longer file (the same records
// re-serialised, plus a new turn) under a new inode. After each, only the
// new turn is posted, once.
func TestMirrorFollowsReplacedRollout(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "MIR-REWRITE", harness.Options{
		Guarantees: []string{"F-MIR-7", "R-2", "R-44"},
		Title:      "Codex replaces a mirrored rollout (shorter, then longer, new inode): only new turns are posted, once",
		Budget:     120 * time.Second,
	})
	defer s.Finish()

	steps.ReadyControlChat(s)
	if !steps.HistoryWatchReady(s) {
		s.Check("history watcher initialised", false, "probe session mirrored", "never mirrored")
		return
	}
	r := harness.NewRollout(t, s.Env, s.Env.WorkDir(t, "rewrite"))
	type turn struct{ prompt, answer string }
	var turns []turn
	add := func(label string) turn {
		tu := turn{fmt.Sprintf("Question %s %s", label, s.Token(label+"prompt")), fmt.Sprintf("Answer %s %s", label, s.Token(label+"answer"))}
		r.AppendTurn(t, tu.prompt, "", tu.answer)
		turns = append(turns, tu)
		return tu
	}
	add("one")
	add("two")
	var chat fakegraph.ChatInfo
	s.Require("the session is mirrored into a work chat", 30*time.Second, func() (bool, string) {
		cs := steps.ChatsWithSUTText(s, turns[1].answer)
		if len(cs) == 1 {
			chat = cs[0]
			return true, ""
		}
		return false, fmt.Sprintf("%d chats with the second answer", len(cs))
	})
	s.Settle(2*time.Second, 10*time.Second)

	// 1. A shorter file: the session_meta marked paginated and only the
	// newest turn; then a live turn.
	err := r.Replace(func(lines [][]byte) [][]byte {
		out := [][]byte{bytes.Replace(lines[0], []byte(`"originator"`), []byte(`"history_mode":"paginated","originator"`), 1)}
		start := len(lines)
		for i := len(lines) - 1; i > 0; i-- {
			if bytes.Contains(lines[i], []byte(`"task_started"`)) {
				start = i
				break
			}
		}
		return append(out, lines[start:]...)
	})
	if err != nil {
		t.Fatal(err)
	}
	three := add("three")
	s.WaitFor("the turn after the shorter replacement is mirrored", 30*time.Second, func() (bool, string) {
		return len(s.SUTMessagesContaining(chat.ID, three.answer)) > 0, "not yet"
	})
	s.Settle(2*time.Second, 10*time.Second)

	// 2. A longer file under a new inode: every record re-serialised with
	// a trailing space, plus a new turn written into the replacement.
	err = r.Replace(func(lines [][]byte) [][]byte {
		var out [][]byte
		for _, l := range lines {
			out = append(out, append(append([]byte(nil), l...), ' '))
		}
		return out
	})
	if err != nil {
		t.Fatal(err)
	}
	four := add("four")
	s.WaitFor("the turn after the longer replacement is mirrored", 30*time.Second, func() (bool, string) {
		return len(s.SUTMessagesContaining(chat.ID, four.answer)) > 0, "not yet"
	})
	s.Settle(3*time.Second, 15*time.Second)

	tr := s.Transcript(chat.ID, 20)
	for i, tu := range turns {
		n := steps.UserRecords(s, chat.ID, tu.prompt)
		s.Check(fmt.Sprintf("turn %d's prompt is in the chat once", i+1), n == 1, "1", fmt.Sprint(n), tr...)
		n = len(s.SUTMessagesContaining(chat.ID, tu.answer))
		s.Check(fmt.Sprintf("turn %d's answer is in the chat once", i+1), n == 1, "1", fmt.Sprint(n), tr...)
	}
	var chats []string
	for _, tu := range turns {
		for _, c := range steps.ChatsWithSUTText(s, tu.answer) {
			if c.ID != chat.ID {
				chats = append(chats, c.ID)
			}
		}
	}
	s.Check("the replaced rollout stays in its one chat", len(chats) == 0, "no other chat", fmt.Sprint(chats))
	s.Check("mirroring never runs Codex", len(s.Codex.Calls()) == 0, "0", fmt.Sprint(len(s.Codex.Calls())))
}
