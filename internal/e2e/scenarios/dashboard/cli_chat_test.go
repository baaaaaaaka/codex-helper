package dashboard

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
)

// The command-line maintenance commands of gap batch G act through the
// running helper: the command leaves a request next to the store and
// prints the helper's result. The command itself never calls Graph.

// cliTimeout bounds one command that waits for the helper (it picks the
// request up within 5 s).
const cliTimeout = 150 * time.Second

// freshChat is the one chat created since before (any topic).
func freshChat(s *harness.Scenario, before map[string]bool) fakegraph.ChatInfo {
	for _, c := range s.Graph.Chats() {
		if !before[c.ID] {
			return c
		}
	}
	return fakegraph.ChatInfo{}
}

// sutAfter lists the helper's messages in chatID posted after seq.
func sutAfter(s *harness.Scenario, chatID string, seq int64) []fakegraph.Message {
	var out []fakegraph.Message
	for _, m := range s.SUTMessages(chatID) {
		if m.RequestSeq > seq {
			out = append(out, m)
		}
	}
	return out
}

// firstWith is the index in msgs of the first message containing sub (-1).
func firstWith(msgs []fakegraph.Message, sub string) int {
	for i, m := range msgs {
		if strings.Contains(harness.NormalizeSpace(m.PlainText()), harness.NormalizeSpace(sub)) {
			return i
		}
	}
	return -1
}

// recreatedURL is the new chat's link the command printed after prefix
// ("Teams control chat recreated: ").
func recreatedURL(out, prefix string) string {
	for _, l := range strings.Split(out, "\n") {
		if i := strings.Index(l, prefix); i >= 0 {
			return strings.TrimSpace(l[i+len(prefix):])
		}
	}
	return "\x00none"
}

// cliGraphCalls counts the Graph requests the commands made since seq.
func cliGraphCalls(s *harness.Scenario, seq int64) int {
	return len(s.Graph.Requests(fakegraph.RequestFilter{Since: seq, Instance: "cxp-cli"}))
}

// unanswered posts text into chatID and checks that the helper answers
// nothing there.
func unanswered(s *harness.Scenario, chatID, text, what string) {
	s.T.Helper()
	seq := s.Graph.LastSeq()
	calls := len(s.Codex.Calls())
	steps.MustPost(s, chatID, text)
	s.Settle(2*time.Second, 12*time.Second)
	got := sutAfter(s, chatID, seq)
	s.Check(what, len(got) == 0 && len(s.Codex.Calls()) == calls, "no reply, no Codex turn",
		fmt.Sprintf("%d replies, %d new Codex calls", len(got), len(s.Codex.Calls())-calls))
}

// CONTROL-RECREATE (F-CTL-2): `cxp teams control --recreate --yes` (v0.1.20
// RecreateControlChat): the running helper creates a fresh control chat,
// posts "🔁 This chat moved" with an @mention and the new link in the old
// one, then "Control chat recreated." (@mention) and the ready message
// with the help in the new one, and serves only the new one. Without
// --yes nothing happens.
func TestControlRecreateCLI(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "CONTROL-RECREATE", harness.Options{
		Guarantees: []string{"F-CTL-2"},
		Title:      "`cxp teams control --recreate --yes`: a fresh control chat, the old one gets an @mentioned link and is no longer served",
		Budget:     4 * time.Minute,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	out, code := s.RunCLI(20*time.Second, "teams", "control", "--recreate")
	s.Check("without --yes the command refuses", code != 0 && strings.Contains(out, "recreating the control chat creates a new Teams chat and sends messages; rerun with --yes"),
		"non-zero exit: rerun with --yes", fmt.Sprintf("exit %d: %s", code, clip(out, 200)))
	s.Check("without --yes no chat is created", len(s.Graph.Chats()) == 1, "1 chat", fmt.Sprint(len(s.Graph.Chats())))

	before := chatsNow(s)
	seq := s.Graph.LastSeq()
	out, code = s.RunCLI(cliTimeout, "teams", "control", "--recreate", "--yes")
	fresh := freshChat(s, before)
	s.Check("the command reports the new chat and the previous one", code == 0 && fresh.ID != "" &&
		strings.Contains(out, "Teams control chat recreated: ") && strings.Contains(out, "Chat ID: "+fresh.ID) &&
		strings.Contains(out, "Previous Chat ID: "+control.ID),
		"exit 0; recreated: <url>; Chat ID; Previous Chat ID", fmt.Sprintf("exit %d: %s", code, clip(out, 400)))
	s.Check("the new chat is a control chat", strings.Contains(fresh.Topic, harness.ControlChatTopicMarker), harness.ControlChatTopicMarker, fresh.Topic)
	s.Check("the command itself makes no Graph request", cliGraphCalls(s, seq) == 0, "0", fmt.Sprint(cliGraphCalls(s, seq)))

	_, ok := s.WaitSUTMessage(fresh.ID, "control chat is ready.", steps.CommandTimeout)
	s.Settle(time.Second, 10*time.Second)
	moved := sutAfter(s, control.ID, seq)
	i := firstWith(moved, "🔁 This chat moved")
	s.Check("the old chat gets one @mentioned link to the new one", len(moved) == 1 && i == 0 && len(moved[0].Mentions) == 1 &&
		strings.Contains(moved[0].PlainText(), "Open the new Control chat:") && strings.Contains(moved[0].PlainText(), recreatedURL(out, "Teams control chat recreated: ")) &&
		strings.Contains(moved[0].PlainText(), "Messages here may not be handled after the switch."),
		"1 message: 🔁 This chat moved / Open the new Control chat: <url>", fmt.Sprintf("%d messages", len(moved)), s.Transcript(control.ID, 3)...)
	news := s.SUTMessages(fresh.ID)
	s.Check("the new chat gets the @mentioned notice, then the ready message with the help", ok && len(news) == 2 &&
		strings.Contains(news[0].PlainText(), "Control chat recreated.") && len(news[0].Mentions) == 1 &&
		strings.Contains(news[1].PlainText(), "control chat is ready.") && strings.Contains(news[1].PlainText(), "Use this chat to choose a workspace"),
		"Control chat recreated. (@mention); control chat is ready. + help", strings.Join(s.Transcript(fresh.ID, 4), " | "))
	s.Check("the link is posted before the new chat is announced", len(moved) == 1 && len(news) > 0 && moved[0].RequestSeq < news[0].RequestSeq,
		"moved link first", "")

	command(s, fresh.ID, "help", "Use this chat to choose a workspace")
	steps.NewWorkChat(s, fresh, "after-recreate")
	unanswered(s, control.ID, "help", "the old control chat is no longer served")
}

// WORK-RECREATE (F-WRK-9): `cxp teams chat recreate <session> --history full
// --yes` (v0.1.20 RecreateSessionChat): a fresh work chat for the same
// Codex session; the old chat gets the @mentioned link, the new one the
// whole local history (helper-run turns included) once, then "Work chat
// recreated: s001." (@mention) and the ready anchor. The session goes on
// in the new chat; the old one is no longer served.
func TestWorkChatRecreateCLI(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "WORK-RECREATE", harness.Options{
		Guarantees: []string{"F-WRK-9"},
		Title:      "`cxp teams chat recreate s001 --history full --yes`: a fresh work chat with the whole history, then the recreated notice; the session continues there",
		Budget:     5 * time.Minute,
		NoStart:    true,
	})
	defer s.Finish()
	dir := s.Env.WorkDir(t, "recreate")
	local := localSession(s, dir, base, "Plan the cache", "Cache planned", "Add eviction", "Eviction added")
	s.StartSUT()
	control := steps.ReadyControlChat(s)
	before := chatsNow(s)
	steps.MustPost(s, control.ID, "continue "+local.SessionID)
	old := newChat(s, before, "s001")
	_, ok := s.WaitSUTMessage(old.ID, "Import complete", steps.CommandTimeout)
	s.Check("s001 gets the local history", ok, "Import complete", strings.Join(s.Transcript(old.ID, 6), " | "))
	first := s.Token("first")
	steps.MustPost(s, old.ID, "Please answer "+first)
	_, answered := steps.WaitAnswered(s, old.ID, first, steps.AnswerTimeout)
	s.Check("s001 answers before the recreate", answered, "answered", strings.Join(s.Transcript(old.ID, 4), " | "))
	s.Settle(time.Second, 10*time.Second)

	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"s001"}, "recreating a work chat creates a new Teams chat and sends messages; rerun with --yes"},
		{[]string{"s001", "--history", "some", "--yes"}, `invalid --history "some": expected none or full`},
		{[]string{"s999", "--yes"}, `Teams session not found for "s999"`},
	} {
		out, code := s.RunCLI(cliTimeout, append([]string{"teams", "chat", "recreate"}, c.args...)...)
		s.Check(fmt.Sprintf("`chat recreate %s` refuses", strings.Join(c.args, " ")), code != 0 && strings.Contains(out, c.want),
			"non-zero exit: "+c.want, fmt.Sprintf("exit %d: %s", code, clip(out, 200)))
	}
	s.Check("the refused commands create no chat", len(s.Graph.Chats()) == 2, "2 chats", fmt.Sprint(len(s.Graph.Chats())))

	before = chatsNow(s)
	seq := s.Graph.LastSeq()
	out, code := s.RunCLI(cliTimeout, "teams", "chat", "recreate", "s001", "--history", "full", "--yes")
	fresh := freshChat(s, before)
	s.Check("the command reports the new chat, the session and the previous chat", code == 0 && fresh.ID != "" &&
		strings.Contains(out, "Teams work chat recreated: ") && strings.Contains(out, "Session: s001") &&
		strings.Contains(out, "Previous Chat ID: "+old.ID),
		"exit 0; recreated: <url>; Session: s001; Previous Chat ID", fmt.Sprintf("exit %d: %s", code, clip(out, 400)))
	s.Check("the command itself makes no Graph request", cliGraphCalls(s, seq) == 0, "0", fmt.Sprint(cliGraphCalls(s, seq)))
	s.WaitSUTMessage(fresh.ID, "Work chat is ready.", steps.CommandTimeout)
	s.Settle(2*time.Second, 15*time.Second)

	moved := sutAfter(s, old.ID, seq)
	s.Check("the old chat gets one @mentioned link to the new one", len(moved) == 1 && len(moved[0].Mentions) == 1 &&
		strings.Contains(moved[0].PlainText(), "🔁 This chat moved") && strings.Contains(moved[0].PlainText(), "Open the new Work chat for s001:") &&
		strings.Contains(moved[0].PlainText(), recreatedURL(out, "Teams work chat recreated: ")),
		"1 message: 🔁 This chat moved / Open the new Work chat for s001: <url>", fmt.Sprintf("%d messages", len(moved)), s.Transcript(old.ID, 3)...)
	onceEach(s, fresh.ID, "the new chat", importTitle, "Plan the cache", "Cache planned", "Add eviction", "Eviction added",
		"Please answer "+first, "Import complete", "Full local Codex history is now up to date in this chat.",
		"Work chat recreated: s001.", "Work chat is ready.")
	n := len(steps.AnswerPosts(s, fresh.ID, first))
	s.Check("the new chat gets the answer of the turn the old chat ran, once", n == 1, "1", fmt.Sprint(n))
	news := s.SUTMessages(fresh.ID)
	hist, notice, anchor := firstWith(news, "Eviction added"), firstWith(news, "Work chat recreated: s001."), firstWith(news, "Work chat is ready.")
	s.Check("the history comes first, then the recreated notice, then the ready anchor", hist >= 0 && hist < notice && notice < anchor,
		"history < notice < anchor", fmt.Sprintf("%d < %d < %d", hist, notice, anchor))
	s.Check("the recreated notice @mentions the owner and names the directory", notice >= 0 && len(news[notice].Mentions) == 1 &&
		strings.Contains(news[notice].PlainText(), "Working directory: "+dir), "@mention; Working directory", "")

	second := s.Token("second")
	steps.MustPost(s, fresh.ID, "Please answer "+second)
	_, answered = steps.WaitAnswered(s, fresh.ID, second, steps.AnswerTimeout)
	calls := s.Codex.CallsWithToken(second)
	firstCalls := s.Codex.CallsWithToken(first)
	s.Check("the session goes on in the new chat on the same Codex thread", answered && len(calls) == 1 && len(firstCalls) == 1 &&
		calls[0].ThreadID == firstCalls[0].ThreadID, "answered on the same thread", fmt.Sprintf("answered=%v calls=%d", answered, len(calls)))
	unanswered(s, old.ID, "Please answer "+s.Token("old"), "the old work chat is no longer served")
}

// PUBLISH-HISTORY-CLI (F-MIR-6): `cxp teams chat publish-history <session>
// --full --yes` (v0.1.20): re-reads the whole rollout into the session's
// chat and posts only what is missing; a second run says it is already
// published; --force replays everything once more. --full and --yes are
// required.
func TestPublishHistoryCLI(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "PUBLISH-HISTORY-CLI", harness.Options{
		Guarantees: []string{"F-MIR-6"},
		Title:      "`cxp teams chat publish-history s001 --full --yes`: posts what is missing, then \"already published\"; --force replays once more",
		Budget:     5 * time.Minute,
		NoStart:    true,
	})
	defer s.Finish()
	dir := s.Env.WorkDir(t, "publish")
	local := localSession(s, dir, base, "Draft the schema", "Schema drafted", "Index the table", "Table indexed")
	s.StartSUT()
	control := steps.ReadyControlChat(s)
	before := chatsNow(s)
	steps.MustPost(s, control.ID, "continue "+local.SessionID)
	work := newChat(s, before, "s001")
	_, ok := s.WaitSUTMessage(work.ID, "Import complete", steps.CommandTimeout)
	s.Check("s001 gets the local history", ok, "Import complete", strings.Join(s.Transcript(work.ID, 6), " | "))
	s.Settle(time.Second, 10*time.Second)

	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"s001", "--yes"}, "publishing complete local history requires --full"},
		{[]string{"s001", "--full"}, "publishing local history sends messages to Teams; rerun with --yes"},
		{[]string{"s999", "--full", "--yes"}, `Teams session not found for "s999"`},
	} {
		out, code := s.RunCLI(cliTimeout, append([]string{"teams", "chat", "publish-history"}, c.args...)...)
		s.Check(fmt.Sprintf("`chat publish-history %s` refuses", strings.Join(c.args, " ")), code != 0 && strings.Contains(out, c.want),
			"non-zero exit: "+c.want, fmt.Sprintf("exit %d: %s", code, clip(out, 200)))
	}

	seq := s.Graph.LastSeq()
	out, code := s.RunCLI(cliTimeout, "teams", "chat", "publish-history", "s001", "--full", "--yes")
	s.Check("publish-history --full reports the history published", code == 0 && strings.Contains(out, "Full local Codex history published to the selected Teams chat."),
		"exit 0: Full local Codex history published to the selected Teams chat.", fmt.Sprintf("exit %d: %s", code, clip(out, 300)))
	_, ok = replyAfter(s, work.ID, seq, "Full local Codex history is now up to date in this chat.", steps.CommandTimeout)
	s.Check("the chat says the full history is up to date", ok, "Full local Codex history is now up to date in this chat.", strings.Join(s.Transcript(work.ID, 4), " | "))
	s.Settle(time.Second, 10*time.Second)
	onceEach(s, work.ID, "s001 after publish-history --full", "Draft the schema", "Schema drafted", "Index the table", "Table indexed")

	out, code = s.RunCLI(cliTimeout, "teams", "chat", "publish-history", "s001", "--full", "--yes")
	s.Check("a second run says it is already published", code == 0 && strings.Contains(out, "Full local Codex history is already published to this Teams chat."),
		"exit 0: Full local Codex history is already published to this Teams chat.", fmt.Sprintf("exit %d: %s", code, clip(out, 300)))

	seq = s.Graph.LastSeq()
	out, code = s.RunCLI(cliTimeout, "teams", "chat", "publish-history", "s001", "--full", "--force", "--yes")
	s.Check("--force replays the history", code == 0 && strings.Contains(out, "Full local Codex history published to the selected Teams chat."),
		"exit 0: published", fmt.Sprintf("exit %d: %s", code, clip(out, 300)))
	replyAfter(s, work.ID, seq, "Full local Codex history is now up to date in this chat.", steps.CommandTimeout)
	s.Settle(time.Second, 10*time.Second)
	var replay []fakegraph.Message
	for _, m := range sutAfter(s, work.ID, seq) {
		if !strings.Contains(m.PlainText(), importTitle) {
			replay = append(replay, m)
		}
	}
	for _, x := range []string{"Draft the schema", "Schema drafted", "Index the table", "Table indexed"} {
		n := steps.CountContaining(replay, x)
		s.Check(fmt.Sprintf("--force posts %q once more", x), n == 1, "1", fmt.Sprint(n))
	}
	s.Check("the commands make no Graph request", cliGraphCalls(s, 0) == 0, "0", fmt.Sprint(cliGraphCalls(s, 0)))
}

// SEND-FILE-CLI (F-RND-10): `cxp teams send-file <path> --session s001`
// (v0.1.20) sends a file from the outbound root to a work chat through the
// helper's upload path; --allow-local-path sends any file, --chat-id needs
// --yes, --message posts a text with it; escapes and unknown targets are
// refused. A request made while no helper runs is kept and sent by the
// next one (`run --once`).
func TestSendFileCLI(t *testing.T) {
	harness.NeedsCLI(t)
	harness.Parallel(t)
	s := harness.NewScenario(t, "SEND-FILE-CLI", harness.Options{
		Guarantees: []string{"F-RND-10"},
		Title:      "`cxp teams send-file`: a file from the outbound root, or a local path with --allow-local-path, sent to s001; refusals; kept while no helper runs",
		Budget:     5 * time.Minute,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "cli-file")
	seq := s.Graph.LastSeq()
	steps.MustPost(s, work.ID, "helper file")
	usage, _ := replyAfter(s, work.ID, seq, "Outbound root:", steps.CommandTimeout)
	root := ""
	if i := strings.Index(usage, "Outbound root:"); i >= 0 {
		root = strings.TrimSpace(usage[i+len("Outbound root:"):])
	}
	if !filepath.IsAbs(root) {
		s.Check("the outbound root is known", false, "an absolute root", usage)
		return
	}
	tok := s.Token("file")
	if err := os.MkdirAll(filepath.Join(root, "reports"), 0o755); err != nil {
		t.Fatal(err)
	}
	content := "release notes " + tok + "\n"
	if err := os.WriteFile(filepath.Join(root, "reports", "notes.txt"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	local := filepath.Join(s.Env.WorkDir(t, "elsewhere"), "local.txt")
	if err := os.WriteFile(local, []byte("local file "+tok+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		args []string
		want string
	}{
		{[]string{"reports/notes.txt"}, "choose a target with --session or --chat-id"},
		{[]string{"reports/notes.txt", "--chat-id", work.ID}, "refusing explicit --chat-id without --yes; prefer --session to avoid sending a file to the wrong Teams chat"},
		{[]string{local, "--session", "s001"}, "absolute paths require --allow-local-path (use --allow-local-path for an explicit local upload)"},
		{[]string{"../outside.txt", "--session", "s001"}, "path must stay under Teams outbound root"},
		{[]string{"reports/notes.txt", "--session", "s999"}, "Teams session not found: s999"},
	} {
		out, code := s.RunCLI(cliTimeout, append([]string{"teams", "send-file"}, c.args...)...)
		s.Check(fmt.Sprintf("`send-file %s` refuses", strings.Join(c.args, " ")), code != 0 && strings.Contains(out, c.want),
			"non-zero exit: "+c.want, fmt.Sprintf("exit %d: %s", code, clip(out, 200)))
	}
	s.Check("nothing is sent by the refused commands", len(s.Graph.OutboundFiles(work.ID)) == 0, "0 files", fmt.Sprint(len(s.Graph.OutboundFiles(work.ID))))

	seq = s.Graph.LastSeq()
	out, code := s.RunCLI(cliTimeout, "teams", "send-file", "reports/notes.txt", "--session", "s001")
	files := s.Graph.OutboundFiles(work.ID)
	s.Check("a file from the outbound root is sent to s001", code == 0 && strings.Contains(out, "Sent Teams file attachment: notes.txt") &&
		strings.Contains(out, "Target session: s001") && len(files) == 1 && files[0].Name == "notes.txt" && string(files[0].Item.Data) == content,
		"exit 0; Sent Teams file attachment: notes.txt; Target session: s001; 1 file with its content",
		fmt.Sprintf("exit %d: %s; %d files", code, clip(out, 300), len(files)))

	out, code = s.RunCLI(cliTimeout, "teams", "send-file", local, "--allow-local-path", "--chat-id", work.ID, "--yes", "--message", "Local notes "+tok)
	files = s.Graph.OutboundFiles(work.ID)
	msgs := sutAfter(s, work.ID, seq)
	text, file := firstWith(msgs, "Local notes "+tok), firstWith(msgs, "📎 local.txt")
	s.Check("--allow-local-path --chat-id --yes --message sends the local file after the message", code == 0 &&
		strings.Contains(out, "Sent Teams file attachment: local.txt") && len(files) == 2 && text >= 0 && text < file,
		"exit 0; message then local.txt", fmt.Sprintf("exit %d: %s; %d files; message %d, file %d", code, clip(out, 300), len(files), text, file))
	s.Check("the commands make no Graph request", cliGraphCalls(s, 0) == 0, "0", fmt.Sprint(cliGraphCalls(s, 0)))

	s.Settle(time.Second, 6*time.Second)
	s.StopSUT()
	out, code = s.RunCLI(cliTimeout, "teams", "send-file", "reports/notes.txt", "--session", "s001")
	s.Check("with no helper running the request is kept", code == 0 && strings.Contains(out, "The Teams helper is not running. The request is kept and runs when the helper starts"),
		"exit 0: kept until the helper starts", fmt.Sprintf("exit %d: %s", code, clip(out, 300)))
	s.Check("nothing is sent while no helper runs", len(s.Graph.OutboundFiles(work.ID)) == 2, "2 files", fmt.Sprint(len(s.Graph.OutboundFiles(work.ID))))
	once := s.NewPeer("once", harness.PeerOptions{Args: []string{"--once"}})
	if err := once.Start(s.Ctx()); err != nil {
		t.Fatalf("start --once: %v", err)
	}
	exited := s.Poll(120*time.Second, func() bool { return !once.Running() })
	s.Check("`run --once` sends the kept file before it exits", exited && once.ExitErr() == nil && len(s.Graph.OutboundFiles(work.ID)) == 3,
		"exit 0, 3 files", fmt.Sprintf("exited=%v err=%v files=%d", exited, once.ExitErr(), len(s.Graph.OutboundFiles(work.ID))))
}
