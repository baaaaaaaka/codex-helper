package edits

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

const userLabel = "🧑‍💻 User:"

func clip(s string, n int) string {
	s = harness.NormalizeSpace(s)
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

// message returns the current state of message id in chat.
func message(s *harness.Scenario, chat, id string) (fakegraph.Message, bool) {
	for _, m := range s.Graph.Messages(chat) {
		if m.ID == id {
			return m, true
		}
	}
	return fakegraph.Message{}, false
}

// waitEdited waits until message id carries sub.
func waitEdited(s *harness.Scenario, chat, id, sub string, timeout time.Duration) (fakegraph.Message, bool) {
	var got fakegraph.Message
	ok := s.Poll(timeout, func() bool {
		m, found := message(s, chat, id)
		got = m
		return found && strings.Contains(harness.NormalizeSpace(m.PlainText()), harness.NormalizeSpace(sub))
	})
	return got, ok
}

// patches lists the message PATCH requests of chat after seq.
func patches(s *harness.Scenario, chat string, seq int64) []fakegraph.Request {
	return s.Graph.Requests(fakegraph.RequestFilter{Route: "PATCH /chats/{chat}/messages/{message}", ChatID: chat, Since: seq})
}

// renames lists the chat title PATCH requests of chat after seq.
func renames(s *harness.Scenario, chat string, seq int64) []fakegraph.Request {
	return s.Graph.Requests(fakegraph.RequestFilter{Route: "PATCH /chats/{chat}", ChatID: chat, Since: seq})
}

func topic(s *harness.Scenario, chat string) string {
	c, _ := s.Graph.Chat(chat)
	return c.Topic
}

func waitTopic(s *harness.Scenario, chat, want string, timeout time.Duration) bool {
	return s.WaitFor(fmt.Sprintf("chat title %q", want), timeout, func() (bool, string) {
		got := topic(s, chat)
		return got == want, "title " + fmt.Sprintf("%q", got)
	})
}

// command posts text and waits for a helper reply containing sub.
func command(s *harness.Scenario, chat, text, sub string) (string, bool) {
	seq := s.Graph.LastSeq()
	steps.MustPost(s, chat, text)
	var got string
	ok := s.WaitFor(fmt.Sprintf("reply containing %q", sub), steps.CommandTimeout, func() (bool, string) {
		for _, m := range s.SUTMessages(chat) {
			if m.RequestSeq > seq && strings.Contains(harness.NormalizeSpace(m.PlainText()), harness.NormalizeSpace(sub)) {
				got = m.PlainText()
				return true, ""
			}
		}
		return false, strings.Join(s.Transcript(chat, 4), " | ")
	})
	return got, ok
}

// ask posts a prompt carrying a fresh token and waits for its answer.
func ask(s *harness.Scenario, chat, prompt, label string) (string, fakegraph.Message, bool) {
	token := s.Token(label)
	m, err := s.Graph.PostAsUser(chat, prompt+" "+token)
	if err != nil {
		s.T.Fatal(err)
	}
	_, ok := steps.WaitAnswered(s, chat, token, steps.AnswerTimeout)
	return token, m, ok
}

// The user label (F-RND-4 / R-8, v0.1.20 annotateIncomingUserMessage):
// every owner message the helper acts on — a control-chat command, a
// work-chat prompt, a `helper` command, a message with a file — is edited
// once to start with "🧑‍💻 User:", its text and attachments kept. A
// message the edit could lose something of (a pasted image) is left alone,
// and the helper's own posts are never labelled.
func TestUserLabel(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "EDIT-LABEL", harness.Options{
		Guarantees: []string{"F-RND-4", "R-8"},
		Title:      "owner messages the helper acts on get the 🧑‍💻 User label by one lossless edit; images are left alone",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	from := s.Graph.LastSeq()
	cmd, err := s.Graph.PostAsUser(control.ID, "status")
	if err != nil {
		t.Fatal(err)
	}
	labelled, ok := waitEdited(s, control.ID, cmd.ID, userLabel, steps.CommandTimeout)
	s.Check("a control-chat command is labelled", ok && strings.HasPrefix(harness.NormalizeSpace(labelled.PlainText()), userLabel) &&
		strings.Contains(labelled.PlainText(), "status"), "\"🧑‍💻 User: status\"", fmt.Sprintf("%q", clip(labelled.PlainText(), 120)))

	work, _ := steps.NewWorkChat(s, control, "label")
	_, prompt, answered := ask(s, work.ID, "Please answer", "prompt")
	pm, ok := waitEdited(s, work.ID, prompt.ID, userLabel, steps.CommandTimeout)
	s.Check("a work-chat prompt is answered and labelled", answered && ok && strings.Contains(pm.PlainText(), "Please answer"),
		"answered; \"🧑‍💻 User: Please answer …\"", fmt.Sprintf("answered=%v text=%q", answered, clip(pm.PlainText(), 120)))

	hc, err := s.Graph.PostAsUser(work.ID, "helper status")
	if err != nil {
		t.Fatal(err)
	}
	hm, ok := waitEdited(s, work.ID, hc.ID, userLabel, steps.CommandTimeout)
	s.Check("a `helper` command is labelled", ok, "\"🧑‍💻 User: helper status\"", fmt.Sprintf("%q", clip(hm.PlainText(), 120)))

	ftok := s.Token("file")
	fm, err := s.Graph.PostFileAsUser(work.ID, "Read the attached notes "+ftok, "notes.txt", []byte("notes"))
	if err != nil {
		t.Fatal(err)
	}
	steps.WaitAnswered(s, work.ID, ftok, steps.AnswerTimeout)
	fe, ok := waitEdited(s, work.ID, fm.ID, userLabel, steps.CommandTimeout)
	s.Check("a message with a file is labelled and keeps its attachment", ok && len(fe.Attachments) == len(fm.Attachments) &&
		strings.Contains(fe.Content, "<attachment"), fmt.Sprintf("labelled, %d attachment(s) kept", len(fm.Attachments)),
		fmt.Sprintf("labelled=%v attachments=%d content=%q", ok, len(fe.Attachments), clip(fe.Content, 200)))

	itok := s.Token("image")
	im, err := s.Graph.PostImageAsUser(work.ID, "Describe this image "+itok, tinyPNG)
	if err != nil {
		t.Fatal(err)
	}
	steps.WaitAnswered(s, work.ID, itok, steps.AnswerTimeout)
	s.Settle(2*time.Second, 10*time.Second)
	ie, _ := message(s, work.ID, im.ID)
	s.Check("a message with a pasted image is not edited (the edit could lose the image)", ie.PatchCount == 0 && ie.Hosted == im.Hosted,
		"0 edits", fmt.Sprintf("%d edits; text %q", ie.PatchCount, clip(ie.PlainText(), 120)))

	// Exactly one edit per labelled message, none of a helper post.
	edits := map[string]int{}
	for _, c := range []string{control.ID, work.ID} {
		for _, r := range patches(s, c, from) {
			edits[r.Path]++
		}
	}
	for _, c := range []string{control.ID, work.ID} {
		for _, m := range s.SUTMessages(c) {
			if m.PatchCount > 0 && strings.Contains(m.PlainText(), userLabel) {
				s.Check("no helper post is labelled", false, "0", "helper post "+clip(m.PlainText(), 80))
			}
		}
	}
	twice := 0
	for _, n := range edits {
		if n > 1 {
			twice++
		}
	}
	for _, m := range []fakegraph.Message{labelled, pm, hm, fe} {
		if strings.Count(m.PlainText(), "User:") != 1 {
			twice++
		}
	}
	// Five: the four above and the `new label` command that made the work chat.
	s.Check("each labelled message is edited once and labelled once", twice == 0 && len(edits) == 5,
		"5 messages, 1 edit each", fmt.Sprintf("%d messages edited: %v", len(edits), edits))
}

// 1x1 transparent PNG.
var tinyPNG = []byte{0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
	0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4, 0x89, 0x00, 0x00, 0x00,
	0x0d, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9c, 0x63, 0x00, 0x01, 0x00, 0x00, 0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00,
	0x00, 0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae, 0x42, 0x60, 0x82}

// The transcript of a voice message (F-RND-11, v0.1.20
// annotateIncomingUserMessageWithASRTranscripts): once the clip is
// transcribed, the voice message itself is edited to carry the user label
// and the transcript under the clip, the audio card kept. A clip whose
// transcription failed is not edited.
func TestVoiceTranscriptEdit(t *testing.T) {
	harness.NeedsCLI(t) // the voice path is wired in the cxp entry point only
	harness.Parallel(t)
	tools := t.TempDir()
	script := filepath.Join(tools, "asr.sh")
	body := "#!/bin/sh\ncase \"$(cat \"$1\")\" in\n*silence*) exit 0 ;;\nesac\nprintf 'transcribed: %s\\n' \"$(cat \"$1\")\"\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	s := harness.NewScenario(t, "EDIT-VOICE", harness.Options{
		Guarantees: []string{"F-RND-11"},
		Title:      "a transcribed voice message is edited to show its transcript under the clip; a failed one is not edited",
		Budget:     120 * time.Second,
		UserEnv:    map[string]string{"CODEX_HELPER_TEAMS_ASR_COMMAND": script},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "voice-edit")
	tok := s.Token("voice")
	vm, err := s.Graph.PostVoiceAsUser(work.ID, "voice.ogg", []byte("please list the files "+tok))
	if err != nil {
		t.Fatal(err)
	}
	_, answered := steps.WaitAnswered(s, work.ID, tok, steps.AnswerTimeout)
	ve, ok := waitEdited(s, work.ID, vm.ID, "transcribed: please list the files "+tok, steps.CommandTimeout)
	text := harness.NormalizeSpace(ve.PlainText())
	s.Check("the voice message is answered and shows its transcript", answered && ok &&
		strings.HasPrefix(text, userLabel) && strings.Contains(text, "Speech-to-text transcript (automatic; may contain recognition errors):") &&
		strings.Contains(text, "Clip: voice.ogg"), "label, transcript heading, clip name and text",
		fmt.Sprintf("answered=%v text=%q", answered, clip(text, 300)))
	s.Check("the audio card is kept", len(ve.Attachments) == len(vm.Attachments) && strings.Contains(ve.Content, "<attachment"),
		"the card and its placeholder", fmt.Sprintf("%d attachments; content %q", len(ve.Attachments), clip(ve.Content, 200)))

	from := s.Graph.LastSeq()
	silent, err := s.Graph.PostVoiceAsUser(work.ID, "quiet.ogg", []byte("silence"))
	if err != nil {
		t.Fatal(err)
	}
	s.Poll(30*time.Second, func() bool {
		for _, m := range s.SUTMessages(work.ID) {
			if m.RequestSeq > from && strings.Contains(m.PlainText(), "no speech was recognised") {
				return true
			}
		}
		return false
	})
	s.Settle(2*time.Second, 10*time.Second)
	se, _ := message(s, work.ID, silent.ID)
	s.Check("a clip with no speech is not edited", se.PatchCount == 0, "0 edits", fmt.Sprintf("%d edits: %q", se.PatchCount, clip(se.PlainText(), 160)))
	ve2, _ := message(s, work.ID, vm.ID)
	s.Check("the transcript edit is made once", ve2.PatchCount == 1, "1 edit", fmt.Sprint(ve2.PatchCount))
}

// The freeze notice (F-WRK-5 / R-22, v0.1.20 parkIdleWorkChat): a work
// chat idle for ParkAfter is parked and its newest message — the helper's
// answer — is edited once to carry the freeze notice (paused, messages are
// read once it wakes, how to wake it). A prompt typed while parked runs
// once after `r <id>`; the next park gets its own notice, once.
func TestFreezeNotice(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "EDIT-FREEZE", harness.Options{
		Guarantees: []string{"F-WRK-5", "R-22"},
		Title:      "an auto-parked chat's newest message is edited once to carry the freeze notice; a prompt sent while parked runs after the wake",
		Budget:     150 * time.Second,
		Seam:       harness.Seam{Knobs: map[string]string{"ParkAfter": "5s"}},
	})
	defer s.Finish()
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "freeze")
	tok, _, answered := ask(s, work.ID, "Please answer", "first")
	id := steps.SessionID(s, work.ID)
	final := answerMessage(s, work.ID, tok)
	fe, frozen := waitEdited(s, work.ID, final.ID, "This chat is paused", 45*time.Second)
	text := harness.NormalizeSpace(fe.PlainText())
	s.Check("the parked chat's newest message (the answer) carries the freeze notice", answered && frozen &&
		strings.Contains(text, tok) && strings.Contains(text, "Messages sent here while it is paused are read and run once it wakes.") &&
		strings.Contains(text, "Send: r "+id), "answer + notice with `r "+id+"`", fmt.Sprintf("answered=%v text=%q", answered, clip(text, 400)))

	held := s.Token("held")
	steps.MustPost(s, work.ID, "Typed while parked "+held)
	time.Sleep(3 * time.Second)
	ranEarly := len(s.Codex.CallsWithToken(held))
	if _, ok := command(s, control.ID, "r "+id, "Resumed "+id); !ok {
		return
	}
	_, heldAnswered := steps.WaitAnswered(s, work.ID, held, steps.AnswerTimeout)
	s.Check("a prompt sent while parked runs once, after the wake", ranEarly == 0 && heldAnswered && len(s.Codex.CallsWithToken(held)) == 1,
		"0 turns while parked, 1 after", fmt.Sprintf("before wake %d, answered=%v, turns %d", ranEarly, heldAnswered, len(s.Codex.CallsWithToken(held))))
	second := answerMessage(s, work.ID, held)
	_, refrozen := waitEdited(s, work.ID, second.ID, "This chat is paused", 45*time.Second)
	s.Settle(2*time.Second, 10*time.Second)
	notices, posted := 0, 0
	for _, m := range s.SUTMessages(work.ID) {
		if strings.Contains(m.PlainText(), "This chat is paused") {
			notices++
			if m.PatchCount == 0 {
				posted++
			}
		}
	}
	first, _ := message(s, work.ID, final.ID)
	s.Check("each park adds its notice once, by an edit", refrozen && notices == 2 && posted == 0 && first.PatchCount == 1,
		"2 notices (one per park), both edits, the first answer edited once",
		fmt.Sprintf("second=%v notices=%d posted=%d first edits=%d", refrozen, notices, posted, first.PatchCount))
}

// answerMessage is the helper's answer post carrying token.
func answerMessage(s *harness.Scenario, chat, token string) fakegraph.Message {
	var out fakegraph.Message
	for _, m := range s.SUTMessages(chat) {
		if strings.Contains(m.PlainText(), "answer for "+token) {
			out = m
		}
	}
	return out
}
