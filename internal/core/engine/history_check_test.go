package engine

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

// The labelled posts of the previous helpers (v0.1.20, rc.59) as the
// resume history check reads them: the kind from the label, the text
// without labels, "[part i/n]" and the completion footer.
func TestHistoryMessageLabels(t *testing.T) {
	for _, c := range []struct {
		text   string
		prompt bool
		want   string
		ok     bool
	}{
		{"🧑‍💻 User:\nfix the tests", true, "fixthetests", true},
		{"🧑‍💻 User [part 2/3]:\nrest of it", true, "restofit", true},
		{"🤖 ✅ Codex answer:\nDone.\n🔧 Helper: ✅ Codex finished responding.", false, "done", true},
		{"🤖 ✅ Codex answer [part 1/2]:\nfirst half", false, "firsthalf", true},
		{"🤖 ⏳ Codex status:\nworking", false, "", false},
		{"🔧 Helper:\nWork chat is ready.", false, "", false},
		{"fix the tests", false, "", false}, // typed in Teams: no label
	} {
		got, ok := historyMessage(channel.InboundMsg{Kind: channel.KindMessage, ByOwner: true, Text: c.text})
		if ok != c.ok || (ok && (got.prompt != c.prompt || got.text != c.want)) {
			t.Errorf("%q: got %+v %v, want prompt=%v %q %v", c.text, got, ok, c.prompt, c.want, c.ok)
		}
	}
	if _, ok := historyMessage(channel.InboundMsg{Kind: channel.KindMessage, Text: "🧑‍💻 User:\nfix"}); ok {
		t.Error("another account's post taken for the helper's")
	}
}

// historyResume: the resume point is the end of the newest record found;
// texts that differ in labels, footers, splits and truncation are found;
// a short record anchors nothing and counts after the anchor only as an
// equal text posted after the anchor's post.
func TestHistoryResume(t *testing.T) {
	long := strings.Repeat("the answer goes on and on ", 20)
	recs := []historyRec{
		{prompt: true, text: letters("first prompt of the session"), end: 10},
		{text: letters(long), end: 20},
		{prompt: true, text: letters("ok"), end: 30},
		{text: letters("a short answer after ok"), end: 40},
		{prompt: true, text: letters("yes"), end: 50},
	}
	at := func(m int) time.Time { return time.Date(2026, 9, 1, 10, m, 0, 0, time.UTC) }
	msg := func(prompt bool, s string, m int) historyMsg {
		return historyMsg{prompt: prompt, text: letters(s), at: at(m)}
	}
	for _, c := range []struct {
		name   string
		msgs   []historyMsg
		resume int64
	}{
		{"nothing posted", nil, 0},
		{"a short prompt alone anchors nothing", []historyMsg{msg(true, "ok", 1)}, 0},
		{"the first prompt", []historyMsg{msg(true, "first prompt of the session", 1)}, 10},
		{"truncated answer (first letters)", []historyMsg{msg(false, long[:100]+" (truncated)", 2)}, 20},
		{"second part of a split answer", []historyMsg{msg(false, long[200:], 2)}, 20},
		{"short answer with another footer", []historyMsg{msg(false, "a short answer after ok. Helper: done. @Owner", 4)}, 40},
		{"short answer inside another text", []historyMsg{msg(false, "see: a short answer after ok", 4)}, 0},
		{"short prompt after the anchor, posted after it", []historyMsg{msg(false, long, 2), msg(true, "ok", 3)}, 30},
		{"short prompt after the anchor, posted before it", []historyMsg{msg(true, "ok", 1), msg(false, long, 2)}, 20},
		{"wrong kind", []historyMsg{msg(false, "first prompt of the session", 1)}, 0},
		{"everything", []historyMsg{msg(true, "first prompt of the session", 1), msg(false, long, 2), msg(true, "ok", 3), msg(false, "a short answer after ok", 4), msg(true, "yes", 5)}, 50},
	} {
		if got, _ := historyResume(recs, c.msgs); got != c.resume {
			t.Errorf("%s: resume %d, want %d", c.name, got, c.resume)
		}
	}
}

// The matching is linear in the text read: a long chat and a long
// rollout are compared quickly.
func TestHistoryResumeLarge(t *testing.T) {
	var recs []historyRec
	var msgs []historyMsg
	for i := range 3000 {
		text := letters(fmt.Sprintf("%05d", i) + strings.Repeat(" record body", 100))
		recs = append(recs, historyRec{text: text, end: int64(i + 1)})
		if i < 2000 {
			msgs = append(msgs, historyMsg{text: text, at: time.Unix(int64(i), 0)})
		}
	}
	start := time.Now()
	if got, _ := historyResume(recs, msgs); got != 2000 {
		t.Fatalf("resume %d, want 2000", got)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("3000 records against 2000 posts took %s", d)
	}
}
