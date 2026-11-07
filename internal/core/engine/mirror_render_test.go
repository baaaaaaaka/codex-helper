package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/channel/teams"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
)

// Mirrored records look as v0.1.20 posted them: a prompt under
// "🧑‍💻 User:" with the text after the label break, an answer under
// "🤖 ✅ Codex answer:" with "🔧 Helper: ✅ Codex finished responding." after
// the last part (no @mention in the work chat), and a long record labelled
// "[part i/n]" in every part. The new core used to post "User: <text>" inline
// and "Codex:" without the footer.
func TestMirrorRendersV0120Labels(t *testing.T) {
	ad, err := teams.New(teams.Config{Tokens: fidelityTokens{}})
	if err != nil {
		t.Fatal(err)
	}
	e := &Engine{ch: ad}
	ctx := context.Background()
	content := func(body string) string {
		var m struct {
			Body struct{ Content string } `json:"body"`
		}
		if err := json.Unmarshal([]byte(body), &m); err != nil {
			t.Fatal(err)
		}
		return m.Body.Content
	}
	const footer = "<p><strong>🔧 Helper:</strong> ✅ Codex finished responding.</p>"

	short, err := e.renderMirror(ctx, "th", transcript.Output{RecordID: "r1", User: true, Text: "看一下这个问题"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(short) != 1 || content(short[0].Body) != "<p><strong>🧑‍💻 User:</strong></p><p>看一下这个问题</p>" {
		t.Fatalf("short prompt: %q", content(short[0].Body))
	}

	ans, err := e.renderMirror(ctx, "th", transcript.Output{RecordID: "r3", Text: "Done."}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(ans) != 1 || content(ans[0].Body) != "<p><strong>🤖 ✅ Codex answer:</strong></p><p>Done.</p>"+footer {
		t.Fatalf("answer: %q", content(ans[0].Body))
	}
	if ans[0].Mention || strings.Contains(content(ans[0].Body), "<at") {
		t.Fatalf("mirrored answer mentions the owner in the work chat: %+v", ans[0])
	}

	var b strings.Builder
	b.WriteString("看一下下面这份反馈报告\n")
	for i := 0; i < 3000; i++ {
		b.WriteString("- 一行很长的说明文字，用来把这条提示撑到超过一条消息的大小\n")
	}
	for _, user := range []bool{true, false} {
		label := "🤖 ✅ Codex answer"
		if user {
			label = "🧑‍💻 User"
		}
		long, err := e.renderMirror(ctx, "th", transcript.Output{RecordID: "r2", User: user, Text: b.String()}, false)
		if err != nil {
			t.Fatal(err)
		}
		if len(long) < 2 {
			t.Fatalf("long record: %d parts, want several", len(long))
		}
		for i, m := range long {
			c := content(m.Body)
			want := fmt.Sprintf("<p><strong>%s [part %d/%d]:</strong></p>", label, i+1, len(long))
			if !strings.HasPrefix(c, want) {
				t.Fatalf("%s part %d header: %.120s", label, i+1, c)
			}
			last := i == len(long)-1
			if strings.HasSuffix(c, footer) != (last && !user) || strings.Count(c, "finished responding") > 1 {
				t.Fatalf("%s part %d footer: …%s", label, i+1, c[max(0, len(c)-200):])
			}
			if m.Part != i+1 || m.Parts != len(long) || m.DeliveryKey != (transcript.Output{RecordID: "r2"}).Key("th", i+1) {
				t.Fatalf("part %d: %+v", i+1, m)
			}
		}
		if !strings.Contains(content(long[0].Body), "看一下下面这份反馈报告") {
			t.Fatal("first part lost the record's first line")
		}
	}
}
