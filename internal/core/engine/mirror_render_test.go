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

// A mirrored prompt split into several messages is labelled "User [part
// i/n]:" in every part (v0.1.20: "🧑‍💻 User [part i/n]:"). It used to get an
// empty " [part i/n]:" header with "User:" inline in the first part only.
// A short prompt keeps the inline "User: <text>" form.
func TestMirrorSplitPromptLabelsEveryPart(t *testing.T) {
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

	short, err := e.renderMirror(ctx, "th", transcript.Output{RecordID: "r1", User: true, Text: "看一下这个问题"})
	if err != nil {
		t.Fatal(err)
	}
	if len(short) != 1 || !strings.HasPrefix(content(short[0].Body), "<p><strong>User:</strong> 看一下这个问题</p>") {
		t.Fatalf("short prompt: %+v", short)
	}

	var b strings.Builder
	b.WriteString("看一下下面这份反馈报告\n")
	for i := 0; i < 3000; i++ {
		b.WriteString("- 一行很长的说明文字，用来把这条提示撑到超过一条消息的大小\n")
	}
	long, err := e.renderMirror(ctx, "th", transcript.Output{RecordID: "r2", User: true, Text: b.String()})
	if err != nil {
		t.Fatal(err)
	}
	if len(long) < 2 {
		t.Fatalf("long prompt: %d parts, want several", len(long))
	}
	for i, m := range long {
		c := content(m.Body)
		want := fmt.Sprintf("<p><strong>User [part %d/%d]:</strong></p>", i+1, len(long))
		if !strings.HasPrefix(c, want) {
			t.Fatalf("part %d header: %.120s", i+1, c)
		}
		if strings.Contains(c, "<strong>User:</strong>") {
			t.Fatalf("part %d repeats the inline label: %.200s", i+1, c)
		}
		if m.Part != i+1 || m.Parts != len(long) || m.DeliveryKey != (transcript.Output{RecordID: "r2"}).Key("th", i+1) {
			t.Fatalf("part %d: %+v", i+1, m)
		}
	}
	if !strings.Contains(content(long[0].Body), "看一下下面这份反馈报告") {
		t.Fatal("first part lost the prompt's first line")
	}
}
