package teams

import (
	"strings"
	"testing"
	"time"
)

func TestRenderTeamsHTMLEscapesAndLabels(t *testing.T) {
	got := RenderTeamsHTML(TeamsRenderInput{
		Surface: TeamsRenderSurfaceTranscript,
		Kind:    TeamsRenderAssistant,
		Text:    `hello <b>world</b> & "team"`,
	})
	if !strings.Contains(got, `<strong>🤖 ✅ Codex answer:</strong>`) {
		t.Fatalf("missing assistant label: %s", got)
	}
	if strings.Contains(got, `<b>world</b>`) {
		t.Fatalf("raw HTML was not escaped: %s", got)
	}
	if !strings.Contains(got, `&lt;b&gt;world&lt;/b&gt; &amp; &#34;team&#34;`) {
		t.Fatalf("escaped text missing: %s", got)
	}

	code := RenderTeamsHTML(TeamsRenderInput{
		Surface: TeamsRenderSurfaceOutbox,
		Kind:    TeamsRenderCode,
		Text:    `fmt.Println("<safe>")`,
	})
	if !strings.Contains(code, `<strong>💻 Code:</strong>`) || !strings.Contains(code, `<pre><code>`) {
		t.Fatalf("code render missing label or block: %s", code)
	}
	if strings.Contains(code, `"<safe>"`) || !strings.Contains(code, `&#34;&lt;safe&gt;&#34;`) {
		t.Fatalf("code text was not escaped: %s", code)
	}
}

func TestRenderTeamsHTMLLabelsSupportedKinds(t *testing.T) {
	cases := []struct {
		kind  TeamsRenderKind
		label string
	}{
		{TeamsRenderUser, "🧑‍💻 User"},
		{TeamsRenderAssistant, "🤖 ✅ Codex answer"},
		{TeamsRenderProgress, "🤖 ⏳ Codex status"},
		{TeamsRenderHelper, "🔧 Helper"},
		{TeamsRenderStatus, "🤖 ⏳ Codex status"},
		{TeamsRenderCode, "💻 Code"},
		{TeamsRenderCommand, "🤖 🛠️ Codex command"},
	}
	for _, tc := range cases {
		got := RenderTeamsHTML(TeamsRenderInput{Kind: tc.kind, Text: "visible"})
		if !strings.Contains(got, "<strong>"+tc.label+":</strong>") {
			t.Fatalf("kind %q missing label %q in %s", tc.kind, tc.label, got)
		}
	}
}

func TestRenderTeamsHTMLProgressBreaksAfterLabel(t *testing.T) {
	got := PlainTextFromTeamsHTML(RenderTeamsHTML(TeamsRenderInput{
		Kind: TeamsRenderProgress,
		Text: "checking tests",
	}))
	if got != "🤖 ⏳ Codex status:\nchecking tests" {
		t.Fatalf("progress render = %q", got)
	}
}

func TestRenderTeamsHTMLStatusUsesCodexStatusLabel(t *testing.T) {
	got := PlainTextFromTeamsHTML(RenderTeamsHTML(TeamsRenderInput{
		Kind: TeamsRenderStatus,
		Text: "checking tests",
	}))
	if got != "🤖 ⏳ Codex status:\nchecking tests" {
		t.Fatalf("status render = %q", got)
	}
}

func TestRenderTeamsHTMLAssistantBreaksAfterAnswerLabel(t *testing.T) {
	got := PlainTextFromTeamsHTML(RenderTeamsHTML(TeamsRenderInput{
		Kind: TeamsRenderAssistant,
		Text: "final answer",
	}))
	if got != "🤖 ✅ Codex answer:\nfinal answer" {
		t.Fatalf("assistant render = %q", got)
	}
}

func TestRenderTeamsHTMLUserBreaksAfterLabel(t *testing.T) {
	got := PlainTextFromTeamsHTML(RenderTeamsHTML(TeamsRenderInput{
		Kind: TeamsRenderUser,
		Text: "original prompt",
	}))
	if got != "🧑‍💻 User:\noriginal prompt" {
		t.Fatalf("user render = %q", got)
	}
}

func TestRenderTeamsHTMLCodexMarkdownSubset(t *testing.T) {
	got := RenderTeamsHTML(TeamsRenderInput{
		Surface: TeamsRenderSurfaceTranscript,
		Kind:    TeamsRenderAssistant,
		Text: strings.Join([]string{
			"## Summary",
			"",
			"- **fixed** *rendering* with `safe <tag>` and ~~old~~",
			"- link [docs](https://example.com/a?x=1&y=2)",
			"- local [fake](./internal/teams/render.go:12)",
			"",
			"```go",
			`fmt.Println("<safe>")`,
			"```",
			"",
			"Raw <b>html</b>",
		}, "\n"),
	})
	for _, want := range []string{
		`<strong>🤖 ✅ Codex answer:</strong><br><strong>Summary</strong>`,
		`<ul><li><strong>fixed</strong>`,
		`<em>rendering</em>`,
		`<code>safe &lt;tag&gt;</code>`,
		`<s>old</s>`,
		`<li>link <a href="https://example.com/a?x=1&amp;y=2">docs</a> (https://example.com/a?x=1&amp;y=2)</li>`,
		`<li>local <code>./internal/teams/render.go:12</code></li></ul>`,
		`<pre><code>fmt.Println(&#34;&lt;safe&gt;&#34;)</code></pre>`,
		`Raw &lt;b&gt;html&lt;/b&gt;`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("rendered markdown missing %q in:\n%s", want, got)
		}
	}
	if strings.Contains(got, `<a href="./internal/teams/render.go:12"`) {
		t.Fatalf("local file link should not become clickable href: %s", got)
	}
}

func TestRenderTeamsHTMLCodexMarkdownPreservesFencedBlankLines(t *testing.T) {
	got := RenderTeamsHTML(TeamsRenderInput{
		Kind: TeamsRenderAssistant,
		Text: "before\n\n```\nline 1\n\nline 3\n```\n\nafter",
	})
	if !strings.Contains(got, "<pre><code>line 1\n\nline 3</code></pre>") {
		t.Fatalf("fenced code blank line was not preserved:\n%s", got)
	}
	plain := PlainTextFromTeamsHTML(got)
	if !strings.Contains(plain, "before\nline 1\n\nline 3\nafter") {
		t.Fatalf("plain text should keep code block readable, got %q", plain)
	}
}

func TestRenderTeamsHTMLCodexMarkdownKeepsIndentedNestedListsReadable(t *testing.T) {
	got := RenderTeamsHTML(TeamsRenderInput{
		Kind: TeamsRenderAssistant,
		Text: "1. parent\n   - nested\n   - more",
	})
	if !strings.Contains(got, "<ol><li>parent<br><ul><li>nested</li><li>more</li></ul></li></ol>") {
		t.Fatalf("nested list should render as nested HTML lists, got %q", got)
	}
}

func TestPlanTeamsHTMLChunksStaysUnderHardLimitAndOrdersParts(t *testing.T) {
	text := strings.Repeat("<&>", 30000)
	chunks := PlanTeamsHTMLChunks(TeamsRenderInput{
		Surface: TeamsRenderSurfaceOutbox,
		Kind:    TeamsRenderAssistant,
		Text:    text,
	}, TeamsRenderOptions{})
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(chunks))
	}
	for i, chunk := range chunks {
		if chunk.PartIndex != i+1 {
			t.Fatalf("chunk %d PartIndex = %d", i, chunk.PartIndex)
		}
		if chunk.PartCount != len(chunks) {
			t.Fatalf("chunk %d PartCount = %d, want %d", i, chunk.PartCount, len(chunks))
		}
		wantLabel := "🤖 ✅ Codex answer [part " + strconvItoa(i+1) + "/" + strconvItoa(len(chunks)) + "]"
		if chunk.Label != wantLabel {
			t.Fatalf("chunk %d label = %q, want %q", i, chunk.Label, wantLabel)
		}
		if chunk.ByteLength != len(chunk.HTML) {
			t.Fatalf("chunk %d ByteLength = %d, len(HTML) = %d", i, chunk.ByteLength, len(chunk.HTML))
		}
		if chunk.ByteLength > TeamsRenderHardLimitBytes {
			t.Fatalf("chunk %d rendered to %d bytes, want <= %d", i, chunk.ByteLength, TeamsRenderHardLimitBytes)
		}
	}
}

func TestPlanTeamsHTMLChunksUsesRenderedByteSize(t *testing.T) {
	text := strings.Repeat("<&>", 100)
	chunks := PlanTeamsHTMLChunks(TeamsRenderInput{Kind: TeamsRenderHelper, Text: text}, TeamsRenderOptions{
		TargetLimitBytes: 240,
		HardLimitBytes:   260,
	})
	if len(chunks) < 2 {
		t.Fatalf("expected HTML escaping to force multiple chunks, got %d", len(chunks))
	}
	for i, chunk := range chunks {
		if chunk.ByteLength > 260 {
			t.Fatalf("chunk %d rendered to %d bytes", i, chunk.ByteLength)
		}
	}
}

func TestRenderDoesNotFilterVisibleManifestText(t *testing.T) {
	visible := "done\n{\"version\":1,\"files\":[{\"path\":\"result.txt\"}]}"
	got := PlainTextFromTeamsHTML(RenderTeamsHTML(TeamsRenderInput{
		Kind: TeamsRenderAssistant,
		Text: visible,
	}))
	if !strings.Contains(got, "done") || !strings.Contains(got, `"files"`) || !strings.Contains(got, "result.txt") {
		t.Fatalf("visible artifact-looking text was filtered: %q", got)
	}
}

func TestRenderTeamsHTMLUsesParagraphsForBlankLines(t *testing.T) {
	got := RenderTeamsHTML(TeamsRenderInput{
		Kind: TeamsRenderHelper,
		Text: "first\n\nsecond\nthird",
	})
	if strings.Contains(got, "<br><br>") {
		t.Fatalf("blank lines should become paragraphs, not repeated br tags: %s", got)
	}
	if !strings.Contains(got, `<p><strong>🔧 Helper:</strong><br>first</p><p>second<br>third</p>`) {
		t.Fatalf("unexpected paragraph rendering: %s", got)
	}
	plain := PlainTextFromTeamsHTML(got)
	if strings.Contains(plain, "\n\n\n") {
		t.Fatalf("plain text has excessive blank lines: %q", plain)
	}
}

func TestRenderTeamsFreezeNoticeHTML(t *testing.T) {
	got := renderTeamsFreezeNoticeHTML("https://teams.microsoft.com/l/chat/chat-id/conversations", "r 8f3c9a2d", "Your Codex work is safe. Paused after 6h idle.")
	for _, want := range []string{
		`<strong>🔧 Helper:</strong><br>🧊 This chat is paused`,
		`⚠ <strong>Messages here will not get a reply.</strong>`,
		`<p>&nbsp;</p>`,
		`▶️ <strong>Continue chat:</strong>`,
		`Step 1: Open <a href="https://teams.microsoft.com/l/chat/chat-id/conversations">Control chat</a>`,
		`Step 2: Send: <code>r 8f3c9a2d</code>`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("freeze notice missing %q in:\n%s", want, got)
		}
	}
	plain := PlainTextFromTeamsHTML(got)
	if strings.Contains(plain, "https://teams.microsoft.com") {
		t.Fatalf("freeze notice plain text leaked raw URL:\n%s", plain)
	}
	for _, want := range []string{
		"🧊 This chat is paused",
		"⚠ Messages here will not get a reply.",
		"▶️ Continue chat:",
		"Step 1: Open Control chat",
		"Step 2: Send: r 8f3c9a2d",
	} {
		if !strings.Contains(plain, want) {
			t.Fatalf("freeze notice plain text missing %q in:\n%s", want, plain)
		}
	}
}

func TestOwnerMentionPolicy(t *testing.T) {
	noMention := []OwnerNotificationEvent{
		OwnerNotificationACK,
		OwnerNotificationImport,
		OwnerNotificationHistory,
		OwnerNotificationStatus,
	}
	for _, event := range noMention {
		if ShouldMentionOwner(OwnerMentionPolicyInput{Event: event, Duration: 5 * time.Minute}) {
			t.Fatalf("event %q should not mention owner", event)
		}
	}
	if !ShouldMentionOwner(OwnerMentionPolicyInput{
		Event:    OwnerNotificationCompletion,
		Duration: time.Minute,
	}) {
		t.Fatal("long-running completion should mention owner")
	}
	if ShouldMentionOwner(OwnerMentionPolicyInput{
		Event:    OwnerNotificationCompletion,
		Duration: 59 * time.Second,
	}) {
		t.Fatal("short completion should not mention owner")
	}
	if ShouldMentionOwner(OwnerMentionPolicyInput{
		Event:     OwnerNotificationCompletion,
		Duration:  time.Minute,
		PartIndex: 2,
		PartCount: 3,
	}) {
		t.Fatal("later chunks should not mention owner")
	}
	if ShouldMentionOwner(OwnerMentionPolicyInput{
		Event:            OwnerNotificationCompletion,
		Duration:         time.Minute,
		AlreadyMentioned: true,
	}) {
		t.Fatal("already-mentioned turn should not mention owner again")
	}
}
