package teams

import (
	"strings"
	"testing"
	"time"
)

func TestSanitizeTopicRemovesCharactersRejectedByTeams(t *testing.T) {
	got := SanitizeTopic(`codex: fix / windows? installer*`)
	if strings.ContainsAny(got, `:<>"/\|?*`) {
		t.Fatalf("topic still has invalid characters: %q", got)
	}
	if got == "" {
		t.Fatal("expected non-empty topic")
	}
}

func TestSessionTopicIncludesTimestampAndRequest(t *testing.T) {
	now := time.Date(2026, 4, 30, 9, 26, 38, 0, time.Local)
	got := SessionTopic(now, "hello: world")
	if !strings.Contains(got, "2026-04-30 092638") {
		t.Fatalf("expected timestamp in topic, got %q", got)
	}
	if strings.Contains(got, ":") {
		t.Fatalf("topic should not include colon: %q", got)
	}
}

func TestPlainTextFromTeamsHTML(t *testing.T) {
	got := PlainTextFromTeamsHTML(`<p>Hello&nbsp;<strong>world</strong></p><DIV>next<BR />line</DIV>`)
	want := "Hello\u00a0world\nnext\nline"
	if got != want {
		t.Fatalf("unexpected text\n got: %q\nwant: %q", got, want)
	}
}

func TestCommandRoutePlainTextFromTeamsHTMLDropsQuotedBlocks(t *testing.T) {
	html := `<p>这是什么意思？</p><blockquote><p>beacon release req-b7ae41d2ab8f40d9aad910e6d3a46d04</p></blockquote>`
	got := CommandRoutePlainTextFromTeamsHTML(html)
	if got != "这是什么意思？" {
		t.Fatalf("route text = %q, want current message only", got)
	}
	if prompt := PlainTextFromTeamsHTML(html); !strings.Contains(prompt, "beacon release req-b7ae41d2ab8f40d9aad910e6d3a46d04") {
		t.Fatalf("prompt text should retain quoted context, got %q", prompt)
	}
}

func TestCommandRoutePlainTextFromTeamsHTMLDropsAttachmentPlaceholders(t *testing.T) {
	got := CommandRoutePlainTextFromTeamsHTML(`<p>answer this</p><attachment id="quote-1"></attachment>`)
	if got != "answer this" {
		t.Fatalf("route text = %q, want attachment placeholder removed", got)
	}
}

func TestHTMLMessageEscapesPrefixAndText(t *testing.T) {
	got := HTMLMessage(`codex <ready>`, `hello <script>alert("x")</script> & goodbye`)
	want := `<p><strong>codex &lt;ready&gt;:</strong> hello &lt;script&gt;alert(&#34;x&#34;)&lt;/script&gt; &amp; goodbye</p>`
	if got != want {
		t.Fatalf("unexpected html\n got: %q\nwant: %q", got, want)
	}
}

func TestIsHelperText(t *testing.T) {
	for _, text := range []string{"Codex: hello", "codex echo: ping", "codex-helper: ready"} {
		if !IsHelperText(text) {
			t.Fatalf("expected helper text: %q", text)
		}
	}
	if IsHelperText("new task") {
		t.Fatal("ordinary command detected as helper text")
	}
}
