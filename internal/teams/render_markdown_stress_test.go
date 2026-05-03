package teams

import (
	"context"
	"html"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestRenderTeamsHTMLCodexMarkdownStressCorpus(t *testing.T) {
	cases := []struct {
		name          string
		kind          TeamsRenderKind
		text          string
		wantHTML      []string
		wantPlain     []string
		forbidHTML    []string
		forbidPlain   []string
		wantPreBlocks int
	}{
		{
			name: "mixed blocks raw html nested lists and code",
			kind: TeamsRenderAssistant,
			text: strings.Join([]string{
				"# Result",
				"",
				"> quoted <b>html</b>",
				"- first item",
				"    - nested item with **bold** and `x < y`",
				"1. ordered",
				"   1. nested ordered",
				"",
				"---",
				"",
				"```go",
				`fmt.Println("<safe>")`,
				"```",
				"",
				"| a | b |",
				"| - | - |",
				"| <x> | **y** |",
			}, "\n"),
			wantHTML: []string{
				`<strong>🤖 ✅ Codex answer:</strong><br><strong>Result</strong>`,
				`&gt; quoted &lt;b&gt;html&lt;/b&gt;`,
				`<ul><li>first item<br><ul><li>nested item with <strong>bold</strong> and <code>x &lt; y</code></li></ul></li></ul>`,
				`<ol><li>ordered<br><ol><li>nested ordered</li></ol></li></ol>`,
				`———`,
				`<pre><code>fmt.Println(&#34;&lt;safe&gt;&#34;)</code></pre>`,
				`<table><tr><th>a</th><th>b</th></tr>`,
				`<tr><td>&lt;x&gt;</td><td><strong>y</strong></td></tr>`,
			},
			wantPlain:     []string{"🤖 ✅ Codex answer:\nResult", "> quoted <b>html</b>", "first item\nnested item with bold and x < y", "ordered\nnested ordered", "fmt.Println(\"<safe>\")", "<x>\ty"},
			forbidHTML:    []string{`<b>html</b>`, `fmt.Println("<safe>")`},
			wantPreBlocks: 1,
		},
		{
			name: "first block is fenced code",
			kind: TeamsRenderAssistant,
			text: "```sh\nprintf '<hello>'\n```\n\nafter",
			wantHTML: []string{
				`<p><strong>🤖 ✅ Codex answer:</strong></p><pre><code>printf &#39;&lt;hello&gt;&#39;</code></pre>`,
				`<p>after</p>`,
			},
			wantPlain:     []string{"🤖 ✅ Codex answer:\nprintf '<hello>'\nafter"},
			wantPreBlocks: 1,
		},
		{
			name: "unclosed fence treats tail as escaped code",
			kind: TeamsRenderAssistant,
			text: "before\n\n```python\nprint('<tail>')\n**not bold**",
			wantHTML: []string{
				`before`,
				"<pre><code>print(&#39;&lt;tail&gt;&#39;)\n**not bold**</code></pre>",
				`**not bold**`,
			},
			forbidHTML:    []string{`<tail>`, `<strong>not bold</strong>`},
			wantPlain:     []string{"before", "print('<tail>')", "**not bold**"},
			wantPreBlocks: 1,
		},
		{
			name: "indented code only",
			kind: TeamsRenderProgress,
			text: "    go test ./...\n    echo '<done>'",
			wantHTML: []string{
				`<p><strong>🤖 ⏳ Codex status:</strong></p><pre><code>go test ./...`,
				`echo &#39;&lt;done&gt;&#39;</code></pre>`,
			},
			wantPlain:     []string{"🤖 ⏳ Codex status:\ngo test ./...\necho '<done>'"},
			wantPreBlocks: 1,
		},
		{
			name: "inline nesting escapes and intraword underscores",
			kind: TeamsRenderAssistant,
			text: `**outer *em* ~~gone ` + "`literal <x> **not bold**`" + `~~** \*literal stars\* snake_case foo_bar_baz 2*3*4`,
			wantHTML: []string{
				`<strong>outer <em>em</em> <s>gone <code>literal &lt;x&gt; **not bold**</code></s></strong>`,
				`*literal stars*`,
				`snake_case`,
				`foo_bar_baz`,
				`2<em>3</em>4`,
			},
			wantPlain: []string{"outer em gone literal <x> **not bold**", "*literal stars*", "snake_case", "foo_bar_baz", "2", "3", "4"},
			forbidHTML: []string{
				`<x>`,
				`<em>case</em>`,
				`<em>bar</em>`,
			},
		},
		{
			name: "links local paths autolinks and unsafe schemes",
			kind: TeamsRenderAssistant,
			text: strings.Join([]string{
				`[docs](https://example.com/path_(with)_parens?x=1&y=2)`,
				`[same](https://example.com/same) https://plain.example/path`,
				`wrapped (https://example.com/path(foo)). xhttps://not-linked.example`,
				`<https://example.org/raw?a=1&b=2>`,
				`[mail](mailto:user@example.com)`,
				`[local](file:///tmp/a%20b.go#L12C3)`,
				`[win](C:\Users\jason\file.go:9)`,
				`[unsafe](javascript:alert(1))`,
				`<javascript:alert(1)>`,
				`[space](https://example.com/a b)`,
				`[nohost](https:///missing-host)`,
			}, "\n"),
			wantHTML: []string{
				`<a href="https://example.com/path_(with)_parens?x=1&amp;y=2">docs</a> (https://example.com/path_(with)_parens?x=1&amp;y=2)`,
				`<a href="https://plain.example/path">https://plain.example/path</a>`,
				`wrapped (<a href="https://example.com/path(foo)">https://example.com/path(foo)</a>). xhttps://not-linked.example`,
				`<a href="https://example.org/raw?a=1&amp;b=2">https://example.org/raw?a=1&amp;b=2</a>`,
				`<a href="mailto:user@example.com">mail</a> (mailto:user@example.com)`,
				`<code>/tmp/a b.go#L12C3</code>`,
				`<code>C:/Users/jason/file.go:9</code>`,
				`unsafe (javascript:alert(1))`,
				`&lt;javascript:alert(1)&gt;`,
				`space (https://example.com/a b)`,
				`nohost (https:///missing-host)`,
			},
			wantPlain: []string{"/tmp/a b.go#L12C3", "C:/Users/jason/file.go:9", "unsafe (javascript:alert(1))", "<javascript:alert(1)>"},
			forbidHTML: []string{
				`href="javascript:`,
				`href="file:`,
				`href="C:`,
				`href="https:///missing-host"`,
				`href="https://not-linked.example"`,
			},
		},
		{
			name: "unicode invalid bytes and raw html",
			kind: TeamsRenderAssistant,
			text: "中文 ✅ café e\u0301 " + string([]byte{0xff}) + " <script>alert('x')</script>",
			wantHTML: []string{
				`中文 ✅ café e`,
				`&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt;`,
			},
			wantPlain:  []string{"中文 ✅ café", "<script>alert('x')</script>"},
			forbidHTML: []string{`<script>`, `</script>`},
		},
		{
			name: "malformed markdown remains readable",
			kind: TeamsRenderAssistant,
			text: "####### not heading\n[broken](https://example.com\n`unterminated code\n~~unterminated strike\n![image <x>](https://example.com/img.png)",
			wantHTML: []string{
				`####### not heading`,
				`[broken](https://example.com`,
				"`unterminated code",
				`~~unterminated strike`,
				`Image: <a href="https://example.com/img.png">image &lt;x&gt;</a> (https://example.com/img.png)`,
			},
			wantPlain:  []string{"####### not heading", "[broken](https://example.com", "`unterminated code", "~~unterminated strike", "Image: image <x> (https://example.com/img.png)"},
			forbidHTML: []string{`<x>`},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RenderTeamsHTML(TeamsRenderInput{
				Surface: TeamsRenderSurfaceTranscript,
				Kind:    tc.kind,
				Text:    tc.text,
			})
			assertTeamsRenderedHTMLSafe(t, got)
			for _, want := range tc.wantHTML {
				if !strings.Contains(got, want) {
					t.Fatalf("rendered HTML missing %q in:\n%s", want, got)
				}
			}
			for _, forbidden := range tc.forbidHTML {
				if strings.Contains(got, forbidden) {
					t.Fatalf("rendered HTML contains forbidden %q in:\n%s", forbidden, got)
				}
			}
			if tc.wantPreBlocks >= 0 && strings.Count(got, "<pre><code>") != tc.wantPreBlocks {
				t.Fatalf("pre block count = %d, want %d in:\n%s", strings.Count(got, "<pre><code>"), tc.wantPreBlocks, got)
			}
			plain := PlainTextFromTeamsHTML(got)
			for _, want := range tc.wantPlain {
				if !strings.Contains(plain, want) {
					t.Fatalf("plain text missing %q in:\n%s", want, plain)
				}
			}
			for _, forbidden := range tc.forbidPlain {
				if strings.Contains(plain, forbidden) {
					t.Fatalf("plain text contains forbidden %q in:\n%s", forbidden, plain)
				}
			}
		})
	}
}

func TestRenderTeamsHTMLCodexMarkdownInlineStressBothCodexKinds(t *testing.T) {
	cases := []struct {
		name       string
		input      string
		contains   []string
		forbidden  []string
		plainParts []string
	}{
		{
			name:     "same delimiter nesting through strike",
			input:    `**outer ~~strike **inner**~~ end**`,
			contains: []string{`<strong>outer <s>strike <strong>inner</strong></s> end</strong>`},
		},
		{
			name:     "underscore word boundaries",
			input:    `Keep snake_case and mid_word_value, but _em_ and __strong__ render.`,
			contains: []string{`snake_case`, `mid_word_value`, `<em>em</em>`, `<strong>strong</strong>`},
			forbidden: []string{
				`snake<em>case`,
				`mid<em>word`,
			},
		},
		{
			name:      "unmatched delimiters stay readable",
			input:     `Unclosed **bold and ~~strike and *em`,
			contains:  []string{`Unclosed **bold and ~~strike and *em`},
			forbidden: []string{`<strong>bold`, `<s>strike`, `<em>em`},
		},
		{
			name:      "inline code suppresses markdown and html",
			input:     "Before `**not bold** <tag> [x](javascript:alert(1))` after",
			contains:  []string{`<code>**not bold** &lt;tag&gt; [x](javascript:alert(1))</code>`},
			forbidden: []string{`<strong>not bold</strong>`, `<a href="javascript:`},
		},
		{
			name:     "double backticks contain single backtick",
			input:    "Use ``code with ` tick and <x>`` ok",
			contains: []string{"<code>code with ` tick and &lt;x&gt;</code>"},
		},
		{
			name:      "escaped markdown punctuation",
			input:     "Escaped \\[not link\\]\\(x\\), \\# heading, \\*not em\\*, \\`not code\\`",
			contains:  []string{"Escaped [not link](x), # heading, *not em*, `not code`"},
			forbidden: []string{`<a href=`, `<em>not em</em>`, `<code>not code</code>`},
		},
		{
			name:     "safe autolinks",
			input:    `See <https://example.com/a?x=1&y=2> and <mailto:user+tag@example.com>.`,
			contains: []string{`<a href="https://example.com/a?x=1&amp;y=2">https://example.com/a?x=1&amp;y=2</a>`, `<a href="mailto:user+tag@example.com">mailto:user+tag@example.com</a>`},
		},
		{
			name:      "unsafe autolinks stay text",
			input:     `Bad <javascript:alert(1)> <data:text/html,<b>x</b>> <ftp://example.com/file>`,
			contains:  []string{`&lt;javascript:alert(1)&gt;`, `&lt;data:text/html,`, `&lt;ftp://example.com/file&gt;`},
			forbidden: []string{`<a href="javascript:`, `<a href="data:`, `<a href="ftp:`, `<b>x</b>`},
		},
		{
			name:     "markdown link balanced parentheses",
			input:    `[spec](https://example.com/path(foo)/bar?q=(x)&ok=1)`,
			contains: []string{`<a href="https://example.com/path(foo)/bar?q=(x)&amp;ok=1">spec</a> (https://example.com/path(foo)/bar?q=(x)&amp;ok=1)`},
		},
		{
			name:      "markdown link angle destination strips title",
			input:     `[docs]( <https://example.com/a(b)?q=1&x=2> "ignored title" )`,
			contains:  []string{`<a href="https://example.com/a(b)?q=1&amp;x=2">docs</a> (https://example.com/a(b)?q=1&amp;x=2)`},
			forbidden: []string{`ignored title`},
		},
		{
			name:      "local and unsafe markdown links",
			input:     `[rel](./internal/teams/render.go:12) [unc](\\server\share\report.md) [js](javascript:alert(1)) [relative](notes.md) [quote](https://example.com/"bad")`,
			contains:  []string{`<code>./internal/teams/render.go:12</code>`, `<code>//server/share/report.md</code>`, `js (javascript:alert(1))`, `relative (notes.md)`, `quote (https://example.com/&#34;bad&#34;)`},
			forbidden: []string{`href="./internal`, `href="javascript:`, `href="notes.md`, `href="https://example.com/&#34;bad`},
		},
	}

	for _, tc := range cases {
		for _, kind := range []TeamsRenderKind{TeamsRenderAssistant, TeamsRenderProgress} {
			t.Run(string(kind)+"/"+tc.name, func(t *testing.T) {
				got := RenderTeamsHTML(TeamsRenderInput{Kind: kind, Text: tc.input})
				assertTeamsRenderedHTMLSafe(t, got)
				for _, want := range tc.contains {
					if !strings.Contains(got, want) {
						t.Fatalf("rendered HTML missing %q in:\n%s", want, got)
					}
				}
				for _, forbidden := range tc.forbidden {
					if strings.Contains(got, forbidden) {
						t.Fatalf("rendered HTML contains forbidden %q in:\n%s", forbidden, got)
					}
				}
				plain := PlainTextFromTeamsHTML(got)
				for _, want := range tc.plainParts {
					if !strings.Contains(plain, want) {
						t.Fatalf("plain text missing %q in:\n%s", want, plain)
					}
				}
			})
		}
	}
}

func TestRenderTeamsHTMLCodexMarkdownBlockStressBothCodexKinds(t *testing.T) {
	cases := []struct {
		name       string
		input      string
		contains   []string
		forbidden  []string
		preBlocks  int
		plainParts []string
	}{
		{
			name:  "headings and malformed headings",
			input: "# H1\n## H2 **bold** ##\n### H3 `x < y`\n#### H4\n###### H6\n####### literal\n#literal",
			contains: []string{
				`<strong>H1</strong>`,
				`<strong>H2 <strong>bold</strong></strong>`,
				`<strong><em>H3 <code>x &lt; y</code></em></strong>`,
				`<em>H4</em>`,
				`<em>H6</em>`,
				`####### literal<br>#literal`,
			},
			forbidden: []string{`<strong># H1</strong>`, `## H2`},
		},
		{
			name:      "tilde and longer fences",
			input:     "~~~~markdown\n```not end\n~~~\nstill code\n~~~~\nafter",
			contains:  []string{"<pre><code>```not end\n~~~\nstill code</code></pre>", "<p>after</p>"},
			preBlocks: 1,
		},
		{
			name:      "closing fence with trailing text stays code",
			input:     "```go\none\n``` still code\ntwo\n```\ntail",
			contains:  []string{"<pre><code>one\n``` still code\ntwo</code></pre>", "<p>tail</p>"},
			preBlocks: 1,
		},
		{
			name: "fenced code inside list item stays code blocks",
			input: strings.Join([]string{
				"1. **Control Chat**",
				"   一个固定 chat，比如：",
				"",
				"   ```text",
				"   codex-helper control",
				"   ```",
				"",
				"   你在里面发管理命令：",
				"",
				"   ```text",
				"   new fix installer bug",
				"   list",
				"   status",
				"   resume 3",
				"   archive 3",
				"   ```",
				"",
				"  最小 Python 实现骨架",
				"",
				"  import time, json, os, requests",
				"",
				"  def graph(method, path, **kwargs):",
				"      t = token()[\"access_token\"]",
				"      headers = kwargs.pop(\"headers\", {})",
				"      return r.json() if r.text else {}",
				"",
				"  Graph Teams API 对应 MIAO 的调用",
			}, "\n"),
			contains: []string{
				"<ol><li><strong>Control Chat</strong>",
				"<pre><code>codex-helper control</code></pre>",
				"<pre><code>new fix installer bug\nlist\nstatus\nresume 3\narchive 3</code></pre>",
				"<pre><code>import time, json, os, requests\n\ndef graph(method, path, **kwargs):\n    t = token()[&#34;access_token&#34;]",
			},
			preBlocks:  3,
			plainParts: []string{"Control Chat", "codex-helper control", "new fix installer bug\nlist\nstatus\nresume 3\narchive 3", "def graph(method, path, **kwargs):\nt = token()[\"access_token\"]"},
		},
		{
			name:       "crlf inside fenced code",
			input:      "```text\r\nline 1\r\n\r\nline 3\r\n```",
			contains:   []string{"<pre><code>line 1\n\nline 3</code></pre>"},
			forbidden:  []string{"\r"},
			preBlocks:  1,
			plainParts: []string{"line 1\n\nline 3"},
		},
		{
			name:       "indented code preserves internal blank",
			input:      "Intro\n\n    line 1\n\n    line 3\n\nDone",
			contains:   []string{"Intro", "<pre><code>line 1\n\nline 3</code></pre>", "<p>Done</p>"},
			preBlocks:  1,
			plainParts: []string{"Intro", "line 1\n\nline 3", "Done"},
		},
		{
			name: "two space pasted python code is kept together",
			input: strings.Join([]string{
				"  最小 Python 实现骨架",
				"",
				"  import time, json, os, requests",
				"",
				"  TENANT = \"example-tenant-id\"",
				"  CLIENT_ID = \"example-client-id\"",
				"",
				"  def graph(method, path, **kwargs):",
				"      t = token()[\"access_token\"]",
				"      headers = kwargs.pop(\"headers\", {})",
				"      headers[\"Authorization\"] = f\"Bearer {t}\"",
				"      if \"json\" in kwargs:",
				"          headers[\"Content-Type\"] = \"application/json\"",
				"      r = requests.request(method, \"https://graph.microsoft.com/v1.0\" + path,",
				"                           headers=headers, timeout=30, **kwargs)",
				"      r.raise_for_status()",
				"      return r.json() if r.text else {}",
				"",
				"  Graph Teams API 对应 MIAO 的调用",
			}, "\n"),
			contains: []string{
				"最小 Python 实现骨架",
				"<pre><code>import time, json, os, requests\n\nTENANT = &#34;example-tenant-id&#34;",
				"def graph(method, path, **kwargs):\n    t = token()[&#34;access_token&#34;]",
				"    return r.json() if r.text else {}</code></pre>",
				"Graph Teams API 对应 MIAO 的调用",
			},
			preBlocks:  1,
			plainParts: []string{"最小 Python 实现骨架", "def graph(method, path, **kwargs):\nt = token()[\"access_token\"]", "Graph Teams API 对应 MIAO 的调用"},
		},
		{
			name:       "nested list after blank remains readable",
			input:      "- parent\n\n    - child after blank\n    continuation",
			contains:   []string{"<ul><li>parent<br><ul><li>child after blank<br>continuation</li></ul></li></ul>"},
			forbidden:  []string{"<pre><code>- child"},
			preBlocks:  0,
			plainParts: []string{"parent", "child after blank\ncontinuation"},
		},
		{
			name:       "blockquote markers stay visible",
			input:      "> quoted **bold**\n> \n> - item\n>> nested\nnormal",
			contains:   []string{`&gt; quoted <strong>bold</strong>`, `&gt; - item<br>&gt;&gt; nested<br>normal`},
			preBlocks:  0,
			plainParts: []string{"> quoted bold", "> - item", ">> nested", "normal"},
		},
		{
			name:       "rules and non rules",
			input:      "before\n\n---\nafter\n\n* * *\n___\n- - -\nnot --- rule",
			contains:   []string{"before", "———", "after", "not --- rule"},
			forbidden:  []string{"<p>---</p>", "<p>* * *</p>", "<p>___</p>"},
			preBlocks:  0,
			plainParts: []string{"before", "———", "after", "not --- rule"},
		},
	}

	for _, tc := range cases {
		for _, kind := range []TeamsRenderKind{TeamsRenderAssistant, TeamsRenderProgress} {
			t.Run(string(kind)+"/"+tc.name, func(t *testing.T) {
				got := RenderTeamsHTML(TeamsRenderInput{Kind: kind, Text: tc.input})
				assertTeamsRenderedHTMLSafe(t, got)
				for _, want := range tc.contains {
					if !strings.Contains(got, want) {
						t.Fatalf("rendered HTML missing %q in:\n%s", want, got)
					}
				}
				for _, forbidden := range tc.forbidden {
					if strings.Contains(got, forbidden) {
						t.Fatalf("rendered HTML contains forbidden %q in:\n%s", forbidden, got)
					}
				}
				if strings.Count(got, "<pre><code>") != tc.preBlocks {
					t.Fatalf("pre block count = %d, want %d in:\n%s", strings.Count(got, "<pre><code>"), tc.preBlocks, got)
				}
				plain := PlainTextFromTeamsHTML(got)
				for _, want := range tc.plainParts {
					if !strings.Contains(plain, want) {
						t.Fatalf("plain text missing %q in:\n%s", want, plain)
					}
				}
			})
		}
	}
}

func TestRenderTeamsHTMLCodexMarkdownListsHeadingsAndBareURLSafety(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		contains  []string
		forbidden []string
	}{
		{
			name: "headings lists and bare urls",
			input: strings.Join([]string{
				"## Tasks ##",
				"",
				"- **inspect** renderer",
				"- link https://example.com/a?x=1&y=2.",
				"  wrapped continuation",
				"1. first",
				"2) second [docs](https://example.com/docs)",
			}, "\n"),
			contains: []string{
				`<strong>Tasks</strong>`,
				`<ul><li><strong>inspect</strong> renderer</li><li>link <a href="https://example.com/a?x=1&amp;y=2">https://example.com/a?x=1&amp;y=2</a>.<br>&nbsp;&nbsp;wrapped continuation</li></ul>`,
				`<ol><li>first</li><li>second <a href="https://example.com/docs">docs</a> (https://example.com/docs)</li></ol>`,
			},
			forbidden: []string{`## Tasks`, `href="https://example.com/a?x=1&amp;y=2."`},
		},
		{
			name: "unsafe links in list items stay non clickable",
			input: strings.Join([]string{
				"- file [local](file:///tmp/render.go#L9)",
				"- bad [js](javascript:alert(1)) and https:///missing",
				"- raw <b>html</b>",
			}, "\n"),
			contains: []string{
				`<ul><li>file <code>/tmp/render.go#L9</code></li>`,
				`<li>bad js (javascript:alert(1)) and https:///missing</li>`,
				`<li>raw &lt;b&gt;html&lt;/b&gt;</li></ul>`,
			},
			forbidden: []string{`href="file:`, `href="javascript:`, `<b>html</b>`, `href="https:///missing"`},
		},
		{
			name: "malformed markdown link does not become a partial bare link",
			input: strings.Join([]string{
				"[broken](https://example.com",
				"(https://example.org/wrapped).",
			}, "\n"),
			contains: []string{
				`[broken](https://example.com`,
				`(<a href="https://example.org/wrapped">https://example.org/wrapped</a>).`,
			},
			forbidden: []string{`[broken](<a href=`},
		},
	}

	for _, tc := range cases {
		for _, kind := range []TeamsRenderKind{TeamsRenderAssistant, TeamsRenderProgress} {
			t.Run(string(kind)+"/"+tc.name, func(t *testing.T) {
				got := RenderTeamsHTML(TeamsRenderInput{Kind: kind, Text: tc.input})
				assertTeamsRenderedHTMLSafe(t, got)
				for _, want := range tc.contains {
					if !strings.Contains(got, want) {
						t.Fatalf("rendered HTML missing %q in:\n%s", want, got)
					}
				}
				for _, forbidden := range tc.forbidden {
					if strings.Contains(got, forbidden) {
						t.Fatalf("rendered HTML contains forbidden %q in:\n%s", forbidden, got)
					}
				}
			})
		}
	}
}

func TestRenderTeamsHTMLCodexMarkdownTaskListAndImageTextFallback(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		contains  []string
		forbidden []string
	}{
		{
			name: "task list markers become text checkboxes",
			input: strings.Join([]string{
				"- [ ] write tests",
				"- [x] fix renderer",
				"1. [X] ship safely",
			}, "\n"),
			contains: []string{
				`<ul><li>☐ write tests</li><li>☑ fix renderer</li></ul>`,
				`<ol><li>☑ ship safely</li></ol>`,
			},
			forbidden: []string{`<input`, `type="checkbox"`},
		},
		{
			name: "images become safe text links or paths",
			input: strings.Join([]string{
				"![plot <x>](https://example.com/plot.png)",
				"![local](file:///tmp/plot.png)",
				"![bad](javascript:alert(1))",
				"![missing](https:///missing-host)",
			}, "\n"),
			contains: []string{
				`Image: <a href="https://example.com/plot.png">plot &lt;x&gt;</a> (https://example.com/plot.png)`,
				`Image: <code>/tmp/plot.png</code>`,
				`Image: bad (javascript:alert(1))`,
				`Image: missing (https:///missing-host)`,
			},
			forbidden: []string{`<img`, `src=`, `href="javascript:`, `href="file:`, `href="https:///missing-host"`},
		},
	}

	for _, tc := range cases {
		for _, kind := range []TeamsRenderKind{TeamsRenderAssistant, TeamsRenderProgress} {
			t.Run(string(kind)+"/"+tc.name, func(t *testing.T) {
				got := RenderTeamsHTML(TeamsRenderInput{Kind: kind, Text: tc.input})
				assertTeamsRenderedHTMLSafe(t, got)
				for _, want := range tc.contains {
					if !strings.Contains(got, want) {
						t.Fatalf("rendered HTML missing %q in:\n%s", want, got)
					}
				}
				for _, forbidden := range tc.forbidden {
					if strings.Contains(got, forbidden) {
						t.Fatalf("rendered HTML contains forbidden %q in:\n%s", forbidden, got)
					}
				}
			})
		}
	}
}

func TestRenderTeamsHTMLCodexMarkdownTableStressBothCodexKinds(t *testing.T) {
	cases := []struct {
		name      string
		input     string
		contains  []string
		forbidden []string
		plain     []string
	}{
		{
			name: "simple table with inline markdown",
			input: strings.Join([]string{
				"| Name | Result | Link |",
				"| --- | :---: | ---: |",
				"| **build** | `ok|pass` | [docs](https://example.com/a?x=1&y=2) |",
				"| raw | <b>x</b> | [bad](javascript:alert(1)) |",
			}, "\n"),
			contains: []string{
				`<table><tr><th>Name</th><th>Result</th><th>Link</th></tr>`,
				`<td><strong>build</strong></td>`,
				`<td><code>ok|pass</code></td>`,
				`<a href="https://example.com/a?x=1&amp;y=2">docs</a> (https://example.com/a?x=1&amp;y=2)`,
				`<td>raw</td>`,
				`&lt;b&gt;x&lt;/b&gt;`,
				`bad (javascript:alert(1))`,
				`</table>`,
			},
			forbidden: []string{`<pre><code>| Name`, `<b>x</b>`, `href="javascript:`},
			plain:     []string{"Name\tResult\tLink", "build\tok|pass\tdocs (https://example.com/a?x=1&y=2)", "bad (javascript:alert(1))"},
		},
		{
			name: "escaped pipes ragged rows and images",
			input: strings.Join([]string{
				"a \\| b | img | extra",
				"- | - | -",
				"`x|y` | ![plot](https://example.com/plot.png) | one | two",
				"missing | only",
			}, "\n"),
			contains: []string{
				`a | b`,
				`x|y`,
				`Image: <a href="https://example.com/plot.png">plot</a> (https://example.com/plot.png)`,
				`one | two`,
				`missing`,
			},
			forbidden: []string{`<img`},
			plain:     []string{"a | b", "x|y", "Image: plot (https://example.com/plot.png)", "one | two", "missing"},
		},
		{
			name: "malformed delimiter stays paragraph",
			input: strings.Join([]string{
				"| a | b |",
				"| --- | nope |",
				"| c | d |",
			}, "\n"),
			contains:  []string{`| a | b |<br>| --- | nope |<br>| c | d |`},
			forbidden: []string{`<pre><code>| a`, `<table`},
			plain:     []string{"| a | b |", "| --- | nope |", "| c | d |"},
		},
	}

	for _, tc := range cases {
		for _, kind := range []TeamsRenderKind{TeamsRenderAssistant, TeamsRenderProgress} {
			t.Run(string(kind)+"/"+tc.name, func(t *testing.T) {
				got := RenderTeamsHTML(TeamsRenderInput{Kind: kind, Text: tc.input})
				assertTeamsRenderedHTMLSafe(t, got)
				for _, want := range tc.contains {
					if !strings.Contains(got, want) {
						t.Fatalf("rendered HTML missing %q in:\n%s", want, got)
					}
				}
				for _, forbidden := range tc.forbidden {
					if strings.Contains(got, forbidden) {
						t.Fatalf("rendered HTML contains forbidden %q in:\n%s", forbidden, got)
					}
				}
				plain := PlainTextFromTeamsHTML(got)
				for _, want := range tc.plain {
					if !strings.Contains(plain, want) {
						t.Fatalf("plain text missing %q in:\n%s", want, plain)
					}
				}
			})
		}
	}
}

func TestPlanTeamsHTMLChunksCodexMarkdownStress(t *testing.T) {
	text := strings.Join([]string{
		"## Long result",
		"",
		"Intro with **bold** and [safe](https://example.com/path?x=1&y=2).",
		"",
		"```txt",
		strings.Repeat("<raw>& ", 1200),
		"```",
		"",
		strings.Repeat("- item with `inline <code>` and ~~old~~\n", 400),
	}, "\n")
	chunks := PlanTeamsHTMLChunks(TeamsRenderInput{
		Surface: TeamsRenderSurfaceOutbox,
		Kind:    TeamsRenderAssistant,
		Text:    text,
	}, TeamsRenderOptions{
		TargetLimitBytes: 2048,
		HardLimitBytes:   2300,
	})
	if len(chunks) < 2 {
		t.Fatalf("expected markdown stress input to split, got %d chunk", len(chunks))
	}
	var plain strings.Builder
	for i, chunk := range chunks {
		if chunk.PartIndex != i+1 || chunk.PartCount != len(chunks) {
			t.Fatalf("chunk %d metadata = %d/%d, want %d/%d", i, chunk.PartIndex, chunk.PartCount, i+1, len(chunks))
		}
		if chunk.ByteLength != len(chunk.HTML) || chunk.ByteLength > 2300 {
			t.Fatalf("chunk %d bytes = %d len=%d", i, chunk.ByteLength, len(chunk.HTML))
		}
		assertTeamsRenderedHTMLSafe(t, chunk.HTML)
		plain.WriteString(PlainTextFromTeamsHTML(chunk.HTML))
		plain.WriteByte('\n')
	}
	joined := plain.String()
	for _, want := range []string{"Long result", "Intro with bold", "<raw>&", "inline <code>", "old"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("chunked plain text missing %q in:\n%s", want, joined)
		}
	}
}

func TestPlanTeamsHTMLChunksCodexMarkdownTableStress(t *testing.T) {
	var rows []string
	rows = append(rows, "| Case | Value | Notes |")
	rows = append(rows, "| --- | --- | --- |")
	for i := 0; i < 80; i++ {
		rows = append(rows, "row "+strconvItoa(i)+" | `a|b` <x> | [safe](https://example.com/"+strconvItoa(i)+") [bad](javascript:alert(1))")
	}
	chunks := PlanTeamsHTMLChunks(TeamsRenderInput{
		Kind: TeamsRenderAssistant,
		Text: strings.Join(rows, "\n"),
	}, TeamsRenderOptions{
		TargetLimitBytes: 1400,
		HardLimitBytes:   1800,
	})
	if len(chunks) < 2 {
		t.Fatalf("expected table stress input to split, got %d chunk", len(chunks))
	}
	var plain strings.Builder
	for i, chunk := range chunks {
		if chunk.PartIndex != i+1 || chunk.PartCount != len(chunks) {
			t.Fatalf("chunk %d metadata = %d/%d, want %d/%d", i, chunk.PartIndex, chunk.PartCount, i+1, len(chunks))
		}
		if chunk.ByteLength != len(chunk.HTML) || chunk.ByteLength > 1800 {
			t.Fatalf("chunk %d bytes = %d len=%d", i, chunk.ByteLength, len(chunk.HTML))
		}
		assertTeamsRenderedHTMLSafe(t, chunk.HTML)
		if strings.Contains(chunk.HTML, `href="javascript:`) {
			t.Fatalf("chunk %d contains unsafe table/link HTML:\n%s", i, chunk.HTML)
		}
		if !strings.Contains(chunk.HTML, `<table>`) || !strings.Contains(chunk.HTML, `<tr><th>Case</th><th>Value</th><th>Notes</th></tr>`) {
			t.Fatalf("chunk %d did not preserve native table structure with repeated header:\n%s", i, chunk.HTML)
		}
		plain.WriteString(PlainTextFromTeamsHTML(chunk.HTML))
		plain.WriteByte('\n')
	}
	joined := plain.String()
	for _, want := range []string{"Case", "row 0", "row 79", "a|b", "<x>", "safe (https://example.com/79)", "bad (javascript:alert(1))"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("chunked table plain text missing %q in:\n%s", want, joined)
		}
	}
}

func TestRenderFinalOutboxCodexMarkdownStress(t *testing.T) {
	outbox := store.OutboxMessage{
		Kind:      "final",
		Body:      "Done with **bold**.\n\n```json\n{\"ok\":\"<yes>\"}\n```",
		PartIndex: 1,
		PartCount: 1,
	}
	got := renderOutboxHTML(outbox)
	assertTeamsRenderedHTMLSafe(t, got)
	for _, want := range []string{
		`<strong>🤖 ✅ Codex answer:</strong><br>Done with <strong>bold</strong>.`,
		`<pre><code>{&#34;ok&#34;:&#34;&lt;yes&gt;&#34;}</code></pre>`,
		`<p><strong>🔧 Helper:</strong> ✅ Codex finished responding.</p>`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("final outbox render missing %q in:\n%s", want, got)
		}
	}
	plain := PlainTextFromTeamsHTML(got)
	if !strings.Contains(plain, "Done with bold.") || !strings.Contains(plain, `{"ok":"<yes>"}`) || !strings.Contains(plain, "Codex finished responding.") {
		t.Fatalf("final outbox plain text is not readable:\n%s", plain)
	}
}

func TestBridgeOutboxMarkdownStressActualGraphPayload(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	stateStore := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, stateStore, &recordingExecutor{})
	ctx := context.Background()
	if _, _, err := stateStore.CreateSession(ctx, store.SessionContext{
		ID:          "s-format",
		Status:      store.SessionStatusActive,
		TeamsChatID: "chat-1",
	}); err != nil {
		t.Fatalf("CreateSession error: %v", err)
	}
	stress := strings.Join([]string{
		"## Stress payload",
		"",
		"Prose with **bold**, `inline <code>`, [safe](https://example.com/a?x=1&y=2), [local](./x.go:9), [unsafe](javascript:alert(1)).",
		"",
		"```txt",
		strings.Repeat("<script>alert('x')</script> & **not bold**\n", 1900),
		"```",
		"",
		strings.Repeat("- child with <&> and ~~strike~~\n    - nested\n", 180),
	}, "\n")
	if err := bridge.queueAndSendOutboxChunks(ctx, "s-format", "turn-final", "chat-1", "final", stress); err != nil {
		t.Fatalf("queue final stress: %v", err)
	}
	if err := bridge.queueAndSendOutboxChunks(ctx, "s-format", "turn-progress", "chat-1", "codex-progress-001", stress); err != nil {
		t.Fatalf("queue progress stress: %v", err)
	}
	if len(*sent) < 4 {
		t.Fatalf("expected multipart Graph payloads, sent %d", len(*sent))
	}
	footerCount := 0
	for i, msg := range *sent {
		if len(msg.Content) > safeTeamsHTMLContentBytes {
			t.Fatalf("sent Graph payload %d rendered to %d bytes", i, len(msg.Content))
		}
		assertTeamsRenderedHTMLSafe(t, msg.Content)
		plain := PlainTextFromTeamsHTML(msg.Content)
		if !strings.Contains(plain, "Stress payload") && !strings.Contains(plain, "<script>alert('x')</script>") && !strings.Contains(plain, "child with <&>") {
			t.Fatalf("sent Graph payload %d lost readable stress content:\n%s", i, plain)
		}
		if strings.Contains(msg.Content, "Codex finished responding") {
			footerCount++
		}
	}
	if footerCount != 1 {
		t.Fatalf("final multipart completion footer count = %d, want 1", footerCount)
	}
	if !strings.Contains((*sent)[len(*sent)-1].Content, "🤖 ⏳ Codex status") {
		t.Fatalf("last payload should be progress chunk after final chunks, got:\n%s", (*sent)[len(*sent)-1].Content)
	}
}

func TestRenderTeamsHTMLUserMessagesUseSafeMarkdown(t *testing.T) {
	got := RenderTeamsHTML(TeamsRenderInput{
		Kind: TeamsRenderUser,
		Text: strings.Join([]string{
			"Use this:",
			"",
			"```text",
			"new fix windows installer",
			"list",
			"status",
			"resume 3",
			"archive 3",
			"```",
			"",
			"19. checks:",
			"   - topic 清洗",
			"   - registry 映射",
			"   - message 去重",
			"",
			`**bold** <b>not html</b> [docs](https://example.com) [bad](javascript:alert(1))`,
		}, "\n"),
	})
	assertTeamsRenderedHTMLSafe(t, got)
	for _, want := range []string{
		`<strong>🧑‍💻 User:</strong><br>Use this:`,
		`<pre><code>new fix windows installer`,
		`resume 3`,
		`<ol><li>checks:<br><ul><li>topic 清洗</li><li>registry 映射</li><li>message 去重</li></ul></li></ol>`,
		`<strong>bold</strong>`,
		`&lt;b&gt;not html&lt;/b&gt;`,
		`<a href="https://example.com">docs</a> (https://example.com)`,
		`bad (javascript:alert(1))`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("user render missing %q in:\n%s", want, got)
		}
	}
	if strings.Contains(got, `<b>not html</b>`) || strings.Contains(got, `href="javascript:`) {
		t.Fatalf("user render escaped unsafe HTML/link incorrectly:\n%s", got)
	}
}

func assertTeamsRenderedHTMLSafe(t *testing.T, got string) {
	t.Helper()
	for _, forbidden := range []string{
		"<script",
		"</script",
		`href="file:`,
		`href="C:`,
		`href="javascript:`,
		`onerror=`,
		`onclick=`,
	} {
		if strings.Contains(strings.ToLower(got), strings.ToLower(forbidden)) {
			t.Fatalf("rendered HTML contains unsafe fragment %q in:\n%s", forbidden, got)
		}
	}
	if strings.Contains(got, "\r") {
		t.Fatalf("rendered HTML kept carriage return: %q", got)
	}
	for _, allowed := range []string{"p", "br", "strong", "em", "s", "code", "pre", "a", "ul", "ol", "li", "table", "tr", "th", "td"} {
		got = strings.ReplaceAll(got, "<"+allowed+">", "")
		got = strings.ReplaceAll(got, "</"+allowed+">", "")
	}
	got = strings.ReplaceAll(got, "<br>", "")
	got = stripAllowedAnchorOpenTags(got)
	if strings.Contains(got, "<") || strings.Contains(got, ">") {
		unescaped := html.UnescapeString(got)
		if strings.Contains(unescaped, "<") || strings.Contains(unescaped, ">") {
			t.Fatalf("rendered HTML has unexpected raw angle bracket markup:\n%s", got)
		}
	}
}

func stripAllowedAnchorOpenTags(text string) string {
	for {
		start := strings.Index(text, `<a href="`)
		if start < 0 {
			return text
		}
		endRel := strings.IndexByte(text[start:], '>')
		if endRel < 0 {
			return text
		}
		text = text[:start] + text[start+endRel+1:]
	}
}
