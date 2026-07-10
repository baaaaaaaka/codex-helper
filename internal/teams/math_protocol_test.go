package teams

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"strings"
	"testing"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type fakeTeamsMathRenderer struct {
	assets []teamsMathAsset
}

type countingTeamsMathRenderer struct {
	calls int
	spans []teamsMathSpan
}

func freezeTestOutboxMath(outbox teamstore.OutboxMessage) teamstore.OutboxMessage {
	outbox.TrustedMath = true
	outbox.MathPlanVersion = teamsMathPlanVersion
	outbox.MathSpans = storeTeamsMathPlan(parseTrustedTeamsMath(outbox.Body))
	return outbox
}

func (r fakeTeamsMathRenderer) Render(_ context.Context, spans []teamsMathSpan) []teamsMathAsset {
	if r.assets != nil {
		return append([]teamsMathAsset(nil), r.assets...)
	}
	out := make([]teamsMathAsset, 0, len(spans))
	for _, span := range spans {
		out = append(out, teamsMathAsset{Index: span.Index, PNG: testMathPNG()})
	}
	return out
}

func (r *countingTeamsMathRenderer) Render(_ context.Context, spans []teamsMathSpan) []teamsMathAsset {
	r.calls++
	r.spans = append([]teamsMathSpan(nil), spans...)
	out := make([]teamsMathAsset, 0, len(spans))
	for _, span := range spans {
		out = append(out, teamsMathAsset{Index: span.Index, PNG: testMathPNG()})
	}
	return out
}

func TestParseTrustedTeamsMathCorpus(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		text string
		want []string
	}{
		{"inline", `结果是 <m>x_i+y_i</m>。`, []string{"x_i+y_i"}},
		{"multiple", `<m>x</m> plain <m>y</m>`, []string{"x", "y"}},
		{"multiline", "before <m>\\begin{aligned}\nx&=1\\\\y&=2\n\\end{aligned}</m> after", []string{"\\begin{aligned}\nx&=1\\\\y&=2\n\\end{aligned}"}},
		{"inline code", "`<m>x</m>` and <m>y</m>", []string{"y"}},
		{"indented code", "    <m>x</m>\n<m>y</m>", []string{"y"}},
		{"escaped marker", `\<m>x</m> <m>y</m>`, []string{"y"}},
		{"fenced code", "```tex\n<m>x</m>\n```\n<m>y</m>", []string{"y"}},
		{"tilde fence", "~~~xml\n<m>x</m>\n~~~\n<m>y</m>", []string{"y"}},
		{"link destination", `[x](https://e.test/<m>x</m>) <m>y</m>`, []string{"y"}},
		{"link label", `[<m>x</m>](https://e.test) <m>y</m>`, []string{"y"}},
		{"image label", `![<m>x</m>](https://e.test/x.png) <m>y</m>`, []string{"y"}},
		{"html attribute", `<span data-x="<m>x</m>">text</span> <m>y</m>`, []string{"y"}},
		{"html content", `<span><m>x</m></span> <m>y</m>`, []string{"y"}},
		{"script content", `<script><m>x</m></script> <m>y</m>`, []string{"y"}},
		{"custom html content", `<formula-note><m>x</m></formula-note> <m>y</m>`, []string{"y"}},
		{"unterminated html attribute", `<span data-x="<m>x</m>"`, nil},
		{"multiline raw html", "<section>\n<m>x</m>\n</section>\n<m>y</m>", []string{"y"}},
		{"html comment", `<!-- <m>x</m> --> <m>y</m>`, []string{"y"}},
		{"multiline html comment", "<!--\n<m>x</m>\n--> <m>y</m>", []string{"y"}},
		{"bare URL", `https://e.test/<m>x</m> <m>y</m>`, []string{"y"}},
		{"literal regex and path", `\[A-Z\] \(foo\) C:\tmp\[draft\]\x <m>A-Z</m>`, []string{"A-Z"}},
		{"empty", `<m></m><m> </m>`, nil},
		{"unclosed", `<m>x`, nil},
		{"closing only", `x</m>`, nil},
		{"nested", `<m>outer <m>x</m> tail</m>`, nil},
		{"shell", `<m>${HOME}</m> <m>$(date)</m> <m>$20</m>`, nil},
		{"paths", `<m>C:\Users\x</m> <m>/home/u/x</m>`, nil},
		{"url", `<m>https://example.com/x</m>`, nil},
		{"email", `<m>a@example.com</m>`, nil},
		{"other delimiters", `<m>\(x\)</m><m>\[y\]</m>`, nil},
		{"unsafe commands", `<m>\includegraphics{x}</m><m>\require{html}</m><m>\def\x{x}\x</m><m>\input{file}</m>`, nil},
		{"unbalanced", `<m>{x</m><m>x}</m>`, nil},
		{"escaped currency", `<m>p=\$5</m>`, []string{`p=\$5`}},
		{"less than", `<m>x<y</m>`, []string{"x<y"}},
		{"comparison tag shape", `<m>x<y>0</m>`, []string{"x<y>0"}},
		{"unicode", `<m>α+β=γ</m>`, []string{"α+β=γ"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := parseTrustedTeamsMath(tc.text)
			got := make([]string, 0, len(plan.Spans))
			for _, span := range plan.Spans {
				got = append(got, span.Source)
				if tc.text[span.Start:span.End] != teamsMathOpenTag+span.Source+teamsMathCloseTag {
					t.Fatalf("invalid span %#v", span)
				}
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("sources = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestTrustedTeamsMathClassifiesLayoutByContextAndStructure(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		text string
		want []teamsMathLayout
	}{
		{name: "inline sentence", text: `结果是 <m>x_i</m>。`, want: []teamsMathLayout{teamsMathLayoutInlineCode}},
		{name: "own line", text: "before\n\n<m>x_i</m>\n\nafter", want: []teamsMathLayout{teamsMathLayoutDisplayOwnLine}},
		{name: "whitespace line", text: "  <m>x_i</m> \r\n", want: []teamsMathLayout{teamsMathLayoutDisplayOwnLine}},
		{name: "multiple inline", text: `<m>x</m> and <m>y</m>`, want: []teamsMathLayout{teamsMathLayoutInlineCode, teamsMathLayoutInlineCode}},
		{name: "inline fraction promoted", text: `结果是 <m>\frac{a}{b}</m>。`, want: []teamsMathLayout{teamsMathLayoutDisplayPromoted}},
		{name: "inline sum promoted", text: `结果是 <m>\sum_i x_i</m>。`, want: []teamsMathLayout{teamsMathLayoutDisplayPromoted}},
		{name: "inline integral promoted", text: `结果是 <m>\int_0^1 x\,dx</m>。`, want: []teamsMathLayout{teamsMathLayoutDisplayPromoted}},
		{name: "inline root stays simple", text: `结果是 <m>\sqrt{x}</m>。`, want: []teamsMathLayout{teamsMathLayoutInlineCode}},
		{name: "table complex stays code", text: "| Formula | Note |\n|---|---|\n| <m>\frac{a}{b}</m> | ok |", want: []teamsMathLayout{teamsMathLayoutContainerCode}},
		{name: "unordered list complex stays code", text: `- <m>\sum_i x_i</m>`, want: []teamsMathLayout{teamsMathLayoutContainerCode}},
		{name: "ordered list complex stays code", text: `12. <m>\int x\,dx</m>`, want: []teamsMathLayout{teamsMathLayoutContainerCode}},
		{name: "blockquote complex stays code", text: `> <m>\frac{a}{b}</m>`, want: []teamsMathLayout{teamsMathLayoutContainerCode}},
		{name: "multiline display", text: "<m>\\begin{aligned}\nx&=1\\\\y&=2\n\\end{aligned}</m>", want: []teamsMathLayout{teamsMathLayoutDisplayOwnLine}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := parseTrustedTeamsMath(tc.text)
			got := make([]teamsMathLayout, 0, len(plan.Spans))
			for _, span := range plan.Spans {
				got = append(got, span.Layout)
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("display modes=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestTeamsMathStructuralComplexityScanner(t *testing.T) {
	t.Parallel()
	complex := []string{
		`\frac{a}{b}`, `\dfrac{a}{b}`, `\tfrac{a}{b}`, `\binom{n}{k}`,
		`\sum_i x_i`, `\prod_i x_i`, `\int_0^1 x\,dx`, `\iint_D f`,
		`\begin{pmatrix}a&b\\c&d\end{pmatrix}`, `\lim_{n\to\infty}a_n`, `\overset{!}{=}`,
		`A=\operatorname{softmax}(QK^T/\sqrt{d_k})VW_O`,
		strings.Repeat("x", 73), "x=1\\\\y=2", "x=1\ny=2",
	}
	for _, source := range complex {
		if !teamsMathSourceIsStructurallyComplex(source) {
			t.Errorf("complex source was classified simple: %q", source)
		}
	}
	simple := []string{`x_i`, `W_{Q,h}`, `a+b`, `O(n\log n)`, `\sqrt{x}`, `x^2`, `\operatorname{softmax}(x)`}
	for _, source := range simple {
		if teamsMathSourceIsStructurallyComplex(source) {
			t.Errorf("simple source was classified complex: %q", source)
		}
	}
}

func TestTeamsMathLayoutDecisionMatrixStress(t *testing.T) {
	t.Parallel()
	formulas := []struct {
		name    string
		source  string
		complex bool
	}{
		{name: "identifier", source: `x_i`},
		{name: "root", source: `\sqrt{x}`},
		{name: "fraction", source: `\frac{a}{b}`, complex: true},
		{name: "sum", source: `\sum_{i=1}^n x_i`, complex: true},
		{name: "integral", source: `\int_0^1 x\,dx`, complex: true},
		{name: "matrix", source: `\begin{pmatrix}a&b\\c&d\end{pmatrix}`, complex: true},
		{name: "line break", source: `x=1\\y=2`, complex: true},
		{name: "multiline", source: "x=1\ny=2", complex: true},
		{name: "long assignment", source: `A=\operatorname{softmax}(QK^T/\sqrt{d_k})VW_O`, complex: true},
	}
	contexts := []struct {
		name string
		text func(string) string
		want func(bool) teamsMathLayout
	}{
		{name: "own line", text: func(source string) string { return "before\n<m>" + source + "</m>\nafter" }, want: func(bool) teamsMathLayout { return teamsMathLayoutDisplayOwnLine }},
		{name: "indented own line", text: func(source string) string { return "  <m>" + source + "</m>  \n" }, want: func(bool) teamsMathLayout { return teamsMathLayoutDisplayOwnLine }},
		{name: "prose", text: func(source string) string { return "before <m>" + source + "</m> after" }, want: func(complex bool) teamsMathLayout {
			if complex {
				return teamsMathLayoutDisplayPromoted
			}
			return teamsMathLayoutInlineCode
		}},
		{name: "table", text: func(source string) string { return "| Formula | Note |\n|---|---|\n| <m>" + source + "</m> | ok |" }, want: func(bool) teamsMathLayout { return teamsMathLayoutContainerCode }},
		{name: "unordered list", text: func(source string) string { return "- <m>" + source + "</m>" }, want: func(bool) teamsMathLayout { return teamsMathLayoutContainerCode }},
		{name: "ordered list", text: func(source string) string { return "42) <m>" + source + "</m>" }, want: func(bool) teamsMathLayout { return teamsMathLayoutContainerCode }},
		{name: "blockquote", text: func(source string) string { return "> <m>" + source + "</m>" }, want: func(bool) teamsMathLayout { return teamsMathLayoutContainerCode }},
	}
	for _, formula := range formulas {
		for _, context := range contexts {
			t.Run(formula.name+"/"+context.name, func(t *testing.T) {
				plan := parseTrustedTeamsMath(context.text(formula.source))
				want := context.want(formula.complex)
				// A physical newline inside a table cell ends the Markdown table
				// before layout classification. Treat that malformed container as
				// promoted display math instead of pretending it is still inline.
				if context.name == "table" && strings.ContainsAny(formula.source, "\r\n") {
					want = teamsMathLayoutDisplayPromoted
				}
				if len(plan.Spans) != 1 || plan.Spans[0].Layout != want {
					t.Fatalf("plan = %#v, want layout %d", plan.Spans, want)
				}
			})
		}
	}
}

func TestTeamsMathCapturedCodexOutputStressFixtures(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		text string
		want []teamsMathLayout
	}{
		{
			name: "one-line request answered with markdown hard break",
			text: "定积分表示有向面积。  \n<m>\\int_a^b f(x)\\,dx</m>",
			want: []teamsMathLayout{teamsMathLayoutDisplayOwnLine},
		},
		{
			name: "low model put fraction between html breaks in table",
			text: "| 变量 | 公式 |\n|---|---|\n| `x_i` | <br><m>P(\\theta\\mid x_i)=\\frac{P(x_i\\mid\\theta)P(\\theta)}{P(x_i)}</m><br> |",
			want: []teamsMathLayout{teamsMathLayoutContainerCode},
		},
		{
			name: "matrix moved below list",
			text: "- 2×2 矩阵：\n\n<m>\\begin{pmatrix}a&b\\\\c&d\\end{pmatrix}</m>",
			want: []teamsMathLayout{teamsMathLayoutDisplayOwnLine},
		},
		{
			name: "simple variables plus multiline attention",
			text: "变量 <m>x_i</m> 与 <m>W_{Q,h}</m>。\n\n<m>\n\\alpha_i=\\frac{\\exp(s_i)}{\\sum_j\\exp(s_j)}\n</m>",
			want: []teamsMathLayout{teamsMathLayoutInlineCode, teamsMathLayoutInlineCode, teamsMathLayoutDisplayOwnLine},
		},
		{
			name: "literal tex code remains untouched",
			text: "请复制 `\\frac{a}{b}`，不要渲染。",
			want: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan := parseTrustedTeamsMath(tc.text)
			got := make([]teamsMathLayout, 0, len(plan.Spans))
			for _, span := range plan.Spans {
				got = append(got, span.Layout)
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("layouts = %v, want %v; spans=%#v", got, tc.want, plan.Spans)
			}
		})
	}
}

func TestRenderTrustedTeamsMathShowsExactCodeBeforeImage(t *testing.T) {
	t.Parallel()
	source := `A=\operatorname{softmax}(QK^T/\sqrt{d})V`
	got := renderTeamsHTMLCodexMarkdownWithMathAfterLabelBreak("Codex", "before\n\n<m>"+source+"</m>\n\nafter", []teamsMathAsset{{
		Index:       1,
		TemporaryID: "math-001",
		PNG:         []byte("png"),
	}})
	code := "<pre><code>" + source + "</code></pre>"
	image := `<p><img src="../hostedContents/math-001/$value" alt="Rendered TeX formula"></p>`
	if !strings.Contains(got, code) || !strings.Contains(got, image) {
		t.Fatalf("rendered math missing source/image:\n%s", got)
	}
	if strings.Index(got, code) > strings.Index(got, image) {
		t.Fatalf("source code must be before image:\n%s", got)
	}
	if strings.Contains(got, teamsMathOpenTag) || strings.Contains(got, teamsMathCloseTag) {
		t.Fatalf("protocol markers leaked:\n%s", got)
	}
}

func TestRenderTrustedTeamsInlineMathStaysInlineAndIgnoresImage(t *testing.T) {
	t.Parallel()
	got := renderTeamsHTMLCodexMarkdownWithMathAfterLabelBreak("Codex", `令 <m>x_i</m> 和 <m>W_{Q,h}</m> 表示变量。`, []teamsMathAsset{
		{Index: 1, TemporaryID: "math-001", PNG: testMathPNG()},
		{Index: 2, TemporaryID: "math-002", PNG: testMathPNG()},
	})
	if !strings.Contains(got, `令 <code>x_i</code> 和 <code>W_{Q,h}</code> 表示变量。`) {
		t.Fatalf("inline math did not remain in its paragraph:\n%s", got)
	}
	if strings.Contains(got, `<pre>`) || strings.Contains(got, `<img`) || strings.Contains(got, teamsMathOpenTag) {
		t.Fatalf("inline math leaked block markup or protocol markers:\n%s", got)
	}
}

func TestRenderTrustedTeamsMathEscapesSourceWithoutChangingIt(t *testing.T) {
	t.Parallel()
	source := `x<y \& y>0`
	got := renderTeamsHTMLCodexMarkdownWithMathAfterLabelBreak("Codex", "<m>"+source+"</m>", nil)
	if !strings.Contains(got, `<pre><code>x&lt;y \&amp; y&gt;0</code></pre>`) {
		t.Fatalf("escaped source = %q", got)
	}
}

func TestRenderTrustedTeamsMathDoesNotReplaceLiteralPlaceholderLikeText(t *testing.T) {
	t.Parallel()
	literal := "\ue000cxp-math-000001\ue001"
	got := renderTeamsHTMLCodexMarkdownWithMathAfterLabelBreak("Codex", literal+" <m>x_i</m>", nil)
	if !strings.Contains(got, literal) || !strings.Contains(got, `<code>x_i</code>`) {
		t.Fatalf("placeholder-like text was lost or replaced: %q", got)
	}
}

func TestRenderTrustedTeamsInlineMathPreservesMarkdownContainers(t *testing.T) {
	t.Parallel()
	asset := []teamsMathAsset{{Index: 1, TemporaryID: "math-001", PNG: testMathPNG()}}
	cases := []struct {
		name string
		text string
		tag  string
	}{
		{name: "table", text: "| Formula | Note |\n|---|---|\n| <m>x_i</m> | ok |", tag: "<table>"},
		{name: "list", text: "- <m>x_i</m>", tag: "<ul>"},
		{name: "blockquote", text: "> <m>x_i</m>", tag: "<blockquote>"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := renderTeamsHTMLCodexMarkdownWithMathAfterLabelBreak("Codex", tc.text, asset)
			if !strings.Contains(got, tc.tag) || !strings.Contains(got, `<code>x_i</code>`) || strings.Contains(got, `<img`) || strings.Contains(got, `<pre>`) {
				t.Fatalf("container was not preserved:\n%s", got)
			}
		})
	}
}

func TestTrustedMathInsideRawHTMLNeverCreatesHostedMath(t *testing.T) {
	t.Parallel()
	text := `<table><tr><td><m>\frac{a}{b}</m></td></tr></table>`
	plan := parseTrustedTeamsMath(text)
	if len(plan.Spans) != 0 {
		t.Fatalf("raw HTML math was parsed: %#v", plan.Spans)
	}
	got := RenderTeamsHTML(TeamsRenderInput{Kind: TeamsRenderAssistant, Text: text, TrustedMath: true})
	if strings.Contains(got, `<pre>`) || strings.Contains(got, `<img`) {
		t.Fatalf("raw HTML math created hosted content: %s", got)
	}
}

func TestTrustedOutboxMathPlanIsFrozenAndFailClosed(t *testing.T) {
	t.Parallel()
	outbox := teamstore.OutboxMessage{
		Kind: "final", Body: `<m>x_i</m>`, TrustedMath: true, MathPlanVersion: teamsMathPlanVersion,
	}
	if got := renderOutboxHTML(outbox); strings.Contains(got, `<pre><code>`) || strings.Contains(got, `<img`) {
		t.Fatalf("empty frozen plan was rescanned: %s", got)
	}
	outbox.MathSpans = []teamstore.OutboxMathSpan{{Start: 0, End: len(outbox.Body), Index: 1, Source: "different"}}
	if got := renderOutboxHTML(outbox); strings.Contains(got, `<pre><code>`) || strings.Contains(got, `<img`) {
		t.Fatalf("corrupt frozen plan was rendered: %s", got)
	}
	outbox = freezeTestOutboxMath(teamstore.OutboxMessage{Kind: "final", Body: strings.Repeat(`<m>x</m>`, maxTeamsMathMarkersPerMessage+1)})
	if got := renderOutboxHTML(outbox); strings.Contains(got, `<pre><code>`) || strings.Contains(got, `<img`) {
		t.Fatalf("oversized frozen plan was rendered: %s", got)
	}
}

func TestRenderTrustedTeamsMathFailureKeepsCodeOnly(t *testing.T) {
	t.Parallel()
	got := RenderTeamsHTML(TeamsRenderInput{
		Kind:        TeamsRenderAssistant,
		Text:        "result\n\n<m>x_i^2</m>",
		TrustedMath: true,
	})
	if !strings.Contains(got, `<pre><code>x_i^2</code></pre>`) || strings.Contains(got, `<img`) {
		t.Fatalf("code-only fallback = %q", got)
	}
}

func TestUntrustedTeamsSurfacesNeverInterpretMathMarkers(t *testing.T) {
	t.Parallel()
	for _, kind := range []TeamsRenderKind{TeamsRenderUser, TeamsRenderHelper, TeamsRenderStatus, TeamsRenderAssistant} {
		got := RenderTeamsHTML(TeamsRenderInput{Kind: kind, Text: `<m>x_i</m>`})
		if strings.Contains(got, `<pre><code>x_i</code></pre>`) || strings.Contains(got, `<img`) {
			t.Fatalf("kind %s interpreted untrusted marker: %s", kind, got)
		}
		if !strings.Contains(got, `&lt;m&gt;x_i&lt;/m&gt;`) {
			t.Fatalf("kind %s did not preserve escaped marker: %s", kind, got)
		}
	}
}

func TestOutboxMathTrustIsRestrictedToCodexOutputKinds(t *testing.T) {
	t.Parallel()
	for _, kind := range []string{"final", "final-002", "codex-progress", "codex-progress-003", "import-assistant-abc", "import-bg-assistant-abc", "sync-assistant-abc", "publish-full-assistant-abc", "codex-assistant-abc"} {
		if !outboxKindTrustsMath(kind) {
			t.Fatalf("kind %q should trust math", kind)
		}
	}
	for _, kind := range []string{"user", "helper", "status", "progress", "progress-003", "artifact", "queued-status", "import-user-abc", "user-assistant-note"} {
		if outboxKindTrustsMath(kind) {
			t.Fatalf("kind %q unexpectedly trusts math", kind)
		}
	}
}

func TestQueueOutboxFreezesTrustedMathPlan(t *testing.T) {
	t.Parallel()
	statePath := filepath.Join(t.TempDir(), "state.json")
	store, err := teamstore.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	bridge := &Bridge{store: store}
	queued, err := bridge.queueOutboxChunksWithOptions(context.Background(), "", "turn-math-plan", "chat-1", "final", `result <m>x_i</m>`, outboxQueueOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(queued) != 1 || !queued[0].TrustedMath || queued[0].MathPlanVersion != teamsMathPlanVersion || len(queued[0].MathSpans) != 1 {
		t.Fatalf("queued math plan = %#v", queued)
	}
	span := queued[0].MathSpans[0]
	if span.Source != "x_i" || queued[0].Body[span.Start:span.End] != `<m>x_i</m>` {
		t.Fatalf("frozen span = %#v body=%q", span, queued[0].Body)
	}
	if trusted := trustedTeamsMathPlanForOutbox(queued[0]); len(trusted.Spans) != 1 || trusted.Spans[0].Layout != teamsMathLayoutInlineCode {
		t.Fatalf("rehydrated inline span = %#v", trusted.Spans)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := teamstore.Open(statePath)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	state, err := reopened.OutboxStateSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	persisted := state.OutboxMessages[queued[0].ID]
	if persisted.MathPlanVersion != teamsMathPlanVersion || !equalOutboxMathSpans(persisted.MathSpans, queued[0].MathSpans) {
		t.Fatalf("persisted math plan = %#v", persisted)
	}
}

func TestTrustedOutboxMathAcceptsLegacyV1AndRejectsUnknownVersion(t *testing.T) {
	t.Parallel()
	outbox := freezeTestOutboxMath(teamstore.OutboxMessage{Kind: "final", Body: `result <m>x_i</m>`})
	outbox.MathPlanVersion = teamsMathLegacyPlanVersion
	legacy := trustedTeamsMathPlanForOutbox(outbox)
	if len(legacy.Spans) != 1 || legacy.Spans[0].Layout != teamsMathLayoutInlineCode {
		t.Fatalf("legacy v1 plan = %#v", legacy)
	}
	outbox.MathPlanVersion = teamsMathPlanVersion + 1
	if unknown := trustedTeamsMathPlanForOutbox(outbox); len(unknown.Spans) != 0 {
		t.Fatalf("unknown plan version was trusted: %#v", unknown)
	}
}

func TestTrustedOutboxMathRejectsExcessiveCumulativeSource(t *testing.T) {
	t.Parallel()
	source := strings.Repeat("x+", 3000) + "x"
	body := strings.Repeat(teamsMathOpenTag+source+teamsMathCloseTag+"\n", 6)
	outbox := freezeTestOutboxMath(teamstore.OutboxMessage{Kind: "final", Body: body})
	if len(outbox.MathSpans) != 6 {
		t.Fatalf("fixture spans = %d, want 6", len(outbox.MathSpans))
	}
	if trusted := trustedTeamsMathPlanForOutbox(outbox); len(trusted.Spans) != 0 {
		t.Fatalf("oversized cumulative source was trusted: %d spans", len(trusted.Spans))
	}
}

func TestBridgeRendersTrustedOutboxAsCodeThenHostedImage(t *testing.T) {
	t.Parallel()
	bridge := &Bridge{mathRenderer: fakeTeamsMathRenderer{}}
	outbox := freezeTestOutboxMath(teamstore.OutboxMessage{
		ID:               "outbox:turn-1:final",
		Kind:             "final",
		Body:             "result\n\n<m>x_i^2</m>",
		NotificationKind: "turn_completed",
		PartIndex:        1,
		PartCount:        1,
	})
	assets, hosted := bridge.renderOutboxMathAssets(context.Background(), outbox)
	if len(assets) != 1 || len(hosted) != 1 || assets[0].TemporaryID == "" {
		t.Fatalf("assets=%#v hosted=%#v", assets, hosted)
	}
	rendered := renderOutboxHTMLWithMathAssets(outbox, assets)
	code := `<pre><code>x_i^2</code></pre>`
	image := `<img src="../hostedContents/` + assets[0].TemporaryID + `/$value" alt="Rendered TeX formula">`
	if !strings.Contains(rendered, code) || !strings.Contains(rendered, image) || strings.Index(rendered, code) > strings.Index(rendered, image) {
		t.Fatalf("rendered outbox = %q", rendered)
	}
}

func TestBridgeNeverRendersInlineMathAndRendersOnlyDisplayMathInMixedMessage(t *testing.T) {
	t.Parallel()
	renderer := &countingTeamsMathRenderer{}
	bridge := &Bridge{mathRenderer: renderer}
	inline := freezeTestOutboxMath(teamstore.OutboxMessage{
		ID: "outbox:inline-only", Kind: "final", Body: `令 <m>x_i</m> 表示第 i 个样本。`,
	})
	assets, hosted := bridge.renderOutboxMathAssets(context.Background(), inline)
	if renderer.calls != 0 || len(assets) != 0 || len(hosted) != 0 {
		t.Fatalf("inline math invoked renderer: calls=%d assets=%d hosted=%d", renderer.calls, len(assets), len(hosted))
	}

	mixed := freezeTestOutboxMath(teamstore.OutboxMessage{
		ID: "outbox:mixed-math", Kind: "final",
		Body: "令 <m>x_i</m> 表示样本。\n\n<m>L=\\frac{1}{N}\\sum_i x_i</m>",
	})
	assets, hosted = bridge.renderOutboxMathAssets(context.Background(), mixed)
	if renderer.calls != 1 || len(renderer.spans) != 1 || renderer.spans[0].Index != 2 || !renderer.spans[0].isDisplay() {
		t.Fatalf("mixed render spans=%#v calls=%d", renderer.spans, renderer.calls)
	}
	if len(assets) != 1 || len(hosted) != 1 {
		t.Fatalf("mixed assets=%d hosted=%d, want one display formula", len(assets), len(hosted))
	}
	rendered := renderOutboxHTMLWithMathAssets(mixed, assets)
	if !strings.Contains(rendered, `令 <code>x_i</code> 表示样本。`) || !strings.Contains(rendered, `<pre><code>L=\frac{1}{N}\sum_i x_i</code></pre>`) || strings.Count(rendered, `<img`) != 1 {
		t.Fatalf("mixed rendered HTML = %s", rendered)
	}
}

func TestBridgePromotesComplexProseMathButKeepsContainersAsCode(t *testing.T) {
	t.Parallel()
	renderer := &countingTeamsMathRenderer{}
	bridge := &Bridge{mathRenderer: renderer}
	promoted := freezeTestOutboxMath(teamstore.OutboxMessage{
		ID: "outbox:promoted-complex", Kind: "final", Body: `结果为 <m>\frac{a}{b}</m>。`,
	})
	plan := trustedTeamsMathPlanForOutbox(promoted)
	if len(plan.Spans) != 1 || plan.Spans[0].Layout != teamsMathLayoutDisplayPromoted {
		t.Fatalf("promoted plan = %#v", plan)
	}
	assets, hosted := bridge.renderOutboxMathAssets(context.Background(), promoted)
	if renderer.calls != 1 || len(assets) != 1 || len(hosted) != 1 {
		t.Fatalf("promoted render calls=%d assets=%d hosted=%d", renderer.calls, len(assets), len(hosted))
	}
	rendered := renderOutboxHTMLWithMathAssets(promoted, assets)
	if !strings.Contains(rendered, `<pre><code>\frac{a}{b}</code></pre>`) || strings.Count(rendered, `<img`) != 1 || strings.Contains(rendered, teamsMathOpenTag) {
		t.Fatalf("promoted HTML = %s", rendered)
	}

	for _, fixture := range []string{
		"| Formula | Note |\n|---|---|\n| <m>\\frac{a}{b}</m> | ok |",
		`- <m>\sum_i x_i</m>`,
		`> <m>\int_0^1 x\,dx</m>`,
	} {
		containerRenderer := &countingTeamsMathRenderer{}
		containerBridge := &Bridge{mathRenderer: containerRenderer}
		outbox := freezeTestOutboxMath(teamstore.OutboxMessage{ID: "outbox:container", Kind: "final", Body: fixture})
		assets, hosted := containerBridge.renderOutboxMathAssets(context.Background(), outbox)
		if containerRenderer.calls != 0 || len(assets) != 0 || len(hosted) != 0 {
			t.Fatalf("container invoked renderer for %q", fixture)
		}
		html := renderOutboxHTMLWithMathAssets(outbox, nil)
		if !strings.Contains(html, `<code>`) || strings.Contains(html, `<img`) || strings.Contains(html, teamsMathOpenTag) {
			t.Fatalf("container HTML for %q = %s", fixture, html)
		}
	}
}

func TestBridgeDeduplicatesIdenticalMathHostedContent(t *testing.T) {
	t.Parallel()
	bridge := &Bridge{mathRenderer: fakeTeamsMathRenderer{}}
	outbox := freezeTestOutboxMath(teamstore.OutboxMessage{
		ID: "outbox:duplicate-math", Kind: "final", Body: "<m>x_i</m>\n\nand\n\n<m>x_i</m>",
	})
	assets, hosted := bridge.renderOutboxMathAssets(context.Background(), outbox)
	if len(assets) != 2 || len(hosted) != 1 {
		t.Fatalf("assets=%d hosted=%d, want 2 references to 1 hosted item", len(assets), len(hosted))
	}
	if assets[0].TemporaryID == "" || assets[0].TemporaryID != assets[1].TemporaryID {
		t.Fatalf("duplicate sources did not share temporary id: %#v", assets)
	}
	rendered := renderOutboxHTMLWithMathAssets(outbox, assets)
	imageRef := `../hostedContents/` + assets[0].TemporaryID + `/$value`
	if strings.Count(rendered, imageRef) != 2 {
		t.Fatalf("rendered duplicate image references = %d, want 2: %s", strings.Count(rendered, imageRef), rendered)
	}
}

func TestBridgeMathMediaFallbackNeverInvokesRenderer(t *testing.T) {
	t.Parallel()
	bridge := &Bridge{mathRenderer: fakeTeamsMathRenderer{assets: []teamsMathAsset{{Index: 1, PNG: testMathPNG()}}}}
	outbox := freezeTestOutboxMath(teamstore.OutboxMessage{ID: "o1", Kind: "final", Body: `<m>x</m>`, MathMediaFallback: true})
	assets, hosted := bridge.renderOutboxMathAssets(context.Background(), outbox)
	if len(assets) != 0 || len(hosted) != 0 {
		t.Fatalf("fallback rendered media: assets=%#v hosted=%#v", assets, hosted)
	}
	if rendered := renderOutboxHTMLWithMathAssets(outbox, nil); !strings.Contains(rendered, `<pre><code>x</code></pre>`) || strings.Contains(rendered, `<img`) {
		t.Fatalf("fallback render = %q", rendered)
	}
}

func TestTeamsMathMediaFallbackOnlyForPayloadRejections(t *testing.T) {
	t.Parallel()
	for _, status := range []int{400, 413, 415, 422} {
		if !shouldFallbackTeamsMathMediaError(teamsMathHostedSendError{err: &GraphStatusError{StatusCode: status}}) {
			t.Fatalf("status %d should use code-only fallback", status)
		}
	}
	for _, status := range []int{401, 403, 404, 429, 500, 503} {
		if shouldFallbackTeamsMathMediaError(teamsMathHostedSendError{err: &GraphStatusError{StatusCode: status}}) {
			t.Fatalf("status %d must preserve media for retry", status)
		}
	}
}

func TestPlanTeamsHTMLChunksKeepsMathAtomicAndCapsImages(t *testing.T) {
	t.Parallel()
	var lines []string
	for i := 0; i < maxTeamsMathImagesPerMessage+2; i++ {
		lines = append(lines, fmt.Sprintf("formula %d\n<m>x_%d</m>", i, i))
	}
	chunks := PlanTeamsHTMLChunks(TeamsRenderInput{
		Kind:        TeamsRenderAssistant,
		Text:        strings.Join(lines, "\n"),
		TrustedMath: true,
	}, TeamsRenderOptions{HardLimitBytes: 32 * 1024, TargetLimitBytes: 24 * 1024})
	if len(chunks) != 2 {
		t.Fatalf("chunks = %d, want 2", len(chunks))
	}
	total := 0
	for i, chunk := range chunks {
		plan := parseTrustedTeamsMath(chunk.Text)
		displayCount := 0
		for _, span := range plan.Spans {
			if span.isDisplay() {
				displayCount++
			}
		}
		if displayCount > maxTeamsMathImagesPerMessage {
			t.Fatalf("chunk %d display formulas = %d", i, displayCount)
		}
		if strings.Count(chunk.Text, teamsMathOpenTag) != strings.Count(chunk.Text, teamsMathCloseTag) {
			t.Fatalf("chunk %d split a formula: %q", i, chunk.Text)
		}
		total += len(plan.Spans)
		plannedAssets := plannedTeamsMathAssets(plan)
		actualLength := len(renderTeamsHTMLPartWithMathAssets(TeamsRenderInput{
			Kind: TeamsRenderAssistant, Text: chunk.Text, TrustedMath: true,
		}, chunk.PartIndex, chunk.PartCount, plannedAssets))
		if chunk.ByteLength != actualLength {
			t.Fatalf("chunk %d planned bytes = %d, actual hosted HTML = %d", i, chunk.ByteLength, actualLength)
		}
	}
	if total != len(lines) {
		t.Fatalf("total formulas = %d, want %d", total, len(lines))
	}
}

func TestPlanTeamsHTMLChunksCapsMarkersSeparatelyFromImages(t *testing.T) {
	t.Parallel()
	markers := make([]string, 0, maxTeamsMathMarkersPerMessage+7)
	for index := 0; index < maxTeamsMathMarkersPerMessage+7; index++ {
		markers = append(markers, fmt.Sprintf("v%d=<m>x_%d</m>", index, index))
	}
	chunks := PlanTeamsHTMLChunks(TeamsRenderInput{
		Kind: TeamsRenderAssistant, Text: strings.Join(markers, " "), TrustedMath: true,
	}, TeamsRenderOptions{HardLimitBytes: 32 * 1024, TargetLimitBytes: 24 * 1024})
	if len(chunks) != 2 {
		t.Fatalf("chunks = %d, want 2", len(chunks))
	}
	total := 0
	for _, chunk := range chunks {
		plan := parseTrustedTeamsMath(chunk.Text)
		if len(plan.Spans) > maxTeamsMathMarkersPerMessage {
			t.Fatalf("chunk markers = %d", len(plan.Spans))
		}
		for _, span := range plan.Spans {
			if span.isDisplay() {
				t.Fatalf("inline marker was classified display: %#v", span)
			}
		}
		total += len(plan.Spans)
	}
	if total != len(markers) {
		t.Fatalf("total markers = %d, want %d", total, len(markers))
	}
}

func TestPlanTeamsHTMLChunksCapsCumulativeMathSource(t *testing.T) {
	t.Parallel()
	source := strings.Repeat("x+", 3000) + "x"
	markers := make([]string, 0, 6)
	for index := 0; index < 6; index++ {
		markers = append(markers, fmt.Sprintf("v%d=<m>%s</m>", index, source))
	}
	chunks := PlanTeamsHTMLChunks(TeamsRenderInput{
		Kind: TeamsRenderAssistant, Text: strings.Join(markers, " "), TrustedMath: true,
	}, TeamsRenderOptions{HardLimitBytes: 128 * 1024, TargetLimitBytes: 120 * 1024})
	if len(chunks) != 2 {
		t.Fatalf("chunks = %d, want 2", len(chunks))
	}
	total := 0
	for _, chunk := range chunks {
		plan := parseTrustedTeamsMath(chunk.Text)
		bytes := 0
		for _, span := range plan.Spans {
			bytes += len(span.Source)
		}
		if bytes > maxTeamsMathTotalSourceBytes {
			t.Fatalf("chunk source bytes = %d", bytes)
		}
		outbox := teamstore.OutboxMessage{
			Kind: "final", Body: chunk.Text, TrustedMath: true, MathPlanVersion: teamsMathPlanVersion,
			MathSpans: storeTeamsMathPlan(plan),
		}
		if trusted := trustedTeamsMathPlanForOutbox(outbox); len(trusted.Spans) != len(plan.Spans) {
			t.Fatalf("chunk was not trusted: got %d spans, want %d", len(trusted.Spans), len(plan.Spans))
		}
		total += len(plan.Spans)
	}
	if total != len(markers) {
		t.Fatalf("total spans = %d, want %d", total, len(markers))
	}
}

func FuzzParseTrustedTeamsMath(f *testing.F) {
	for _, seed := range []string{
		`<m>x_i</m>`, "```\n<m>x</m>\n```", `` + "`<m>x</m>`", `<m><m>x</m></m>`,
		`C:\tmp\[draft\]\x`, `\[A-Z\]`, `<m>${HOME}</m>`, `<m>{x</m>`,
		`结果是 <m>\frac{a}{b}</m>。`, "| Formula | Note |\n|---|---|\n| <m>\\frac{a}{b}</m> | ok |",
		`- <m>\sum_i x_i</m>`, `<table><tr><td><m>\int x\,dx</m></td></tr></table>`,
	} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, text string) {
		plan := parseTrustedTeamsMath(text)
		last := 0
		for _, span := range plan.Spans {
			if span.Start < last || span.Start < 0 || span.End > len(text) || span.Start >= span.End {
				t.Fatalf("invalid ordered span %#v for %q", span, text)
			}
			if text[span.Start:span.End] != teamsMathOpenTag+span.Source+teamsMathCloseTag {
				t.Fatalf("span/source mismatch %#v for %q", span, text)
			}
			switch span.Layout {
			case teamsMathLayoutInlineCode, teamsMathLayoutDisplayOwnLine, teamsMathLayoutDisplayPromoted, teamsMathLayoutContainerCode:
			default:
				t.Fatalf("invalid layout %d in span %#v for %q", span.Layout, span, text)
			}
			if span.isDisplay() != (span.Layout == teamsMathLayoutDisplayOwnLine || span.Layout == teamsMathLayoutDisplayPromoted) {
				t.Fatalf("display/layout mismatch in span %#v for %q", span, text)
			}
			last = span.End
		}
	})
}

func TestParseTrustedTeamsMathDeterministicStress(t *testing.T) {
	formulas := []string{"x_i", `\frac{a}{b}`, `\sum_{i=1}^n x_i`, `A=\operatorname{softmax}(QK^T/\sqrt{d})V`, "α+β=γ"}
	plain := []string{`\[A-Z\]`, `\(foo\)`, `C:\tmp\[draft\]\x`, `$PATH`, `${HOME}`, `$(date)`, `$20`, `https://e.test/<m>x</m>`, `foo_bar`, `[docs](https://e.test)`}
	rng := rand.New(rand.NewSource(0xC0DE))
	for round := 0; round < 50_000; round++ {
		var parts []string
		var want []string
		for i := 0; i < 1+rng.Intn(10); i++ {
			switch rng.Intn(6) {
			case 0, 1:
				parts = append(parts, plain[rng.Intn(len(plain))])
			case 2, 3:
				source := formulas[rng.Intn(len(formulas))]
				parts = append(parts, teamsMathOpenTag+source+teamsMathCloseTag)
				want = append(want, source)
			case 4:
				source := formulas[rng.Intn(len(formulas))]
				parts = append(parts, "`"+teamsMathOpenTag+source+teamsMathCloseTag+"`")
			default:
				parts = append(parts, `<span data-x="<m>x</m>">html</span>`)
			}
		}
		text := strings.Join(parts, []string{" ", "\n", "，", " | "}[rng.Intn(4)])
		plan := parseTrustedTeamsMath(text)
		got := make([]string, 0, len(plan.Spans))
		for _, span := range plan.Spans {
			got = append(got, span.Source)
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("round %d: got=%#v want=%#v text=%q", round, got, want, text)
		}
	}

	nearMiss := []string{`<mm>x</mm>`, `<m->x</m->`, `<M>x</M>`, `<m attr=x>y</m>`, `<math>x</math>`, "`<m>x</m>`", `<m><m>x</m></m>`, `\[A-Z\]`, `\(foo\)`, `$PATH`, `$(date)`}
	for round := 0; round < 50_000; round++ {
		var parts []string
		for i := 0; i < 1+rng.Intn(10); i++ {
			parts = append(parts, nearMiss[rng.Intn(len(nearMiss))])
		}
		text := strings.Join(parts, []string{" ", "\n", " | "}[rng.Intn(3)])
		if got := parseTrustedTeamsMath(text).Spans; len(got) != 0 {
			t.Fatalf("near miss %d: got=%#v text=%q", round, got, text)
		}
	}

	alphabet := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789<>/m`~[](){}\\$:_- \n")
	for round := 0; round < 50_000; round++ {
		value := make([]rune, rng.Intn(200))
		for i := range value {
			value[i] = alphabet[rng.Intn(len(alphabet))]
		}
		text := string(value)
		last := 0
		for _, span := range parseTrustedTeamsMath(text).Spans {
			if span.Start < last || span.Start < 0 || span.End > len(text) || text[span.Start:span.End] != teamsMathOpenTag+span.Source+teamsMathCloseTag {
				t.Fatalf("malformed %d produced invalid span %#v for %q", round, span, text)
			}
			last = span.End
		}
	}
}
