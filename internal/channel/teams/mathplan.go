package teams

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"
)

// The formula plan of v0.1.20 (math_protocol.go / math_render.go there).
//
// A <m>TeX</m> span outside code, links, URLs and raw HTML is a formula.
// It is a display formula when it owns its line(s), or when it is
// structurally complex (fractions, sums, line breaks, long equations) and
// not inside a list item, quote or table; any other formula is inline code.
// Each span is replaced by an opaque token before the Markdown is parsed (a
// display token on its own paragraph), and the token is replaced by the
// formula's HTML after rendering: <pre><code>TeX</code></pre> (followed by
// the rendered image for a top-level display formula) or <code>TeX</code>.

type mathLayout uint8

const (
	mathInline mathLayout = iota
	mathOwnLine
	mathPromoted
	mathContainer
)

type mathSpan struct {
	start, end int
	src        string
	layout     mathLayout
}

func (s mathSpan) display() bool { return s.layout == mathOwnLine || s.layout == mathPromoted }

// mathToken is one formula in the tokenized Markdown.
type mathToken struct {
	tok     string
	src     string
	display bool
}

var mathBareURLRE = regexp.MustCompile(`(?i)(?:https?://|www\.)[^\s，。、|]+`)

// planMath finds the formulas of text (already normalized and compacted)
// and returns the Markdown with each formula replaced by a token.
func planMath(text string) (string, []mathToken) {
	spans := findMath(text)
	if len(spans) == 0 {
		return text, nil
	}
	var md strings.Builder
	tail := ""
	write := func(s string) {
		md.WriteString(s)
		tail += s
		if len(tail) > 2 {
			tail = tail[len(tail)-2:]
		}
	}
	prefix := mathTokenPrefix(text)
	toks := make([]mathToken, 0, len(spans))
	last := 0
	for i, sp := range spans {
		write(text[last:sp.start])
		tok := fmt.Sprintf("%s%06d", prefix, i+1)
		toks = append(toks, mathToken{tok: tok, src: sp.src, display: sp.display()})
		if sp.display() && md.Len() > 0 && tail != "\n\n" {
			write("\n\n")
		}
		write(tok)
		if sp.display() {
			write("\n\n")
		}
		last = sp.end
	}
	write(text[last:])
	return md.String(), toks
}

func mathTokenPrefix(text string) string {
	sum := sha256.Sum256([]byte(text))
	base := "cxp-math-" + hex.EncodeToString(sum[:8])
	for n := 0; ; n++ {
		if p := fmt.Sprintf("%s-%d-", base, n); !strings.Contains(text, p) {
			return p
		}
	}
}

// findMath is v0.1.20 parseTrustedTeamsMath plus its layout classification.
func findMath(text string) []mathSpan {
	if !strings.Contains(text, mathOpen) {
		return nil
	}
	mask := mathProtectedMask(text)
	var spans []mathSpan
	for i := 0; i < len(text); {
		if mask[i] || !strings.HasPrefix(text[i:], mathOpen) || escapedAt(text, i) {
			i++
			continue
		}
		srcStart := i + len(mathOpen)
		closeAt, nestedAt := -1, -1
		for j := srcStart; j < len(text); j++ {
			if mask[j] {
				continue
			}
			if strings.HasPrefix(text[j:], mathOpen) {
				nestedAt = j
				break
			}
			if strings.HasPrefix(text[j:], mathClose) {
				closeAt = j
				break
			}
		}
		if nestedAt >= 0 {
			if k := strings.Index(text[nestedAt+len(mathOpen):], mathClose); k >= 0 {
				i = nestedAt + len(mathOpen) + k + len(mathClose)
			} else {
				i = srcStart
			}
			continue
		}
		if closeAt < 0 {
			i = srcStart
			continue
		}
		if src := text[srcStart:closeAt]; safeTeX(src) {
			spans = append(spans, mathSpan{start: i, end: closeAt + len(mathClose), src: src})
			i = closeAt + len(mathClose)
			continue
		}
		i = srcStart
	}
	var table []bool
	if strings.Contains(text, "|") {
		table = tableMask(text)
	}
	for k := range spans {
		sp := &spans[k]
		switch {
		case sp.start < len(table) && table[sp.start]:
			sp.layout = mathContainer
		case ownsLine(text, sp.start, sp.end):
			sp.layout = mathOwnLine
		case inLineContainer(text, sp.start):
			sp.layout = mathContainer
		case complexTeX(sp.src):
			sp.layout = mathPromoted
		default:
			sp.layout = mathInline
		}
	}
	return spans
}

func ownsLine(text string, start, end int) bool {
	lineStart := strings.LastIndex(text[:start], "\n") + 1
	lineEnd := len(text)
	if next := strings.Index(text[end:], "\n"); next >= 0 {
		lineEnd = end + next
	}
	return strings.TrimSpace(text[lineStart:start]) == "" && strings.TrimSpace(text[end:lineEnd]) == ""
}

// inLineContainer reports a formula on a quote or list item line.
func inLineContainer(text string, start int) bool {
	lineStart := strings.LastIndex(text[:start], "\n") + 1
	prefix := strings.TrimLeft(text[lineStart:start], " \t")
	if strings.HasPrefix(prefix, ">") {
		return true
	}
	if len(prefix) >= 2 && (prefix[0] == '-' || prefix[0] == '*' || prefix[0] == '+') && (prefix[1] == ' ' || prefix[1] == '\t') {
		return true
	}
	n := 0
	for n < len(prefix) && prefix[n] >= '0' && prefix[n] <= '9' {
		n++
	}
	return n > 0 && n+1 < len(prefix) && (prefix[n] == '.' || prefix[n] == ')') && (prefix[n+1] == ' ' || prefix[n+1] == '\t')
}

// complexTeX reports a formula too tall or long to read inline.
func complexTeX(src string) bool {
	if strings.ContainsAny(src, "\r\n") {
		return true
	}
	t := strings.TrimSpace(src)
	n := utf8.RuneCountInString(t)
	if n > 72 || (n > 36 && strings.ContainsAny(t, "=<>")) {
		return true
	}
	for i := 0; i < len(src); i++ {
		if src[i] != '\\' || i+1 >= len(src) {
			continue
		}
		if src[i+1] == '\\' {
			return true
		}
		end := i + 1
		for end < len(src) && ((src[end] >= 'a' && src[end] <= 'z') || (src[end] >= 'A' && src[end] <= 'Z')) {
			end++
		}
		if end == i+1 {
			continue
		}
		switch src[i+1 : end] {
		case "frac", "dfrac", "tfrac", "cfrac", "genfrac",
			"binom", "dbinom", "tbinom",
			"sum", "prod", "coprod", "bigcap", "bigcup", "bigoplus", "bigotimes", "bigsqcup", "bigvee", "bigwedge",
			"int", "iint", "iiint", "iiiint", "oint", "oiint", "oiiint",
			"lim", "liminf", "limsup", "argmin", "argmax",
			"begin", "substack", "overset", "underset", "stackrel", "overbrace", "underbrace", "displaystyle":
			return true
		}
		i = end - 1
	}
	return false
}

// tableMask marks the bytes of text inside Markdown tables.
func tableMask(text string) []bool {
	mask := make([]bool, len(text))
	lines := strings.Split(text, "\n")
	offsets := make([]int, len(lines)+1)
	for i, line := range lines {
		offsets[i+1] = offsets[i] + len(line)
		if i+1 < len(lines) {
			offsets[i+1]++
		}
	}
	for i := 0; i < len(lines); i++ {
		_, next, ok := parseTable(lines, i)
		if !ok {
			continue
		}
		for j := offsets[i]; j < min(offsets[next], len(mask)); j++ {
			mask[j] = true
		}
		i = next - 1
	}
	return mask
}

func escapedAt(text string, start int) bool {
	n := 0
	for i := start - 1; i >= 0 && text[i] == '\\'; i-- {
		n++
	}
	return n%2 == 1
}

// mathProtectedMask marks where a <m> is not a formula: fenced and
// indented code, code spans, link and image destinations, HTML comments,
// raw HTML elements, tags that look like URLs or addresses, and bare URLs.
func mathProtectedMask(text string) []bool {
	mask := make([]bool, len(text))
	mark := func(start, end int) {
		for i := max(start, 0); i < min(end, len(mask)); i++ {
			mask[i] = true
		}
	}
	type rawRegion struct {
		name  string
		start int
	}
	var raw []rawRegion
	offset := 0
	inFence, inComment := false, false
	var fence byte
	var fenceLen int
	for _, line := range strings.SplitAfter(text, "\n") {
		bare := strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		if inComment {
			end := strings.Index(bare, "-->")
			if end < 0 {
				mark(offset, offset+len(line))
				offset += len(line)
				continue
			}
			mark(offset, offset+end+3)
			inComment = false
		}
		if inFence {
			mark(offset, offset+len(line))
			if fenceEnd(bare, fence, fenceLen) {
				inFence = false
			}
			offset += len(line)
			continue
		}
		if f, n, ok := fenceStart(bare); ok {
			inFence, fence, fenceLen = true, f, n
			mark(offset, offset+len(line))
			offset += len(line)
			continue
		}
		if isIndentedCodeLine(bare) {
			mark(offset, offset+len(line))
			offset += len(line)
			continue
		}

		for i := 0; i < len(bare); {
			if bare[i] != '`' {
				i++
				continue
			}
			run := delimRun(bare, i, '`')
			end := findBacktickRun(bare, i+run, run)
			if end < 0 {
				mark(offset+i, offset+len(bare))
				break
			}
			mark(offset+i, offset+end+run)
			i = end + run
		}

		for i := 0; i+1 < len(bare); {
			if mask[offset+i] || bare[i:i+2] != "](" || (i > 0 && bare[i-1] == '\\') {
				i++
				continue
			}
			depth := 1
			j := i + 2
			for j < len(bare) && depth > 0 {
				if bare[j] == '\\' {
					j += 2
					continue
				}
				switch bare[j] {
				case '(':
					depth++
				case ')':
					depth--
				}
				j++
			}
			if depth == 0 {
				start := i + 2
				for k := i - 1; k >= 0; k-- {
					if bare[k] == ']' {
						break
					}
					if bare[k] == '[' && (k == 0 || bare[k-1] != '\\') {
						start = k
						if k > 0 && bare[k-1] == '!' {
							start = k - 1
						}
						break
					}
				}
				mark(offset+start, offset+j)
				i = j
				continue
			}
			i += 2
		}

		for i := 0; i < len(bare); {
			if mask[offset+i] || bare[i] != '<' {
				i++
				continue
			}
			if strings.HasPrefix(bare[i:], mathOpen) || strings.HasPrefix(bare[i:], mathClose) {
				i++
				continue
			}
			if strings.HasPrefix(bare[i:], "<!--") {
				endRel := strings.Index(bare[i+4:], "-->")
				if endRel < 0 {
					mark(offset+i, offset+len(line))
					inComment = true
					break
				}
				end := i + 4 + endRel + 3
				mark(offset+i, offset+end)
				i = end
				continue
			}
			end := htmlTagEnd(bare, i)
			if end < 0 {
				if looksLikeHTMLTagStart(bare, i) {
					mark(offset+i, offset+len(line))
					break
				}
				i++
				continue
			}
			inner := bare[i+1 : end]
			_, _, _, isTag := htmlTag(inner)
			if strings.Contains(inner, "://") || strings.Contains(inner, "@") || isTag {
				mark(offset+i, offset+end+1)
			}
			if name, closing, selfClosing, ok := rawHTMLTag(inner); ok {
				if closing {
					for k := len(raw) - 1; k >= 0; k-- {
						if raw[k].name != name {
							continue
						}
						mark(raw[k].start, offset+end+1)
						raw = raw[:k]
						break
					}
				} else if !selfClosing {
					raw = append(raw, rawRegion{name: name, start: offset + i})
				}
			}
			i = end + 1
		}
		offset += len(line)
	}
	for _, r := range raw {
		mark(r.start, len(text))
	}
	for _, m := range mathBareURLRE.FindAllStringIndex(text, -1) {
		mark(m[0], m[1])
	}
	return mask
}

func htmlTagEnd(text string, start int) int {
	var quote byte
	for i := start + 1; i < len(text); i++ {
		if quote != 0 {
			if text[i] == quote {
				quote = 0
			}
			continue
		}
		switch text[i] {
		case '\'', '"':
			quote = text[i]
		case '>':
			return i
		}
	}
	return -1
}

func looksLikeHTMLTagStart(text string, start int) bool {
	if start < 0 || start+1 >= len(text) || text[start] != '<' {
		return false
	}
	i := start + 1
	if text[i] == '/' {
		i++
	}
	if i >= len(text) {
		return false
	}
	if text[i] == '!' || text[i] == '?' {
		return true
	}
	nameStart := i
	for i < len(text) {
		c := text[i]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (i > nameStart && ((c >= '0' && c <= '9') || c == '-'))) {
			break
		}
		i++
	}
	if i == nameStart {
		return false
	}
	name := strings.ToLower(text[nameStart:i])
	return rawHTMLTags[name] || voidHTMLTags[name] || strings.Contains(name, "-")
}

func rawHTMLTag(inner string) (name string, closing, selfClosing, ok bool) {
	name, closing, selfClosing, ok = htmlTag(inner)
	if !ok || name == "m" || (!rawHTMLTags[name] && !voidHTMLTags[name] && !strings.Contains(name, "-")) {
		return "", false, false, false
	}
	return name, closing, selfClosing || voidHTMLTags[name], true
}

func htmlTag(inner string) (name string, closing, selfClosing, ok bool) {
	inner = strings.TrimSpace(inner)
	if strings.HasPrefix(inner, "/") {
		closing = true
		inner = strings.TrimSpace(inner[1:])
	}
	end := 0
	for end < len(inner) {
		c := inner[end]
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (end > 0 && ((c >= '0' && c <= '9') || c == '-'))) {
			break
		}
		end++
	}
	if end == 0 {
		return "", false, false, false
	}
	name = strings.ToLower(inner[:end])
	rest := inner[end:]
	if rest != "" && rest[0] != '/' && rest[0] != ' ' && rest[0] != '\t' && rest[0] != '\r' && rest[0] != '\n' {
		return "", false, false, false
	}
	var quote byte
	for i := 0; i < len(rest); i++ {
		if quote != 0 {
			if rest[i] == quote {
				quote = 0
			}
			continue
		}
		switch rest[i] {
		case '\'', '"':
			quote = rest[i]
		case '<':
			return "", false, false, false
		}
	}
	if quote != 0 {
		return "", false, false, false
	}
	return name, closing, strings.HasSuffix(strings.TrimSpace(inner), "/"), true
}

var rawHTMLTags = setOf("a", "address", "article", "aside", "audio",
	"b", "bdi", "bdo", "blockquote", "body", "button",
	"canvas", "caption", "cite", "code", "data",
	"datalist", "dd", "del", "details", "dfn",
	"dialog", "div", "dl", "dt", "em", "fieldset",
	"figcaption", "figure", "footer", "form", "h1",
	"h2", "h3", "h4", "h5", "h6", "head",
	"header", "hgroup", "html", "i", "kbd",
	"label", "legend", "li", "main", "map",
	"mark", "math", "menu", "meter", "nav",
	"noscript", "object", "ol", "optgroup", "option",
	"output", "p", "picture", "pre", "progress",
	"q", "rp", "rt", "ruby", "s", "samp",
	"script", "search", "section", "select", "slot",
	"small", "span", "strong", "style", "sub",
	"summary", "sup", "table", "tbody", "td",
	"template", "textarea", "tfoot", "th", "thead",
	"time", "title", "tr", "u", "ul", "var",
	"video")

var voidHTMLTags = setOf("area", "base", "br", "col", "embed",
	"hr", "img", "input", "link", "meta",
	"param", "source", "track", "wbr")

func setOf(names ...string) map[string]bool {
	m := make(map[string]bool, len(names))
	for _, n := range names {
		m[n] = true
	}
	return m
}
