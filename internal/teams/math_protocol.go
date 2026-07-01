package teams

import (
	"html"
	"regexp"
	"strings"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	teamsMathOpenTag             = "<m>"
	teamsMathCloseTag            = "</m>"
	maxTeamsMathSourceBytes      = 8 * 1024
	maxTeamsMathBraceDepth       = 64
	maxTeamsMathEscapedHTMLBytes = 16 * 1024
	maxTeamsMathPerMessage       = 5
	teamsMathPlanVersion         = 1
)

var (
	teamsMathBareURLPattern  = regexp.MustCompile(`(?i)(?:https?://|www\.)[^\s，。、|]+`)
	teamsMathEmailPattern    = regexp.MustCompile(`\b[\w.+-]+@[\w.-]+\.[A-Za-z]{2,}\b`)
	teamsMathWinPathPattern  = regexp.MustCompile(`\b[A-Za-z]:\\`)
	teamsMathUnixPathPattern = regexp.MustCompile(
		`(?:^|\s)/(?:home|usr|var|etc|tmp)/`,
	)
	teamsMathUnsafeCommandPattern = regexp.MustCompile(
		`(?i)\\(?:require|includegraphics|href|url|htmlclass|htmlstyle|style|class|def|gdef|edef|xdef|newcommand|renewcommand|let|futurelet|csname|endcsname|loop|repeat|input|include|openin|openout|read|write|catcode|usepackage)\b`,
	)
)

type teamsMathSpan struct {
	Start  int
	End    int
	Source string
	Index  int
}

type teamsMathPlan struct {
	Text  string
	Spans []teamsMathSpan
}

func storeTeamsMathPlan(plan teamsMathPlan) []teamstore.OutboxMathSpan {
	spans := make([]teamstore.OutboxMathSpan, 0, len(plan.Spans))
	for _, span := range plan.Spans {
		spans = append(spans, teamstore.OutboxMathSpan{
			Start: span.Start, End: span.End, Index: span.Index, Source: span.Source,
		})
	}
	return spans
}

func trustedTeamsMathPlanForOutbox(outbox teamstore.OutboxMessage) teamsMathPlan {
	plan := teamsMathPlan{Text: outbox.Body}
	if !outbox.TrustedMath || outbox.MathPlanVersion != teamsMathPlanVersion || len(outbox.MathSpans) > maxTeamsMathPerMessage {
		return plan
	}
	last := 0
	for i, stored := range outbox.MathSpans {
		if stored.Start < last || stored.Start < 0 || stored.End > len(outbox.Body) || stored.Start >= stored.End || stored.Index != i+1 || !safeTeamsMathSource(stored.Source) {
			return teamsMathPlan{Text: outbox.Body}
		}
		if outbox.Body[stored.Start:stored.End] != teamsMathOpenTag+stored.Source+teamsMathCloseTag {
			return teamsMathPlan{Text: outbox.Body}
		}
		plan.Spans = append(plan.Spans, teamsMathSpan{
			Start: stored.Start, End: stored.End, Index: stored.Index, Source: stored.Source,
		})
		last = stored.End
	}
	return plan
}

func parseTrustedTeamsMath(text string) teamsMathPlan {
	plan := teamsMathPlan{Text: text}
	if !strings.Contains(text, teamsMathOpenTag) {
		return plan
	}
	mask := protectedTeamsMathMask(text)
	for i := 0; i < len(text); {
		if mask[i] || !strings.HasPrefix(text[i:], teamsMathOpenTag) || teamsMathMarkdownEscapedAt(text, i) {
			i++
			continue
		}
		sourceStart := i + len(teamsMathOpenTag)
		closeAt := -1
		nested := false
		nestedAt := -1
		for j := sourceStart; j < len(text); j++ {
			if mask[j] {
				continue
			}
			if strings.HasPrefix(text[j:], teamsMathOpenTag) {
				nested = true
				nestedAt = j
				break
			}
			if strings.HasPrefix(text[j:], teamsMathCloseTag) {
				closeAt = j
				break
			}
		}
		if nested {
			if nestedClose := strings.Index(text[nestedAt+len(teamsMathOpenTag):], teamsMathCloseTag); nestedClose >= 0 {
				i = nestedAt + len(teamsMathOpenTag) + nestedClose + len(teamsMathCloseTag)
			} else {
				i = sourceStart
			}
			continue
		}
		if closeAt < 0 {
			i = sourceStart
			continue
		}
		source := text[sourceStart:closeAt]
		if safeTeamsMathSource(source) {
			plan.Spans = append(plan.Spans, teamsMathSpan{
				Start:  i,
				End:    closeAt + len(teamsMathCloseTag),
				Source: source,
				Index:  len(plan.Spans) + 1,
			})
			i = closeAt + len(teamsMathCloseTag)
			continue
		}
		i = sourceStart
	}
	return plan
}

func protectedTeamsMathMask(text string) []bool {
	mask := make([]bool, len(text))
	mark := func(start, end int) {
		if start < 0 {
			start = 0
		}
		if end > len(mask) {
			end = len(mask)
		}
		for i := start; i < end; i++ {
			mask[i] = true
		}
	}

	lines := strings.SplitAfter(text, "\n")
	offset := 0
	inFence := false
	inHTMLComment := false
	var fence byte
	var fenceLen int
	type rawHTMLRegion struct {
		name  string
		start int
	}
	var rawHTMLStack []rawHTMLRegion
	for _, line := range lines {
		bare := strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
		if inHTMLComment {
			end := strings.Index(bare, "-->")
			if end < 0 {
				mark(offset, offset+len(line))
				offset += len(line)
				continue
			}
			mark(offset, offset+end+3)
			inHTMLComment = false
		}
		if inFence {
			mark(offset, offset+len(line))
			if teamsMarkdownFenceEnd(bare, fence, fenceLen) {
				inFence = false
			}
			offset += len(line)
			continue
		}
		if f, n, ok := teamsMarkdownFenceStart(bare); ok {
			inFence, fence, fenceLen = true, f, n
			mark(offset, offset+len(line))
			offset += len(line)
			continue
		}
		if teamsMarkdownIsIndentedCodeLine(bare) {
			mark(offset, offset+len(line))
			offset += len(line)
			continue
		}

		for i := 0; i < len(bare); {
			if bare[i] != '`' {
				i++
				continue
			}
			run := teamsMarkdownDelimiterRun(bare, i, '`')
			end := teamsMarkdownFindBacktickRun(bare, i+run, run)
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
			if strings.HasPrefix(bare[i:], teamsMathOpenTag) || strings.HasPrefix(bare[i:], teamsMathCloseTag) {
				i++
				continue
			}
			if strings.HasPrefix(bare[i:], "<!--") {
				endRel := strings.Index(bare[i+4:], "-->")
				if endRel < 0 {
					mark(offset+i, offset+len(line))
					inHTMLComment = true
					break
				}
				end := i + 4 + endRel + 3
				mark(offset+i, offset+end)
				i = end
				continue
			}
			end := teamsMathHTMLTagEnd(bare, i)
			if end < 0 {
				if teamsMathLooksLikeHTMLTagStart(bare, i) {
					mark(offset+i, offset+len(line))
					break
				}
				i++
				continue
			}
			inner := bare[i+1 : end]
			_, _, _, htmlTag := teamsMathHTMLTag(inner)
			if strings.Contains(inner, "://") || strings.Contains(inner, "@") || htmlTag {
				mark(offset+i, offset+end+1)
			}
			if name, closing, selfClosing, ok := teamsMathRawHTMLTag(inner); ok {
				if closing {
					for k := len(rawHTMLStack) - 1; k >= 0; k-- {
						if rawHTMLStack[k].name != name {
							continue
						}
						mark(rawHTMLStack[k].start, offset+end+1)
						rawHTMLStack = rawHTMLStack[:k]
						break
					}
				} else if !selfClosing {
					rawHTMLStack = append(rawHTMLStack, rawHTMLRegion{name: name, start: offset + i})
				}
			}
			i = end + 1
		}
		offset += len(line)
	}
	for _, region := range rawHTMLStack {
		mark(region.start, len(text))
	}

	for _, match := range teamsMathBareURLPattern.FindAllStringIndex(text, -1) {
		mark(match[0], match[1])
	}
	return mask
}

func teamsMathHTMLTagEnd(text string, start int) int {
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

func teamsMathMarkdownEscapedAt(text string, start int) bool {
	slashes := 0
	for i := start - 1; i >= 0 && text[i] == '\\'; i-- {
		slashes++
	}
	return slashes%2 == 1
}

func teamsMathLooksLikeHTMLTagStart(text string, start int) bool {
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
	return teamsMathRawHTMLTags[name] || teamsMathVoidHTMLTags[name] || strings.Contains(name, "-")
}

func teamsMathRawHTMLTag(inner string) (name string, closing bool, selfClosing bool, ok bool) {
	name, closing, selfClosing, ok = teamsMathHTMLTag(inner)
	if !ok || name == "m" || (!teamsMathRawHTMLTags[name] && !teamsMathVoidHTMLTags[name] && !strings.Contains(name, "-")) {
		return "", false, false, false
	}
	selfClosing = selfClosing || teamsMathVoidHTMLTags[name]
	return name, closing, selfClosing, true
}

func teamsMathHTMLTag(inner string) (name string, closing bool, selfClosing bool, ok bool) {
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
	remainder := inner[end:]
	if remainder != "" && remainder[0] != '/' && remainder[0] != ' ' && remainder[0] != '\t' && remainder[0] != '\r' && remainder[0] != '\n' {
		return "", false, false, false
	}
	var quote byte
	for i := 0; i < len(remainder); i++ {
		if quote != 0 {
			if remainder[i] == quote {
				quote = 0
			}
			continue
		}
		switch remainder[i] {
		case '\'', '"':
			quote = remainder[i]
		case '<':
			return "", false, false, false
		}
	}
	if quote != 0 {
		return "", false, false, false
	}
	selfClosing = strings.HasSuffix(strings.TrimSpace(inner), "/")
	return name, closing, selfClosing, true
}

var teamsMathRawHTMLTags = map[string]bool{
	"a": true, "address": true, "article": true, "aside": true, "audio": true,
	"b": true, "bdi": true, "bdo": true, "blockquote": true, "body": true, "button": true,
	"canvas": true, "caption": true, "cite": true, "code": true, "data": true,
	"datalist": true, "dd": true, "del": true, "details": true, "dfn": true,
	"dialog": true, "div": true, "dl": true, "dt": true, "em": true, "fieldset": true,
	"figcaption": true, "figure": true, "footer": true, "form": true, "h1": true,
	"h2": true, "h3": true, "h4": true, "h5": true, "h6": true, "head": true,
	"header": true, "hgroup": true, "html": true, "i": true, "kbd": true,
	"label": true, "legend": true, "li": true, "main": true, "map": true,
	"mark": true, "math": true, "menu": true, "meter": true, "nav": true,
	"noscript": true, "object": true, "ol": true, "optgroup": true, "option": true,
	"output": true, "p": true, "picture": true, "pre": true, "progress": true,
	"q": true, "rp": true, "rt": true, "ruby": true, "s": true, "samp": true,
	"script": true, "search": true, "section": true, "select": true, "slot": true,
	"small": true, "span": true, "strong": true, "style": true, "sub": true,
	"summary": true, "sup": true, "table": true, "tbody": true, "td": true,
	"template": true, "textarea": true, "tfoot": true, "th": true, "thead": true,
	"time": true, "title": true, "tr": true, "u": true, "ul": true, "var": true,
	"video": true,
}

var teamsMathVoidHTMLTags = map[string]bool{
	"area": true, "base": true, "br": true, "col": true, "embed": true,
	"hr": true, "img": true, "input": true, "link": true, "meta": true,
	"param": true, "source": true, "track": true, "wbr": true,
}

func safeTeamsMathSource(source string) bool {
	if strings.TrimSpace(source) == "" || len(source) > maxTeamsMathSourceBytes {
		return false
	}
	if len(html.EscapeString(source)) > maxTeamsMathEscapedHTMLBytes {
		return false
	}
	if strings.ContainsRune(source, 0) || strings.Contains(source, teamsMathOpenTag) || strings.Contains(source, teamsMathCloseTag) {
		return false
	}
	for _, token := range []string{`\(`, `\)`, `\[`, `\]`, "${", "$(", "$["} {
		if strings.Contains(source, token) {
			return false
		}
	}
	if containsUnescapedDollar(source) || strings.Contains(source, "://") || teamsMathWinPathPattern.MatchString(source) || teamsMathUnixPathPattern.MatchString(source) || teamsMathEmailPattern.MatchString(source) || teamsMathUnsafeCommandPattern.MatchString(source) {
		return false
	}
	depth := 0
	peak := 0
	escaped := false
	for _, r := range source {
		if escaped {
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		switch r {
		case '{':
			depth++
			if depth > peak {
				peak = depth
			}
		case '}':
			depth--
			if depth < 0 {
				return false
			}
		}
	}
	return depth == 0 && peak <= maxTeamsMathBraceDepth
}

func containsUnescapedDollar(text string) bool {
	for i := 0; i < len(text); i++ {
		if text[i] != '$' {
			continue
		}
		slashes := 0
		for j := i - 1; j >= 0 && text[j] == '\\'; j-- {
			slashes++
		}
		if slashes%2 == 0 {
			return true
		}
	}
	return false
}
