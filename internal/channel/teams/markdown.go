package teams

import (
	"html"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"
)

// The Markdown reader and HTML writer of v0.1.20 (internal/teams/markdown.go
// there), ported so every message keeps the exact format the old helper
// posted: the same blocks (paragraphs, ATX headings, fenced, indented and
// "pasted" code, rules, block quotes, nested lists with task boxes, GFM
// tables) and the same inlines (code spans, emphasis, strikethrough, links,
// images as text, autolinks, bare URLs). Raw HTML in the source is always
// escaped. Anything it does not recognise is shown as text.

type blockKind int

const (
	bPara blockKind = iota
	bHeading
	bCode
	bRule
	bList
	bTable
	bQuote
	bMath // a display formula (only produced by the math plan)
)

type mdBlock struct {
	kind    blockKind
	level   int    // heading level
	text    string // paragraph, heading or code text
	ordered bool
	items   []mdItem
	head    []string // table header cells
	rows    [][]string
	kids    []mdBlock // quote children
	tex     string    // display formula source
	png     *MathResult
}

type mdItem struct {
	text string
	kids []mdBlock
}

type listMarker struct {
	ordered bool
	indent  int
	content string
}

// compactChatLinkLabel is the label of a Teams chat link that is shown
// without its (long) URL.
const compactChatLinkLabel = "▶ Open chat"

// normalizeText is v0.1.20 normalizeTeamsRenderText.
func normalizeText(text string) string {
	text = strings.ToValidUTF8(text, "�")
	text = strings.ReplaceAll(text, "\r\n", "\n")
	return strings.ReplaceAll(text, "\r", "\n")
}

// compactBlankLines collapses runs of blank lines outside fenced code and
// trims blank edges (v0.1.20 compactTeamsRenderBlankLinesOutsideMarkdownFences).
func compactBlankLines(text string) string {
	text = normalizeText(text)
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	blankSeen := false
	inFence := false
	var fence byte
	var fenceLen int
	for _, line := range lines {
		if inFence {
			out = append(out, line)
			if fenceEnd(line, fence, fenceLen) {
				inFence = false
				blankSeen = false
			}
			continue
		}
		if f, n, ok := fenceStart(line); ok {
			out = append(out, line)
			inFence, fence, fenceLen = true, f, n
			blankSeen = false
			continue
		}
		if strings.TrimSpace(line) == "" {
			if !blankSeen && len(out) > 0 {
				out = append(out, "")
				blankSeen = true
			}
			continue
		}
		out = append(out, line)
		blankSeen = false
	}
	return joinTrimBlankEdges(out)
}

func joinTrimBlankEdges(lines []string) string {
	start := 0
	for start < len(lines) && strings.TrimSpace(lines[start]) == "" {
		start++
	}
	end := len(lines)
	for end > start && strings.TrimSpace(lines[end-1]) == "" {
		end--
	}
	if start >= end {
		return ""
	}
	return strings.Join(lines[start:end], "\n")
}

// ---------------------------------------------------------------------------
// Blocks

func parseBlocks(text string) []mdBlock {
	text = normalizeText(text)
	lines := strings.Split(text, "\n")
	blocks := make([]mdBlock, 0, 1)
	paragraph := make([]string, 0, len(lines))
	flush := func() {
		if body := joinTrimBlankEdges(paragraph); body != "" {
			blocks = append(blocks, mdBlock{kind: bPara, text: body})
		}
		paragraph = paragraph[:0]
	}

	for i := 0; i < len(lines); i++ {
		line := lines[i]
		if fence, fenceLen, ok := fenceStart(line); ok {
			flush()
			var code []string
			i++
			for ; i < len(lines); i++ {
				if fenceEnd(lines[i], fence, fenceLen) {
					break
				}
				code = append(code, lines[i])
			}
			blocks = append(blocks, mdBlock{kind: bCode, text: strings.Join(code, "\n")})
			continue
		}
		if isBlank(line) {
			flush()
			continue
		}
		if block, next, ok := parseQuote(lines, i); ok {
			flush()
			blocks = append(blocks, block)
			i = next - 1
			continue
		}
		if block, next, ok := parseTable(lines, i); ok {
			flush()
			blocks = append(blocks, block)
			i = next - 1
			continue
		}
		if isRule(line) {
			flush()
			blocks = append(blocks, mdBlock{kind: bRule})
			continue
		}
		if marker, ok := parseListMarker(line); ok && marker.indent <= 3 {
			flush()
			block, next := parseList(lines, i, marker)
			if len(block.items) > 0 {
				blocks = append(blocks, block)
				i = next - 1
				continue
			}
		}
		if len(paragraph) == 0 {
			if block, next, ok := parseLooseCode(lines, i); ok {
				flush()
				blocks = append(blocks, block)
				i = next - 1
				continue
			}
		}
		if len(paragraph) == 0 && isIndentedCodeLine(line) && !indentedLooksLikeListMarker(line) {
			flush()
			var code []string
			for ; i < len(lines); i++ {
				if isIndentedCodeLine(lines[i]) {
					code = append(code, stripCodeIndent(lines[i]))
					continue
				}
				if isBlank(lines[i]) && i+1 < len(lines) && isIndentedCodeLine(lines[i+1]) {
					code = append(code, "")
					continue
				}
				i--
				break
			}
			blocks = append(blocks, mdBlock{kind: bCode, text: strings.Join(code, "\n")})
			continue
		}
		if level, heading, ok := parseHeading(line); ok {
			flush()
			blocks = append(blocks, mdBlock{kind: bHeading, level: level, text: heading})
			continue
		}
		paragraph = append(paragraph, line)
	}
	flush()
	return blocks
}

func parseQuote(lines []string, start int) (mdBlock, int, bool) {
	var quoted []string
	i := start
	for ; i < len(lines); i++ {
		text, ok := quoteLine(lines[i])
		if !ok {
			break
		}
		quoted = append(quoted, text)
	}
	if len(quoted) == 0 {
		return mdBlock{}, start, false
	}
	return mdBlock{kind: bQuote, kids: parseBlocks(strings.Join(quoted, "\n"))}, i, true
}

func quoteLine(line string) (string, bool) {
	trimmed := strings.TrimLeft(line, " \t")
	if !strings.HasPrefix(trimmed, ">") {
		return "", false
	}
	body := strings.TrimPrefix(trimmed, ">")
	if strings.HasPrefix(body, " ") {
		body = body[1:]
	}
	return body, true
}

// parseLooseCode keeps code pasted with a one to three space indent
// together (a def/class/import/assignment/call line and what follows it).
func parseLooseCode(lines []string, start int) (mdBlock, int, bool) {
	baseIndent, index := lineIndentAndIndex(lines[start])
	if baseIndent <= 0 || baseIndent >= 4 {
		return mdBlock{}, start, false
	}
	first := strings.TrimSpace(lines[start][index:])
	if !looksLikeCodeStart(first) {
		return mdBlock{}, start, false
	}
	code := []string{first}
	i := start + 1
	for ; i < len(lines); i++ {
		line := lines[i]
		if isBlank(line) {
			if i+1 < len(lines) && looseCodeContinuesAfterBlank(lines[i+1], baseIndent) {
				code = append(code, "")
				continue
			}
			break
		}
		indent := lineIndent(line)
		if indent < baseIndent {
			break
		}
		stripped := stripIndentWidth(line, baseIndent)
		if indent == baseIndent && len(code) > 0 && !looksLikeCodeLine(strings.TrimSpace(stripped)) {
			break
		}
		code = append(code, stripped)
	}
	if len(code) == 1 {
		return mdBlock{}, start, false
	}
	return mdBlock{kind: bCode, text: strings.Join(code, "\n")}, i, true
}

func looseCodeContinuesAfterBlank(line string, baseIndent int) bool {
	if isBlank(line) || lineIndent(line) < baseIndent {
		return false
	}
	stripped := strings.TrimSpace(stripIndentWidth(line, baseIndent))
	return lineIndent(line) > baseIndent || looksLikeCodeLine(stripped)
}

func looksLikeCodeLine(text string) bool {
	if looksLikeCodeStart(text) {
		return true
	}
	trimmed := strings.TrimSpace(text)
	lower := strings.ToLower(trimmed)
	if trimmed == "" {
		return true
	}
	if strings.HasPrefix(trimmed, "#") || strings.HasPrefix(trimmed, "//") {
		return true
	}
	for _, prefix := range []string{
		"return ", "if ", "elif ", "else:", "for ", "while ", "with ", "try:", "except ",
		"finally:", "raise ", "continue", "break", "pass", "case ", "switch ", "catch ",
		"}", ")", "]",
	} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return looksLikeAssignment(trimmed) || looksLikeCall(trimmed)
}

func looksLikeCodeStart(text string) bool {
	trimmed := strings.TrimSpace(text)
	lower := strings.ToLower(trimmed)
	for _, prefix := range []string{
		"async def ", "def ", "class ", "func ", "function ", "import ", "from ",
		"const ", "let ", "var ", "type ", "interface ", "package ",
	} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	for _, prefix := range []string{"get ", "post ", "put ", "patch ", "delete "} {
		if strings.HasPrefix(lower, prefix) && (strings.Contains(trimmed, "/") || strings.Contains(trimmed, "http://") || strings.Contains(trimmed, "https://")) {
			return true
		}
	}
	return looksLikeAssignment(trimmed) || looksLikeCall(trimmed)
}

func looksLikeAssignment(text string) bool {
	for _, op := range []string{":=", "+=", "-=", "*=", "/=", "%=", "="} {
		pos := strings.Index(text, op)
		if pos <= 0 {
			continue
		}
		if op == "=" {
			if strings.ContainsAny(text[pos-1:pos], "!<>=") || (pos+1 < len(text) && text[pos+1] == '=') {
				continue
			}
		}
		return looksLikeIdentifier(strings.TrimSpace(text[:pos]))
	}
	return false
}

func looksLikeCall(text string) bool {
	open := strings.IndexByte(text, '(')
	if open <= 0 {
		return false
	}
	return looksLikeIdentifier(strings.TrimSpace(text[:open]))
}

func looksLikeIdentifier(text string) bool {
	if text == "" {
		return false
	}
	for _, part := range strings.FieldsFunc(text, func(r rune) bool { return r == '.' || r == '[' || r == ']' }) {
		for i, r := range part {
			if i == 0 {
				if r != '_' && !unicode.IsLetter(r) {
					return false
				}
				continue
			}
			if r != '_' && !unicode.IsLetter(r) && !unicode.IsDigit(r) {
				return false
			}
		}
	}
	return true
}

func parseList(lines []string, start int, first listMarker) (mdBlock, int) {
	block := mdBlock{kind: bList, ordered: first.ordered}
	baseIndent := first.indent
	var current []string
	var children []mdBlock
	haveItem := false
	flushSegment := func() {
		if body := joinTrimBlankEdges(current); body != "" {
			children = append(children, mdBlock{kind: bPara, text: body})
		}
		current = current[:0]
	}
	flushItem := func() {
		if !haveItem {
			return
		}
		text := joinTrimBlankEdges(current)
		if len(children) > 0 {
			flushSegment()
			text = ""
		}
		block.items = append(block.items, mdItem{text: text, kids: append([]mdBlock(nil), children...)})
		current = current[:0]
		children = children[:0]
		haveItem = false
	}

	for i := start; i < len(lines); i++ {
		line := lines[i]
		if isBlank(line) {
			if !haveItem {
				continue
			}
			if i+1 < len(lines) {
				if next, ok := parseListMarker(lines[i+1]); ok {
					if next.indent <= baseIndent {
						continue
					}
					current = append(current, "")
					continue
				}
				if lineIndent(lines[i+1]) > baseIndent {
					current = append(current, "")
					continue
				}
			}
			flushItem()
			return block, i + 1
		}

		if marker, ok := parseListMarker(line); ok {
			if marker.indent <= baseIndent {
				if marker.ordered != first.ordered {
					flushItem()
					return block, i
				}
				flushItem()
				current = append(current, marker.content)
				haveItem = true
				continue
			}
			if haveItem {
				child, next := parseList(lines, i, marker)
				if len(child.items) > 0 {
					flushSegment()
					children = append(children, child)
					i = next - 1
					continue
				}
				current = append(current, stripIndentWidth(line, baseIndent))
				continue
			}
		}

		if !haveItem {
			return block, i
		}
		if child, next, ok := parseIndentedFence(lines, i, baseIndent); ok {
			flushSegment()
			children = append(children, child)
			i = next - 1
			continue
		}
		if child, next, ok := parseLooseCode(lines, i); ok && lineIndent(line) > baseIndent {
			flushSegment()
			children = append(children, child)
			i = next - 1
			continue
		}
		if listContinuationCloses(line, baseIndent) {
			flushItem()
			return block, i
		}
		current = append(current, stripIndentWidth(line, baseIndent))
	}
	flushItem()
	return block, len(lines)
}

func listContinuationCloses(line string, baseIndent int) bool {
	indent := lineIndent(line)
	if indent < baseIndent {
		return true
	}
	return baseIndent <= 0 && indent <= baseIndent
}

func parseIndentedFence(lines []string, start int, parentIndent int) (mdBlock, int, bool) {
	indent := lineIndent(lines[start])
	if indent <= parentIndent {
		return mdBlock{}, start, false
	}
	fence, fenceLen, ok := fenceStart(lines[start])
	if !ok {
		return mdBlock{}, start, false
	}
	code := make([]string, 0, 1)
	i := start + 1
	for ; i < len(lines); i++ {
		if fenceEnd(lines[i], fence, fenceLen) {
			return mdBlock{kind: bCode, text: strings.Join(code, "\n")}, i + 1, true
		}
		code = append(code, stripIndentWidth(lines[i], indent))
	}
	return mdBlock{kind: bCode, text: strings.Join(code, "\n")}, i, true
}

func parseTable(lines []string, start int) (mdBlock, int, bool) {
	if start+1 >= len(lines) || !hasTablePipe(lines[start]) {
		return mdBlock{}, start, false
	}
	header := splitTableRow(lines[start])
	delimiter := splitTableRow(lines[start+1])
	if len(header) < 2 || len(delimiter) != len(header) || !isTableDelimiter(delimiter) {
		return mdBlock{}, start, false
	}
	b := mdBlock{kind: bTable, head: normalizeTableRow(header, len(header))}
	next := start + 2
	for ; next < len(lines); next++ {
		line := lines[next]
		if isBlank(line) || !hasTablePipe(line) {
			break
		}
		if _, _, ok := fenceStart(line); ok {
			break
		}
		row := splitTableRow(line)
		if len(row) < 2 {
			break
		}
		b.rows = append(b.rows, normalizeTableRow(row, len(b.head)))
	}
	return b, next, true
}

// normalizeTableRow pads a row to the header's width; cells past it stay in
// the last column so text written after a row's closing pipe is kept.
func normalizeTableRow(row []string, columns int) []string {
	out := make([]string, columns)
	for i := 0; i < columns; i++ {
		if i == columns-1 && len(row) > columns {
			out[i] = strings.TrimSpace(strings.Join(row[i:], " | "))
		} else if i < len(row) {
			out[i] = strings.TrimSpace(row[i])
		}
	}
	return out
}

func isTableDelimiter(cells []string) bool {
	for _, cell := range cells {
		t := strings.TrimSpace(cell)
		if strings.HasPrefix(t, ":") {
			t = strings.TrimSpace(t[1:])
		}
		if strings.HasSuffix(t, ":") {
			t = strings.TrimSpace(t[:len(t)-1])
		}
		t = strings.ReplaceAll(t, " ", "")
		if len(t) < 1 || strings.Trim(t, "-") != "" {
			return false
		}
	}
	return true
}

func splitTableRow(line string) []string {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return nil
	}
	pipes := tablePipePositions(trimmed)
	if len(pipes) == 0 {
		return []string{trimmed}
	}
	start, end := 0, len(trimmed)
	inner := pipes[:0]
	for _, p := range pipes {
		switch {
		case p == 0:
			start = 1
		case p == len(trimmed)-1:
			end = len(trimmed) - 1
		default:
			inner = append(inner, p)
		}
	}
	var cells []string
	last := start
	for _, p := range inner {
		if p < start || p > end {
			continue
		}
		cells = append(cells, strings.TrimSpace(trimmed[last:p]))
		last = p + 1
	}
	return append(cells, strings.TrimSpace(trimmed[last:end]))
}

func hasTablePipe(line string) bool {
	return len(tablePipePositions(strings.TrimSpace(line))) > 0
}

// tablePipePositions lists the cell separators: pipes that are neither
// escaped nor inside a code span.
func tablePipePositions(text string) []int {
	var positions []int
	codeRun := 0
	for i := 0; i < len(text); {
		if text[i] == '\\' {
			if _, size := runeAt(text, i+1); size > 0 {
				i += 1 + size
				continue
			}
			i++
			continue
		}
		if text[i] == '`' {
			run := delimRun(text, i, '`')
			if codeRun == 0 {
				codeRun = run
			} else if run >= codeRun {
				codeRun = 0
			}
			i += run
			continue
		}
		if text[i] == '|' && codeRun == 0 {
			positions = append(positions, i)
		}
		if _, size := runeAt(text, i); size <= 0 {
			i++
		} else {
			i += size
		}
	}
	return positions
}

func fenceStart(line string) (byte, int, bool) {
	trimmed := strings.TrimLeft(line, " \t")
	if trimmed == "" || (trimmed[0] != '`' && trimmed[0] != '~') {
		return 0, 0, false
	}
	fence := trimmed[0]
	count := delimRun(trimmed, 0, fence)
	if count < 3 {
		return 0, 0, false
	}
	return fence, count, true
}

func fenceEnd(line string, fence byte, fenceLen int) bool {
	trimmed := strings.TrimLeft(line, " \t")
	if len(trimmed) < fenceLen || trimmed[0] != fence {
		return false
	}
	count := delimRun(trimmed, 0, fence)
	return count >= fenceLen && strings.TrimSpace(trimmed[count:]) == ""
}

func isBlank(line string) bool { return strings.TrimSpace(line) == "" }

func isIndentedCodeLine(line string) bool {
	return strings.HasPrefix(line, "    ") || strings.HasPrefix(line, "\t")
}

func stripCodeIndent(line string) string {
	if strings.HasPrefix(line, "\t") {
		return line[1:]
	}
	if strings.HasPrefix(line, "    ") {
		return line[4:]
	}
	return line
}

func indentedLooksLikeListMarker(line string) bool {
	s := strings.TrimLeft(line, " \t")
	if len(s) >= 2 && (strings.HasPrefix(s, "- ") || strings.HasPrefix(s, "* ") || strings.HasPrefix(s, "+ ")) {
		return true
	}
	n := 0
	for n < len(s) && s[n] >= '0' && s[n] <= '9' {
		n++
	}
	return n > 0 && n+1 < len(s) && (s[n] == '.' || s[n] == ')') && s[n+1] == ' '
}

func parseListMarker(line string) (listMarker, bool) {
	indent, index := lineIndentAndIndex(line)
	t := line[index:]
	if len(t) >= 2 && (strings.HasPrefix(t, "- ") || strings.HasPrefix(t, "* ") || strings.HasPrefix(t, "+ ")) {
		return listMarker{indent: indent, content: taskContent(strings.TrimSpace(t[2:]))}, true
	}
	digits := 0
	for digits < len(t) && t[digits] >= '0' && t[digits] <= '9' {
		digits++
	}
	if digits == 0 || digits > 9 || digits+1 >= len(t) || (t[digits] != '.' && t[digits] != ')') || t[digits+1] != ' ' {
		return listMarker{}, false
	}
	return listMarker{ordered: true, indent: indent, content: taskContent(strings.TrimSpace(t[digits+2:]))}, true
}

// taskContent shows task boxes as ☐ / ☑.
func taskContent(content string) string {
	lower := strings.ToLower(content)
	if strings.HasPrefix(lower, "[ ] ") {
		return "☐ " + strings.TrimSpace(content[4:])
	}
	if strings.HasPrefix(lower, "[x] ") {
		return "☑ " + strings.TrimSpace(content[4:])
	}
	return content
}

func lineIndent(line string) int {
	indent, _ := lineIndentAndIndex(line)
	return indent
}

// lineIndentAndIndex counts leading columns (tab = 4) and returns the byte
// index of the first other character.
func lineIndentAndIndex(line string) (int, int) {
	indent, index := 0, 0
	for index < len(line) {
		switch line[index] {
		case ' ':
			indent++
		case '\t':
			indent += 4
		default:
			return indent, index
		}
		index++
	}
	return indent, index
}

func stripIndentWidth(line string, width int) string {
	if width <= 0 {
		return line
	}
	remaining := width
	for i := 0; i < len(line); i++ {
		if line[i] != ' ' && line[i] != '\t' {
			return line[i:]
		}
		if line[i] == '\t' {
			remaining -= 4
		} else {
			remaining--
		}
		if remaining <= 0 {
			return line[i+1:]
		}
	}
	return strings.TrimLeft(line, " \t")
}

func isRule(line string) bool {
	t := strings.ReplaceAll(strings.TrimSpace(line), " ", "")
	if len(t) < 3 {
		return false
	}
	for _, m := range []string{"-", "*", "_"} {
		if strings.Trim(t, m) == "" {
			return true
		}
	}
	return false
}

func parseHeading(line string) (int, string, bool) {
	trimmed := strings.TrimLeft(line, " \t")
	count := delimRun(trimmed, 0, '#')
	if count < 1 || count > 6 || count >= len(trimmed) || trimmed[count] != ' ' {
		return 0, "", false
	}
	heading := strings.TrimSpace(trimmed[count+1:])
	if hashStart := strings.LastIndex(heading, " #"); hashStart >= 0 {
		tail := strings.TrimSpace(heading[hashStart+1:])
		if tail != "" && strings.Trim(tail, "#") == "" {
			heading = strings.TrimSpace(heading[:hashStart])
		}
	}
	if heading == "" {
		return 0, "", false
	}
	return count, heading, true
}

// ---------------------------------------------------------------------------
// Inlines

func esc(s string) string { return html.EscapeString(s) }

// inlineLines renders text with each line break as <br>, keeping leading
// indentation as &nbsp;.
func inlineLines(text string) string {
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		lines[i] = lineHTML(line)
	}
	return strings.Join(lines, "<br>")
}

// lineHTML renders one line, its leading indentation as &nbsp; (tab = 4).
func lineHTML(line string) string {
	indent, index := lineIndentAndIndex(line)
	prefix := strings.Repeat("&nbsp;", indent)
	if index == len(line) {
		return prefix
	}
	return prefix + inline(line[index:])
}

func inline(text string) string { return inlineDepth(text, 0) }

func inlineDepth(text string, depth int) string {
	if depth > 8 {
		return esc(text)
	}
	sc := newSpanScan(text)
	var out strings.Builder
	for i := 0; i < len(text); {
		if text[i] == '\\' {
			if r, size := runeAt(text, i+1); r != utf8.RuneError && isEscapable(r) {
				out.WriteString(esc(string(r)))
				i += 1 + size
				continue
			}
		}
		if text[i] == '`' {
			run := delimRun(text, i, '`')
			if end := findBacktickRun(text, i+run, run); end >= 0 {
				out.WriteString("<code>" + esc(text[i+run:end]) + "</code>")
				i = end + run
				continue
			}
		}
		if strings.HasPrefix(text[i:], "~~") {
			if end := sc.findDelim(i+2, "~~"); end >= 0 {
				out.WriteString("<s>" + inlineDepth(text[i+2:end], depth+1) + "</s>")
				i = end + 2
				continue
			}
		}
		if strings.HasPrefix(text[i:], "**") {
			if end := sc.findDelim(i+2, "**"); end >= 0 {
				out.WriteString("<strong>" + inlineDepth(text[i+2:end], depth+1) + "</strong>")
				i = end + 2
				continue
			}
		}
		if strings.HasPrefix(text[i:], "__") && canOpenEmphasis(text, i, '_') {
			if end := sc.findDelim(i+2, "__"); end >= 0 {
				out.WriteString("<strong>" + inlineDepth(text[i+2:end], depth+1) + "</strong>")
				i = end + 2
				continue
			}
		}
		if strings.HasPrefix(text[i:], "![") {
			if rendered, next, ok := inlineImage(text, i, depth); ok {
				out.WriteString(rendered)
				i = next
				continue
			}
		}
		if text[i] == '[' {
			if rendered, next, ok := inlineLink(text, i, depth); ok {
				out.WriteString(rendered)
				i = next
				continue
			}
		}
		if text[i] == '<' {
			if rendered, next, ok := autolink(text, i); ok {
				out.WriteString(rendered)
				i = next
				continue
			}
		}
		if rendered, next, ok := bareURL(text, i); ok {
			out.WriteString(rendered)
			i = next
			continue
		}
		if (text[i] == '*' || text[i] == '_') && canOpenEmphasis(text, i, text[i]) {
			if i+1 < len(text) && text[i+1] == text[i] {
				out.WriteString(esc(text[i : i+2]))
				i += 2
				continue
			}
			if end := sc.findEmphasisClose(i+1, text[i]); end >= 0 {
				out.WriteString("<em>" + inlineDepth(text[i+1:end], depth+1) + "</em>")
				i = end + 1
				continue
			}
		}
		r, size := runeAt(text, i)
		out.WriteString(esc(string(r)))
		i += size
	}
	return out.String()
}

// inlineLink renders [label](dest). A local path becomes code; a safe URL
// becomes an anchor followed by the URL itself, unless the label already is
// the URL (or it is a compact Teams chat link); anything else is shown as
// "label (dest)" text.
func inlineLink(text string, start int, depth int) (string, int, bool) {
	labelEnd := findClosingBracket(text, start+1)
	if labelEnd < 0 || labelEnd+1 >= len(text) || text[labelEnd+1] != '(' {
		return "", start, false
	}
	destEnd := findClosingParen(text, labelEnd+2)
	if destEnd < 0 {
		return "", start, false
	}
	label := text[start+1 : labelEnd]
	dest := linkDest(strings.TrimSpace(text[labelEnd+2 : destEnd]))
	if dest == "" {
		return "", start, false
	}
	if isLocalPathLink(dest) {
		return "<code>" + esc(localLinkTarget(dest)) + "</code>", destEnd + 1, true
	}
	labelHTML := inlineDepth(label, depth+1)
	if href, ok := safeURL(dest); ok {
		rendered := `<a href="` + esc(href) + `">` + labelHTML + `</a>`
		if strings.TrimSpace(label) != dest && !isCompactChatLink(label, href) {
			rendered += " (" + esc(dest) + ")"
		}
		return rendered, destEnd + 1, true
	}
	return labelHTML + " (" + esc(dest) + ")", destEnd + 1, true
}

// inlineImage renders ![alt](dest) as text: "Image: " and a link.
func inlineImage(text string, start int, depth int) (string, int, bool) {
	labelEnd := findClosingBracket(text, start+2)
	if labelEnd < 0 || labelEnd+1 >= len(text) || text[labelEnd+1] != '(' {
		return "", start, false
	}
	destEnd := findClosingParen(text, labelEnd+2)
	if destEnd < 0 {
		return "", start, false
	}
	alt := strings.TrimSpace(text[start+2 : labelEnd])
	dest := linkDest(strings.TrimSpace(text[labelEnd+2 : destEnd]))
	if dest == "" {
		return "", start, false
	}
	labelHTML := inlineDepth(alt, depth+1)
	if alt == "" {
		labelHTML = esc(dest)
	}
	if isLocalPathLink(dest) {
		return "Image: <code>" + esc(localLinkTarget(dest)) + "</code>", destEnd + 1, true
	}
	if href, ok := safeURL(dest); ok && isHTTPURL(href) {
		rendered := `Image: <a href="` + esc(href) + `">` + labelHTML + `</a>`
		if alt != dest {
			rendered += " (" + esc(dest) + ")"
		}
		return rendered, destEnd + 1, true
	}
	return "Image: " + labelHTML + " (" + esc(dest) + ")", destEnd + 1, true
}

func autolink(text string, start int) (string, int, bool) {
	endRel := strings.IndexByte(text[start+1:], '>')
	if endRel < 0 {
		return "", start, false
	}
	end := start + 1 + endRel
	if href, ok := safeURL(text[start+1 : end]); ok {
		e := esc(href)
		return `<a href="` + e + `">` + e + `</a>`, end + 1, true
	}
	return "", start, false
}

func bareURL(text string, start int) (string, int, bool) {
	if !canStartBareURL(text, start) {
		return "", start, false
	}
	// Lower only the first runes: ToLower maps rune by rune, and lowering
	// the whole rest at every position made long lines quadratic.
	head := text[start:]
	for i, n := 0, 0; i < len(head); n++ {
		if n == len("https://") {
			head = head[:i]
			break
		}
		_, size := utf8.DecodeRuneInString(head[i:])
		i += size
	}
	lower := strings.ToLower(head)
	if !strings.HasPrefix(lower, "https://") && !strings.HasPrefix(lower, "http://") && !strings.HasPrefix(lower, "mailto:") {
		return "", start, false
	}
	end := start
	for end < len(text) {
		r, size := runeAt(text, end)
		if r == utf8.RuneError || unicode.IsSpace(r) || unicode.IsControl(r) || r == '<' || r == '>' || r == '"' {
			break
		}
		end += size
	}
	trimmed := strings.TrimRight(text[start:end], ".,;:!?")
	for strings.HasSuffix(trimmed, ")") && strings.Count(trimmed, ")") > strings.Count(trimmed, "(") {
		trimmed = strings.TrimSuffix(trimmed, ")")
	}
	for strings.HasSuffix(trimmed, "]") && strings.Count(trimmed, "]") > strings.Count(trimmed, "[") {
		trimmed = strings.TrimSuffix(trimmed, "]")
	}
	if trimmed == "" {
		return "", start, false
	}
	if href, ok := safeURL(trimmed); ok {
		e := esc(href)
		return `<a href="` + e + `">` + e + `</a>`, start + len(trimmed), true
	}
	return "", start, false
}

func canStartBareURL(text string, start int) bool {
	if start <= 0 {
		return true
	}
	if start >= 2 && text[start-1] == '(' && text[start-2] == ']' {
		return false
	}
	prev, _ := runeBefore(text, start)
	if prev == utf8.RuneError {
		return true
	}
	return !(unicode.IsLetter(prev) || unicode.IsDigit(prev) || prev == '_' || prev == '-')
}

// safeURL accepts http(s) URLs with a host and mailto: addresses.
func safeURL(dest string) (string, bool) {
	if strings.ContainsAny(dest, "<>\"") {
		return "", false
	}
	for _, r := range dest {
		if unicode.IsSpace(r) || unicode.IsControl(r) {
			return "", false
		}
	}
	parsed, err := url.Parse(dest)
	if err != nil {
		return "", false
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" && scheme != "mailto" {
		return "", false
	}
	if (scheme == "http" || scheme == "https") && parsed.Host == "" {
		return "", false
	}
	if scheme == "mailto" && parsed.Opaque == "" && parsed.Path == "" {
		return "", false
	}
	return dest, true
}

func isCompactChatLink(label string, href string) bool {
	if strings.TrimSpace(label) != compactChatLinkLabel {
		return false
	}
	parsed, err := url.Parse(href)
	if err != nil {
		return false
	}
	return strings.EqualFold(parsed.Scheme, "https") && strings.EqualFold(parsed.Hostname(), "teams.microsoft.com") &&
		strings.HasPrefix(parsed.Path, "/l/chat/")
}

func isHTTPURL(dest string) bool {
	parsed, err := url.Parse(dest)
	if err != nil {
		return false
	}
	scheme := strings.ToLower(parsed.Scheme)
	return scheme == "http" || scheme == "https"
}

// linkDest drops <> around a destination and a trailing title.
func linkDest(dest string) string {
	dest = strings.TrimSpace(dest)
	if strings.HasPrefix(dest, "<") {
		if end := strings.IndexByte(dest, '>'); end > 0 {
			return dest[1:end]
		}
	}
	fields := strings.Fields(dest)
	if len(fields) == 1 {
		return fields[0]
	}
	if len(fields) > 1 && (strings.HasPrefix(fields[1], `"`) || strings.HasPrefix(fields[1], `'`) || strings.HasPrefix(fields[1], "(")) {
		return fields[0]
	}
	return dest
}

func delimRun(text string, start int, delim byte) int {
	n := 0
	for start+n < len(text) && text[start+n] == delim {
		n++
	}
	return n
}

// findBacktickRun finds the next run of at least run backticks.
func findBacktickRun(text string, start int, run int) int {
	if start > len(text) {
		return -1
	}
	if i := strings.Index(text[start:], strings.Repeat("`", run)); i >= 0 {
		return start + i
	}
	return -1
}

// spanScan finds the closing delimiters of inline spans in one text. It
// knows where the last possible closer of each delimiter is, so a search
// that cannot succeed stops at once instead of scanning to the end: an
// unclosed "*" before every word made a line quadratic.
type spanScan struct {
	text string
	last map[string]int // delimiter -> last index it can close at, or -1
}

func newSpanScan(text string) *spanScan {
	sc := &spanScan{text: text, last: map[string]int{"*": -1, "_": -1}}
	for _, d := range []string{"~~", "**", "__"} {
		sc.last[d] = strings.LastIndex(text, d)
	}
	for _, d := range []byte{'*', '_'} {
		for i := strings.LastIndexByte(text, d); i >= 0; i = strings.LastIndexByte(text[:i], d) {
			if canCloseEmphasis(text, i, d) {
				sc.last[string(d)] = i
				break
			}
		}
	}
	return sc
}

func (sc *spanScan) findDelim(start int, delim string) int {
	text := sc.text
	if sc.last[delim] < start {
		return -1
	}
	for i := start; i < len(text); {
		if text[i] == '\\' {
			if _, size := runeAt(text, i+1); size > 0 {
				i += 1 + size
				continue
			}
			i++
			continue
		}
		if text[i] == '`' {
			run := delimRun(text, i, '`')
			if end := findBacktickRun(text, i+run, run); end >= 0 {
				i = end + run
				continue
			}
		}
		if strings.HasPrefix(text[i:], delim) {
			return i
		}
		if next, ok := sc.skipNestedSpan(i, delim); ok {
			i = next
			continue
		}
		if _, size := runeAt(text, i); size <= 0 {
			i++
		} else {
			i += size
		}
	}
	return -1
}

func (sc *spanScan) findEmphasisClose(start int, delim byte) int {
	text := sc.text
	if sc.last[string(delim)] < start {
		return -1
	}
	for i := start; i < len(text); {
		if text[i] == '\\' {
			if _, size := runeAt(text, i+1); size > 0 {
				i += 1 + size
				continue
			}
			i++
			continue
		}
		if text[i] == '`' {
			run := delimRun(text, i, '`')
			if end := findBacktickRun(text, i+run, run); end >= 0 {
				i = end + run
				continue
			}
		}
		if next, ok := sc.skipNestedSpan(i, string(delim)); ok {
			i = next
			continue
		}
		if text[i] != delim {
			if _, size := runeAt(text, i); size <= 0 {
				i++
			} else {
				i += size
			}
			continue
		}
		if i+1 < len(text) && text[i+1] == delim {
			i += 2
			continue
		}
		if canCloseEmphasis(text, i, delim) {
			return i
		}
		i++
	}
	return -1
}

func (sc *spanScan) skipNestedSpan(index int, target string) (int, bool) {
	text := sc.text
	for _, delim := range []string{"~~", "**", "__", "*", "_"} {
		if delim == target || !strings.HasPrefix(text[index:], delim) {
			continue
		}
		if delim == "__" && !canOpenEmphasis(text, index, '_') {
			continue
		}
		if delim == "*" || delim == "_" {
			if !canOpenEmphasis(text, index, delim[0]) {
				continue
			}
			if end := sc.findEmphasisClose(index+1, delim[0]); end >= 0 {
				return end + 1, true
			}
			continue
		}
		if end := sc.findDelim(index+len(delim), delim); end >= 0 {
			return end + len(delim), true
		}
	}
	return 0, false
}

func findClosingBracket(text string, start int) int {
	if start > len(text) || strings.IndexByte(text[start:], ']') < 0 {
		return -1 // fast path: the scan below could only fail
	}
	for i := start; i < len(text); i++ {
		if text[i] == '\\' {
			if _, size := runeAt(text, i+1); size > 0 {
				i += size
			}
			continue
		}
		if text[i] == ']' {
			return i
		}
	}
	return -1
}

func findClosingParen(text string, start int) int {
	if start > len(text) || strings.IndexByte(text[start:], ')') < 0 {
		return -1 // fast path: the scan below could only fail
	}
	depth := 0
	for i := start; i < len(text); i++ {
		if text[i] == '\\' {
			if _, size := runeAt(text, i+1); size > 0 {
				i += size
			}
			continue
		}
		switch text[i] {
		case '(':
			depth++
		case ')':
			if depth == 0 {
				return i
			}
			depth--
		}
	}
	return -1
}

func canOpenEmphasis(text string, index int, delim byte) bool {
	prev, _ := runeBefore(text, index)
	next, _ := runeAt(text, index+1)
	if next == utf8.RuneError || unicode.IsSpace(next) {
		return false
	}
	return !(delim == '_' && isAlnum(prev) && isAlnum(next))
}

func canCloseEmphasis(text string, index int, delim byte) bool {
	prev, _ := runeBefore(text, index)
	next, _ := runeAt(text, index+1)
	if prev == utf8.RuneError || unicode.IsSpace(prev) {
		return false
	}
	return !(delim == '_' && isAlnum(prev) && isAlnum(next))
}

func isEscapable(r rune) bool { return strings.ContainsRune(`\`+"`*{}_[]<>()#+-.!|~", r) }

func isLocalPathLink(dest string) bool {
	for _, p := range []string{"file://", "/", "~/", "./", "../", `\\`} {
		if strings.HasPrefix(dest, p) {
			return true
		}
	}
	return len(dest) >= 3 && ((dest[0] >= 'a' && dest[0] <= 'z') || (dest[0] >= 'A' && dest[0] <= 'Z')) &&
		dest[1] == ':' && (dest[2] == '/' || dest[2] == '\\')
}

// localLinkTarget shows a local link as a slash path.
func localLinkTarget(dest string) string {
	target := dest
	if strings.HasPrefix(dest, "file://") {
		if parsed, err := url.Parse(dest); err == nil {
			target = parsed.Path
			if host := parsed.Host; host != "" && host != "localhost" {
				target = "//" + host + target
			} else if len(target) >= 4 && target[0] == '/' && target[2] == ':' && (target[3] == '/' || target[3] == '\\') {
				target = target[1:]
			}
			if parsed.Fragment != "" {
				target += "#" + parsed.Fragment
			}
		}
	}
	if decoded, err := url.PathUnescape(target); err == nil {
		target = decoded
	}
	return strings.ReplaceAll(target, "\\", "/")
}

func runeAt(text string, index int) (rune, int) {
	if index >= len(text) {
		return utf8.RuneError, 0
	}
	return utf8.DecodeRuneInString(text[index:])
}

func runeBefore(text string, index int) (rune, int) {
	if index <= 0 {
		return utf8.RuneError, 0
	}
	return utf8.DecodeLastRuneInString(text[:index])
}

func isAlnum(r rune) bool {
	return r != utf8.RuneError && (unicode.IsLetter(r) || unicode.IsDigit(r))
}
