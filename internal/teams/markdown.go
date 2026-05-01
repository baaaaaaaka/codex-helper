package teams

import (
	"html"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"
)

type teamsMarkdownBlockKind int

const (
	teamsMarkdownParagraph teamsMarkdownBlockKind = iota
	teamsMarkdownHeading
	teamsMarkdownCodeBlock
	teamsMarkdownRule
	teamsMarkdownList
	teamsMarkdownTable
)

type teamsMarkdownBlock struct {
	kind    teamsMarkdownBlockKind
	level   int
	text    string
	ordered bool
	items   []teamsMarkdownListItem
	table   teamsMarkdownTableData
}

type teamsMarkdownListItem struct {
	text     string
	children []teamsMarkdownBlock
}

type teamsMarkdownListMarker struct {
	ordered bool
	indent  int
	content string
}

type teamsMarkdownTableData struct {
	header []string
	rows   [][]string
}

func renderTeamsHTMLCodexMarkdownAfterLabelBreak(label string, text string) string {
	blocks := parseTeamsMarkdownBlocks(text)
	labelHTML := "<strong>" + html.EscapeString(label) + ":</strong>"
	if len(blocks) == 0 {
		return "<p>" + labelHTML + "</p>"
	}

	var out strings.Builder
	labelWritten := false
	for _, block := range blocks {
		if !labelWritten && teamsMarkdownBlockCanShareLabel(block) {
			out.WriteString("<p>")
			out.WriteString(labelHTML)
			out.WriteString("<br>")
			out.WriteString(renderTeamsMarkdownBlockBodyHTML(block))
			out.WriteString("</p>")
			labelWritten = true
			continue
		}
		if !labelWritten {
			out.WriteString("<p>")
			out.WriteString(labelHTML)
			out.WriteString("</p>")
			labelWritten = true
		}
		out.WriteString(renderTeamsMarkdownBlockHTML(block))
	}
	return out.String()
}

func teamsMarkdownBlockCanShareLabel(block teamsMarkdownBlock) bool {
	return block.kind != teamsMarkdownCodeBlock && block.kind != teamsMarkdownList && block.kind != teamsMarkdownTable
}

func renderTeamsMarkdownBlockHTML(block teamsMarkdownBlock) string {
	if block.kind == teamsMarkdownCodeBlock {
		return "<pre><code>" + html.EscapeString(block.text) + "</code></pre>"
	}
	if block.kind == teamsMarkdownList {
		return renderTeamsMarkdownListHTML(block)
	}
	if block.kind == teamsMarkdownTable {
		return renderTeamsMarkdownTableHTML(block.table)
	}
	return "<p>" + renderTeamsMarkdownBlockBodyHTML(block) + "</p>"
}

func renderTeamsMarkdownBlockBodyHTML(block teamsMarkdownBlock) string {
	switch block.kind {
	case teamsMarkdownHeading:
		body := renderTeamsInlineMarkdown(block.text)
		switch {
		case block.level <= 2:
			return "<strong>" + body + "</strong>"
		case block.level == 3:
			return "<strong><em>" + body + "</em></strong>"
		default:
			return "<em>" + body + "</em>"
		}
	case teamsMarkdownRule:
		return "———"
	default:
		return renderTeamsInlineMarkdownWithLineBreaks(block.text)
	}
}

func renderTeamsMarkdownListHTML(block teamsMarkdownBlock) string {
	tag := "ul"
	if block.ordered {
		tag = "ol"
	}
	var out strings.Builder
	out.WriteString("<")
	out.WriteString(tag)
	out.WriteString(">")
	for _, item := range block.items {
		out.WriteString("<li>")
		wroteAny := false
		if strings.TrimSpace(item.text) != "" {
			out.WriteString(renderTeamsInlineMarkdownWithLineBreaks(item.text))
			wroteAny = true
		}
		for _, child := range item.children {
			if wroteAny {
				out.WriteString("<br>")
			}
			out.WriteString(renderTeamsMarkdownListChildHTML(child))
			wroteAny = true
		}
		out.WriteString("</li>")
	}
	out.WriteString("</")
	out.WriteString(tag)
	out.WriteString(">")
	return out.String()
}

func renderTeamsMarkdownListChildHTML(block teamsMarkdownBlock) string {
	if block.kind == teamsMarkdownParagraph || block.kind == teamsMarkdownHeading || block.kind == teamsMarkdownRule {
		return renderTeamsMarkdownBlockBodyHTML(block)
	}
	return renderTeamsMarkdownBlockHTML(block)
}

func renderTeamsMarkdownTableHTML(table teamsMarkdownTableData) string {
	var out strings.Builder
	out.WriteString("<table><tr>")
	for _, cell := range table.header {
		out.WriteString("<th>")
		out.WriteString(renderTeamsMarkdownTableCellHTML(cell))
		out.WriteString("</th>")
	}
	out.WriteString("</tr>")
	for _, row := range table.rows {
		out.WriteString("<tr>")
		for _, cell := range row {
			out.WriteString("<td>")
			out.WriteString(renderTeamsMarkdownTableCellHTML(cell))
			out.WriteString("</td>")
		}
		out.WriteString("</tr>")
	}
	out.WriteString("</table>")
	return out.String()
}

func renderTeamsMarkdownTableCellHTML(cell string) string {
	return renderTeamsInlineMarkdownWithLineBreaks(strings.TrimSpace(cell))
}

func parseTeamsMarkdownBlocks(text string) []teamsMarkdownBlock {
	text = normalizeTeamsRenderText(text)
	lines := strings.Split(text, "\n")
	blocks := make([]teamsMarkdownBlock, 0, 1)
	paragraph := make([]string, 0, len(lines))
	flushParagraph := func() {
		body := joinTeamsMarkdownLinesTrimBlankEdges(paragraph)
		if body != "" {
			blocks = append(blocks, teamsMarkdownBlock{kind: teamsMarkdownParagraph, text: body})
		}
		paragraph = paragraph[:0]
	}

	for i := 0; i < len(lines); i++ {
		line := lines[i]
		if fence, fenceLen, ok := teamsMarkdownFenceStart(line); ok {
			flushParagraph()
			var code []string
			i++
			for ; i < len(lines); i++ {
				if teamsMarkdownFenceEnd(lines[i], fence, fenceLen) {
					break
				}
				code = append(code, lines[i])
			}
			blocks = append(blocks, teamsMarkdownBlock{kind: teamsMarkdownCodeBlock, text: strings.Join(code, "\n")})
			continue
		}
		if teamsMarkdownIsBlank(line) {
			flushParagraph()
			continue
		}
		if block, next, ok := parseTeamsMarkdownTableBlock(lines, i); ok {
			flushParagraph()
			blocks = append(blocks, block)
			i = next - 1
			continue
		}
		if teamsMarkdownIsRule(line) {
			flushParagraph()
			blocks = append(blocks, teamsMarkdownBlock{kind: teamsMarkdownRule})
			continue
		}
		if marker, ok := parseTeamsMarkdownListMarker(line); ok && marker.indent <= 3 {
			flushParagraph()
			block, next := parseTeamsMarkdownListBlock(lines, i, marker)
			if len(block.items) > 0 {
				blocks = append(blocks, block)
				i = next - 1
				continue
			}
		}
		if len(paragraph) == 0 {
			if block, next, ok := parseTeamsMarkdownLooseIndentedCodeBlock(lines, i); ok {
				flushParagraph()
				blocks = append(blocks, block)
				i = next - 1
				continue
			}
		}
		if len(paragraph) == 0 && teamsMarkdownIsIndentedCodeLine(line) && !teamsMarkdownIndentedLineLooksLikeListMarker(line) {
			flushParagraph()
			var code []string
			for ; i < len(lines); i++ {
				if teamsMarkdownIsIndentedCodeLine(lines[i]) {
					code = append(code, teamsMarkdownStripCodeIndent(lines[i]))
					continue
				}
				if teamsMarkdownIsBlank(lines[i]) && i+1 < len(lines) && teamsMarkdownIsIndentedCodeLine(lines[i+1]) {
					code = append(code, "")
					continue
				}
				if !teamsMarkdownIsIndentedCodeLine(lines[i]) {
					i--
					break
				}
			}
			blocks = append(blocks, teamsMarkdownBlock{kind: teamsMarkdownCodeBlock, text: strings.Join(code, "\n")})
			continue
		}
		if level, heading, ok := parseTeamsMarkdownHeading(line); ok {
			flushParagraph()
			blocks = append(blocks, teamsMarkdownBlock{kind: teamsMarkdownHeading, level: level, text: heading})
			continue
		}
		paragraph = append(paragraph, line)
	}
	flushParagraph()
	return blocks
}

func parseTeamsMarkdownLooseIndentedCodeBlock(lines []string, start int) (teamsMarkdownBlock, int, bool) {
	baseIndent, index := teamsMarkdownLineIndentAndIndex(lines[start])
	if baseIndent <= 0 || baseIndent >= 4 {
		return teamsMarkdownBlock{}, start, false
	}
	first := strings.TrimSpace(lines[start][index:])
	if !teamsMarkdownLooseIndentedLineLooksLikeCodeStart(first) {
		return teamsMarkdownBlock{}, start, false
	}

	code := []string{strings.TrimSpace(lines[start][index:])}
	i := start + 1
	for ; i < len(lines); i++ {
		line := lines[i]
		if teamsMarkdownIsBlank(line) {
			if i+1 < len(lines) && teamsMarkdownLooseIndentedCodeCanContinueAfterBlank(lines[i+1], baseIndent) {
				code = append(code, "")
				continue
			}
			break
		}
		indent := teamsMarkdownLineIndent(line)
		if indent < baseIndent {
			break
		}
		stripped := teamsMarkdownStripIndentWidth(line, baseIndent)
		trimmed := strings.TrimSpace(stripped)
		if indent == baseIndent && len(code) > 0 && !teamsMarkdownLooseIndentedLineLooksLikeCodeLine(trimmed) {
			break
		}
		code = append(code, stripped)
	}
	if len(code) == 1 {
		return teamsMarkdownBlock{}, start, false
	}
	return teamsMarkdownBlock{kind: teamsMarkdownCodeBlock, text: strings.Join(code, "\n")}, i, true
}

func teamsMarkdownLooseIndentedCodeCanContinueAfterBlank(line string, baseIndent int) bool {
	if teamsMarkdownIsBlank(line) || teamsMarkdownLineIndent(line) < baseIndent {
		return false
	}
	stripped := strings.TrimSpace(teamsMarkdownStripIndentWidth(line, baseIndent))
	return teamsMarkdownLineIndent(line) > baseIndent || teamsMarkdownLooseIndentedLineLooksLikeCodeLine(stripped)
}

func teamsMarkdownLooseIndentedLineLooksLikeCodeLine(text string) bool {
	if teamsMarkdownLooseIndentedLineLooksLikeCodeStart(text) {
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
	return teamsMarkdownLineLooksLikeAssignment(trimmed) || teamsMarkdownLineLooksLikeCall(trimmed)
}

func teamsMarkdownLooseIndentedLineLooksLikeCodeStart(text string) bool {
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
	return teamsMarkdownLineLooksLikeAssignment(trimmed) || teamsMarkdownLineLooksLikeCall(trimmed)
}

func teamsMarkdownLineLooksLikeAssignment(text string) bool {
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
		left := strings.TrimSpace(text[:pos])
		return teamsMarkdownLooksLikeCodeIdentifier(left)
	}
	return false
}

func teamsMarkdownLineLooksLikeCall(text string) bool {
	open := strings.IndexByte(text, '(')
	if open <= 0 {
		return false
	}
	fn := strings.TrimSpace(text[:open])
	return teamsMarkdownLooksLikeCodeIdentifier(fn)
}

func teamsMarkdownLooksLikeCodeIdentifier(text string) bool {
	if text == "" {
		return false
	}
	for _, part := range strings.FieldsFunc(text, func(r rune) bool { return r == '.' || r == '[' || r == ']' }) {
		if part == "" {
			continue
		}
		runes := []rune(part)
		for i, r := range runes {
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

func parseTeamsMarkdownListBlock(lines []string, start int, first teamsMarkdownListMarker) (teamsMarkdownBlock, int) {
	block := teamsMarkdownBlock{kind: teamsMarkdownList, ordered: first.ordered}
	baseIndent := first.indent
	var current []string
	var currentChildren []teamsMarkdownBlock
	haveItem := false
	flushCurrentSegment := func() {
		body := joinTeamsMarkdownLinesTrimBlankEdges(current)
		if body != "" {
			currentChildren = append(currentChildren, teamsMarkdownBlock{kind: teamsMarkdownParagraph, text: body})
		}
		current = current[:0]
	}
	flushItem := func() {
		if !haveItem {
			return
		}
		text := joinTeamsMarkdownLinesTrimBlankEdges(current)
		if len(currentChildren) > 0 {
			flushCurrentSegment()
			text = ""
		}
		children := append([]teamsMarkdownBlock(nil), currentChildren...)
		block.items = append(block.items, teamsMarkdownListItem{
			text:     text,
			children: children,
		})
		current = current[:0]
		currentChildren = currentChildren[:0]
		haveItem = false
	}

	for i := start; i < len(lines); i++ {
		line := lines[i]
		if teamsMarkdownIsBlank(line) {
			if !haveItem {
				continue
			}
			if i+1 < len(lines) {
				if nextMarker, ok := parseTeamsMarkdownListMarker(lines[i+1]); ok {
					if nextMarker.indent <= baseIndent {
						continue
					}
					current = append(current, "")
					continue
				}
				if teamsMarkdownLineIndent(lines[i+1]) > baseIndent {
					current = append(current, "")
					continue
				}
			}
			flushItem()
			return block, i + 1
		}

		if marker, ok := parseTeamsMarkdownListMarker(line); ok {
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
				child, next := parseTeamsMarkdownListBlock(lines, i, marker)
				if len(child.items) > 0 {
					flushCurrentSegment()
					currentChildren = append(currentChildren, child)
					i = next - 1
					continue
				}
				current = append(current, teamsMarkdownStripListContinuation(line, baseIndent))
				continue
			}
		}

		if !haveItem {
			return block, i
		}
		if child, next, ok := parseTeamsMarkdownIndentedFenceBlock(lines, i, baseIndent); ok {
			flushCurrentSegment()
			currentChildren = append(currentChildren, child)
			i = next - 1
			continue
		}
		if child, next, ok := parseTeamsMarkdownLooseIndentedCodeBlock(lines, i); ok && teamsMarkdownLineIndent(line) > baseIndent {
			flushCurrentSegment()
			currentChildren = append(currentChildren, child)
			i = next - 1
			continue
		}
		if teamsMarkdownListContinuationCloses(line, baseIndent) {
			flushItem()
			return block, i
		}
		current = append(current, teamsMarkdownStripListContinuation(line, baseIndent))
	}
	flushItem()
	return block, len(lines)
}

func teamsMarkdownListContinuationCloses(line string, baseIndent int) bool {
	indent := teamsMarkdownLineIndent(line)
	if indent < baseIndent {
		return true
	}
	return baseIndent <= 0 && indent <= baseIndent
}

func parseTeamsMarkdownIndentedFenceBlock(lines []string, start int, parentIndent int) (teamsMarkdownBlock, int, bool) {
	indent := teamsMarkdownLineIndent(lines[start])
	if indent <= parentIndent {
		return teamsMarkdownBlock{}, start, false
	}
	fence, fenceLen, ok := teamsMarkdownFenceStart(lines[start])
	if !ok {
		return teamsMarkdownBlock{}, start, false
	}
	code := make([]string, 0, 1)
	i := start + 1
	for ; i < len(lines); i++ {
		if teamsMarkdownFenceEnd(lines[i], fence, fenceLen) {
			return teamsMarkdownBlock{kind: teamsMarkdownCodeBlock, text: strings.Join(code, "\n")}, i + 1, true
		}
		code = append(code, teamsMarkdownStripIndentWidth(lines[i], indent))
	}
	return teamsMarkdownBlock{kind: teamsMarkdownCodeBlock, text: strings.Join(code, "\n")}, i, true
}

func parseTeamsMarkdownTableBlock(lines []string, start int) (teamsMarkdownBlock, int, bool) {
	if start+1 >= len(lines) || !teamsMarkdownLineHasTablePipe(lines[start]) {
		return teamsMarkdownBlock{}, start, false
	}
	header := splitTeamsMarkdownTableRow(lines[start])
	delimiter := splitTeamsMarkdownTableRow(lines[start+1])
	if len(header) < 2 || len(delimiter) != len(header) || !teamsMarkdownIsTableDelimiter(delimiter) {
		return teamsMarkdownBlock{}, start, false
	}
	table := teamsMarkdownTableData{header: normalizeTeamsMarkdownTableRow(header, len(header))}
	next := start + 2
	for ; next < len(lines); next++ {
		line := lines[next]
		if teamsMarkdownIsBlank(line) || !teamsMarkdownLineHasTablePipe(line) {
			break
		}
		if _, _, ok := teamsMarkdownFenceStart(line); ok {
			break
		}
		row := splitTeamsMarkdownTableRow(line)
		if len(row) < 2 {
			break
		}
		table.rows = append(table.rows, normalizeTeamsMarkdownTableRow(row, len(table.header)))
	}
	return teamsMarkdownBlock{kind: teamsMarkdownTable, table: table}, next, true
}

func normalizeTeamsMarkdownTableRow(row []string, columns int) []string {
	normalized := make([]string, columns)
	for i := 0; i < columns; i++ {
		if i == columns-1 && len(row) > columns {
			normalized[i] = strings.TrimSpace(strings.Join(row[i:], " | "))
		} else if i < len(row) {
			normalized[i] = strings.TrimSpace(row[i])
		}
	}
	return normalized
}

func teamsMarkdownIsTableDelimiter(cells []string) bool {
	for _, cell := range cells {
		trimmed := strings.TrimSpace(cell)
		if strings.HasPrefix(trimmed, ":") {
			trimmed = strings.TrimSpace(trimmed[1:])
		}
		if strings.HasSuffix(trimmed, ":") {
			trimmed = strings.TrimSpace(trimmed[:len(trimmed)-1])
		}
		trimmed = strings.ReplaceAll(trimmed, " ", "")
		if len(trimmed) < 1 || strings.Trim(trimmed, "-") != "" {
			return false
		}
	}
	return true
}

func splitTeamsMarkdownTableRow(line string) []string {
	trimmed := strings.TrimSpace(line)
	if trimmed == "" {
		return nil
	}
	pipes := teamsMarkdownTablePipePositions(trimmed)
	if len(pipes) == 0 {
		return []string{strings.TrimSpace(trimmed)}
	}
	start := 0
	end := len(trimmed)
	filtered := pipes[:0]
	for _, pipe := range pipes {
		switch {
		case pipe == 0:
			start = 1
		case pipe == len(trimmed)-1:
			end = len(trimmed) - 1
		default:
			filtered = append(filtered, pipe)
		}
	}
	var cells []string
	last := start
	for _, pipe := range filtered {
		if pipe < start || pipe > end {
			continue
		}
		cells = append(cells, strings.TrimSpace(trimmed[last:pipe]))
		last = pipe + 1
	}
	cells = append(cells, strings.TrimSpace(trimmed[last:end]))
	return cells
}

func teamsMarkdownLineHasTablePipe(line string) bool {
	return len(teamsMarkdownTablePipePositions(strings.TrimSpace(line))) > 0
}

func teamsMarkdownTablePipePositions(text string) []int {
	var positions []int
	codeRun := 0
	for i := 0; i < len(text); {
		if text[i] == '\\' {
			if _, size := nextRune(text, i+1); size > 0 {
				i += 1 + size
				continue
			}
			i++
			continue
		}
		if text[i] == '`' {
			run := teamsMarkdownDelimiterRun(text, i, '`')
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
		_, size := nextRune(text, i)
		if size <= 0 {
			i++
		} else {
			i += size
		}
	}
	return positions
}

func compactTeamsRenderBlankLinesOutsideMarkdownFences(text string) string {
	text = normalizeTeamsRenderText(text)
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	blankSeen := false
	inFence := false
	var fence byte
	var fenceLen int
	for _, line := range lines {
		if inFence {
			out = append(out, line)
			if teamsMarkdownFenceEnd(line, fence, fenceLen) {
				inFence = false
				blankSeen = false
			}
			continue
		}
		if nextFence, nextFenceLen, ok := teamsMarkdownFenceStart(line); ok {
			out = append(out, line)
			inFence = true
			fence = nextFence
			fenceLen = nextFenceLen
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
	return joinTeamsMarkdownLinesTrimBlankEdges(out)
}

func joinTeamsMarkdownLinesTrimBlankEdges(lines []string) string {
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

func renderTeamsInlineMarkdownWithLineBreaks(text string) string {
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		lines[i] = renderTeamsInlineMarkdownPreservingLeadingIndent(line)
	}
	return strings.Join(lines, "<br>")
}

func renderTeamsInlineMarkdownPreservingLeadingIndent(line string) string {
	indentWidth := 0
	index := 0
	for index < len(line) {
		switch line[index] {
		case ' ':
			indentWidth++
			index++
		case '\t':
			indentWidth += 4
			index++
		default:
			prefix := strings.Repeat("&nbsp;", indentWidth)
			return prefix + renderTeamsInlineMarkdown(line[index:])
		}
	}
	return strings.Repeat("&nbsp;", indentWidth)
}

func renderTeamsInlineMarkdown(text string) string {
	return renderTeamsInlineMarkdownDepth(text, 0)
}

func renderTeamsInlineMarkdownDepth(text string, depth int) string {
	if depth > 8 {
		return html.EscapeString(text)
	}
	var out strings.Builder
	for i := 0; i < len(text); {
		if text[i] == '\\' {
			if r, size := nextRune(text, i+1); r != utf8.RuneError && teamsMarkdownIsEscapable(r) {
				out.WriteString(html.EscapeString(string(r)))
				i += 1 + size
				continue
			}
		}
		if text[i] == '`' {
			run := teamsMarkdownDelimiterRun(text, i, '`')
			if end := teamsMarkdownFindBacktickRun(text, i+run, run); end >= 0 {
				out.WriteString("<code>")
				out.WriteString(html.EscapeString(text[i+run : end]))
				out.WriteString("</code>")
				i = end + run
				continue
			}
		}
		if strings.HasPrefix(text[i:], "~~") {
			if end := teamsMarkdownFindDelimiter(text, i+2, "~~"); end >= 0 {
				out.WriteString("<s>")
				out.WriteString(renderTeamsInlineMarkdownDepth(text[i+2:end], depth+1))
				out.WriteString("</s>")
				i = end + 2
				continue
			}
		}
		if strings.HasPrefix(text[i:], "**") {
			if end := teamsMarkdownFindDelimiter(text, i+2, "**"); end >= 0 {
				out.WriteString("<strong>")
				out.WriteString(renderTeamsInlineMarkdownDepth(text[i+2:end], depth+1))
				out.WriteString("</strong>")
				i = end + 2
				continue
			}
		}
		if strings.HasPrefix(text[i:], "__") && teamsMarkdownCanOpenEmphasis(text, i, '_') {
			if end := teamsMarkdownFindDelimiter(text, i+2, "__"); end >= 0 {
				out.WriteString("<strong>")
				out.WriteString(renderTeamsInlineMarkdownDepth(text[i+2:end], depth+1))
				out.WriteString("</strong>")
				i = end + 2
				continue
			}
		}
		if strings.HasPrefix(text[i:], "![") {
			if rendered, next, ok := renderTeamsMarkdownInlineImage(text, i, depth); ok {
				out.WriteString(rendered)
				i = next
				continue
			}
		}
		if text[i] == '[' {
			if rendered, next, ok := renderTeamsMarkdownInlineLink(text, i, depth); ok {
				out.WriteString(rendered)
				i = next
				continue
			}
		}
		if text[i] == '<' {
			if rendered, next, ok := renderTeamsMarkdownAutolink(text, i); ok {
				out.WriteString(rendered)
				i = next
				continue
			}
		}
		if rendered, next, ok := renderTeamsMarkdownBareURL(text, i); ok {
			out.WriteString(rendered)
			i = next
			continue
		}
		if (text[i] == '*' || text[i] == '_') && teamsMarkdownCanOpenEmphasis(text, i, text[i]) {
			if i+1 < len(text) && text[i+1] == text[i] {
				out.WriteString(html.EscapeString(text[i : i+2]))
				i += 2
				continue
			}
			delimiter := string(text[i])
			if end := teamsMarkdownFindEmphasisClose(text, i+1, text[i]); end >= 0 {
				out.WriteString("<em>")
				out.WriteString(renderTeamsInlineMarkdownDepth(text[i+1:end], depth+1))
				out.WriteString("</em>")
				i = end + len(delimiter)
				continue
			}
		}
		r, size := nextRune(text, i)
		out.WriteString(html.EscapeString(string(r)))
		i += size
	}
	return out.String()
}

func renderTeamsMarkdownInlineLink(text string, start int, depth int) (string, int, bool) {
	labelEnd := teamsMarkdownFindClosingBracket(text, start+1)
	if labelEnd < 0 || labelEnd+1 >= len(text) || text[labelEnd+1] != '(' {
		return "", start, false
	}
	destEnd := teamsMarkdownFindClosingParen(text, labelEnd+2)
	if destEnd < 0 {
		return "", start, false
	}
	label := text[start+1 : labelEnd]
	dest := strings.TrimSpace(text[labelEnd+2 : destEnd])
	dest = teamsMarkdownDestinationURL(dest)
	if dest == "" {
		return "", start, false
	}
	if teamsMarkdownIsLocalPathLikeLink(dest) {
		return "<code>" + html.EscapeString(renderTeamsMarkdownLocalLinkTarget(dest)) + "</code>", destEnd + 1, true
	}
	labelHTML := renderTeamsInlineMarkdownDepth(label, depth+1)
	if href, ok := safeTeamsMarkdownURL(dest); ok {
		rendered := `<a href="` + html.EscapeString(href) + `">` + labelHTML + `</a>`
		if strings.TrimSpace(label) != dest {
			rendered += " (" + html.EscapeString(dest) + ")"
		}
		return rendered, destEnd + 1, true
	}
	return labelHTML + " (" + html.EscapeString(dest) + ")", destEnd + 1, true
}

func renderTeamsMarkdownInlineImage(text string, start int, depth int) (string, int, bool) {
	labelEnd := teamsMarkdownFindClosingBracket(text, start+2)
	if labelEnd < 0 || labelEnd+1 >= len(text) || text[labelEnd+1] != '(' {
		return "", start, false
	}
	destEnd := teamsMarkdownFindClosingParen(text, labelEnd+2)
	if destEnd < 0 {
		return "", start, false
	}
	alt := strings.TrimSpace(text[start+2 : labelEnd])
	dest := strings.TrimSpace(text[labelEnd+2 : destEnd])
	dest = teamsMarkdownDestinationURL(dest)
	if dest == "" {
		return "", start, false
	}
	labelHTML := renderTeamsInlineMarkdownDepth(alt, depth+1)
	if strings.TrimSpace(alt) == "" {
		labelHTML = html.EscapeString(dest)
	}
	if teamsMarkdownIsLocalPathLikeLink(dest) {
		return "Image: <code>" + html.EscapeString(renderTeamsMarkdownLocalLinkTarget(dest)) + "</code>", destEnd + 1, true
	}
	if href, ok := safeTeamsMarkdownURL(dest); ok && teamsMarkdownURLIsHTTP(href) {
		rendered := `Image: <a href="` + html.EscapeString(href) + `">` + labelHTML + `</a>`
		if strings.TrimSpace(alt) != dest {
			rendered += " (" + html.EscapeString(dest) + ")"
		}
		return rendered, destEnd + 1, true
	}
	return "Image: " + labelHTML + " (" + html.EscapeString(dest) + ")", destEnd + 1, true
}

func renderTeamsMarkdownAutolink(text string, start int) (string, int, bool) {
	endRel := strings.IndexByte(text[start+1:], '>')
	if endRel < 0 {
		return "", start, false
	}
	end := start + 1 + endRel
	dest := text[start+1 : end]
	if href, ok := safeTeamsMarkdownURL(dest); ok {
		escaped := html.EscapeString(href)
		return `<a href="` + escaped + `">` + escaped + `</a>`, end + 1, true
	}
	return "", start, false
}

func renderTeamsMarkdownBareURL(text string, start int) (string, int, bool) {
	if !teamsMarkdownCanStartBareURL(text, start) {
		return "", start, false
	}
	matched := false
	lower := strings.ToLower(text[start:])
	for _, prefix := range []string{"https://", "http://", "mailto:"} {
		if strings.HasPrefix(lower, prefix) {
			matched = true
			break
		}
	}
	if !matched {
		return "", start, false
	}
	end := start
	for end < len(text) {
		r, size := nextRune(text, end)
		if r == utf8.RuneError || unicode.IsSpace(r) || unicode.IsControl(r) || r == '<' || r == '>' || r == '"' {
			break
		}
		end += size
	}
	dest := text[start:end]
	trimmed := strings.TrimRight(dest, ".,;:!?")
	for strings.HasSuffix(trimmed, ")") && strings.Count(trimmed, ")") > strings.Count(trimmed, "(") {
		trimmed = strings.TrimSuffix(trimmed, ")")
	}
	for strings.HasSuffix(trimmed, "]") && strings.Count(trimmed, "]") > strings.Count(trimmed, "[") {
		trimmed = strings.TrimSuffix(trimmed, "]")
	}
	if trimmed == "" {
		return "", start, false
	}
	if href, ok := safeTeamsMarkdownURL(trimmed); ok {
		escaped := html.EscapeString(href)
		return `<a href="` + escaped + `">` + escaped + `</a>`, start + len(trimmed), true
	}
	return "", start, false
}

func teamsMarkdownCanStartBareURL(text string, start int) bool {
	if start <= 0 {
		return true
	}
	if start >= 2 && text[start-1] == '(' && text[start-2] == ']' {
		return false
	}
	prev, _ := prevRune(text, start)
	if prev == utf8.RuneError {
		return true
	}
	if unicode.IsLetter(prev) || unicode.IsDigit(prev) || prev == '_' || prev == '-' {
		return false
	}
	return true
}

func safeTeamsMarkdownURL(dest string) (string, bool) {
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

func teamsMarkdownURLIsHTTP(dest string) bool {
	parsed, err := url.Parse(dest)
	if err != nil {
		return false
	}
	scheme := strings.ToLower(parsed.Scheme)
	return scheme == "http" || scheme == "https"
}

func teamsMarkdownDestinationURL(dest string) string {
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

func teamsMarkdownFenceStart(line string) (byte, int, bool) {
	trimmed := strings.TrimLeft(line, " \t")
	if trimmed == "" || (trimmed[0] != '`' && trimmed[0] != '~') {
		return 0, 0, false
	}
	fence := trimmed[0]
	count := teamsMarkdownDelimiterRun(trimmed, 0, fence)
	if count < 3 {
		return 0, 0, false
	}
	return fence, count, true
}

func teamsMarkdownFenceEnd(line string, fence byte, fenceLen int) bool {
	trimmed := strings.TrimLeft(line, " \t")
	if len(trimmed) < fenceLen || trimmed[0] != fence {
		return false
	}
	count := teamsMarkdownDelimiterRun(trimmed, 0, fence)
	return count >= fenceLen && strings.TrimSpace(trimmed[count:]) == ""
}

func teamsMarkdownIsBlank(line string) bool {
	return strings.TrimSpace(line) == ""
}

func teamsMarkdownIsIndentedCodeLine(line string) bool {
	return strings.HasPrefix(line, "    ") || strings.HasPrefix(line, "\t")
}

func teamsMarkdownStripCodeIndent(line string) string {
	if strings.HasPrefix(line, "\t") {
		return line[1:]
	}
	if strings.HasPrefix(line, "    ") {
		return line[4:]
	}
	return line
}

func teamsMarkdownIndentedLineLooksLikeListMarker(line string) bool {
	stripped := strings.TrimLeft(line, " \t")
	if len(stripped) >= 2 && (strings.HasPrefix(stripped, "- ") || strings.HasPrefix(stripped, "* ") || strings.HasPrefix(stripped, "+ ")) {
		return true
	}
	index := 0
	for index < len(stripped) && stripped[index] >= '0' && stripped[index] <= '9' {
		index++
	}
	return index > 0 && index+1 < len(stripped) && (stripped[index] == '.' || stripped[index] == ')') && stripped[index+1] == ' '
}

func parseTeamsMarkdownListMarker(line string) (teamsMarkdownListMarker, bool) {
	indent, index := teamsMarkdownLineIndentAndIndex(line)
	trimmed := line[index:]
	if len(trimmed) >= 2 && (strings.HasPrefix(trimmed, "- ") || strings.HasPrefix(trimmed, "* ") || strings.HasPrefix(trimmed, "+ ")) {
		return teamsMarkdownListMarker{indent: indent, content: normalizeTeamsMarkdownTaskListContent(strings.TrimSpace(trimmed[2:]))}, true
	}
	digits := 0
	for digits < len(trimmed) && trimmed[digits] >= '0' && trimmed[digits] <= '9' {
		digits++
	}
	if digits == 0 || digits > 9 || digits+1 >= len(trimmed) || (trimmed[digits] != '.' && trimmed[digits] != ')') || trimmed[digits+1] != ' ' {
		return teamsMarkdownListMarker{}, false
	}
	return teamsMarkdownListMarker{ordered: true, indent: indent, content: normalizeTeamsMarkdownTaskListContent(strings.TrimSpace(trimmed[digits+2:]))}, true
}

func normalizeTeamsMarkdownTaskListContent(content string) string {
	lower := strings.ToLower(content)
	if strings.HasPrefix(lower, "[ ] ") {
		return "☐ " + strings.TrimSpace(content[4:])
	}
	if strings.HasPrefix(lower, "[x] ") {
		return "☑ " + strings.TrimSpace(content[4:])
	}
	return content
}

func teamsMarkdownLineIndent(line string) int {
	indent, _ := teamsMarkdownLineIndentAndIndex(line)
	return indent
}

func teamsMarkdownLineIndentAndIndex(line string) (int, int) {
	indent := 0
	index := 0
	for index < len(line) {
		switch line[index] {
		case ' ':
			indent++
			index++
		case '\t':
			indent += 4
			index++
		default:
			return indent, index
		}
	}
	return indent, index
}

func teamsMarkdownStripListContinuation(line string, baseIndent int) string {
	return teamsMarkdownStripIndentWidth(line, baseIndent)
}

func teamsMarkdownStripIndentWidth(line string, baseIndent int) string {
	if baseIndent <= 0 {
		return line
	}
	remaining := baseIndent
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

func teamsMarkdownIsRule(line string) bool {
	trimmed := strings.ReplaceAll(strings.TrimSpace(line), " ", "")
	if len(trimmed) < 3 {
		return false
	}
	for _, marker := range []string{"-", "*", "_"} {
		if strings.Trim(trimmed, marker) == "" {
			return true
		}
	}
	return false
}

func parseTeamsMarkdownHeading(line string) (int, string, bool) {
	trimmed := strings.TrimLeft(line, " \t")
	count := teamsMarkdownDelimiterRun(trimmed, 0, '#')
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

func teamsMarkdownDelimiterRun(text string, start int, delimiter byte) int {
	count := 0
	for start+count < len(text) && text[start+count] == delimiter {
		count++
	}
	return count
}

func teamsMarkdownFindBacktickRun(text string, start int, run int) int {
	delimiter := strings.Repeat("`", run)
	for i := start; i < len(text); {
		next := strings.Index(text[i:], delimiter)
		if next < 0 {
			return -1
		}
		idx := i + next
		return idx
	}
	return -1
}

func teamsMarkdownFindDelimiter(text string, start int, delimiter string) int {
	for i := start; i < len(text); {
		if text[i] == '\\' {
			if _, size := nextRune(text, i+1); size > 0 {
				i += 1 + size
				continue
			}
			i++
			continue
		}
		if text[i] == '`' {
			run := teamsMarkdownDelimiterRun(text, i, '`')
			if end := teamsMarkdownFindBacktickRun(text, i+run, run); end >= 0 {
				i = end + run
				continue
			}
		}
		if strings.HasPrefix(text[i:], delimiter) {
			return i
		}
		if next, ok := teamsMarkdownSkipNestedSpan(text, i, delimiter); ok {
			i = next
			continue
		}
		_, size := nextRune(text, i)
		if size <= 0 {
			i++
		} else {
			i += size
		}
	}
	return -1
}

func teamsMarkdownFindEmphasisClose(text string, start int, delimiter byte) int {
	for i := start; i < len(text); {
		if text[i] == '\\' {
			if _, size := nextRune(text, i+1); size > 0 {
				i += 1 + size
				continue
			}
			i++
			continue
		}
		if text[i] == '`' {
			run := teamsMarkdownDelimiterRun(text, i, '`')
			if end := teamsMarkdownFindBacktickRun(text, i+run, run); end >= 0 {
				i = end + run
				continue
			}
		}
		if next, ok := teamsMarkdownSkipNestedSpan(text, i, string(delimiter)); ok {
			i = next
			continue
		}
		if text[i] != delimiter {
			_, size := nextRune(text, i)
			if size <= 0 {
				i++
			} else {
				i += size
			}
			continue
		}
		if i+1 < len(text) && text[i+1] == delimiter {
			i += 2
			continue
		}
		if teamsMarkdownCanCloseEmphasis(text, i, delimiter) {
			return i
		}
		i++
	}
	return -1
}

func teamsMarkdownSkipNestedSpan(text string, index int, targetDelimiter string) (int, bool) {
	for _, delimiter := range []string{"~~", "**", "__", "*", "_"} {
		if delimiter == targetDelimiter || !strings.HasPrefix(text[index:], delimiter) {
			continue
		}
		if delimiter == "__" && !teamsMarkdownCanOpenEmphasis(text, index, '_') {
			continue
		}
		if delimiter == "*" || delimiter == "_" {
			if !teamsMarkdownCanOpenEmphasis(text, index, delimiter[0]) {
				continue
			}
			end := teamsMarkdownFindEmphasisClose(text, index+1, delimiter[0])
			if end >= 0 {
				return end + 1, true
			}
			continue
		}
		end := teamsMarkdownFindDelimiter(text, index+len(delimiter), delimiter)
		if end >= 0 {
			return end + len(delimiter), true
		}
	}
	return 0, false
}

func teamsMarkdownFindClosingBracket(text string, start int) int {
	for i := start; i < len(text); i++ {
		if text[i] == '\\' {
			if _, size := nextRune(text, i+1); size > 0 {
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

func teamsMarkdownFindClosingParen(text string, start int) int {
	depth := 0
	for i := start; i < len(text); i++ {
		if text[i] == '\\' {
			if _, size := nextRune(text, i+1); size > 0 {
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

func teamsMarkdownCanOpenEmphasis(text string, index int, delimiter byte) bool {
	prev, _ := prevRune(text, index)
	next, _ := nextRune(text, index+1)
	if next == utf8.RuneError || unicode.IsSpace(next) {
		return false
	}
	if delimiter == '_' && isAlphaNumeric(prev) && isAlphaNumeric(next) {
		return false
	}
	return true
}

func teamsMarkdownCanCloseEmphasis(text string, index int, delimiter byte) bool {
	prev, _ := prevRune(text, index)
	next, _ := nextRune(text, index+1)
	if prev == utf8.RuneError || unicode.IsSpace(prev) {
		return false
	}
	if delimiter == '_' && isAlphaNumeric(prev) && isAlphaNumeric(next) {
		return false
	}
	return true
}

func teamsMarkdownIsEscapable(r rune) bool {
	return strings.ContainsRune(`\`+"`*{}_[]<>()#+-.!|~", r)
}

func teamsMarkdownIsLocalPathLikeLink(dest string) bool {
	if strings.HasPrefix(dest, "file://") ||
		strings.HasPrefix(dest, "/") ||
		strings.HasPrefix(dest, "~/") ||
		strings.HasPrefix(dest, "./") ||
		strings.HasPrefix(dest, "../") ||
		strings.HasPrefix(dest, "\\\\") {
		return true
	}
	if len(dest) >= 3 && ((dest[0] >= 'a' && dest[0] <= 'z') || (dest[0] >= 'A' && dest[0] <= 'Z')) && dest[1] == ':' && (dest[2] == '/' || dest[2] == '\\') {
		return true
	}
	return false
}

func renderTeamsMarkdownLocalLinkTarget(dest string) string {
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

func nextRune(text string, index int) (rune, int) {
	if index >= len(text) {
		return utf8.RuneError, 0
	}
	r, size := utf8.DecodeRuneInString(text[index:])
	return r, size
}

func prevRune(text string, index int) (rune, int) {
	if index <= 0 {
		return utf8.RuneError, 0
	}
	r, size := utf8.DecodeLastRuneInString(text[:index])
	return r, size
}

func isAlphaNumeric(r rune) bool {
	return r != utf8.RuneError && (unicode.IsLetter(r) || unicode.IsDigit(r))
}
