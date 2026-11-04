package teams

import (
	"html"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// A small Markdown reader for chat output: CommonMark-style blocks
// (paragraphs, ATX/setext headings, fenced and indented code, rules, block
// quotes, nested lists with task boxes) plus GFM tables, and inlines (code
// spans, emphasis, strikethrough, links, autolinks, bare URLs). Raw HTML in
// the source is always escaped. It is deliberately forgiving: anything it
// does not recognise is shown as text.

type blockKind int

const (
	bPara blockKind = iota
	bHeading
	bCode
	bRule
	bTable
	bQuote
	bList
	bMath
)

type mdBlock struct {
	kind    blockKind
	level   int      // heading level
	lines   []string // paragraph / code lines, heading text in lines[0]
	head    []string // table header cells
	rows    [][]string
	kids    []mdBlock // quote children
	ordered bool
	start   int
	items   []mdItem
	tex     string // display math source
	png     *MathResult
	math    bool // <m> spans are formulas (RichDoc.Math)
}

type mdItem struct {
	task int // 0 none, 1 open, 2 done
	kids []mdBlock
}

var (
	headingRE   = regexp.MustCompile(`^ {0,3}(#{1,6})(?:[ \t]+(.*?))?(?:[ \t]+#+)?[ \t]*$`)
	setext1RE   = regexp.MustCompile(`^ {0,3}=+[ \t]*$`)
	setext2RE   = regexp.MustCompile(`^ {0,3}-+[ \t]*$`)
	listRE      = regexp.MustCompile(`^( {0,3})([-*+]|\d{1,9}[.)])([ \t]+|$)`)
	quoteRE     = regexp.MustCompile(`^ {0,3}>`)
	tableDelim  = regexp.MustCompile(`^ {0,3}\|?[ \t]*:?-+:?[ \t]*(\|[ \t]*:?-+:?[ \t]*)*\|?[ \t]*$`)
	taskRE      = regexp.MustCompile(`^\[([ xX])\][ \t]+`)
	bareURLRE   = regexp.MustCompile(`^(?:https?://|www\.)[^\s<>]+`)
	autolinkRE  = regexp.MustCompile(`^<((?:https?://|mailto:)[^\s<>]+)>`)
	trailPunctR = ".,:;!?'\"*_~"
)

func parseMarkdown(src string, math bool) []mdBlock {
	src = strings.ReplaceAll(strings.ReplaceAll(src, "\r\n", "\n"), "\r", "\n")
	src = strings.ReplaceAll(src, "\x00", "")
	blocks := parseBlocks(strings.Split(src, "\n"), math)
	setMath(blocks, math)
	return blocks
}

func setMath(bs []mdBlock, m bool) {
	for i := range bs {
		bs[i].math = m
		setMath(bs[i].kids, m)
		for j := range bs[i].items {
			setMath(bs[i].items[j].kids, m)
		}
	}
}

func isBlank(s string) bool { return strings.TrimSpace(s) == "" }

// indentOf counts leading columns (tab = 4).
func indentOf(s string) int {
	n := 0
	for _, r := range s {
		switch r {
		case ' ':
			n++
		case '\t':
			n += 4 - n%4
		default:
			return n
		}
	}
	return n
}

// stripCols removes up to n leading columns of whitespace.
func stripCols(s string, n int) string {
	col := 0
	for i, r := range s {
		if col >= n {
			return s[i:]
		}
		switch r {
		case ' ':
			col++
		case '\t':
			w := 4 - col%4
			if col+w > n {
				return strings.Repeat(" ", col+w-n) + s[i+1:]
			}
			col += w
		default:
			return s[i:]
		}
	}
	return ""
}

func fenceOf(line string) (marker string, ok bool) {
	if indentOf(line) > 3 {
		return "", false
	}
	t := strings.TrimLeft(line, " ")
	for _, c := range []byte{'`', '~'} {
		n := 0
		for n < len(t) && t[n] == c {
			n++
		}
		if n >= 3 && !(c == '`' && strings.Contains(t[n:], "`")) {
			return t[:n], true
		}
	}
	return "", false
}

func isRule(line string) bool {
	if indentOf(line) > 3 {
		return false
	}
	t := strings.Join(strings.Fields(line), "")
	if len(t) < 3 {
		return false
	}
	c := t[0]
	return (c == '-' || c == '*' || c == '_') && strings.Count(t, string(c)) == len(t)
}

type listMark struct {
	ordered bool
	delim   byte // '-', '*', '+', '.', ')'
	start   int
	width   int // content column
	rest    string
}

func listMarker(line string) (listMark, bool) {
	m := listRE.FindStringSubmatch(line)
	if m == nil {
		return listMark{}, false
	}
	mk := listMark{delim: m[2][len(m[2])-1]}
	if mk.delim == '.' || mk.delim == ')' {
		mk.ordered = true
		mk.start, _ = strconv.Atoi(m[2][:len(m[2])-1])
	}
	rest := line[len(m[0]):]
	pad := indentOf(m[3])
	if rest == "" || isBlank(rest) {
		pad = 1
		rest = ""
	} else if pad > 4 {
		rest = strings.Repeat(" ", pad-1) + rest
		pad = 1
	}
	mk.width = len(m[1]) + len(m[2]) + pad
	mk.rest = rest
	return mk, true
}

func (a listMark) sameList(b listMark) bool { return a.ordered == b.ordered && a.delim == b.delim }

// mathBlockAt reports a display formula starting at lines[i]: a <m> on its
// own line(s) with nothing else around it. It returns the TeX and the
// number of lines used.
func mathBlockAt(lines []string, i int) (string, int, bool) {
	t := strings.TrimSpace(lines[i])
	if !strings.HasPrefix(t, mathOpen) || indentOf(lines[i]) > 3 {
		return "", 0, false
	}
	var b strings.Builder
	for j := i; j < len(lines) && j < i+200; j++ {
		s := strings.TrimSpace(lines[j])
		if j == i {
			s = s[len(mathOpen):]
		}
		if k := strings.Index(s, mathClose); k >= 0 {
			if strings.TrimSpace(s[k+len(mathClose):]) != "" {
				return "", 0, false
			}
			b.WriteString(s[:k])
			tex := strings.TrimSpace(b.String())
			if tex == "" || strings.Contains(tex, mathOpen) {
				return "", 0, false
			}
			return tex, j - i + 1, true
		}
		if j > i && isBlank(lines[j]) {
			return "", 0, false
		}
		b.WriteString(s)
		b.WriteByte('\n')
	}
	return "", 0, false
}

// startsBlock reports whether line interrupts a paragraph.
func startsBlock(lines []string, i int, math bool) bool {
	line := lines[i]
	if indentOf(line) > 3 {
		return false
	}
	if _, ok := fenceOf(line); ok {
		return true
	}
	if headingRE.MatchString(line) || isRule(line) || quoteRE.MatchString(line) {
		return true
	}
	if mk, ok := listMarker(line); ok && mk.rest != "" && (!mk.ordered || mk.start == 1) {
		return true
	}
	if math {
		if _, _, ok := mathBlockAt(lines, i); ok {
			return true
		}
	}
	return false
}

func parseBlocks(lines []string, math bool) []mdBlock {
	var out []mdBlock
	var para []string
	flush := func() {
		if len(para) > 0 {
			out = append(out, mdBlock{kind: bPara, lines: para})
			para = nil
		}
	}
	for i := 0; i < len(lines); {
		line := lines[i]
		switch {
		case isBlank(line):
			flush()
			i++
			continue
		case len(para) > 0 && setext1RE.MatchString(line):
			out = append(out, mdBlock{kind: bHeading, level: 1, lines: []string{strings.Join(trimAll(para), " ")}})
			para = nil
			i++
			continue
		case len(para) > 0 && setext2RE.MatchString(line):
			out = append(out, mdBlock{kind: bHeading, level: 2, lines: []string{strings.Join(trimAll(para), " ")}})
			para = nil
			i++
			continue
		case len(para) > 0:
			// A paragraph line followed by a delimiter row is a table header.
			if tb, n, ok := parseTable(para[len(para)-1], lines, i); ok {
				para = para[:len(para)-1]
				flush()
				out = append(out, tb)
				i += n
				continue
			}
			if !startsBlock(lines, i, math) {
				para = append(para, line)
				i++
				continue
			}
		}
		flush()
		if math {
			if tex, n, ok := mathBlockAt(lines, i); ok {
				out = append(out, mdBlock{kind: bMath, tex: tex})
				i += n
				continue
			}
		}
		if fence, ok := fenceOf(line); ok {
			ind := indentOf(line)
			var code []string
			j := i + 1
			for ; j < len(lines); j++ {
				t := strings.TrimSpace(lines[j])
				if indentOf(lines[j]) <= 3 && strings.HasPrefix(t, fence) && strings.Trim(t, fence[:1]) == "" {
					break
				}
				code = append(code, stripCols(lines[j], ind))
			}
			out = append(out, mdBlock{kind: bCode, lines: code})
			i = j + 1
			continue
		}
		if indentOf(line) >= 4 {
			var code []string
			j := i
			for ; j < len(lines) && (isBlank(lines[j]) || indentOf(lines[j]) >= 4); j++ {
				code = append(code, stripCols(lines[j], 4))
			}
			for len(code) > 0 && isBlank(code[len(code)-1]) {
				code = code[:len(code)-1]
			}
			out = append(out, mdBlock{kind: bCode, lines: code})
			i = j
			continue
		}
		if m := headingRE.FindStringSubmatch(line); m != nil {
			out = append(out, mdBlock{kind: bHeading, level: len(m[1]), lines: []string{m[2]}})
			i++
			continue
		}
		if isRule(line) {
			out = append(out, mdBlock{kind: bRule})
			i++
			continue
		}
		if quoteRE.MatchString(line) {
			var inner []string
			j := i
			for ; j < len(lines); j++ {
				l := lines[j]
				if quoteRE.MatchString(l) {
					l = strings.TrimLeft(l, " ")[1:]
					if strings.HasPrefix(l, " ") {
						l = l[1:]
					}
					inner = append(inner, l)
					continue
				}
				// Lazy continuation of a quoted paragraph.
				if !isBlank(l) && len(inner) > 0 && !isBlank(inner[len(inner)-1]) && !startsBlock(lines, j, math) {
					inner = append(inner, l)
					continue
				}
				break
			}
			out = append(out, mdBlock{kind: bQuote, kids: parseBlocks(inner, math)})
			i = j
			continue
		}
		if mk, ok := listMarker(line); ok {
			b, n := parseList(lines, i, mk, math)
			out = append(out, b)
			i += n
			continue
		}
		para = append(para, line)
		i++
	}
	flush()
	return out
}

func trimAll(ss []string) []string {
	out := make([]string, len(ss))
	for i, s := range ss {
		out[i] = strings.TrimSpace(s)
	}
	return out
}

// parseTable reads a GFM table whose header row is header and whose
// delimiter row is lines[i]. It returns the block and the lines consumed
// after the header.
func parseTable(header string, lines []string, i int) (mdBlock, int, bool) {
	if i >= len(lines) || !strings.Contains(header, "|") || !tableDelim.MatchString(lines[i]) {
		return mdBlock{}, 0, false
	}
	head := splitRow(header)
	if len(head) != len(splitRow(lines[i])) {
		return mdBlock{}, 0, false
	}
	b := mdBlock{kind: bTable, head: head}
	j := i + 1
	for ; j < len(lines) && !isBlank(lines[j]) && strings.Contains(lines[j], "|"); j++ {
		row := splitRow(lines[j])
		for len(row) < len(head) {
			row = append(row, "")
		}
		// Cells past the header's width stay in the last column, as in
		// v0.1.20 (normalizeTeamsMarkdownTableRow): text written after a
		// row's closing pipe must not vanish.
		if n := len(head); len(row) > n {
			row[n-1] = strings.TrimSpace(strings.Join(row[n-1:], " | "))
		}
		b.rows = append(b.rows, row[:len(head)])
	}
	return b, j - i, true
}

// splitRow splits a table row on unescaped pipes outside code spans.
func splitRow(line string) []string {
	t := strings.TrimSpace(line)
	t = strings.TrimPrefix(t, "|")
	if strings.HasSuffix(t, "|") && !strings.HasSuffix(t, `\|`) {
		t = t[:len(t)-1]
	}
	var cells []string
	var cur strings.Builder
	inCode := 0
	for i := 0; i < len(t); i++ {
		c := t[i]
		switch {
		case c == '\\' && i+1 < len(t) && t[i+1] == '|':
			cur.WriteByte('|')
			i++
			continue
		case c == '`':
			n := 1
			for i+n < len(t) && t[i+n] == '`' {
				n++
			}
			if inCode == 0 {
				if strings.Contains(t[i+n:], strings.Repeat("`", n)) {
					inCode = n
				}
			} else if inCode == n {
				inCode = 0
			}
			cur.WriteString(t[i : i+n])
			i += n - 1
			continue
		case c == '|' && inCode == 0:
			cells = append(cells, strings.TrimSpace(cur.String()))
			cur.Reset()
			continue
		}
		cur.WriteByte(c)
	}
	return append(cells, strings.TrimSpace(cur.String()))
}

func parseList(lines []string, i int, first listMark, math bool) (mdBlock, int) {
	b := mdBlock{kind: bList, ordered: first.ordered, start: first.start}
	j := i
	mk := first
	for {
		item := []string{mk.rest}
		j++
		for j < len(lines) {
			l := lines[j]
			if isBlank(l) {
				// Keep blank lines only if the item continues after them.
				k := j
				for k < len(lines) && isBlank(lines[k]) {
					k++
				}
				if k < len(lines) && indentOf(lines[k]) >= mk.width {
					for ; j < k; j++ {
						item = append(item, "")
					}
					continue
				}
				break
			}
			if indentOf(l) >= mk.width {
				item = append(item, stripCols(l, mk.width))
				j++
				continue
			}
			if _, ok := listMarker(l); ok {
				break
			}
			// Lazy paragraph continuation.
			if !isBlank(item[len(item)-1]) && !startsBlock(lines, j, math) && !inFence(item) {
				item = append(item, strings.TrimLeft(l, " \t"))
				j++
				continue
			}
			break
		}
		it := mdItem{}
		if m := taskRE.FindStringSubmatch(item[0]); m != nil {
			it.task = 1
			if m[1] != " " {
				it.task = 2
			}
			item[0] = item[0][len(m[0]):]
		}
		it.kids = parseBlocks(item, math)
		b.items = append(b.items, it)
		// Next item of the same list, possibly after blank lines.
		k := j
		for k < len(lines) && isBlank(lines[k]) {
			k++
		}
		next, ok := mk, false
		if k < len(lines) {
			next, ok = listMarker(lines[k])
		}
		if !ok || !next.sameList(first) {
			return b, j - i
		}
		mk = next
		j = k
	}
}

// inFence reports whether item lines end inside an open code fence.
func inFence(item []string) bool {
	open := ""
	for _, l := range item {
		if f, ok := fenceOf(l); ok {
			if open == "" {
				open = f
			} else if strings.HasPrefix(strings.TrimSpace(l), open) && strings.Trim(strings.TrimSpace(l), open[:1]) == "" {
				open = ""
			}
		}
	}
	return open != ""
}

// ---------------------------------------------------------------------------
// Inlines

func esc(s string) string { return html.EscapeString(s) }

// inline renders one line of Markdown inline content as HTML.
func inline(s string, math bool) string {
	var b strings.Builder
	for i := 0; i < len(s); {
		c := s[i]
		switch {
		case c == '\\' && i+1 < len(s) && isASCIIPunct(s[i+1]):
			b.WriteString(esc(s[i+1 : i+2]))
			i += 2
			continue
		case c == '`':
			n := runLen(s, i, '`')
			if end := findCodeClose(s, i+n, n); end >= 0 {
				code := s[i+n : end]
				if len(code) >= 2 && code[0] == ' ' && code[len(code)-1] == ' ' && strings.TrimSpace(code) != "" {
					code = code[1 : len(code)-1]
				}
				b.WriteString("<code>" + esc(code) + "</code>")
				i = end + n
				continue
			}
			b.WriteString(s[i : i+n])
			i += n
			continue
		case math && strings.HasPrefix(s[i:], mathOpen):
			if end := strings.Index(s[i+len(mathOpen):], mathClose); end >= 0 {
				tex := s[i+len(mathOpen) : i+len(mathOpen)+end]
				b.WriteString("<code>" + esc(tex) + "</code>")
				i += len(mathOpen) + end + len(mathClose)
				continue
			}
		case c == '!' && i+1 < len(s) && s[i+1] == '[':
			if label, dest, n, ok := parseLink(s[i+1:]); ok {
				b.WriteString("Image: ")
				b.WriteString(link(inline(label, math), plainLabel(label), dest))
				i += 1 + n
				continue
			}
		case c == '[':
			if label, dest, n, ok := parseLink(s[i:]); ok {
				b.WriteString(link(inline(label, math), plainLabel(label), dest))
				i += n
				continue
			}
		case c == '<':
			if m := autolinkRE.FindStringSubmatch(s[i:]); m != nil {
				b.WriteString(link(esc(m[1]), m[1], m[1]))
				i += len(m[0])
				continue
			}
		case (c == 'h' || c == 'w') && (i == 0 || !isWordByte(s[i-1])):
			if m := bareURLRE.FindString(s[i:]); m != "" {
				u := trimURL(m)
				dest := u
				if strings.HasPrefix(u, "www.") {
					dest = "https://" + u
				}
				if safeURL(dest) {
					b.WriteString(`<a href="` + esc(dest) + `">` + esc(u) + `</a>`)
					i += len(u)
					continue
				}
			}
		case c == '*' || c == '_' || c == '~':
			if h, n, ok := emphasis(s, i, math); ok {
				b.WriteString(h)
				i += n
				continue
			}
			n := runLen(s, i, c)
			b.WriteString(s[i : i+n])
			i += n
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			b.WriteString("�")
		} else {
			b.WriteString(esc(s[i : i+size]))
		}
		i += size
	}
	return b.String()
}

func runLen(s string, i int, c byte) int {
	n := 0
	for i+n < len(s) && s[i+n] == c {
		n++
	}
	return n
}

func findCodeClose(s string, from, n int) int {
	for i := from; i < len(s); {
		if s[i] != '`' {
			i++
			continue
		}
		m := runLen(s, i, '`')
		if m == n {
			return i
		}
		i += m
	}
	return -1
}

func isASCIIPunct(c byte) bool {
	return c < 128 && unicode.IsPunct(rune(c)) || strings.IndexByte("$+<=>^`|~", c) >= 0
}

func isWordByte(c byte) bool {
	return c == '_' || c >= '0' && c <= '9' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= 0x80
}

// emphasis renders a *, _, ** , __, *** or ~~ span starting at s[i].
func emphasis(s string, i int, math bool) (string, int, bool) {
	c := s[i]
	n := runLen(s, i, c)
	if c == '~' {
		if n != 2 {
			return "", 0, false
		}
	} else if n > 3 {
		return "", 0, false
	}
	delim := s[i : i+n]
	open := i + n
	if open >= len(s) || s[open] == ' ' || s[open] == '\t' {
		return "", 0, false
	}
	if c == '_' && i > 0 && isWordByte(s[i-1]) {
		return "", 0, false
	}
	for j := open + 1; j <= len(s)-n; j++ {
		switch s[j] {
		case '\\':
			j++
			continue
		case '`':
			m := runLen(s, j, '`')
			if end := findCodeClose(s, j+m, m); end >= 0 {
				j = end + m - 1
			} else {
				j += m - 1
			}
			continue
		}
		if !strings.HasPrefix(s[j:], delim) || runLen(s, j, c) != n {
			if s[j] == c {
				j += runLen(s, j, c) - 1
			}
			continue
		}
		if s[j-1] == ' ' || s[j-1] == '\t' {
			continue
		}
		if c == '_' && j+n < len(s) && isWordByte(s[j+n]) {
			continue
		}
		inner := inline(s[open:j], math)
		var h string
		switch {
		case c == '~':
			h = "<s>" + inner + "</s>"
		case n == 1:
			h = "<em>" + inner + "</em>"
		case n == 2:
			h = "<strong>" + inner + "</strong>"
		default:
			h = "<strong><em>" + inner + "</em></strong>"
		}
		return h, j + n - i, true
	}
	return "", 0, false
}

// parseLink parses [label](dest "title") at the start of s.
func parseLink(s string) (label, dest string, n int, ok bool) {
	if !strings.HasPrefix(s, "[") {
		return
	}
	depth := 0
	end := -1
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				end = i
			}
		}
		if end >= 0 {
			break
		}
	}
	if end < 0 || end+1 >= len(s) || s[end+1] != '(' {
		return
	}
	rest := s[end+2:]
	par := 0
	close := -1
	for i := 0; i < len(rest); i++ {
		switch rest[i] {
		case '\\':
			i++
		case '(':
			par++
		case ')':
			if par == 0 {
				close = i
			} else {
				par--
			}
		}
		if close >= 0 {
			break
		}
	}
	if close < 0 {
		return
	}
	inner := strings.TrimSpace(rest[:close])
	if strings.HasPrefix(inner, "<") {
		if k := strings.Index(inner, ">"); k > 0 {
			inner = inner[1:k]
		}
	} else if k := strings.IndexAny(inner, " \t"); k >= 0 {
		inner = inner[:k] // drop the title
	}
	return s[1:end], inner, end + 2 + close + 1, true
}

func plainLabel(label string) string {
	return strings.TrimSpace(strings.Map(func(r rune) rune {
		if strings.ContainsRune("*_`~", r) {
			return -1
		}
		return r
	}, label))
}

// link renders a link. Only http(s) and mailto links become anchors; a
// label that differs from its destination is shown with the destination so
// the reader always sees where a link goes.
func link(labelHTML, labelText, dest string) string {
	if !safeURL(dest) {
		if labelText == dest || labelHTML == "" {
			return esc(dest)
		}
		return labelHTML + " (" + esc(dest) + ")"
	}
	a := `<a href="` + esc(dest) + `">` + esc(dest) + `</a>`
	if labelText == "" || labelText == dest || "mailto:"+labelText == dest {
		return a
	}
	return labelHTML + " (" + a + ")"
}

func safeURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || strings.ContainsAny(raw, " \t\n\"<>") {
		return false
	}
	switch strings.ToLower(u.Scheme) {
	case "http", "https":
		return u.Host != "" && u.User == nil
	case "mailto":
		return u.Opaque != ""
	}
	return false
}

// trimURL drops trailing punctuation and unbalanced closing parentheses.
func trimURL(u string) string {
	for len(u) > 0 {
		last := u[len(u)-1]
		if strings.IndexByte(trailPunctR, last) >= 0 {
			u = u[:len(u)-1]
			continue
		}
		if last == ')' && strings.Count(u, ")") > strings.Count(u, "(") {
			u = u[:len(u)-1]
			continue
		}
		break
	}
	return u
}
