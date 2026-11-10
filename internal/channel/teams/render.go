package teams

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

// Rendering: Markdown -> Teams chatMessage parts.
//
// Every part is <p><strong>Label [part i/n]:</strong> @owner</p> followed by
// whole blocks. Blocks are packed greedily up to targetPartBytes; a block
// that does not fit on its own is split at safe boundaries (lines for
// paragraphs and code, rows for tables with the header repeated, items for
// lists, children for quotes). List items are not cut unless one item alone
// exceeds a part. The blocks themselves are written exactly as v0.1.20 wrote
// them (markdown.go, mathplan.go).

const (
	targetPartBytes = 24 << 10
	markerReserve   = len("<!-- "+markerPrefix+" -->") + 200
	imgPlaceholder  = "\x00img\x00"
	imgRefGrowth    = 32 // len("../hostedContents/NN/$value") - len(imgPlaceholder), rounded up
)

type frag struct {
	html string
	pngs [][]byte
}

// Render implements channel.Channel.
func (a *Adapter) Render(ctx context.Context, doc channel.RichDoc) ([]channel.Part, error) {
	d := parseDoc(doc.Markdown, doc.Math)
	if doc.Math {
		a.renderMath(ctx, d.blocks)
	}
	var mention *identity
	if doc.Mention {
		me, err := a.me(ctx)
		if err != nil {
			return nil, err
		}
		mention = &me
	}
	atts, attHTML, err := outboundAttachments(doc.Files)
	if err != nil {
		return nil, err
	}

	foot := footer(doc.FooterLabel, doc.Footer)
	if len(doc.Notes) > 0 {
		var notes strings.Builder
		for _, n := range doc.Notes {
			if n.SeparatorBefore {
				notes.WriteString("<hr/>")
			}
			notes.WriteString(footer(n.Label, n.Text))
		}
		foot = notes.String() + foot
	}
	reserve := len(header(doc.Label, 999, 999, mention)) + markerReserve + len(attHTML) + len(foot)
	budget := targetPartBytes - reserve
	if budget < 4<<10 {
		return nil, fmt.Errorf("teams: label and attachments leave no room for content")
	}
	var frags []frag
	for _, b := range d.blocks {
		frags = append(frags, d.fit(b, budget)...)
	}

	// Greedy packing.
	var packs [][]frag
	var cur []frag
	size, imgs, imgBytes := 0, 0, 0
	for _, f := range frags {
		fb := 0
		for _, p := range f.pngs {
			fb += len(p)
		}
		// Image placeholders grow to "../hostedContents/N/$value" when packed.
		fs := len(f.html) + len(f.pngs)*imgRefGrowth
		if len(cur) > 0 && (size+fs > budget || imgs+len(f.pngs) > maxImagesPerPart || imgBytes+fb > maxPartImageBytes) {
			packs = append(packs, cur)
			cur, size, imgs, imgBytes = nil, 0, 0, 0
		}
		cur = append(cur, f)
		size += fs
		imgs += len(f.pngs)
		imgBytes += fb
	}
	packs = append(packs, cur)

	parts := make([]channel.Part, len(packs))
	for i, pk := range packs {
		var m *identity
		if i == 0 {
			m = mention
		}
		var content strings.Builder
		content.WriteString(header(doc.Label, i+1, len(packs), m))
		var hosted []map[string]string
		for _, f := range pk {
			h := f.html
			for _, png := range f.pngs {
				id := strconv.Itoa(len(hosted) + 1)
				h = strings.Replace(h, imgPlaceholder, "../hostedContents/"+id+"/$value", 1)
				hosted = append(hosted, map[string]string{
					"@microsoft.graph.temporaryId": id,
					"contentType":                  "image/png",
					"contentBytes":                 base64.StdEncoding.EncodeToString(png),
				})
			}
			content.WriteString(h)
		}
		if i == len(packs)-1 {
			content.WriteString(foot)
		}
		msg := map[string]any{}
		if i == len(packs)-1 && len(atts) > 0 {
			content.WriteString(attHTML)
			msg["attachments"] = atts
		}
		html := content.String()
		msg["body"] = map[string]string{"contentType": "html", "content": html}
		if m != nil {
			msg["mentions"] = []any{map[string]any{
				"id": 0, "mentionText": m.Name,
				"mentioned": map[string]any{"user": map[string]any{"id": m.ID, "displayName": m.Name, "userIdentityType": "aadUser"}},
			}}
		}
		if len(hosted) > 0 {
			msg["hostedContents"] = hosted
		}
		body, err := json.Marshal(msg)
		if err != nil {
			return nil, err
		}
		if len(html)+markerReserve > maxPartBytes {
			return nil, fmt.Errorf("teams: rendered part %d is %d bytes, over the %d limit", i+1, len(html), maxPartBytes)
		}
		p := channel.Part{
			Index: i + 1, Count: len(packs), Body: string(body), Size: len(html),
			Hash: channel.TextHash(plainText(html)), Mention: m != nil,
		}
		if i == 0 {
			p.ReplyTo = doc.ReplyTo
		}
		parts[i] = p
	}
	return parts, nil
}

func header(label string, i, n int, mention *identity) string {
	label = strings.TrimSpace(label)
	if n > 1 {
		label += fmt.Sprintf(" [part %d/%d]", i, n)
	}
	h := ""
	if label != "" {
		h = "<strong>" + esc(label) + ":</strong>"
	}
	if mention != nil {
		if h != "" {
			h += " "
		}
		h += `<at id="0">` + esc(mention.Name) + `</at>`
	}
	if h == "" {
		return ""
	}
	return "<p>" + h + "</p>"
}

// footer is the closing line of the last part: "<p><strong>Label:</strong>
// text</p>", as v0.1.20 appended "✅ Codex finished responding." to answers.
func footer(label, text string) string {
	label, text = strings.TrimSpace(label), strings.TrimSpace(text)
	switch {
	case text == "":
		return ""
	case label == "":
		return "<p>" + esc(text) + "</p>"
	}
	return "<p><strong>" + esc(label) + ":</strong> " + esc(text) + "</p>"
}

// mdDoc is a parsed document: its top-level blocks and, with formulas on,
// the tokens its Markdown was parsed with (see mathplan.go).
type mdDoc struct {
	blocks []mdBlock
	fix    *strings.Replacer // token -> formula HTML
}

// parseDoc parses Markdown the way v0.1.20 did: line endings normalized,
// blank-line runs outside fences collapsed, then blocks. With math, each
// formula is tokenized first and a top-level paragraph holding only a
// display formula becomes a bMath block.
func parseDoc(src string, math bool) *mdDoc {
	text := compactBlankLines(strings.ReplaceAll(src, "\x00", ""))
	var toks []mathToken
	if math {
		text, toks = planMath(text)
	}
	d := &mdDoc{blocks: parseBlocks(text)}
	if len(toks) == 0 {
		return d
	}
	display := map[string]string{}
	pairs := make([]string, 0, 4*len(toks))
	for _, t := range toks {
		h := "<code>" + esc(t.src) + "</code>"
		if t.display {
			h = codeHTML(t.src)
			display[t.tok] = t.src
		}
		// A formula alone in a paragraph replaces the paragraph.
		pairs = append(pairs, "<p>"+t.tok+"</p>", h, t.tok, h)
	}
	d.fix = strings.NewReplacer(pairs...)
	for i, b := range d.blocks {
		if src, ok := display[b.text]; ok && b.kind == bPara {
			d.blocks[i] = mdBlock{kind: bMath, tex: src}
		}
	}
	return d
}

func (d *mdDoc) fixup(h string) string {
	if d.fix == nil {
		return h
	}
	return d.fix.Replace(h)
}

// renderMath renders the top-level display formulas in one batch. Formulas
// over the limits, refused by safeTeX, or failed by the renderer stay as code.
func (a *Adapter) renderMath(ctx context.Context, blocks []mdBlock) {
	if a.cfg.Math == nil {
		return
	}
	var idx []int
	var srcs []string
	total := 0
	for i, b := range blocks {
		if b.kind != bMath || !safeTeX(b.tex) {
			continue
		}
		if len(srcs) >= maxMathFormulas || total+len(b.tex) > maxMathTotalBytes {
			break
		}
		total += len(b.tex)
		idx = append(idx, i)
		srcs = append(srcs, b.tex)
	}
	if len(srcs) == 0 {
		return
	}
	res, err := a.cfg.Math.Render(ctx, srcs)
	if err != nil || len(res) != len(srcs) {
		return
	}
	for k, i := range idx {
		if checkPNG(res[k]) == nil {
			r := res[k]
			blocks[i].png = &r
		}
	}
}

// render renders one top-level block.
func (d *mdDoc) render(b mdBlock) frag {
	if b.kind == bMath {
		code := codeHTML(b.tex)
		if b.png != nil {
			// Same <img> as v0.1.20: no width/height, Teams sizes the image.
			img := fmt.Sprintf(`<p><img src="%s" alt="Rendered TeX formula"></p>`, imgPlaceholder)
			return frag{html: code + img, pngs: [][]byte{b.png.PNG}}
		}
		return frag{html: code}
	}
	return frag{html: d.fixup(blockHTML(b))}
}

// blockHTML is v0.1.20 renderTeamsMarkdownBlockHTML.
func blockHTML(b mdBlock) string {
	switch b.kind {
	case bCode:
		return codeHTML(b.text)
	case bMath:
		return codeHTML(b.tex)
	case bRule:
		return "<hr/>"
	case bList:
		var s strings.Builder
		s.WriteString(listOpen(b))
		for _, it := range b.items {
			s.WriteString(itemHTML(it))
		}
		s.WriteString(listClose(b))
		return s.String()
	case bTable:
		var s strings.Builder
		s.WriteString("<table>")
		s.WriteString(rowHTML(b.head, "th"))
		for _, r := range b.rows {
			s.WriteString(rowHTML(r, "td"))
		}
		s.WriteString("</table>")
		return s.String()
	case bQuote:
		var s strings.Builder
		s.WriteString("<blockquote>")
		for _, k := range b.kids {
			s.WriteString(blockHTML(k))
		}
		s.WriteString("</blockquote>")
		return s.String()
	}
	return "<p>" + blockBodyHTML(b) + "</p>"
}

// blockBodyHTML is the content of a paragraph or heading without its <p>.
func blockBodyHTML(b mdBlock) string {
	if b.kind != bHeading {
		return inlineLines(b.text)
	}
	t := inline(b.text)
	switch {
	case b.level <= 2:
		return "<strong>" + t + "</strong>"
	case b.level == 3:
		return "<strong><em>" + t + "</em></strong>"
	default:
		return "<em>" + t + "</em>"
	}
}

func codeHTML(text string) string {
	return "<pre><code>" + esc(text) + "</code></pre>"
}

func paraHTML(lines []string) string {
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = lineHTML(l)
	}
	return "<p>" + strings.Join(out, "<br>") + "</p>"
}

func rowHTML(cells []string, tag string) string {
	var s strings.Builder
	s.WriteString("<tr>")
	for _, c := range cells {
		s.WriteString("<" + tag + ">" + inlineLines(strings.TrimSpace(c)) + "</" + tag + ">")
	}
	s.WriteString("</tr>")
	return s.String()
}

// listOpen has no start attribute: v0.1.20 numbered every list from 1.
func listOpen(b mdBlock) string {
	if b.ordered {
		return "<ol>"
	}
	return "<ul>"
}

func listClose(b mdBlock) string {
	if b.ordered {
		return "</ol>"
	}
	return "</ul>"
}

// itemHTML renders an item: its text, then each child after a <br>;
// paragraphs inside items have no <p>.
func itemHTML(it mdItem) string {
	var s strings.Builder
	s.WriteString("<li>")
	wrote := false
	if strings.TrimSpace(it.text) != "" {
		s.WriteString(inlineLines(it.text))
		wrote = true
	}
	for _, k := range it.kids {
		if wrote {
			s.WriteString("<br>")
		}
		if k.kind == bPara || k.kind == bHeading || k.kind == bRule {
			s.WriteString(blockBodyHTML(k))
		} else {
			s.WriteString(blockHTML(k))
		}
		wrote = true
	}
	s.WriteString("</li>")
	return s.String()
}

// fit renders a top-level block as one or more fragments, each at most
// budget bytes.
func (d *mdDoc) fit(b mdBlock, budget int) []frag {
	f := d.render(b)
	if len(f.html) <= budget {
		return []frag{f}
	}
	switch b.kind {
	case bCode, bMath:
		text := b.text
		if b.kind == bMath {
			text = b.tex
		}
		return packLines(strings.Split(text, "\n"), budget-len("<pre><code></code></pre>"), func(ls []string) string {
			return d.fixup(codeHTML(strings.Join(ls, "\n")))
		}, func(l string) string { return d.fixup(esc(l)) })
	case bTable:
		head := "<table>" + d.fixup(rowHTML(b.head, "th"))
		var rows []string
		for _, r := range b.rows {
			rows = append(rows, d.fixup(rowHTML(r, "td")))
		}
		return packItems(rows, budget, head, "</table>", func(_ string, i int) []frag {
			// One row larger than a part: show its cells as paragraph lines.
			return d.fit(mdBlock{kind: bPara, text: strings.Join(b.rows[i], "\n")}, budget)
		})
	case bList:
		open, close := listOpen(b), listClose(b)
		over := len(open) + len(close)
		var out []frag
		var cur strings.Builder
		flush := func() {
			if cur.Len() > 0 {
				out = append(out, frag{html: open + cur.String() + close})
				cur.Reset()
			}
		}
		for _, it := range b.items {
			h := d.fixup(itemHTML(it))
			if over+len(h) > budget {
				// One item larger than a part: the only case an item is cut.
				flush()
				if strings.TrimSpace(it.text) != "" {
					out = append(out, d.fit(mdBlock{kind: bPara, text: it.text}, budget)...)
				}
				for _, k := range it.kids {
					out = append(out, d.fit(k, budget)...)
				}
				continue
			}
			if cur.Len() > 0 && over+cur.Len()+len(h) > budget {
				flush()
			}
			cur.WriteString(h)
		}
		flush()
		return out
	case bQuote:
		var out []frag
		for _, k := range b.kids {
			for _, kf := range d.fit(k, budget-len("<blockquote></blockquote>")) {
				out = append(out, frag{html: "<blockquote>" + kf.html + "</blockquote>", pngs: kf.pngs})
			}
		}
		return mergeQuotes(out, budget)
	case bHeading:
		return d.fit(mdBlock{kind: bPara, text: b.text}, budget)
	default:
		return packLines(strings.Split(b.text, "\n"), budget-len("<p></p>"), func(ls []string) string {
			return d.fixup(paraHTML(ls))
		}, func(l string) string { return d.fixup(lineHTML(l)) })
	}
}

// packLines groups lines into fragments rendered by wrap. A single line
// that does not fit is cut at whitespace (or, failing that, at a rune
// boundary) so each piece renders within budget.
func packLines(lines []string, budget int, wrap func([]string) string, one func(string) string) []frag {
	var out []frag
	var cur []string
	size := 0
	flush := func() {
		if len(cur) > 0 {
			out = append(out, frag{html: wrap(cur)})
			cur, size = nil, 0
		}
	}
	for _, l := range lines {
		pieces := []string{l}
		if len(one(l)) > budget {
			pieces = cutLine(l, budget, one)
		}
		for _, p := range pieces {
			n := len(one(p)) + len("<br>")
			if len(cur) > 0 && size+n > budget {
				flush()
			}
			cur = append(cur, p)
			size += n
		}
	}
	flush()
	return out
}

func cutLine(l string, budget int, render func(string) string) []string {
	var out []string
	for l != "" {
		n := min(len(l), budget)
		for n > 0 && len(render(l[:n])) > budget {
			n = n * 3 / 4
		}
		for n > 0 && n < len(l) && !utf8.RuneStart(l[n]) {
			n--
		}
		if n == 0 {
			_, n = utf8.DecodeRuneInString(l)
		}
		if n < len(l) {
			if k := strings.LastIndexAny(l[:n], " \t"); k > n/2 {
				n = k + 1
			}
			// Never cut through a formula token.
			if k := strings.LastIndex(l[:n], ""); k > 0 && !strings.Contains(l[k:n], "") {
				n = k
			}
		}
		out = append(out, l[:n])
		l = l[n:]
	}
	return out
}

// packItems packs pre-rendered rows between open and close.
func packItems(items []string, budget int, open, close string, single func(string, int) []frag) []frag {
	var out []frag
	var cur strings.Builder
	over := len(open) + len(close)
	flush := func() {
		if cur.Len() > 0 {
			out = append(out, frag{html: open + cur.String() + close})
			cur.Reset()
		}
	}
	for i, it := range items {
		if over+len(it) > budget {
			flush()
			out = append(out, single(it, i)...)
			continue
		}
		if cur.Len() > 0 && over+cur.Len()+len(it) > budget {
			flush()
		}
		cur.WriteString(it)
	}
	flush()
	return out
}

func mergeQuotes(fs []frag, budget int) []frag {
	var out []frag
	for _, f := range fs {
		if n := len(out); n > 0 {
			last := out[n-1]
			merged := strings.TrimSuffix(last.html, "</blockquote>") + strings.TrimPrefix(f.html, "<blockquote>")
			if len(merged) <= budget && len(last.pngs)+len(f.pngs) <= maxImagesPerPart {
				out[n-1] = frag{html: merged, pngs: append(last.pngs, f.pngs...)}
				continue
			}
		}
		out = append(out, f)
	}
	return out
}

// outboundAttachments turns uploaded FileRefs into Graph reference
// attachments and the matching <attachment> tags.
func outboundAttachments(files []channel.FileRef) ([]map[string]string, string, error) {
	var atts []map[string]string
	var tags strings.Builder
	for _, f := range files {
		d, err := parseDriveRef(f)
		if err != nil {
			return nil, "", err
		}
		atts = append(atts, map[string]string{"id": d.AttachmentID, "contentType": "reference", "contentUrl": d.URL, "name": first(f.Name, d.Name)})
		tags.WriteString(`<attachment id="` + esc(d.AttachmentID) + `"></attachment>`)
	}
	return atts, tags.String(), nil
}
