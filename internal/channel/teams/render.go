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
// lists with numbering kept, children for quotes). List items are not cut
// unless one item alone exceeds a part.

const (
	targetPartBytes = 24 << 10
	markerReserve   = len("<!-- "+markerPrefix+" -->") + 200
	imgPlaceholder  = "\x00img\x00"
)

type frag struct {
	html string
	pngs [][]byte
}

// Render implements channel.Channel.
func (a *Adapter) Render(ctx context.Context, doc channel.RichDoc) ([]channel.Part, error) {
	blocks := parseMarkdown(doc.Markdown, doc.Math)
	if doc.Math {
		a.renderMath(ctx, blocks)
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

	// Image placeholders grow to "../hostedContents/N/$value" when packed.
	reserve := len(header(doc.Label, 999, 999, mention)) + markerReserve + len(attHTML) + maxImagesPerPart*32
	budget := targetPartBytes - reserve
	if budget < 4<<10 {
		return nil, fmt.Errorf("teams: label and attachments leave no room for content")
	}
	var frags []frag
	for _, b := range blocks {
		frags = append(frags, fit(b, budget)...)
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
		if len(cur) > 0 && (size+len(f.html) > budget || imgs+len(f.pngs) > maxImagesPerPart || imgBytes+fb > maxPartImageBytes) {
			packs = append(packs, cur)
			cur, size, imgs, imgBytes = nil, 0, 0, 0
		}
		cur = append(cur, f)
		size += len(f.html)
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

// renderBlock renders one block. top is false inside lists, quotes and
// tables, where formulas are always shown as code.
func renderBlock(b mdBlock, top bool) frag {
	switch b.kind {
	case bHeading:
		t := inline(strings.TrimSpace(b.lines[0]), b.math)
		switch {
		case b.level <= 2:
			return frag{html: "<p><strong>" + t + "</strong></p>"}
		case b.level == 3:
			return frag{html: "<p><strong><em>" + t + "</em></strong></p>"}
		default:
			return frag{html: "<p><em>" + t + "</em></p>"}
		}
	case bCode:
		return frag{html: codeHTML(b.lines)}
	case bRule:
		return frag{html: "<hr/>"}
	case bMath:
		code := codeHTML(strings.Split(b.tex, "\n"))
		if top && b.png != nil {
			// Same <img> as v0.1.20: no width/height, Teams sizes the image.
			img := fmt.Sprintf(`<p><img src="%s" alt="Rendered TeX formula"></p>`, imgPlaceholder)
			return frag{html: code + img, pngs: [][]byte{b.png.PNG}}
		}
		return frag{html: code}
	case bTable:
		var s strings.Builder
		s.WriteString("<table>")
		s.WriteString(rowHTML(b.head, "th", b.math))
		for _, r := range b.rows {
			s.WriteString(rowHTML(r, "td", b.math))
		}
		s.WriteString("</table>")
		return frag{html: s.String()}
	case bQuote:
		return frag{html: "<blockquote>" + kidsHTML(b.kids) + "</blockquote>"}
	case bList:
		var s strings.Builder
		s.WriteString(listOpen(b, b.start))
		for _, it := range b.items {
			s.WriteString(itemHTML(it))
		}
		s.WriteString(listClose(b))
		return frag{html: s.String()}
	default:
		return frag{html: paraHTML(b.lines, b.math)}
	}
}

func codeHTML(lines []string) string {
	return "<pre><code>" + esc(strings.Join(lines, "\n")) + "</code></pre>"
}

func paraHTML(lines []string, math bool) string {
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		out = append(out, lineHTML(l, math))
	}
	return "<p>" + strings.Join(out, "<br>") + "</p>"
}

func lineHTML(l string, math bool) string {
	l = strings.TrimRight(l, " \t")
	n := indentOf(l)
	return strings.Repeat("&nbsp;", n) + inline(strings.TrimLeft(l, " \t"), math)
}

func rowHTML(cells []string, tag string, math bool) string {
	var s strings.Builder
	s.WriteString("<tr>")
	for _, c := range cells {
		s.WriteString("<" + tag + ">" + inline(c, math) + "</" + tag + ">")
	}
	s.WriteString("</tr>")
	return s.String()
}

func kidsHTML(kids []mdBlock) string {
	var s strings.Builder
	for _, k := range kids {
		s.WriteString(renderBlock(k, false).html)
	}
	return s.String()
}

func listOpen(b mdBlock, start int) string {
	if !b.ordered {
		return "<ul>"
	}
	if start == 1 {
		return "<ol>"
	}
	return `<ol start="` + strconv.Itoa(start) + `">`
}

func listClose(b mdBlock) string {
	if b.ordered {
		return "</ol>"
	}
	return "</ul>"
}

func itemHTML(it mdItem) string {
	box := ""
	switch it.task {
	case 1:
		box = "☐ "
	case 2:
		box = "☑ "
	}
	// A single paragraph item renders inline (no <p> spacing).
	if len(it.kids) == 1 && it.kids[0].kind == bPara {
		k := it.kids[0]
		out := make([]string, len(k.lines))
		for i, l := range k.lines {
			out[i] = lineHTML(l, k.math)
		}
		return "<li>" + box + strings.Join(out, "<br>") + "</li>"
	}
	if len(it.kids) == 0 {
		return "<li>" + strings.TrimSpace(box) + "</li>"
	}
	return "<li>" + box + kidsHTML(it.kids) + "</li>"
}

// fit renders b as one or more fragments, each at most budget bytes.
func fit(b mdBlock, budget int) []frag {
	f := renderBlock(b, true)
	if len(f.html) <= budget {
		return []frag{f}
	}
	switch b.kind {
	case bCode, bMath:
		lines := b.lines
		if b.kind == bMath {
			lines = strings.Split(b.tex, "\n")
		}
		return packLines(lines, budget-len("<pre><code></code></pre>"), func(ls []string) string {
			return codeHTML(ls)
		}, func(l string) string { return esc(l) })
	case bTable:
		head := "<table>" + rowHTML(b.head, "th", b.math)
		var rows []string
		for _, r := range b.rows {
			rows = append(rows, rowHTML(r, "td", b.math))
		}
		return packItems(rows, budget, head, "</table>", func(r string, i int) []frag {
			// One row larger than a part: show its cells as paragraphs.
			return fit(mdBlock{kind: bPara, lines: b.rows[i], math: b.math}, budget)
		})
	case bList:
		var items []string
		for _, it := range b.items {
			items = append(items, itemHTML(it))
		}
		var out []frag
		start := 0 // index of the first item in the current chunk
		var cur strings.Builder
		flush := func(end int) {
			if end > start {
				out = append(out, frag{html: listOpen(b, b.start+start) + cur.String() + listClose(b)})
			}
			cur.Reset()
			start = end
		}
		over := len(listOpen(b, b.start+len(items))) + len(listClose(b))
		for i, h := range items {
			if over+len(h) > budget {
				// One item larger than a part: the only case an item is cut.
				flush(i)
				for _, k := range b.items[i].kids {
					out = append(out, fit(k, budget)...)
				}
				start = i + 1
				continue
			}
			if cur.Len() > 0 && over+cur.Len()+len(h) > budget {
				flush(i)
			}
			cur.WriteString(h)
		}
		flush(len(items))
		return out
	case bQuote:
		var out []frag
		for _, k := range b.kids {
			for _, kf := range fit(k, budget-len("<blockquote></blockquote>")) {
				out = append(out, frag{html: "<blockquote>" + kf.html + "</blockquote>", pngs: kf.pngs})
			}
		}
		return mergeQuotes(out, budget)
	case bHeading:
		return fit(mdBlock{kind: bPara, lines: b.lines, math: b.math}, budget)
	default:
		return packLines(b.lines, budget-len("<p></p>"), func(ls []string) string {
			return paraHTML(ls, b.math)
		}, func(l string) string { return lineHTML(l, b.math) })
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
		d, err := parseDriveRef(f.Ref)
		if err != nil {
			return nil, "", err
		}
		atts = append(atts, map[string]string{"id": d.AttachmentID, "contentType": "reference", "contentUrl": d.URL, "name": first(f.Name, d.Name)})
		tags.WriteString(`<attachment id="` + esc(d.AttachmentID) + `"></attachment>`)
	}
	return atts, tags.String(), nil
}
