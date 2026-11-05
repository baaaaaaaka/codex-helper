package transcript

import "strings"

// Hidden text in Codex records (SPEC F-MIR-8, R-23): Codex memory
// citations, and the helper's own prompt envelope when a prompt record is
// shown or used as a title. Both are ports of v0.1.20 artifact_handoff.go
// (StripOAIMemoryCitationBlocks, StripHelperPromptEchoes).

// StripMemCitations removes the <oai-mem-citation> metadata blocks Codex
// appends to answers that used its memories. Only a block whose payload
// starts with <citation_entries> or <rollout_ids> is removed; a block inside
// a closed Markdown code fence and a literal mention of the tag stay.
// Text without the tag is returned unchanged.
func StripMemCitations(text string) string {
	if indexASCIIFold(text, "<oai-mem-citation>") < 0 {
		return text
	}
	text = strings.TrimSpace(text)
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	plain := make([]string, 0, len(lines))
	inFence := false
	var fence byte
	var fenceLen int
	flushPlain := func() {
		if len(plain) == 0 {
			return
		}
		if stripped := stripMemCitationsPlain(strings.Join(plain, "\n")); stripped != "" {
			out = append(out, strings.Split(stripped, "\n")...)
		}
		plain = plain[:0]
	}
	for i, line := range lines {
		if inFence {
			out = append(out, line)
			if fenceEnd(line, fence, fenceLen) {
				inFence = false
			}
			continue
		}
		if f, n, ok := fenceStart(line); ok && indexASCIIFold(line, "<oai-mem-citation>") < 0 && fenceClosed(lines[i+1:], f, n) {
			flushPlain()
			out = append(out, line)
			inFence, fence, fenceLen = true, f, n
			continue
		}
		plain = append(plain, line)
	}
	flushPlain()
	return strings.TrimSpace(strings.Join(out, "\n"))
}

func stripMemCitationsPlain(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	const startTag, endTag = "<oai-mem-citation>", "</oai-mem-citation>"
	offset := 0
	for {
		rel := citationBlockStart(text[offset:], startTag)
		if rel < 0 {
			break
		}
		start := offset + rel
		afterStart := start + len(startTag)
		endRel := indexASCIIFold(text[afterStart:], endTag)
		if endRel < 0 {
			break
		}
		if !citationPayload(strings.Split(text[afterStart:afterStart+endRel], "\n")) {
			offset = afterStart
			continue
		}
		text = text[:start] + text[afterStart+endRel+len(endTag):]
		offset = 0
	}
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	inBlock := false
	for i, line := range lines {
		lower := strings.ToLower(strings.TrimSpace(line))
		if !inBlock {
			if lower == startTag && citationPayload(lines[i+1:]) {
				inBlock = true
				continue
			}
			out = append(out, line)
			continue
		}
		if lower == endTag {
			inBlock = false
		}
	}
	return strings.TrimSpace(strings.Join(out, "\n"))
}

// citationPayload reports whether the first non-blank line opens a memory
// citation payload.
func citationPayload(lines []string) bool {
	for _, line := range lines {
		switch strings.ToLower(strings.TrimSpace(line)) {
		case "":
			continue
		case "<citation_entries>", "<rollout_ids>":
			return true
		default:
			return false
		}
	}
	return false
}

// citationBlockStart finds a start tag that ends its line.
func citationBlockStart(text, tag string) int {
	for offset := 0; offset <= len(text); {
		idx := indexASCIIFold(text[offset:], tag)
		if idx < 0 {
			return -1
		}
		start := offset + idx
		after := start + len(tag)
		if after >= len(text) || text[after] == '\n' || text[after] == '\r' {
			return start
		}
		offset = after
	}
	return -1
}

func fenceStart(line string) (byte, int, bool) {
	t := strings.TrimLeft(line, " \t")
	if t == "" || (t[0] != '`' && t[0] != '~') {
		return 0, 0, false
	}
	n := delimiterRun(t, t[0])
	if n < 3 {
		return 0, 0, false
	}
	return t[0], n, true
}

func fenceEnd(line string, fence byte, n int) bool {
	t := strings.TrimLeft(line, " \t")
	if len(t) < n || t[0] != fence {
		return false
	}
	c := delimiterRun(t, fence)
	return c >= n && strings.TrimSpace(t[c:]) == ""
}

func fenceClosed(lines []string, fence byte, n int) bool {
	for _, l := range lines {
		if fenceEnd(l, fence, n) {
			return true
		}
	}
	return false
}

func delimiterRun(s string, d byte) int {
	n := 0
	for n < len(s) && s[n] == d {
		n++
	}
	return n
}

func indexASCIIFold(text, needle string) int {
	if needle == "" {
		return 0
	}
	for i := 0; i+len(needle) <= len(text); i++ {
		ok := true
		for j := 0; j < len(needle); j++ {
			if foldByte(text[i+j]) != foldByte(needle[j]) {
				ok = false
				break
			}
		}
		if ok {
			return i
		}
	}
	return -1
}

func foldByte(c byte) byte {
	if c >= 'A' && c <= 'Z' {
		return c + 'a' - 'A'
	}
	return c
}

// Leads of the helper's prompt envelope (engine envelope.go, v0.1.20
// artifact_handoff.go).
const (
	userMessageLead = "User message:"
	safetyLead      = "Teams helper safety:"
	artifactLead    = "If you need to return generated files or images to the Teams user, write them under this local directory:"
)

// StripHelperPrompt returns the user's own text of a prompt the helper
// wrapped in its envelope (the "User message:" section without the safety
// block and the file instructions after it). Other text is returned
// trimmed and unchanged.
func StripHelperPrompt(text string) string {
	text = strings.TrimSpace(text)
	for _, lead := range []string{safetyLead, artifactLead} {
		if i := strings.Index(text, lead); i >= 0 {
			text = strings.TrimSpace(text[:i])
			if len(text) >= len(userMessageLead) && strings.EqualFold(text[:len(userMessageLead)], userMessageLead) {
				text = strings.TrimSpace(text[len(userMessageLead):])
			}
		}
	}
	return text
}
