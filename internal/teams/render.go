package teams

import (
	"html"
	"strings"
	"time"
)

const (
	TeamsRenderHardLimitBytes    = 80 * 1024
	teamsRenderTargetLimitBytes  = 72 * 1024
	defaultOwnerMentionThreshold = 60 * time.Second
)

type TeamsRenderSurface string

const (
	TeamsRenderSurfaceTranscript TeamsRenderSurface = "transcript"
	TeamsRenderSurfaceControl    TeamsRenderSurface = "control"
	TeamsRenderSurfaceOutbox     TeamsRenderSurface = "outbox"
)

type TeamsRenderKind string

const (
	TeamsRenderUser      TeamsRenderKind = "user"
	TeamsRenderAssistant TeamsRenderKind = "assistant"
	TeamsRenderProgress  TeamsRenderKind = "progress"
	TeamsRenderHelper    TeamsRenderKind = "helper"
	TeamsRenderStatus    TeamsRenderKind = "status"
	TeamsRenderCode      TeamsRenderKind = "code"
	TeamsRenderCommand   TeamsRenderKind = "command"
)

type TeamsRenderInput struct {
	Surface      TeamsRenderSurface
	Kind         TeamsRenderKind
	Text         string
	CodeLanguage string
}

type TeamsRenderOptions struct {
	HardLimitBytes   int
	TargetLimitBytes int
}

type TeamsRenderedChunk struct {
	HTML       string
	Text       string
	Label      string
	PartIndex  int
	PartCount  int
	ByteLength int
}

func RenderTeamsHTML(input TeamsRenderInput) string {
	return renderTeamsHTMLPart(input, 1, 1)
}

func renderTeamsFreezeNoticeHTML(controlLink string, resumeCommand string, safeLine string) string {
	controlLink = strings.TrimSpace(controlLink)
	resumeCommand = strings.TrimSpace(resumeCommand)
	if resumeCommand == "" {
		resumeCommand = "r <resume-key>"
	}
	safeLine = strings.TrimSpace(safeLine)
	if safeLine == "" {
		safeLine = "Your Codex work is safe."
	}
	step1 := "Step 1: Open Control chat"
	if href, ok := safeTeamsMarkdownURL(controlLink); ok {
		step1 = `Step 1: Open <a href="` + html.EscapeString(href) + `">Control chat</a>`
	}
	return strings.Join([]string{
		`<p><strong>🔧 Helper:</strong><br>` +
			`🧊 This chat is paused<br>` +
			`⚠ <strong>Messages here will not get a reply.</strong><br>` +
			html.EscapeString(safeLine) + `</p>`,
		`<p>&nbsp;</p>`,
		`<p>▶️ <strong>Continue chat:</strong><br>` +
			step1 + `<br>` +
			`Step 2: Send: <code>` + html.EscapeString(resumeCommand) + `</code></p>`,
	}, "")
}

func PlanTeamsHTMLChunks(input TeamsRenderInput, opts TeamsRenderOptions) []TeamsRenderedChunk {
	_, targetLimit := normalizeTeamsRenderLimits(opts)
	text := normalizeTeamsRenderTextForKind(input.Kind, input.Text)
	if text == "" {
		input.Text = ""
		html := renderTeamsHTMLPart(input, 1, 1)
		return []TeamsRenderedChunk{{
			HTML:       html,
			Text:       "",
			Label:      teamsRenderLabel(input.Kind, 1, 1),
			PartIndex:  1,
			PartCount:  1,
			ByteLength: len(html),
		}}
	}

	plannedCount := 1
	var parts []string
	for attempts := 0; attempts < 8; attempts++ {
		parts = splitTeamsRenderText(input, text, targetLimit, plannedCount)
		if len(parts) <= 1 {
			plannedCount = 1
			break
		}
		if len(parts) == plannedCount {
			break
		}
		plannedCount = len(parts)
	}
	if len(parts) == 0 {
		parts = []string{""}
	}

	total := len(parts)
	chunks := make([]TeamsRenderedChunk, 0, total)
	for i, part := range parts {
		partInput := input
		partInput.Text = part
		html := renderTeamsHTMLPart(partInput, i+1, total)
		chunks = append(chunks, TeamsRenderedChunk{
			HTML:       html,
			Text:       part,
			Label:      teamsRenderLabel(input.Kind, i+1, total),
			PartIndex:  i + 1,
			PartCount:  total,
			ByteLength: len(html),
		})
	}
	return chunks
}

func normalizeTeamsRenderLimits(opts TeamsRenderOptions) (int, int) {
	hardLimit := opts.HardLimitBytes
	if hardLimit <= 0 {
		hardLimit = TeamsRenderHardLimitBytes
	}
	targetLimit := opts.TargetLimitBytes
	if targetLimit <= 0 {
		targetLimit = teamsRenderTargetLimitBytes
	}
	if targetLimit > hardLimit {
		targetLimit = hardLimit
	}
	minimum := len(renderTeamsHTMLPart(TeamsRenderInput{Kind: TeamsRenderHelper}, 1, 1)) + 16
	if hardLimit < minimum {
		hardLimit = minimum
	}
	if targetLimit < minimum {
		targetLimit = minimum
	}
	if targetLimit > hardLimit {
		targetLimit = hardLimit
	}
	return hardLimit, targetLimit
}

func splitTeamsRenderText(input TeamsRenderInput, text string, limitBytes int, plannedCount int) []string {
	if teamsRenderKindUsesCodexMarkdown(input.Kind) {
		if parts, ok := splitTeamsRenderMarkdownTables(input, text, limitBytes, plannedCount); ok {
			return parts
		}
	}
	return splitTeamsRenderTextGeneric(input, text, limitBytes, plannedCount)
}

func splitTeamsRenderTextGeneric(input TeamsRenderInput, text string, limitBytes int, plannedCount int) []string {
	runes := []rune(text)
	if len(runes) == 0 {
		return []string{""}
	}
	var chunks []string
	for len(runes) > 0 {
		partIndex := len(chunks) + 1
		if plannedCount < 2 {
			plannedCount = 1
		}
		remaining := string(runes)
		partInput := input
		partInput.Text = remaining
		if len(renderTeamsHTMLPart(partInput, partIndex, plannedCount)) <= limitBytes {
			chunks = append(chunks, remaining)
			break
		}

		best := 0
		low, high := 1, len(runes)
		for low <= high {
			mid := low + (high-low)/2
			candidate := string(runes[:mid])
			partInput.Text = candidate
			if len(renderTeamsHTMLPart(partInput, partIndex, plannedCount)) <= limitBytes {
				best = mid
				low = mid + 1
			} else {
				high = mid - 1
			}
		}
		if best <= 0 {
			best = 1
		}

		cut := best
		for i := best; i > best/2; i-- {
			if runes[i-1] != '\n' && runes[i-1] != ' ' && runes[i-1] != '\t' {
				continue
			}
			candidate := string(runes[:i])
			if candidate == "" {
				continue
			}
			partInput.Text = candidate
			if len(renderTeamsHTMLPart(partInput, partIndex, plannedCount)) <= limitBytes {
				cut = i
				break
			}
		}

		chunk := string(runes[:cut])
		if chunk == "" {
			chunk = string(runes[:best])
			cut = best
		}
		chunks = append(chunks, chunk)
		runes = runes[cut:]
	}
	return chunks
}

func splitTeamsRenderMarkdownTables(input TeamsRenderInput, text string, limitBytes int, plannedCount int) ([]string, bool) {
	lines := strings.Split(text, "\n")
	var parts []string
	var current []string
	tableSeen := false

	fits := func(candidate string, partIndex int) bool {
		partInput := input
		partInput.Text = candidate
		return len(renderTeamsHTMLPart(partInput, partIndex, plannedCount)) <= limitBytes
	}
	flushCurrent := func() {
		body := joinTeamsMarkdownLinesTrimBlankEdges(current)
		current = current[:0]
		if body == "" {
			return
		}
		partIndex := len(parts) + 1
		if fits(body, partIndex) {
			parts = append(parts, body)
			return
		}
		parts = append(parts, splitTeamsRenderTextGeneric(input, body, limitBytes, plannedCount)...)
	}
	appendNonTableLine := func(line string) {
		candidateLines := append(append([]string{}, current...), line)
		candidate := joinTeamsMarkdownLinesTrimBlankEdges(candidateLines)
		if candidate == "" || fits(candidate, len(parts)+1) {
			current = candidateLines
			return
		}
		flushCurrent()
		current = append(current, line)
		candidate = joinTeamsMarkdownLinesTrimBlankEdges(current)
		if candidate != "" && !fits(candidate, len(parts)+1) {
			flushCurrent()
		}
	}
	appendTableChunk := func(chunk string) {
		if chunk == "" {
			return
		}
		if len(current) > 0 {
			candidateLines := append(append([]string{}, current...), "")
			candidateLines = append(candidateLines, strings.Split(chunk, "\n")...)
			candidate := joinTeamsMarkdownLinesTrimBlankEdges(candidateLines)
			if candidate != "" && fits(candidate, len(parts)+1) {
				current = candidateLines
				return
			}
			flushCurrent()
		}
		if fits(chunk, len(parts)+1) {
			parts = append(parts, chunk)
			return
		}
		parts = append(parts, splitTeamsRenderTextGeneric(input, chunk, limitBytes, plannedCount)...)
	}

	for i := 0; i < len(lines); {
		if _, next, ok := parseTeamsMarkdownTableBlock(lines, i); ok {
			tableSeen = true
			tableLines := lines[i:next]
			for _, chunk := range splitTeamsMarkdownTableLines(input, tableLines, limitBytes, plannedCount, func() int { return len(parts) + 1 }) {
				appendTableChunk(chunk)
			}
			i = next
			continue
		}
		appendNonTableLine(lines[i])
		i++
	}
	flushCurrent()
	if !tableSeen {
		return nil, false
	}
	if len(parts) == 0 {
		return []string{""}, true
	}
	return parts, true
}

func splitTeamsMarkdownTableLines(input TeamsRenderInput, lines []string, limitBytes int, plannedCount int, nextPartIndex func() int) []string {
	if len(lines) <= 2 {
		return []string{strings.Join(lines, "\n")}
	}
	fits := func(candidate string) bool {
		partInput := input
		partInput.Text = candidate
		return len(renderTeamsHTMLPart(partInput, nextPartIndex(), plannedCount)) <= limitBytes
	}
	full := strings.Join(lines, "\n")
	if fits(full) {
		return []string{full}
	}

	header := []string{lines[0], lines[1]}
	rows := lines[2:]
	var chunks []string
	current := append([]string{}, header...)
	flush := func() {
		if len(current) <= len(header) {
			return
		}
		chunks = append(chunks, strings.Join(current, "\n"))
		current = append([]string{}, header...)
	}
	for _, row := range rows {
		candidate := append(append([]string{}, current...), row)
		candidateText := strings.Join(candidate, "\n")
		if fits(candidateText) {
			current = candidate
			continue
		}
		flush()
		single := append(append([]string{}, header...), row)
		singleText := strings.Join(single, "\n")
		if fits(singleText) {
			current = single
			continue
		}
		chunks = append(chunks, splitTeamsRenderTextGeneric(input, singleText, limitBytes, plannedCount)...)
		current = append([]string{}, header...)
	}
	flush()
	if len(chunks) == 0 {
		return []string{full}
	}
	return chunks
}

func renderTeamsHTMLPart(input TeamsRenderInput, partIndex int, partCount int) string {
	label := teamsRenderLabel(input.Kind, partIndex, partCount)
	text := normalizeTeamsRenderTextForKind(input.Kind, input.Text)
	if input.Kind == TeamsRenderCode || input.Kind == TeamsRenderCommand {
		return "<p><strong>" + html.EscapeString(label) + ":</strong></p><pre><code>" + html.EscapeString(text) + "</code></pre>"
	}
	if teamsRenderKindUsesCodexMarkdown(input.Kind) {
		return renderTeamsHTMLCodexMarkdownAfterLabelBreak(label, text)
	}
	return renderTeamsHTMLParagraphs(label, text, "")
}

func teamsRenderLabel(kind TeamsRenderKind, partIndex int, partCount int) string {
	base := "🔧 Helper"
	switch kind {
	case TeamsRenderUser:
		base = "🧑‍💻 User"
	case TeamsRenderAssistant:
		base = "🤖 ✅ Codex answer"
	case TeamsRenderProgress:
		base = "🤖 ⏳ Codex status"
	case TeamsRenderHelper:
		base = "🔧 Helper"
	case TeamsRenderStatus:
		base = "🤖 ⏳ Codex status"
	case TeamsRenderCode:
		base = "💻 Code"
	case TeamsRenderCommand:
		base = "🤖 🛠️ Codex command"
	}
	if partCount > 1 {
		return base + " [part " + strconvItoa(partIndex) + "/" + strconvItoa(partCount) + "]"
	}
	return base
}

func normalizeTeamsRenderText(text string) string {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	return text
}

func normalizeTeamsRenderTextForKind(kind TeamsRenderKind, text string) string {
	text = normalizeTeamsRenderText(text)
	if kind == TeamsRenderCode || kind == TeamsRenderCommand {
		return text
	}
	if teamsRenderKindUsesCodexMarkdown(kind) {
		return compactTeamsRenderBlankLinesOutsideMarkdownFences(text)
	}
	return compactTeamsRenderBlankLines(text)
}

func teamsRenderKindUsesCodexMarkdown(kind TeamsRenderKind) bool {
	return kind == TeamsRenderAssistant || kind == TeamsRenderProgress || kind == TeamsRenderUser || kind == TeamsRenderHelper || kind == TeamsRenderStatus
}

func compactTeamsRenderBlankLines(text string) string {
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	blankSeen := false
	for _, line := range lines {
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
	return strings.TrimSpace(strings.Join(out, "\n"))
}

func renderTeamsHTMLParagraphs(label string, text string, firstPrefixHTML string) string {
	paragraphs := splitTeamsRenderParagraphs(text)
	var out strings.Builder
	for i, paragraph := range paragraphs {
		body := html.EscapeString(paragraph)
		body = strings.ReplaceAll(body, "\n", "<br>")
		out.WriteString("<p>")
		if i == 0 {
			out.WriteString("<strong>")
			out.WriteString(html.EscapeString(label))
			out.WriteString(":</strong>")
			if strings.TrimSpace(firstPrefixHTML) != "" {
				out.WriteString(" ")
				out.WriteString(firstPrefixHTML)
			}
			if body != "" {
				out.WriteString(" ")
				out.WriteString(body)
			}
		} else {
			out.WriteString(body)
		}
		out.WriteString("</p>")
	}
	return out.String()
}

func renderTeamsHTMLParagraphsAfterLabelBreak(label string, text string) string {
	paragraphs := splitTeamsRenderParagraphs(text)
	var out strings.Builder
	for i, paragraph := range paragraphs {
		body := html.EscapeString(paragraph)
		body = strings.ReplaceAll(body, "\n", "<br>")
		out.WriteString("<p>")
		if i == 0 {
			out.WriteString("<strong>")
			out.WriteString(html.EscapeString(label))
			out.WriteString(":</strong>")
			if body != "" {
				out.WriteString("<br>")
				out.WriteString(body)
			}
		} else {
			out.WriteString(body)
		}
		out.WriteString("</p>")
	}
	return out.String()
}

func splitTeamsRenderParagraphs(text string) []string {
	text = normalizeTeamsRenderText(text)
	lines := strings.Split(text, "\n")
	paragraphs := make([]string, 0, 1)
	current := make([]string, 0, len(lines))
	flush := func() {
		paragraph := strings.TrimSpace(strings.Join(current, "\n"))
		if paragraph != "" {
			paragraphs = append(paragraphs, paragraph)
		}
		current = current[:0]
	}
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			flush()
			continue
		}
		current = append(current, line)
	}
	flush()
	if len(paragraphs) == 0 {
		return []string{""}
	}
	return paragraphs
}

type OwnerMentionIntent string

const (
	OwnerMentionNone  OwnerMentionIntent = "none"
	OwnerMentionOwner OwnerMentionIntent = "owner"
)

type OwnerNotificationEvent string

const (
	OwnerNotificationACK         OwnerNotificationEvent = "ack"
	OwnerNotificationImport      OwnerNotificationEvent = "import"
	OwnerNotificationHistory     OwnerNotificationEvent = "history"
	OwnerNotificationStatus      OwnerNotificationEvent = "status"
	OwnerNotificationCompletion  OwnerNotificationEvent = "completion"
	OwnerNotificationInterrupted OwnerNotificationEvent = "interrupted"
	OwnerNotificationFailure     OwnerNotificationEvent = "failure"
)

type OwnerMentionPolicyInput struct {
	Event                OwnerNotificationEvent
	Duration             time.Duration
	LongRunningThreshold time.Duration
	AlreadyMentioned     bool
	PartIndex            int
	PartCount            int
}

func OwnerMentionIntentFor(input OwnerMentionPolicyInput) OwnerMentionIntent {
	if input.AlreadyMentioned {
		return OwnerMentionNone
	}
	if input.PartCount > 1 {
		if input.PartIndex != 1 {
			return OwnerMentionNone
		}
	}
	switch input.Event {
	case OwnerNotificationACK, OwnerNotificationImport, OwnerNotificationHistory, OwnerNotificationStatus:
		return OwnerMentionNone
	case OwnerNotificationCompletion:
		threshold := input.LongRunningThreshold
		if threshold <= 0 {
			threshold = defaultOwnerMentionThreshold
		}
		if input.Duration >= threshold {
			return OwnerMentionOwner
		}
		return OwnerMentionNone
	case OwnerNotificationInterrupted, OwnerNotificationFailure:
		return OwnerMentionOwner
	default:
		return OwnerMentionNone
	}
}

func ShouldMentionOwner(input OwnerMentionPolicyInput) bool {
	return OwnerMentionIntentFor(input) == OwnerMentionOwner
}
