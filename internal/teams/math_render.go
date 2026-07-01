package teams

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"html"
	"strings"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type teamsMathHostedSendError struct{ err error }

func (e teamsMathHostedSendError) Error() string { return e.err.Error() }
func (e teamsMathHostedSendError) Unwrap() error { return e.err }

func shouldFallbackTeamsMathMediaError(err error) bool {
	var hostedErr teamsMathHostedSendError
	if !errors.As(err, &hostedErr) {
		return false
	}
	var statusErr *GraphStatusError
	if !errors.As(err, &statusErr) {
		return false
	}
	switch statusErr.StatusCode {
	case 400, 413, 415, 422:
		return true
	default:
		return false
	}
}

type teamsMathAsset struct {
	Index       int
	TemporaryID string
	PNG         validatedTeamsMathPNG
	Error       string
}

func (b *Bridge) renderOutboxMathAssets(ctx context.Context, outbox teamstore.OutboxMessage) ([]teamsMathAsset, []OutboundHostedContent) {
	if !outbox.TrustedMath || outbox.MathMediaFallback {
		return nil, nil
	}
	plan := trustedTeamsMathPlanForOutbox(outbox)
	if len(plan.Spans) == 0 {
		return nil, nil
	}
	renderer := b.mathRenderer
	if renderer == nil {
		renderer = teamsMathRenderer()
	}
	assets := renderer.Render(ctx, plan.Spans)
	hosted := make([]OutboundHostedContent, 0, len(assets))
	spanSource := make(map[int]string, len(plan.Spans))
	for _, span := range plan.Spans {
		spanSource[span.Index] = span.Source
	}
	hostedBySource := make(map[string]OutboundHostedContent, len(assets))
	for i := range assets {
		if len(assets[i].PNG) == 0 {
			continue
		}
		source, ok := spanSource[assets[i].Index]
		if !ok {
			assets[i].PNG = nil
			continue
		}
		if existing, ok := hostedBySource[source]; ok {
			assets[i].TemporaryID = existing.TemporaryID
			continue
		}
		assets[i].TemporaryID = teamsMathTemporaryID(outbox.ID, outbox.Body, assets[i].Index)
		item := OutboundHostedContent{
			TemporaryID:  assets[i].TemporaryID,
			Bytes:        assets[i].PNG,
			ContentType:  "image/png",
			validatedPNG: true,
		}
		hostedBySource[source] = item
		hosted = append(hosted, item)
	}
	return assets, hosted
}

func teamsMathTemporaryID(outboxID string, body string, index int) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(outboxID) + "\x00" + body + "\x00" + fmt.Sprintf("%d", index)))
	return "math-" + hex.EncodeToString(sum[:8])
}

func renderTeamsHTMLCodexMarkdownWithMathAfterLabelBreak(label string, text string, assets []teamsMathAsset) string {
	return renderTeamsHTMLCodexMarkdownWithMathPlanAfterLabelBreak(label, parseTrustedTeamsMath(text), assets)
}

func renderTeamsHTMLCodexMarkdownWithMathPlanAfterLabelBreak(label string, plan teamsMathPlan, assets []teamsMathAsset) string {
	text := plan.Text
	if len(plan.Spans) == 0 {
		return renderTeamsHTMLCodexMarkdownAfterLabelBreak(label, text)
	}
	assetByIndex := make(map[int]teamsMathAsset, len(assets))
	for _, asset := range assets {
		if asset.Index > 0 {
			assetByIndex[asset.Index] = asset
		}
	}

	var markdown strings.Builder
	last := 0
	tokens := make([]string, 0, len(plan.Spans))
	placeholderPrefix := teamsMathPlaceholderPrefix(text)
	tail := ""
	writeMarkdown := func(value string) {
		markdown.WriteString(value)
		tail += value
		if len(tail) > 2 {
			tail = tail[len(tail)-2:]
		}
	}
	tableMask := teamsMathMarkdownTableMask(text)
	for _, span := range plan.Spans {
		writeMarkdown(text[last:span.Start])
		token := fmt.Sprintf("%s%06d\ue001", placeholderPrefix, span.Index)
		tokens = append(tokens, token)
		insideTable := span.Start < len(tableMask) && tableMask[span.Start]
		if !insideTable {
			if markdown.Len() > 0 && tail != "\n\n" {
				writeMarkdown("\n\n")
			}
		}
		writeMarkdown(token)
		if !insideTable {
			writeMarkdown("\n\n")
		}
		last = span.End
	}
	writeMarkdown(text[last:])

	rendered := renderTeamsHTMLCodexMarkdownAfterLabelBreak(label, markdown.String())
	for i, span := range plan.Spans {
		formulaHTML := renderTeamsMathNodeHTML(span.Source, assetByIndex[span.Index])
		paragraphToken := "<p>" + tokens[i] + "</p>"
		if strings.Contains(rendered, paragraphToken) {
			rendered = strings.Replace(rendered, paragraphToken, formulaHTML, 1)
			continue
		}
		rendered = strings.Replace(rendered, tokens[i], formulaHTML, 1)
	}
	return rendered
}

func teamsMathPlaceholderPrefix(text string) string {
	sum := sha256.Sum256([]byte(text))
	base := "\ue000cxp-math-" + hex.EncodeToString(sum[:8])
	for counter := 0; ; counter++ {
		prefix := fmt.Sprintf("%s-%d-", base, counter)
		if !strings.Contains(text, prefix) {
			return prefix
		}
	}
}

func teamsMathMarkdownTableMask(text string) []bool {
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
		_, next, ok := parseTeamsMarkdownTableBlock(lines, i)
		if !ok {
			continue
		}
		start := offsets[i]
		end := offsets[next]
		if end > len(mask) {
			end = len(mask)
		}
		for j := start; j < end; j++ {
			mask[j] = true
		}
		i = next - 1
	}
	return mask
}

func plannedTeamsMathAssets(plan teamsMathPlan) []teamsMathAsset {
	assets := make([]teamsMathAsset, 0, len(plan.Spans))
	for _, span := range plan.Spans {
		assets = append(assets, teamsMathAsset{
			Index: span.Index, TemporaryID: "math-0000000000000000", PNG: []byte{1},
		})
	}
	return assets
}

func plannedTeamsMathHTMLLength(input TeamsRenderInput, partIndex int, partCount int) int {
	plan := parseTrustedTeamsMath(normalizeTeamsRenderTextForKind(input.Kind, input.Text))
	if len(plan.Spans) == 0 {
		return len(renderTeamsHTMLPart(input, partIndex, partCount))
	}
	label := teamsRenderLabel(input.Kind, partIndex, partCount)
	return len(renderTeamsHTMLCodexMarkdownWithMathPlanAfterLabelBreak(label, plan, plannedTeamsMathAssets(plan)))
}

func renderTeamsMathNodeHTML(source string, asset teamsMathAsset) string {
	var out strings.Builder
	out.WriteString("<pre><code>")
	out.WriteString(html.EscapeString(source))
	out.WriteString("</code></pre>")
	if asset.Index <= 0 || strings.TrimSpace(asset.TemporaryID) == "" || len(asset.PNG) == 0 {
		return out.String()
	}
	out.WriteString(`<p><img src="../hostedContents/`)
	out.WriteString(html.EscapeString(asset.TemporaryID))
	out.WriteString(`/$value" alt="Rendered TeX formula"></p>`)
	return out.String()
}

type teamsMathChunkUnit struct {
	text   string
	isMath bool
}

func splitTeamsRenderTextWithMath(input TeamsRenderInput, text string, limitBytes int, plannedCount int) ([]string, bool) {
	plan := parseTrustedTeamsMath(text)
	if len(plan.Spans) == 0 {
		return nil, false
	}
	units := make([]teamsMathChunkUnit, 0, len(plan.Spans)*2+1)
	last := 0
	for _, span := range plan.Spans {
		if span.Start > last {
			units = append(units, teamsMathChunkUnit{text: text[last:span.Start]})
		}
		units = append(units, teamsMathChunkUnit{text: text[span.Start:span.End], isMath: true})
		last = span.End
	}
	if last < len(text) {
		units = append(units, teamsMathChunkUnit{text: text[last:]})
	}

	fits := func(candidate string, partIndex int) bool {
		partInput := input
		partInput.Text = candidate
		return plannedTeamsMathHTMLLength(partInput, partIndex, plannedCount) <= limitBytes
	}
	var parts []string
	var current strings.Builder
	mathCount := 0
	flush := func() {
		if current.Len() == 0 {
			return
		}
		parts = append(parts, current.String())
		current.Reset()
		mathCount = 0
	}
	appendPlainOversize := func(value string) {
		plainInput := input
		plainInput.TrustedMath = false
		plainInput.Text = value
		plainParts, ok := splitTeamsRenderTextByLines(plainInput, value, limitBytes, plannedCount)
		if !ok {
			plainParts = splitTeamsRenderTextGeneric(plainInput, value, limitBytes, plannedCount)
		}
		for _, part := range plainParts {
			if part != "" {
				parts = append(parts, part)
			}
		}
	}

	for _, unit := range units {
		if unit.text == "" {
			continue
		}
		if unit.isMath && mathCount >= maxTeamsMathPerMessage {
			flush()
		}
		candidate := current.String() + unit.text
		if fits(candidate, len(parts)+1) {
			current.WriteString(unit.text)
			if unit.isMath {
				mathCount++
			}
			continue
		}
		flush()
		if fits(unit.text, len(parts)+1) {
			current.WriteString(unit.text)
			if unit.isMath {
				mathCount = 1
			}
			continue
		}
		appendPlainOversize(unit.text)
	}
	flush()
	if len(parts) == 0 {
		return []string{""}, true
	}
	return parts, true
}
