package chats

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/steps"
)

// R-36 / R-1 / R-6: a final of a 200-row table plus ~100 KB of prose is
// split so that no Teams message exceeds 32 KiB of HTML, every table part
// repeats the header, parts are labelled and in order, nothing is lost or
// duplicated, and only one part mentions the owner.
func TestR36_LongAnswerIsChunked(t *testing.T) {
	harness.Parallel(t)
	s := harness.NewScenario(t, "R-36", harness.Options{
		Guarantees: []string{"R-36", "R-1", "R-2", "R-6"},
		Title:      "200-row table + 100 KB prose: parts <= 32 KiB, headers repeated, in order, complete, one mention",
		Budget:     120 * time.Second,
	})
	defer s.Finish()
	tok := s.Token("long")
	answer := longAnswer(tok)
	s.Codex.SetScript(func(c harness.CodexCall) harness.CodexBehavior {
		if c.HasToken(tok) {
			return harness.CodexBehavior{Answer: answer}
		}
		return harness.CodexBehavior{}
	})
	control := steps.ReadyControlChat(s)
	work, _ := steps.NewWorkChat(s, control, "r36")
	steps.MustPost(s, work.ID, "Write the long report "+tok)
	lastRow := fmt.Sprintf("ROW%03d-%s", 199, tok)
	lastProse := fmt.Sprintf("PROSE%04d-%s", proseParas-1, tok)
	s.WaitFor("all parts posted", 90*time.Second, func() (bool, string) {
		return len(s.SUTMessagesContaining(work.ID, lastRow)) > 0 && len(s.SUTMessagesContaining(work.ID, lastProse)) > 0, "last row/paragraph not visible yet"
	})
	s.Settle(3*time.Second, 15*time.Second)

	// Parts = SUT messages carrying any sentinel of this answer.
	var parts []int
	msgs := s.SUTMessages(work.ID)
	for i, m := range msgs {
		if strings.Contains(m.PlainText(), tok) && (strings.Contains(m.PlainText(), "ROW") || strings.Contains(m.PlainText(), "PROSE")) {
			parts = append(parts, i)
		}
	}
	maxBytes, over := 0, 0
	mentions, atTags := 0, 0
	headerMissing := 0
	var labels []string
	labelRE := regexp.MustCompile(`\[part (\d+)/(\d+)\]|\((\d+)/(\d+)\)`)
	var seq []string // sentinels in posting order
	sentRE := regexp.MustCompile(`(ROW\d{3}|PROSE\d{4})-` + regexp.QuoteMeta(tok))
	for _, i := range parts {
		m := msgs[i]
		if n := len(m.Content); n > maxBytes {
			maxBytes = n
		}
		if len(m.Content) > 32*1024 {
			over++
		}
		mentions += len(m.Mentions)
		atTags += strings.Count(m.Content, "<at ")
		text := m.PlainText()
		if strings.Contains(text, "ROW") && !strings.Contains(text, "HeaderAlpha") {
			headerMissing++
		}
		if l := labelRE.FindString(text); l != "" {
			labels = append(labels, l)
		}
		for _, sm := range sentRE.FindAllStringSubmatch(text, -1) {
			seq = append(seq, sm[1])
		}
	}
	s.Note("parts=%d largest=%d bytes labels=%v", len(parts), maxBytes, labels)
	s.Check("R-36 no part exceeds 32 KiB of HTML", len(parts) > 1 && over == 0, "every part <= 32768 bytes", fmt.Sprintf("%d parts, %d over, largest %d", len(parts), over, maxBytes))
	s.Check("R-36 every table part repeats the header", headerMissing == 0, "0 table parts without header", fmt.Sprint(headerMissing))
	tail := ""
	if len(parts) > 0 {
		c := msgs[parts[len(parts)-1]].Content
		tail = c[max(0, len(c)-300):]
	}
	s.Check("R-36 only one part mentions the owner", mentions == 1, "1 mention", fmt.Sprintf("%d mentions, %d <at> tags; last part ends: %q", mentions, atTags, tail))
	s.Check("R-36 parts are labelled", len(labels) == len(parts), "a part label on every part", fmt.Sprintf("%d labels for %d parts", len(labels), len(parts)))

	want := expectedSentinels()
	missing, dup := 0, 0
	count := map[string]int{}
	for _, x := range seq {
		count[x]++
	}
	for _, w := range want {
		switch count[w] {
		case 0:
			missing++
		case 1:
		default:
			dup++
		}
	}
	ordered := len(seq) == len(want)
	for i := range seq {
		if !ordered || seq[i] != want[i] {
			ordered = false
			break
		}
	}
	s.Check("R-1 nothing lost: every row and paragraph present", missing == 0, "0 missing of "+fmt.Sprint(len(want)), fmt.Sprint(missing))
	s.Check("R-2 nothing duplicated across parts", dup == 0, "0 duplicated", fmt.Sprint(dup))
	s.Check("R-6 de-chunked content is in the original order", ordered, "same order as the final", fmt.Sprintf("%d sentinels seen, in order=%v", len(seq), ordered))
}

const proseParas = 220

// longAnswer is a markdown final: a 200-row table, then ~100 KB of prose in
// numbered paragraphs.
func longAnswer(tok string) string {
	var b strings.Builder
	b.WriteString("Report for " + tok + "\n\n| HeaderAlpha | HeaderBeta | HeaderGamma |\n|---|---|---|\n")
	for i := 0; i < 200; i++ {
		fmt.Fprintf(&b, "| ROW%03d-%s | value %d | %s |\n", i, tok, i*7, strings.Repeat("x", 40))
	}
	b.WriteString("\n")
	filler := strings.Repeat("Lorem ipsum dolor sit amet, consectetur adipiscing elit. ", 8)
	for i := 0; i < proseParas; i++ {
		fmt.Fprintf(&b, "PROSE%04d-%s %s\n\n", i, tok, filler)
	}
	return b.String()
}

func expectedSentinels() []string {
	var out []string
	for i := 0; i < 200; i++ {
		out = append(out, fmt.Sprintf("ROW%03d", i))
	}
	for i := 0; i < proseParas; i++ {
		out = append(out, fmt.Sprintf("PROSE%04d", i))
	}
	return out
}
