package teams

import (
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestCodexOutputSymbolStressSurvivesTeamsCitationFilter(t *testing.T) {
	for i, payload := range codexOutputSymbolStressCorpus() {
		t.Run(fmt.Sprintf("case-%02d", i), func(t *testing.T) {
			marker := fmt.Sprintf("CASE_%02d_VISIBLE", i)
			visible := marker + " " + payload
			raw := visible + codexOutputStressCitationBlock()
			cleaned := StripOAIMemoryCitationBlocks(raw)
			if !sameCodexVisibleText(visible, raw) {
				t.Fatalf("sameCodexVisibleText() = false\nvisible=%q\nraw=%q\ncleaned=%q", visible, raw, cleaned)
			}
			for _, forbidden := range []string{"HIDDEN_MEMORY_LINE", "MEMORY.md", "citation_entries", "rollout_ids"} {
				if strings.Contains(cleaned, forbidden) {
					t.Fatalf("cleaned text leaked %q:\n%s", forbidden, cleaned)
				}
			}

			transcript := formatTranscriptRecordForTeams(TranscriptRecord{
				Kind: TranscriptKindAssistant,
				Text: raw,
			})
			if !strings.Contains(transcript, marker) {
				t.Fatalf("transcript lost marker %q:\n%s", marker, transcript)
			}
			for _, forbidden := range []string{"HIDDEN_MEMORY_LINE", "MEMORY.md", "citation_entries", "rollout_ids"} {
				if strings.Contains(transcript, forbidden) {
					t.Fatalf("transcript leaked %q:\n%s", forbidden, transcript)
				}
			}

			html := renderFinalOutboxBodyHTML(teamstore.OutboxMessage{Kind: "final", Body: raw})
			assertTeamsRenderedHTMLSafe(t, html)
			plain := PlainTextFromTeamsHTML(html)
			if !strings.Contains(plain, marker) {
				t.Fatalf("rendered final lost marker %q:\nplain=%s\nhtml=%s", marker, plain, html)
			}
			for _, forbidden := range []string{"HIDDEN_MEMORY_LINE", "MEMORY.md", "citation_entries", "rollout_ids"} {
				if strings.Contains(plain, forbidden) || strings.Contains(html, forbidden) {
					t.Fatalf("rendered final leaked %q:\nplain=%s\nhtml=%s", forbidden, plain, html)
				}
			}
		})
	}
}

func TestStripOAIMemoryCitationBlocksPreservesLiteralMultilineTagStress(t *testing.T) {
	visible := strings.Join([]string{
		"visible literal mention <oai-mem-citation>",
		"this line must stay; it is not a metadata payload",
		"literal closing mention </oai-mem-citation> should also stay",
		"",
		"standalone literal tag follows:",
		"<oai-mem-citation>",
		"still visible because the next line is prose, not a citation payload",
		"</oai-mem-citation>",
	}, "\n")
	raw := visible + "\n" + codexOutputStressCitationBlock()
	got := StripOAIMemoryCitationBlocks(raw)
	for _, want := range []string{
		"visible literal mention <oai-mem-citation>",
		"this line must stay",
		"literal closing mention </oai-mem-citation>",
		"standalone literal tag follows:",
		"<oai-mem-citation>",
		"still visible because the next line is prose",
		"</oai-mem-citation>",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("stripped text missing visible literal %q:\n%s", want, got)
		}
	}
	for _, forbidden := range []string{"HIDDEN_MEMORY_LINE", "MEMORY.md", "citation_entries", "rollout_ids"} {
		if strings.Contains(got, forbidden) {
			t.Fatalf("stripped text leaked %q:\n%s", forbidden, got)
		}
	}
}

func TestPlanTeamsHTMLChunksUnicodeStressKeepsMarkersAndLimits(t *testing.T) {
	text := strings.Join([]string{
		"UNICODE_START_MARKER",
		strings.Repeat(strings.Join(codexOutputSymbolStressCorpus(), "\n"), 48),
		"UNICODE_MIDDLE_MARKER",
		strings.Repeat("👩🏽‍💻 العربية עברית ภาษาไทย क्षि e\u0301 \u202eabc\u202c ＡＢＣ１２３ 《「【】」》\n", 160),
		"UNICODE_END_MARKER",
		codexOutputStressCitationBlock(),
	}, "\n")
	chunks := PlanTeamsHTMLChunks(TeamsRenderInput{
		Surface: TeamsRenderSurfaceOutbox,
		Kind:    TeamsRenderAssistant,
		Text:    StripOAIMemoryCitationBlocks(text),
	}, TeamsRenderOptions{
		TargetLimitBytes: 900,
		HardLimitBytes:   1200,
	})
	expectedText := normalizeTeamsRenderTextForKind(TeamsRenderAssistant, StripOAIMemoryCitationBlocks(text))
	requireTeamsChunkTextConservation(t, expectedText, chunks)
	if len(chunks) < 4 {
		t.Fatalf("expected unicode stress input to split into several chunks, got %d", len(chunks))
	}
	var plain strings.Builder
	for i, chunk := range chunks {
		if chunk.PartIndex != i+1 || chunk.PartCount != len(chunks) {
			t.Fatalf("chunk %d metadata = %d/%d, want %d/%d", i, chunk.PartIndex, chunk.PartCount, i+1, len(chunks))
		}
		if chunk.ByteLength != len(chunk.HTML) || chunk.ByteLength > 1200 {
			t.Fatalf("chunk %d bytes = %d len=%d", i, chunk.ByteLength, len(chunk.HTML))
		}
		if !utf8.ValidString(chunk.Text) || !utf8.ValidString(chunk.HTML) {
			t.Fatalf("chunk %d contains invalid UTF-8", i)
		}
		assertTeamsRenderedHTMLSafe(t, chunk.HTML)
		for _, forbidden := range []string{"HIDDEN_MEMORY_LINE", "MEMORY.md", "citation_entries", "rollout_ids", "\r"} {
			if strings.Contains(chunk.Text, forbidden) || strings.Contains(chunk.HTML, forbidden) {
				t.Fatalf("chunk %d leaked forbidden %q:\ntext=%s\nhtml=%s", i, forbidden, chunk.Text, chunk.HTML)
			}
		}
		plain.WriteString(PlainTextFromTeamsHTML(chunk.HTML))
		plain.WriteByte('\n')
	}
	joined := plain.String()
	for _, want := range []string{"UNICODE_START_MARKER", "UNICODE_MIDDLE_MARKER", "UNICODE_END_MARKER", "👩", "العربية", "עברית", "ภาษาไทย", "क्षि", "ＡＢＣ１２３"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("chunked unicode plain text missing %q in:\n%s", want, joined)
		}
	}
}

func TestPlanTeamsHTMLChunksTextConservationStressCorpus(t *testing.T) {
	for i, payload := range codexOutputSymbolStressCorpus() {
		t.Run(fmt.Sprintf("case-%02d", i), func(t *testing.T) {
			raw := strings.Join([]string{
				fmt.Sprintf("TEXT_CONSERVATION_START_%02d", i),
				payload,
				fmt.Sprintf("TEXT_CONSERVATION_END_%02d", i),
				codexOutputStressCitationBlock(),
			}, "\n")
			cleaned := StripOAIMemoryCitationBlocks(raw)
			expectedText := normalizeTeamsRenderTextForKind(TeamsRenderAssistant, cleaned)
			if codexOutputStressHasMarkdownTable(expectedText) {
				t.Skip("table chunking may intentionally duplicate table headers")
			}

			chunks := PlanTeamsHTMLChunks(TeamsRenderInput{
				Surface: TeamsRenderSurfaceOutbox,
				Kind:    TeamsRenderAssistant,
				Text:    cleaned,
			}, TeamsRenderOptions{
				TargetLimitBytes: 120,
				HardLimitBytes:   180,
			})
			requireTeamsChunkTextConservation(t, expectedText, chunks)
		})
	}
}

func TestPlanTeamsHTMLChunksMarkdownTableKeepsEveryRow(t *testing.T) {
	lines := []string{
		"TABLE_CONTENT_START",
		"| item | value |",
		"| --- | --- |",
	}
	var markers []string
	for i := 0; i < 80; i++ {
		marker := fmt.Sprintf("TABLE_ROW_%03d_KEEP", i)
		markers = append(markers, marker)
		lines = append(lines, fmt.Sprintf("| %s | 中文 emoji 👩🏽‍💻 rtl العربية value %03d |", marker, i))
	}
	lines = append(lines, "TABLE_CONTENT_END")
	raw := strings.Join(lines, "\n") + "\n" + codexOutputStressCitationBlock()
	cleaned := StripOAIMemoryCitationBlocks(raw)
	chunks := PlanTeamsHTMLChunks(TeamsRenderInput{
		Surface: TeamsRenderSurfaceOutbox,
		Kind:    TeamsRenderAssistant,
		Text:    cleaned,
	}, TeamsRenderOptions{
		TargetLimitBytes: 700,
		HardLimitBytes:   900,
	})
	if len(chunks) < 2 {
		t.Fatalf("expected table stress input to split, got %d chunk(s)", len(chunks))
	}
	var chunkText strings.Builder
	var chunkPlain strings.Builder
	for _, chunk := range chunks {
		chunkText.WriteString(chunk.Text)
		chunkText.WriteByte('\n')
		chunkPlain.WriteString(PlainTextFromTeamsHTML(chunk.HTML))
		chunkPlain.WriteByte('\n')
	}
	for _, marker := range append([]string{"TABLE_CONTENT_START", "TABLE_CONTENT_END"}, markers...) {
		if !strings.Contains(chunkText.String(), marker) {
			t.Fatalf("chunk text lost table marker %q", marker)
		}
		if !strings.Contains(chunkPlain.String(), marker) {
			t.Fatalf("chunk HTML plain text lost table marker %q", marker)
		}
	}
}

func FuzzStripOAIMemoryCitationBlocksKeepsVisibleMarker(f *testing.F) {
	for _, seed := range codexOutputSymbolStressCorpus() {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, payload string) {
		visible := "FUZZ_VISIBLE_MARKER " + payload
		got := StripOAIMemoryCitationBlocks(visible + codexOutputStressCitationBlock())
		if !strings.Contains(got, "FUZZ_VISIBLE_MARKER") {
			t.Fatalf("stripped text lost visible marker:\n%s", got)
		}
		for _, forbidden := range []string{"HIDDEN_MEMORY_LINE", "MEMORY.md", "citation_entries", "rollout_ids"} {
			if strings.Contains(got, forbidden) {
				t.Fatalf("stripped text leaked %q:\n%s", forbidden, got)
			}
		}
	})
}

func FuzzTeamsCodexOutputRenderAndCitationFilter(f *testing.F) {
	for _, seed := range codexOutputSymbolStressCorpus() {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, payload string) {
		started := time.Now()
		if len(payload) > 4096 {
			payload = payload[:4096]
		}
		for _, forbidden := range codexOutputStressForbiddenMetadata() {
			if strings.Contains(payload, forbidden) {
				return
			}
		}

		raw := "FUZZ_RENDER_VISIBLE_MARKER\n" + payload + codexOutputStressCitationBlock()
		cleaned := StripOAIMemoryCitationBlocks(raw)
		if !strings.Contains(cleaned, "FUZZ_RENDER_VISIBLE_MARKER") {
			t.Fatalf("cleaned output lost visible marker:\n%s", cleaned)
		}
		requireNoCodexOutputStressMetadataLeak(t, "cleaned output", cleaned)

		transcript := formatTranscriptRecordForTeams(TranscriptRecord{
			Kind: TranscriptKindAssistant,
			Text: raw,
		})
		if !strings.Contains(transcript, "FUZZ_RENDER_VISIBLE_MARKER") {
			t.Fatalf("transcript lost visible marker:\n%s", transcript)
		}
		requireNoCodexOutputStressMetadataLeak(t, "transcript", transcript)

		html := renderFinalOutboxBodyHTML(teamstore.OutboxMessage{Kind: "final", Body: raw})
		if !utf8.ValidString(html) {
			t.Fatalf("final HTML is invalid UTF-8")
		}
		assertTeamsRenderedHTMLSafe(t, html)
		plain := PlainTextFromTeamsHTML(html)
		if !strings.Contains(plain, "FUZZ_RENDER_VISIBLE_MARKER") {
			t.Fatalf("rendered final lost visible marker:\nplain=%s\nhtml=%s", plain, html)
		}
		requireNoCodexOutputStressMetadataLeak(t, "rendered final plain", plain)
		requireNoCodexOutputStressMetadataLeak(t, "rendered final html", html)

		chunks := PlanTeamsHTMLChunks(TeamsRenderInput{
			Surface: TeamsRenderSurfaceOutbox,
			Kind:    TeamsRenderAssistant,
			Text:    cleaned,
		}, TeamsRenderOptions{TargetLimitBytes: 700, HardLimitBytes: 900})
		if len(chunks) == 0 {
			t.Fatalf("chunk planner returned no chunks")
		}
		expectedChunkText := normalizeTeamsRenderTextForKind(TeamsRenderAssistant, cleaned)
		if !codexOutputStressHasMarkdownTable(expectedChunkText) {
			requireTeamsChunkTextConservation(t, expectedChunkText, chunks)
		}
		var chunkPlain strings.Builder
		for i, chunk := range chunks {
			if chunk.ByteLength != len(chunk.HTML) || chunk.ByteLength > 900 {
				t.Fatalf("chunk %d bytes = %d len=%d", i, chunk.ByteLength, len(chunk.HTML))
			}
			if !utf8.ValidString(chunk.Text) || !utf8.ValidString(chunk.HTML) {
				t.Fatalf("chunk %d contains invalid UTF-8", i)
			}
			assertTeamsRenderedHTMLSafe(t, chunk.HTML)
			requireNoCodexOutputStressMetadataLeak(t, fmt.Sprintf("chunk %d text", i), chunk.Text)
			requireNoCodexOutputStressMetadataLeak(t, fmt.Sprintf("chunk %d html", i), chunk.HTML)
			chunkPlain.WriteString(PlainTextFromTeamsHTML(chunk.HTML))
			chunkPlain.WriteByte('\n')
		}
		if !strings.Contains(chunkPlain.String(), "FUZZ_RENDER_VISIBLE_MARKER") {
			t.Fatalf("chunked render lost visible marker:\n%s", chunkPlain.String())
		}
		if elapsed := time.Since(started); elapsed > 3*time.Second {
			t.Fatalf("render/citation/chunk processing took %s for payload length %d", elapsed, len(payload))
		}
	})
}

func requireTeamsChunkTextConservation(t *testing.T, expected string, chunks []TeamsRenderedChunk) {
	t.Helper()
	var got strings.Builder
	for _, chunk := range chunks {
		got.WriteString(chunk.Text)
	}
	if got.String() != expected {
		t.Fatalf("chunk text did not conserve expected content\nwant: %q\n got: %q", expected, got.String())
	}
}

func codexOutputStressHasMarkdownTable(text string) bool {
	lines := strings.Split(text, "\n")
	for i := 0; i < len(lines); i++ {
		if _, _, ok := parseTeamsMarkdownTableBlock(lines, i); ok {
			return true
		}
	}
	return false
}

func codexOutputStressCitationBlock() string {
	return strings.Join([]string{
		"<oai-mem-citation>",
		"<citation_entries>",
		"MEMORY.md:1-3|note=[HIDDEN_MEMORY_LINE]",
		"</citation_entries>",
		"<rollout_ids>",
		"019d4393-5109-7b10-b5c2-05b2fe8635ba",
		"</rollout_ids>",
		"</oai-mem-citation>",
	}, "\n")
}

func codexOutputStressForbiddenMetadata() []string {
	return []string{"HIDDEN_MEMORY_LINE", "MEMORY.md", "citation_entries", "rollout_ids"}
}

func requireNoCodexOutputStressMetadataLeak(t *testing.T, label string, text string) {
	t.Helper()
	for _, forbidden := range codexOutputStressForbiddenMetadata() {
		if strings.Contains(text, forbidden) {
			t.Fatalf("%s leaked %q:\n%s", label, forbidden, text)
		}
	}
}

func codexOutputSymbolStressCorpus() []string {
	return []string{
		"letters abc ABC xyz XYZ numbers 0123456789",
		"punctuation ! ? . , ; : @ # $ % ^ & * _ - + = | / \\ ~",
		"paired ascii () [] {} <> \"double quoted\" 'single quoted' `backtick quoted`",
		"nested pairs ([{<alpha attr=\"x&y\">value</alpha>}]) and mirrored }])>",
		"markdown pairs **bold** __strong__ *em* _em_ ~~gone~~ `code <tag>`",
		"fence markers ``` ``` ~~~ ~~~ and inline pipes | a | b |",
		"html-like <script>alert('x')</script> &lt;escaped&gt; & raw ampersand",
		"links [label](https://example.com/a_(b)?x=1&y=2) <https://example.test/raw>",
		"json-ish {\"key\":[1,true,null],\"tag\":\"<oai-mem-citation> literal\"}",
		"unicode 中文 日本語 한글 café e\u0301 ✅ ←→",
		"path-ish /tmp/a b.go:12 C:\\Users\\Jason\\file.go:9 ./relative#L1",
		"combining marks a\u0301 e\u0301 n\u0303 Z\u0351\u0357\u035b and devanagari क्षि",
		"emoji zwj 👩🏽‍💻 👨‍👩‍👧‍👦 🏳️‍🌈 🧑‍🚀 1️⃣ *️⃣ #️⃣",
		"rtl scripts العربية עברית فارسی \u202eabc\u202c \u2066ltr isolate\u2069",
		"fullwidth ＡＢＣ１２３（）［］｛｝＜＞＂＇｀，。！？：；",
		"cjk punctuation 「」『』【】（）《》〈〉、。・…〜",
		"line separators first\u2028second\u2029third",
		"crlf text line1\r\nline2\rline3",
		"ansi-like escape \x1b[31mred\x1b[0m stays text",
		"zero width A\u200bB\u200cC\u200dD\u2060E and bom \ufeffinside",
		"thai and indic ภาษาไทย สวัสดี हिन्दी मराठी বাংলা தமிழ்",
	}
}
