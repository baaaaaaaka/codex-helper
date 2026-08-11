package teams

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestPlanTranscriptImportRecordsUsesSharedDedupeRules(t *testing.T) {
	records := []TranscriptRecord{
		{ItemID: "user-1", Kind: TranscriptKindUser, Text: "same prompt"},
		{ItemID: "user-1-response", Kind: TranscriptKindUser, Text: "same prompt"},
		{ItemID: "user-2", Kind: TranscriptKindUser, Text: "same prompt"},
		{ItemID: "status-1", Kind: TranscriptKindStatus, Text: "same status"},
		{ItemID: "status-2", Kind: TranscriptKindStatus, Text: "same status"},
	}
	dedupe := newTranscriptDedupeState()
	planned := make([]transcriptImportBatchRecord, 0, len(records))
	for i, record := range records {
		line := i + 1
		if i == 2 {
			line = 42
		}
		record.SourceLine = line
		record.SourceOffset = int64(line)
		item, _, _, included := planTranscriptImportRecord(record, line, int64(line), "fork", i+1, dedupe, transcriptPlanOptions{ForkVisibleOnly: true})
		if included {
			planned = append(planned, item)
		}
	}
	if len(planned) != 3 {
		t.Fatalf("planned records = %d, want first user, later repeated user, and one status", len(planned))
	}
	if planned[0].Body != "same prompt" || planned[1].Body != "same prompt" || planned[2].Body != "same status" {
		t.Fatalf("planned bodies = %#v", planned)
	}
}

func TestPlanTranscriptImportRecordPreservesParserLineForAdjacentDedupe(t *testing.T) {
	dedupe := newTranscriptDedupeState()
	first, _, _, included := planTranscriptImportRecord(TranscriptRecord{
		ItemID:     "user-before",
		Kind:       TranscriptKindUser,
		Text:       "same prompt",
		SourceLine: 10,
	}, 10, 100, "sync", 1, dedupe, transcriptPlanOptions{})
	if !included {
		t.Fatal("first record was unexpectedly deduped")
	}
	second, _, _, included := planTranscriptImportRecord(TranscriptRecord{
		ItemID:     "user-after",
		Kind:       TranscriptKindUser,
		Text:       "same prompt",
		SourceLine: 14,
	}, 13, 130, "sync", 2, dedupe, transcriptPlanOptions{})
	if !included {
		t.Fatal("non-adjacent parser records were incorrectly deduped using the adjusted checkpoint line")
	}
	if first.Record.SourceLine != 10 || second.Record.SourceLine != 13 || second.Record.SourceOffset != 130 {
		t.Fatalf("planned source positions = %#v, %#v", first.Record, second.Record)
	}
}

func TestPlanTranscriptHistoryBatchesIsDeterministicAndCombinesRecords(t *testing.T) {
	records := make([]transcriptImportBatchRecord, 0, 8)
	for i := 0; i < 8; i++ {
		kind := TranscriptKindAssistant
		if i%2 == 0 {
			kind = TranscriptKindUser
		}
		record := TranscriptRecord{
			ItemID:       "record-" + string(rune('a'+i)),
			Kind:         kind,
			Text:         "history body " + string(rune('a'+i)),
			SourceLine:   i + 1,
			SourceOffset: int64(i + 1),
		}
		records = append(records, transcriptImportBatchRecord{
			Record: record,
			Kind:   transcriptRecordOutboxKind("fork", record, i+1),
			Body:   record.Text,
		})
	}
	first := planTranscriptHistoryBatches(records)
	second := planTranscriptHistoryBatches(records)
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("batch planning is not deterministic:\nfirst=%#v\nsecond=%#v", first, second)
	}
	if len(first) != 1 {
		t.Fatalf("batch count = %d for %d small records, want exactly one combined batch", len(first), len(records))
	}
	if !strings.Contains(first[0].HTML, "history body") {
		t.Fatalf("first batch HTML = %#v", first)
	}
	for i := 0; i < len(records); i++ {
		body := "history body " + string(rune('a'+i))
		if strings.Count(first[0].HTML, body) != 1 {
			t.Fatalf("source body %q occurs %d times in combined batch: %q", body, strings.Count(first[0].HTML, body), first[0].HTML)
		}
	}
	for i, batch := range first {
		if strings.TrimSpace(batch.HTML) == "" || batch.PartIndex <= 0 || batch.PartCount <= 0 {
			t.Fatalf("batch %d = %#v", i, batch)
		}
	}
}

func TestPlanTranscriptHistoryBatchesSplitsOversizedUnicodeRecord(t *testing.T) {
	record := TranscriptRecord{
		ItemID:       "oversized-record",
		Kind:         TranscriptKindAssistant,
		Text:         strings.Repeat("界", 20000),
		SourceLine:   7,
		SourceOffset: 99,
	}
	planned := planTranscriptHistoryBatches([]transcriptImportBatchRecord{{
		Record: record,
		Kind:   "fork-history-assistant",
		Body:   record.Text,
	}, {
		Record: TranscriptRecord{ItemID: "after-oversized", Kind: TranscriptKindUser, Text: "after oversized"},
		Kind:   "fork-history-user",
		Body:   "after oversized",
	}})
	if len(planned) < 2 {
		t.Fatalf("oversized record produced %d batch(es), want multiple parts", len(planned))
	}
	partCount := 0
	for _, part := range planned {
		if part.First.ItemID == record.ItemID {
			partCount++
		}
	}
	if partCount < 2 {
		t.Fatalf("oversized record produced %d parts, want multiple parts", partCount)
	}
	joinedHTML := strings.Builder{}
	for i, part := range planned {
		if part.First.ItemID == record.ItemID && (part.PartIndex <= 0 || part.PartCount != partCount) {
			t.Fatalf("part %d metadata = %#v, want %d/%d", i, part, i+1, partCount)
		}
		if len(part.HTML) > safeTeamsHTMLContentBytes {
			t.Fatalf("part %d rendered bytes = %d, want <= %d", i, len(part.HTML), safeTeamsHTMLContentBytes)
		}
		joinedHTML.WriteString(part.HTML)
	}
	if got := strings.Count(joinedHTML.String(), "界"); got != strings.Count(record.Text, "界") {
		t.Fatalf("oversized Unicode content count = %d, want %d", got, strings.Count(record.Text, "界"))
	}
	if strings.Count(joinedHTML.String(), "after oversized") != 1 {
		t.Fatalf("record after oversized was lost or duplicated: %q", joinedHTML.String())
	}
	last := planned[len(planned)-1]
	if last.First.ItemID != "after-oversized" || last.Last.ItemID != "after-oversized" {
		t.Fatalf("record after oversized ended with source range %#v..%#v", last.First, last.Last)
	}
}

func BenchmarkPlanTranscriptHistoryBatches(b *testing.B) {
	records := make([]transcriptImportBatchRecord, 0, 512)
	for i := 0; i < 512; i++ {
		record := TranscriptRecord{
			ItemID:       fmt.Sprintf("benchmark-record-%d", i),
			Kind:         TranscriptKindAssistant,
			Text:         "benchmark transcript body with a stable amount of text",
			SourceLine:   i + 1,
			SourceOffset: int64(i + 1),
		}
		records = append(records, transcriptImportBatchRecord{
			Record: record,
			Kind:   "fork-history-assistant",
			Body:   record.Text,
		})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = planTranscriptHistoryBatches(records)
	}
}
