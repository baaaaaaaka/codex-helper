package teams

import "strings"

// transcriptPlanOptions controls the small policy differences between the
// normal incremental importer and a fork snapshot. The filtering and dedupe
// implementation is shared so the two paths cannot silently drift again.
type transcriptPlanOptions struct {
	ForkVisibleOnly bool
}

// planTranscriptImportRecord applies the transcript visibility and dedupe
// rules used by publish-history and fork snapshots. It deliberately does not
// perform any persistence or delivery side effect.
func planTranscriptImportRecord(record TranscriptRecord, sourceLine int, sourceOffset int64, kindPrefix string, fallback int, dedupe *transcriptDedupeState, opts transcriptPlanOptions) (transcriptImportBatchRecord, string, bool, bool) {
	checkpointKey := transcriptRecordCheckpointKey(record)
	if record.Internal || record.Kind == TranscriptKindUnknown || strings.TrimSpace(record.Text) == "" {
		return transcriptImportBatchRecord{}, checkpointKey, false, false
	}
	if shouldSkipImportedTranscriptRecord(record) {
		return transcriptImportBatchRecord{}, checkpointKey, true, false
	}
	if opts.ForkVisibleOnly {
		switch record.Kind {
		case TranscriptKindUser, TranscriptKindAssistant, TranscriptKindStatus, TranscriptKindCompact:
		default:
			return transcriptImportBatchRecord{}, checkpointKey, false, false
		}
	}
	body := formatTranscriptRecordForTeams(record)
	if strings.TrimSpace(body) == "" {
		return transcriptImportBatchRecord{}, checkpointKey, false, false
	}
	if dedupe != nil && dedupe.shouldSkip(record, body) {
		return transcriptImportBatchRecord{}, checkpointKey, false, false
	}
	// Keep dedupe semantics tied to the parser's source position. The import
	// checkpoint position may intentionally move to the start of a JSONL line
	// when that line expands into multiple logical records; using that adjusted
	// position for adjacency would change the normal importer's duplicate rule.
	record.SourceLine = sourceLine
	record.SourceOffset = sourceOffset
	return transcriptImportBatchRecord{
		Record:        record,
		Kind:          transcriptRecordOutboxKind(kindPrefix, record, fallback),
		Body:          body,
		CheckpointKey: checkpointKey,
	}, checkpointKey, false, true
}

// transcriptHistoryBatch is the side-effect-free output of the shared batch
// planner. HTML is stored here because fork history items are immutable and
// must be re-sent exactly as planned after a restart.
type transcriptHistoryBatch struct {
	HTML      string
	First     TranscriptRecord
	Last      TranscriptRecord
	PartIndex int
	PartCount int
}

func renderTranscriptImportRecordHTML(record transcriptImportBatchRecord) string {
	return renderTeamsHTMLPart(TeamsRenderInput{
		Surface: TeamsRenderSurfaceOutbox,
		Kind:    renderKindForOutbox(record.Kind),
		Text:    record.Body,
	}, 1, 1)
}

func renderTranscriptImportBatchHTMLParts(parts []string) string {
	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, transcriptImportBatchSeparatorHTML)
}

// planTranscriptHistoryBatches mirrors the normal publish-history batch
// limits, but returns immutable plans instead of touching the store or Graph.
// A single oversized record is split with the same Teams chunk planner used by
// normal transcript delivery.
func planTranscriptHistoryBatches(records []transcriptImportBatchRecord) []transcriptHistoryBatch {
	if len(records) == 0 {
		return nil
	}
	planned := make([]transcriptHistoryBatch, 0)
	current := make([]transcriptImportBatchRecord, 0)
	currentHTML := make([]string, 0)
	currentBytes := 0
	flushCurrent := func() {
		if len(current) == 0 {
			return
		}
		planned = append(planned, transcriptHistoryBatch{
			HTML:      renderTranscriptImportBatchHTMLParts(currentHTML),
			First:     current[0].Record,
			Last:      current[len(current)-1].Record,
			PartIndex: 1,
			PartCount: 1,
		})
		current = nil
		currentHTML = nil
		currentBytes = 0
	}
	for _, record := range records {
		html := renderTranscriptImportRecordHTML(record)
		if len(html) > teamsChunkHTMLContentBytes {
			flushCurrent()
			chunks := PlanTeamsHTMLChunks(TeamsRenderInput{
				Surface: TeamsRenderSurfaceOutbox,
				Kind:    renderKindForOutbox(record.Kind),
				Text:    record.Body,
			}, TeamsRenderOptions{
				HardLimitBytes:   safeTeamsHTMLContentBytes,
				TargetLimitBytes: teamsChunkHTMLContentBytes,
			})
			for _, chunk := range chunks {
				chunkHTML := renderTeamsHTMLPart(TeamsRenderInput{
					Surface: TeamsRenderSurfaceOutbox,
					Kind:    renderKindForOutbox(record.Kind),
					Text:    chunk.Text,
				}, chunk.PartIndex, chunk.PartCount)
				planned = append(planned, transcriptHistoryBatch{
					HTML:      chunkHTML,
					First:     record.Record,
					Last:      record.Record,
					PartIndex: chunk.PartIndex,
					PartCount: chunk.PartCount,
				})
			}
			continue
		}
		addedBytes := len(html)
		if len(current) > 0 {
			addedBytes += len(transcriptImportBatchSeparatorHTML)
		}
		if len(current) > 0 && currentBytes+addedBytes > teamsChunkHTMLContentBytes {
			flushCurrent()
		}
		if len(current) > 0 {
			currentBytes += len(transcriptImportBatchSeparatorHTML)
		}
		current = append(current, record)
		currentHTML = append(currentHTML, html)
		currentBytes += len(html)
	}
	flushCurrent()
	return planned
}
