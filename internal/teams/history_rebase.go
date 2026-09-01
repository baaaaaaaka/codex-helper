package teams

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// Codex's canonical rollout writer bounds individual JSONL records, but a
// history can be arbitrarily large. Rebase therefore reads at most one line at
// a time and never materializes the complete rollout. Keep this guard above the
// canonical per-line limit so a malformed/future source cannot make recovery
// allocate an unbounded buffer.
const historyRebaseMaxLineBytes = 64 * 1024 * 1024

// A source-rewrite recovery scan is cold work. Keep each listener pass
// resumable so a large, otherwise valid rollout cannot monopolize the entire
// history-watch phase. The durable checkpoint may retain the bounded scan
// cursor, but it remains blocked until a complete, source-bound anchor is
// found.
const historyRebaseMaxScanBytesPerPass int64 = 4 * 1024 * 1024

type codexHistoryHeader struct {
	ThreadID    string
	HistoryMode string
}

type codexHistoryFile struct {
	Header   codexHistoryHeader
	Info     os.FileInfo
	Identity string
}

type historyWatchRebaseAnchor struct {
	Record       TranscriptRecord
	CursorLine   int
	CursorOffset int64
	Info         os.FileInfo
	Identity     string
}

type historyWatchRebaseScanProgress struct {
	SourceIdentity string
	Offset         int64
	Line           int
	State          transcriptParseState
}

type historyWatchRebaseAnchorScanResult struct {
	Anchor   historyWatchRebaseAnchor
	Found    bool
	Complete bool
	Progress historyWatchRebaseScanProgress
}

// historyRewriteRecoverySnapshotMatches is the second half of the automatic
// rebase retry key. SourceFileIdentity deliberately stays stable across
// append-only writes (and can stay stable after an in-place repair), so a
// recovery marker containing only the inode would permanently hide a newly
// repairable source. Conversely, an unchanged marker must keep blocked legacy
// rows out of the cold rebase scanner on every poll.
func historyRewriteRecoverySnapshotMatches(state historyTieredFileState, info os.FileInfo) bool {
	if info == nil || info.IsDir() || strings.TrimSpace(state.SourceRewriteRecoveryIdentity) == "" {
		return false
	}
	if state.SourceRewriteRecoverySize <= 0 || state.SourceRewriteRecoverySize != info.Size() ||
		state.SourceRewriteRecoveryModTime.IsZero() || !state.SourceRewriteRecoveryModTime.Equal(info.ModTime()) {
		return false
	}
	if state.SourceRewriteRecoveryChangeTime != 0 &&
		teamstore.SourceFileChangeTimeFromFileInfo(info) != state.SourceRewriteRecoveryChangeTime {
		return false
	}
	return true
}

// readCodexHistoryHeader only reads the first JSONL record. Invalid or
// unrecognized headers are treated as ineligible for automatic migration; the
// existing explicit recovery path remains available for those files.
func readCodexHistoryHeader(path string) (codexHistoryHeader, bool, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return codexHistoryHeader{}, false, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return codexHistoryHeader{}, false, err
	}
	defer f.Close()
	reader := bufio.NewReaderSize(f, 64*1024)
	read, err := historyTieredReadJSONLRecord(reader, historyRebaseMaxLineBytes, historyRebaseMaxLineBytes)
	if read.BytesRead == 0 {
		if err == io.EOF {
			return codexHistoryHeader{}, false, nil
		}
		return codexHistoryHeader{}, false, err
	}
	if !read.Complete || read.Oversized {
		return codexHistoryHeader{}, false, nil
	}
	line := read.Line
	var envelope struct {
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}
	if json.Unmarshal(bytes.TrimSpace(line), &envelope) != nil || strings.TrimSpace(envelope.Type) != "session_meta" {
		return codexHistoryHeader{}, false, nil
	}
	var payload map[string]json.RawMessage
	if json.Unmarshal(envelope.Payload, &payload) != nil {
		return codexHistoryHeader{}, false, nil
	}
	header := codexHistoryHeader{
		ThreadID:    strings.TrimSpace(jsonStringField(payload, "id", "thread_id", "session_id", "sessionId")),
		HistoryMode: strings.ToLower(strings.TrimSpace(jsonStringField(payload, "history_mode"))),
	}
	return header, true, nil
}

func codexPaginatedHistoryFile(path string, expectedThreadID string) (codexHistoryFile, bool, error) {
	header, ok, err := readCodexHistoryHeader(path)
	if err != nil || !ok || header.HistoryMode != "paginated" {
		return codexHistoryFile{}, false, err
	}
	expectedThreadID = strings.TrimSpace(expectedThreadID)
	if expectedThreadID == "" || header.ThreadID == "" || header.ThreadID != expectedThreadID {
		return codexHistoryFile{}, false, nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return codexHistoryFile{}, false, err
	}
	if info.IsDir() {
		return codexHistoryFile{}, false, nil
	}
	identity, err := teamstore.SourceFileIdentityFromFileInfo(path, info)
	if err != nil || strings.TrimSpace(identity) == "" {
		return codexHistoryFile{}, false, err
	}
	return codexHistoryFile{Header: header, Info: info, Identity: strings.TrimSpace(identity)}, true, nil
}

func codexPaginatedHistoryIdentity(path string, expectedThreadID string) (string, bool) {
	file, ok, err := codexPaginatedHistoryFile(path, expectedThreadID)
	if err != nil || !ok {
		return "", false
	}
	return file.Identity, true
}

// historyWatchRebaseAnchorScan locates the old HistoryWatch final without
// parsing the complete file into memory. A stable file identity is required on
// both sides of the scan so an atomic replacement racing this lookup fails
// closed instead of producing a cursor for a different source.  The scan is
// resumable across listener cycles, and checks its context between complete
// JSONL records; a slow source therefore yields the phase to other paths.
func historyWatchRebaseAnchorScan(ctx context.Context, path string, previous historyTieredFileState, source codexHistoryFile, progress historyWatchRebaseScanProgress) (historyWatchRebaseAnchorScanResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(progress.SourceIdentity) != strings.TrimSpace(source.Identity) || progress.Offset < 0 {
		progress = historyWatchRebaseScanProgress{
			SourceIdentity: strings.TrimSpace(source.Identity),
			State: transcriptParseState{
				sessionID: strings.TrimSpace(previous.SessionID),
				threadID:  strings.TrimSpace(previous.ThreadID),
				turnID:    strings.TrimSpace(previous.TurnID),
			},
		}
	}
	result := historyWatchRebaseAnchorScanResult{Progress: progress}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	f, err := os.Open(path)
	if err != nil {
		return result, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return result, err
	}
	if info.IsDir() || !os.SameFile(source.Info, info) {
		return result, fmt.Errorf("history source %q changed before rebase scan", path)
	}
	if progress.Offset > info.Size() {
		return result, fmt.Errorf("history source %q is shorter than its rebase scan cursor", path)
	}
	if progress.Offset > 0 {
		if _, err := f.Seek(progress.Offset, io.SeekStart); err != nil {
			return result, err
		}
	}
	reader := bufio.NewReaderSize(f, 64*1024)
	state := progress.State
	offset := progress.Offset
	lineNo := progress.Line
	scanStart := offset
	for {
		if err := ctx.Err(); err != nil {
			result.Progress = historyWatchRebaseScanProgress{SourceIdentity: strings.TrimSpace(source.Identity), Offset: offset, Line: lineNo, State: state}
			return result, err
		}
		if offset > scanStart && offset-scanStart >= historyRebaseMaxScanBytesPerPass {
			if stableErr := historyRebaseStableSource(path, info, source.Identity); stableErr != nil {
				return result, stableErr
			}
			result.Progress = historyWatchRebaseScanProgress{SourceIdentity: strings.TrimSpace(source.Identity), Offset: offset, Line: lineNo, State: state}
			return result, nil
		}
		read, readErr := historyTieredReadJSONLRecord(reader, historyRebaseMaxLineBytes, historyRebaseMaxLineBytes)
		if err := ctx.Err(); err != nil {
			result.Progress = historyWatchRebaseScanProgress{SourceIdentity: strings.TrimSpace(source.Identity), Offset: offset, Line: lineNo, State: state}
			return result, err
		}
		complete := read.Complete || (readErr == io.EOF && read.BytesRead > 0)
		if read.BytesRead > 0 {
			line := read.Line
			if read.Oversized || !complete {
				if stableErr := historyRebaseStableSource(path, info, source.Identity); stableErr != nil {
					return result, stableErr
				}
				result.Complete = true
				result.Progress = historyWatchRebaseScanProgress{SourceIdentity: strings.TrimSpace(source.Identity), Offset: offset, Line: lineNo, State: state}
				return result, nil
			}
			lineStart := offset
			nextOffset := offset + read.BytesRead
			lineNo++
			trimmed := bytes.TrimSpace(line)
			if len(trimmed) > 0 {
				records, diagnostics := parseTranscriptLine(trimmed, lineNo, &state)
				if len(diagnostics) > 0 {
					if stableErr := historyRebaseStableSource(path, info, source.Identity); stableErr != nil {
						return result, stableErr
					}
					result.Complete = true
					result.Progress = historyWatchRebaseScanProgress{SourceIdentity: strings.TrimSpace(source.Identity), Offset: offset, Line: lineNo, State: state}
					return result, nil
				}
				for i := range records {
					records[i].SourceStartOffset = lineStart
					records[i].SourceOffset = nextOffset
					if !historyWatchRebaseAnchorMatches(records[i], previous) {
						continue
					}
					cursorLine, cursorOffset := lineNo, nextOffset
					// A future parser may emit more than one logical record from a
					// physical JSONL line. Re-read the whole line rather than skip
					// any record after the anchor.
					if i+1 < len(records) {
						cursorLine, cursorOffset = lineNo-1, lineStart
					}
					if historyRebaseStableSource(path, info, source.Identity) != nil {
						return result, nil
					}
					result.Anchor = historyWatchRebaseAnchor{
						Record:       records[i],
						CursorLine:   cursorLine,
						CursorOffset: cursorOffset,
						Info:         info,
						Identity:     source.Identity,
					}
					result.Found = true
					result.Complete = true
					result.Progress = historyWatchRebaseScanProgress{SourceIdentity: strings.TrimSpace(source.Identity), Offset: nextOffset, Line: lineNo, State: state}
					return result, nil
				}
			}
			offset = nextOffset
			result.Progress = historyWatchRebaseScanProgress{SourceIdentity: strings.TrimSpace(source.Identity), Offset: offset, Line: lineNo, State: state}
		}
		if readErr != nil {
			if readErr != io.EOF {
				return result, readErr
			}
			if stableErr := historyRebaseStableSource(path, info, source.Identity); stableErr != nil {
				return result, stableErr
			}
			result.Complete = true
			result.Progress = historyWatchRebaseScanProgress{SourceIdentity: strings.TrimSpace(source.Identity), Offset: offset, Line: lineNo, State: state}
			break
		}
	}
	result.Anchor = historyWatchRebaseAnchor{Info: info, Identity: source.Identity}
	return result, nil
}

func historyRebaseStableSource(path string, expected os.FileInfo, expectedIdentity string) error {
	current, err := os.Stat(path)
	if err != nil {
		return err
	}
	if current.IsDir() || !os.SameFile(expected, current) {
		return fmt.Errorf("history source %q changed during rebase", path)
	}
	identity, err := teamstore.SourceFileIdentityFromFileInfo(path, current)
	if err != nil {
		return err
	}
	if strings.TrimSpace(identity) == "" || strings.TrimSpace(identity) != strings.TrimSpace(expectedIdentity) {
		return fmt.Errorf("history source %q identity changed during rebase", path)
	}
	return nil
}

func historyWatchRebaseAnchorMatches(record TranscriptRecord, previous historyTieredFileState) bool {
	if record.Internal || record.Kind != TranscriptKindAssistant || strings.TrimSpace(record.Text) == "" {
		return false
	}
	if sourceID := historyTieredFinalSourceID(previous.LastFinalID); sourceID != "" {
		return strings.TrimSpace(record.SourceItemID) == sourceID
	}
	if strings.TrimSpace(previous.LastFinalID) != "" && historyTieredCompletionKey(record, 0, "") == strings.TrimSpace(previous.LastFinalID) {
		return true
	}
	if strings.TrimSpace(previous.LastFinalTextHash) == "" || strings.TrimSpace(previous.LastFinalTurnID) == "" {
		return false
	}
	if strings.TrimSpace(record.TurnID) != strings.TrimSpace(previous.LastFinalTurnID) {
		return false
	}
	if strings.TrimSpace(previous.LastFinalThreadID) != "" && strings.TrimSpace(record.ThreadID) != "" &&
		strings.TrimSpace(record.ThreadID) != strings.TrimSpace(previous.LastFinalThreadID) {
		return false
	}
	return normalizedTextHash(strings.TrimSpace(record.Text)) == strings.TrimSpace(previous.LastFinalTextHash)
}

func historyTieredFinalSourceID(key string) string {
	key = strings.TrimSpace(key)
	const prefix = "codex-final:v1:"
	if !strings.HasPrefix(key, prefix) {
		return ""
	}
	parts := strings.SplitN(strings.TrimPrefix(key, prefix), ":", 3)
	if len(parts) != 3 || strings.HasPrefix(parts[2], "terminal-line:") {
		return ""
	}
	return strings.TrimSpace(parts[2])
}

func historyRebaseSourceProof(path string, expected os.FileInfo, offset int64) (string, string, os.FileInfo, bool) {
	f, err := os.Open(path)
	if err != nil {
		return "", "", nil, false
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || info.IsDir() || !os.SameFile(expected, info) || info.Size() < offset {
		return "", "", nil, false
	}
	identity, err := teamstore.SourceFileIdentityFromFileInfo(path, info)
	if err != nil || strings.TrimSpace(identity) == "" {
		return "", "", nil, false
	}
	fingerprint := strings.TrimSpace(transcriptCheckpointSourceFingerprintFromReaderWithIdentity(f, path, identity, info.Size(), offset))
	if fingerprint == "" || historyRebaseStableSource(path, info, identity) != nil {
		return "", "", nil, false
	}
	return strings.TrimSpace(identity), fingerprint, info, true
}

func (b *Bridge) recordHistoryWatchRebaseAttempt(ctx context.Context, id string, expected *teamstore.HistoryWatchCheckpoint, previous historyTieredFileState, path string, source codexHistoryFile, now time.Time) error {
	if expected == nil || strings.TrimSpace(source.Identity) == "" {
		return nil
	}
	previous.Path = path
	previous.SourceRewriteRecoveryIdentity = strings.TrimSpace(source.Identity)
	previous.SourceRewriteRecoverySize = source.Info.Size()
	previous.SourceRewriteRecoveryModTime = source.Info.ModTime()
	previous.SourceRewriteRecoveryChangeTime = teamstore.SourceFileChangeTimeFromFileInfo(source.Info)
	previous.SourceRewriteRecoveryScanPending = false
	previous.SourceRewriteRecoveryScanOffset = 0
	previous.SourceRewriteRecoveryScanLine = 0
	previous.SourceRewriteRecoveryScanSessionID = ""
	previous.SourceRewriteRecoveryScanThreadID = ""
	previous.SourceRewriteRecoveryScanTurnID = ""
	return b.recordHistoryWatchCheckpointIfCurrent(ctx, id, expected, previous, now)
}

func (b *Bridge) historyWatchRebaseScanProgress(id string, identity string, previous historyTieredFileState) historyWatchRebaseScanProgress {
	initial := historyWatchRebaseScanProgress{
		SourceIdentity: strings.TrimSpace(identity),
		State: transcriptParseState{
			sessionID: strings.TrimSpace(previous.SessionID),
			threadID:  strings.TrimSpace(previous.ThreadID),
			turnID:    strings.TrimSpace(previous.TurnID),
		},
	}
	if previous.SourceRewriteRecoveryScanPending &&
		strings.TrimSpace(previous.SourceRewriteRecoveryIdentity) == strings.TrimSpace(identity) &&
		previous.SourceRewriteRecoveryScanOffset >= 0 {
		initial.Offset = previous.SourceRewriteRecoveryScanOffset
		initial.Line = previous.SourceRewriteRecoveryScanLine
		initial.State = transcriptParseState{
			sessionID: firstNonEmptyString(previous.SourceRewriteRecoveryScanSessionID, previous.SessionID),
			threadID:  firstNonEmptyString(previous.SourceRewriteRecoveryScanThreadID, previous.ThreadID),
			turnID:    firstNonEmptyString(previous.SourceRewriteRecoveryScanTurnID, previous.TurnID),
		}
	}
	if b == nil {
		return initial
	}
	b.historyRebaseMu.Lock()
	defer b.historyRebaseMu.Unlock()
	progress, ok := b.historyRebaseScans[strings.TrimSpace(id)]
	if !ok || strings.TrimSpace(progress.SourceIdentity) != strings.TrimSpace(identity) || progress.Offset < 0 {
		return initial
	}
	return progress
}

func (b *Bridge) recordHistoryWatchRebaseScanProgress(ctx context.Context, id string, expected *teamstore.HistoryWatchCheckpoint, previous historyTieredFileState, path string, source codexHistoryFile, progress historyWatchRebaseScanProgress, now time.Time) error {
	if expected == nil || strings.TrimSpace(source.Identity) == "" {
		return nil
	}
	next := previous
	next.Path = path
	next.SourceRewriteRecoveryIdentity = strings.TrimSpace(source.Identity)
	next.SourceRewriteRecoverySize = source.Info.Size()
	next.SourceRewriteRecoveryModTime = source.Info.ModTime()
	next.SourceRewriteRecoveryChangeTime = teamstore.SourceFileChangeTimeFromFileInfo(source.Info)
	next.SourceRewriteRecoveryScanPending = true
	next.SourceRewriteRecoveryScanOffset = progress.Offset
	next.SourceRewriteRecoveryScanLine = progress.Line
	next.SourceRewriteRecoveryScanSessionID = strings.TrimSpace(progress.State.sessionID)
	next.SourceRewriteRecoveryScanThreadID = strings.TrimSpace(progress.State.threadID)
	next.SourceRewriteRecoveryScanTurnID = strings.TrimSpace(progress.State.turnID)
	// The phase context may have expired because this cold scan used its whole
	// budget. Use a short context that preserves owner values but not the
	// cancellation, so the durable cursor can still be recorded for the next
	// cycle without allowing a slow store write to hang the listener.
	persistBase := ctx
	if persistBase == nil {
		persistBase = context.Background()
	}
	persistCtx := context.WithoutCancel(persistBase)
	persistCtx, cancel := context.WithTimeout(persistCtx, 2*time.Second)
	defer cancel()
	return b.recordHistoryWatchCheckpointIfCurrent(persistCtx, id, expected, next, now)
}

func (b *Bridge) rememberHistoryWatchRebaseScanProgress(id string, progress historyWatchRebaseScanProgress) {
	if b == nil || strings.TrimSpace(id) == "" || strings.TrimSpace(progress.SourceIdentity) == "" {
		return
	}
	b.historyRebaseMu.Lock()
	defer b.historyRebaseMu.Unlock()
	if b.historyRebaseScans == nil {
		b.historyRebaseScans = make(map[string]historyWatchRebaseScanProgress)
	}
	b.historyRebaseScans[strings.TrimSpace(id)] = progress
}

func (b *Bridge) forgetHistoryWatchRebaseScanProgress(id string) {
	if b == nil {
		return
	}
	b.historyRebaseMu.Lock()
	defer b.historyRebaseMu.Unlock()
	delete(b.historyRebaseScans, strings.TrimSpace(id))
}

func (b *Bridge) rebaseHistoryWatchSourceRewrite(ctx context.Context, id string, expected *teamstore.HistoryWatchCheckpoint, previous historyTieredFileState, path string, now time.Time) (bool, error) {
	if !previous.SourceRewriteBlocked {
		return false, nil
	}
	source, ok, err := codexPaginatedHistoryFile(path, firstNonEmptyString(previous.ThreadID, previous.SessionID))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil || !ok {
		return false, err
	}
	sameIdentity := strings.TrimSpace(source.Identity) == strings.TrimSpace(previous.SourceRewriteRecoveryIdentity)
	if sameIdentity && historyRewriteRecoverySnapshotMatches(previous, source.Info) && !previous.SourceRewriteRecoveryScanPending {
		return false, nil
	}
	// A same-inode source may have been repaired in place. Do not resume a
	// completed scan from the old EOF in that case; the repaired anchor can be
	// anywhere in the source. Appends also reset the cold cursor harmlessly.
	if sameIdentity && !historyRewriteRecoverySnapshotMatches(previous, source.Info) {
		b.forgetHistoryWatchRebaseScanProgress(id)
		previous.SourceRewriteRecoveryScanPending = false
		previous.SourceRewriteRecoveryScanOffset = 0
		previous.SourceRewriteRecoveryScanLine = 0
		previous.SourceRewriteRecoveryScanSessionID = ""
		previous.SourceRewriteRecoveryScanThreadID = ""
		previous.SourceRewriteRecoveryScanTurnID = ""
	}
	progress := b.historyWatchRebaseScanProgress(id, source.Identity, previous)
	scan, err := historyWatchRebaseAnchorScan(ctx, path, previous, source, progress)
	if err != nil {
		if scan.Progress.Offset >= progress.Offset && strings.TrimSpace(scan.Progress.SourceIdentity) == strings.TrimSpace(source.Identity) {
			b.rememberHistoryWatchRebaseScanProgress(id, scan.Progress)
		}
		// Context cancellation is expected when the history phase budget expires;
		// preserve the in-memory scan cursor and let a later cycle continue. Other
		// source/read errors remain fail-closed and are retried only after the
		// candidate identity changes or an explicit recovery is requested.
		if ctx != nil && ctx.Err() != nil {
			if persistErr := b.recordHistoryWatchRebaseScanProgress(ctx, id, expected, previous, path, source, scan.Progress, now); persistErr != nil {
				return false, persistErr
			}
			return false, nil
		}
		b.forgetHistoryWatchRebaseScanProgress(id)
		return false, nil
	}
	if !scan.Complete {
		b.rememberHistoryWatchRebaseScanProgress(id, scan.Progress)
		if persistErr := b.recordHistoryWatchRebaseScanProgress(ctx, id, expected, previous, path, source, scan.Progress, now); persistErr != nil {
			return false, persistErr
		}
		return false, nil
	}
	b.forgetHistoryWatchRebaseScanProgress(id)
	if !scan.Found {
		return false, b.recordHistoryWatchRebaseAttempt(ctx, id, expected, previous, path, source, now)
	}
	anchor := scan.Anchor
	identity, fingerprint, info, ok := historyRebaseSourceProof(path, anchor.Info, anchor.CursorOffset)
	if !ok || identity != source.Identity {
		return false, nil
	}
	next := previous
	next.Path = path
	next.Size = anchor.CursorOffset
	next.ModTime = info.ModTime()
	next.SourceFingerprint = fingerprint
	next.SourceGeneration = source.Identity
	next.SourceRewriteBlocked = false
	next.SourceRewriteRecoveryIdentity = ""
	next.SourceRewriteRecoverySize = 0
	next.SourceRewriteRecoveryModTime = time.Time{}
	next.SourceRewriteRecoveryChangeTime = 0
	next.SourceRewriteRecoveryScanPending = false
	next.SourceRewriteRecoveryScanOffset = 0
	next.SourceRewriteRecoveryScanLine = 0
	next.SourceRewriteRecoveryScanSessionID = ""
	next.SourceRewriteRecoveryScanThreadID = ""
	next.SourceRewriteRecoveryScanTurnID = ""
	next.Offset = anchor.CursorOffset
	next.Line = anchor.CursorLine
	next.SessionID = firstNonEmptyString(source.Header.ThreadID, previous.SessionID)
	next.ThreadID = firstNonEmptyString(source.Header.ThreadID, previous.ThreadID)
	next.TurnID = strings.TrimSpace(anchor.Record.TurnID)
	next.LastFinalLine = anchor.Record.SourceLine
	next.LastFinalStartOffset = anchor.Record.SourceStartOffset
	next.LastFinalStartOffsetKnown = true
	next.LastFinalThreadID = strings.TrimSpace(anchor.Record.ThreadID)
	next.LastFinalTurnID = strings.TrimSpace(anchor.Record.TurnID)
	next.LastFinalTextHash = normalizedTextHash(strings.TrimSpace(anchor.Record.Text))
	if strings.TrimSpace(anchor.Record.SourceItemID) != "" {
		next.LastFinalID = historyTieredCompletionKey(anchor.Record, 0, "")
	}
	if next.TerminalBoundarySeen {
		next.TerminalBoundaryLine = anchor.Record.SourceLine
	}
	next.ExternalUserPromptSeen = false
	next.PendingRootTaskStarted = false
	next.PendingRootTaskStartedLine = 0
	next.PendingRootTaskStartedOffset = 0
	next.pendingAssistant = historyTieredAssistantCandidate{}
	if next.UnresolvedContinuation {
		next.UnresolvedContinuationLine = anchor.Record.SourceLine
		next.UnresolvedContinuationOffset = anchor.Record.SourceStartOffset
	}
	if err := b.recordHistoryWatchCheckpointIfCurrent(ctx, id, expected, next, now); err != nil {
		return false, err
	}
	return true, nil
}

func (b *Bridge) rebaseLinkedTranscriptSourceRewrite(ctx context.Context, session Session, local codexhistory.Session, checkpoint teamstore.ImportCheckpoint) (bool, error) {
	if b == nil || b.store == nil || !checkpoint.SourceRewriteBlocked || strings.TrimSpace(checkpoint.LastRecordID) == "" {
		return false, nil
	}
	path := strings.TrimSpace(firstNonEmptyString(local.FilePath, checkpoint.SourcePath))
	if path == "" || (strings.TrimSpace(checkpoint.SourcePath) != "" && cleanComparablePath(checkpoint.SourcePath) != cleanComparablePath(path)) {
		return false, nil
	}
	expectedThreadID := firstNonEmptyString(local.SessionID, session.CodexThreadID)
	source, ok, err := codexPaginatedHistoryFile(path, expectedThreadID)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil || !ok {
		return false, err
	}
	recoverySnapshot := historyTieredFileState{
		SourceRewriteRecoveryIdentity:   strings.TrimSpace(checkpoint.SourceRewriteRecoveryIdentity),
		SourceRewriteRecoverySize:       checkpoint.SourceRewriteRecoverySize,
		SourceRewriteRecoveryModTime:    checkpoint.SourceRewriteRecoveryModTime,
		SourceRewriteRecoveryChangeTime: checkpoint.SourceRewriteRecoveryChangeTime,
	}
	if strings.TrimSpace(source.Identity) == strings.TrimSpace(checkpoint.SourceRewriteRecoveryIdentity) &&
		historyRewriteRecoverySnapshotMatches(recoverySnapshot, source.Info) {
		return false, nil
	}
	position, found, err := findTranscriptCheckpointPositionWithContext(ctx, path, checkpoint.LastRecordID)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !found {
		_, _, updateErr := b.updateImportCheckpoint(ctx, checkpoint.ID, func(current teamstore.ImportCheckpoint, found bool, now time.Time) (teamstore.ImportCheckpoint, bool, error) {
			if !found || !current.SourceRewriteBlocked || strings.TrimSpace(current.LastRecordID) != strings.TrimSpace(checkpoint.LastRecordID) {
				return current, false, nil
			}
			current.SourceRewriteRecoveryIdentity = source.Identity
			current.SourceRewriteRecoverySize = source.Info.Size()
			current.SourceRewriteRecoveryModTime = source.Info.ModTime()
			current.SourceRewriteRecoveryChangeTime = teamstore.SourceFileChangeTimeFromFileInfo(source.Info)
			current.UpdatedAt = now
			return current, true, nil
		})
		return false, updateErr
	}
	currentInfo, err := os.Stat(path)
	if err != nil || currentInfo.IsDir() || !os.SameFile(source.Info, currentInfo) || currentInfo.Size() < position.Offset {
		return false, nil
	}
	identity, fingerprint, info, ok := historyRebaseSourceProof(path, source.Info, position.Offset)
	if !ok || identity != source.Identity {
		return false, nil
	}
	_, changed, err := b.updateImportCheckpoint(ctx, checkpoint.ID, func(current teamstore.ImportCheckpoint, found bool, now time.Time) (teamstore.ImportCheckpoint, bool, error) {
		if !found || !current.SourceRewriteBlocked || strings.TrimSpace(current.LastRecordID) != strings.TrimSpace(checkpoint.LastRecordID) ||
			strings.TrimSpace(current.SourcePath) != "" && cleanComparablePath(current.SourcePath) != cleanComparablePath(path) {
			return current, false, nil
		}
		next := current
		next.SourcePath = path
		next.SourceFingerprint = fingerprint
		next.SourceGeneration = identity
		next.LastSourceLine = position.Line
		next.LastOffset = position.Offset
		next.LastOffsetKnown = true
		next.SourceSize = info.Size()
		next.SourceModTime = info.ModTime()
		next.SourceRewriteBlocked = false
		next.SourceRewriteRecoveryIdentity = ""
		next.SourceRewriteRecoverySize = 0
		next.SourceRewriteRecoveryModTime = time.Time{}
		next.SourceRewriteRecoveryChangeTime = 0
		if next.CompletionPending {
			next.Status = importCheckpointStatusImporting
		} else if next.Status == importCheckpointStatusBlocked {
			next.Status = importCheckpointStatusComplete
		}
		next.UpdatedAt = now
		return next, true, nil
	})
	return changed, err
}
