//go:build linux

package teams

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// TestTeamsRuntimeSafetySQLiteFullFilesystemDockerCI exercises the OS-level
// boundary that PRAGMA max_page_count cannot model: a real filesystem runs out
// of space while the durable Teams poll projection is being updated. It only
// runs from the isolated Docker boundary job, where root is a small tmpfs and
// no live helper state is mounted.
func TestTeamsRuntimeSafetySQLiteFullFilesystemDockerCI(t *testing.T) {
	if os.Getenv("CXP_TEAMS_BOUNDARY_DOCKER") != "1" {
		t.Skip("runs only in the isolated Docker boundary job")
	}
	root := strings.TrimSpace(os.Getenv("CXP_TEAMS_BOUNDARY_FS_ROOT"))
	if filepath.Clean(root) != "/state" {
		t.Fatalf("CXP_TEAMS_BOUNDARY_FS_ROOT = %q, want the isolated /state tmpfs", root)
	}
	info, err := os.Stat(root)
	if err != nil {
		t.Fatalf("stat boundary filesystem root: %v", err)
	}
	if !info.IsDir() {
		t.Fatalf("boundary filesystem root %q is not a directory", root)
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatalf("resolve boundary filesystem root: %v", err)
	}
	if resolvedRoot != root {
		t.Fatalf("boundary filesystem root %q resolves to %q", root, resolvedRoot)
	}
	if filesystemType, err := boundaryFilesystemType(root); err != nil {
		t.Fatalf("stat boundary filesystem type: %v", err)
	} else if filesystemType != boundaryTmpfsMagic {
		t.Fatalf("boundary filesystem type = %#x, want tmpfs %#x", filesystemType, boundaryTmpfsMagic)
	}
	freeAtStart, err := boundaryFilesystemFreeBytes(root)
	if err != nil {
		t.Fatalf("measure boundary filesystem capacity: %v", err)
	}
	if freeAtStart > 32*1024*1024 {
		t.Fatalf("boundary filesystem has %d free bytes; refuse to fill a filesystem larger than the CI /state tmpfs contract", freeAtStart)
	}
	testRoot := filepath.Join(root, "sqlite-full-filesystem")
	if err := os.MkdirAll(testRoot, 0o700); err != nil {
		t.Fatalf("create test filesystem root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(testRoot) })

	ctx := context.Background()
	statePath := filepath.Join(testRoot, "teams-state.json")
	store, err := teamstore.Open(statePath)
	if err != nil {
		t.Fatalf("open state on boundary filesystem: %v", err)
	}
	defer func() { _ = store.Close() }()
	if _, err := store.RecordChatPollSuccessWithContinuation(ctx, "chat-full-fs", time.Now(), true, true, 20, "/chats/chat-full-fs/messages?$skiptoken=old"); err != nil {
		t.Fatalf("seed poll projection: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate poll projection to SQLite: %v", err)
	}

	// Keep a linked transcript checkpoint in the same SQLite store. The poll
	// projection already exercises one durable write path below; this second
	// fixture verifies that a full filesystem cannot partially advance a
	// transcript cursor or lose its ledger row.
	const transcriptSessionID = "full-fs-transcript-session"
	const transcriptCheckpointID = "transcript:full-fs-transcript-session"
	transcriptPath := filepath.Join(testRoot, "session.jsonl")
	transcriptPrefix := []byte(`{"id":"old-record","role":"assistant","text":"old"}` + "\n")
	if err := os.WriteFile(transcriptPath, transcriptPrefix, 0o600); err != nil {
		t.Fatalf("write transcript prefix: %v", err)
	}
	transcriptOffset := int64(len(transcriptPrefix))
	transcriptRecord := []byte(`{"id":"new-record","role":"assistant","text":"new"}` + "\n")
	appendFile, err := os.OpenFile(transcriptPath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open transcript append: %v", err)
	}
	if written, writeErr := appendFile.Write(transcriptRecord); writeErr != nil || written != len(transcriptRecord) {
		_ = appendFile.Close()
		t.Fatalf("append transcript record: wrote=%d/%d err=%v", written, len(transcriptRecord), writeErr)
	}
	if err := appendFile.Close(); err != nil {
		t.Fatalf("close transcript append: %v", err)
	}
	newTranscriptOffset := transcriptOffset + int64(len(transcriptRecord))
	newTranscriptFingerprint := transcriptCheckpointSourceFingerprint(transcriptPath, newTranscriptOffset)
	transcriptInfo, err := os.Stat(transcriptPath)
	if err != nil {
		t.Fatalf("stat transcript after append: %v", err)
	}
	transcriptGeneration := historyTieredSourceIdentity(transcriptPath, transcriptInfo)
	transcriptFingerprint := transcriptCheckpointSourceFingerprint(transcriptPath, transcriptOffset)
	if transcriptFingerprint == "" {
		t.Fatal("transcript prefix fingerprint is empty")
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Sessions[transcriptSessionID] = teamstore.SessionContext{ID: transcriptSessionID, Status: teamstore.SessionStatusActive}
		state.ImportCheckpoints[transcriptCheckpointID] = teamstore.ImportCheckpoint{
			ID:                transcriptCheckpointID,
			SessionID:         transcriptSessionID,
			SourcePath:        transcriptPath,
			SourceFingerprint: transcriptFingerprint,
			LastRecordID:      "old-record",
			LastSourceLine:    1,
			LastOffset:        transcriptOffset,
			LastOffsetKnown:   true,
			SourceSize:        transcriptOffset,
			Status:            "complete",
		}
		return nil
	}); err != nil {
		t.Fatalf("seed transcript checkpoint: %v", err)
	}

	fillerPath := filepath.Join(testRoot, "filler")
	filler, err := os.OpenFile(fillerPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open filesystem filler: %v", err)
	}
	chunk := make([]byte, 64*1024)
	for {
		free, err := boundaryFilesystemFreeBytes(testRoot)
		if err != nil {
			_ = filler.Close()
			t.Fatalf("measure filesystem free space: %v", err)
		}
		if free < 512*1024 {
			break
		}
		written, err := filler.Write(chunk)
		if written != len(chunk) && err == nil {
			_ = filler.Close()
			t.Fatalf("fill boundary filesystem: short write %d/%d without error", written, len(chunk))
		}
		if err != nil {
			if !errors.Is(err, syscall.ENOSPC) {
				_ = filler.Close()
				t.Fatalf("fill boundary filesystem: %v", err)
			}
			break
		}
	}
	if err := filler.Close(); err != nil {
		t.Fatalf("close filesystem filler: %v", err)
	}

	oldCheckpoint, found, err := store.ImportCheckpoint(ctx, transcriptCheckpointID)
	if err != nil || !found {
		t.Fatalf("load transcript checkpoint before full write: found=%v err=%v", found, err)
	}
	newCheckpoint := oldCheckpoint
	newCheckpoint.LastRecordID = "new-record"
	newCheckpoint.LastSourceLine = 2
	newCheckpoint.LastOffset = newTranscriptOffset
	newCheckpoint.LastOffsetKnown = true
	newCheckpoint.SourceSize = newTranscriptOffset
	newCheckpoint.SourceFingerprint = newTranscriptFingerprint
	newCheckpoint.SourceGeneration = transcriptGeneration
	newCheckpoint.SourceModTime = transcriptInfo.ModTime()
	ledger := teamstore.TranscriptLedgerRecord{
		ID:             "ledger:full-fs-transcript-session:new-record",
		SessionID:      transcriptSessionID,
		SourcePath:     transcriptPath,
		SourceLine:     2,
		SourceRecordID: "new-record",
		// The large provenance value forces SQLite to allocate a new page after
		// the filler consumes the filesystem. The checkpoint and ledger must still
		// commit atomically; this is an allocation-boundary test, not a production
		// size expectation for Teams IDs.
		TeamsOriginID: strings.Repeat("x", 4*1024*1024),
	}
	checkpointWriteErr := store.RecordTranscriptCheckpoint(ctx, newCheckpoint, ledger)
	if checkpointWriteErr == nil {
		t.Fatal("filesystem-full transcript checkpoint unexpectedly succeeded")
	}
	if !boundaryDiskFullError(checkpointWriteErr) {
		t.Fatalf("filesystem-full transcript checkpoint error = %v, want ENOSPC/SQLITE_FULL", checkpointWriteErr)
	}
	unchanged, found, err := store.ImportCheckpoint(ctx, transcriptCheckpointID)
	if err != nil || !found || !reflect.DeepEqual(unchanged, oldCheckpoint) {
		t.Fatalf("filesystem-full transcript checkpoint changed state: checkpoint=%#v found=%v err=%v", unchanged, found, err)
	}
	stateAfterFailure, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load state after filesystem-full transcript checkpoint: %v", err)
	}
	if _, ok := stateAfterFailure.TranscriptLedger[ledger.ID]; ok {
		t.Fatalf("filesystem-full transcript checkpoint partially committed ledger: %#v", stateAfterFailure.TranscriptLedger[ledger.ID])
	}

	largePath := "/chats/chat-full-fs/messages?$skiptoken=" + strings.Repeat("x", 16*1024*1024)
	pollBefore, pollFound, err := store.ChatPoll(ctx, "chat-full-fs")
	if err != nil || !pollFound {
		t.Fatalf("load poll before filesystem-full deferred write: found=%v err=%v", pollFound, err)
	}
	deferredWriteErr := error(nil)
	if _, deferredWriteErr = store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:                      "chat-full-fs",
		DeferredContinuationPath:    largePath,
		SetDeferredContinuationPath: true,
	}); deferredWriteErr == nil || !boundaryDiskFullError(deferredWriteErr) {
		t.Fatalf("filesystem-full deferred write error = %v, want ENOSPC/SQLITE_FULL", deferredWriteErr)
	}
	poll, ok, err := store.ChatPoll(ctx, "chat-full-fs")
	if err != nil || !ok {
		t.Fatalf("read poll after filesystem-full write: ok=%v err=%v", ok, err)
	}
	if !reflect.DeepEqual(poll, pollBefore) {
		t.Fatalf("filesystem-full write partially committed deferred state: before=%#v after=%#v", pollBefore, poll)
	}

	if err := os.Remove(fillerPath); err != nil {
		t.Fatalf("release filesystem capacity: %v", err)
	}
	if err := store.RecordTranscriptCheckpoint(ctx, newCheckpoint, ledger); err != nil {
		t.Fatalf("retry transcript checkpoint after capacity recovery: %v", err)
	}
	recoveredCheckpoint, found, err := store.ImportCheckpoint(ctx, transcriptCheckpointID)
	if err != nil || !found || recoveredCheckpoint.LastRecordID != "new-record" || recoveredCheckpoint.LastSourceLine != 2 || recoveredCheckpoint.LastOffset != newTranscriptOffset || recoveredCheckpoint.SourceSize != newTranscriptOffset || recoveredCheckpoint.SourceFingerprint != newTranscriptFingerprint {
		t.Fatalf("recovered transcript checkpoint = %#v found=%v err=%v", recoveredCheckpoint, found, err)
	}
	recoveredState, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load state after transcript checkpoint recovery: %v", err)
	}
	if got := recoveredState.TranscriptLedger[ledger.ID]; got.SourceRecordID != "new-record" || got.SourcePath != transcriptPath {
		t.Fatalf("recovered transcript ledger = %#v", got)
	}
	const recoveredPath = "/chats/chat-full-fs/messages?$skiptoken=recovered"
	if _, err := store.UpdateChatPollSchedule(ctx, teamstore.ChatPollScheduleUpdate{
		ChatID:                      "chat-full-fs",
		DeferredContinuationPath:    recoveredPath,
		SetDeferredContinuationPath: true,
	}); err != nil {
		t.Fatalf("retry deferred write after capacity recovery: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close recovered boundary store: %v", err)
	}
	reopened, err := teamstore.Open(statePath)
	if err != nil {
		t.Fatalf("reopen recovered boundary store: %v", err)
	}
	defer reopened.Close()
	poll, ok, err = reopened.ChatPoll(ctx, "chat-full-fs")
	if err != nil || !ok || poll.DeferredContinuationPath != recoveredPath {
		t.Fatalf("reopened filesystem-full recovery = %#v ok=%v err=%v", poll, ok, err)
	}
}

// TestTeamsRuntimeSafetyBridgeTranscriptGraphAcceptedThenSQLiteFullDockerCI
// covers the cross-system failure boundary that a store-only ENOSPC test cannot
// model: Graph has already accepted a transcript message and the outbox has a
// durable sent identity, but the following automatic checkpoint CAS cannot
// allocate SQLite space. After capacity recovery and a restart, linked sync
// must use the durable outbox identity and advance the checkpoint without a
// second Graph POST.
func TestTeamsRuntimeSafetyBridgeTranscriptGraphAcceptedThenSQLiteFullDockerCI(t *testing.T) {
	if os.Getenv("CXP_TEAMS_BOUNDARY_DOCKER") != "1" {
		t.Skip("runs only in the isolated Docker boundary job")
	}
	root := strings.TrimSpace(os.Getenv("CXP_TEAMS_BOUNDARY_FS_ROOT"))
	if filepath.Clean(root) != "/state" {
		t.Fatalf("CXP_TEAMS_BOUNDARY_FS_ROOT = %q, want the isolated /state tmpfs", root)
	}
	if filesystemType, err := boundaryFilesystemType(root); err != nil {
		t.Fatalf("stat boundary filesystem type: %v", err)
	} else if filesystemType != boundaryTmpfsMagic {
		t.Fatalf("boundary filesystem type = %#x, want tmpfs %#x", filesystemType, boundaryTmpfsMagic)
	}
	freeAtStart, err := boundaryFilesystemFreeBytes(root)
	if err != nil {
		t.Fatalf("measure boundary filesystem capacity: %v", err)
	}
	if freeAtStart > 32*1024*1024 {
		t.Fatalf("boundary filesystem has %d free bytes; refuse to fill a filesystem larger than the CI /state tmpfs contract", freeAtStart)
	}
	testRoot := filepath.Join(root, "bridge-transcript-accepted-full")
	if err := os.MkdirAll(testRoot, 0o700); err != nil {
		t.Fatalf("create bridge boundary root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(testRoot) })

	ctx := context.Background()
	statePath := filepath.Join(testRoot, "teams-state.json")
	store, err := teamstore.Open(statePath)
	if err != nil {
		t.Fatalf("open bridge boundary store: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		_ = store.Close()
		t.Fatalf("migrate bridge boundary store: %v", err)
	}
	defer func() { _ = store.Close() }()

	transcriptPath := filepath.Join(testRoot, "session.jsonl")
	threadID := "thread-bridge-transcript-enospc"
	prefix := []byte(`{"id":"bridge-old","thread_id":"thread-bridge-transcript-enospc","role":"assistant","text":"old"}` + "\n")
	if err := os.WriteFile(transcriptPath, prefix, 0o600); err != nil {
		t.Fatalf("write bridge transcript prefix: %v", err)
	}
	prefixOffset := int64(len(prefix))
	appendLine(t, transcriptPath, `{"id":"bridge-new","thread_id":"thread-bridge-transcript-enospc","role":"assistant","text":"bridge answer after accepted Graph send"}`)
	transcriptInfo, err := os.Stat(transcriptPath)
	if err != nil {
		t.Fatalf("stat bridge transcript: %v", err)
	}
	checkpointID := transcriptCheckpointID("s001")
	checkpointBefore, err := func() (teamstore.ImportCheckpoint, error) {
		checkpoint := teamstore.ImportCheckpoint{
			ID:                checkpointID,
			SessionID:         "s001",
			SourcePath:        transcriptPath,
			SourceFingerprint: transcriptCheckpointSourceFingerprint(transcriptPath, prefixOffset),
			SourceGeneration:  historyTieredSourceIdentity(transcriptPath, transcriptInfo),
			LastRecordID:      "bridge-old",
			LastSourceLine:    1,
			LastOffset:        prefixOffset,
			LastOffsetKnown:   true,
			SourceSize:        prefixOffset,
			SourceModTime:     transcriptInfo.ModTime(),
			Status:            importCheckpointStatusComplete,
		}
		if checkpoint.SourceFingerprint == "" {
			return teamstore.ImportCheckpoint{}, fmt.Errorf("bridge transcript prefix fingerprint is empty")
		}
		return checkpoint, nil
	}()
	if err != nil {
		t.Fatalf("prepare bridge transcript checkpoint: %v", err)
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.ImportCheckpoints[checkpointID] = checkpointBefore
		return nil
	}); err != nil {
		t.Fatalf("seed bridge transcript checkpoint: %v", err)
	}

	var posts int
	graph := &GraphClient{
		auth: &fakeGraphAuth{token: "access"},
		client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method != http.MethodPost || r.URL.Path != "/chats/chat-1/messages" {
				return nil, fmt.Errorf("unexpected bridge Graph request: %s %s", r.Method, r.URL.String())
			}
			posts++
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"application/json"}},
				Body:       io.NopCloser(strings.NewReader(`{"id":"bridge-accepted-1","messageType":"message"}`)),
				Request:    r,
			}, nil
		})},
		baseURL:    "https://graph.example.test",
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByID("s001")
	if session == nil {
		t.Fatal("bridge boundary registry missing s001")
	}
	session.CodexThreadID = threadID
	if err := bridge.ensureDurableSession(ctx, session); err != nil {
		t.Fatalf("ensure bridge boundary session: %v", err)
	}
	checkpoint, found, err := store.ImportCheckpoint(ctx, checkpointID)
	if err != nil || !found {
		t.Fatalf("load bridge boundary checkpoint: found=%v err=%v", found, err)
	}
	local, ok := linkedTranscriptLocalFromCheckpoint(*session, checkpoint)
	if !ok {
		t.Fatalf("linked transcript local not found for checkpoint %#v", checkpoint)
	}
	transcript, err := bridge.readLinkedTranscriptDelta(transcriptPath, checkpoint, session.ID, threadID)
	if err != nil || len(transcript.Records) != 1 {
		t.Fatalf("read bridge transcript delta: records=%d err=%v transcript=%#v", len(transcript.Records), err, transcript)
	}
	record := transcript.Records[0]
	line, offset := transcriptCheckpointPositionForRecord(transcript.Records, 0)
	record.SourceLine = line
	record.SourceOffset = offset
	body := formatTranscriptRecordForTeams(record)
	proof := transcriptSourceProofQueueOptions(checkpoint)
	addTranscriptReadProofToQueueOptions(&proof, transcript)
	opts := transcriptSyncOutboxOptions(record)
	opts.ParentFenceSessionID = session.ID
	opts.ExpectedSourcePath = proof.ExpectedSourcePath
	opts.ExpectedSourceFingerprint = proof.ExpectedSourceFingerprint
	opts.ExpectedSourceOffset = proof.ExpectedSourceOffset
	opts.ExpectedSourceOffsetKnown = proof.ExpectedSourceOffsetKnown
	opts.ExpectedSourceReadFingerprint = proof.ExpectedSourceReadFingerprint
	opts.ExpectedSourceReadStartOffset = proof.ExpectedSourceReadStartOffset
	opts.ExpectedSourceReadEndOffset = proof.ExpectedSourceReadEndOffset
	opts.ExpectedSourceReadRangeKnown = proof.ExpectedSourceReadRangeKnown
	if err := bridge.queueOrSendTranscriptDeliveryChunksWithOptions(ctx, *session, local, record, line, offset, transcriptRecordOutboxKind("sync", record, 1), body, opts, "sync:"+session.ID, checkpointID, false, false, ""); err != nil {
		t.Fatalf("send bridge transcript before checkpoint boundary: %v", err)
	}
	if posts != 1 {
		t.Fatalf("bridge transcript Graph posts before ENOSPC = %d, want one", posts)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load bridge state after accepted Graph send: %v", err)
	}
	var sentOutbox teamstore.OutboxMessage
	for _, outbox := range state.OutboxMessages {
		if strings.Contains(outbox.Body, "bridge answer after accepted Graph send") {
			sentOutbox = outbox
			break
		}
	}
	if sentOutbox.Status != teamstore.OutboxStatusSent || sentOutbox.TeamsMessageID != "bridge-accepted-1" {
		t.Fatalf("bridge outbox after accepted Graph send = %#v, want durable sent identity", sentOutbox)
	}

	fillerPath := filepath.Join(testRoot, "bridge-filler")
	filler, err := os.OpenFile(fillerPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatalf("open bridge filesystem filler: %v", err)
	}
	chunk := make([]byte, 64*1024)
	for {
		free, freeErr := boundaryFilesystemFreeBytes(testRoot)
		if freeErr != nil {
			_ = filler.Close()
			t.Fatalf("measure bridge filesystem free space: %v", freeErr)
		}
		if free < 16*1024 {
			break
		}
		written, writeErr := filler.Write(chunk)
		if written != len(chunk) && writeErr == nil {
			_ = filler.Close()
			t.Fatalf("fill bridge filesystem: short write %d/%d without error", written, len(chunk))
		}
		if writeErr != nil {
			if !errors.Is(writeErr, syscall.ENOSPC) {
				_ = filler.Close()
				t.Fatalf("fill bridge filesystem: %v", writeErr)
			}
			break
		}
	}
	if err := filler.Close(); err != nil {
		t.Fatalf("close bridge filesystem filler: %v", err)
	}
	checkpointWriteErr := bridge.recordTranscriptCheckpointProgressDetailedWithSourceProofAndParentFence(ctx, *session, transcriptPath, transcriptRecordCheckpointKey(record), line, offset, checkpointID, proof, session.ID)
	if checkpointWriteErr == nil || !boundaryDiskFullError(checkpointWriteErr) {
		t.Fatalf("bridge checkpoint after accepted Graph send error = %v, want ENOSPC/SQLITE_FULL", checkpointWriteErr)
	}
	unchanged, found, err := store.ImportCheckpoint(ctx, checkpointID)
	if err != nil || !found || unchanged.LastOffset != checkpointBefore.LastOffset || unchanged.LastRecordID != checkpointBefore.LastRecordID {
		t.Fatalf("bridge checkpoint advanced across ENOSPC: before=%#v after=%#v found=%v err=%v", checkpointBefore, unchanged, found, err)
	}
	if err := os.Remove(fillerPath); err != nil {
		t.Fatalf("release bridge filesystem capacity: %v", err)
	}
	storePath := store.Path()
	if err := store.Close(); err != nil {
		t.Fatalf("close bridge store after ENOSPC: %v", err)
	}
	recoveredStore, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("reopen bridge store after ENOSPC: %v", err)
	}
	t.Cleanup(func() { _ = recoveredStore.Close() })
	recovered := newBridgeTestBridge(graph, recoveredStore, &recordingExecutor{})
	recovered.registryPath = filepath.Join(testRoot, "recovered-registry.json")
	if err := recovered.restoreRegistryFromStore(ctx); err != nil {
		t.Fatalf("restore bridge registry after ENOSPC: %v", err)
	}
	if err := recovered.syncLinkedTranscripts(ctx); err != nil && !isOutboxDeliveryDeferred(err) {
		t.Fatalf("recover bridge transcript after ENOSPC: %v", err)
	}
	state, err = recoveredStore.Load(ctx)
	if err != nil {
		t.Fatalf("load recovered bridge state: %v", err)
	}
	checkpoint = state.ImportCheckpoints[checkpointID]
	if checkpoint.LastOffset != checkpoint.SourceSize {
		t.Fatalf("recovered bridge checkpoint = %#v, want transcript EOF", checkpoint)
	}
	if posts != 1 {
		t.Fatalf("recovered bridge issued duplicate Graph POSTs: %d, want one", posts)
	}
	var recoveredDelivery teamstore.TranscriptDeliveryRecord
	for _, delivery := range state.TranscriptDeliveries {
		if delivery.SessionID == session.ID && delivery.SourceRecordID == "bridge-new" {
			if recoveredDelivery.ID != "" {
				t.Fatalf("duplicate recovered bridge deliveries: first=%#v second=%#v", recoveredDelivery, delivery)
			}
			recoveredDelivery = delivery
		}
	}
	if recoveredDelivery.Status != teamstore.TranscriptDeliveryStatusSent || recoveredDelivery.OutboxID != sentOutbox.ID {
		t.Fatalf("recovered bridge delivery = %#v, want one sent delivery linked to durable outbox %#v", recoveredDelivery, sentOutbox)
	}
}

func boundaryFilesystemFreeBytes(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, fmt.Errorf("statfs %s: %w", path, err)
	}
	return stat.Bavail * uint64(stat.Bsize), nil
}

const boundaryTmpfsMagic = 0x01021994

func boundaryFilesystemType(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, fmt.Errorf("statfs %s: %w", path, err)
	}
	return uint64(stat.Type), nil
}

func boundaryDiskFullError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ENOSPC) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "database or disk is full") ||
		strings.Contains(message, "sqlite_full") ||
		strings.Contains(message, "no space left on device")
}
