package teams

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestTeamsCyclePhaseScopeAndProgress(t *testing.T) {
	bridge := &Bridge{phaseBudget: 5 * time.Millisecond}

	err := bridge.runMainLoopPhase(context.Background(), "slow-phase", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	})
	if !isTeamsPhaseDeadline(err) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("slow phase error = %v, want deadline-wrapped context error", err)
	}
	stats := bridge.mainLoopPhaseStatsSnapshot("slow-phase")
	if stats.Runs != 1 || stats.DeadlineExceeded != 1 || stats.Errors != 1 {
		t.Fatalf("slow phase stats = %#v, want one bounded failed run", stats)
	}

	progressed := false
	if err := bridge.runMainLoopPhase(context.Background(), "next-phase", func(context.Context) error {
		progressed = true
		return nil
	}); err != nil {
		t.Fatalf("next phase error: %v", err)
	}
	if !progressed {
		t.Fatal("next phase did not run after a bounded phase timeout")
	}
	if stats := bridge.mainLoopPhaseStatsSnapshot("next-phase"); stats.Runs != 1 || stats.Errors != 0 {
		t.Fatalf("next phase stats = %#v, want one successful run", stats)
	}
	if !teamstore.IsProcessWideStateError(teamstore.ErrControlLeaseNotHeld) {
		t.Fatal("control lease loss was not classified as process-wide")
	}
	if teamstore.IsProcessWideStateError(os.ErrNotExist) {
		t.Fatal("path-local missing input was classified as process-wide")
	}
}

// seedBridgeTestOutboxRows persists sender/recovery fixtures in one state
// transaction. These tests exercise outbox ordering and delivery, not the
// QueueOutbox admission path. Seeding after SQLite migration (or calling
// QueueOutbox once per row) adds a durable file/database sync for every row;
// that setup cost can dominate hosted Windows test budgets and obscure the
// behavior under test.
func seedBridgeTestOutboxRows(t testing.TB, ctx context.Context, store *teamstore.Store, rows ...teamstore.OutboxMessage) {
	t.Helper()
	if err := store.Update(ctx, func(state *teamstore.State) error {
		now := time.Now().UTC()
		for _, row := range rows {
			if row.Status == "" {
				row.Status = teamstore.OutboxStatusQueued
			}
			if row.CreatedAt.IsZero() {
				row.CreatedAt = now
			}
			if row.UpdatedAt.IsZero() {
				row.UpdatedAt = row.CreatedAt
			}
			if row.PartCount <= 0 {
				row.PartCount = 1
			}
			if row.PartIndex <= 0 {
				row.PartIndex = 1
			}
			state.OutboxMessages[row.ID] = row
		}
		return nil
	}); err != nil {
		t.Fatalf("seed outbox rows: %v", err)
	}
}

// TestTeamsActiveOutboxPredecessorUsesShortRetryGate verifies the race seen by
// the real listener: a global outbox flush can observe an earlier row that a
// targeted flush has already claimed.  The later final must remain behind the
// predecessor, but it must be rechecked promptly after that short-lived lease
// finishes rather than inheriting the 30-second recovery backoff.
func TestTeamsActiveOutboxPredecessorUsesShortRetryGate(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", useSQLite), func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			graph, sent := newBridgeTestGraph(t)
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			earlier := teamstore.OutboxMessage{
				ID: "outbox:active-predecessor", TeamsChatID: "chat-1", Sequence: 1,
				Kind: "ack", Body: "predecessor", Status: teamstore.OutboxStatusQueued,
				CreatedAt: time.Now().Add(-time.Second),
			}
			later := teamstore.OutboxMessage{
				ID: "outbox:active-final", TeamsChatID: "chat-1", Sequence: 2,
				Kind: "final", NotificationKind: "turn_completed", Body: "final", CreatedAt: time.Now(),
			}
			seedBridgeTestOutboxRows(t, ctx, store, earlier, later)
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			var err error
			if earlier, err = store.MarkOutboxSendAttempt(ctx, earlier.ID); err != nil {
				t.Fatalf("claim predecessor: %v", err)
			}
			if earlier.Status != teamstore.OutboxStatusSending || teamstore.OutboxSendIsAmbiguous(earlier) {
				t.Fatalf("claimed predecessor = %#v, want a normal active Sending row", earlier)
			}

			started := time.Now()
			err = bridge.sendQueuedOutboxWithOptions(ctx, later, outboxSendOptions{})
			var deferred outboxDeliveryDeferredError
			if !errors.As(err, &deferred) {
				t.Fatalf("later send error = %v, want active-predecessor deferral", err)
			}
			if deferred.Until.Before(started) || deferred.Until.After(started.Add(5*time.Second)) {
				t.Fatalf("active-predecessor retry gate = %s, want a short gate near %s", deferred.Until, started.Add(outboxActivePredecessorRetryBackoff))
			}
			if got := len(*sent); got != 0 {
				t.Fatalf("later send posted while predecessor was active: %d POST(s)", got)
			}

			if _, err := store.MarkOutboxSent(ctx, earlier.ID, "teams-predecessor"); err != nil {
				t.Fatalf("finish predecessor: %v", err)
			}
			latest, err := store.OutboxMessageByID(ctx, later.ID)
			if err != nil {
				t.Fatalf("reload final: %v", err)
			}
			if err := bridge.sendQueuedOutboxWithOptions(ctx, latest, outboxSendOptions{}); err != nil {
				t.Fatalf("send final after predecessor finished: %v", err)
			}
			if got := len(*sent); got != 1 {
				t.Fatalf("Graph POST count after predecessor finished = %d, want one final POST", got)
			}
			final, err := store.OutboxMessageByID(ctx, later.ID)
			if err != nil || final.Status != teamstore.OutboxStatusSent {
				t.Fatalf("final after predecessor finished = %#v, err=%v, want Sent", final, err)
			}
		})
	}
}

func TestTeamsAcceptedOutboxPredecessorUsesShortRetryGate(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", useSQLite), func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			graph, sent := newBridgeTestGraph(t)
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			now := time.Now().UTC()
			earlier := teamstore.OutboxMessage{
				ID: "outbox:accepted-predecessor", TeamsChatID: "chat-1", Sequence: 1,
				Kind: "ack", Body: "accepted predecessor", Status: teamstore.OutboxStatusAccepted,
				TeamsMessageID: "teams-accepted-predecessor", CreatedAt: now.Add(-time.Second),
			}
			later := teamstore.OutboxMessage{
				ID: "outbox:accepted-tail", TeamsChatID: "chat-1", Sequence: 2,
				TurnID: "turn:accepted-tail", Kind: "final", NotificationKind: "turn_completed",
				Body: "accepted tail", CreatedAt: now,
			}
			seedBridgeTestOutboxRows(t, ctx, store, earlier, later)
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}

			started := time.Now()
			err := bridge.sendQueuedOutboxWithOptions(ctx, later, outboxSendOptions{})
			var deferred outboxDeliveryDeferredError
			if !errors.As(err, &deferred) {
				t.Fatalf("tail send error = %v, want accepted-predecessor deferral", err)
			}
			if deferred.Until.Before(started) || deferred.Until.After(started.Add(5*time.Second)) {
				t.Fatalf("accepted-predecessor retry gate = %s, want a short gate near %s", deferred.Until, started.Add(outboxActivePredecessorRetryBackoff))
			}
			if got := len(*sent); got != 0 {
				t.Fatalf("tail send posted before accepted predecessor promotion: %d POST(s)", got)
			}

			if _, err := store.MarkOutboxSent(ctx, earlier.ID, earlier.TeamsMessageID); err != nil {
				t.Fatalf("finish accepted predecessor: %v", err)
			}
			latest, err := store.OutboxMessageByID(ctx, later.ID)
			if err != nil {
				t.Fatalf("reload accepted tail: %v", err)
			}
			if err := bridge.sendQueuedOutboxWithOptions(ctx, latest, outboxSendOptions{}); err != nil {
				t.Fatalf("send accepted tail after promotion: %v", err)
			}
			if got := len(*sent); got != 1 {
				t.Fatalf("Graph POST count after accepted predecessor promotion = %d, want one", got)
			}
		})
	}
}

// TestTeamsSameTurnAmbiguousOutboxNeverOvertakesProtectedChunk exercises the
// production sender boundary after the store's FIFO query has classified an
// ambiguous predecessor.  Rows from another turn may continue in the same
// chat, but chunks of one terminal turn must not pass an earlier chunk whose
// external Graph result is still unknown.
func TestTeamsSameTurnAmbiguousOutboxNeverOvertakesProtectedChunk(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(fmt.Sprintf("sqlite=%t", useSQLite), func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			graph, sent := newBridgeTestGraph(t)
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			now := time.Now().UTC()
			ambiguous := teamstore.OutboxMessage{
				ID: "outbox:same-turn-protected", TeamsChatID: "chat-1", TurnID: "turn:same-turn",
				Sequence: 1, Kind: "final-part", Body: "protected part", Status: teamstore.OutboxStatusSending,
				SendAttemptToken: "attempt:same-turn-protected", LastSendAttempt: now.Add(-time.Hour),
				LastSendError: "ambiguous Graph send; response lost", CreatedAt: now,
			}
			later := teamstore.OutboxMessage{
				ID: "outbox:same-turn-terminal", TeamsChatID: "chat-1", TurnID: ambiguous.TurnID,
				Sequence: 2, Kind: "final", NotificationKind: "turn_completed", Body: "terminal part",
				Status: teamstore.OutboxStatusQueued, CreatedAt: now.Add(time.Second),
			}
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.OutboxMessages[ambiguous.ID] = ambiguous
				state.OutboxMessages[later.ID] = later
				return nil
			}); err != nil {
				t.Fatalf("seed same-turn ambiguous rows: %v", err)
			}

			err := bridge.sendQueuedOutboxWithOptions(ctx, later, outboxSendOptions{})
			var deferred outboxDeliveryDeferredError
			if !errors.As(err, &deferred) {
				t.Fatalf("same-turn terminal send error = %v, want protected FIFO deferral", err)
			}
			if got := len(*sent); got != 0 {
				t.Fatalf("same-turn terminal overtook ambiguous predecessor with %d Graph POST(s)", got)
			}
			persisted, err := store.OutboxMessageByID(ctx, later.ID)
			if err != nil {
				t.Fatalf("load same-turn terminal: %v", err)
			}
			if persisted.Status != teamstore.OutboxStatusQueued {
				t.Fatalf("same-turn terminal was mutated while predecessor was protected: %#v", persisted)
			}
		})
	}
}

func TestTeamsReconcilePendingSentOutboxSideEffectsAfterRestart(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Now().UTC()
	const outboxID = "outbox:post-send-replay"
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.OutboxMessages[outboxID] = teamstore.OutboxMessage{
			ID: outboxID, TeamsChatID: "chat:post-send-replay", Kind: "ack",
			Status: teamstore.OutboxStatusSent, TeamsMessageID: "teams:post-send-replay",
			PostSendEffectsPending: true, CreatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed post-send replay row: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate post-send replay row: %v", err)
	}

	// A newly constructed bridge represents the next listener generation. The
	// recovery lane must settle local post-send bookkeeping from the durable
	// marker and must not need (or issue) another Graph POST.
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	if err := bridge.reconcilePendingSentOutboxSideEffects(ctx); err != nil {
		t.Fatalf("reconcile pending post-send effects: %v", err)
	}
	row, err := store.OutboxMessageByID(ctx, outboxID)
	if err != nil {
		t.Fatalf("reload post-send replay row: %v", err)
	}
	if row.Status != teamstore.OutboxStatusSent || row.TeamsMessageID != "teams:post-send-replay" || row.PostSendEffectsPending {
		t.Fatalf("post-send replay row after recovery = %#v, want Sent with marker cleared", row)
	}
}

func TestOutboxPredecessorRetryGateDoesNotReuseExpiredAttemptTimestamp(t *testing.T) {
	now := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		row  teamstore.OutboxMessage
	}{
		{
			name: "expired markerless",
			row: teamstore.OutboxMessage{
				Status: teamstore.OutboxStatusSending, LastSendAttempt: now.Add(-time.Hour),
			},
		},
		{
			name: "expired ambiguous",
			row: teamstore.OutboxMessage{
				Status: teamstore.OutboxStatusSending, LastSendAttempt: now.Add(-time.Hour),
				SendAttemptToken: "attempt:ambiguous", LastSendError: "ambiguous Graph send; response lost",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			until := outboxPredecessorRetryAt(tc.row, now)
			if !until.After(now) {
				t.Fatalf("retry gate = %s, want a future recovery wake-up", until)
			}
			if until.Before(now.Add(outboxRecoveryRetryBackoff)) {
				t.Fatalf("retry gate = %s, want at least the recovery backoff", until)
			}
		})
	}
}

func TestTeamsOwnerCapabilityTakeover(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	now := time.Now().UTC()
	first, acquired, err := store.BeginChatPollAttempt(ctx, teamstore.ChatPollAttemptRequest{
		ChatID:             "owner-capability-chat",
		Owner:              "owner-a",
		ProcessIncarnation: "process-a",
		LeaseGeneration:    1,
		Now:                now,
		TTL:                time.Minute,
	})
	if err != nil || !acquired || first.Attempt == nil {
		t.Fatalf("begin first poll attempt acquired=%v attempt=%#v err=%v", acquired, first.Attempt, err)
	}
	capability := teamstore.ChatPollAttemptCapability{
		ID:                 first.Attempt.ID,
		Owner:              first.Attempt.Owner,
		ProcessIncarnation: first.Attempt.ProcessIncarnation,
		LeaseGeneration:    first.Attempt.LeaseGeneration,
	}

	if _, changed, err := store.UpdateChatPoll(ctx, "owner-capability-chat", func(poll *teamstore.ChatPollState) error {
		if poll.Attempt == nil {
			return errors.New("poll attempt disappeared during takeover")
		}
		poll.Attempt.Owner = "owner-b"
		poll.Attempt.ProcessIncarnation = "process-b"
		poll.Attempt.LeaseGeneration = 2
		// This is a valid replacement capability, not a stale callback. The
		// targeted writer advances the row revision and keeps the attempt alive.
		poll.Attempt.ExpectedPollRevision = poll.PollRevision + 1
		return nil
	}); err != nil || !changed {
		t.Fatalf("replace poll capability changed=%v err=%v", changed, err)
	}
	current, found, err := store.ChatPoll(ctx, "owner-capability-chat")
	if err != nil || !found || current.Attempt == nil {
		t.Fatalf("current poll found=%v attempt=%#v err=%v", found, current.Attempt, err)
	}
	if _, committed, err := store.CommitChatPollAttemptWithCapability(ctx, "owner-capability-chat", capability, current.PollRevision, func(poll *teamstore.ChatPollState) error {
		poll.LastError = "stale callback must not win"
		return nil
	}); err != nil || committed {
		t.Fatalf("stale capability commit committed=%v err=%v, want no commit", committed, err)
	}

	replacement := capability
	replacement.Owner = "owner-b"
	replacement.ProcessIncarnation = "process-b"
	replacement.LeaseGeneration = 2
	current, found, err = store.ChatPoll(ctx, "owner-capability-chat")
	if err != nil || !found || current.Attempt == nil {
		t.Fatalf("poll after stale callback found=%v attempt=%#v err=%v", found, current.Attempt, err)
	}
	if _, committed, err := store.CommitChatPollAttemptWithCapability(ctx, "owner-capability-chat", replacement, current.PollRevision, func(poll *teamstore.ChatPollState) error {
		poll.LastError = "replacement callback committed"
		return nil
	}); err != nil || !committed {
		t.Fatalf("replacement capability commit committed=%v err=%v, want commit", committed, err)
	}
	final, found, err := store.ChatPoll(ctx, "owner-capability-chat")
	if err != nil || !found || final.Attempt != nil || final.LastError != "replacement callback committed" {
		t.Fatalf("final poll found=%v state=%#v err=%v", found, final, err)
	}
}

func TestTeamsTranscriptSourceIdentityReplacement(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rollout.jsonl")
	original := []byte("stable transcript prefix\n")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatalf("write original transcript: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat original transcript: %v", err)
	}
	checkpoint := teamstore.HistoryWatchCheckpoint{
		ID:                historyWatchCheckpointID(path),
		Path:              path,
		Size:              info.Size(),
		ModTime:           info.ModTime(),
		Offset:            info.Size(),
		SourceFingerprint: transcriptCheckpointSourceFingerprint(path, info.Size()),
	}
	if checkpoint.SourceFingerprint == "" {
		t.Fatal("missing original source fingerprint")
	}

	replacementPath := filepath.Join(dir, "rollout.replacement.jsonl")
	if err := os.WriteFile(replacementPath, original, 0o600); err != nil {
		t.Fatalf("write replacement transcript: %v", err)
	}
	if err := os.Chtimes(replacementPath, info.ModTime(), info.ModTime()); err != nil {
		t.Fatalf("set replacement mtime: %v", err)
	}
	if err := os.Rename(replacementPath, path); err != nil {
		t.Fatalf("atomically replace transcript: %v", err)
	}

	changes, err := historyWatchChangedPaths([]string{path}, teamstore.State{HistoryWatch: map[string]teamstore.HistoryWatchCheckpoint{
		checkpoint.ID: checkpoint,
	}}, true)
	if err != nil {
		t.Fatalf("historyWatchChangedPaths: %v", err)
	}
	if len(changes) != 1 || cleanComparablePath(changes[0]) != cleanComparablePath(path) {
		t.Fatalf("same-size replacement changes = %#v, want %q", changes, path)
	}
}

func TestTeamsTranscriptClaimSourceRewriteFence(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "claimed.jsonl")
	original := []byte("trusted prefix\n")
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatalf("write original transcript: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat original transcript: %v", err)
	}
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	checkpointID := transcriptCheckpointID("s001")
	checkpoint := teamstore.ImportCheckpoint{
		ID:                checkpointID,
		SessionID:         "s001",
		SourcePath:        path,
		SourceFingerprint: transcriptCheckpointSourceFingerprint(path, info.Size()),
		LastOffset:        info.Size(),
		LastOffsetKnown:   true,
		SourceSize:        info.Size(),
		SourceModTime:     info.ModTime(),
		Status:            "complete",
	}
	if _, _, err := store.UpdateImportCheckpoint(ctx, checkpointID, func(teamstore.ImportCheckpoint, bool, time.Time) (teamstore.ImportCheckpoint, bool, error) {
		return checkpoint, true, nil
	}); err != nil {
		t.Fatalf("seed import checkpoint: %v", err)
	}
	outbox, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:                               "outbox:claimed-source-rewrite",
		SessionID:                        "s001",
		TeamsChatID:                      "chat-1",
		TurnID:                           "sync:turn-1",
		Kind:                             "sync-assistant",
		NotificationKind:                 "turn_completed",
		Body:                             "stale answer",
		TranscriptCheckpointID:           checkpointID,
		TranscriptSourcePath:             path,
		TranscriptSourceProofFingerprint: checkpoint.SourceFingerprint,
		TranscriptSourceProofOffset:      info.Size(),
		TranscriptSourceProofOffsetKnown: true,
	})
	if err != nil || outbox.ID == "" {
		t.Fatalf("queue source-bound outbox = %#v err=%v", outbox, err)
	}
	claimed, err := store.MarkOutboxSendAttempt(ctx, outbox.ID)
	if err != nil {
		t.Fatalf("claim source-bound outbox: %v", err)
	}

	replacementPath := filepath.Join(dir, "claimed.replacement.jsonl")
	if err := os.WriteFile(replacementPath, original, 0o600); err != nil {
		t.Fatalf("write replacement transcript: %v", err)
	}
	if err := os.Chtimes(replacementPath, info.ModTime(), info.ModTime()); err != nil {
		t.Fatalf("set replacement mtime: %v", err)
	}
	if err := os.Rename(replacementPath, path); err != nil {
		t.Fatalf("replace claimed transcript: %v", err)
	}

	if err := bridge.retireTranscriptOutboxBeforePostSourceRewrite(ctx, claimed, "source changed during claim"); err != nil {
		t.Fatalf("retire claimed source-bound outbox: %v", err)
	}
	fenced, err := store.OutboxMessageByID(ctx, outbox.ID)
	if err != nil {
		t.Fatalf("load fenced outbox: %v", err)
	}
	if fenced.Status != teamstore.OutboxStatusSkipped {
		t.Fatalf("fenced outbox status = %q, want skipped", fenced.Status)
	}
	blocked, found, err := store.ImportCheckpoint(ctx, checkpointID)
	if err != nil || !found || !blocked.SourceRewriteBlocked || blocked.Status != "blocked" {
		t.Fatalf("fenced checkpoint found=%v state=%#v err=%v", found, blocked, err)
	}
}

func TestTeamsExternalSideEffectRecovery(t *testing.T) {
	ctx := context.Background()
	var posts int
	graph := &GraphClient{
		auth: &fakeGraphAuth{token: "access"},
		client: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			posts++
			return nil, errors.New("connection reset by peer")
		})},
		baseURL:    graphBaseURL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	row, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:          "outbox:ambiguous-post",
		TeamsChatID: "ambiguous-chat",
		Kind:        "helper",
		Body:        "must not duplicate",
	})
	if err != nil {
		t.Fatalf("queue ambiguous outbox: %v", err)
	}
	_ = bridge.flushPendingOutboxForChat(ctx, "ambiguous-chat")
	if posts != 1 {
		t.Fatalf("first flush posts = %d, want one POST attempt", posts)
	}
	row, err = store.OutboxMessageByID(ctx, row.ID)
	if err != nil || !teamstore.OutboxSendIsAmbiguous(row) || row.Status != teamstore.OutboxStatusSending {
		t.Fatalf("ambiguous row after first flush = %#v err=%v", row, err)
	}
	_ = bridge.flushPendingOutboxForChat(ctx, "ambiguous-chat")
	if posts != 1 {
		t.Fatalf("second flush posts = %d, want no replay of unknown POST", posts)
	}
}

func TestTeamsHistoryWatchErrorIsolation(t *testing.T) {
	dir := t.TempDir()
	good := filepath.Join(dir, "healthy.jsonl")
	if err := os.WriteFile(good, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write healthy transcript: %v", err)
	}
	bad := filepath.Join(dir, "bad\x00.jsonl")
	changes, err := historyWatchChangedPaths([]string{bad, good}, teamstore.State{HistoryWatch: map[string]teamstore.HistoryWatchCheckpoint{
		historyWatchCheckpointID(good): {
			ID:   historyWatchCheckpointID(good),
			Path: good,
			Size: 0,
		},
	}}, false)
	if err == nil {
		t.Fatal("bad path did not produce a path-local diagnostic")
	}
	seenHealthy := false
	for _, path := range changes {
		if cleanComparablePath(path) == cleanComparablePath(good) {
			seenHealthy = true
			break
		}
	}
	if !seenHealthy {
		t.Fatalf("healthy path was starved by bad path: changes=%#v err=%v", changes, err)
	}
	if !teamstore.IsProcessWideStateError(teamstore.ErrControlLeaseNotHeld) {
		t.Fatal("lease loss should stop durable writes for the remaining phase")
	}
}

func TestTeamsLinkedTranscriptMissingSourceDoesNotStarveHealthySession(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	badPath := filepath.Join(dir, "bad-linked.jsonl")
	goodPath := filepath.Join(dir, "healthy-linked.jsonl")
	badInitial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-bad-linked"}}`,
		`{"type":"event_msg","payload":{"id":"bad-old","type":"agent_message","turn_id":"bad-old-turn","phase":"final_answer","message":"old bad answer"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"bad-old-turn"}}`,
	}, "\n") + "\n"
	goodInitial := strings.Join([]string{
		`{"type":"session_meta","payload":{"id":"thread-healthy-linked"}}`,
		`{"type":"event_msg","payload":{"id":"good-old","type":"agent_message","turn_id":"good-old-turn","phase":"final_answer","message":"old healthy answer"}}`,
		`{"type":"event_msg","payload":{"type":"task_complete","turn_id":"good-old-turn"}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(badPath, []byte(badInitial), 0o600); err != nil {
		t.Fatalf("write bad linked transcript: %v", err)
	}
	if err := os.WriteFile(goodPath, []byte(goodInitial), 0o600); err != nil {
		t.Fatalf("write healthy linked transcript: %v", err)
	}
	badInfo, err := os.Stat(badPath)
	if err != nil {
		t.Fatalf("stat bad linked transcript: %v", err)
	}
	goodInfo, err := os.Stat(goodPath)
	if err != nil {
		t.Fatalf("stat healthy linked transcript: %v", err)
	}
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	badSession := bridge.reg.SessionByID("s001")
	if badSession == nil {
		t.Fatal("missing first linked session")
	}
	badSession.CodexThreadID = "thread-bad-linked"
	if err := bridge.ensureDurableSession(ctx, badSession); err != nil {
		t.Fatalf("ensure bad linked session: %v", err)
	}
	goodSession := appendBridgeTestSession(t, bridge, store, "s002", "chat-2")
	goodSession.CodexThreadID = "thread-healthy-linked"
	if _, _, err := store.UpdateSessionContext(ctx, goodSession.ID, func(current teamstore.SessionContext, found bool, now time.Time) (teamstore.SessionContext, bool, error) {
		if !found {
			return current, false, fmt.Errorf("healthy linked session was not persisted")
		}
		current.CodexThreadID = goodSession.CodexThreadID
		current.UpdatedAt = now
		return current, true, nil
	}); err != nil {
		t.Fatalf("persist healthy linked session thread: %v", err)
	}
	seedCheckpoint := func(session *Session, path string, info os.FileInfo, recordID string, line int) {
		t.Helper()
		checkpointID := transcriptCheckpointID(session.ID)
		checkpoint := teamstore.ImportCheckpoint{
			ID:                   checkpointID,
			SessionID:            session.ID,
			SourcePath:           path,
			SourceGeneration:     historyTieredSourceIdentity(path, info),
			SourceFingerprint:    transcriptCheckpointSourceFingerprint(path, info.Size()),
			LastRecordID:         recordID,
			LastSourceLine:       line,
			LastOffset:           info.Size(),
			LastOffsetKnown:      true,
			SourceSize:           info.Size(),
			SourceModTime:        info.ModTime(),
			SourceRewriteBlocked: session.ID == "s001",
			Status:               map[bool]string{true: importCheckpointStatusBlocked, false: importCheckpointStatusComplete}[session.ID == "s001"],
			HistoryRootReleased:  session.ID == "s002",
			UpdatedAt:            time.Now(),
		}
		if checkpoint.SourceFingerprint == "" || checkpoint.SourceGeneration == "" {
			t.Fatalf("incomplete checkpoint proof for %s: %#v", session.ID, checkpoint)
		}
		if _, _, err := store.UpdateImportCheckpoint(ctx, checkpointID, func(teamstore.ImportCheckpoint, bool, time.Time) (teamstore.ImportCheckpoint, bool, error) {
			return checkpoint, true, nil
		}); err != nil {
			t.Fatalf("seed checkpoint %s: %v", session.ID, err)
		}
	}
	seedCheckpoint(badSession, badPath, badInfo, "bad-old", 3)
	seedCheckpoint(goodSession, goodPath, goodInfo, "good-old", 3)
	// Remove the source after its checkpoint has been persisted. A missing
	// linked source is a stable, cross-platform source-local failure (unlike
	// POSIX mode bits, which do not make a file unreadable on Windows). The
	// healthy session must still be processed in the same discovery cycle.
	if err := os.Remove(badPath); err != nil {
		t.Fatalf("remove bad linked transcript: %v", err)
	}
	appendLine(t, goodPath, `{"type":"event_msg","payload":{"type":"task_started","turn_id":"good-new-turn","started_at":1786181090,"model_context_window":128000,"collaboration_mode_kind":"default"}}`)
	appendLine(t, goodPath, `{"type":"response_item","thread_id":"thread-healthy-linked","payload":{"id":"good-prompt","type":"message","role":"user","content":[{"type":"input_text","text":"healthy linked prompt"}]}}`)
	appendLine(t, goodPath, `{"type":"event_msg","payload":{"id":"good-new","type":"agent_message","turn_id":"good-new-turn","phase":"final_answer","message":"healthy linked answer"}}`)
	appendLine(t, goodPath, `{"type":"event_msg","payload":{"type":"task_complete","turn_id":"good-new-turn"}}`)
	err = bridge.syncLinkedTranscripts(ctx)
	flushBridgeQueuedNotificationsForTest(t, bridge)
	if err != nil && !strings.Contains(err.Error(), "Codex sessions dir not found") {
		t.Fatalf("missing linked source returned an unexpected non-local error: %v", err)
	}
	if !sentPlainContains(*sent, "healthy linked answer") {
		t.Fatalf("healthy linked session was starved by an earlier unreadable source: %#v", *sent)
	}
}

func TestTeamsOutboxAttemptBudgetAndFairness(t *testing.T) {
	ctx := context.Background()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	base := time.Now().UTC().Add(-time.Hour)
	rows := make([]teamstore.OutboxMessage, 0, mainLoopOutboxFlushMaxScannedMessages+1)
	for i := 0; i < mainLoopOutboxFlushMaxScannedMessages; i++ {
		rows = append(rows, teamstore.OutboxMessage{
			ID:                     fmt.Sprintf("outbox:blocked:%02d", i),
			TeamsChatID:            "blocked-chat",
			Kind:                   "helper",
			Body:                   "blocked",
			BlockedBySourceRewrite: true,
			Sequence:               int64(i + 1),
			CreatedAt:              base.Add(time.Duration(i) * time.Nanosecond),
		})
	}
	healthyID := "outbox:healthy-after-blocked"
	rows = append(rows, teamstore.OutboxMessage{
		ID:          healthyID,
		TeamsChatID: "healthy-chat",
		Kind:        "helper",
		Body:        "healthy",
		Sequence:    mainLoopOutboxFlushMaxScannedMessages + 1,
		CreatedAt:   base.Add(time.Duration(mainLoopOutboxFlushMaxScannedMessages+1) * time.Nanosecond),
	})
	seedBridgeTestOutboxRows(t, ctx, store, rows...)
	if err := bridge.flushPendingOutboxMainLoop(ctx); err != nil {
		t.Fatalf("first bounded outbox flush: %v", err)
	}
	if len(*sent) != 0 {
		t.Fatalf("first flush sent = %#v, want blocked prefix to consume scan budget", *sent)
	}
	all, err := store.PendingOutboxPageAt(ctx, teamstore.PendingOutboxQuery{Now: time.Now(), Limit: 128, IgnoreRetryGate: true})
	if err != nil {
		t.Fatalf("load bounded outbox page: %v", err)
	}
	for _, row := range all.Messages {
		if row.TeamsChatID == "blocked-chat" && row.NextAttemptAt.IsZero() {
			t.Fatalf("blocked row was not durably gated: %#v", row)
		}
	}
	if err := bridge.flushPendingOutboxMainLoop(ctx); err != nil {
		t.Fatalf("second bounded outbox flush: %v", err)
	}
	if len(*sent) != 1 || (*sent)[0].ChatID != "healthy-chat" {
		t.Fatalf("healthy chat did not bypass gated scan prefix: %#v", *sent)
	}
}

func TestTeamsUnresolvedTranscriptOutboxDoesNotLivelockHealthyTail(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			graph, sent := newBridgeTestGraph(t)
			store := newBridgeTestStore(t)
			const sessionID = "s001"
			const chatID = "chat-1"
			now := time.Now().UTC().Add(-time.Hour)
			sourcePath := filepath.Join(t.TempDir(), "s001.jsonl")
			sourceBody := []byte("blocked transcript proof\n")
			if err := os.WriteFile(sourcePath, sourceBody, 0o600); err != nil {
				t.Fatalf("write source proof fixture: %v", err)
			}
			sourceProof := transcriptCheckpointSourceFingerprint(sourcePath, int64(len(sourceBody)))
			if err := store.Update(ctx, func(state *teamstore.State) error {
				checkpointID := transcriptCheckpointID(sessionID)
				state.ImportCheckpoints[checkpointID] = teamstore.ImportCheckpoint{
					ID: checkpointID, SessionID: sessionID, Status: importCheckpointStatusBlocked,
					UnresolvedExecution: &teamstore.ExecutionAnchor{
						SessionID: sessionID, State: executionAnchorStateUnresolved,
						Generation: 7, OuterTurnID: "turn:unresolved-prefix",
					},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed unresolved execution checkpoint: %v", err)
			}
			rows := make([]teamstore.OutboxMessage, 0, mainLoopOutboxFlushMaxScannedMessages+1)
			for i := 0; i < mainLoopOutboxFlushMaxScannedMessages; i++ {
				rows = append(rows, teamstore.OutboxMessage{
					ID:                               fmt.Sprintf("outbox:unresolved:%02d", i),
					SessionID:                        sessionID,
					TeamsChatID:                      chatID,
					TurnID:                           fmt.Sprintf("sync:unresolved:%02d", i),
					Kind:                             "sync-assistant",
					NotificationKind:                 "turn_completed",
					Body:                             "blocked transcript answer",
					TranscriptCheckpointID:           transcriptCheckpointID(sessionID),
					TranscriptSourcePath:             sourcePath,
					TranscriptSourceProofFingerprint: sourceProof,
					TranscriptSourceProofOffset:      int64(len(sourceBody)),
					TranscriptSourceProofOffsetKnown: true,
					Sequence:                         int64(i + 1),
					CreatedAt:                        now.Add(time.Duration(i) * time.Nanosecond),
				})
			}
			healthyID := "outbox:healthy-after-unresolved"
			rows = append(rows, teamstore.OutboxMessage{
				ID: healthyID, TeamsChatID: "healthy-chat", Kind: "helper", Body: "healthy tail",
				Sequence:  mainLoopOutboxFlushMaxScannedMessages + 1,
				CreatedAt: now.Add(time.Duration(mainLoopOutboxFlushMaxScannedMessages+1) * time.Nanosecond),
			})
			seedBridgeTestOutboxRows(t, ctx, store, rows...)
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate store to SQLite: %v", err)
				}
			}

			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			if err := bridge.flushPendingOutboxMainLoop(ctx); err != nil {
				t.Fatalf("first outbox flush: %v", err)
			}
			if len(*sent) != 0 {
				t.Fatalf("first flush sent = %#v, want only deferred unresolved prefix", *sent)
			}
			for i := 0; i < mainLoopOutboxFlushMaxScannedMessages; i++ {
				row, err := store.OutboxMessageByID(ctx, fmt.Sprintf("outbox:unresolved:%02d", i))
				if err != nil {
					t.Fatalf("load unresolved row %d: %v", i, err)
				}
				if row.Status != teamstore.OutboxStatusQueued || row.NextAttemptAt.IsZero() {
					t.Fatalf("unresolved row %d = %#v, want queued with durable retry gate", i, row)
				}
			}

			if err := bridge.flushPendingOutboxMainLoop(ctx); err != nil {
				t.Fatalf("second outbox flush: %v", err)
			}
			if len(*sent) != 1 || (*sent)[0].ChatID != "healthy-chat" || (*sent)[0].Content == "" {
				t.Fatalf("healthy tail did not bypass deferred unresolved prefix: %#v", *sent)
			}
			healthy, err := store.OutboxMessageByID(ctx, healthyID)
			if err != nil {
				t.Fatalf("load healthy tail: %v", err)
			}
			if healthy.Status != teamstore.OutboxStatusSent {
				t.Fatalf("healthy tail status = %q, want sent", healthy.Status)
			}
		})
	}
}

// TestTeamsUnresolvedTranscriptOutboxUsesFutureRetryGate protects the sender
// boundary directly. A legacy row may have an old CreatedAt/LastSendAttempt,
// but an unresolved execution fence must still return a future gate; otherwise
// the flusher falls back to its generic short retry and can revisit a slow
// unresolved prefix before the current pass has finished.
func TestTeamsUnresolvedTranscriptOutboxUsesFutureRetryGate(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			ctx := context.Background()
			graph, sent := newBridgeTestGraph(t)
			store := newBridgeTestStore(t)
			const sessionID = "s001"
			const chatID = "chat-1"
			sourcePath := filepath.Join(t.TempDir(), "s001.jsonl")
			sourceBody := []byte("blocked transcript proof\n")
			if err := os.WriteFile(sourcePath, sourceBody, 0o600); err != nil {
				t.Fatalf("write source proof fixture: %v", err)
			}
			sourceProof := transcriptCheckpointSourceFingerprint(sourcePath, int64(len(sourceBody)))
			checkpointID := transcriptCheckpointID(sessionID)
			if err := store.Update(ctx, func(state *teamstore.State) error {
				state.ImportCheckpoints[checkpointID] = teamstore.ImportCheckpoint{
					ID: checkpointID, SessionID: sessionID, Status: importCheckpointStatusBlocked,
					UnresolvedExecution: &teamstore.ExecutionAnchor{
						SessionID: sessionID, State: executionAnchorStateUnresolved,
						Generation: 7, OuterTurnID: "turn:unresolved-prefix",
					},
				}
				return nil
			}); err != nil {
				t.Fatalf("seed unresolved execution checkpoint: %v", err)
			}
			row := teamstore.OutboxMessage{
				ID:                               "outbox:unresolved-future-gate",
				SessionID:                        sessionID,
				TeamsChatID:                      chatID,
				TurnID:                           "sync:unresolved-future-gate",
				Kind:                             "sync-assistant",
				NotificationKind:                 "turn_completed",
				Body:                             "blocked transcript answer",
				TranscriptCheckpointID:           checkpointID,
				TranscriptSourcePath:             sourcePath,
				TranscriptSourceProofFingerprint: sourceProof,
				TranscriptSourceProofOffset:      int64(len(sourceBody)),
				TranscriptSourceProofOffsetKnown: true,
				CreatedAt:                        time.Now().Add(-time.Hour),
				LastSendAttempt:                  time.Now().Add(-time.Hour),
			}
			seedBridgeTestOutboxRows(t, ctx, store, row)
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate store to SQLite: %v", err)
				}
			}

			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			started := time.Now()
			err := bridge.sendQueuedOutboxWithOptions(ctx, row, outboxSendOptions{SkipUnresolvedTranscript: true})
			var deferred outboxDeliveryDeferredError
			if !errors.As(err, &deferred) {
				t.Fatalf("send error = %v, want unresolved-transcript deferral", err)
			}
			if !deferred.Until.After(started.Add(outboxUnresolvedAnchorRetryBackoff - time.Second)) {
				t.Fatalf("unresolved-transcript retry gate = %s, want a future %s gate", deferred.Until, outboxUnresolvedAnchorRetryBackoff)
			}
			if len(*sent) != 0 {
				t.Fatalf("unresolved transcript row posted %d Graph message(s)", len(*sent))
			}
		})
	}
}

func TestTeamsOutboxPacingReservation(t *testing.T) {
	var mu sync.Mutex
	var sleeps []time.Duration
	graph := &GraphClient{
		baseURL: graphBaseURL,
		sleep: func(_ context.Context, d time.Duration) error {
			mu.Lock()
			sleeps = append(sleeps, d)
			mu.Unlock()
			return nil
		},
	}
	bridge := &Bridge{graph: graph}
	for i := 0; i < 3; i++ {
		if err := bridge.waitForOutboxSendPace(context.Background(), "same-chat"); err != nil {
			t.Fatalf("same-chat pacing %d: %v", i, err)
		}
	}
	if err := bridge.waitForOutboxSendPace(context.Background(), "other-chat"); err != nil {
		t.Fatalf("other-chat pacing: %v", err)
	}
	mu.Lock()
	got := append([]time.Duration(nil), sleeps...)
	mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("pacing sleeps = %#v, want two same-chat sleeps", got)
	}
	if got[1] <= graphOutboxSendMinInterval+graphOutboxSendMinInterval/3 {
		t.Fatalf("third same-chat slot delay = %s, want reservation beyond one interval", got[1])
	}
}

func TestTeamsQueuedAdmissionBoundedStart(t *testing.T) {
	graph, _ := newBridgeQueuedTurnGraph(t, map[string]string{
		"admission-first":  "first queued",
		"admission-second": "second queued",
	})
	store := newBridgeTestStore(t)
	executor := &parallelBlockingExecutor{
		started: make(chan parallelSessionStart, 4),
		release: make(chan struct{}),
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.asyncTurns = true
	bridge.maxQueuedTurnStartsPerCycle = 1
	first := bridge.reg.SessionByID("s001")
	if first == nil {
		t.Fatal("missing base session")
	}
	if err := bridge.ensureDurableSession(context.Background(), first); err != nil {
		t.Fatalf("ensure first session: %v", err)
	}
	second := appendBridgeTestSession(t, bridge, store, "s002", "chat-2")
	queueBridgeTurnForTest(t, bridge, first, "admission-first", "first queued", time.Now().Add(-time.Minute))
	queueBridgeTurnForTest(t, bridge, second, "admission-second", "second queued", time.Now())
	started, err := bridge.processQueuedTurnsWithStartBudget(context.Background(), 1, true)
	if err != nil || started != 1 {
		t.Fatalf("bounded admission started=%d err=%v, want one", started, err)
	}
	select {
	case got := <-executor.started:
		if got.SessionID != first.ID {
			t.Fatalf("first admitted session = %#v, want %s", got, first.ID)
		}
	case <-time.After(bridgeAsyncTestTimeout):
		t.Fatal("first queued turn did not start")
	}
	started, err = bridge.processQueuedTurnsWithStartBudget(context.Background(), 0, true)
	if err != nil || started != 0 {
		t.Fatalf("zero-budget admission started=%d err=%v, want no start", started, err)
	}
	select {
	case got := <-executor.started:
		close(executor.release)
		t.Fatalf("second session bypassed admission budget: %#v", got)
	case <-time.After(50 * time.Millisecond):
	}
	close(executor.release)
	waitForBridgeAsyncTurns(t, bridge)
	waitForCompletedTurnCount(t, store, first.ID, 1)
	waitForCompletedTurnCount(t, store, second.ID, 0)
}

func TestTeamsQueuedAdmissionPhaseDoesNotCancelStartedTurn(t *testing.T) {
	graph, _ := newBridgeQueuedTurnGraph(t, map[string]string{
		"admission-phase": "phase-started queued turn",
	})
	store := newBridgeTestStore(t)
	executor := &parallelBlockingExecutor{
		started: make(chan parallelSessionStart, 1),
		release: make(chan struct{}),
	}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.asyncTurns = true
	// The phase cancellation under test happens after the callback returns. Use
	// the normal async-test budget for admission bookkeeping: one-second
	// deadlines are not stable on hosted Windows runners under full-suite load
	// and would test runner scheduling rather than execution-context ownership.
	bridge.phaseBudget = bridgeAsyncTestTimeout
	session := bridge.reg.SessionByID("s001")
	if session == nil {
		t.Fatal("missing base session")
	}
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensure durable session: %v", err)
	}
	queueBridgeTurnForTest(t, bridge, session, "admission-phase", "phase-started queued turn", time.Now())

	listenerCtx, cancelListener := context.WithCancel(context.Background())
	defer cancelListener()
	err := bridge.runMainLoopPhase(context.Background(), "queued-turns", func(phaseCtx context.Context) error {
		phaseCtx = withTeamsPhaseExecutionContext(phaseCtx, listenerCtx)
		started, err := bridge.processQueuedTurnsWithStartBudget(phaseCtx, 1, true)
		if err != nil || started != 1 {
			return fmt.Errorf("admission started=%d err=%v", started, err)
		}
		select {
		case <-executor.started:
			return nil
		case <-time.After(bridgeAsyncTestTimeout):
			return errors.New("queued turn did not reach executor before phase ended")
		}
	})
	if err != nil {
		t.Fatalf("queued-turn phase: %v", err)
	}
	// The phase has now canceled its short admission context. The claimed
	// asynchronous turn must still use listenerCtx and complete normally.
	close(executor.release)
	waitForBridgeAsyncTurns(t, bridge)
	waitForCompletedTurnCount(t, store, session.ID, 1)
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("load completed phase turn: %v", err)
	}
	for _, turn := range state.Turns {
		if turn.SessionID == session.ID && turn.Status == teamstore.TurnStatusInterrupted {
			t.Fatalf("phase cancellation interrupted claimed turn: %#v", turn)
		}
	}
}

func TestTeamsDuePollAndPhaseDeadline(t *testing.T) {
	now := time.Now().UTC()
	decisions := make([]inboundPollDecision, 0, 10)
	for i := 0; i < 8; i++ {
		decisions = append(decisions, inboundPollDecision{ChatID: "hot-" + string(rune('a'+i)), State: inboundPollStateHot, Due: true})
	}
	decisions = append(decisions,
		inboundPollDecision{ChatID: "parked-probe", State: inboundPollStateParked, Due: true, NextPollAt: now},
		inboundPollDecision{ChatID: "warm-backlog", State: inboundPollStateWarm, Due: true, NextPollAt: now},
	)
	limited := limitInboundPollDecisions(decisions, 8)
	seenParked, seenWarm := false, false
	for _, decision := range limited {
		seenParked = seenParked || decision.State == inboundPollStateParked
		seenWarm = seenWarm || decision.ChatID == "warm-backlog"
	}
	if !seenParked || !seenWarm {
		t.Fatalf("due fairness dropped parked=%v warm=%v: %#v", seenParked, seenWarm, limited)
	}
	bridge := &Bridge{phaseBudget: 5 * time.Millisecond}
	if err := bridge.runMainLoopPhase(context.Background(), "graph-blackhole", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}); !isTeamsPhaseDeadline(err) {
		t.Fatalf("Graph-blackhole phase error = %v, want bounded deadline", err)
	}
	if err := bridge.runMainLoopPhase(context.Background(), "after-blackhole", func(context.Context) error { return nil }); err != nil {
		t.Fatalf("phase after Graph blackhole: %v", err)
	}
}

func TestTeamsBacklogScaleAndVirtualOutage(t *testing.T) {
	ctx := context.Background()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	base := time.Now().UTC().Add(-time.Hour)
	const total = 100
	rows := make([]teamstore.OutboxMessage, 0, total+1)
	for i := 0; i < total; i++ {
		rows = append(rows, teamstore.OutboxMessage{
			ID:          fmt.Sprintf("outbox:scale:%03d", i),
			TeamsChatID: fmt.Sprintf("scale-chat-%d", i%4),
			Kind:        "helper",
			Body:        "backlog",
			Sequence:    int64(i + 1),
			CreatedAt:   base.Add(time.Duration(i) * time.Nanosecond),
		})
	}
	gatedID := "outbox:scale:gate"
	rows = append(rows, teamstore.OutboxMessage{
		ID:          gatedID,
		TeamsChatID: "scale-chat-a",
		Kind:        "helper",
		Body:        "gated",
		Sequence:    total + 1,
		CreatedAt:   base.Add(total * time.Nanosecond),
	})
	seedBridgeTestOutboxRows(t, ctx, store, rows...)
	if _, err := store.DeferOutboxDeliveryUntil(ctx, gatedID, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("defer scale row: %v", err)
	}
	for i := 0; i < 80; i++ {
		if err := bridge.flushPendingOutboxMainLoop(ctx); err != nil {
			t.Fatalf("scale flush %d: %v", i, err)
		}
	}
	page, err := store.PendingOutboxPageAt(ctx, teamstore.PendingOutboxQuery{Now: time.Now(), Limit: 200, IgnoreRetryGate: true})
	if err != nil {
		t.Fatalf("load remaining scale rows: %v", err)
	}
	if len(page.Messages) != 1 || page.Messages[0].ID != gatedID {
		t.Fatalf("remaining scale rows = %#v, want only gated row", teamsBacklogOutboxIDs(page.Messages))
	}
	if err := store.ClearChatRateLimit(ctx, "scale-chat-a"); err != nil {
		t.Fatalf("clear scale chat gate: %v", err)
	}
	if err := bridge.flushPendingOutboxMainLoop(ctx); err != nil {
		t.Fatalf("flush released scale row: %v", err)
	}
	if len(*sent) != total+1 {
		t.Fatalf("scale Graph sends = %d, want %d after recovery", len(*sent), total+1)
	}
}

func teamsBacklogOutboxIDs(messages []teamstore.OutboxMessage) []string {
	ids := make([]string, 0, len(messages))
	for _, message := range messages {
		ids = append(ids, message.ID)
	}
	sort.Strings(ids)
	return ids
}
