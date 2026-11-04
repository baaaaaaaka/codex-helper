package delegation

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRequestRecordHasStableSourceKeyAndDelegationID(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	spec := TaskSpec{Title: "check Windows", Objective: "Inspect the Windows-only failure.", AllowedActions: []string{"read-only"}}
	a, err := NewRequestRecord("session-a", "turn-1", "", []string{"machine-a"}, "machine-b", spec, now)
	if err != nil {
		t.Fatalf("NewRequestRecord a: %v", err)
	}
	b, err := NewRequestRecord("session-a", "turn-1", "", []string{"machine-a"}, "machine-b", spec, now.Add(time.Minute))
	if err != nil {
		t.Fatalf("NewRequestRecord b: %v", err)
	}
	if a.SourceKey != b.SourceKey || a.DelegationID != b.DelegationID || a.RecordID != b.RecordID {
		t.Fatalf("request identity changed:\na=%#v\nb=%#v", a, b)
	}
	if Reduce([]Record{a, b}, now).Status != StateOpen {
		t.Fatalf("duplicate request should reduce to open: %#v", Reduce([]Record{a, b}, now))
	}
	if a.HopBudget != 0 {
		t.Fatalf("hop budget = %d, want no remaining hops after machine-a -> machine-b", a.HopBudget)
	}
}

func TestReducerSelectsOneClaimAndRejectsConflictingTerminals(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	req := mustRequest(t, now)
	claimA, err := NewClaimRecord(req.DelegationID, "machine-b", "worker-a", 1, now.Add(time.Second))
	if err != nil {
		t.Fatalf("claimA: %v", err)
	}
	claimB, err := NewClaimRecord(req.DelegationID, "machine-b", "worker-b", 1, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("claimB: %v", err)
	}
	staleResult, err := NewResultRecord(req.DelegationID, claimB, StateComplete, "wrong worker result", 1, now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("stale result: %v", err)
	}
	goodResult, err := NewResultRecord(req.DelegationID, claimA, StateComplete, "the answer", 1, now.Add(4*time.Second))
	if err != nil {
		t.Fatalf("good result: %v", err)
	}

	state := Reduce([]Record{req, claimB, staleResult, claimA, goodResult}, now)
	if state.Status != StateComplete || state.Terminal == nil || state.Terminal.Body != "the answer" {
		t.Fatalf("state = %#v, want winning complete result", state)
	}
	if state.WinningClaim == nil || state.WinningClaim.WorkerInstanceID != "worker-a" {
		t.Fatalf("winning claim = %#v, want earliest worker-a", state.WinningClaim)
	}
	if !contains(state.ConflictRecordIDs, claimB.RecordID) || !contains(state.ConflictRecordIDs, staleResult.RecordID) {
		t.Fatalf("conflicts = %#v, want losing claim and stale result", state.ConflictRecordIDs)
	}
}

func TestReducerKeepsClaimedStatusForCompetingClaimsUntilWinnerCompletes(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	req := mustRequest(t, now)
	claimA, err := NewClaimRecord(req.DelegationID, "machine-b", "worker-a", 1, now.Add(time.Second))
	if err != nil {
		t.Fatalf("claimA: %v", err)
	}
	claimB, err := NewClaimRecord(req.DelegationID, "machine-b", "worker-b", 1, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("claimB: %v", err)
	}
	losingResult, err := NewResultRecord(req.DelegationID, claimB, StateComplete, "wrong worker result", 1, now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("losing result: %v", err)
	}
	winningResult, err := NewResultRecord(req.DelegationID, claimA, StateComplete, "winner result", 1, now.Add(4*time.Second))
	if err != nil {
		t.Fatalf("winning result: %v", err)
	}

	claimed := Reduce([]Record{req, claimB, claimA}, now)
	if claimed.Status != StateClaimed || claimed.WinningClaim == nil || claimed.WinningClaim.RecordID != claimA.RecordID {
		t.Fatalf("claimed state = %#v, want claimed with worker-a winner", claimed)
	}
	if !contains(claimed.ConflictRecordIDs, claimB.RecordID) {
		t.Fatalf("claimed conflicts = %#v, want losing claim conflict", claimed.ConflictRecordIDs)
	}

	afterLosingResult := Reduce([]Record{req, claimB, claimA, losingResult}, now)
	if afterLosingResult.Status != StateClaimed || afterLosingResult.Terminal != nil {
		t.Fatalf("after losing result = %#v, want still claimed without terminal", afterLosingResult)
	}
	if !contains(afterLosingResult.ConflictRecordIDs, losingResult.RecordID) {
		t.Fatalf("conflicts = %#v, want losing result conflict", afterLosingResult.ConflictRecordIDs)
	}

	complete := Reduce([]Record{req, claimB, claimA, losingResult, winningResult}, now)
	if complete.Status != StateComplete || complete.Terminal == nil || complete.Terminal.Body != "winner result" {
		t.Fatalf("complete state = %#v, want winning terminal", complete)
	}
}

func TestReducerUsesKindPrecedenceForSameTimestampClaimResult(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	req := mustRequest(t, now)
	claim, err := NewClaimRecord(req.DelegationID, "machine-b", "worker-a", 1, now)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	result, err := NewResultRecord(req.DelegationID, claim, StateComplete, "same timestamp result", 1, now)
	if err != nil {
		t.Fatalf("result: %v", err)
	}
	state := Reduce([]Record{result, claim, req}, now)
	if state.Status != StateComplete || state.Terminal == nil || state.Terminal.Body != "same timestamp result" {
		t.Fatalf("state = %#v, want complete despite same timestamp ordering", state)
	}
}

func TestQuestionStatusIsIntermediateAndDoesNotBlockTerminalResult(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	req := mustRequest(t, now)
	claim, err := NewClaimRecord(req.DelegationID, "machine-b", "worker-a", 1, now.Add(time.Second))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	question, err := NewQuestionRecord(req.DelegationID, claim, "Need the log path.", now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("question: %v", err)
	}
	result, err := NewResultRecord(req.DelegationID, claim, StateComplete, "done", 1, now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("result: %v", err)
	}
	state := Reduce([]Record{req, claim, question, result}, now)
	if state.Status != StateComplete || state.Terminal == nil || state.Terminal.Body != "done" {
		t.Fatalf("state = %#v, want terminal result to win over question", state)
	}
	if len(state.StatusRecords) != 1 || state.StatusRecords[0].Status != StateQuestion || state.StatusRecords[0].Body != "Need the log path." {
		t.Fatalf("status records = %#v, want retained question", state.StatusRecords)
	}
}

func TestReducerUsesLatestIntermediateStatusBeforeTerminal(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	req := mustRequest(t, now)
	claim, err := NewClaimRecord(req.DelegationID, "machine-b", "worker-a", 1, now.Add(time.Second))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	running, err := NewStatusRecord(req.DelegationID, claim, StateRunning, "started", now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("running: %v", err)
	}
	question, err := NewQuestionRecord(req.DelegationID, claim, "Need the log path.", now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("question: %v", err)
	}
	state := Reduce([]Record{req, claim, running, question}, now.Add(4*time.Second))
	if state.Status != StateQuestion {
		t.Fatalf("state = %#v, want latest question status", state)
	}
	if len(state.StatusRecords) != 2 || state.StatusRecords[0].Status != StateRunning || state.StatusRecords[1].Status != StateQuestion {
		t.Fatalf("status records = %#v, want running then question", state.StatusRecords)
	}
	if _, err := NewStatusRecord(req.DelegationID, claim, "bogus", "bad", now); err == nil {
		t.Fatal("invalid intermediate status accepted")
	}
}

func TestReducerCancelBeatsLateClaimAndResult(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	req := mustRequest(t, now)
	cancel := NewTombstoneRecord(req.DelegationID, "user canceled", now.Add(time.Second))
	claim, err := NewClaimRecord(req.DelegationID, "machine-b", "worker-a", 1, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	result, err := NewResultRecord(req.DelegationID, claim, StateComplete, "late", 1, now.Add(3*time.Second))
	if err != nil {
		t.Fatalf("result: %v", err)
	}
	state := Reduce([]Record{req, cancel, claim, result}, now)
	if state.Status != StateCanceled || state.Terminal == nil || state.Terminal.RecordID != cancel.RecordID {
		t.Fatalf("state = %#v, want canceled by tombstone", state)
	}
	if !contains(state.IgnoredRecordIDs, claim.RecordID) || !contains(state.IgnoredRecordIDs, result.RecordID) {
		t.Fatalf("ignored = %#v, want late claim/result ignored", state.IgnoredRecordIDs)
	}
}

func TestReducerExpiresOpenRequestAndIgnoresLateResultWithoutClaim(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	req := mustRequest(t, now)
	state := Reduce([]Record{req}, now.Add(DefaultDelegationTTL+time.Minute))
	if state.Status != StateExpired || !state.Expired {
		t.Fatalf("state = %#v, want expired", state)
	}
}

func TestDelegationPathRejectsLoopsAndHopOverflow(t *testing.T) {
	if err := ValidatePath([]string{"machine-a"}, "machine-a"); err == nil || !strings.Contains(err.Error(), "loop") {
		t.Fatalf("ValidatePath loop err = %v, want loop rejection", err)
	}
	if err := ValidatePath([]string{"machine-a", "machine-b"}, "machine-c"); err == nil || !strings.Contains(err.Error(), "hop") {
		t.Fatalf("ValidatePath hop err = %v, want hop rejection", err)
	}
	if err := ValidatePath([]string{"machine-a"}, "machine-b"); err != nil {
		t.Fatalf("ValidatePath valid path: %v", err)
	}
}

func TestCandidateTokenExpiresAndTaskSpecBounds(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	token, validUntil, err := NewCandidateToken("machine-b", now, time.Minute)
	if err != nil {
		t.Fatalf("NewCandidateToken: %v", err)
	}
	payload, err := DecodeCandidateToken(token, now.Add(30*time.Second))
	if err != nil {
		t.Fatalf("DecodeCandidateToken valid: %v", err)
	}
	if payload.MachineID != "machine-b" || payload.ValidUntil != validUntil {
		t.Fatalf("payload = %#v validUntil=%q", payload, validUntil)
	}
	if _, err := DecodeCandidateToken(token, now.Add(2*time.Minute)); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("DecodeCandidateToken expired err = %v, want expired", err)
	}
	if err := (TaskSpec{Objective: strings.Repeat("x", MaxTaskObjectiveRunes+1)}).Validate(); err == nil {
		t.Fatal("oversized task objective accepted")
	}
}

func TestCandidateTokenCanBindInboxLocator(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	token, _, err := NewCandidateTokenForCandidate(Candidate{
		MachineID:             "machine-b",
		InboxRef:              "inbox-ref-b",
		InboxGeneration:       "gen-b",
		RegistryGeneration:    "registry-gen",
		CardRevision:          17,
		CapabilityFingerprint: "cap-fp",
		ProtocolVersions:      []string{"cxp-delegation-v1"},
	}, now, time.Minute)
	if err != nil {
		t.Fatalf("NewCandidateTokenForCandidate: %v", err)
	}
	payload, err := DecodeCandidateToken(token, now)
	if err != nil {
		t.Fatalf("DecodeCandidateToken: %v", err)
	}
	if payload.InboxRef != "inbox-ref-b" || payload.InboxGeneration != "gen-b" ||
		payload.RegistryGeneration != "registry-gen" || payload.CardRevision != 17 ||
		payload.CapabilityFingerprint != "cap-fp" || len(payload.ProtocolVersions) != 1 || payload.ProtocolVersions[0] != "cxp-delegation-v1" {
		t.Fatalf("payload = %#v, want bound candidate metadata", payload)
	}
	req := mustRequest(t, now)
	bound := BindRequestToCandidate(req, payload)
	if bound.InboxRef != "inbox-ref-b" || bound.InboxGeneration != "gen-b" {
		t.Fatalf("bound request = %#v, want inbox locator", bound)
	}
}

func TestThreadTokensBindNewAndReusableRemoteThreads(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	newToken, newPayload, err := NewThreadTokenForCandidate(Candidate{
		MachineID:       "machine-b",
		InboxRef:        "inbox-ref-b",
		InboxGeneration: "gen-b",
	}, "session-a", "workspace-1", now, time.Minute)
	if err != nil {
		t.Fatalf("NewThreadTokenForCandidate: %v", err)
	}
	decodedNew, err := DecodeThreadToken(newToken, now.Add(30*time.Second))
	if err != nil {
		t.Fatalf("DecodeThreadToken new: %v", err)
	}
	if decodedNew.Policy != ThreadPolicyNew || decodedNew.ThreadID == "" || decodedNew.ThreadID != newPayload.ThreadID ||
		decodedNew.InboxRef != "inbox-ref-b" || decodedNew.InboxGeneration != "gen-b" ||
		decodedNew.SourceSessionID != "session-a" || decodedNew.WorkspaceFingerprint != "workspace-1" {
		t.Fatalf("decoded new thread payload = %#v", decodedNew)
	}
	if _, err := DecodeThreadToken(newToken, now.Add(2*time.Minute)); err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expired new thread token err = %v, want expired", err)
	}

	thread := RemoteThread{
		ThreadID:             "rth-existing",
		MachineID:            "machine-b",
		SourceSessionID:      "session-a",
		WorkspaceFingerprint: "workspace-1",
		Generation:           "gen-existing",
		SummaryHash:          "summary-hash",
		State:                RemoteThreadStateIdle,
	}
	reuseToken, _, err := NewThreadTokenForThread(thread, now, time.Minute)
	if err != nil {
		t.Fatalf("NewThreadTokenForThread: %v", err)
	}
	decodedReuse, err := DecodeThreadToken(reuseToken, now)
	if err != nil {
		t.Fatalf("DecodeThreadToken reuse: %v", err)
	}
	if decodedReuse.Policy != ThreadPolicyReuse || decodedReuse.ThreadID != "rth-existing" ||
		decodedReuse.ThreadGeneration != "gen-existing" || decodedReuse.SummaryHash != "summary-hash" {
		t.Fatalf("decoded reuse payload = %#v", decodedReuse)
	}
	if _, err := EncodeThreadToken(ThreadTokenPayload{Policy: ThreadPolicyReuse, MachineID: "machine-b", ObservedAt: now.Format(time.RFC3339Nano), ValidUntil: now.Add(time.Minute).Format(time.RFC3339Nano)}); err == nil {
		t.Fatal("reuse token without thread id was accepted")
	}
}

func TestReuseRejectedIsTerminalResult(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	req := mustRequest(t, now)
	claim, err := NewClaimRecord(req.DelegationID, "machine-b", "worker-a", 1, now.Add(time.Second))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	result, err := NewResultRecord(req.DelegationID, claim, StateReuseRejected, "wrong context", 1, now.Add(2*time.Second))
	if err != nil {
		t.Fatalf("reuse rejected result: %v", err)
	}
	state := Reduce([]Record{req, claim, result}, now.Add(3*time.Second))
	if state.Status != StateReuseRejected || state.Terminal == nil || state.Terminal.Body != "wrong context" {
		t.Fatalf("state = %#v, want reuse_rejected terminal", state)
	}
}

func TestRemoteThreadsPersistSortAndPrune(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	path := filepath.Join(t.TempDir(), "delegation.json")
	store := Store{}
	store.UpsertRemoteThread(RemoteThread{
		ThreadID:           "rth-old",
		MachineID:          "machine-b",
		Title:              strings.Repeat("x", MaxRemoteThreadTitleRunes+10),
		Summary:            strings.Repeat("s", MaxRemoteThreadSummaryRunes+10),
		State:              RemoteThreadStateIdle,
		LastUsedAt:         now.Add(-2 * time.Hour).Format(time.RFC3339Nano),
		UpdatedAt:          now.Add(-2 * time.Hour).Format(time.RFC3339Nano),
		ExpiresAt:          now.Add(-30 * time.Minute).Format(time.RFC3339Nano),
		Generation:         "gen-old",
		ActiveDelegationID: "",
	})
	store.UpsertRemoteThread(RemoteThread{
		ThreadID:   "rth-new",
		MachineID:  "machine-b",
		Title:      "new",
		State:      RemoteThreadStateIdle,
		LastUsedAt: now.Add(-time.Minute).Format(time.RFC3339Nano),
		UpdatedAt:  now.Add(-time.Minute).Format(time.RFC3339Nano),
		ExpiresAt:  now.Add(time.Hour).Format(time.RFC3339Nano),
		Generation: "gen-new",
	})
	if _, err := SaveStore(path, store); err != nil {
		t.Fatalf("SaveStore: %v", err)
	}
	loaded, err := LoadStore(path)
	if err != nil {
		t.Fatalf("LoadStore: %v", err)
	}
	old, ok := loaded.RemoteThreadForID("rth-old")
	if !ok || len([]rune(old.Title)) != MaxRemoteThreadTitleRunes || len([]rune(old.Summary)) != MaxRemoteThreadSummaryRunes {
		t.Fatalf("old thread = %#v ok=%v, want truncated persisted thread", old, ok)
	}
	threads := loaded.RemoteThreadsForMachine("machine-b")
	if len(threads) != 2 || threads[0].ThreadID != "rth-new" || threads[1].ThreadID != "rth-old" {
		t.Fatalf("sorted threads = %#v", threads)
	}
	loaded.Prune(now, time.Hour)
	if _, ok := loaded.RemoteThreadForID("rth-old"); ok {
		t.Fatalf("expired idle thread still present: %#v", loaded.RemoteThreads)
	}
	if _, ok := loaded.RemoteThreadForID("rth-new"); !ok {
		t.Fatalf("fresh idle thread was pruned: %#v", loaded.RemoteThreads)
	}
}

func TestStoreSaveIsContentSensitive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delegation.json")
	req := mustRequest(t, time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC))
	store := Store{Records: []Record{req}}
	store.UpsertOutbox(OutboxRecord{RecordID: req.RecordID, DelegationID: req.DelegationID, ChatID: "chat-inbox", Status: OutboxVisible, Attempts: 1})
	store.UpsertInboxCursor(InboxCursor{ChatID: "chat-inbox", LastHeadMessageID: "message-head"})
	store.UpsertInboxBackoff(InboxBackoff{ChatID: "chat-inbox", BlockedUntil: time.Date(2026, 6, 20, 10, 5, 0, 0, time.UTC).Format(time.RFC3339Nano)})
	written, err := SaveStore(path, store)
	if err != nil || !written {
		t.Fatalf("first SaveStore written=%v err=%v", written, err)
	}
	written, err = SaveStore(path, store)
	if err != nil {
		t.Fatalf("second SaveStore: %v", err)
	}
	if written {
		t.Fatal("SaveStore rewrote identical content")
	}
	loaded, err := LoadStore(path)
	if err != nil {
		t.Fatalf("LoadStore: %v", err)
	}
	if len(loaded.Records) != 1 || loaded.Records[0].DelegationID != req.DelegationID {
		t.Fatalf("loaded = %#v", loaded)
	}
	if outbox, ok := loaded.OutboxForRecordID(req.RecordID); !ok || outbox.Status != OutboxVisible || outbox.Attempts != 1 {
		t.Fatalf("outbox = %#v ok=%v, want visible outbox", outbox, ok)
	}
	if cursor, ok := loaded.InboxCursorForChat("chat-inbox"); !ok || cursor.LastHeadMessageID != "message-head" {
		t.Fatalf("cursor = %#v ok=%v, want persisted cursor", cursor, ok)
	}
	if backoff, ok := loaded.InboxBackoffForChat("chat-inbox"); !ok || backoff.BlockedUntil == "" {
		t.Fatalf("backoff = %#v ok=%v, want persisted backoff", backoff, ok)
	}
}

func TestStoreUpsertRoutePreservesCreatedAt(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	createdAt := now.Add(-time.Hour).Format(time.RFC3339Nano)
	store := Store{}
	store.UpsertRoute(Route{
		DelegationID: "del-1",
		MachineID:    "machine-b",
		InboxRef:     "old-inbox",
		CreatedAt:    createdAt,
		UpdatedAt:    now.Add(-time.Hour).Format(time.RFC3339Nano),
	})
	store.UpsertRoute(Route{
		DelegationID: "del-1",
		MachineID:    "machine-b",
		InboxRef:     "new-inbox",
		UpdatedAt:    now.Format(time.RFC3339Nano),
	})
	route, ok := store.RouteForID("del-1")
	if !ok || route.InboxRef != "new-inbox" || route.CreatedAt != createdAt {
		t.Fatalf("route = %#v ok=%v, want updated inbox with original CreatedAt", route, ok)
	}
}

func TestSQLiteStoreSaveIsContentSensitiveAndImportsLegacyJSON(t *testing.T) {
	dir := t.TempDir()
	sqlitePath := filepath.Join(dir, "delegation.sqlite")
	legacyPath := filepath.Join(dir, "delegation.json")
	req := mustRequest(t, time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC))
	store := Store{Records: []Record{req}}
	store.UpsertRoute(Route{DelegationID: req.DelegationID, MachineID: req.MachineID, InboxRef: "inbox-ref"})
	store.UpsertRemoteThread(RemoteThread{
		ThreadID:          "rth-1",
		MachineID:         req.MachineID,
		State:             RemoteThreadStateIdle,
		LastUsedAt:        req.CreatedAt,
		ExpiresAt:         time.Date(2026, 6, 20, 11, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
		SummaryHash:       "summary-hash",
		LastResultSummary: "done",
	})
	store.UpsertExecution(ExecutionFence{DelegationID: req.DelegationID, Status: StateComplete, UpdatedAt: req.CreatedAt})
	store.UpsertOutbox(OutboxRecord{RecordID: req.RecordID, DelegationID: req.DelegationID, ChatID: "chat-inbox", Status: OutboxVisible, Attempts: 1})
	store.UpsertInboxCursor(InboxCursor{ChatID: "chat-inbox", LastHeadMessageID: "message-head", UpdatedAt: req.CreatedAt})
	store.UpsertInboxBackoff(InboxBackoff{ChatID: "chat-inbox", BlockedUntil: time.Date(2026, 6, 20, 10, 5, 0, 0, time.UTC).Format(time.RFC3339Nano), UpdatedAt: req.CreatedAt})
	if _, err := SaveStore(legacyPath, store); err != nil {
		t.Fatalf("SaveStore legacy: %v", err)
	}
	loaded, err := LoadStore(sqlitePath)
	if err != nil {
		t.Fatalf("LoadStore sqlite legacy import: %v", err)
	}
	if len(loaded.Records) != 1 || loaded.Records[0].RecordID != req.RecordID {
		t.Fatalf("loaded legacy records = %#v", loaded.Records)
	}
	written, err := SaveStore(sqlitePath, loaded)
	if err != nil || !written {
		t.Fatalf("first sqlite SaveStore written=%v err=%v", written, err)
	}
	if _, err := os.Stat(sqlitePath); err != nil {
		t.Fatalf("sqlite store was not created: %v", err)
	}
	written, err = SaveStore(sqlitePath, loaded)
	if err != nil {
		t.Fatalf("second sqlite SaveStore: %v", err)
	}
	if written {
		t.Fatal("sqlite SaveStore rewrote identical content")
	}
	roundTrip, err := LoadStore(sqlitePath)
	if err != nil {
		t.Fatalf("LoadStore sqlite round trip: %v", err)
	}
	if _, ok := roundTrip.RouteForID(req.DelegationID); !ok {
		t.Fatalf("route missing after sqlite round trip: %#v", roundTrip)
	}
	if _, ok := roundTrip.RemoteThreadForID("rth-1"); !ok {
		t.Fatalf("remote thread missing after sqlite round trip: %#v", roundTrip.RemoteThreads)
	}
	if _, ok := roundTrip.ExecutionForID(req.DelegationID); !ok {
		t.Fatalf("execution missing after sqlite round trip: %#v", roundTrip.Executions)
	}
	if _, ok := roundTrip.OutboxForRecordID(req.RecordID); !ok {
		t.Fatalf("outbox missing after sqlite round trip: %#v", roundTrip.Outbox)
	}
	if _, ok := roundTrip.InboxCursorForChat("chat-inbox"); !ok {
		t.Fatalf("cursor missing after sqlite round trip: %#v", roundTrip.InboxCursors)
	}
	if _, ok := roundTrip.InboxBackoffForChat("chat-inbox"); !ok {
		t.Fatalf("backoff missing after sqlite round trip: %#v", roundTrip.InboxBackoffs)
	}
}

func TestSQLiteRowLevelUpsertSkipsIdenticalPayload(t *testing.T) {
	path := filepath.Join(t.TempDir(), "delegation.sqlite")
	db, err := openDelegationSQLiteStore(path, true)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	defer db.Close()
	ctx := context.Background()
	if err := ensureDelegationSQLiteSchema(ctx, db); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	cursor := InboxCursor{
		ChatID:            "chat-inbox",
		LastHeadMessageID: "head-1",
		UpdatedAt:         time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin first tx: %v", err)
	}
	if err := sqliteUpsertJSONTx(ctx, tx, "inbox_cursors", cursor.ChatID, cursor); err != nil {
		_ = tx.Rollback()
		t.Fatalf("first upsert: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit first tx: %v", err)
	}
	firstChanges := sqliteTestTotalChanges(t, db)

	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin second tx: %v", err)
	}
	if err := sqliteUpsertJSONTx(ctx, tx, "inbox_cursors", cursor.ChatID, cursor); err != nil {
		_ = tx.Rollback()
		t.Fatalf("second upsert: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit second tx: %v", err)
	}
	secondChanges := sqliteTestTotalChanges(t, db)
	if secondChanges != firstChanges {
		t.Fatalf("identical upsert changed rows: first=%d second=%d", firstChanges, secondChanges)
	}

	cursor.LastHeadMessageID = "head-2"
	tx, err = db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin third tx: %v", err)
	}
	if err := sqliteUpsertJSONTx(ctx, tx, "inbox_cursors", cursor.ChatID, cursor); err != nil {
		_ = tx.Rollback()
		t.Fatalf("third upsert: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit third tx: %v", err)
	}
	if thirdChanges := sqliteTestTotalChanges(t, db); thirdChanges <= secondChanges {
		t.Fatalf("changed upsert did not write: second=%d third=%d", secondChanges, thirdChanges)
	}
}

func sqliteTestTotalChanges(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var total int64
	if err := db.QueryRow(`SELECT total_changes()`).Scan(&total); err != nil {
		t.Fatalf("query total_changes: %v", err)
	}
	return total
}

func TestSQLiteRowLevelHelpersMaterializeLegacyJSON(t *testing.T) {
	dir := t.TempDir()
	sqlitePath := filepath.Join(dir, "delegation.sqlite")
	legacyPath := filepath.Join(dir, "delegation.json")
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	req := mustRequest(t, now)
	createdAt := now.Add(-time.Hour).Format(time.RFC3339Nano)
	store := Store{Records: []Record{req}}
	store.UpsertRoute(Route{
		DelegationID: req.DelegationID,
		MachineID:    req.MachineID,
		InboxRef:     "legacy-inbox",
		CreatedAt:    createdAt,
		UpdatedAt:    now.Format(time.RFC3339Nano),
	})
	store.UpsertRemoteThread(RemoteThread{
		ThreadID:          "rth-legacy",
		MachineID:         req.MachineID,
		State:             RemoteThreadStateIdle,
		Generation:        "gen-legacy",
		Title:             "legacy title",
		Summary:           "legacy summary",
		LastUsedAt:        createdAt,
		CreatedAt:         createdAt,
		ExpiresAt:         now.Add(time.Hour).Format(time.RFC3339Nano),
		LastResultSummary: "legacy result",
	})
	store.UpsertOutbox(OutboxRecord{
		RecordID:     req.RecordID,
		DelegationID: req.DelegationID,
		ChatID:       "legacy-chat",
		InboxRef:     "legacy-inbox",
		Status:       OutboxPending,
		Attempts:     1,
		CreatedAt:    createdAt,
		UpdatedAt:    createdAt,
	})
	if _, err := SaveStore(legacyPath, store); err != nil {
		t.Fatalf("SaveStore legacy: %v", err)
	}

	route, ok, err := RouteSQLite(sqlitePath, req.DelegationID)
	if err != nil || !ok {
		t.Fatalf("RouteSQLite legacy materialize route=%#v ok=%v err=%v", route, ok, err)
	}
	if route.InboxRef != "legacy-inbox" || route.CreatedAt != createdAt {
		t.Fatalf("legacy route = %#v", route)
	}
	if _, err := os.Stat(sqlitePath); err != nil {
		t.Fatalf("sqlite store was not materialized: %v", err)
	}
	thread, ok, err := RemoteThreadSQLite(sqlitePath, "rth-legacy")
	if err != nil || !ok || thread.Generation != "gen-legacy" || thread.Summary != "legacy summary" {
		t.Fatalf("RemoteThreadSQLite thread=%#v ok=%v err=%v", thread, ok, err)
	}
	if err := UpsertRouteSQLite(sqlitePath, Route{
		DelegationID: req.DelegationID,
		MachineID:    req.MachineID,
		InboxRef:     "new-inbox",
		UpdatedAt:    now.Add(time.Minute).Format(time.RFC3339Nano),
	}); err != nil {
		t.Fatalf("UpsertRouteSQLite: %v", err)
	}
	route, ok, err = RouteSQLite(sqlitePath, req.DelegationID)
	if err != nil || !ok {
		t.Fatalf("RouteSQLite after upsert route=%#v ok=%v err=%v", route, ok, err)
	}
	if route.InboxRef != "new-inbox" || route.CreatedAt != createdAt {
		t.Fatalf("upserted route = %#v, want new inbox with legacy CreatedAt", route)
	}
	if err := UpsertOutboxSQLite(sqlitePath, req, OutboxSent, "new-chat", "new-inbox", "msg-1", "", now.Add(time.Minute), DefaultStoreRetention); err != nil {
		t.Fatalf("UpsertOutboxSQLite: %v", err)
	}
	roundTrip, err := LoadStore(sqlitePath)
	if err != nil {
		t.Fatalf("LoadStore sqlite: %v", err)
	}
	outbox, ok := roundTrip.OutboxForRecordID(req.RecordID)
	if !ok || outbox.Status != OutboxSent || outbox.MessageID != "msg-1" || outbox.CreatedAt != createdAt || outbox.Attempts != 1 {
		t.Fatalf("outbox after row-level upsert = %#v ok=%v", outbox, ok)
	}
}

func TestSQLiteWorkerHelpersMaterializeLegacyJSON(t *testing.T) {
	dir := t.TempDir()
	sqlitePath := filepath.Join(dir, "worker.sqlite")
	legacyPath := filepath.Join(dir, "worker.json")
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	req := mustRequest(t, now)
	req.RemoteThreadID = "rth-busy"
	req.ThreadPolicy = ThreadPolicyReuse
	req.ThreadGeneration = "gen-busy"
	store := Store{}
	store.UpsertRemoteThread(RemoteThread{
		ThreadID:           req.RemoteThreadID,
		MachineID:          req.MachineID,
		State:              RemoteThreadStateActive,
		ActiveDelegationID: "dlg-other",
		Generation:         req.ThreadGeneration,
		UpdatedAt:          now.Format(time.RFC3339Nano),
		CreatedAt:          now.Add(-time.Hour).Format(time.RFC3339Nano),
		ExpiresAt:          now.Add(time.Hour).Format(time.RFC3339Nano),
	})
	if _, err := SaveStore(legacyPath, store); err != nil {
		t.Fatalf("SaveStore legacy: %v", err)
	}
	claim, err := NewClaimRecord(req.DelegationID, req.MachineID, "worker-1", 1, now)
	if err != nil {
		t.Fatalf("NewClaimRecord: %v", err)
	}
	started, err := TryStartWorkerSQLite(sqlitePath, req, claim, now, WorkerStorePruneLimits{
		TerminalExecutionLimit: 8,
		TerminalOutboxLimit:    8,
	})
	if err != nil {
		t.Fatalf("TryStartWorkerSQLite: %v", err)
	}
	if started {
		t.Fatal("TryStartWorkerSQLite ignored legacy active remote thread")
	}
	if _, err := os.Stat(sqlitePath); err != nil {
		t.Fatalf("sqlite worker store was not materialized: %v", err)
	}
}

func TestStorePruneRemovesTerminalStateAfterRetention(t *testing.T) {
	now := time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC)
	req := mustRequest(t, now.Add(-48*time.Hour))
	claim, err := NewClaimRecord(req.DelegationID, "machine-b", "worker-a", 1, now.Add(-47*time.Hour))
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	result, err := NewResultRecord(req.DelegationID, claim, StateComplete, "done", 1, now.Add(-46*time.Hour))
	if err != nil {
		t.Fatalf("result: %v", err)
	}
	openReq, err := NewRequestRecord(
		"session-a",
		"turn-open",
		"",
		[]string{"machine-a"},
		"machine-c",
		TaskSpec{Objective: "still running elsewhere"},
		now.Add(-time.Hour),
	)
	if err != nil {
		t.Fatalf("open request: %v", err)
	}
	store := Store{Records: []Record{req, claim, result, openReq}}
	store.UpsertRoute(Route{DelegationID: req.DelegationID, MachineID: "machine-b", UpdatedAt: result.CreatedAt})
	store.UpsertRoute(Route{DelegationID: openReq.DelegationID, MachineID: "machine-c", UpdatedAt: now.Add(-48 * time.Hour).Format(time.RFC3339Nano)})
	store.UpsertOutbox(OutboxRecord{
		RecordID:     result.RecordID,
		DelegationID: req.DelegationID,
		Status:       OutboxVisible,
		UpdatedAt:    result.CreatedAt,
	})
	store.UpsertInboxCursor(InboxCursor{ChatID: "chat-old", UpdatedAt: now.Add(-48 * time.Hour).Format(time.RFC3339Nano)})
	store.UpsertInboxBackoff(InboxBackoff{
		ChatID:       "chat-backoff",
		BlockedUntil: now.Add(-48 * time.Hour).Format(time.RFC3339Nano),
		UpdatedAt:    now.Add(-48 * time.Hour).Format(time.RFC3339Nano),
	})

	store.Prune(now, 24*time.Hour)
	if got := RecordsForID(store.Records, req.DelegationID); len(got) != 0 {
		t.Fatalf("terminal records = %#v, want pruned", got)
	}
	if got := RecordsForID(store.Records, openReq.DelegationID); len(got) != 1 {
		t.Fatalf("open records = %#v, want retained", got)
	}
	if _, ok := store.RouteForID(req.DelegationID); ok {
		t.Fatalf("terminal route still present: %#v", store.Routes)
	}
	if _, ok := store.RouteForID(openReq.DelegationID); !ok {
		t.Fatalf("open route was pruned by age: %#v", store.Routes)
	}
	if _, ok := store.OutboxForRecordID(result.RecordID); ok {
		t.Fatalf("terminal outbox still present: %#v", store.Outbox)
	}
	if _, ok := store.InboxCursorForChat("chat-old"); ok {
		t.Fatalf("old inbox cursor still present: %#v", store.InboxCursors)
	}
	if _, ok := store.InboxBackoffForChat("chat-backoff"); ok {
		t.Fatalf("old inbox backoff still present: %#v", store.InboxBackoffs)
	}
}

func TestRecordRoundTripAndObserveDedupes(t *testing.T) {
	req := mustRequest(t, time.Date(2026, 6, 20, 10, 0, 0, 0, time.UTC))
	html := RenderRecordHTML(req)
	parsed, ok := ParseRecordMessage(html)
	if !ok {
		t.Fatal("expected record to parse")
	}
	if parsed.RecordID != req.RecordID || parsed.DelegationID != req.DelegationID || parsed.SpecHash != req.SpecHash {
		t.Fatalf("parsed = %#v, want request identity", parsed)
	}
	messages := []ChatMessage{
		delegationMessage("m1", html),
		delegationMessage("m2", html),
		delegationMessage("m3", "<p>not a delegation record</p>"),
	}
	records := ObserveRecords(messages)
	if len(records) != 1 || records[0].RecordID != req.RecordID {
		t.Fatalf("records = %#v, want one deduped record", records)
	}
	if got := RecordsForID(records, req.DelegationID); len(got) != 1 {
		t.Fatalf("RecordsForID = %#v, want matching record", got)
	}
}

func mustRequest(t *testing.T, now time.Time) Record {
	t.Helper()
	req, err := NewRequestRecord(
		"session-a",
		"turn-1",
		"",
		[]string{"machine-a"},
		"machine-b",
		TaskSpec{Title: "check remote", Objective: "Inspect the remote machine and report the final answer."},
		now,
	)
	if err != nil {
		t.Fatalf("NewRequestRecord: %v", err)
	}
	return req
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func delegationMessage(id string, content string) ChatMessage {
	msg := ChatMessage{ID: id}
	msg.Body.Content = content
	return msg
}
