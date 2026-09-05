package teams

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
	"github.com/gofrs/flock"
)

// TestBridgePollWindowSQLiteBatchLookupAndInboundWriter exercises the hot
// poll-window path as one operation.  A single page contains a durable helper
// echo and a fresh user prompt: batch provenance lookup must suppress only the
// echo, while the global SQLite inbound writer must claim/complete the prompt
// without losing it or allowing a second pass to run it again.
func TestBridgePollWindowSQLiteBatchLookupAndInboundWriter(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	tmp := t.TempDir()
	bridge.registryPath = filepath.Join(tmp, "teams", "scopes", "scope-current", "registry.json")
	if err := os.MkdirAll(filepath.Dir(bridge.registryPath), 0o700); err != nil {
		t.Fatalf("create registry directory: %v", err)
	}

	const helperMessageID = "helper-echo-in-mixed-window"
	const userMessageID = "fresh-user-in-mixed-window"
	if _, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:             "outbox:mixed-window-helper",
		SessionID:      "s001",
		TeamsChatID:    "chat-1",
		TeamsMessageID: helperMessageID,
		Kind:           "final",
		Body:           "already delivered helper answer",
		Status:         teamstore.OutboxStatusSent,
		SentAt:         time.Date(2026, 9, 5, 12, 0, 0, 0, time.UTC),
	}); err != nil {
		t.Fatalf("seed helper outbox: %v", err)
	}
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		t.Fatalf("migrate poll store to SQLite: %v", err)
	}

	helperEcho := bridgeTestMessageWithText(helperMessageID, "already delivered helper answer")
	helperEcho.CreatedDateTime = "2026-09-05T12:00:00Z"
	helperEcho.LastModifiedDateTime = helperEcho.CreatedDateTime
	freshUser := bridgeTestMessageWithText(userMessageID, "run the fresh user request")
	freshUser.CreatedDateTime = "2026-09-05T12:00:01Z"
	freshUser.LastModifiedDateTime = freshUser.CreatedDateTime
	poll := teamstore.ChatPollState{ChatID: "chat-1", Seeded: true}
	window := MessageWindow{Messages: []ChatMessage{helperEcho, freshUser}}
	var handled []string
	handle := func(_ context.Context, msg ChatMessage, text string) error {
		handled = append(handled, msg.ID+":"+text)
		return nil
	}

	result, err := bridge.handlePollMessageWindow(ctx, "chat-1", inboundPollRoleWork, poll, true, window, 20, 0, handle)
	if err != nil {
		t.Fatalf("mixed SQLite poll window: %v", err)
	}
	if !result.Handled || !result.Progressed || !result.PageComplete {
		t.Fatalf("mixed poll result = %#v, want handled/progressed/complete", result)
	}
	if len(handled) != 1 || handled[0] != userMessageID+":run the fresh user request" {
		t.Fatalf("mixed poll handled = %#v, want only fresh user prompt", handled)
	}

	helperLookup, err := store.MessageLookup(ctx, "chat-1", helperMessageID)
	if err != nil {
		t.Fatalf("helper provenance lookup: %v", err)
	}
	if !helperLookup.HasProvenance || helperLookup.Provenance.Origin != teamstore.MessageOriginHelperOutbox {
		t.Fatalf("helper provenance = %#v, want durable helper echo provenance", helperLookup)
	}
	ledgerPath, ok := globalInboundLedgerPathForRegistry(bridge.registryPath)
	if !ok {
		t.Fatal("mixed poll fixture did not enable global inbound ledger")
	}
	ledger, err := readGlobalInboundLedger(ledgerPath)
	if err != nil {
		t.Fatalf("read mixed poll inbound ledger: %v", err)
	}
	if item := ledger.Items[globalInboundKey("chat-1", userMessageID)]; item.Status != "done" {
		t.Fatalf("fresh user global inbound item = %#v, want done", item)
	}
	if _, exists := ledger.Items[globalInboundKey("chat-1", helperMessageID)]; exists {
		t.Fatalf("helper echo unexpectedly claimed global inbound ledger: %#v", ledger.Items)
	}

	// The same page may be replayed after a poll-window crash.  Registry and
	// durable provenance/ledger state must make it a no-op rather than invoke
	// the user handler for either record a second time.
	result, err = bridge.handlePollMessageWindow(ctx, "chat-1", inboundPollRoleWork, poll, true, window, 20, 0, handle)
	if err != nil {
		t.Fatalf("replayed mixed SQLite poll window: %v", err)
	}
	if result.Handled || len(handled) != 1 {
		t.Fatalf("replayed mixed poll result=%#v handled=%#v, want no second action", result, handled)
	}
}

// TestBridgePollWindowCompletionFailureDoesNotMarkRegistrySeen joins the
// batch-lookup, global-claim and local-registry paths at their failure
// boundary. A handler can finish after claiming a message while the ledger
// completion is temporarily locked. The message must remain recoverable; a
// local seen mark made before durable completion would hide it on every later
// poll until the process is restarted.
func TestBridgePollWindowCompletionFailureDoesNotMarkRegistrySeen(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	tmp := t.TempDir()
	bridge.registryPath = filepath.Join(tmp, "teams", "scopes", "scope-current", "registry.json")
	if err := os.MkdirAll(filepath.Dir(bridge.registryPath), 0o700); err != nil {
		t.Fatalf("create registry directory: %v", err)
	}

	const chatID = "chat-completion-retry"
	const messageID = "message-completion-retry"
	msg := bridgeTestMessageWithText(messageID, "retry after ledger completion failure")
	msg.CreatedDateTime = "2026-09-05T12:00:00Z"
	msg.LastModifiedDateTime = msg.CreatedDateTime
	poll := teamstore.ChatPollState{ChatID: chatID, Seeded: true}
	window := MessageWindow{Messages: []ChatMessage{msg}}
	ledgerPath, ok := globalInboundLedgerPathForRegistry(bridge.registryPath)
	if !ok {
		t.Fatal("completion-retry fixture did not enable global inbound ledger")
	}

	completionLock := flock.New(ledgerPath + ".lock")
	lockHeld := false
	t.Cleanup(func() {
		if lockHeld {
			_ = completionLock.Unlock()
		}
	})
	var handled []string
	handle := func(_ context.Context, msg ChatMessage, text string) error {
		handled = append(handled, msg.ID+":"+text)
		if len(handled) > 1 {
			return nil
		}
		locked, err := completionLock.TryLockContext(context.Background(), globalInboundLockTimeout)
		if err != nil {
			t.Fatalf("hold inbound completion lock: %v", err)
		}
		if !locked {
			t.Fatal("failed to hold inbound completion lock")
		}
		lockHeld = true
		return nil
	}

	result, err := bridge.handlePollMessageWindow(ctx, chatID, inboundPollRoleWork, poll, true, window, 20, 0, handle)
	if err == nil {
		t.Fatal("poll window completion failure unexpectedly succeeded")
	}
	if result.Progressed {
		t.Fatalf("completion-failure result = %#v, want no durable progress before completion", result)
	}
	if !lockHeld {
		t.Fatal("completion-failure test did not hold the ledger lock")
	}
	if err := completionLock.Unlock(); err != nil {
		t.Fatalf("release inbound completion lock: %v", err)
	}
	lockHeld = false

	if bridge.registryHasSeenOrSentForPoll(chatID, messageID) {
		t.Fatal("message entered local seen registry before durable inbound completion")
	}
	ledger, err := readGlobalInboundLedger(ledgerPath)
	if err != nil {
		t.Fatalf("read incomplete inbound ledger: %v", err)
	}
	item := ledger.Items[globalInboundKey(chatID, messageID)]
	if item.Status != "claimed" {
		t.Fatalf("incomplete inbound ledger item = %#v, want claimed for recovery", item)
	}

	// Simulate a replacement owner recovering and releasing the stale claim.
	// Supplying a post-TTL timestamp keeps the test deterministic without
	// sleeping for the production claim lifetime; releasing it models a failed
	// first handler whose work must be retried rather than hidden by registry.
	recoveryClaim, claimed, err := claimGlobalInbound(ctx, ledgerPath, chatID, messageID, "recovery-owner", time.Now().UTC().Add(globalInboundClaimTTL+time.Second))
	if err != nil {
		t.Fatalf("recover stale inbound claim: %v", err)
	}
	if !claimed {
		t.Fatal("stale inbound claim should be recoverable")
	}
	releaseGlobalInbound(ctx, recoveryClaim)

	result, err = bridge.handlePollMessageWindow(ctx, chatID, inboundPollRoleWork, poll, true, window, 20, 0, handle)
	if err != nil {
		t.Fatalf("replayed poll window after claim release: %v", err)
	}
	if !result.Handled || len(handled) != 2 {
		t.Fatalf("replayed recovered message result=%#v handled=%#v, want one retry after released claim", result, handled)
	}
}

func TestBridgePollWindowSidecarReplacementBeforeCompletionRetriesMessage(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	tmp := t.TempDir()
	bridge.registryPath = filepath.Join(tmp, "teams", "scopes", "scope-current", "registry.json")
	if err := os.MkdirAll(filepath.Dir(bridge.registryPath), 0o700); err != nil {
		t.Fatalf("create registry directory: %v", err)
	}

	const chatID = "chat-sidecar-replacement"
	const messageID = "message-sidecar-replacement"
	msg := bridgeTestMessageWithText(messageID, "retry after sidecar replacement")
	msg.CreatedDateTime = "2026-09-05T12:01:00Z"
	msg.LastModifiedDateTime = msg.CreatedDateTime
	poll := teamstore.ChatPollState{ChatID: chatID, Seeded: true}
	window := MessageWindow{Messages: []ChatMessage{msg}}
	ledgerPath, ok := globalInboundLedgerPathForRegistry(bridge.registryPath)
	if !ok {
		t.Fatal("sidecar-replacement fixture did not enable global inbound ledger")
	}
	sidecarPath := teamsLedgerSQLitePath(ledgerPath)
	var handled []string
	handle := func(_ context.Context, msg ChatMessage, text string) error {
		handled = append(handled, msg.ID+":"+text)
		if len(handled) != 1 {
			return nil
		}
		oldPath := sidecarPath + ".old"
		if err := os.Rename(sidecarPath, oldPath); err != nil {
			t.Fatalf("replace inbound sqlite sidecar: %v", err)
		}
		for _, suffix := range []string{"-wal", "-shm"} {
			if err := os.Rename(sidecarPath+suffix, oldPath+suffix); err != nil && !os.IsNotExist(err) {
				t.Fatalf("replace inbound sqlite sidecar %s: %v", suffix, err)
			}
		}
		replacement, err := openTeamsLedgerSQLite(sidecarPath)
		if err != nil {
			t.Fatalf("create replacement inbound sqlite sidecar: %v", err)
		}
		if err := replacement.Close(); err != nil {
			t.Fatalf("close replacement inbound sqlite sidecar: %v", err)
		}
		return nil
	}

	result, err := bridge.handlePollMessageWindow(ctx, chatID, inboundPollRoleWork, poll, true, window, 20, 0, handle)
	if err == nil {
		t.Fatal("sidecar replacement before completion unexpectedly succeeded")
	}
	if result.Progressed || result.Handled {
		t.Fatalf("sidecar replacement result = %#v, want no durable progress", result)
	}
	if bridge.registryHasSeenOrSentForPoll(chatID, messageID) {
		t.Fatal("message entered local seen registry after sidecar claim was lost")
	}

	// The replacement sidecar contains no claim, so the next poll can safely
	// claim and process the message. This is the recovery path that a pre-CAS
	// registry mark would have hidden.
	result, err = bridge.handlePollMessageWindow(ctx, chatID, inboundPollRoleWork, poll, true, window, 20, 0, handle)
	if err != nil {
		t.Fatalf("retry after sidecar replacement: %v", err)
	}
	if !result.Handled || len(handled) != 2 {
		t.Fatalf("retry result=%#v handled=%#v, want one recovered retry", result, handled)
	}
	ledger, err := readGlobalInboundLedger(ledgerPath)
	if err != nil {
		t.Fatalf("read recovered sidecar ledger: %v", err)
	}
	if item := ledger.Items[globalInboundKey(chatID, messageID)]; item.Status != "done" {
		t.Fatalf("recovered sidecar inbound item = %#v, want done", item)
	}
}
