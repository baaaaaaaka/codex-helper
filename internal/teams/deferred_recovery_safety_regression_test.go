package teams

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type deferredRecoveryAuthStub struct {
	err   error
	calls atomic.Int32
}

func (a *deferredRecoveryAuthStub) AccessToken(context.Context, io.Writer, bool) (string, error) {
	a.calls.Add(1)
	return "", a.err
}

func (a *deferredRecoveryAuthStub) RefreshAccessToken(context.Context) (string, error) {
	a.calls.Add(1)
	return "", a.err
}

func TestClassifyDeferredInboundFailureKeepsExternalBoundarySafe(t *testing.T) {
	resolverErr := &deferredInboundResolverError{err: errors.New("default profile unavailable")}
	tests := []struct {
		name string
		err  error
		want deferredInboundFailureDisposition
	}{
		{name: "read 429 is retryable", err: &GraphStatusError{Method: http.MethodGet, StatusCode: http.StatusTooManyRequests}, want: deferredInboundFailureRetry},
		{name: "read 500 is retryable", err: &GraphStatusError{Method: http.MethodGet, StatusCode: http.StatusInternalServerError}, want: deferredInboundFailureRetry},
		{name: "read transport is retryable", err: &GraphTransportError{Method: http.MethodGet, Path: "/chats/c/messages", Err: io.ErrUnexpectedEOF}, want: deferredInboundFailureRetry},
		{name: "malformed read response is held", err: &GraphResponseError{Method: http.MethodGet, Path: "/chats/c/messages", Err: io.ErrUnexpectedEOF}, want: deferredInboundFailureHold},
		{name: "resolver is retryable", err: resolverErr, want: deferredInboundFailureRetry},
		{name: "temporary auth is retryable", err: &TemporaryAuthError{Action: "Teams auth", Err: errors.New("oauth 503")}, want: deferredInboundFailureRetry},
		{name: "reauth required is held", err: &ReauthRequiredError{Action: "Teams auth", Reason: "auth cache is missing"}, want: deferredInboundFailureHold},
		{name: "auth cache failure is held", err: &AuthCacheError{Action: "Teams auth", Err: errors.New("invalid JSON")}, want: deferredInboundFailureHold},
		{name: "write 408 is uncertain", err: &GraphStatusError{Method: http.MethodPost, StatusCode: http.StatusRequestTimeout}, want: deferredInboundFailureUncertain},
		{name: "write 409 is uncertain", err: &GraphStatusError{Method: http.MethodPost, StatusCode: http.StatusConflict}, want: deferredInboundFailureUncertain},
		{name: "write 425 is uncertain", err: &GraphStatusError{Method: http.MethodPost, StatusCode: http.StatusTooEarly}, want: deferredInboundFailureUncertain},
		{name: "write 429 is uncertain", err: &GraphStatusError{Method: http.MethodPost, StatusCode: http.StatusTooManyRequests}, want: deferredInboundFailureUncertain},
		{name: "write 500 is uncertain", err: &GraphStatusError{Method: http.MethodPatch, StatusCode: http.StatusInternalServerError}, want: deferredInboundFailureUncertain},
		{name: "write 503 is uncertain", err: &GraphStatusError{Method: http.MethodPost, StatusCode: http.StatusServiceUnavailable}, want: deferredInboundFailureUncertain},
		{name: "write transport is uncertain", err: &GraphTransportError{Method: http.MethodPost, Path: "/chats/c/messages", Err: io.ErrUnexpectedEOF}, want: deferredInboundFailureUncertain},
		{name: "known write rejection is held", err: &GraphStatusError{Method: http.MethodPost, StatusCode: http.StatusBadRequest}, want: deferredInboundFailureHold},
		{name: "owner preflight is fatal", err: &graphRequestPreflightError{cause: teamstore.ErrControlLeaseNotHeld}, want: deferredInboundFailureFatal},
		{name: "untyped eof is fatal", err: io.EOF, want: deferredInboundFailureFatal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyDeferredInboundFailure(tt.err); got != tt.want {
				t.Fatalf("classifyDeferredInboundFailure(%T: %v) = %d, want %d", tt.err, tt.err, got, tt.want)
			}
		})
	}
}

func TestDeferredUnknownRowFailureIsHeldInsteadOfHotLooping(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	inbound := teamstore.InboundEvent{
		ID:          "inbound:deferred-unknown-row-error",
		SessionID:   controlFallbackSessionID,
		TeamsChatID: "control-chat",
		Source:      "teams_control_poll_deferred",
		Status:      teamstore.InboundStatusDeferred,
		CreatedAt:   time.Now().UTC(),
		UpdatedAt:   time.Now().UTC(),
	}
	if _, _, err := store.PersistInbound(ctx, inbound); err != nil {
		t.Fatalf("PersistInbound: %v", err)
	}

	handled, err := bridge.handleDeferredInboundRowFailure(ctx, inbound, io.EOF)
	if !handled || err != nil {
		t.Fatalf("unknown deferred row failure handled=%t err=%v, want durable hold", handled, err)
	}
	held, found, err := store.InboundEventByID(ctx, inbound.ID)
	if err != nil || !found {
		t.Fatalf("read held deferred row: found=%v err=%v row=%#v", found, err, held)
	}
	if held.Status != teamstore.InboundStatusManualHold || !held.NextAttemptAt.IsZero() || held.HoldReason == "" || held.HoldWakeCondition == "" {
		t.Fatalf("unknown deferred row disposition = %#v, want durable manual hold without retry gate", held)
	}
	candidates, err := store.InboundRecoveryCandidates(ctx)
	if err != nil {
		t.Fatalf("InboundRecoveryCandidates: %v", err)
	}
	if len(candidates) != 0 {
		t.Fatalf("held unknown row remained an automatic candidate: %#v", candidates)
	}

	handled, err = bridge.handleDeferredInboundRowFailure(ctx, inbound, io.EOF)
	if !handled || err != nil {
		t.Fatalf("repeated unknown deferred row failure handled=%t err=%v, want idempotent hold", handled, err)
	}
	if candidates, err := store.InboundRecoveryCandidates(ctx); err != nil {
		t.Fatalf("second InboundRecoveryCandidates: %v", err)
	} else if len(candidates) != 0 {
		t.Fatalf("repeated held unknown row became an automatic candidate: %#v", candidates)
	}
}

func TestDeferredControlSourcesFailClosedInsteadOfEnteringCodex(t *testing.T) {
	sources := []string{
		"teams_control_restart",
		"teams_control_reload",
		"teams_control_update",
		"teams_control_codex_update",
		"teams_control_webhook_disable",
		"teams_control_webhook_test",
		"teams_control_webhook_configure",
		"teams_control_future",
	}

	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			executor := &recordingExecutor{}
			bridge := newBridgeTestBridge(nil, store, executor)
			now := time.Now().UTC()
			for i, source := range sources {
				inbound := teamstore.InboundEvent{
					ID:             fmt.Sprintf("inbound:control-fail-closed-%02d", i),
					SessionID:      controlFallbackSessionID,
					TeamsChatID:    "control-chat",
					TeamsMessageID: fmt.Sprintf("control-fail-closed-%02d", i),
					Text:           "helper upgrade prerelease",
					Source:         source,
					Status:         teamstore.InboundStatusDeferred,
					CreatedAt:      now.Add(time.Duration(i) * time.Second),
					UpdatedAt:      now.Add(time.Duration(i) * time.Second),
				}
				if _, _, err := store.PersistInbound(ctx, inbound); err != nil {
					t.Fatalf("PersistInbound(%s): %v", source, err)
				}
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}

			if err := bridge.processDeferredInbound(ctx); err != nil {
				t.Fatalf("processDeferredInbound: %v", err)
			}
			if got := executor.promptCount(); got != 0 {
				t.Fatalf("deferred control commands entered Codex: %d executor calls", got)
			}
			for i, source := range sources {
				id := fmt.Sprintf("inbound:control-fail-closed-%02d", i)
				inbound, found, err := store.InboundEventByID(ctx, id)
				if err != nil || !found {
					t.Fatalf("read %s: found=%v err=%v row=%#v", source, found, err, inbound)
				}
				if inbound.Status != teamstore.InboundStatusManualHold {
					t.Fatalf("source %s status=%q, want manual hold: %#v", source, inbound.Status, inbound)
				}
			}
			candidates, err := store.InboundRecoveryCandidates(ctx)
			if err != nil {
				t.Fatalf("InboundRecoveryCandidates: %v", err)
			}
			if len(candidates) != 0 {
				t.Fatalf("held control commands remained automatic candidates: %#v", candidates)
			}
		})
	}
}

func TestQueuedNonReplayableControlSourceIsQuarantinedBeforeCodex(t *testing.T) {
	sources := []string{
		"teams_control_restart",
		"teams_control_reload",
		"teams_control_update",
		"teams_control_future",
	}
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			for _, source := range sources {
				t.Run(source, func(t *testing.T) {
					ctx := context.Background()
					store := newBridgeTestStore(t)
					executor := &recordingExecutor{}
					bridge := newBridgeTestBridge(nil, store, executor)
					if _, _, err := store.CreateSession(ctx, teamstore.SessionContext{
						ID:         controlFallbackSessionID,
						Status:     teamstore.SessionStatusActive,
						RunnerKind: "control_fallback",
						Model:      DefaultControlFallbackModel,
					}); err != nil {
						t.Fatalf("CreateSession: %v", err)
					}
					now := time.Now().UTC()
					inbound, created, err := store.PersistInbound(ctx, teamstore.InboundEvent{
						ID:             "inbound:queued-control-fail-closed",
						SessionID:      controlFallbackSessionID,
						TeamsChatID:    "control-chat",
						TeamsMessageID: "queued-control-fail-closed",
						Text:           "helper upgrade prerelease",
						Source:         source,
						Status:         teamstore.InboundStatusPersisted,
						CreatedAt:      now,
						UpdatedAt:      now,
					})
					if err != nil || !created {
						t.Fatalf("PersistInbound created=%v err=%v", created, err)
					}
					turn, created, err := store.QueueTurn(ctx, teamstore.Turn{
						ID:             "turn:queued-control-fail-closed",
						SessionID:      controlFallbackSessionID,
						InboundEventID: inbound.ID,
						Status:         teamstore.TurnStatusQueued,
						CreatedAt:      now,
						UpdatedAt:      now,
					})
					if err != nil || !created {
						t.Fatalf("QueueTurn created=%v err=%v", created, err)
					}
					if useSQLite {
						if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
							t.Fatalf("MigrateLargeStateToSQLite: %v", err)
						}
					}
					claimed, claimedInbound, claimedOK, inboundFound, inboundRead, err := store.ClaimNextQueuedTurnWithInbound(ctx, controlFallbackSessionID)
					if err != nil || !claimedOK || !inboundFound || !inboundRead {
						t.Fatalf("ClaimNextQueuedTurnWithInbound claimed=%v inboundFound=%v inboundRead=%v err=%v turn=%#v", claimedOK, inboundFound, inboundRead, err, claimed)
					}
					if claimed.ID != turn.ID || claimed.Status != teamstore.TurnStatusRunning {
						t.Fatalf("claimed turn=%#v, want running %s", claimed, turn.ID)
					}

					state := teamstore.State{SchemaVersion: teamstore.SchemaVersion, InboundEvents: map[string]teamstore.InboundEvent{
						claimedInbound.ID: claimedInbound,
					}}
					if err := bridge.recoverQueuedTurn(ctx, &Session{ID: controlFallbackSessionID, ChatID: "control-chat", Status: "active"}, claimed, state); err != nil {
						t.Fatalf("recoverQueuedTurn: %v", err)
					}
					if got := executor.promptCount(); got != 0 {
						t.Fatalf("source %s entered Codex: %d executor calls", source, got)
					}
					storedTurn, found, err := store.TurnByID(ctx, claimed.ID)
					if err != nil || !found {
						t.Fatalf("read quarantined turn: found=%v err=%v turn=%#v", found, err, storedTurn)
					}
					if storedTurn.Status != teamstore.TurnStatusInterrupted || !strings.Contains(storedTurn.RecoveryReason, source) {
						t.Fatalf("quarantined turn=%#v, want interrupted reason containing %s", storedTurn, source)
					}
					storedInbound, found, err := store.InboundEventByID(ctx, claimedInbound.ID)
					if err != nil || !found {
						t.Fatalf("read quarantined inbound: found=%v err=%v inbound=%#v", found, err, storedInbound)
					}
					if storedInbound.Status != teamstore.InboundStatusIgnored {
						t.Fatalf("quarantined inbound=%#v, want ignored", storedInbound)
					}
				})
			}
		})
	}
}

func TestDeferredInboundRecoveryUsesBoundedDurableQuantum(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			store := newBridgeTestStore(t)
			bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
			now := time.Now().UTC()
			wantRows := maxDeferredInboundRecoveryPerPhase + 3
			for i := 0; i < wantRows; i++ {
				inbound := teamstore.InboundEvent{
					ID:             fmt.Sprintf("inbound:bounded-recovery-%02d", i),
					SessionID:      controlFallbackSessionID,
					TeamsChatID:    "control-chat",
					TeamsMessageID: fmt.Sprintf("bounded-recovery-%02d", i),
					Text:           "helper restart now",
					Source:         "teams_control_poll_deferred",
					Status:         teamstore.InboundStatusDeferred,
					CreatedAt:      now.Add(time.Duration(i) * time.Second),
					UpdatedAt:      now.Add(time.Duration(i) * time.Second),
				}
				if _, _, err := store.PersistInbound(ctx, inbound); err != nil {
					t.Fatalf("PersistInbound(%d): %v", i, err)
				}
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}

			if err := bridge.processDeferredInbound(ctx); err != nil {
				t.Fatalf("bounded processDeferredInbound: %v", err)
			}
			candidates, err := store.InboundRecoveryCandidates(ctx)
			if err != nil {
				t.Fatalf("InboundRecoveryCandidates: %v", err)
			}
			if got, want := len(candidates), wantRows-maxDeferredInboundRecoveryPerPhase; got != want {
				t.Fatalf("remaining automatic recovery candidates=%d, want %d after one bounded quantum", got, want)
			}
			for _, inbound := range candidates {
				if inbound.Status != teamstore.InboundStatusDeferred {
					t.Fatalf("unselected recovery row changed status: %#v", inbound)
				}
			}
		})
	}
}

func TestDeferredCancellationAndProcessWideFailureRemainFatal(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	inbound := teamstore.InboundEvent{ID: "inbound:deferred-fatal-boundary", Status: teamstore.InboundStatusDeferred}
	for _, rowErr := range []error{context.Canceled, context.DeadlineExceeded, teamstore.ErrControlLeaseNotHeld} {
		handled, err := bridge.handleDeferredInboundRowFailure(ctx, inbound, rowErr)
		if handled || !errors.Is(err, rowErr) {
			t.Fatalf("failure %v handled=%t err=%v, want fatal propagation", rowErr, handled, err)
		}
	}
}

func TestDeferredAuthPoisonRowIsHeldAndDoesNotBlockFollowingWork(t *testing.T) {
	ctx := context.Background()
	writeGraph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "following work completed"}}
	bridge := newBridgeTestBridge(writeGraph, store, executor)
	bridge.asyncTurns = false
	session := bridge.reg.SessionByChatID("chat-1")
	if err := bridge.ensureDurableSession(ctx, session); err != nil {
		t.Fatalf("ensureDurableSession: %v", err)
	}
	readAuth := &deferredRecoveryAuthStub{err: &ReauthRequiredError{Action: "Teams chat access", Reason: "auth cache is missing"}}
	bridge.readGraph = &GraphClient{
		auth:       readAuth,
		client:     http.DefaultClient,
		baseURL:    "https://graph.example.test",
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(delay time.Duration) time.Duration { return delay },
	}
	now := time.Now().UTC()
	poison := teamstore.InboundEvent{
		ID:             "inbound:deferred-auth-poison",
		SessionID:      session.ID,
		TeamsChatID:    session.ChatID,
		TeamsMessageID: "deferred-auth-poison",
		Source:         "teams_session_import_deferred_attachment",
		Status:         teamstore.InboundStatusDeferred,
		TeamsAttachments: []teamstore.InboundAttachmentContext{{
			ID: "attachment-1", ContentType: "reference", Name: "input.txt",
		}},
		CreatedAt: now,
		UpdatedAt: now,
	}
	following := teamstore.InboundEvent{
		ID:             "inbound:deferred-auth-following",
		SessionID:      session.ID,
		TeamsChatID:    session.ChatID,
		TeamsMessageID: "deferred-auth-following",
		Source:         "teams_session_import_deferred",
		Status:         teamstore.InboundStatusDeferred,
		TeamsBodyType:  "text",
		Text:           "following work should still execute",
		CreatedAt:      now.Add(time.Second),
		UpdatedAt:      now.Add(time.Second),
	}
	for _, inbound := range []teamstore.InboundEvent{poison, following} {
		if _, _, err := store.PersistInbound(ctx, inbound); err != nil {
			t.Fatalf("PersistInbound %s: %v", inbound.ID, err)
		}
	}

	if err := bridge.processDeferredInbound(ctx); err != nil {
		t.Fatalf("processDeferredInbound: %v", err)
	}
	poisonAfter, found, err := store.InboundEventByID(ctx, poison.ID)
	if err != nil || !found {
		t.Fatalf("read poison row: found=%v err=%v row=%#v", found, err, poisonAfter)
	}
	if poisonAfter.Status != teamstore.InboundStatusManualHold || !poisonAfter.NextAttemptAt.IsZero() {
		t.Fatalf("auth poison row = %#v, want durable manual hold with no retry eligibility", poisonAfter)
	}
	followingAfter, found, err := store.InboundEventByID(ctx, following.ID)
	if err != nil || !found {
		t.Fatalf("read following row: found=%v err=%v row=%#v", found, err, followingAfter)
	}
	if followingAfter.TurnID == "" {
		t.Fatalf("following deferred row was blocked by auth poison row: %#v", followingAfter)
	}
	turn, found, err := store.TurnByID(ctx, followingAfter.TurnID)
	if err != nil || !found || turn.Status != teamstore.TurnStatusCompleted {
		t.Fatalf("following turn = found:%v err:%v turn:%#v, want completed", found, err, turn)
	}
	if got := executor.promptCount(); got != 1 {
		t.Fatalf("following work executor calls = %d, want one", got)
	}
	if readAuth.calls.Load() != 1 {
		t.Fatalf("auth poison row Graph/auth calls = %d, want one", readAuth.calls.Load())
	}
	if len(*sent) == 0 {
		t.Fatal("following work did not publish its durable output")
	}
	if candidates, err := store.InboundRecoveryCandidates(ctx); err != nil {
		t.Fatalf("read remaining deferred candidates: %v", err)
	} else if len(candidates) != 0 {
		t.Fatalf("manual-held poison row or completed following row remained an automatic candidate: %#v", candidates)
	}
}

func TestDeferredTemporaryAuthFailureUsesDurableBackoff(t *testing.T) {
	ctx := context.Background()
	writeGraph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(writeGraph, store, &recordingExecutor{})
	bridge.asyncTurns = false
	session := bridge.reg.SessionByChatID("chat-1")
	if err := bridge.ensureDurableSession(ctx, session); err != nil {
		t.Fatalf("ensureDurableSession: %v", err)
	}
	readAuth := &deferredRecoveryAuthStub{err: &TemporaryAuthError{Action: "Teams chat access", Err: errors.New("oauth 503")}}
	bridge.readGraph = &GraphClient{
		auth:       readAuth,
		client:     http.DefaultClient,
		baseURL:    "https://graph.example.test",
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(delay time.Duration) time.Duration { return delay },
	}
	inbound := teamstore.InboundEvent{
		ID:             "inbound:deferred-temporary-auth",
		SessionID:      session.ID,
		TeamsChatID:    session.ChatID,
		TeamsMessageID: "deferred-temporary-auth",
		Source:         "teams_session_import_deferred_attachment",
		Status:         teamstore.InboundStatusDeferred,
		TeamsAttachments: []teamstore.InboundAttachmentContext{{
			ID: "attachment-1", ContentType: "reference", Name: "input.txt",
		}},
		CreatedAt: time.Now().UTC(),
	}
	if _, _, err := store.PersistInbound(ctx, inbound); err != nil {
		t.Fatalf("PersistInbound: %v", err)
	}
	if err := bridge.processDeferredInbound(ctx); err != nil {
		t.Fatalf("first processDeferredInbound: %v", err)
	}
	first, found, err := store.InboundEventByID(ctx, inbound.ID)
	if err != nil || !found {
		t.Fatalf("read deferred temporary-auth row: found=%v err=%v row=%#v", found, err, first)
	}
	if first.Status != teamstore.InboundStatusDeferred || first.FailureCount != 1 || !first.NextAttemptAt.After(time.Now()) {
		t.Fatalf("temporary auth row = %#v, want deferred durable backoff", first)
	}
	if got := readAuth.calls.Load(); got != 1 {
		t.Fatalf("first temporary auth pass calls = %d, want one", got)
	}
	if err := bridge.processDeferredInbound(ctx); err != nil {
		t.Fatalf("immediate second processDeferredInbound: %v", err)
	}
	if got := readAuth.calls.Load(); got != 1 {
		t.Fatalf("durable temporary-auth gate was ignored: calls=%d, want one", got)
	}
}

func TestDeferredControlPollInboundIsHeldInsteadOfGenericReplay(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			graph, sent := newBridgeTestGraph(t)
			executor := &recordingExecutor{result: ExecutionResult{Text: "must not run"}}
			store := newBridgeTestStore(t)
			bridge := newBridgeTestBridge(graph, store, executor)
			inbound := teamstore.InboundEvent{
				ID:             "deferred-control-lifecycle-" + name,
				SessionID:      controlFallbackSessionID,
				TeamsChatID:    "control-chat",
				TeamsMessageID: "control-lifecycle-" + name,
				Text:           "helper restart now",
				Source:         "teams_control_poll_deferred",
				Status:         teamstore.InboundStatusDeferred,
				CreatedAt:      time.Now().UTC(),
				UpdatedAt:      time.Now().UTC(),
			}
			if _, _, err := store.PersistInbound(ctx, inbound); err != nil {
				t.Fatalf("PersistInbound: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}

			if err := bridge.processDeferredInbound(ctx); err != nil {
				t.Fatalf("processDeferredInbound: %v", err)
			}
			got, found, err := store.InboundEventByID(ctx, inbound.ID)
			if err != nil || !found {
				t.Fatalf("InboundEventByID: found=%v err=%v row=%#v", found, err, got)
			}
			if got.Status != teamstore.InboundStatusManualHold {
				t.Fatalf("held control status = %q, want %q", got.Status, teamstore.InboundStatusManualHold)
			}
			if got.OperationState != "manual_hold" || got.OperationKey == "" || got.HoldReason == "" || got.HoldRequiredEvidence == "" || got.HoldNextAction == "" || got.HoldWakeCondition == "" {
				t.Fatalf("held control metadata is incomplete: %#v", got)
			}
			candidates, err := store.InboundRecoveryCandidates(ctx)
			if err != nil {
				t.Fatalf("InboundRecoveryCandidates: %v", err)
			}
			if len(candidates) != 0 {
				t.Fatalf("manual-hold row remained an automatic candidate: %#v", candidates)
			}
			if got := executor.promptCount(); got != 0 {
				t.Fatalf("generic deferred control replay started executor %d time(s)", got)
			}
			if len(*sent) != 0 {
				t.Fatalf("generic deferred control replay issued %d Graph message POST(s)", len(*sent))
			}

			// A restart/recovery sweep must not rediscover the held command.
			if err := bridge.processDeferredInbound(ctx); err != nil {
				t.Fatalf("second processDeferredInbound: %v", err)
			}
			if gotAgain, _, err := store.InboundEventByID(ctx, inbound.ID); err != nil || gotAgain.Status != teamstore.InboundStatusManualHold {
				t.Fatalf("held row changed on second sweep: row=%#v err=%v", gotAgain, err)
			}
		})
	}
}

func TestDeferredLegacyControlNewWithoutOperationKeyIsHeld(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			graph, sent := newBridgeTestGraph(t)
			executor := &recordingExecutor{result: ExecutionResult{Text: "must not run"}}
			store := newBridgeTestStore(t)
			bridge := newBridgeTestBridge(graph, store, executor)
			inbound := teamstore.InboundEvent{
				ID:             "inbound:legacy-control-new-" + name,
				SessionID:      controlFallbackSessionID,
				TeamsChatID:    "control-chat",
				TeamsMessageID: "legacy-control-new-" + name,
				Text:           "new " + t.TempDir(),
				TeamsBodyType:  "text",
				Source:         "teams_control_new",
				Status:         teamstore.InboundStatusDeferred,
				CreatedAt:      time.Now().UTC(),
				UpdatedAt:      time.Now().UTC(),
			}
			if _, _, err := store.PersistInbound(ctx, inbound); err != nil {
				t.Fatalf("PersistInbound: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}

			if err := bridge.processDeferredInbound(ctx); err != nil {
				t.Fatalf("processDeferredInbound: %v", err)
			}
			got, found, err := store.InboundEventByID(ctx, inbound.ID)
			if err != nil || !found {
				t.Fatalf("InboundEventByID: found=%v err=%v row=%#v", found, err, got)
			}
			if got.Status != teamstore.InboundStatusManualHold || got.OperationState != "manual_hold" {
				t.Fatalf("legacy control-new disposition = %#v, want durable manual hold", got)
			}
			if got.OperationKey == "" || got.HoldRequiredEvidence == "" || got.HoldNextAction == "" || got.HoldWakeCondition == "" {
				t.Fatalf("legacy control-new hold metadata is incomplete: %#v", got)
			}
			if candidates, err := store.InboundRecoveryCandidates(ctx); err != nil {
				t.Fatalf("InboundRecoveryCandidates: %v", err)
			} else if len(candidates) != 0 {
				t.Fatalf("legacy manual-hold row remained a candidate: %#v", candidates)
			}
			if executor.promptCount() != 0 || len(*sent) != 0 {
				t.Fatalf("legacy control-new replay crossed a side effect boundary: executor=%d sent=%d", executor.promptCount(), len(*sent))
			}
		})
	}
}

func TestDeferredControlModelProfileFailureUsesDurableBackoff(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		name := "json"
		if useSQLite {
			name = "sqlite"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			graph, sent := newBridgeTestGraph(t)
			store := newBridgeTestStore(t)
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			var resolverCalls int
			bridge.modelProfileResolver = func(context.Context, string) (modelprofile.Snapshot, error) {
				resolverCalls++
				return modelprofile.Snapshot{}, errors.New("synthetic profile resolver outage")
			}
			messageID := "deferred-profile-failure-" + name
			inbound := teamstore.InboundEvent{
				SessionID:      controlFallbackSessionID,
				TeamsChatID:    "control-chat",
				TeamsMessageID: messageID,
				Text:           "new " + t.TempDir() + " --model-profile broken",
				TeamsBodyType:  "text",
				Source:         "teams_control_new",
				Status:         teamstore.InboundStatusDeferred,
				CreatedAt:      time.Now().UTC().Add(-time.Minute),
			}
			if _, _, err := store.PersistInbound(ctx, inbound); err != nil {
				t.Fatalf("PersistInbound: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			inboundID := teamstoreInboundIDForChatMessage("control-chat", messageID)
			key := bridge.deferredControlOperationKey("teams_control_new", inboundID, messageID)
			if _, _, err := store.UpdateInboundEvent(ctx, inboundID, func(current teamstore.InboundEvent, found bool, now time.Time) (teamstore.InboundEvent, bool, error) {
				if !found {
					t.Fatalf("inbound %q was not found while seeding operation metadata", inboundID)
				}
				current.OperationState = "deferred"
				current.OperationKey = key
				current.UpdatedAt = now
				return current, true, nil
			}); err != nil {
				t.Fatalf("seed operation metadata: %v", err)
			}

			if err := bridge.processDeferredInbound(ctx); err != nil {
				t.Fatalf("profile resolver failure should be row-local: %v", err)
			}
			first, found, err := store.InboundEventByID(ctx, inboundID)
			if err != nil || !found {
				t.Fatalf("read deferred profile row: found=%v err=%v row=%#v", found, err, first)
			}
			if first.Status != teamstore.InboundStatusDeferred || first.FailureCount != 1 || first.NextAttemptAt.IsZero() || first.OperationState != "prepared" {
				t.Fatalf("profile resolver failure row = %#v, want prepared durable backoff", first)
			}
			if resolverCalls != 1 || len(*sent) != 0 {
				t.Fatalf("profile resolver failure crossed boundary: resolverCalls=%d sent=%d", resolverCalls, len(*sent))
			}
			if err := bridge.processDeferredInbound(ctx); err != nil {
				t.Fatalf("immediate retry before durable deadline: %v", err)
			}
			if resolverCalls != 1 {
				t.Fatalf("durable retry gate was ignored: resolverCalls=%d, want 1", resolverCalls)
			}
		})
	}
}
