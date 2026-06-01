package teams

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestInfraLaunchFailureNotice(t *testing.T) {
	t.Run("reframes launch_failure structurally", func(t *testing.T) {
		err := &codexrunner.Error{Kind: codexrunner.ErrorLaunch, Message: "unsupported config version"}
		body, ok := infraLaunchFailureNotice(err)
		if !ok {
			t.Fatal("expected launch_failure to be reframed")
		}
		if !strings.Contains(body, "setup issue") {
			t.Fatalf("body should frame as a setup issue, got %q", body)
		}
		if !strings.Contains(body, "helper retry last") {
			t.Fatalf("body should be Teams-actionable, got %q", body)
		}
		// The raw diagnostic is preserved for the admin, not hidden.
		if !strings.Contains(body, "unsupported config version") {
			t.Fatalf("body should retain the diagnostic, got %q", body)
		}
	})

	t.Run("reframes wrapped launch_failure", func(t *testing.T) {
		err := fmt.Errorf("run failed: %w", &codexrunner.Error{Kind: codexrunner.ErrorLaunch, Err: fmt.Errorf("boom")})
		if _, ok := infraLaunchFailureNotice(err); !ok {
			t.Fatal("expected wrapped launch_failure to be reframed")
		}
	})

	t.Run("leaves codex/content failures alone", func(t *testing.T) {
		if _, ok := infraLaunchFailureNotice(&codexrunner.Error{Kind: codexrunner.ErrorCodex, Message: "model error"}); ok {
			t.Fatal("codex failures must keep their own text")
		}
		if _, ok := infraLaunchFailureNotice(fmt.Errorf("plain error")); ok {
			t.Fatal("plain errors must not be reframed")
		}
		if _, ok := infraLaunchFailureNotice(nil); ok {
			t.Fatal("nil must not be reframed")
		}
	})
}

// TestRunQueuedTurnReframesLaunchFailure is the end-to-end check: a turn that
// fails with a codexrunner launch_failure must reach the user as the reframed,
// non-blaming setup-issue message (owner mentioned), not the raw error text.
func TestRunQueuedTurnReframesLaunchFailure(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	session := (&Registry{
		Sessions: []Session{{ID: "s001", ChatID: "chat-1", Status: "active"}},
	}).SessionByChatID("chat-1")

	launchErr := &codexrunner.Error{Kind: codexrunner.ErrorLaunch, Message: "unsupported config version 2"}
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{err: launchErr})
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}
	turn, _, err := store.QueueTurn(context.Background(), teamstore.Turn{ID: "turn:launchfail", SessionID: session.ID})
	if err != nil {
		t.Fatalf("QueueTurn error: %v", err)
	}

	if err := bridge.runQueuedTurn(context.Background(), session, turn, session.ChatID, "do something"); err != nil {
		t.Fatalf("runQueuedTurn error: %v", err)
	}

	if len(*sent) != 1 {
		t.Fatalf("sent count = %d, want 1: %#v", len(*sent), *sent)
	}
	if (*sent)[0].Mentions != 1 {
		t.Fatalf("infra error should mention owner, mentions = %d", (*sent)[0].Mentions)
	}
	got := PlainTextFromTeamsHTML((*sent)[0].Content)
	if !strings.Contains(got, "setup issue") || !strings.Contains(got, "helper retry last") {
		t.Fatalf("expected reframed setup-issue message, got %q", got)
	}
	// The raw "error: launch_failure: ..." form must NOT be what the user sees.
	if strings.HasPrefix(strings.TrimSpace(got), "error: launch_failure") {
		t.Fatalf("user should not see the raw launch_failure text, got %q", got)
	}
}
