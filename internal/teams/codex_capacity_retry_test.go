package teams

import (
	"context"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
)

func TestFormatCodexStreamRetryStatusForModelCapacity(t *testing.T) {
	got := formatCodexStreamRetryStatus(codexrunner.StreamEvent{
		Kind:      codexrunner.StreamEventStreamRetry,
		WillRetry: true,
		Failure: &codexrunner.TurnFailure{
			Message: "Selected model is at capacity. Please try a different model.",
		},
	})
	if got != "Codex is waiting for model capacity and will retry this turn." {
		t.Fatalf("capacity retry status = %q", got)
	}
	if strings.Contains(got, "Connection dropped") || strings.Contains(got, "Please try a different model") {
		t.Fatalf("capacity retry used stream-disconnect/raw-error wording: %q", got)
	}
}

func TestFormatCodexStreamRetryStatusForServerOverloadedCode(t *testing.T) {
	got := formatCodexStreamRetryStatus(codexrunner.StreamEvent{
		Kind: codexrunner.StreamEventStreamRetry,
		Failure: &codexrunner.TurnFailure{
			Code: "serverOverloaded",
		},
	})
	if got != "Codex is waiting for model capacity and will retry this turn." {
		t.Fatalf("server overloaded retry status = %q", got)
	}
}

func TestCodexEventForwarderSendsCapacityRetryStatus(t *testing.T) {
	ctx := context.Background()
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	seedBridgeCanQueueLiveTurnOutboxFixtures(t, ctx, store)
	forwarder := &codexEventForwarder{
		ctx:              ctx,
		bridge:           bridge,
		sessionID:        "session-1",
		expectedThreadID: "thread-1",
		turnID:           "turn-running",
		chatID:           "chat-1",
	}
	forwarder.handle(codexrunner.StreamEvent{
		Kind:      codexrunner.StreamEventStreamRetry,
		ThreadID:  "thread-1",
		TurnID:    "codex-turn-1",
		WillRetry: true,
		Failure: &codexrunner.TurnFailure{
			Code: "serverOverloaded",
		},
	})
	if got := sentPlainJoined(*sent); !strings.Contains(got, "Codex is waiting for model capacity and will retry this turn.") {
		t.Fatalf("capacity retry status was not forwarded to Teams: %q", got)
	}
}
