package teams

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestTeamsQueueOnlyPollAnnotationDoesNotPerformGraphWrites(t *testing.T) {
	for _, tc := range []struct {
		name string
		key  any
	}{
		{name: "work", key: workPollQueueOnlyContextKey{}},
		{name: "control", key: controlPollQueueOnlyContextKey{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			requests := 0
			graph := &GraphClient{
				auth: &fakeGraphAuth{token: "access"},
				client: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					requests++
					response := httptest.NewRecorder()
					response.WriteHeader(http.StatusNoContent)
					return response.Result(), nil
				})},
				baseURL:    graphBaseURL,
				maxRetries: 0,
				sleep:      sleepContext,
			}
			store := newBridgeTestStore(t)
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			bridge.annotateUserMessages = true
			ctx := context.WithValue(context.Background(), tc.key, true)

			self := bridgePollMessage("queue-only-self", "2026-05-03T01:00:00Z", "self-authored prompt")
			bridge.annotateIncomingUserMessage(ctx, "chat-1", self)
			if requests != 0 {
				t.Fatalf("self-authored queue-only annotation issued %d Graph write(s)", requests)
			}

			external := bridgePollMessage("queue-only-external", "2026-05-03T01:00:01Z", "external prompt")
			external.From.User.ID = "user-2"
			external.From.User.DisplayName = "Alex"
			bridge.annotateIncomingUserMessage(ctx, "chat-1", external)
			if requests != 0 {
				t.Fatalf("queue-only marker mirror issued %d Graph write(s)", requests)
			}

			state, err := store.Load(context.Background())
			if err != nil {
				t.Fatalf("Load state: %v", err)
			}
			marker, ok := state.OutboxMessages["outbox:user-marker:"+shortStableID("chat-1:"+external.ID)]
			if !ok {
				t.Fatalf("queue-only external annotation did not persist its marker outbox")
			}
			if marker.Status != teamstore.OutboxStatusQueued {
				t.Fatalf("queue-only marker status = %q, want queued", marker.Status)
			}
		})
	}
}

func TestTeamsQueueOnlyControlNewDefersGraphCreation(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	ctx := context.WithValue(context.Background(), controlPollQueueOnlyContextKey{}, true)
	msg := bridgePollMessage("queue-only-control-new", "2026-05-03T01:00:00Z", "new /tmp/queue-only-workspace")

	if err := bridge.handleControlMessage(ctx, msg, msg.Body.Content); err != nil {
		t.Fatalf("queue-only control new: %v", err)
	}
	if len(*sent) != 0 {
		t.Fatalf("queue-only control new issued %d Graph message POST(s), want zero", len(*sent))
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	var found *teamstore.InboundEvent
	for _, inbound := range state.InboundEvents {
		if inbound.TeamsMessageID == msg.ID {
			copy := inbound
			found = &copy
			break
		}
	}
	if found == nil {
		t.Fatalf("queue-only control new did not create a durable inbound hand-off: %#v", state.InboundEvents)
	}
	if found.Status != teamstore.InboundStatusDeferred || found.Source != "teams_control_new" {
		t.Fatalf("queue-only control new inbound = %#v, want deferred teams_control_new", *found)
	}
	wantKey := bridge.deferredControlOperationKey("teams_control_new", found.ID, msg.ID)
	if found.OperationState != "deferred" || found.OperationKey != wantKey {
		t.Fatalf("queue-only control new operation metadata = state=%q key=%q, want deferred/%q", found.OperationState, found.OperationKey, wantKey)
	}
}

func TestTeamsQueueOnlyControlSelectUsesGenericReplayLane(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	ctx := context.WithValue(context.Background(), controlPollQueueOnlyContextKey{}, true)
	msg := bridgePollMessage("queue-only-control-select", "2026-05-03T01:00:00Z", "4")

	if err := bridge.handleControlMessage(ctx, msg, msg.Body.Content); err != nil {
		t.Fatalf("queue-only control select: %v", err)
	}
	if len(*sent) != 0 {
		t.Fatalf("queue-only control select issued %d Graph message POST(s), want zero", len(*sent))
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	var found *teamstore.InboundEvent
	for _, inbound := range state.InboundEvents {
		if inbound.TeamsMessageID == msg.ID {
			copy := inbound
			found = &copy
			break
		}
	}
	if found == nil {
		t.Fatalf("queue-only control select did not create a durable inbound hand-off: %#v", state.InboundEvents)
	}
	if found.Status != teamstore.InboundStatusDeferred || found.Source != "teams_control_poll_deferred" {
		t.Fatalf("queue-only control select inbound = %#v, want deferred teams_control_poll_deferred", *found)
	}
}

func TestTeamsQueueOnlyControlFallbackDefersPlainAndAskWithoutExecutor(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "must not run"}}
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, executor)
	ctx := context.WithValue(context.Background(), controlPollQueueOnlyContextKey{}, true)

	for _, tc := range []struct {
		name string
		text string
	}{
		{name: "plain", text: "a normal control prompt"},
		{name: "ask", text: "ask a control prompt"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			msg := bridgePollMessage("queue-only-control-"+tc.name, "2026-05-03T01:00:00Z", tc.text)
			if err := bridge.handleControlMessage(ctx, msg, msg.Body.Content); err != nil {
				t.Fatalf("queue-only control %s: %v", tc.name, err)
			}
		})
	}
	if len(*sent) != 0 {
		t.Fatalf("queue-only control fallback issued %d Graph POST(s), want zero", len(*sent))
	}
	if got := executor.promptCount(); got != 0 {
		t.Fatalf("queue-only control fallback started executor %d time(s), want zero", got)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	for _, id := range []string{"queue-only-control-plain", "queue-only-control-ask"} {
		var found *teamstore.InboundEvent
		for _, inbound := range state.InboundEvents {
			if inbound.TeamsMessageID == id {
				copy := inbound
				found = &copy
				break
			}
		}
		if found == nil {
			t.Fatalf("queue-only control %s did not create durable hand-off: %#v", id, state.InboundEvents)
		}
		if found.Status != teamstore.InboundStatusDeferred || found.Source != "teams_control_poll_deferred" {
			t.Fatalf("queue-only control %s inbound = %#v, want deferred generic replay", id, *found)
		}
	}
}

func TestTeamsQueueOnlyWorkMutatingHelperDefersWithoutExecutorOrGraph(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "must not run"}}
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, executor)
	ctx := context.WithValue(context.Background(), workPollQueueOnlyContextKey{}, true)
	msg := bridgePollMessage("queue-only-work-close", "2026-05-03T01:00:00Z", "helper close")

	if err := bridge.handleSessionMessageWithQueueState(ctx, "chat-1", msg, msg.Body.Content, nil, nil); err != nil {
		t.Fatalf("queue-only work close: %v", err)
	}
	if len(*sent) != 0 {
		t.Fatalf("queue-only work close issued %d Graph POST(s), want zero", len(*sent))
	}
	if got := executor.promptCount(); got != 0 {
		t.Fatalf("queue-only work close started executor %d time(s), want zero", got)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	var found *teamstore.InboundEvent
	for _, inbound := range state.InboundEvents {
		if inbound.TeamsMessageID == msg.ID {
			copy := inbound
			found = &copy
			break
		}
	}
	if found == nil {
		t.Fatalf("queue-only work close did not create durable hand-off: %#v", state.InboundEvents)
	}
	if found.Status != teamstore.InboundStatusDeferred || found.Source != queueOnlySessionCommandSource {
		t.Fatalf("queue-only work close inbound = %#v, want deferred work replay", *found)
	}
	if session, ok := state.Sessions["s001"]; !ok || session.Status != teamstore.SessionStatusActive {
		t.Fatalf("queue-only work close changed durable session: present=%v session=%#v", ok, session)
	}
}

func TestTeamsQueueOnlyOnceModeDoesNotExecuteAdmittedWork(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	executor := &recordingExecutor{result: ExecutionResult{Text: "must not run"}}
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.asyncTurns = false // Listen(Once: true) uses this synchronous path.
	ctx := context.WithValue(context.Background(), workPollQueueOnlyContextKey{}, true)
	msg := bridgePollMessage("queue-only-once-prompt", "2026-05-03T01:00:00Z", "ordinary work prompt")

	if err := bridge.handleSessionMessageWithQueueState(ctx, "chat-1", msg, msg.Body.Content, nil, nil); err != nil {
		t.Fatalf("queue-only once-mode work prompt: %v", err)
	}
	if len(*sent) != 0 {
		t.Fatalf("queue-only once-mode work prompt issued %d Graph POST(s), want zero", len(*sent))
	}
	if got := executor.promptCount(); got != 0 {
		t.Fatalf("queue-only once-mode work prompt started executor %d time(s), want zero", got)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	var found *teamstore.InboundEvent
	for _, inbound := range state.InboundEvents {
		if inbound.TeamsMessageID == msg.ID {
			copy := inbound
			found = &copy
			break
		}
	}
	if found == nil || found.Status != teamstore.InboundStatusQueued {
		t.Fatalf("queue-only once-mode inbound = %#v, want durable queued receipt", found)
	}
	turn, ok := state.Turns[found.TurnID]
	if !ok || turn.Status != teamstore.TurnStatusQueued {
		t.Fatalf("queue-only once-mode turn = %#v present=%v, want queued", turn, ok)
	}
}

func TestTeamsQueueOnlyStatsLeavesDeliveryInOutbox(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	ctx := context.WithValue(context.Background(), workPollQueueOnlyContextKey{}, true)

	if err := bridge.sendStatsToChat(ctx, "chat-1", "tokens: 42"); err != nil {
		t.Fatalf("queue-only stats: %v", err)
	}
	if len(*sent) != 0 {
		t.Fatalf("queue-only stats issued %d Graph POST(s), want zero", len(*sent))
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	found := false
	for _, msg := range state.OutboxMessages {
		if msg.TeamsChatID == "chat-1" && msg.Kind == "helper-stats" && msg.Status == teamstore.OutboxStatusQueued {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("queue-only stats did not leave a queued helper-stats outbox row: %#v", state.OutboxMessages)
	}
}

func TestQueueOnlyCommandClassifiersAreFailClosed(t *testing.T) {
	for _, command := range []DashboardCommandName{
		DashboardCommandNew, DashboardCommandAsk, DashboardCommandWorkspaces,
		DashboardCommandWorkspace, DashboardCommandSessions, DashboardCommandOpen,
		DashboardCommandPark, DashboardCommandResume, DashboardCommandSelect,
		DashboardCommandPublish, DashboardCommandMkdir, DashboardCommandRename,
		DashboardCommandCancel, DashboardCommandSkills, DashboardCommandBeacon,
		DashboardCommandModel, DashboardCommandEffort, DashboardCommandDefault,
		DashboardCommandRestart, DashboardCommandReload, DashboardCommandUpdate,
		DashboardCommandCodexUpdate, DashboardCommandWebhook, DashboardCommandFork,
	} {
		if !controlCommandRequiresForeground(command) {
			t.Fatalf("control command %q unexpectedly allowed in queue-only lane", command)
		}
	}
	for _, command := range []DashboardCommandName{
		DashboardCommandStatus, DashboardCommandDetails, DashboardCommandHelp,
	} {
		if controlCommandRequiresForeground(command) {
			t.Fatalf("control diagnostic command %q unexpectedly fenced", command)
		}
	}
	for _, command := range []DashboardCommandName{
		DashboardCommandClose, DashboardCommandPark, DashboardCommandResume,
		DashboardCommandRetry, DashboardCommandRestoreThread, DashboardCommandCancel,
		DashboardCommandSendFile, DashboardCommandRename, DashboardCommandPublishHistory,
		DashboardCommandFork, DashboardCommandSkills, DashboardCommandBeacon,
		DashboardCommandModel, DashboardCommandEffort, DashboardCommandDefault,
	} {
		if !workCommandRequiresForeground(command) {
			t.Fatalf("work command %q unexpectedly allowed in queue-only lane", command)
		}
	}
	for _, command := range []DashboardCommandName{
		DashboardCommandStatus, DashboardCommandStats, DashboardCommandDetails,
		DashboardCommandHelp,
	} {
		if workCommandRequiresForeground(command) {
			t.Fatalf("work diagnostic command %q unexpectedly fenced", command)
		}
	}
}
