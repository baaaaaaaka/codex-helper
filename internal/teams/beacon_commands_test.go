package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/beacon"
	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestTeamsBeaconControlProfileLifecycleAndList(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})

	ctx := context.Background()
	if err := bridge.handleControlMessage(ctx, bridgeTestMessageWithText("beacon-create", "beacon profile create gpu --provider slurm --partition interactive --image image.sqsh --nodes 1 --gpu 1 --duration 4"), "beacon profile create gpu --provider slurm --partition interactive --image image.sqsh --nodes 1 --gpu 1 --duration 4"); err != nil {
		t.Fatalf("create profile: %v", err)
	}
	if err := bridge.handleControlMessage(ctx, bridgeTestMessageWithText("beacon-doctor", "beacon profile doctor gpu"), "beacon profile doctor gpu"); err != nil {
		t.Fatalf("doctor profile: %v", err)
	}
	if err := bridge.handleControlMessage(ctx, bridgeTestMessageWithText("beacon-confirm", "beacon profile confirm gpu"), "beacon profile confirm gpu"); err != nil {
		t.Fatalf("confirm profile: %v", err)
	}
	if err := bridge.handleControlMessage(ctx, bridgeTestMessageWithText("beacon-list", "beacon list"), "beacon list"); err != nil {
		t.Fatalf("list beacon: %v", err)
	}

	joined := sentPlainJoined(*sent)
	for _, want := range []string{"Created beacon profile \"gpu\"", "Confirmed beacon profile \"gpu\"", "Beacon list", "Profiles:", "gpu: ready", "Machines:"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("beacon output missing %q:\n%s", want, joined)
		}
	}
}

func TestTeamsBeaconNewWorkChatAndWorkSwitch(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	seedTeamsBeaconProfile(t, "cpu")
	graph, sent := newBeaconMeetingBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	workDir := t.TempDir()

	ctx := context.Background()
	if err := bridge.handleControlMessage(ctx, bridgeTestMessageWithText("beacon-new", "new "+workDir+" --beacon gpu"), "new "+workDir+" --beacon gpu"); err != nil {
		t.Fatalf("new beacon work chat: %v", err)
	}
	if len(bridge.reg.Sessions) < 2 {
		t.Fatalf("expected a new session, sessions=%#v", bridge.reg.Sessions)
	}
	session := bridge.reg.Sessions[len(bridge.reg.Sessions)-1]
	if err := bridge.handleSessionMessage(ctx, session.ChatID, bridgeTestMessageWithText("beacon-status", "beacon status"), "beacon status"); err != nil {
		t.Fatalf("work beacon status: %v", err)
	}
	if err := bridge.handleSessionMessage(ctx, session.ChatID, bridgeTestMessageWithText("beacon-switch", "beacon switch cpu"), "beacon switch cpu"); err != nil {
		t.Fatalf("work beacon switch: %v", err)
	}

	joined := sentPlainJoined(*sent)
	for _, want := range []string{"Execution target: beacon:gpu", "Beacon status", "current_target=beacon", "profile=gpu", "Switched this Work chat to beacon:cpu"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("beacon work output missing %q:\n%s", want, joined)
		}
	}
	st := loadTeamsBeaconState(t)
	conv := st.Conversations[session.ID]
	if conv.Current.Target != beacon.TargetBeacon || conv.Current.Profile != "cpu" {
		t.Fatalf("conversation current = %#v, want beacon cpu", conv.Current)
	}
}

func TestTeamsBeaconNewUsesSelectedWorkspaceAndIsolation(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	parent := t.TempDir()
	workDir := filepath.Join(parent, "selected-workspace")
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "selected",
			Path: workDir,
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-selected",
				FirstPrompt: "existing selected work",
				ProjectPath: workDir,
				ModifiedAt:  time.Date(2026, 4, 30, 12, 0, 0, 0, time.UTC),
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })

	var createdTopic string
	graph, sent := newBridgeCreateChatGraph(t, &createdTopic)
	store := newBridgeTestStore(t)
	bindBridgeTestControlChat(t, store, "control-chat")
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.reg.Sessions = nil

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-selected-projects", "projects"), "projects"); err != nil {
		t.Fatalf("projects: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-selected-workspace", "1"), "1"); err != nil {
		t.Fatalf("select workspace: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-selected-new", "new --beacon gpu --beacon-isolation exclusive"), "new --beacon gpu --beacon-isolation exclusive"); err != nil {
		t.Fatalf("new selected beacon workspace: %v", err)
	}

	session := bridge.reg.SessionByID("s001")
	if session == nil || session.Cwd != workDir {
		t.Fatalf("new --beacon without directory did not use selected workspace: %#v", bridge.reg.Sessions)
	}
	if !strings.Contains(createdTopic, "New message in "+filepath.Base(workDir)) {
		t.Fatalf("created topic = %q, want selected workspace placeholder title", createdTopic)
	}
	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "Execution target: beacon:gpu isolation=exclusive") {
		t.Fatalf("ready anchor missing beacon isolation target:\n%s", joined)
	}
	st := loadTeamsBeaconState(t)
	conv := st.Conversations["s001"]
	if conv.Current.Target != beacon.TargetBeacon || conv.Current.Profile != "gpu" || conv.Current.Isolation != beacon.IsolationExclusive {
		t.Fatalf("conversation target = %#v, want beacon gpu exclusive", conv.Current)
	}
}

func TestTeamsBeaconSwitchDuringRunningTurnSchedulesPending(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByID("s001")
	if session == nil {
		t.Fatal("test bridge missing s001")
	}
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensure durable session: %v", err)
	}
	if _, _, err := store.QueueTurn(context.Background(), teamstore.Turn{ID: "turn-running", SessionID: "s001", Status: teamstore.TurnStatusRunning}); err != nil {
		t.Fatalf("seed running turn: %v", err)
	}

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessageWithText("beacon-switch-running", "beacon switch gpu"), "beacon switch gpu"); err != nil {
		t.Fatalf("switch during running: %v", err)
	}

	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "Scheduled switch to beacon:gpu") || !strings.Contains(joined, "Future turns will use the pending target") {
		t.Fatalf("pending switch acknowledgement missing:\n%s", joined)
	}
	st := loadTeamsBeaconState(t)
	conv := st.Conversations["s001"]
	if conv.Current.Target != "" && conv.Current.Target != beacon.TargetLocal {
		t.Fatalf("current target = %#v, want local/default", conv.Current)
	}
	if conv.Pending == nil || conv.Pending.Target != beacon.TargetBeacon || conv.Pending.Profile != "gpu" {
		t.Fatalf("pending target = %#v, want beacon gpu", conv.Pending)
	}
}

func TestTeamsBeaconWorkSwitchLocalImmediateAndPending(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByID("s001")
	if session == nil {
		t.Fatal("test bridge missing s001")
	}
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensure durable session: %v", err)
	}
	beaconStore, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	if err := beaconStore.Update(func(st *beacon.State) error {
		st.Conversations["s001"] = beacon.Conversation{ID: "s001", Current: beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu"}}
		return nil
	}); err != nil {
		t.Fatalf("seed beacon conversation: %v", err)
	}

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessageWithText("beacon-local-idle", "beacon local"), "beacon local"); err != nil {
		t.Fatalf("switch local idle: %v", err)
	}
	st := loadTeamsBeaconState(t)
	if conv := st.Conversations["s001"]; conv.Current.Target != beacon.TargetLocal || conv.Pending != nil {
		t.Fatalf("idle local switch conversation = %#v, want current local with no pending", conv)
	}

	pending := appendBridgeTestSession(t, bridge, store, "s002", "chat-2")
	if err := beaconStore.Update(func(st *beacon.State) error {
		st.Conversations[pending.ID] = beacon.Conversation{ID: pending.ID, Current: beacon.TargetSnapshot{Target: beacon.TargetBeacon, Profile: "gpu"}}
		return nil
	}); err != nil {
		t.Fatalf("seed pending beacon conversation: %v", err)
	}
	if _, _, err := store.QueueTurn(context.Background(), teamstore.Turn{ID: "turn-running-local", SessionID: pending.ID, Status: teamstore.TurnStatusRunning}); err != nil {
		t.Fatalf("seed running turn: %v", err)
	}

	if err := bridge.handleSessionMessage(context.Background(), pending.ChatID, bridgeTestMessageWithText("beacon-local-running", "beacon switch local"), "beacon switch local"); err != nil {
		t.Fatalf("switch local running: %v", err)
	}
	joined := sentPlainJoined(*sent)
	for _, want := range []string{"Switched this Work chat to local execution", "Scheduled target switch", "Future turns will use local"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("local switch output missing %q:\n%s", want, joined)
		}
	}
	st = loadTeamsBeaconState(t)
	conv := st.Conversations[pending.ID]
	if conv.Current.Target != beacon.TargetBeacon || conv.Current.Profile != "gpu" {
		t.Fatalf("running local switch changed current target: %#v", conv.Current)
	}
	if conv.Pending == nil || conv.Pending.Target != beacon.TargetLocal {
		t.Fatalf("running local switch pending target = %#v, want local", conv.Pending)
	}
}

func TestTeamsBeaconListIncludesMachines(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	storePath := filepath.Clean(strings.TrimSpace(os.Getenv("CODEX_HELPER_BEACON_STORE")))
	store, err := beacon.NewStore(storePath)
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	if err := store.Update(func(st *beacon.State) error {
		st.Machines["gpu-a"] = beacon.Machine{ID: "gpu-a", LeaseID: "lease-gpu", Profile: "gpu", State: "accepting", Jobs: []string{"job-1"}, Chats: []string{"s001"}}
		return nil
	}); err != nil {
		t.Fatalf("seed machine: %v", err)
	}
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, newBridgeTestStore(t), &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-list-machines", "beacon list"), "beacon list"); err != nil {
		t.Fatalf("beacon list: %v", err)
	}
	joined := sentPlainJoined(*sent)
	for _, want := range []string{"Beacon list", "Profiles:", "gpu: ready", "Machines:", "gpu-a: state=accepting"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("beacon list missing %q:\n%s", want, joined)
		}
	}
}

func TestTeamsBeaconProfileStatusShowsMissingProxyProfile(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-missing-proxy-create", "beacon profile create jump --provider local --proxy ssh_profile --proxy-profile missing"), "beacon profile create jump --provider local --proxy ssh_profile --proxy-profile missing"); err != nil {
		t.Fatalf("create proxy profile: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-missing-proxy-doctor", "beacon profile doctor jump"), "beacon profile doctor jump"); err != nil {
		t.Fatalf("doctor proxy profile: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-missing-proxy-confirm", "beacon profile confirm jump"), "beacon profile confirm jump"); err != nil {
		t.Fatalf("confirm proxy profile: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-missing-proxy-status", "beacon profile status jump"), "beacon profile status jump"); err != nil {
		t.Fatalf("status proxy profile: %v", err)
	}

	joined := sentPlainJoined(*sent)
	for _, want := range []string{"ssh_profile:missing", "proxy profile not found", "ready: false"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("missing proxy output missing %q:\n%s", want, joined)
		}
	}
	st := loadTeamsBeaconState(t)
	if st.Profiles["jump"].Ready(bridge.beaconProxyResolver()) {
		t.Fatalf("profile with missing proxy should remain draft: %#v", st.Profiles["jump"])
	}
}

func TestTeamsBeaconNewWithDraftProfileRejectsWithoutCreatingWorkChat(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	store, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	if err := store.Update(func(st *beacon.State) error {
		st.Profiles["draft"] = beacon.Profile{Name: "draft", Provider: beacon.ProviderSlurm, ProxyMode: beacon.ProxyNone}
		return nil
	}); err != nil {
		t.Fatalf("seed draft profile: %v", err)
	}
	graph, sent := newBridgeTestGraph(t)
	teamStore := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, teamStore, &recordingExecutor{})
	workDir := filepath.Join(t.TempDir(), "should-not-create")

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-new-draft", "new "+workDir+" --beacon draft"), "new "+workDir+" --beacon draft"); err != nil {
		t.Fatalf("new draft beacon: %v", err)
	}

	if len(bridge.reg.Sessions) != 1 {
		t.Fatalf("draft beacon should not create a new work chat, sessions=%#v", bridge.reg.Sessions)
	}
	if _, err := os.Stat(workDir); !os.IsNotExist(err) {
		t.Fatalf("draft beacon should not create the workspace, stat err=%v", err)
	}
	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "cannot create beacon work chat for profile \"draft\"") ||
		strings.Contains(joined, "Work chat created") {
		t.Fatalf("draft beacon response mismatch:\n%s", joined)
	}
}

func TestTeamsBeaconNewRejectsInvalidIsolationBeforeSideEffects(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	graph, sent := newBridgeTestGraph(t)
	teamStore := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, teamStore, &recordingExecutor{})
	missingProfileDir := filepath.Join(t.TempDir(), "missing-profile")
	badIsolationDir := filepath.Join(t.TempDir(), "bad-isolation")

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-new-isolation-no-profile", "new "+missingProfileDir+" --beacon-isolation exclusive"), "new "+missingProfileDir+" --beacon-isolation exclusive"); err != nil {
		t.Fatalf("new beacon isolation without profile: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("beacon-new-isolation-invalid", "new "+badIsolationDir+" --beacon gpu --beacon-isolation solo"), "new "+badIsolationDir+" --beacon gpu --beacon-isolation solo"); err != nil {
		t.Fatalf("new beacon invalid isolation: %v", err)
	}

	if len(bridge.reg.Sessions) != 1 {
		t.Fatalf("invalid beacon new requests should not create Work chats, sessions=%#v", bridge.reg.Sessions)
	}
	for _, dir := range []string{missingProfileDir, badIsolationDir} {
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Fatalf("invalid beacon new request should not create workspace %s, stat err=%v", dir, err)
		}
	}
	joined := sentPlainJoined(*sent)
	for _, want := range []string{"--beacon-isolation requires --beacon <profile>", "isolation must be shared or exclusive"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("invalid isolation response missing %q:\n%s", want, joined)
		}
	}
}

func TestTeamsBeaconWrongChatCommandsDoNotMutateState(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessageWithText("wrong-work-profile", "beacon profile create gpu --provider local"), "beacon profile create gpu --provider local"); err != nil {
		t.Fatalf("work wrong-chat profile: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("wrong-control-switch", "beacon switch gpu"), "beacon switch gpu"); err != nil {
		t.Fatalf("control wrong-chat switch: %v", err)
	}

	joined := sentPlainJoined(*sent)
	if countSentPlainContaining(*sent, "Wrong chat") != 2 {
		t.Fatalf("wrong chat responses missing:\n%s", joined)
	}
	st := loadTeamsBeaconState(t)
	if len(st.Profiles) != 0 || len(st.Conversations) != 0 {
		t.Fatalf("wrong-chat commands mutated beacon state: %#v", st)
	}
}

func TestTeamsBeaconWorkCommandFromCoworkerIsRejectedWithoutMutatingState(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	msg := bridgeTestMessageWithText("beacon-coworker-switch", "beacon switch gpu")
	msg.From.User.ID = "user-2"
	msg.From.User.DisplayName = "Alex Kim"

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", msg, "beacon switch gpu"); err != nil {
		t.Fatalf("coworker beacon switch: %v", err)
	}

	joined := sentPlainJoined(*sent)
	if len(*sent) != 1 || (*sent)[0].Mentions != 1 || !strings.Contains(joined, "Alex Kim") || !strings.Contains(joined, "Only the helper owner can run helper commands") {
		t.Fatalf("coworker beacon rejection mismatch, sent=%#v plain=%q", *sent, joined)
	}
	st := loadTeamsBeaconState(t)
	if len(st.Conversations) != 0 {
		t.Fatalf("coworker beacon command mutated conversations: %#v", st.Conversations)
	}
}

func TestTeamsBeaconMutationIsIdempotentAfterSendFailure(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	graph, sent := newFlakyBeaconMessageGraph(t, 1)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	msg := bridgeTestMessageWithText("beacon-create-flaky", "beacon profile create gpu --provider local")

	if err := bridge.handleControlMessage(context.Background(), msg, "beacon profile create gpu --provider local"); err == nil {
		t.Fatal("first send should fail after mutating beacon state")
	}
	st := loadTeamsBeaconState(t)
	if _, ok := st.Profiles["gpu"]; !ok {
		t.Fatalf("profile should exist after failed Teams send: %#v", st.Profiles)
	}
	if err := bridge.handleControlMessage(context.Background(), msg, "beacon profile create gpu --provider local"); err != nil {
		t.Fatalf("retry same Teams message should use beacon idempotency: %v", err)
	}
	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "Created beacon profile \"gpu\"") {
		t.Fatalf("retry response missing create result:\n%s", joined)
	}
	st = loadTeamsBeaconState(t)
	if len(st.Profiles) != 1 {
		t.Fatalf("duplicate retry should not create extra profiles: %#v", st.Profiles)
	}
}

func TestTeamsBeaconWorkSwitchIsIdempotentAfterSendFailure(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	graph, sent := newFlakyBeaconMessageGraph(t, 1)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	msg := bridgeTestMessageWithText("beacon-switch-flaky", "beacon switch gpu")

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", msg, "beacon switch gpu"); err == nil {
		t.Fatal("first switch send should fail after mutating beacon state")
	}
	st := loadTeamsBeaconState(t)
	if conv := st.Conversations["s001"]; conv.Current.Target != beacon.TargetBeacon || conv.Current.Profile != "gpu" {
		t.Fatalf("switch should persist before failed Teams send: %#v", conv)
	}
	if err := bridge.handleSessionMessage(context.Background(), "chat-1", msg, "beacon switch gpu"); err != nil {
		t.Fatalf("retry same Teams switch should be idempotent: %v", err)
	}
	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "Switched this Work chat to beacon:gpu") {
		t.Fatalf("retry response missing switch result:\n%s", joined)
	}
	st = loadTeamsBeaconState(t)
	if len(st.Conversations) != 1 || len(st.Idempotency) != 1 {
		t.Fatalf("duplicate retry should not create extra state, conversations=%#v idempotency=%#v", st.Conversations, st.Idempotency)
	}
}

func TestTeamsBeaconIdempotencyIsScopedPerWorkChat(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	graph, _ := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByID("s001")
	if session == nil {
		t.Fatal("test bridge missing s001")
	}
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensure durable session: %v", err)
	}
	second := appendBridgeTestSession(t, bridge, store, "s002", "chat-2")

	firstMsg := bridgeTestMessageWithText("same-teams-message-id", "beacon switch gpu")
	secondMsg := bridgeTestMessageWithText("same-teams-message-id", "beacon switch gpu")
	if err := bridge.handleSessionMessage(context.Background(), session.ChatID, firstMsg, "beacon switch gpu"); err != nil {
		t.Fatalf("first chat switch: %v", err)
	}
	if err := bridge.handleSessionMessage(context.Background(), second.ChatID, secondMsg, "beacon switch gpu"); err != nil {
		t.Fatalf("second chat switch with same Teams message id: %v", err)
	}

	st := loadTeamsBeaconState(t)
	for _, sessionID := range []string{session.ID, second.ID} {
		conv := st.Conversations[sessionID]
		if conv.Current.Target != beacon.TargetBeacon || conv.Current.Profile != "gpu" {
			t.Fatalf("conversation %s target = %#v, want beacon gpu", sessionID, conv.Current)
		}
	}
	if len(st.Idempotency) != 2 {
		t.Fatalf("idempotency records = %#v, want separate records per Work chat", st.Idempotency)
	}
}

func TestTeamsBeaconMachineReleaseAndKillConfirmation(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	store, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	if err := store.Update(func(st *beacon.State) error {
		st.Machines["gpu-a"] = beacon.Machine{ID: "gpu-a", LeaseID: "lease-gpu", Profile: "gpu", State: "accepting", Jobs: []string{"job-1"}, Chats: []string{"s001"}}
		return nil
	}); err != nil {
		t.Fatalf("seed machine: %v", err)
	}
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, newBridgeTestStore(t), &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("machine-release", "beacon machine release gpu-a"), "beacon machine release gpu-a"); err != nil {
		t.Fatalf("release machine: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("machine-kill-missing", "beacon machine kill gpu-a"), "beacon machine kill gpu-a"); err != nil {
		t.Fatalf("kill missing confirm: %v", err)
	}
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("machine-kill-confirm", "beacon machine kill gpu-a --confirm KILL-lease-gpu"), "beacon machine kill gpu-a --confirm KILL-lease-gpu"); err != nil {
		t.Fatalf("kill confirmed: %v", err)
	}

	joined := sentPlainJoined(*sent)
	for _, want := range []string{"action: drain", "action: reject_confirmation", "kill confirmation: KILL-lease-gpu", "action: kill_quarantine"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("machine output missing %q:\n%s", want, joined)
		}
	}
	st := loadTeamsBeaconState(t)
	m := st.Machines["gpu-a"]
	if m.State != "kill_quarantine" {
		t.Fatalf("machine state = %q, want kill_quarantine", m.State)
	}
}

func TestTeamsBeaconMachineReleaseDeletesIdleMachine(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	store, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	if err := store.Update(func(st *beacon.State) error {
		st.Machines["idle-a"] = beacon.Machine{ID: "idle-a", LeaseID: "lease-idle", Profile: "gpu", State: "accepting"}
		return nil
	}); err != nil {
		t.Fatalf("seed idle machine: %v", err)
	}
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, newBridgeTestStore(t), &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("machine-release-idle", "beacon machine release idle-a"), "beacon machine release idle-a"); err != nil {
		t.Fatalf("release idle machine: %v", err)
	}

	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "action: release") {
		t.Fatalf("idle machine release response mismatch:\n%s", joined)
	}
	st := loadTeamsBeaconState(t)
	if _, ok := st.Machines["idle-a"]; ok {
		t.Fatalf("idle machine should be removed after release: %#v", st.Machines["idle-a"])
	}
}

func TestTeamsBeaconMachineKillRejectsExternalOwnedMachine(t *testing.T) {
	t.Setenv("CODEX_HELPER_BEACON_STORE", filepath.Join(t.TempDir(), "beacon.json"))
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	seedTeamsBeaconProfile(t, "gpu")
	store, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	if err := store.Update(func(st *beacon.State) error {
		st.Machines["external-a"] = beacon.Machine{ID: "external-a", LeaseID: "lease-external", Profile: "gpu", State: "accepting", ExternalOwned: true}
		return nil
	}); err != nil {
		t.Fatalf("seed external machine: %v", err)
	}
	graph, sent := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, newBridgeTestStore(t), &recordingExecutor{})

	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("machine-kill-external", "beacon machine kill external-a --confirm KILL-lease-external"), "beacon machine kill external-a --confirm KILL-lease-external"); err != nil {
		t.Fatalf("kill external machine: %v", err)
	}

	joined := sentPlainJoined(*sent)
	if !strings.Contains(joined, "action: reject_external") || strings.Contains(joined, "action: kill_quarantine") {
		t.Fatalf("external machine kill response mismatch:\n%s", joined)
	}
	st := loadTeamsBeaconState(t)
	m := st.Machines["external-a"]
	if m.State != "accepting" || !m.ExternalOwned {
		t.Fatalf("external machine should remain unchanged, got %#v", m)
	}
}

func seedTeamsBeaconProfile(t *testing.T, name string) {
	t.Helper()
	store, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	if err := store.Update(func(st *beacon.State) error {
		st.Profiles[name] = beacon.Profile{
			Name:              name,
			Provider:          beacon.ProviderSlurm,
			ProxyMode:         beacon.ProxyNone,
			IsolationDefault:  beacon.IsolationShared,
			Slurm:             beacon.SlurmProfile{Nodes: 1, GPUCount: 1, Partition: "interactive", Image: "image.sqsh", Duration: 4},
			Confirmed:         true,
			ProviderPreviewOK: true,
			DoctorOK:          true,
			CreatedAt:         time.Now(),
			UpdatedAt:         time.Now(),
		}
		return nil
	}); err != nil {
		t.Fatalf("seed beacon profile: %v", err)
	}
}

func newFlakyBeaconMessageGraph(t *testing.T, failures int) (*GraphClient, *[]bridgeSentMessage) {
	t.Helper()
	var sent []bridgeSentMessage
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		w := httptest.NewRecorder()
		if r.Method != http.MethodPost || !strings.HasPrefix(r.URL.Path, "/chats/") || !strings.HasSuffix(r.URL.Path, "/messages") {
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
		if failures > 0 {
			failures--
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = fmt.Fprint(w, `{"error":{"message":"temporary Teams send failure"}}`)
			return w.Result(), nil
		}
		var body struct {
			Body struct {
				Content string `json:"content"`
			} `json:"body"`
			Mentions []json.RawMessage `json:"mentions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("decode message: %v", err)
		}
		chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
		sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content, Mentions: len(body.Mentions)})
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, len(sent))
		return w.Result(), nil
	})}
	return &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     client,
		baseURL:    "https://graph.example.test",
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, &sent
}

func loadTeamsBeaconState(t *testing.T) beacon.State {
	t.Helper()
	store, err := beacon.NewStore("")
	if err != nil {
		t.Fatalf("beacon store: %v", err)
	}
	st, err := store.Load()
	if err != nil {
		t.Fatalf("load beacon state: %v", err)
	}
	return st
}

func newBeaconMeetingBridgeTestGraph(t *testing.T) (*GraphClient, *[]bridgeSentMessage) {
	t.Helper()
	var sent []bridgeSentMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/me/onlineMeetings":
			_, _ = fmt.Fprint(w, `{"subject":"work topic","joinWebUrl":"https://teams.example/join","chatInfo":{"threadId":"work-chat"}}`)
		case r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/chats/") && strings.HasSuffix(r.URL.Path, "/messages"):
			var body struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
				Mentions []json.RawMessage `json:"mentions"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatalf("decode message: %v", err)
			}
			chatID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/chats/"), "/messages")
			sent = append(sent, bridgeSentMessage{ChatID: chatID, Content: body.Body.Content, Mentions: len(body.Mentions)})
			_, _ = fmt.Fprintf(w, `{"id":"sent-%d","messageType":"message"}`, len(sent))
		default:
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(server.Close)
	return &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 0,
		sleep:      sleepContext,
		jitter:     func(d time.Duration) time.Duration { return d },
	}, &sent
}
