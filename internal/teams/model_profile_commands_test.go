package teams

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

type backupWarningModelProfileManager struct{ *fakeModelProfileManager }

type recordingSetupModelProfileManager struct {
	*fakeModelProfileManager
	requests []ModelProfileSetupRequest
}

func (m *recordingSetupModelProfileManager) SetupModelProfile(ctx context.Context, req ModelProfileSetupRequest) (ModelProfileSetupResult, error) {
	m.requests = append(m.requests, req)
	return m.fakeModelProfileManager.SetupModelProfile(ctx, req)
}

func (m *backupWarningModelProfileManager) ModelProfileRuntimeWarning(context.Context, modelprofile.Snapshot) (string, bool, error) {
	return "⚠️ backup JSON remains active", true, nil
}

func TestModelNaturalLanguageListRequest(t *testing.T) {
	for _, value := range []string{
		"里面应该列出来所有可用的 gpt 模型，另外列出来 default 具体是什么模型",
		"有哪些可用模型",
		"what models are available if I am logged in",
		"please list models and explain default",
	} {
		if !modelNaturalLanguageListRequest(value) {
			t.Errorf("modelNaturalLanguageListRequest(%q) = false, want true", value)
		}
	}
	for _, value := range []string{
		"list",
		"status",
		"switch nemotron",
		"请帮我切换到 nemotron",
		"delete old-profile",
	} {
		if modelNaturalLanguageListRequest(value) {
			t.Errorf("modelNaturalLanguageListRequest(%q) = true, want false", value)
		}
	}
}

func TestPromptAckAlwaysIncludesActiveBackupJSONWarning(t *testing.T) {
	bridge := &Bridge{modelProfileManager: &backupWarningModelProfileManager{fakeModelProfileManager: &fakeModelProfileManager{}}}
	for turn := 1; turn <= 2; turn++ {
		got := bridge.appendModelProfileRuntimeWarning(context.Background(), "⏳ accepted", modelprofile.Snapshot{Name: "work-glm", Provider: "glm"})
		if !strings.Contains(got, "backup JSON remains active") {
			t.Fatalf("turn %d omitted persistent warning: %q", turn, got)
		}
	}
}

func TestModelNaturalLanguageListRequestRoutesInControlAndWorkChats(t *testing.T) {
	bridge := &Bridge{modelProfileManager: &fakeModelProfileManager{}}
	request := "里面应该列出来所有可用的 gpt 模型，另外列出来 default 具体是什么模型"
	control, err := bridge.handleModelControlCommand(context.Background(), ChatMessage{}, request)
	if err != nil || !strings.Contains(control, "Current chat model (Control)") || !strings.HasSuffix(control, "profiles") {
		t.Fatalf("control result = %q, err=%v", control, err)
	}
	work, err := bridge.handleModelWorkCommand(context.Background(), &Session{ID: "s1"}, request)
	if err != nil || !strings.Contains(work, "Current chat model (Work)") || !strings.HasSuffix(work, "profiles") {
		t.Fatalf("work result = %q, err=%v", work, err)
	}
}

func TestModelControlCommandsOperateOnControlSessionNotGlobalDefault(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	executor := testEffortCatalogExecutor()
	bridge := newBridgeTestBridge(nil, store, executor)
	bridge.controlFallbackExecutor = executor
	bridge.modelProfileResolver = func(_ context.Context, ref string) (modelprofile.Snapshot, error) {
		switch strings.TrimSpace(ref) {
		case "":
			return modelprofile.Snapshot{Name: "old", Provider: modelprofile.DefaultProvider, Model: "gpt-old", Revision: 1}, nil
		case "official:gpt-next":
			return modelprofile.Snapshot{Name: "gpt-next", Provider: modelprofile.DefaultProvider, Model: "gpt-next", Revision: 1}, nil
		default:
			return modelprofile.Snapshot{}, fmt.Errorf("unknown model %q", ref)
		}
	}

	status, err := bridge.handleModelControlCommand(ctx, ChatMessage{}, "status")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status, "Chat: Control") || !strings.Contains(status, "gpt-old") {
		t.Fatalf("control model status = %q", status)
	}
	message, err := bridge.handleModelControlCommand(ctx, ChatMessage{}, "use official:gpt-next")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(message, "this Control chat") || !strings.Contains(message, "start a Codex thread") {
		t.Fatalf("control model switch = %q", message)
	}
	session, err := bridge.ensureControlFallbackSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if session.ModelProfile.Model != "gpt-next" || session.ModelGeneration != 1 {
		t.Fatalf("control session after switch = %#v", session)
	}
	if session.ModelSelectionSource != modelSelectionSourceChatOverride {
		t.Fatalf("control selection source after switch = %q", session.ModelSelectionSource)
	}
	state, err := bridge.store.Load(ctx)
	durable := state.Sessions[session.ID]
	if err != nil || durable.ModelSelectionSource != modelSelectionSourceChatOverride {
		t.Fatalf("durable selection source after switch = %q err=%v", durable.ModelSelectionSource, err)
	}
	guidance, err := bridge.handleModelControlCommand(ctx, ChatMessage{}, "default official:gpt-old")
	if err != nil || !strings.Contains(guidance, "default model set") {
		t.Fatalf("legacy default guidance = %q err=%v", guidance, err)
	}
	session, err = bridge.ensureControlFallbackSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if session.ModelProfile.Model != "gpt-next" {
		t.Fatalf("legacy default command changed current control model: %#v", session.ModelProfile)
	}
	reset, err := bridge.handleModelControlCommand(ctx, ChatMessage{}, "reset")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(reset, "this Control chat") {
		t.Fatalf("control reset message = %q", reset)
	}
	session, err = bridge.ensureControlFallbackSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if session.ModelProfile.Model != "gpt-old" || session.ModelGeneration != 2 {
		t.Fatalf("control session after reset = %#v", session)
	}
	if session.ModelSelectionSource != modelSelectionSourceGlobalDefault {
		t.Fatalf("control selection source after reset = %q", session.ModelSelectionSource)
	}
}

func TestModelListPutsCurrentChatBeforeGlobalDefaultAndVerifiedCatalog(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	manager := &fakeModelProfileManager{}
	bridge := newBridgeTestBridge(nil, store, testEffortCatalogExecutor())
	bridge.modelProfileManager = manager
	bridge.modelProfileResolver = func(context.Context, string) (modelprofile.Snapshot, error) {
		return modelprofile.Snapshot{Name: "sol", Provider: modelprofile.DefaultProvider, Model: "gpt-sol", Revision: 1}, nil
	}
	session := bridge.reg.SessionByID("s001")
	session.ModelProfile = modelprofile.Snapshot{Name: "luna", Provider: modelprofile.DefaultProvider, Model: "gpt-luna", Revision: 1}
	session.ModelSelectionSource = modelSelectionSourceChatOverride
	if err := bridge.ensureDurableSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	out, err := bridge.modelManagerList(ctx, session, "Work")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "Effective: Codex Official (`gpt-luna`)") || !strings.Contains(out, "Selection: chat override") {
		t.Fatalf("current chat summary = %q", out)
	}
	if !strings.Contains(out, "Global default for new chats: Codex Official (`gpt-sol`)") {
		t.Fatalf("global default summary = %q", out)
	}
	if strings.Contains(out, "current default:") || strings.Contains(out, "Model profile: default") {
		t.Fatalf("ambiguous model labels remain: %q", out)
	}
}

func TestModelStatusShowsCapturedQueuedModelSeparatelyFromNextChatModel(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, testEffortCatalogExecutor())
	session := bridge.reg.SessionByID("s001")
	session.ModelProfile = modelprofile.Snapshot{Name: "luna", Provider: modelprofile.DefaultProvider, Model: "gpt-luna", Revision: 1}
	session.ModelSelectionSource = modelSelectionSourceChatOverride
	if err := bridge.ensureDurableSession(ctx, session); err != nil {
		t.Fatal(err)
	}
	_, _, err := store.QueueTurn(ctx, teamstore.Turn{
		ID:              "turn-queued-model",
		SessionID:       session.ID,
		Status:          teamstore.TurnStatusQueued,
		ModelProfile:    modelprofile.Snapshot{Name: "sol", Provider: modelprofile.DefaultProvider, Model: "gpt-sol", Revision: 1},
		ReasoningEffort: "low",
	})
	if err != nil {
		t.Fatal(err)
	}
	status, err := bridge.formatChatModelStatus(ctx, session, "Work")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status, "Effective: Codex Official (`gpt-luna`)") || !strings.Contains(status, "Queued turn: Codex Official (`gpt-sol`)") {
		t.Fatalf("status did not distinguish current and queued models:\n%s", status)
	}
}

func TestModelSetupDoesNotSelectCurrentChatOrGlobalDefault(t *testing.T) {
	manager := &recordingSetupModelProfileManager{fakeModelProfileManager: &fakeModelProfileManager{}}
	bridge := &Bridge{modelProfileManager: manager}
	message, err := bridge.handleModelControlCommand(context.Background(), ChatMessage{}, "setup default")
	if err != nil {
		t.Fatal(err)
	}
	if len(manager.requests) != 1 || manager.requests[0].SetDefault {
		t.Fatalf("setup requests = %#v", manager.requests)
	}
	if !strings.Contains(message, "Current chats and global defaults are unchanged") {
		t.Fatalf("setup message = %q", message)
	}
}
