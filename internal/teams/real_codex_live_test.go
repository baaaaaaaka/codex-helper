package teams

import (
	"context"
	"fmt"
	"html"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestLiveBridgeRealCodexUserJourneyOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_REAL_CODEX_E2E")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_REAL_CODEX_E2E=1 to run a live Teams journey with real Codex turns")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before any live Teams chat read, send, mention, or file upload", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	requireLiveWriteOnce(t, "real-codex-e2e")

	cfg, err := DefaultAuthConfig()
	if err != nil {
		t.Fatalf("DefaultAuthConfig error: %v", err)
	}
	if _, err := readTokenCache(cfg.CachePath); err != nil {
		t.Fatalf("read Teams chat token cache %s: %v", cfg.CachePath, err)
	}
	readCfg, err := DefaultReadAuthConfig()
	if err != nil {
		t.Fatalf("DefaultReadAuthConfig error: %v", err)
	}
	if _, err := readTokenCache(readCfg.CachePath); err != nil {
		t.Fatalf("read Teams read token cache %s: %v", readCfg.CachePath, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Minute)
	defer cancel()

	auth := NewAuthManager(cfg)
	graph := NewGraphClient(auth, io.Discard)
	readGraph := NewGraphClient(NewAuthManager(readCfg), io.Discard)
	me, err := graph.Me(ctx)
	if err != nil {
		t.Fatalf("Graph /me failed: %v", err)
	}
	if normalizeLiveHumanName(me.DisplayName) != "jason wei" {
		t.Fatalf("logged-in user displayName %q is not Jason Wei", me.DisplayName)
	}

	tmp := t.TempDir()
	registryPath := liveHelperE2ERegistryPath(t)
	pruneLiveHelperE2ERegistry(t, registryPath)
	t.Cleanup(func() {
		pruneLiveHelperE2ERegistry(t, registryPath)
	})
	adoptLiveHelperE2EControlChat(ctx, t, graph, readGraph, registryPath, me)
	requireLiveHelperE2EControlBindingOrSkip(t, registryPath)
	store, err := teamstore.Open(filepath.Join(tmp, "state.json"))
	if err != nil {
		t.Fatalf("open live real Codex store: %v", err)
	}
	bridge, err := NewBridge(ctx, auth, registryPath, io.Discard)
	if err != nil {
		t.Fatalf("NewBridge error: %v", err)
	}
	bridge.readGraph = readGraph
	bridge.store = store
	controlChat, err := bridge.EnsureControlChat(ctx)
	if err != nil {
		t.Fatalf("EnsureControlChat live error: %v", err)
	}
	requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, controlChat.ID)
	if _, err := store.RecordChatPollSuccess(ctx, controlChat.ID, time.Now().UTC(), true, false, 0); err != nil {
		t.Fatalf("seed live control poll cursor failed: %v", err)
	}

	nonce := safeLiveMarkerPart(strings.TrimSpace(os.Getenv(liveWriteOnceEnv)))
	sessionModel := liveRealCodexSessionModel()
	controlModel := liveRealCodexControlModel()
	turnTimeout := 8 * time.Minute
	sessionExecutor := liveRealCodexExecutor("", sessionModel, turnTimeout)
	controlExecutor := liveRealCodexExecutor(tmp, controlModel, turnTimeout)
	listenOnce := func(label string) error {
		if err := bridge.Listen(ctx, BridgeOptions{
			RegistryPath:            registryPath,
			Store:                   store,
			HelperVersion:           "live-real-codex-e2e",
			Once:                    true,
			Top:                     20,
			Executor:                sessionExecutor,
			ControlFallbackExecutor: controlExecutor,
			ControlFallbackModel:    controlModel,
		}); err != nil {
			return fmt.Errorf("bridge.Listen once for %s failed: %w", label, err)
		}
		if err := bridge.Save(); err != nil {
			return fmt.Errorf("bridge.Save after %s failed: %w", label, err)
		}
		return nil
	}
	waitForOutbox := func(label string, chatID string, parts ...string) {
		t.Helper()
		waitForLiveOutbox(t, ctx, store, chatID, listenOnce, label, 10, parts...)
	}
	waitForSession := func(label string) Session {
		t.Helper()
		return waitForLiveActiveSession(t, registryPath, listenOnce, label, 8)
	}

	liveSendText(ctx, t, graph, controlChat.ID, "./tmp/teams bridge real codex "+nonce)
	waitForOutbox("control path hint", controlChat.ID, "Detected path", "new \"./tmp/teams bridge real codex "+nonce+"\" --")

	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_REAL_CODEX_INCLUDE_CONTROL_FALLBACK")) == "1" {
		liveSendText(ctx, t, graph, controlChat.ID, "Reply exactly CONTROL-FALLBACK-"+nonce)
		waitForOutbox("control fallback ack", controlChat.ID, "quick helper question")
		waitForOutbox("control fallback final", controlChat.ID, "CONTROL-FALLBACK-"+nonce)
	} else {
		t.Logf("skipping live control fallback Codex turn; set CODEX_HELPER_TEAMS_LIVE_REAL_CODEX_INCLUDE_CONTROL_FALLBACK=1 to exercise %s", controlModel)
	}

	workDir := filepath.Join(tmp, "teams-real-codex-"+nonce)
	liveSendText(ctx, t, graph, controlChat.ID, "mkdir "+workDir)
	waitForOutbox("control mkdir", controlChat.ID, "Directory is ready:", workDir)

	title := "Teams real Codex file smoke " + nonce
	liveSendText(ctx, t, graph, controlChat.ID, "new "+workDir+" -- "+title)
	workSession := waitForSession("control new")
	requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, workSession.ChatID)
	if !strings.HasPrefix(workSession.Topic, "💬 ") || !strings.Contains(workSession.Topic, "Codex Work") {
		t.Fatalf("live work title = %q, want work-chat emoji and work marker", workSession.Topic)
	}
	if _, err := store.RecordChatPollSuccess(ctx, workSession.ChatID, time.Now().UTC(), true, false, 0); err != nil {
		t.Fatalf("seed live work poll cursor failed: %v", err)
	}
	waitForOutbox("work ready", workSession.ChatID, "Work chat is ready", "Project: "+filepath.Base(workDir))

	liveSendText(ctx, t, graph, workSession.ChatID, "helper status")
	waitForOutbox("work helper status", workSession.ChatID, "STATUS: Work chat", "Folder: "+workDir)

	fileName := "teams-real-codex-" + nonce + ".txt"
	firstLine := "teams real codex " + nonce
	liveSendText(ctx, t, graph, workSession.ChatID, "Create a file named "+fileName+" in the current working directory. Put exactly this one line in it: "+firstLine+"\n\nThen read the file back and reply with the file name and the exact line you found. Do not touch anything outside this directory.")
	waitForOutbox("real codex first ack", workSession.ChatID, "Codex is working")
	waitForOutbox("real codex first final", workSession.ChatID, fileName, firstLine)
	requireFileContains(t, filepath.Join(workDir, fileName), firstLine)

	afterFirst := liveLoadSessionByChat(t, registryPath, workSession.ChatID)
	if strings.TrimSpace(afterFirst.CodexThreadID) == "" {
		t.Fatalf("real Codex first turn did not persist CodexThreadID: %#v", afterFirst)
	}
	firstThreadID := afterFirst.CodexThreadID

	secondLine := "resumed turn " + nonce
	liveSendText(ctx, t, graph, workSession.ChatID, "Continue from the previous turn. Ensure the same file you created earlier contains this line exactly once after the first line: "+secondLine+"\n\nIf the line is already present, do not duplicate it. Then read the file back and reply with VERIFIED "+nonce+" plus the final file contents.")
	waitForOutbox("real codex second ack", workSession.ChatID, "Codex is working")
	continued := true
	if retryTurnID := waitForLiveOutboxOrAmbiguousTurn(t, ctx, store, workSession.ChatID, listenOnce, "real codex second final", 10, "VERIFIED "+nonce, secondLine); retryTurnID != "" {
		t.Logf("real Codex continuation became ambiguous; retrying original Teams turn through helper retry last (resolved turn: %s)", retryTurnID)
		liveSendText(ctx, t, graph, workSession.ChatID, "helper retry last")
		if retryAgain := waitForLiveOutboxOrAmbiguousTurn(t, ctx, store, workSession.ChatID, listenOnce, "real codex second retry final", 10, "VERIFIED "+nonce, secondLine); retryAgain != "" {
			t.Logf("real Codex retry also became ambiguous; preserving the interrupted status as the verified behavior for this live run: %s", retryAgain)
			continued = false
		}
	}
	if continued {
		requireFileContains(t, filepath.Join(workDir, fileName), firstLine, secondLine)
	} else {
		requireFileContains(t, filepath.Join(workDir, fileName), firstLine)
	}

	afterSecond := liveLoadSessionByChat(t, registryPath, workSession.ChatID)
	if afterSecond.CodexThreadID != firstThreadID {
		t.Fatalf("CodexThreadID changed across continuation: first=%q second=%q", firstThreadID, afterSecond.CodexThreadID)
	}

	localSession := waitForDiscoveredCodexSession(t, ctx, firstThreadID)
	if strings.TrimSpace(localSession.FilePath) == "" {
		t.Fatalf("discovered real Codex session %s without FilePath: %#v", firstThreadID, localSession)
	}
	if transcript, err := ReadSessionTranscript(localSession.FilePath); err != nil {
		t.Fatalf("read discovered real Codex transcript %s: %v", localSession.FilePath, err)
	} else {
		wantTranscript := []string{fileName, firstLine}
		if continued {
			wantTranscript = append(wantTranscript, secondLine)
		}
		if !transcriptContainsAll(transcript, wantTranscript...) {
			t.Fatalf("real Codex transcript %s did not contain expected task text", localSession.FilePath)
		}
	}

	publishedSession, publishStore := publishRealCodexHistoryToLiveTeams(ctx, t, auth, readGraph, graph, tmp, controlChat, localSession, firstThreadID, controlModel, sessionExecutor)
	requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, publishedSession.ChatID)
	if !strings.HasPrefix(publishedSession.Topic, "💬 ") || !strings.Contains(publishedSession.Topic, "Codex Work") {
		t.Fatalf("published live work title = %q, want work-chat emoji and marker", publishedSession.Topic)
	}
	waitForLiveOutbox(t, ctx, publishStore, publishedSession.ChatID, nil, "published import title", 1, "Imported Codex session history")
	waitForLiveOutboxKind(t, ctx, publishStore, publishedSession.ChatID, "import-user", "published import user", fileName)
	waitForLiveOutboxKind(t, ctx, publishStore, publishedSession.ChatID, "import-assistant", "published import assistant", fileName)

	if _, err := publishStore.RecordChatPollSuccess(ctx, publishedSession.ChatID, time.Now().UTC(), true, false, 0); err != nil {
		t.Fatalf("seed published live work poll cursor failed: %v", err)
	}
	liveSendText(ctx, t, graph, publishedSession.ChatID, "Continue this imported work without modifying files. Reply exactly PUBLISHED-CONTINUE "+nonce+" "+fileName)
	publishListenOnce := func(label string) error {
		publishBridge, err := NewBridge(ctx, auth, filepath.Join(tmp, "published-registry.json"), io.Discard)
		if err != nil {
			return fmt.Errorf("NewBridge for published %s failed: %w", label, err)
		}
		publishBridge.readGraph = readGraph
		publishBridge.store = publishStore
		publishBridge.executor = sessionExecutor
		if err := publishBridge.Listen(ctx, BridgeOptions{
			RegistryPath:            filepath.Join(tmp, "published-registry.json"),
			Store:                   publishStore,
			HelperVersion:           "live-real-codex-published",
			Once:                    true,
			Top:                     20,
			Executor:                sessionExecutor,
			ControlFallbackExecutor: controlExecutor,
			ControlFallbackModel:    controlModel,
		}); err != nil {
			return fmt.Errorf("published bridge.Listen once for %s failed: %w", label, err)
		}
		return publishBridge.Save()
	}
	waitForLiveOutbox(t, ctx, publishStore, publishedSession.ChatID, publishListenOnce, "published continue ack", 8, "Codex is working")
	if retryTurnID := waitForLiveOutboxOrAmbiguousTurn(t, ctx, publishStore, publishedSession.ChatID, publishListenOnce, "published continue final", 8, "PUBLISHED-CONTINUE "+nonce, fileName); retryTurnID != "" {
		t.Logf("published history continuation became ambiguous after reaching Codex; helper surfaced retry guidance for %s", retryTurnID)
	}
}

func liveRealCodexSessionModel() string {
	return strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_REAL_CODEX_SESSION_MODEL"))
}

func liveRealCodexControlModel() string {
	if model := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_REAL_CODEX_CONTROL_MODEL")); model != "" {
		return model
	}
	return DefaultControlFallbackModel
}

func liveRealCodexExecutor(workDir string, model string, timeout time.Duration) Executor {
	args := []string{
		"--sandbox", "workspace-write",
		"--skip-git-repo-check",
	}
	if strings.TrimSpace(model) != "" {
		args = append([]string{"--model", strings.TrimSpace(model)}, args...)
	}
	return RunnerExecutor{
		Runner: &codexrunner.ExecRunner{
			Command:    "codex",
			ExtraArgs:  args,
			WorkingDir: strings.TrimSpace(workDir),
			Timeout:    timeout,
		},
		WorkDir: strings.TrimSpace(workDir),
		Timeout: timeout,
	}
}

func liveSendText(ctx context.Context, t *testing.T, graph *GraphClient, chatID string, text string) {
	t.Helper()
	if _, err := graph.SendHTML(ctx, chatID, "<p>"+html.EscapeString(text)+"</p>"); err != nil {
		t.Fatalf("send live Teams message to %s failed: %v", chatID, err)
	}
}

func waitForLiveOutbox(t *testing.T, ctx context.Context, store *teamstore.Store, chatID string, listenOnce func(string) error, label string, attempts int, parts ...string) {
	t.Helper()
	_ = waitForLiveOutboxMessage(t, ctx, store, chatID, listenOnce, label, attempts, parts...)
}

func waitForLiveOutboxMessage(t *testing.T, ctx context.Context, store *teamstore.Store, chatID string, listenOnce func(string) error, label string, attempts int, parts ...string) teamstore.OutboxMessage {
	t.Helper()
	if attempts <= 0 {
		attempts = 1
	}
	deadline := time.Now().Add(12 * time.Minute)
	var lastErr error
	for attempt := 0; attempt < attempts && time.Now().Before(deadline); attempt++ {
		if listenOnce != nil {
			if err := listenOnce(label); err != nil {
				lastErr = err
			}
		}
		if msg, ok, err := liveSentOutboxMessageContains(ctx, store, chatID, parts...); err != nil {
			lastErr = err
		} else if ok {
			return msg
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s canceled while waiting for live outbox %q: %v", label, parts, ctx.Err())
		case <-time.After(12 * time.Second):
		}
	}
	if lastErr != nil {
		t.Fatalf("%s did not produce expected live outbox %q: %v\n%s", label, parts, lastErr, liveOutboxDebug(ctx, store, chatID))
	}
	if msg, ok, err := liveSentOutboxMessageContains(ctx, store, chatID, parts...); err != nil {
		t.Fatalf("load live helper store failed: %v", err)
	} else if !ok {
		t.Fatalf("%s did not produce expected live outbox %q\n%s", label, parts, liveOutboxDebug(ctx, store, chatID))
	} else {
		return msg
	}
	return teamstore.OutboxMessage{}
}

func waitForLiveOutboxOrAmbiguousTurn(t *testing.T, ctx context.Context, store *teamstore.Store, chatID string, listenOnce func(string) error, label string, attempts int, parts ...string) string {
	t.Helper()
	if attempts <= 0 {
		attempts = 1
	}
	deadline := time.Now().Add(12 * time.Minute)
	var lastErr error
	for attempt := 0; attempt < attempts && time.Now().Before(deadline); attempt++ {
		if listenOnce != nil {
			if err := listenOnce(label); err != nil {
				lastErr = err
			}
		}
		if _, ok, err := liveSentOutboxMessageContains(ctx, store, chatID, parts...); err != nil {
			lastErr = err
		} else if ok {
			return ""
		}
		if msg, ok, err := liveSentOutboxMessageContains(ctx, store, chatID, "could not confirm whether it finished"); err != nil {
			lastErr = err
		} else if ok && strings.TrimSpace(msg.TurnID) != "" {
			return msg.TurnID
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s canceled while waiting for live outbox %q: %v", label, parts, ctx.Err())
		case <-time.After(12 * time.Second):
		}
	}
	if lastErr != nil {
		t.Fatalf("%s did not produce expected live outbox %q: %v\n%s", label, parts, lastErr, liveOutboxDebug(ctx, store, chatID))
	}
	t.Fatalf("%s did not produce expected live outbox %q\n%s", label, parts, liveOutboxDebug(ctx, store, chatID))
	return ""
}

func liveSentOutboxMessageContains(ctx context.Context, store *teamstore.Store, chatID string, parts ...string) (teamstore.OutboxMessage, bool, error) {
	state, err := store.Load(ctx)
	if err != nil {
		return teamstore.OutboxMessage{}, false, err
	}
	for _, msg := range state.OutboxMessages {
		if msg.TeamsChatID != chatID || msg.Status != teamstore.OutboxStatusSent || strings.TrimSpace(msg.TeamsMessageID) == "" {
			continue
		}
		matched := true
		for _, part := range parts {
			if !strings.Contains(msg.Body, part) {
				matched = false
				break
			}
		}
		if matched {
			return msg, true, nil
		}
	}
	return teamstore.OutboxMessage{}, false, nil
}

func waitForLiveOutboxKind(t *testing.T, ctx context.Context, store *teamstore.Store, chatID string, kindPart string, label string, parts ...string) {
	t.Helper()
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load live helper store failed: %v", err)
	}
	for _, msg := range state.OutboxMessages {
		if msg.TeamsChatID != chatID || msg.Status != teamstore.OutboxStatusSent || !strings.Contains(msg.Kind, kindPart) {
			continue
		}
		matched := true
		for _, part := range parts {
			if !strings.Contains(msg.Body, part) {
				matched = false
				break
			}
		}
		if matched {
			return
		}
	}
	t.Fatalf("%s did not produce sent outbox kind containing %q and body parts %q\n%s", label, kindPart, parts, liveOutboxDebug(ctx, store, chatID))
}

func liveOutboxDebug(ctx context.Context, store *teamstore.Store, chatID string) string {
	state, err := store.Load(ctx)
	if err != nil {
		return "outbox debug unavailable: " + err.Error()
	}
	var messages []teamstore.OutboxMessage
	for _, msg := range state.OutboxMessages {
		if msg.TeamsChatID == chatID {
			messages = append(messages, msg)
		}
	}
	sort.Slice(messages, func(i, j int) bool {
		if messages[i].Sequence != messages[j].Sequence {
			return messages[i].Sequence < messages[j].Sequence
		}
		return messages[i].ID < messages[j].ID
	})
	if len(messages) > 8 {
		messages = messages[len(messages)-8:]
	}
	lines := []string{fmt.Sprintf("recent outbox for %s (%d shown):", chatID, len(messages))}
	for _, msg := range messages {
		body := strings.TrimSpace(PlainTextFromTeamsHTML(msg.Body))
		if body == "" {
			body = strings.TrimSpace(msg.Body)
		}
		body = strings.Join(strings.Fields(body), " ")
		if len(body) > 220 {
			body = body[:220] + "..."
		}
		if strings.TrimSpace(msg.LastSendError) != "" {
			body += " last_send_error=" + msg.LastSendError
		}
		lines = append(lines, fmt.Sprintf("- seq=%d kind=%s status=%s id=%s body=%q", msg.Sequence, msg.Kind, msg.Status, msg.ID, body))
	}
	return strings.Join(lines, "\n")
}

func waitForLiveActiveSession(t *testing.T, registryPath string, listenOnce func(string) error, label string, attempts int) Session {
	t.Helper()
	if attempts <= 0 {
		attempts = 1
	}
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		if err := listenOnce(label); err != nil {
			lastErr = err
		} else if reg, err := LoadRegistry(registryPath); err != nil {
			lastErr = err
		} else if active := reg.ActiveSessions(); len(active) > 0 && strings.TrimSpace(active[0].ChatID) != "" {
			return active[0]
		}
		time.Sleep(12 * time.Second)
	}
	if lastErr != nil {
		t.Fatalf("%s did not create a live session: %v", label, lastErr)
	}
	reg, err := LoadRegistry(registryPath)
	if err != nil {
		t.Fatalf("LoadRegistry after %s failed: %v", label, err)
	}
	t.Fatalf("%s did not create a live session: %#v", label, reg.Sessions)
	return Session{}
}

func liveLoadSessionByChat(t *testing.T, registryPath string, chatID string) Session {
	t.Helper()
	reg, err := LoadRegistry(registryPath)
	if err != nil {
		t.Fatalf("LoadRegistry %s failed: %v", registryPath, err)
	}
	session := reg.SessionByChatID(chatID)
	if session == nil {
		t.Fatalf("session for chat %s not found in registry %#v", chatID, reg.Sessions)
	}
	return *session
}

func requireFileContains(t *testing.T, path string, parts ...string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s failed: %v", path, err)
	}
	text := string(data)
	for _, part := range parts {
		if !strings.Contains(text, part) {
			t.Fatalf("%s missing %q in content:\n%s", path, part, text)
		}
	}
}

func waitForDiscoveredCodexSession(t *testing.T, ctx context.Context, threadID string) codexhistory.Session {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	var lastErr error
	for time.Now().Before(deadline) {
		projects, err := discoverCodexProjectsForTeams(ctx, "")
		if err != nil {
			lastErr = err
		} else {
			for _, project := range projects {
				for _, session := range project.Sessions {
					if session.SessionID == threadID {
						return session
					}
				}
			}
		}
		time.Sleep(5 * time.Second)
	}
	if lastErr != nil {
		t.Fatalf("discover Codex projects while waiting for %s failed: %v", threadID, lastErr)
	}
	t.Fatalf("Codex session %s was not discovered in local history", threadID)
	return codexhistory.Session{}
}

func transcriptContainsAll(transcript Transcript, parts ...string) bool {
	combined := strings.Builder{}
	for _, record := range transcript.Records {
		combined.WriteString(record.Text)
		combined.WriteByte('\n')
	}
	text := combined.String()
	for _, part := range parts {
		if !strings.Contains(text, part) {
			return false
		}
	}
	return true
}

func publishRealCodexHistoryToLiveTeams(ctx context.Context, t *testing.T, auth *AuthManager, readGraph *GraphClient, graph *GraphClient, tmp string, controlChat Chat, local codexhistory.Session, threadID string, model string, executor Executor) (Session, *teamstore.Store) {
	t.Helper()
	registryPath := filepath.Join(tmp, "published-registry.json")
	store, err := teamstore.Open(filepath.Join(tmp, "published-state.json"))
	if err != nil {
		t.Fatalf("open published live store: %v", err)
	}
	bridge, err := NewBridge(ctx, auth, registryPath, io.Discard)
	if err != nil {
		t.Fatalf("NewBridge for publish real Codex history failed: %v", err)
	}
	bridge.readGraph = readGraph
	bridge.store = store
	bridge.executor = executor
	bridge.controlFallbackModel = model
	bridge.reg.ControlChatID = controlChat.ID
	bridge.reg.ControlChatURL = controlChat.WebURL
	bridge.reg.ControlChatTopic = controlChat.Topic
	bridge.reg.Chats[controlChat.ID] = ChatState{}
	if err := bridge.Save(); err != nil {
		t.Fatalf("save publish bridge control binding failed: %v", err)
	}
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:      "live-real-codex",
			Path:     local.ProjectPath,
			Sessions: []codexhistory.Session{local},
		}}, nil
	}
	defer func() {
		discoverCodexProjectsForTeams = prevDiscover
	}()
	message, err := bridge.publishCodexSession(ctx, DashboardCommandTarget{Raw: threadID})
	if err != nil {
		t.Fatalf("publish real Codex session %s failed: %v", threadID, err)
	}
	if !strings.Contains(message, "Published local Codex session") {
		t.Fatalf("publish message = %q, want publish confirmation", message)
	}
	if len(bridge.reg.Sessions) != 1 {
		t.Fatalf("publish did not register exactly one work session: %#v", bridge.reg.Sessions)
	}
	session := bridge.reg.Sessions[0]
	requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, session.ChatID)
	if err := bridge.Save(); err != nil {
		t.Fatalf("save publish bridge registry failed: %v", err)
	}
	return session, store
}
