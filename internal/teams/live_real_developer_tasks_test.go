package teams

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestLiveBridgeRealDeveloperTasksOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_REAL_DEV_TASKS")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_REAL_DEV_TASKS=1 to run live Teams developer-task UX tests with real Codex")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before any live Teams chat read or send", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	requireLiveWriteOnce(t, "real-dev-tasks")

	ctx, cancel := context.WithTimeout(context.Background(), 35*time.Minute)
	defer cancel()

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

	nonce := safeLiveMarkerPart(strings.TrimSpace(os.Getenv(liveWriteOnceEnv)))
	tmp := t.TempDir()
	projectDir := filepath.Join(tmp, "calc-"+nonce)
	writeRealDeveloperTaskProject(t, projectDir)

	registryPath := liveHelperE2ERegistryPath(t)
	pruneLiveHelperE2ERegistry(t, registryPath)
	t.Cleanup(func() {
		pruneLiveHelperE2ERegistry(t, registryPath)
	})
	adoptLiveHelperE2EControlChat(ctx, t, graph, readGraph, registryPath, me)
	requireLiveHelperE2EControlBindingOrSkip(t, registryPath)

	store, err := teamstore.Open(filepath.Join(tmp, "state.json"))
	if err != nil {
		t.Fatalf("open real developer task store: %v", err)
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

	roundStarted := time.Now().Add(-5 * time.Second)
	sessionModel := liveRealDeveloperTaskModel()
	t.Logf("using live real developer task test model %q", firstNonEmptyString(sessionModel, "codex default"))
	sessionExecutor := liveRealCodexExecutor("", sessionModel, 10*time.Minute)
	controlExecutor := liveRealCodexExecutor(tmp, liveRealCodexControlModel(), 5*time.Minute)
	listenOnce := func(label string) error {
		if err := bridge.Listen(ctx, BridgeOptions{
			RegistryPath:            registryPath,
			Store:                   store,
			HelperVersion:           "live-real-dev-tasks",
			Once:                    true,
			Top:                     20,
			Executor:                sessionExecutor,
			ControlFallbackExecutor: controlExecutor,
			ControlFallbackModel:    liveRealCodexControlModel(),
		}); err != nil {
			return fmt.Errorf("bridge.Listen once for %s failed: %w", label, err)
		}
		return bridge.Save()
	}

	title := "Fix Go calc tests " + nonce
	liveSendText(ctx, t, graph, controlChat.ID, "new "+projectDir+" -- "+title)
	workSession := waitForLiveActiveSession(t, registryPath, listenOnce, "real dev control new", 8)
	requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, workSession.ChatID)
	if _, err := store.RecordChatPollSuccess(ctx, workSession.ChatID, time.Now().UTC(), true, false, 0); err != nil {
		t.Fatalf("seed live work poll cursor failed: %v", err)
	}
	waitForLiveOutbox(t, ctx, store, workSession.ChatID, listenOnce, "real dev work ready", 8, "Work chat is ready", "Project: "+filepath.Base(projectDir))

	firstMarker := "REAL-DEV-FIXED-" + nonce
	firstTask := strings.Join([]string{
		"Real developer task 1.",
		"Run `go test ./...` in this Go project.",
		"The test fails because the implementation is wrong.",
		"Fix `calc.go` only; do not edit `calc_test.go`.",
		"After tests pass, reply with exactly `" + firstMarker + "` and a one-sentence summary.",
	}, "\n")
	liveSendText(ctx, t, graph, workSession.ChatID, firstTask)
	waitForLiveOutbox(t, ctx, store, workSession.ChatID, listenOnce, "real dev first ack", 8, "Codex is working")
	waitForRealDeveloperTaskOutcome(t, realDeveloperTaskOutcomeInput{
		Ctx:           ctx,
		Graph:         graph,
		Store:         store,
		ChatID:        workSession.ChatID,
		ListenOnce:    listenOnce,
		Label:         "real dev first",
		Marker:        firstMarker,
		FallbackModel: liveRealDeveloperTaskFallbackModel(),
		SetModel: func(model string) {
			sessionExecutor = liveRealCodexExecutor("", model, 10*time.Minute)
		},
		Verify: func() error {
			if err := verifyGoTestInDir(projectDir); err != nil {
				return err
			}
			return verifyFileContains(filepath.Join(projectDir, "calc.go"), "return a + b")
		},
	})
	runGoTestInDir(t, projectDir)
	requireFileContains(t, filepath.Join(projectDir, "calc.go"), "return a + b")

	secondMarker := "REAL-DEV-NEGATIVE-" + nonce
	secondTask := strings.Join([]string{
		"Real developer task 2, continuing the same Codex session.",
		"Add a table-driven test named `TestAddNegativeNumbers` to `calc_test.go` that covers negative input.",
		"Run `go test ./...` and keep the previous behavior passing.",
		"After tests pass, reply with exactly `" + secondMarker + "` and a one-sentence summary.",
	}, "\n")
	liveSendText(ctx, t, graph, workSession.ChatID, secondTask)
	waitForLiveOutbox(t, ctx, store, workSession.ChatID, listenOnce, "real dev second ack", 8, "Codex is working")
	waitForRealDeveloperTaskOutcome(t, realDeveloperTaskOutcomeInput{
		Ctx:           ctx,
		Graph:         graph,
		Store:         store,
		ChatID:        workSession.ChatID,
		ListenOnce:    listenOnce,
		Label:         "real dev second",
		Marker:        secondMarker,
		FallbackModel: liveRealDeveloperTaskFallbackModel(),
		SetModel: func(model string) {
			sessionExecutor = liveRealCodexExecutor("", model, 10*time.Minute)
		},
		Verify: func() error {
			if err := verifyGoTestInDir(projectDir); err != nil {
				return err
			}
			return verifyFileContains(filepath.Join(projectDir, "calc_test.go"), "TestAddNegativeNumbers")
		},
	})
	runGoTestInDir(t, projectDir)
	requireFileContains(t, filepath.Join(projectDir, "calc_test.go"), "TestAddNegativeNumbers")

	dumpLiveDeveloperTaskTranscript(ctx, t, readGraph, workSession.ChatID, roundStarted, nonce)
}

func liveRealDeveloperTaskModel() string {
	if model := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_REAL_DEV_TASKS_MODEL")); model != "" {
		return model
	}
	if model := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_REAL_CODEX_SESSION_MODEL")); model != "" {
		return model
	}
	return "gpt-5.4-mini"
}

func liveRealDeveloperTaskFallbackModel() string {
	return strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_REAL_DEV_TASKS_FALLBACK_MODEL"))
}

type realDeveloperTaskOutcomeInput struct {
	Ctx           context.Context
	Graph         *GraphClient
	Store         *teamstore.Store
	ChatID        string
	ListenOnce    func(string) error
	Label         string
	Marker        string
	FallbackModel string
	SetModel      func(string)
	Verify        func() error
}

func waitForRealDeveloperTaskOutcome(t *testing.T, input realDeveloperTaskOutcomeInput) {
	t.Helper()
	retryTurnID := waitForLiveOutboxOrAmbiguousTurn(t, input.Ctx, input.Store, input.ChatID, input.ListenOnce, input.Label+" final", 12, input.Marker)
	if retryTurnID == "" {
		if input.Verify != nil {
			if err := input.Verify(); err != nil {
				t.Fatalf("%s final was sent but workspace verification failed: %v", input.Label, err)
			}
		}
		return
	}
	if input.Verify != nil {
		if err := input.Verify(); err == nil {
			t.Logf("%s became ambiguous after Codex accepted the turn, but workspace state already matches expectations; interrupted guidance was exercised for %s", input.Label, retryTurnID)
			return
		} else {
			t.Logf("%s became ambiguous and workspace is not complete yet: %v", input.Label, err)
		}
	}
	if input.SetModel != nil {
		t.Logf("%s retrying with fallback model %q", input.Label, firstNonEmptyString(input.FallbackModel, "codex default"))
		input.SetModel(input.FallbackModel)
	}
	afterSeq := liveOutboxMaxSequence(t, input.Ctx, input.Store, input.ChatID)
	liveSendText(input.Ctx, t, input.Graph, input.ChatID, "helper retry last")
	retryAgain := waitForLiveOutboxOrAmbiguousTurnAfter(t, input.Ctx, input.Store, input.ChatID, input.ListenOnce, input.Label+" retry final", 12, afterSeq, input.Marker)
	if retryAgain != "" {
		t.Logf("%s retry also became ambiguous after reaching Codex: %s", input.Label, retryAgain)
	}
	if input.Verify != nil {
		if err := input.Verify(); err != nil {
			t.Fatalf("%s did not reach expected workspace state after retry: %v", input.Label, err)
		}
	}
}

func liveOutboxMaxSequence(t *testing.T, ctx context.Context, store *teamstore.Store, chatID string) int64 {
	t.Helper()
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load live helper store failed: %v", err)
	}
	var maxSeq int64
	for _, msg := range state.OutboxMessages {
		if msg.TeamsChatID == chatID && msg.Sequence > maxSeq {
			maxSeq = msg.Sequence
		}
	}
	return maxSeq
}

func waitForLiveOutboxOrAmbiguousTurnAfter(t *testing.T, ctx context.Context, store *teamstore.Store, chatID string, listenOnce func(string) error, label string, attempts int, afterSeq int64, parts ...string) string {
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
		if _, ok, err := liveSentOutboxMessageContainsAfter(ctx, store, chatID, afterSeq, parts...); err != nil {
			lastErr = err
		} else if ok {
			return ""
		}
		if msg, ok, err := liveSentOutboxMessageContainsAfter(ctx, store, chatID, afterSeq, "could not confirm whether it finished"); err != nil {
			lastErr = err
		} else if ok && strings.TrimSpace(msg.TurnID) != "" {
			return msg.TurnID
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s canceled while waiting for live outbox after seq %d %q: %v", label, afterSeq, parts, ctx.Err())
		case <-time.After(12 * time.Second):
		}
	}
	if lastErr != nil {
		t.Fatalf("%s did not produce expected live outbox after seq %d %q: %v\n%s", label, afterSeq, parts, lastErr, liveOutboxDebug(ctx, store, chatID))
	}
	t.Fatalf("%s did not produce expected live outbox after seq %d %q\n%s", label, afterSeq, parts, liveOutboxDebug(ctx, store, chatID))
	return ""
}

func liveSentOutboxMessageContainsAfter(ctx context.Context, store *teamstore.Store, chatID string, afterSeq int64, parts ...string) (teamstore.OutboxMessage, bool, error) {
	state, err := store.Load(ctx)
	if err != nil {
		return teamstore.OutboxMessage{}, false, err
	}
	for _, msg := range state.OutboxMessages {
		if msg.Sequence <= afterSeq || msg.TeamsChatID != chatID || msg.Status != teamstore.OutboxStatusSent || strings.TrimSpace(msg.TeamsMessageID) == "" {
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

func writeRealDeveloperTaskProject(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("mkdir project: %v", err)
	}
	files := map[string]string{
		"go.mod": "module example.com/calc\n\ngo 1.22\n",
		"calc.go": strings.Join([]string{
			"package calc",
			"",
			"func Add(a, b int) int {",
			"\treturn a - b",
			"}",
			"",
		}, "\n"),
		"calc_test.go": strings.Join([]string{
			"package calc",
			"",
			"import \"testing\"",
			"",
			"func TestAdd(t *testing.T) {",
			"\tif got := Add(2, 3); got != 5 {",
			"\t\tt.Fatalf(\"Add(2, 3) = %d, want 5\", got)",
			"\t}",
			"}",
			"",
		}, "\n"),
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
}

func runGoTestInDir(t *testing.T, dir string) {
	t.Helper()
	if err := verifyGoTestInDir(dir); err != nil {
		t.Fatal(err)
	}
}

func verifyGoTestInDir(dir string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, "go", "test", "./...")
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("go test ./... failed in %s: %w\n%s", dir, err, string(out))
	}
	return nil
}

func verifyFileContains(path string, parts ...string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read %s failed: %w", path, err)
	}
	text := string(data)
	for _, part := range parts {
		if !strings.Contains(text, part) {
			return fmt.Errorf("%s missing %q in content:\n%s", path, part, text)
		}
	}
	return nil
}

func dumpLiveDeveloperTaskTranscript(ctx context.Context, t *testing.T, readGraph *GraphClient, chatID string, since time.Time, nonce string) {
	t.Helper()
	window, err := readGraph.ListMessagesWindow(ctx, chatID, 24, since)
	if err != nil {
		t.Fatalf("read live developer task transcript failed: %v", err)
	}
	messages := window.Messages
	sort.SliceStable(messages, func(i, j int) bool {
		left := parseGraphTime(messages[i].CreatedDateTime)
		right := parseGraphTime(messages[j].CreatedDateTime)
		if !left.IsZero() && !right.IsZero() && !left.Equal(right) {
			return left.Before(right)
		}
		return messages[i].ID < messages[j].ID
	})
	t.Logf("REAL_DEV_TRANSCRIPT_BEGIN nonce=%s chat=%s", nonce, chatID)
	line := 0
	for _, message := range messages {
		text := strings.TrimSpace(PlainTextFromTeamsHTML(message.Body.Content))
		if text == "" {
			continue
		}
		line++
		t.Logf("REAL_DEV_TRANSCRIPT %02d %s", line, strings.ReplaceAll(text, "\n", "\\n"))
	}
	t.Logf("REAL_DEV_TRANSCRIPT_END nonce=%s", nonce)
}
