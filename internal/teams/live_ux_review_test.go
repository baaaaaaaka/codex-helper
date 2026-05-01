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
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestLiveBridgeUXReviewRoundOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_UX_REVIEW")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_UX_REVIEW=1 to run one live Teams UX review round")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before any live Teams chat read or send", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	scenario := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_UX_SCENARIO"))
	if scenario == "" {
		t.Fatal("CODEX_HELPER_TEAMS_LIVE_UX_SCENARIO is required")
	}
	requireLiveWriteOnce(t, "ux-review-"+scenario)

	ctx := context.Background()
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
	roundStarted := time.Now().Add(-5 * time.Second)
	registryPath := liveHelperE2ERegistryPath(t)
	pruneLiveHelperE2ERegistry(t, registryPath)
	t.Cleanup(func() {
		pruneLiveHelperE2ERegistry(t, registryPath)
	})
	adoptLiveHelperE2EControlChat(ctx, t, graph, readGraph, registryPath, me)
	requireLiveHelperE2EControlBindingOrSkip(t, registryPath)
	store, err := teamstore.Open(filepath.Join(tmp, "state.json"))
	if err != nil {
		t.Fatalf("open live UX store: %v", err)
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

	executor := uxReviewExecutor{}
	listenOnce := func(label string) error {
		if err := bridge.Listen(ctx, BridgeOptions{
			RegistryPath:            registryPath,
			Store:                   store,
			HelperVersion:           "live-ux-review-" + scenario,
			Once:                    true,
			Top:                     20,
			Executor:                executor,
			ControlFallbackExecutor: executor,
			ControlFallbackModel:    "ux-review",
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
		deadline := time.Now().Add(6 * time.Minute)
		var lastErr error
		for attempts := 0; attempts < 10 && time.Now().Before(deadline); attempts++ {
			if err := listenOnce(label); err != nil {
				lastErr = err
			} else if ok, err := liveSentOutboxContains(ctx, store, chatID, parts...); err != nil {
				lastErr = err
			} else if ok {
				return
			}
			time.Sleep(12 * time.Second)
		}
		if lastErr != nil {
			t.Fatalf("%s did not produce expected live outbox %q: %v", label, parts, lastErr)
		}
		requireLiveSentOutboxContaining(ctx, t, store, chatID, parts...)
	}
	sendHTML := func(label string, chatID string, text string) {
		t.Helper()
		if _, err := graph.SendHTML(ctx, chatID, "<p>"+html.EscapeString(text)+"</p>"); err != nil {
			t.Fatalf("send live %s failed: %v", label, err)
		}
	}
	waitForSession := func(label string) Session {
		t.Helper()
		deadline := time.Now().Add(6 * time.Minute)
		var lastErr error
		for attempts := 0; attempts < 10 && time.Now().Before(deadline); attempts++ {
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
		t.Fatalf("%s did not create a live session", label)
		return Session{}
	}
	dumpTranscript := func(label string, chatID string, top int) {
		t.Helper()
		window, err := readGraph.ListMessagesWindow(ctx, chatID, top, roundStarted)
		if err != nil {
			t.Fatalf("read live transcript for %s failed: %v", label, err)
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
		t.Logf("UX_TRANSCRIPT_BEGIN scenario=%s label=%s chat=%s nonce=%s", scenario, label, chatID, nonce)
		line := 0
		for _, message := range messages {
			text := strings.TrimSpace(PlainTextFromTeamsHTML(message.Body.Content))
			if text == "" {
				continue
			}
			line++
			t.Logf("UX_TRANSCRIPT %02d %s", line, strings.ReplaceAll(text, "\n", "\\n"))
		}
		t.Logf("UX_TRANSCRIPT_END scenario=%s label=%s", scenario, label)
	}

	switch scenario {
	case "control":
		sendHTML("control help", controlChat.ID, "help")
		waitForOutbox("control help", controlChat.ID, "Start here:", "help advanced")
		sendHTML("control wrong helper", controlChat.ID, "helper file report.md")
		waitForOutbox("control wrong helper", controlChat.ID, "control chat", "Work chat")
		sendHTML("control path hint", controlChat.ID, "./tmp/live ux "+nonce)
		waitForOutbox("control path hint", controlChat.ID, "Detected path", "copy and send")
		sendHTML("control fallback", controlChat.ID, "live ux quick question "+nonce)
		waitForOutbox("control fallback ack", controlChat.ID, "Quick helper question")
		waitForOutbox("control fallback final", controlChat.ID, "quick helper answer", "live ux quick question "+nonce)
		dumpTranscript("control", controlChat.ID, 16)
	case "work":
		workDir := filepath.Join(tmp, "live-ux-work")
		sendHTML("control new work", controlChat.ID, "new "+workDir+" -- live ux work "+nonce)
		workSession := waitForSession("control new work")
		requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, workSession.ChatID)
		if _, err := store.RecordChatPollSuccess(ctx, workSession.ChatID, time.Now().UTC(), true, false, 0); err != nil {
			t.Fatalf("seed live work poll cursor failed: %v", err)
		}
		waitForOutbox("work ready", workSession.ChatID, "Codex will start automatically")
		sendHTML("work status", workSession.ChatID, "helper status")
		waitForOutbox("work status", workSession.ChatID, "STATUS: Work chat", "Last request")
		sendHTML("work help", workSession.ChatID, "help")
		waitForOutbox("work help", workSession.ChatID, "Work chat", "helper status")
		sendHTML("work prompt", workSession.ChatID, "Summarize exactly LIVE-UX-WORK "+nonce)
		waitForOutbox("work prompt ack", workSession.ChatID, "Codex is working")
		waitForOutbox("work prompt final", workSession.ChatID, "Result:", "LIVE-UX-WORK "+nonce)
		dumpTranscript("control", controlChat.ID, 8)
		dumpTranscript("work", workSession.ChatID, 16)
	case "history":
		transcriptPath := filepath.Join(tmp, "session.jsonl")
		sessionID := "live-ux-history-" + nonce
		records := []string{
			`{"id":"u1","role":"user","text":"live ux history user prompt ` + nonce + `"}`,
			`{"id":"a1","role":"assistant","text":"live ux history assistant reply ` + nonce + `"}`,
			`{"id":"tool1","type":"tool","text":"live ux history tool/status ` + nonce + `"}`,
			`{"id":"u2","role":"user","text":"live ux history second user ` + nonce + `"}`,
			`{"id":"a2","role":"assistant","text":"live ux history second assistant ` + nonce + `"}`,
			``,
		}
		if err := os.WriteFile(transcriptPath, []byte(strings.Join(records, "\n")), 0o600); err != nil {
			t.Fatalf("write live UX transcript: %v", err)
		}
		prevDiscover := discoverCodexProjectsForTeams
		discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
			return []codexhistory.Project{{
				Key:  "live-ux-history",
				Path: tmp,
				Sessions: []codexhistory.Session{{
					SessionID:   sessionID,
					FirstPrompt: "live ux history " + nonce,
					ProjectPath: tmp,
					FilePath:    transcriptPath,
					ModifiedAt:  time.Now(),
				}},
			}}, nil
		}
		t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })
		if _, err := bridge.publishCodexSession(ctx, DashboardCommandTarget{Raw: sessionID}); err != nil {
			t.Fatalf("publish live UX transcript failed: %v", err)
		}
		if err := bridge.Save(); err != nil {
			t.Fatalf("save live UX registry after history publish failed: %v", err)
		}
		reg, err := LoadRegistry(registryPath)
		if err != nil {
			t.Fatalf("LoadRegistry after history publish failed: %v", err)
		}
		if len(reg.Sessions) != 1 {
			t.Fatalf("history publish sessions = %#v, want one", reg.Sessions)
		}
		workSession := reg.Sessions[0]
		requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, workSession.ChatID)
		waitForLiveHistory := []string{"User:\nlive ux history user prompt " + nonce, "Codex answer:\nlive ux history second assistant " + nonce}
		deadline := time.Now().Add(6 * time.Minute)
		for time.Now().Before(deadline) {
			messages, err := liveRecentPlainMessagesAscending(ctx, readGraph, workSession.ChatID, 20)
			if err == nil && containsOrderedLiveHistoryMessages(messages, waitForLiveHistory) {
				dumpTranscript("history", workSession.ChatID, 20)
				return
			}
			time.Sleep(12 * time.Second)
		}
		dumpTranscript("history-timeout", workSession.ChatID, 20)
		t.Fatalf("live UX history transcript did not appear in order")
	case "retry-status":
		workDir := filepath.Join(tmp, "live-ux-retry")
		sendHTML("control new retry", controlChat.ID, "new "+workDir+" -- live ux retry "+nonce)
		workSession := waitForSession("control new retry")
		requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, workSession.ChatID)
		if _, err := store.RecordChatPollSuccess(ctx, workSession.ChatID, time.Now().UTC(), true, false, 0); err != nil {
			t.Fatalf("seed live retry work poll cursor failed: %v", err)
		}
		waitForOutbox("retry work ready", workSession.ChatID, "Codex will start automatically")
		turn, _, err := store.QueueTurn(ctx, teamstore.Turn{ID: "turn:live-ux-interrupted-" + nonce, SessionID: workSession.ID})
		if err != nil {
			t.Fatalf("QueueTurn live UX interrupted: %v", err)
		}
		if _, err := store.MarkTurnInterrupted(ctx, turn.ID, "live UX simulated uncertain completion"); err != nil {
			t.Fatalf("MarkTurnInterrupted live UX: %v", err)
		}
		sendHTML("retry helper status", workSession.ChatID, "helper status")
		waitForOutbox("retry helper status", workSession.ChatID, "interrupted", "helper retry last", "changed files first")
		dumpTranscript("retry-status", workSession.ChatID, 12)
	case "lifecycle":
		workDir := filepath.Join(tmp, "live-ux-lifecycle")
		sendHTML("control new lifecycle", controlChat.ID, "new "+workDir+" -- live ux lifecycle "+nonce)
		workSession := waitForSession("control new lifecycle")
		requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, workSession.ChatID)
		if _, err := store.RecordChatPollSuccess(ctx, workSession.ChatID, time.Now().UTC(), true, false, 0); err != nil {
			t.Fatalf("seed live lifecycle work poll cursor failed: %v", err)
		}
		waitForOutbox("lifecycle work ready", workSession.ChatID, "Codex will start automatically")
		sendHTML("lifecycle rename", workSession.ChatID, "helper rename live ux renamed "+nonce)
		waitForOutbox("lifecycle rename", workSession.ChatID, "renamed")
		sendHTML("lifecycle details", workSession.ChatID, "helper details")
		waitForOutbox("lifecycle details", workSession.ChatID, "Session:", "Teams chat:")
		sendHTML("lifecycle close", workSession.ChatID, "helper close")
		waitForOutbox("lifecycle close", workSession.ChatID, "Session closed", "no longer read or respond")
		dumpTranscript("lifecycle", workSession.ChatID, 14)
	default:
		t.Fatalf("unknown CODEX_HELPER_TEAMS_LIVE_UX_SCENARIO %q", scenario)
	}
}

type uxReviewExecutor struct{}

func (uxReviewExecutor) Run(_ context.Context, session *Session, prompt string) (ExecutionResult, error) {
	visible := StripHelperPromptEchoes(StripArtifactManifestBlocks(prompt))
	visible = strings.TrimSpace(visible)
	if session != nil && session.ID == controlFallbackSessionID {
		return ExecutionResult{Text: "quick helper answer: I received `" + visible + "`. For project work, create a 💬 Work chat with `new <directory> -- <title>`."}, nil
	}
	if visible == "" {
		visible = "the request"
	}
	return ExecutionResult{Text: "I completed the requested Teams helper test task.\n\nResult: " + visible}, nil
}
