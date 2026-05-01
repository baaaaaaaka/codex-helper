package teams

import (
	"context"
	"fmt"
	"html"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestLiveBridgeWorkSessionCommandsOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_WORK_SESSION_COMMANDS")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_WORK_SESSION_COMMANDS=1 to create one Jason Wei-only live work chat and verify work-session commands")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before any live Teams chat read or send", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	requireLiveWriteOnce(t, "work-session-commands")

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
	if chats, err := readGraph.ListChats(ctx, 10); err != nil {
		t.Fatalf("Graph /me/chats list failed: %v", err)
	} else if len(chats) == 0 {
		t.Fatal("Graph /me/chats returned no chats")
	}

	tmp := t.TempDir()
	registryPath := liveHelperE2ERegistryPath(t)
	pruneLiveHelperE2ERegistry(t, registryPath)
	t.Cleanup(func() {
		pruneLiveHelperE2ERegistry(t, registryPath)
	})
	adoptLiveHelperE2EControlChat(ctx, t, graph, readGraph, registryPath, me)
	requireLiveHelperE2EControlBindingOrSkip(t, registryPath)
	storePath := filepath.Join(tmp, "state.json")
	store, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("open live work-session store: %v", err)
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
	listenOnce := func(label string) error {
		if err := bridge.Listen(ctx, BridgeOptions{
			RegistryPath:            registryPath,
			Store:                   store,
			HelperVersion:           "live-work-session-commands",
			Once:                    true,
			Top:                     20,
			Executor:                EchoExecutor{},
			ControlFallbackExecutor: EchoExecutor{},
			ControlFallbackModel:    "echo",
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
		for attempts := 0; attempts < 8 && time.Now().Before(deadline); attempts++ {
			if err := listenOnce(label); err != nil {
				lastErr = err
			} else if ok, err := liveSentOutboxContains(context.Background(), store, chatID, parts...); err != nil {
				lastErr = err
			} else if ok {
				return
			}
			time.Sleep(15 * time.Second)
		}
		if lastErr != nil {
			t.Fatalf("%s did not produce expected live outbox %q: %v", label, parts, lastErr)
		}
		requireLiveSentOutboxContaining(context.Background(), t, store, chatID, parts...)
	}
	waitForSession := func(label string) Session {
		t.Helper()
		deadline := time.Now().Add(6 * time.Minute)
		var lastErr error
		for attempts := 0; attempts < 8 && time.Now().Before(deadline); attempts++ {
			if err := listenOnce(label); err != nil {
				lastErr = err
			} else if reg, err := LoadRegistry(registryPath); err != nil {
				lastErr = err
			} else if active := reg.ActiveSessions(); len(active) > 0 && strings.TrimSpace(active[0].ChatID) != "" {
				return active[0]
			}
			time.Sleep(15 * time.Second)
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

	workDir := filepath.Join(tmp, "live-work-session-commands")
	task := "live work-session commands " + nonce
	newCommand := "<p>new " + html.EscapeString(workDir) + " -- " + html.EscapeString(task) + "</p>"
	if _, err := graph.SendHTML(ctx, controlChat.ID, newCommand); err != nil {
		t.Fatalf("send live new failed: %v", err)
	}
	workSession := waitForSession("control new")
	requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, workSession.ChatID)
	if _, err := store.RecordChatPollSuccess(ctx, workSession.ChatID, time.Now().UTC(), true, false, 0); err != nil {
		t.Fatalf("seed live work poll cursor failed: %v", err)
	}
	waitForOutbox("work ready", workSession.ChatID, "Work chat is ready")

	closed := false
	t.Cleanup(func() {
		if closed || strings.TrimSpace(workSession.ChatID) == "" {
			return
		}
		if _, err := graph.SendHTML(context.Background(), workSession.ChatID, "<p>helper close</p>"); err != nil {
			t.Logf("cleanup live work helper close send failed: %v", err)
			return
		}
		if err := listenOnce("cleanup work close"); err != nil {
			t.Logf("cleanup live work helper close listen failed: %v", err)
		}
	})

	if _, err := graph.SendHTML(ctx, workSession.ChatID, "<p>helper status</p>"); err != nil {
		t.Fatalf("send live work helper status failed: %v", err)
	}
	waitForOutbox("work status", workSession.ChatID, workSession.ID, "active")

	if _, err := graph.SendHTML(ctx, workSession.ChatID, "<p>help</p>"); err != nil {
		t.Fatalf("send live work help failed: %v", err)
	}
	waitForOutbox("work help", workSession.ChatID, "commands:", "helper retry <turn-id>", "unknown slash-prefixed")

	if _, err := graph.SendHTML(ctx, workSession.ChatID, "<p>helper cancel</p>"); err != nil {
		t.Fatalf("send live work helper cancel usage failed: %v", err)
	}
	waitForOutbox("work cancel usage", workSession.ChatID, "usage:", "helper cancel <turn-id>")

	missingCancel := "turn:missing-cancel-" + nonce
	if _, err := graph.SendHTML(ctx, workSession.ChatID, "<p>helper cancel "+html.EscapeString(missingCancel)+"</p>"); err != nil {
		t.Fatalf("send live work helper cancel missing failed: %v", err)
	}
	waitForOutbox("work cancel missing", workSession.ChatID, "turn not found in this session: "+missingCancel)

	if _, err := graph.SendHTML(ctx, workSession.ChatID, "<p>helper retry</p>"); err != nil {
		t.Fatalf("send live work helper retry usage failed: %v", err)
	}
	waitForOutbox("work retry usage", workSession.ChatID, "usage:", "helper retry <turn-id>")

	missingRetry := "turn:missing-retry-" + nonce
	if _, err := graph.SendHTML(ctx, workSession.ChatID, "<p>helper retry "+html.EscapeString(missingRetry)+"</p>"); err != nil {
		t.Fatalf("send live work helper retry missing failed: %v", err)
	}
	waitForOutbox("work retry missing", workSession.ChatID, "turn not found in this session: "+missingRetry)

	unknownSlashPrompt := "/tmp/live-work-session-" + nonce + " should be treated as Codex input"
	if _, err := graph.SendHTML(ctx, workSession.ChatID, "<p>"+html.EscapeString(unknownSlashPrompt)+"</p>"); err != nil {
		t.Fatalf("send live work unknown slash prompt failed: %v", err)
	}
	waitForOutbox("work unknown slash ack", workSession.ChatID, "accepted")
	waitForOutbox("work unknown slash echo", workSession.ChatID, "echo:", unknownSlashPrompt)

	if _, err := graph.SendHTML(ctx, workSession.ChatID, "<p>helper close</p>"); err != nil {
		t.Fatalf("send live work helper close failed: %v", err)
	}
	waitForOutbox("work close", workSession.ChatID, "session closed")
	closed = true
}
