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

func TestLiveBridgeControlDashboardFallbackOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_CONTROL_DASHBOARD_E2E")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_CONTROL_DASHBOARD_E2E=1 to run live control dashboard and fallback checks")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before any live Teams chat read or send", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	requireLiveWriteOnce(t, "control-dashboard-e2e")

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

	ctx := context.Background()
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

	registryPath := liveHelperE2ERegistryPath(t)
	pruneLiveHelperE2ERegistry(t, registryPath)
	t.Cleanup(func() {
		pruneLiveHelperE2ERegistry(t, registryPath)
	})
	adoptLiveHelperE2EControlChat(ctx, t, graph, readGraph, registryPath, me)
	requireLiveHelperE2EControlBindingOrSkip(t, registryPath)
	reg, err := LoadRegistry(registryPath)
	if err != nil {
		t.Fatalf("load live helper e2e registry %s: %v", registryPath, err)
	}

	tmp := t.TempDir()
	store, err := teamstore.Open(filepath.Join(tmp, "state.json"))
	if err != nil {
		t.Fatalf("open live control dashboard store: %v", err)
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
	if controlChat.ID != reg.ControlChatID {
		t.Fatalf("control chat changed from adopted %q to %q; refusing live test", reg.ControlChatID, controlChat.ID)
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
			HelperVersion:           "live-control-dashboard-e2e",
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
	waitAny := func(label string, alternatives ...[]string) {
		t.Helper()
		deadline := time.Now().Add(6 * time.Minute)
		var lastErr error
		for attempts := 0; attempts < 6 && time.Now().Before(deadline); attempts++ {
			if err := listenOnce(label); err != nil {
				lastErr = err
			} else if ok, err := liveSentOutboxContainsAny(ctx, store, controlChat.ID, alternatives...); err != nil {
				lastErr = err
			} else if ok {
				return
			}
			time.Sleep(15 * time.Second)
		}
		if lastErr != nil {
			t.Fatalf("%s did not produce expected live outbox: %v", label, lastErr)
		}
		state, err := store.Load(ctx)
		if err != nil {
			t.Fatalf("load live helper store after %s failed: %v", label, err)
		}
		t.Fatalf("%s did not produce any expected live outbox alternative %q among %d outbox message(s)", label, alternatives, len(state.OutboxMessages))
	}
	sendControl := func(label string, text string) {
		t.Helper()
		if _, err := graph.SendHTML(ctx, controlChat.ID, "<p>"+html.EscapeString(text)+"</p>"); err != nil {
			t.Fatalf("send live %s failed: %v", label, err)
		}
	}

	sendControl("control help", "help")
	waitAny("control help", []string{"first-time path:", "`projects`"})

	mkdirTarget := filepath.Join(tmp, "mkdir-"+nonce)
	sendControl("control mkdir", "mkdir "+mkdirTarget)
	waitAny("control mkdir", []string{"Directory is ready:", mkdirTarget})
	if info, err := os.Stat(mkdirTarget); err != nil || !info.IsDir() {
		t.Fatalf("mkdir target was not created as a directory: info=%#v err=%v", info, err)
	}

	sendControl("control projects", "projects")
	waitAny("control projects", []string{"workspaces:", "Send a number in this control chat"}, []string{"No local Codex workspaces found on this machine"})

	sendControl("control sessions", "sessions")
	waitAny("control sessions", []string{"sessions", "continue <number>"}, []string{"No local Codex sessions found"})

	sendControl("control unknown slash", "/definitely-not-a-helper-command-"+nonce)
	waitAny("control unknown slash", []string{"unknown control command", "projects"})

	fallbackPrompt := "live control fallback " + nonce
	sendControl("control fallback", fallbackPrompt)
	waitAny("control fallback ack", []string{"quick helper question"})
	waitAny("control fallback final", []string{"echo:", fallbackPrompt})
}

func liveSentOutboxContainsAny(ctx context.Context, store *teamstore.Store, chatID string, alternatives ...[]string) (bool, error) {
	for _, parts := range alternatives {
		ok, err := liveSentOutboxContains(ctx, store, chatID, parts...)
		if err != nil || ok {
			return ok, err
		}
	}
	return false, nil
}
