package teams

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestLiveProductionTeamsRunThirdPartyLongCacheStressOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_PRODUCTION_CXP_CACHE_STRESS")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_PRODUCTION_CXP_CACHE_STRESS=1 to run a longer production Teams third-party cache stress")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before any live Teams chat read or send", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	requireLiveWriteOnce(t, "production-teams-run-thirdparty-cache-stress")

	cxpPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_PRODUCTION_CXP_PATH"))
	if cxpPath == "" {
		t.Fatal("CODEX_HELPER_TEAMS_LIVE_PRODUCTION_CXP_PATH is required")
	}
	codexPath := firstNonEmptyString(
		strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_PRODUCTION_CODEX_PATH")),
		strings.TrimSpace(os.Getenv("CODEX_CLI")),
	)
	if codexPath == "" {
		t.Fatal("CODEX_HELPER_TEAMS_LIVE_PRODUCTION_CODEX_PATH or CODEX_CLI is required")
	}
	if strings.TrimSpace(os.Getenv("DEEPSEEK_API_KEY")) == "" {
		t.Fatal("DEEPSEEK_API_KEY is required")
	}
	if strings.TrimSpace(os.Getenv("MIMO_API_KEY")) == "" {
		t.Fatal("MIMO_API_KEY is required")
	}

	t.Setenv("CODEX_HELPER_TEAMS_AUTH_PROFILE", "default")
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

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Minute)
	defer cancel()
	auth := NewAuthManager(cfg)
	graph := NewGraphClient(auth, io.Discard)
	me, err := graph.Me(ctx)
	if err != nil {
		t.Fatalf("Graph /me failed: %v", err)
	}
	if normalizeLiveHumanName(me.DisplayName) != "jason wei" {
		t.Fatalf("logged-in user displayName %q is not Jason Wei", me.DisplayName)
	}

	tmp := t.TempDir()
	nonce := safeLiveMarkerPart(strings.TrimSpace(os.Getenv(liveWriteOnceEnv)))
	scopeProfile := "cache-cxp-" + strings.ToLower(shortStableID(nonce))
	configPath := filepath.Join(tmp, "codex-proxy-config.json")
	codexHome := filepath.Join(tmp, "codex-home")
	registryPath := filepath.Join(tmp, "teams-registry.json")
	if err := os.MkdirAll(codexHome, 0o700); err != nil {
		t.Fatalf("mkdir Codex home: %v", err)
	}
	writeProductionThirdPartyModelConfig(t, configPath)

	t.Setenv(envTeamsProfile, scopeProfile)
	t.Setenv("CODEX_HELPER_TEAMS_AUTH_PROFILE", "default")
	t.Setenv("CODEX_HELPER_CONFIG", configPath)
	t.Setenv("CODEX_HOME", codexHome)
	t.Setenv("CODEX_DIR", codexHome)
	scope := ScopeIdentityForUser(me)
	scope, storePath, err := ResolveStorePathForScope(scope)
	if err != nil {
		t.Fatalf("ResolveStorePathForScope: %v", err)
	}
	store, err := teamstore.Open(storePath)
	if err != nil {
		t.Fatalf("open production Teams store %s: %v", storePath, err)
	}
	t.Logf("LIVE_CACHE_STRESS_SCOPE profile=%s scope=%s registry=%s store=%s codex_home=%s", scopeProfile, scope.ID, registryPath, storePath, codexHome)

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, cxpPath,
		"--config", configPath,
		"teams", "--registry", registryPath, "run",
		"--interval", "2s",
		"--top", "30",
		"--max-work-chat-polls", "8",
		"--auto-service=false",
		"--auto-update=false",
		"--runner", "exec",
		"--codex-path", codexPath,
		"--codex-arg=--skip-git-repo-check",
		"--codex-timeout", "10m",
	)
	cmd.Env = productionTeamsRunEnv(os.Environ(), configPath, codexHome, scopeProfile)
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("start production teams run: %v", err)
	}
	t.Cleanup(func() {
		stopProductionCacheStressProcess(t, cmd, &stdout, &stderr)
	})

	controlChat := waitForProductionTeamsRunControlChat(t, ctx, registryPath)
	requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, controlChat.ID)
	t.Logf("LIVE_CACHE_STRESS_CONTROL_CHAT url=%s", controlChat.WebURL)

	scenarios := []productionCacheStressScenario{
		{
			Name:     "deepseek",
			Provider: "deepseek",
			Profile:  "deepseek-live",
			WorkDir:  filepath.Join(tmp, "deepseek-cache-stress-"+nonce),
			FileName: "deepseek-realistic-cache.md",
		},
		{
			Name:     "mimo",
			Provider: "mimo",
			Profile:  "mimo25-live",
			WorkDir:  filepath.Join(tmp, "mimo-cache-stress-"+nonce),
			FileName: "mimo-realistic-cache.md",
		},
	}
	for i := range scenarios {
		scenario := &scenarios[i]
		if err := os.MkdirAll(scenario.WorkDir, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", scenario.WorkDir, err)
		}
		liveSendText(ctx, t, graph, controlChat.ID, "new "+scenario.WorkDir+" --model-profile "+scenario.Profile+" -- cache stress "+scenario.Name+" "+nonce)
	}
	for i := range scenarios {
		scenario := &scenarios[i]
		session := waitForLiveSessionByCwd(t, ctx, registryPath, scenario.WorkDir)
		scenario.Session = session
		requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, session.ChatID)
		if session.ModelProfile.Provider != scenario.Provider {
			t.Fatalf("%s session model profile = %#v, want provider %s", scenario.Name, session.ModelProfile, scenario.Provider)
		}
		waitForLiveOutbox(t, ctx, store, session.ChatID, nil, scenario.Name+" ready", 50, "Work chat is ready", filepath.Base(scenario.WorkDir), "model:"+scenario.Profile)
		t.Logf("LIVE_CACHE_STRESS_WORK_CHAT provider=%s chat=%s url=%s", scenario.Provider, session.ChatID, session.ChatURL)
	}

	rounds := productionCacheStressRounds(t)
	for round := 0; round < rounds; round++ {
		afterSeq := make(map[string]int64, len(scenarios))
		for _, scenario := range scenarios {
			afterSeq[scenario.Name] = liveOutboxMaxSequence(t, ctx, store, scenario.Session.ChatID)
		}
		for _, scenario := range scenarios {
			marker := scenario.marker(nonce, round)
			liveSendText(ctx, t, graph, scenario.Session.ChatID, scenario.prompt(nonce, round, marker))
		}
		for i := range scenarios {
			scenario := &scenarios[i]
			marker := scenario.marker(nonce, round)
			waitForLiveThirdPartyFinalAfter(t, ctx, store, scenario.Session.ChatID, fmt.Sprintf("%s cache stress round %02d final", scenario.Name, round+1), 90, afterSeq[scenario.Name], marker)
			scenario.Session = liveLoadSessionByChat(t, registryPath, scenario.Session.ChatID)
			if strings.TrimSpace(scenario.Session.CodexThreadID) == "" {
				t.Fatalf("%s round %02d did not persist CodexThreadID", scenario.Name, round+1)
			}
			if round > 0 && scenario.ThreadID != "" && scenario.Session.CodexThreadID != scenario.ThreadID {
				t.Fatalf("%s CodexThreadID changed: first=%s now=%s", scenario.Name, scenario.ThreadID, scenario.Session.CodexThreadID)
			}
			scenario.ThreadID = scenario.Session.CodexThreadID
			stats := waitForProductionTeamsRunTokenStats(t, ctx, scenario.Session.CodexThreadID)
			usage := firstNonZeroProductionUsage(stats.Info.Last, stats.Info.Total)
			if usage.InputTokens <= 0 {
				t.Fatalf("%s round %02d token stats missing input tokens: %#v", scenario.Name, round+1, stats.Info)
			}
			hitRate := 0.0
			if usage.InputTokens > 0 {
				hitRate = 100 * float64(usage.CachedInputTokens) / float64(usage.InputTokens)
			}
			t.Logf("LIVE_CACHE_STRESS provider=%s round=%02d thread=%s input=%d cached=%d hit_rate=%.1f%% source=%s",
				scenario.Provider, round+1, scenario.Session.CodexThreadID, usage.InputTokens, usage.CachedInputTokens, hitRate, stats.Source)
			if round > 0 && usage.CachedInputTokens <= 0 {
				t.Fatalf("%s round %02d cache did not hit: input=%d cached=%d source=%s", scenario.Name, round+1, usage.InputTokens, usage.CachedInputTokens, stats.Source)
			}
		}
	}

	for _, scenario := range scenarios {
		wants := []string{"# Cache stress " + nonce}
		for round := 0; round < rounds; round++ {
			wants = append(wants, scenario.fileLine(nonce, round))
		}
		requireFileContains(t, filepath.Join(scenario.WorkDir, scenario.FileName), wants...)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load production Teams store failed: %v", err)
	}
	for _, scenario := range scenarios {
		if got := liveCompletedTurnCount(state, scenario.Session.ID); got < rounds {
			t.Fatalf("%s completed turns = %d, want at least %d", scenario.Name, got, rounds)
		}
	}
	if stuck := liveNonTerminalTurnSummary(state); stuck != "" {
		t.Fatalf("production cache stress left non-terminal turns: %s", stuck)
	}
}

type productionCacheStressScenario struct {
	Name     string
	Provider string
	Profile  string
	WorkDir  string
	FileName string
	Session  Session
	ThreadID string
}

func (s productionCacheStressScenario) marker(nonce string, round int) string {
	return "CACHE" + strings.ToUpper(shortStableID(nonce + s.Provider))[:5] + fmt.Sprintf("%02d", round+1)
}

func (s productionCacheStressScenario) fileLine(nonce string, round int) string {
	return fmt.Sprintf("- round %02d %s %s", round+1, s.Provider, nonce)
}

func (s productionCacheStressScenario) prompt(nonce string, round int, marker string) string {
	line := s.fileLine(nonce, round)
	switch round {
	case 0:
		return "Create " + s.FileName + " in the current directory. The file must contain a heading '# Cache stress " + nonce + "' and this bullet: " + line + ". Then read the file and finish with marker " + marker + " on its own line."
	case 1:
		return "Continue this existing task. Read " + s.FileName + ", append this bullet if missing: " + line + ". Then report the current bullet count and finish with marker " + marker + " on its own line."
	case 2:
		return "Continue from the same file. Read " + s.FileName + ", append this bullet if missing: " + line + ". Also add a short 'Notes' section if it does not exist. Finish with marker " + marker + " on its own line."
	default:
		return "Continue the same cache stress task. Read " + s.FileName + ", append this bullet if missing: " + line + ". Verify all bullets from round 01 through round " + fmt.Sprintf("%02d", round+1) + " are present exactly once. Finish with marker " + marker + " on its own line."
	}
}

func productionCacheStressRounds(t *testing.T) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_PRODUCTION_CXP_CACHE_STRESS_ROUNDS"))
	if raw == "" {
		return 6
	}
	rounds, err := strconv.Atoi(raw)
	if err != nil || rounds <= 0 || rounds > 12 {
		t.Fatalf("CODEX_HELPER_TEAMS_LIVE_PRODUCTION_CXP_CACHE_STRESS_ROUNDS=%q, want 1..12", raw)
	}
	return rounds
}

func stopProductionCacheStressProcess(t *testing.T, cmd *exec.Cmd, stdout *bytes.Buffer, stderr *bytes.Buffer) {
	t.Helper()
	if cmd == nil || cmd.Process == nil {
		return
	}
	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()
	_ = cmd.Process.Signal(syscall.SIGINT)
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		_ = cmd.Process.Kill()
		<-done
	}
	t.Logf("LIVE_CACHE_STRESS_TEAMS_RUN_STDOUT\n%s", strings.TrimSpace(stdout.String()))
	if text := strings.TrimSpace(stderr.String()); text != "" {
		t.Logf("LIVE_CACHE_STRESS_TEAMS_RUN_STDERR\n%s", text)
	}
}
