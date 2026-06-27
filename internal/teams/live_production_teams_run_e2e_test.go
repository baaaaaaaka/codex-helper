package teams

import (
	"bytes"
	"context"
	"encoding/json"
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

	"github.com/baaaaaaaka/codex-helper/internal/config"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestLiveProductionTeamsRunThirdPartyCodexE2EOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_PRODUCTION_CXP_E2E")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_PRODUCTION_CXP_E2E=1 to run a live production `cxp teams run` third-party Codex E2E")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before any live Teams chat read or send", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	requireLiveWriteOnce(t, "production-teams-run-thirdparty-codex-e2e")

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

	ctx, cancel := context.WithTimeout(context.Background(), 55*time.Minute)
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
	scopeProfile := "prod-cxp-" + strings.ToLower(shortStableID(nonce))
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
	t.Logf("LIVE_PRODUCTION_SCOPE profile=%s scope=%s registry=%s store=%s codex_home=%s", scopeProfile, scope.ID, registryPath, storePath, codexHome)

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
		stopProductionTeamsRunProcess(t, cmd, &stdout, &stderr)
	})

	controlChat := waitForProductionTeamsRunControlChat(t, ctx, registryPath)
	requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, controlChat.ID)
	t.Logf("LIVE_PRODUCTION_CONTROL_CHAT url=%s", controlChat.WebURL)

	scenarios := []productionTeamsRunScenario{
		{
			Name:      "deepseek",
			Provider:  "deepseek",
			Profile:   "deepseek-live",
			WorkDir:   filepath.Join(tmp, "deepseek-"+nonce),
			FileName:  "deepseek-cache-probe.txt",
			MarkerKey: "DS",
		},
		{
			Name:      "mimo",
			Provider:  "mimo",
			Profile:   "mimo25-live",
			WorkDir:   filepath.Join(tmp, "mimo-"+nonce),
			FileName:  "mimo-cache-probe.txt",
			MarkerKey: "MI",
		},
	}
	for i := range scenarios {
		scenario := &scenarios[i]
		if err := os.MkdirAll(scenario.WorkDir, 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", scenario.WorkDir, err)
		}
		liveSendText(ctx, t, graph, controlChat.ID, "new "+scenario.WorkDir+" --model-profile "+scenario.Profile+" -- production "+scenario.Name+" "+nonce)
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
		t.Logf("LIVE_PRODUCTION_WORK_CHAT provider=%s chat=%s url=%s", scenario.Provider, session.ChatID, session.ChatURL)
	}

	rounds := productionTeamsRunRounds(t)
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
			waitForLiveThirdPartyFinalAfter(t, ctx, store, scenario.Session.ChatID, fmt.Sprintf("%s round %02d final", scenario.Name, round+1), 80, afterSeq[scenario.Name], marker)
			scenario.Session = liveLoadSessionByChat(t, registryPath, scenario.Session.ChatID)
			if strings.TrimSpace(scenario.Session.CodexThreadID) == "" {
				t.Fatalf("%s round %02d did not persist CodexThreadID", scenario.Name, round+1)
			}
			stats := waitForProductionTeamsRunTokenStats(t, ctx, scenario.Session.CodexThreadID)
			usage := firstNonZeroProductionUsage(stats.Info.Last, stats.Info.Total)
			if usage.InputTokens <= 0 {
				t.Fatalf("%s round %02d token stats missing input tokens: %#v", scenario.Name, round+1, stats.Info)
			}
			hitRate := 0.0
			if usage.InputTokens > 0 {
				hitRate = 100 * float64(usage.CachedInputTokens) / float64(usage.InputTokens)
			}
			t.Logf("LIVE_PRODUCTION_CACHE provider=%s round=%02d thread=%s input=%d cached=%d hit_rate=%.1f%% source=%s",
				scenario.Provider, round+1, scenario.Session.CodexThreadID, usage.InputTokens, usage.CachedInputTokens, hitRate, stats.Source)
			if round > 0 && usage.CachedInputTokens <= 0 {
				t.Fatalf("%s round %02d cache did not hit: input=%d cached=%d source=%s", scenario.Name, round+1, usage.InputTokens, usage.CachedInputTokens, stats.Source)
			}
		}
	}

	for _, scenario := range scenarios {
		requireFileContains(t, filepath.Join(scenario.WorkDir, scenario.FileName), "round 1 "+nonce, "round 2 "+nonce)
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
		t.Fatalf("production teams run left non-terminal turns: %s", stuck)
	}
}

type productionTeamsRunScenario struct {
	Name      string
	Provider  string
	Profile   string
	WorkDir   string
	FileName  string
	MarkerKey string
	Session   Session
}

func (s productionTeamsRunScenario) marker(nonce string, round int) string {
	return "E2E" + s.MarkerKey + strings.ToUpper(shortStableID(nonce))[:6] + fmt.Sprintf("%02d", round+1)
}

func (s productionTeamsRunScenario) prompt(nonce string, round int, marker string) string {
	anchor := strings.Repeat("stable cache anchor "+nonce+" "+s.Provider+"\n", 180)
	switch round {
	case 0:
		return "In the current working directory, create " + s.FileName + " containing exactly two lines: header " + nonce + " and round 1 " + nonce + ". Then read the file back. End your final answer with the exact marker on its own line: " + marker + "\n\n" + anchor
	case 1:
		return "Continue from the previous turn. Append exactly one new line to " + s.FileName + ": round 2 " + nonce + ". If the line already exists, do not duplicate it. Then read the file back. End your final answer with the exact marker on its own line: " + marker + "\n\n" + anchor
	default:
		return "Continue from the previous turn. Verify " + s.FileName + " contains header " + nonce + ", round 1 " + nonce + ", and round 2 " + nonce + " exactly once each. Do not modify the file. End your final answer with the exact marker on its own line: " + marker + "\n\n" + anchor
	}
}

func productionTeamsRunRounds(t *testing.T) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_PRODUCTION_CXP_E2E_ROUNDS"))
	if raw == "" {
		return 3
	}
	rounds, err := strconv.Atoi(raw)
	if err != nil || rounds <= 0 || rounds > 4 {
		t.Fatalf("CODEX_HELPER_TEAMS_LIVE_PRODUCTION_CXP_E2E_ROUNDS=%q, want 1..4", raw)
	}
	return rounds
}

func writeProductionThirdPartyModelConfig(t *testing.T, path string) {
	t.Helper()
	now := time.Now().UTC()
	cfg := config.Config{
		Version:      config.CurrentVersion,
		ProxyEnabled: boolPtr(false),
		ModelProfiles: map[string]config.ModelProfile{
			"deepseek-live": {
				Provider:  "deepseek",
				APIKeyRef: "env:DEEPSEEK_API_KEY",
				Revision:  1,
				CreatedAt: now,
				UpdatedAt: now,
			},
			"mimo25-live": {
				Provider:  "mimo",
				APIKeyRef: "env:MIMO_API_KEY",
				Revision:  1,
				CreatedAt: now,
				UpdatedAt: now,
			},
		},
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		t.Fatalf("marshal production config: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("mkdir config dir: %v", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o600); err != nil {
		t.Fatalf("write production config: %v", err)
	}
}

func boolPtr(value bool) *bool {
	return &value
}

func productionTeamsRunEnv(base []string, configPath string, codexHome string, profile string) []string {
	out := make([]string, 0, len(base)+8)
	skip := map[string]bool{
		"CODEX_HELPER_CONFIG":             true,
		"CODEX_HELPER_TEAMS_PROFILE":      true,
		"CODEX_HELPER_TEAMS_AUTH_PROFILE": true,
		"CODEX_HOME":                      true,
		"CODEX_DIR":                       true,
	}
	for _, entry := range base {
		name, _, ok := strings.Cut(entry, "=")
		if ok && skip[name] {
			continue
		}
		out = append(out, entry)
	}
	out = append(out,
		"CODEX_HELPER_CONFIG="+configPath,
		"CODEX_HELPER_TEAMS_PROFILE="+profile,
		"CODEX_HELPER_TEAMS_AUTH_PROFILE=default",
		"CODEX_HOME="+codexHome,
		"CODEX_DIR="+codexHome,
	)
	return out
}

func waitForProductionTeamsRunControlChat(t *testing.T, ctx context.Context, registryPath string) Chat {
	t.Helper()
	deadline := time.Now().Add(4 * time.Minute)
	for time.Now().Before(deadline) {
		reg, err := LoadRegistry(registryPath)
		if err == nil && strings.TrimSpace(reg.ControlChatID) != "" {
			return Chat{ID: reg.ControlChatID, WebURL: reg.ControlChatURL, Topic: reg.ControlChatTopic, ChatType: "meeting"}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for production Teams control chat canceled: %v", ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	t.Fatalf("timed out waiting for production Teams control chat in %s", registryPath)
	return Chat{}
}

func waitForProductionTeamsRunTokenStats(t *testing.T, ctx context.Context, threadID string) CodexTokenStats {
	t.Helper()
	deadline := time.Now().Add(3 * time.Minute)
	var lastErr error
	for time.Now().Before(deadline) {
		local := waitForDiscoveredCodexSession(t, ctx, threadID)
		stats, err := ReadCodexTokenStats(local.FilePath)
		if err == nil && stats.HasUsage() {
			return stats
		}
		lastErr = err
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for token stats for %s canceled: %v", threadID, ctx.Err())
		case <-time.After(5 * time.Second):
		}
	}
	if lastErr != nil {
		t.Fatalf("token stats for %s unavailable: %v", threadID, lastErr)
	}
	t.Fatalf("token stats for %s unavailable", threadID)
	return CodexTokenStats{}
}

func firstNonZeroProductionUsage(values ...CodexTokenUsage) CodexTokenUsage {
	for _, value := range values {
		if value.hasTokens() {
			return value
		}
	}
	return CodexTokenUsage{}
}

func stopProductionTeamsRunProcess(t *testing.T, cmd *exec.Cmd, stdout *bytes.Buffer, stderr *bytes.Buffer) {
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
	t.Logf("LIVE_PRODUCTION_TEAMS_RUN_STDOUT\n%s", strings.TrimSpace(stdout.String()))
	if text := strings.TrimSpace(stderr.String()); text != "" {
		t.Logf("LIVE_PRODUCTION_TEAMS_RUN_STDERR\n%s", text)
	}
}
