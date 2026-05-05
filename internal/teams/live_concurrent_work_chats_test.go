package teams

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestLiveBridgeConcurrentWorkChatsOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_CONCURRENT_WORK_CHATS")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_CONCURRENT_WORK_CHATS=1 to run live concurrent Teams work-chat stress")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before any live Teams chat read or send", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	requireLiveWriteOnce(t, "concurrent-work-chats")

	nonce := safeLiveMarkerPart(strings.TrimSpace(os.Getenv(liveWriteOnceEnv)))

	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Minute)
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
	t.Setenv(envTeamsProfile, "concurrent-"+nonce)

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
	registryPath := filepath.Join(tmp, "registry.json")
	store, err := teamstore.Open(filepath.Join(tmp, "state.json"))
	if err != nil {
		t.Fatalf("open concurrent live store: %v", err)
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
	t.Logf("LIVE_CONCURRENT_CONTROL_CHAT_URL=%s", controlChat.WebURL)
	if _, err := store.RecordChatPollSuccess(ctx, controlChat.ID, time.Now().UTC(), true, false, 0); err != nil {
		t.Fatalf("seed concurrent control poll cursor failed: %v", err)
	}

	model := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_CONCURRENT_WORK_CHATS_MODEL"))
	if model == "" {
		model = "gpt-5.4-mini"
	}
	rounds := liveConcurrentRounds(t)
	scenarios := liveConcurrentScenarios(t, tmp, nonce, rounds)
	for _, scenario := range scenarios {
		if scenario.Setup != nil {
			scenario.Setup(t, scenario.Dir)
		}
	}

	listenCtx, stopListen := context.WithCancel(ctx)
	listenErr := make(chan error, 1)
	go func() {
		err := bridge.Listen(listenCtx, BridgeOptions{
			RegistryPath:             registryPath,
			Store:                    store,
			HelperVersion:            "live-concurrent-work-chats",
			Interval:                 2 * time.Second,
			Top:                      20,
			MaxWorkChatPollsPerCycle: len(scenarios),
			Executor:                 liveRealCodexExecutor("", model, 5*time.Minute),
			ControlFallbackExecutor:  liveRealCodexExecutor(tmp, liveRealCodexControlModel(), 3*time.Minute),
			ControlFallbackModel:     liveRealCodexControlModel(),
		})
		listenErr <- err
	}()
	defer func() {
		stopListen()
		select {
		case err := <-listenErr:
			if err != nil && !strings.Contains(err.Error(), "context canceled") {
				t.Fatalf("concurrent live bridge listener failed: %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Fatal("timed out waiting for concurrent live bridge listener to stop")
		}
	}()

	for i := range scenarios {
		scenario := &scenarios[i]
		liveSendText(ctx, t, graph, controlChat.ID, "new "+scenario.Dir+" -- "+scenario.Title)
	}
	for i := range scenarios {
		scenario := &scenarios[i]
		session := waitForLiveSessionByCwd(t, ctx, registryPath, scenario.Dir)
		scenario.Session = session
		requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, session.ChatID)
		waitForLiveOutbox(t, ctx, store, session.ChatID, nil, scenario.Name+" ready", 60, "Work chat is ready", filepath.Base(scenario.Dir))
		if _, err := store.RecordChatPollSuccess(ctx, session.ChatID, time.Now().UTC(), true, false, 0); err != nil {
			t.Fatalf("seed concurrent work poll cursor for %s failed: %v", scenario.Name, err)
		}
		t.Logf("LIVE_CONCURRENT_WORK_CHAT name=%s chat=%s url=%s", scenario.Name, session.ChatID, session.ChatURL)
	}

	for round := 0; round < rounds; round++ {
		afterSeq := make(map[string]int64, len(scenarios))
		for _, scenario := range scenarios {
			afterSeq[scenario.Name] = liveOutboxMaxSequence(t, ctx, store, scenario.Session.ChatID)
		}
		for _, scenario := range scenarios {
			liveSendText(ctx, t, graph, scenario.Session.ChatID, scenario.Prompts[round].Text)
		}
		for _, scenario := range scenarios {
			prompt := scenario.Prompts[round]
			if retryTurnID := waitForLiveOutboxOrAmbiguousTurnAfter(t, ctx, store, scenario.Session.ChatID, nil, fmt.Sprintf("%s round %02d final", scenario.Name, round+1), 40, afterSeq[scenario.Name], prompt.Marker); retryTurnID != "" {
				t.Fatalf("%s round %02d became ambiguous: %s", scenario.Name, round+1, retryTurnID)
			}
		}
		t.Logf("LIVE_CONCURRENT_ROUND_DONE round=%02d chats=%d model=%s", round+1, len(scenarios), model)
	}

	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load concurrent live store failed: %v", err)
	}
	for _, scenario := range scenarios {
		if err := scenario.Verify(scenario.Dir); err != nil {
			t.Fatalf("%s workspace verification failed: %v", scenario.Name, err)
		}
		for _, prompt := range scenario.Prompts {
			if got := liveOutboxMarkerCount(state, scenario.Session.ChatID, prompt.Marker); got != 1 {
				t.Fatalf("%s marker %s count = %d, want 1", scenario.Name, prompt.Marker, got)
			}
		}
		if got := liveCompletedTurnCount(state, scenario.Session.ID); got < rounds {
			t.Fatalf("%s completed turns = %d, want at least %d", scenario.Name, got, rounds)
		}
	}
	if stuck := liveNonTerminalTurnSummary(state); stuck != "" {
		t.Fatalf("concurrent live test left non-terminal turns: %s", stuck)
	}
}

type liveConcurrentScenario struct {
	Name    string
	Title   string
	Dir     string
	Setup   func(*testing.T, string)
	Prompts []liveConcurrentPrompt
	Verify  func(string) error
	Session Session
}

type liveConcurrentPrompt struct {
	Text   string
	Marker string
}

func liveConcurrentRounds(t *testing.T) int {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_CONCURRENT_WORK_CHATS_ROUNDS"))
	if raw == "" {
		return 10
	}
	rounds, err := strconv.Atoi(raw)
	if err != nil || rounds <= 0 || rounds > 10 {
		t.Fatalf("CODEX_HELPER_TEAMS_LIVE_CONCURRENT_WORK_CHATS_ROUNDS=%q, want 1..10", raw)
	}
	return rounds
}

func liveConcurrentScenarios(t *testing.T, baseDir string, nonce string, rounds int) []liveConcurrentScenario {
	t.Helper()
	all := []liveConcurrentScenario{
		liveArtifactChainScenario(baseDir, nonce),
		liveDataNotesScenario(baseDir, nonce),
		liveDocsRefactorScenario(baseDir, nonce),
	}
	for i := range all {
		all[i].Prompts = all[i].Prompts[:rounds]
	}
	return all
}

func liveArtifactChainScenario(baseDir string, nonce string) liveConcurrentScenario {
	prefix := "SCN_ART_" + nonce + "_"
	dir := filepath.Join(baseDir, "artifact-chain-"+nonce)
	return liveConcurrentScenario{
		Name:  "artifact-chain",
		Title: "Concurrent artifact chain " + nonce,
		Dir:   dir,
		Setup: func(t *testing.T, dir string) {
			t.Helper()
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatalf("mkdir artifact scenario: %v", err)
			}
		},
		Prompts: []liveConcurrentPrompt{
			{Text: livePrompt("Create notes/01-alpha.txt with the lines \"charlie\", \"alpha\", and \"bravo\" in that order.", prefix+"01_OK"), Marker: prefix + "01_OK"},
			{Text: livePrompt("Sort notes/01-alpha.txt into notes/01-alpha.sorted and verify the sorted file is exactly \"alpha\", \"bravo\", \"charlie\".", prefix+"02_OK"), Marker: prefix + "02_OK"},
			{Text: livePrompt("Create notes/02-pairs.csv with header key,value and rows x,4 and y,5.", prefix+"03_OK"), Marker: prefix + "03_OK"},
			{Text: livePrompt("Sum the value column from notes/02-pairs.csv, write the total 9 to notes/02-total.txt.", prefix+"04_OK"), Marker: prefix + "04_OK"},
			{Text: livePrompt("Create notes/03-title.md containing only the H1 \"# Codex Remote Stress\".", prefix+"05_OK"), Marker: prefix + "05_OK"},
			{Text: livePrompt("Verify the H1 in notes/03-title.md and copy that exact heading to notes/03-title.only.", prefix+"06_OK"), Marker: prefix + "06_OK"},
			{Text: livePrompt("Create notes/04-list.txt with the lines \"delta\", \"echo\", \"foxtrot\", and \"golf\".", prefix+"07_OK"), Marker: prefix + "07_OK"},
			{Text: livePrompt("Reverse notes/04-list.txt into notes/04-list.reversed.txt and verify it is exactly \"golf\", \"foxtrot\", \"echo\", \"delta\".", prefix+"08_OK"), Marker: prefix + "08_OK"},
			{Text: livePrompt("Write notes/05-manifest.txt containing the sorted filenames of every file created so far under notes/, excluding notes/05-manifest.txt itself.", prefix+"09_OK"), Marker: prefix + "09_OK"},
			{Text: livePrompt("Verify all notes/* files exist and that notes/05-manifest.txt has exactly 8 lines.", prefix+"10_OK"), Marker: prefix + "10_OK"},
		},
		Verify: func(dir string) error {
			return verifyFileContains(filepath.Join(dir, "notes", "05-manifest.txt"), "01-alpha.sorted", "04-list.reversed.txt")
		},
	}
}

func liveDataNotesScenario(baseDir string, nonce string) liveConcurrentScenario {
	prefix := "SCN_DATA_" + nonce + "_"
	dir := filepath.Join(baseDir, "data-notes-"+nonce)
	return liveConcurrentScenario{
		Name:  "data-notes",
		Title: "Concurrent data notes " + nonce,
		Dir:   dir,
		Setup: func(t *testing.T, dir string) {
			t.Helper()
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatalf("mkdir data scenario: %v", err)
			}
		},
		Prompts: []liveConcurrentPrompt{
			{Text: livePrompt("Create notes/agenda.txt with exactly three bullet items: plan, data, verify. Then print the file back.", prefix+"01_OK"), Marker: prefix + "01_OK"},
			{Text: livePrompt("Create data/items.json as a JSON array with exactly 3 objects: {\"id\":1,\"name\":\"alpha\"}, {\"id\":2,\"name\":\"beta\"}, {\"id\":3,\"name\":\"gamma\"}. Then print its length.", prefix+"02_OK"), Marker: prefix + "02_OK"},
			{Text: livePrompt("Read data/items.json and write data/items.tsv with one row per item in id<TAB>name format, sorted by id. Then print the line count.", prefix+"03_OK"), Marker: prefix + "03_OK"},
			{Text: livePrompt("Create notes/summary.txt with one sentence that mentions alpha, beta, and gamma in that order.", prefix+"04_OK"), Marker: prefix + "04_OK"},
			{Text: livePrompt("Verify data/items.tsv has exactly 3 lines and the second line is 2<TAB>beta. Print PASS if true, otherwise FAIL.", prefix+"05_OK"), Marker: prefix + "05_OK"},
			{Text: livePrompt("Create data/stats.txt containing exactly these two lines: count=3 and names=alpha,beta,gamma. Then print the file.", prefix+"06_OK"), Marker: prefix + "06_OK"},
			{Text: livePrompt("Append a new line to notes/agenda.txt that says review complete. Then print the last two lines of notes/agenda.txt.", prefix+"07_OK"), Marker: prefix + "07_OK"},
			{Text: livePrompt("Create data/checksum.txt containing the sha256 of data/items.json only. Then print the checksum file.", prefix+"08_OK"), Marker: prefix + "08_OK"},
			{Text: livePrompt("Compare data/stats.txt and notes/summary.txt, then print a one-line note saying whether alpha, beta, and gamma all appear in both.", prefix+"09_OK"), Marker: prefix + "09_OK"},
			{Text: livePrompt("Create notes/final.txt with exactly one line: workflow complete. Then print wc -c notes/final.txt.", prefix+"10_OK"), Marker: prefix + "10_OK"},
		},
		Verify: func(dir string) error {
			return verifyFileContains(filepath.Join(dir, "notes", "final.txt"), "workflow complete")
		},
	}
}

func liveDocsRefactorScenario(baseDir string, nonce string) liveConcurrentScenario {
	prefix := "SCN_DOCS_" + nonce + "_"
	dir := filepath.Join(baseDir, "docs-refactor-"+nonce)
	return liveConcurrentScenario{
		Name:  "docs-refactor",
		Title: "Concurrent docs refactor " + nonce,
		Dir:   dir,
		Setup: writeLiveDocsRefactorProject,
		Prompts: []liveConcurrentPrompt{
			{Text: livePrompt("Open README.md, rewrite the first paragraph into one sentence, keep meaning unchanged.", prefix+"01_OK"), Marker: prefix + "01_OK"},
			{Text: livePrompt("Read docs/workflow.md, add a 3-item numbered list under the existing intro.", prefix+"02_OK"), Marker: prefix + "02_OK"},
			{Text: livePrompt("Verify src/app.py still contains a function named main; say whether it is present.", prefix+"03_OK"), Marker: prefix + "03_OK"},
			{Text: livePrompt("Create docs/refactor-plan.md with exactly these headings: Goal, Constraints, Steps.", prefix+"04_OK"), Marker: prefix + "04_OK"},
			{Text: livePrompt("Add one sentence to docs/refactor-plan.md under Constraints saying no network calls are allowed.", prefix+"05_OK"), Marker: prefix + "05_OK"},
			{Text: livePrompt("Check that scripts/check_docs.py exists and is non-empty; say whether it is present.", prefix+"06_OK"), Marker: prefix + "06_OK"},
			{Text: livePrompt("Append a one-line checklist to docs/workflow.md with the exact text - Verify markers after each turn.", prefix+"07_OK"), Marker: prefix + "07_OK"},
			{Text: livePrompt("Compare README.md and docs/workflow.md; say whether both mention the word workflow.", prefix+"08_OK"), Marker: prefix + "08_OK"},
			{Text: livePrompt("Write docs/refactor-summary.txt containing exactly 2 lines: done and verified.", prefix+"09_OK"), Marker: prefix + "09_OK"},
			{Text: livePrompt("List the changed files only, without diffs, and confirm the set is limited to docs files plus README.md.", prefix+"10_OK"), Marker: prefix + "10_OK"},
		},
		Verify: func(dir string) error {
			return verifyFileContains(filepath.Join(dir, "docs", "refactor-summary.txt"), "done", "verified")
		},
	}
}

func livePrompt(task string, marker string) string {
	return task + "\n\nEnd your response with the exact marker on its own line: " + marker
}

func writeLiveDocsRefactorProject(t *testing.T, dir string) {
	t.Helper()
	files := map[string]string{
		"README.md":             "This workflow demo explains how the helper keeps a small project organized.\n\nMore details will be added by Codex.\n",
		"docs/workflow.md":      "The workflow starts with a short checklist and ends with verification.\n",
		"src/app.py":            "def main():\n    return \"ok\"\n\nif __name__ == \"__main__\":\n    print(main())\n",
		"scripts/check_docs.py": "from pathlib import Path\nprint(Path('docs/workflow.md').exists())\n",
		"docs/.keep":            "",
		"src/.keep":             "",
		"scripts/.keep":         "",
	}
	for name, content := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", filepath.Dir(path), err)
		}
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
}

func waitForLiveSessionByCwd(t *testing.T, ctx context.Context, registryPath string, cwd string) Session {
	t.Helper()
	deadline := time.Now().Add(4 * time.Minute)
	for time.Now().Before(deadline) {
		reg, err := LoadRegistry(registryPath)
		if err == nil {
			for _, session := range reg.Sessions {
				if filepath.Clean(session.Cwd) == filepath.Clean(cwd) && strings.TrimSpace(session.ChatID) != "" {
					return session
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for live session %s canceled: %v", cwd, ctx.Err())
		case <-time.After(2 * time.Second):
		}
	}
	t.Fatalf("timed out waiting for live session cwd=%s", cwd)
	return Session{}
}

func liveOutboxMarkerCount(state teamstore.State, chatID string, marker string) int {
	count := 0
	for _, msg := range state.OutboxMessages {
		if msg.TeamsChatID == chatID && msg.Status == teamstore.OutboxStatusSent && strings.Contains(msg.Body, marker) {
			count++
		}
	}
	return count
}

func liveCompletedTurnCount(state teamstore.State, sessionID string) int {
	count := 0
	for _, turn := range state.Turns {
		if turn.SessionID == sessionID && turn.Status == teamstore.TurnStatusCompleted {
			count++
		}
	}
	return count
}

func liveNonTerminalTurnSummary(state teamstore.State) string {
	var parts []string
	for _, turn := range state.Turns {
		switch turn.Status {
		case teamstore.TurnStatusCompleted, teamstore.TurnStatusFailed, teamstore.TurnStatusInterrupted:
			continue
		default:
			parts = append(parts, turn.ID+":"+string(turn.Status))
		}
	}
	return strings.Join(parts, ", ")
}
