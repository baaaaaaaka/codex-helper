package teams

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

const (
	liveJasonWeiSafetyAckEnv   = "CODEX_HELPER_TEAMS_LIVE_JASON_WEI_ONLY"
	liveJasonWeiSafetyAckValue = "jason-wei-only"
	liveWriteOnceEnv           = "CODEX_HELPER_TEAMS_LIVE_WRITE_ONCE"
)

func TestLiveGraphSmokeOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_TEST")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_TEST=1 to run live Microsoft Graph smoke checks")
	}
	cfg, err := DefaultReadAuthConfig()
	if err != nil {
		t.Fatalf("DefaultReadAuthConfig error: %v", err)
	}
	tok, err := readTokenCache(cfg.CachePath)
	if err != nil {
		t.Fatalf("read Teams read token cache %s: %v", cfg.CachePath, err)
	}
	token := strings.TrimSpace(tok.AccessToken)
	if tok.ExpiresAt <= time.Now().Add(time.Minute).Unix() {
		if strings.TrimSpace(tok.RefreshToken) == "" {
			t.Fatalf("Teams read token cache %s has no fresh access token or refresh token", cfg.CachePath)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		token, err = NewAuthManager(cfg).RefreshAccessToken(ctx)
		if err != nil {
			t.Fatalf("refresh Teams access token: %v", err)
		}
	}
	if token == "" {
		t.Fatalf("Teams read token cache %s has no access token", cfg.CachePath)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	graph := newLiveSmokeGraph(token)
	me, err := graph.Me(ctx)
	if err != nil {
		t.Fatalf("Graph /me smoke failed: %v", err)
	}
	if strings.TrimSpace(me.ID) == "" {
		t.Fatalf("Graph /me returned no user id: %#v", me)
	}

	if chatID := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_CHAT_ID")); chatID != "" {
		requireLiveJasonWeiSingleMemberChat(ctx, t, graph, chatID)
		if _, err := graph.ListMessages(ctx, chatID, 20); err != nil {
			t.Fatalf("Graph chat read smoke failed for configured chat: %v", err)
		}
	}
}

func TestLiveGraphReadPermissionBenchmarkOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_READ_BENCH")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_READ_BENCH=1 to benchmark live Teams read latency across token profiles")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before any live Teams chat read benchmark", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	defer cancel()

	readCfg, err := DefaultReadAuthConfig()
	if err != nil {
		t.Fatalf("DefaultReadAuthConfig error: %v", err)
	}
	writeCfg, err := DefaultAuthConfig()
	if err != nil {
		t.Fatalf("DefaultAuthConfig error: %v", err)
	}
	fileCfg, err := DefaultFileWriteAuthConfig()
	if err != nil {
		t.Fatalf("DefaultFileWriteAuthConfig error: %v", err)
	}
	readGraph := NewGraphClient(NewAuthManager(readCfg), io.Discard)
	writeGraph := NewGraphClient(NewAuthManager(writeCfg), io.Discard)
	chatID, chatTopic := resolveLiveReadBenchmarkChat(ctx, t, readGraph, writeGraph)
	t.Logf("READ_BENCH_TARGET chat=%s topic=%q", shortGraphID(chatID), chatTopic)

	profiles := []struct {
		name string
		cfg  AuthConfig
	}{
		{name: "read-only", cfg: readCfg},
		{name: "chat-write", cfg: writeCfg},
		{name: "file-write", cfg: fileCfg},
	}
	iterations := liveReadBenchmarkIterations()
	for _, profile := range profiles {
		tok, err := readTokenCache(profile.cfg.CachePath)
		if errors.Is(err, os.ErrNotExist) {
			t.Logf("READ_BENCH_SKIP profile=%s reason=missing_cache cache=%s", profile.name, profile.cfg.CachePath)
			continue
		}
		if err != nil {
			t.Logf("READ_BENCH_SKIP profile=%s reason=%q cache=%s", profile.name, err.Error(), profile.cfg.CachePath)
			continue
		}
		graph := NewGraphClient(NewAuthManager(profile.cfg), io.Discard)
		scopes := firstNonEmptyString(strings.TrimSpace(tok.Scope), accessTokenScopesForLog(tok.AccessToken), profile.cfg.Scopes)
		t.Logf("READ_BENCH_PROFILE profile=%s client=%s configured_scopes=%q token_scopes=%q cache=%s", profile.name, profile.cfg.ClientID, profile.cfg.Scopes, scopes, profile.cfg.CachePath)
		liveBenchmarkGraphReadOperation(ctx, t, profile.name, "me", iterations, func(context.Context) error {
			_, err := graph.Me(ctx)
			return err
		})
		for _, top := range []int{10, 20} {
			top := top
			liveBenchmarkGraphReadOperation(ctx, t, profile.name, fmt.Sprintf("messages_top_%d", top), iterations, func(context.Context) error {
				_, err := graph.ListMessages(ctx, chatID, top)
				return err
			})
		}
		liveBenchmarkGraphReadOperation(ctx, t, profile.name, "messages_window_top_20_filtered", iterations, func(context.Context) error {
			_, err := graph.ListMessagesWindow(ctx, chatID, 20, time.Now().Add(-10*time.Minute))
			return err
		})
	}
}

func TestLiveTeamsFormattingRenderOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_FORMATTING_TEST")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_FORMATTING_TEST=1 to send a live Teams formatting smoke transcript")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before any live Teams chat read, send, mention, or file upload", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	existingChatID := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_FORMATTING_CHAT_ID"))
	if existingChatID == "" {
		requireLiveWriteOnce(t, "teams-formatting-render")
	}

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

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	graph := NewGraphClient(NewAuthManager(cfg), io.Discard)
	readGraph := NewGraphClient(NewAuthManager(readCfg), io.Discard)
	me, err := graph.Me(ctx)
	if err != nil {
		t.Fatalf("Graph /me failed: %v", err)
	}
	if normalizeLiveHumanName(me.DisplayName) != "jason wei" {
		t.Fatalf("logged-in user displayName %q is not Jason Wei", me.DisplayName)
	}

	var chat Chat
	if existingChatID != "" {
		requireLiveJasonWeiSingleMemberChat(ctx, t, graph, existingChatID)
		chat, err = graph.GetChat(ctx, existingChatID)
		if err != nil {
			t.Fatalf("get live formatting chat failed: %v", err)
		}
	} else {
		nonce := safeLiveMarkerPart(strings.TrimSpace(os.Getenv(liveWriteOnceEnv)))
		title := "💬 Codex Format Live - " + nonce
		chat, err = graph.CreateSingleMemberGroupChat(ctx, me.ID, title)
		if err != nil {
			t.Fatalf("create live formatting chat failed: %v", err)
		}
		requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, chat.ID)
	}
	if refreshed, err := graph.GetChat(ctx, chat.ID); err == nil && strings.TrimSpace(refreshed.WebURL) != "" {
		chat = refreshed
	}
	t.Logf("LIVE_FORMATTING_CHAT_ID=%s", chat.ID)
	t.Logf("LIVE_FORMATTING_CHAT_URL=%s", chat.WebURL)
	t.Logf("LIVE_FORMATTING_CHAT_TOPIC=%s", chat.Topic)

	if existingChatID == "" {
		transcript := []TeamsRenderInput{
			{
				Surface: TeamsRenderSurfaceOutbox,
				Kind:    TeamsRenderUser,
				Text:    "检查 Teams renderer 的 Markdown 和状态输出；只给我结论和必要的测试证据。",
			},
			{
				Surface: TeamsRenderSurfaceOutbox,
				Kind:    TeamsRenderProgress,
				Text: strings.Join([]string{
					"Reviewing `internal/teams/markdown.go` and the live Graph send path.",
					"",
					"- Found the renderer uses a conservative Markdown subset.",
					"- Next step: run focused render tests and send one live Teams sample.",
				}, "\n"),
			},
			{
				Surface: TeamsRenderSurfaceOutbox,
				Kind:    TeamsRenderCommand,
				Text: strings.Join([]string{
					`/usr/bin/zsh -lc 'go test ./internal/teams -run TestRenderTeamsHTMLCodexMarkdown -count=1'`,
					"",
					"Status: completed",
					"Exit code: 0",
					"",
					"Output:",
					"ok  github.com/baaaaaaaka/codex-helper/internal/teams  0.018s",
				}, "\n"),
			},
			{
				Surface: TeamsRenderSurfaceOutbox,
				Kind:    TeamsRenderAssistant,
				Text: strings.Join([]string{
					"## Summary",
					"",
					"The Teams renderer now keeps the important Codex shapes readable:",
					"",
					"- **Status** messages stay separate from final answers.",
					"- Inline code like `**not bold** <tag>` remains literal.",
					"- Local links such as [markdown.go](./internal/teams/markdown.go:238) are shown as paths, not clickable external links.",
					"",
					"### Regression checks from real history",
					"",
					"1. **Control Chat**",
					"   一个固定 chat，比如：",
					"",
					"   ```text",
					"   codex-helper control",
					"   ```",
					"",
					"   你在里面发管理命令：",
					"",
					"   ```text",
					"   new fix windows installer",
					"   list",
					"   status",
					"   resume 3",
					"   archive 3",
					"   ```",
					"",
					"  最小 Python 实现骨架",
					"",
					"  import time, json, os, requests",
					"",
					"  def graph(method, path, **kwargs):",
					"      t = token()[\"access_token\"]",
					"      headers = kwargs.pop(\"headers\", {})",
					"      headers[\"Authorization\"] = f\"Bearer {t}\"",
					"      if \"json\" in kwargs:",
					"          headers[\"Content-Type\"] = \"application/json\"",
					"      r = requests.request(method, \"https://graph.microsoft.com/v1.0\" + path,",
					"                           headers=headers, timeout=30, **kwargs)",
					"      r.raise_for_status()",
					"      return r.json() if r.text else {}",
					"",
					"  Graph Teams API 对应 MIAO 的调用",
					"",
					"| Metric | Result | Evidence |",
					"| --- | --- | --- |",
					"| render | **ok** | [test](https://example.com/render-table) |",
					"| unsafe | blocked | [bad](javascript:alert(1)) |",
					"",
					"```go",
					"got := RenderTeamsHTML(TeamsRenderInput{",
					"    Kind: TeamsRenderAssistant,",
					`    Text: "Use ` + "`**not bold** <tag>`" + ` safely",`,
					"})",
					"```",
					"",
					"Raw HTML from history stays visible instead of executing: <script>alert(\"x\")</script>",
				}, "\n"),
			},
		}
		for _, item := range transcript {
			if _, err := graph.SendHTML(ctx, chat.ID, RenderTeamsHTML(item)); err != nil {
				t.Fatalf("send live Teams formatting message kind=%s failed: %v", item.Kind, err)
			}
			time.Sleep(350 * time.Millisecond)
		}
		mentionText := strings.TrimSpace(firstNonEmptyString(me.DisplayName, me.UserPrincipalName, "owner"))
		mention := `<at id="0">` + html.EscapeString(mentionText) + `</at>`
		footer := `<p><strong>🔧 Helper:</strong> ✅ Codex finished responding. ` + mention + `</p>`
		if _, err := graph.SendHTMLWithMentions(ctx, chat.ID, footer, []ChatMention{{ID: 0, Text: mentionText, User: me}}); err != nil {
			t.Fatalf("send live Teams formatting footer failed: %v", err)
		}
	}

	want := []string{
		"🧑‍💻 User:\n\n检查 Teams renderer",
		"🤖 ⏳ Codex status:\n\nReviewing internal/teams/markdown.go",
		"🤖 🛠️ Codex command:\n\n/usr/bin/zsh -lc",
		"🤖 ✅ Codex answer:\n\nSummary",
		"🔧 Helper: ✅ Codex finished responding.",
	}
	wantAny := []string{
		"codex-helper control",
		"def graph(method, path, **kwargs):",
		"Metric\nResult\nEvidence",
		"render\nok\ntest (https://example.com/render-table)",
	}
	deadline := time.Now().Add(90 * time.Second)
	var got []string
	var lastErr error
	for time.Now().Before(deadline) {
		got, lastErr = liveRecentPlainMessagesAscending(ctx, readGraph, chat.ID, 20)
		if lastErr == nil && containsOrderedLiveHistoryMessages(got, want) && containsAllLivePlainText(got, wantAny) {
			rawHTML, err := liveRecentHTMLMessagesAscending(ctx, readGraph, chat.ID, 20)
			if err != nil {
				t.Fatalf("read live formatting raw HTML failed: %v", err)
			}
			joinedHTML := strings.Join(rawHTML, "\n")
			for _, fragment := range []string{"codex-helper control", "def graph(method, path, **kwargs):"} {
				if !strings.Contains(joinedHTML, fragment) {
					t.Fatalf("live formatting raw HTML missing %q in:\n%s", fragment, joinedHTML)
				}
			}
			if strings.Contains(joinedHTML, "```text") {
				t.Fatalf("live formatting raw HTML still contains literal fenced marker:\n%s", joinedHTML)
			}
			if !strings.Contains(joinedHTML, "<codeblock") && !strings.Contains(joinedHTML, "<pre") {
				t.Fatalf("live formatting raw HTML did not preserve code blocks:\n%s", joinedHTML)
			}
			if !liveHTMLCodeBlockContainsAll(joinedHTML,
				"import time, json, os, requests",
				"def graph(method, path, **kwargs):",
				"return r.json() if r.text else {}",
			) {
				t.Fatalf("live formatting raw HTML did not keep the copied Python snippet in one code block; code blocks=%#v\nraw:\n%s", liveHTMLCodeBlockPlainTexts(joinedHTML), joinedHTML)
			}
			for i, text := range got {
				if strings.Contains(text, "Codex") || strings.Contains(text, "User") || strings.Contains(text, "Helper") {
					t.Logf("LIVE_FORMATTING_RECENT[%d]=%s", i, text)
				}
			}
			return
		}
		time.Sleep(10 * time.Second)
	}
	if lastErr != nil {
		t.Fatalf("read live formatting messages failed: %v", lastErr)
	}
	t.Fatalf("live formatting messages were not in expected order.\nwant subsequence: %#v\ngot: %#v", want, got)
}

func TestLiveTeamsOAIMemoryCitationFilterOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_OAI_MEMORY_FILTER")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_OAI_MEMORY_FILTER=1 to send one live Teams citation-filter smoke message")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before any live Teams chat read or send", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	requireLiveWriteOnce(t, "teams-oai-memory-filter")

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

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	graph := NewGraphClient(NewAuthManager(cfg), io.Discard)
	readGraph := NewGraphClient(NewAuthManager(readCfg), io.Discard)
	me, err := graph.Me(ctx)
	if err != nil {
		t.Fatalf("Graph /me failed: %v", err)
	}
	if normalizeLiveHumanName(me.DisplayName) != "jason wei" {
		t.Fatalf("logged-in user displayName %q is not Jason Wei", me.DisplayName)
	}
	nonce := safeLiveMarkerPart(strings.TrimSpace(os.Getenv(liveWriteOnceEnv)))
	title := "💬 Codex Citation Filter Live - " + nonce
	chat, err := graph.CreateSingleMemberGroupChat(ctx, me.ID, title)
	if err != nil {
		t.Fatalf("create live citation-filter chat failed: %v", err)
	}
	requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, chat.ID)
	if refreshed, err := graph.GetChat(ctx, chat.ID); err == nil && strings.TrimSpace(refreshed.WebURL) != "" {
		chat = refreshed
	}
	t.Logf("LIVE_OAI_MEMORY_FILTER_CHAT_ID=%s", chat.ID)
	t.Logf("LIVE_OAI_MEMORY_FILTER_CHAT_URL=%s", chat.WebURL)
	t.Logf("LIVE_OAI_MEMORY_FILTER_CHAT_TOPIC=%s", chat.Topic)

	store, err := teamstore.Open(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatalf("open live citation-filter store: %v", err)
	}
	bridge := newBridgeTestBridge(graph, store, EchoExecutor{})
	bridge.user = me
	text := strings.Join([]string{
		"Live citation filter check " + nonce + ".",
		"",
		"<oai-mem-citation>",
		"<citation_entries>",
		"MEMORY.md:1-3|note=[should not appear in Teams]",
		"</citation_entries>",
		"<rollout_ids>",
		"019d4393-5109-7b10-b5c2-05b2fe8635ba",
		"</rollout_ids>",
		"</oai-mem-citation>",
		"",
		"Visible text after the internal metadata block should remain.",
	}, "\n")
	if err := bridge.queueAndSendOutboxChunks(ctx, "s-live-oai-memory-filter", "turn-live-oai-memory-filter", chat.ID, "final", text); err != nil {
		t.Fatalf("send live citation-filter outbox failed: %v", err)
	}

	deadline := time.Now().Add(90 * time.Second)
	var got []string
	var lastErr error
	for time.Now().Before(deadline) {
		got, lastErr = liveRecentPlainMessagesAscending(ctx, readGraph, chat.ID, 10)
		joined := strings.Join(got, "\n")
		if lastErr == nil &&
			strings.Contains(joined, "🤖 ✅ Codex answer:") &&
			strings.Contains(joined, "Live citation filter check "+nonce) &&
			strings.Contains(joined, "Visible text after the internal metadata block should remain.") &&
			!strings.Contains(joined, "oai-mem-citation") &&
			!strings.Contains(joined, "citation_entries") &&
			!strings.Contains(joined, "MEMORY.md") &&
			!strings.Contains(joined, "rollout_ids") {
			for i, text := range got {
				if strings.Contains(text, "Codex") || strings.Contains(text, "citation filter") {
					t.Logf("LIVE_OAI_MEMORY_FILTER_RECENT[%d]=%s", i, text)
				}
			}
			return
		}
		time.Sleep(10 * time.Second)
	}
	if lastErr != nil {
		t.Fatalf("read live citation-filter messages failed: %v", lastErr)
	}
	t.Fatalf("live citation-filter message did not match expectations; got: %#v", got)
}

func TestLiveTeamsTableStressOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_TABLE_STRESS")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_TABLE_STRESS=1 to send a live Teams native table stress transcript")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before any live Teams chat read or send", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	existingChatID := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_TABLE_STRESS_CHAT_ID"))
	if existingChatID == "" {
		requireLiveWriteOnce(t, "teams-table-stress")
	}

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

	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	graph := NewGraphClient(NewAuthManager(cfg), io.Discard)
	readGraph := NewGraphClient(NewAuthManager(readCfg), io.Discard)
	me, err := graph.Me(ctx)
	if err != nil {
		t.Fatalf("Graph /me failed: %v", err)
	}
	if normalizeLiveHumanName(me.DisplayName) != "jason wei" {
		t.Fatalf("logged-in user displayName %q is not Jason Wei", me.DisplayName)
	}

	nonce := safeLiveMarkerPart(strings.TrimSpace(os.Getenv(liveWriteOnceEnv)))
	if nonce == "" {
		nonce = "readback-only"
	}
	var chat Chat
	if existingChatID != "" {
		requireLiveJasonWeiSingleMemberChat(ctx, t, graph, existingChatID)
		chat, err = graph.GetChat(ctx, existingChatID)
		if err != nil {
			t.Fatalf("get existing live table stress chat failed: %v", err)
		}
	} else {
		title := "💬 Codex Table Stress Live - " + nonce
		chat, err = graph.CreateSingleMemberGroupChat(ctx, me.ID, title)
		if err != nil {
			t.Fatalf("create live table stress chat failed: %v", err)
		}
		requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, chat.ID)
		if refreshed, err := graph.GetChat(ctx, chat.ID); err == nil && strings.TrimSpace(refreshed.WebURL) != "" {
			chat = refreshed
		}
	}
	t.Logf("LIVE_TABLE_STRESS_CHAT_ID=%s", chat.ID)
	t.Logf("LIVE_TABLE_STRESS_CHAT_URL=%s", chat.WebURL)
	t.Logf("LIVE_TABLE_STRESS_CHAT_TOPIC=%s", chat.Topic)

	if existingChatID == "" {
		messages := []TeamsRenderInput{
			{Kind: TeamsRenderHelper, Text: "Native table stress test " + nonce + ". This chat is Jason Wei-only."},
			{Kind: TeamsRenderAssistant, Text: strings.Join([]string{
				"## CASE 01 basic alignment and inline formatting",
				"",
				"| Case | Result | Evidence |",
				"| --- | :---: | ---: |",
				"| **bold** | `ok|pass` | [docs](https://example.com/a?x=1&y=2) |",
				"| unsafe link | [blocked](javascript:alert(1)) | <b>raw html</b> |",
			}, "\n")},
			{Kind: TeamsRenderAssistant, Text: strings.Join([]string{
				"## CASE 02 escaped pipes and code pipes",
				"",
				"| Input | Parsed as | Notes |",
				"| --- | --- | --- |",
				"| a \\| b | one cell | escaped pipe should stay visible |",
				"| `x|y` | one code cell | code span pipe should not split |",
				"| [pipe\\|label](https://example.com/a%7Cb?x=1&y=2) | safe link | label keeps pipe |",
			}, "\n")},
			{Kind: TeamsRenderAssistant, Text: strings.Join([]string{
				"## CASE 03 ragged rows, empty cells, and extra cells",
				"",
				"| A | B | C |",
				"| --- | --- | --- |",
				"| fewer | two |",
				"| extra | one | two | three | four |",
				"| empty-left |  | after empty |",
				"|  | empty first | empty trailing |",
			}, "\n")},
			{Kind: TeamsRenderAssistant, Text: strings.Join([]string{
				"## CASE 04 unicode, emoji, numbers, and long cells",
				"",
				"| Locale | Value | Comment |",
				"| --- | ---: | --- |",
				"| 中文 ✅ | -123.456 | mixed CJK and emoji |",
				"| café é | 0 | combining mark should stay readable |",
				"| long | 999999 | this is a deliberately longer table cell to check wrapping on desktop, web, and mobile clients |",
			}, "\n")},
			{Kind: TeamsRenderAssistant, Text: strings.Join([]string{
				"## CASE 05 local paths and image fallback",
				"",
				"| Kind | Rendered target | Expected behavior |",
				"| --- | --- | --- |",
				"| local repo | [markdown.go](./internal/teams/markdown.go:132) | path should not become an external link |",
				"| windows path | [file](C:\\Users\\jason\\table.md:9) | normalized as a path |",
				"| remote image | ![plot](https://example.com/plot.png) | image is text/link fallback, not inline img |",
				"| local image | ![local](file:///tmp/plot.png) | local path is text/code only |",
			}, "\n")},
			{Kind: TeamsRenderAssistant, Text: strings.Join([]string{
				"## CASE 06 raw HTML and injection attempts",
				"",
				"| Attack | Payload | Expected |",
				"| --- | --- | --- |",
				"| script | <script>alert('x')</script> | visible text only |",
				"| img event | <img src=x onerror=alert(1)> | no image, no handler |",
				"| table break | </td></tr><tr><td>breakout | cannot escape the cell |",
				"| unsafe autolink | <javascript:alert(1)> | not clickable |",
			}, "\n")},
			{Kind: TeamsRenderAssistant, Text: strings.Join([]string{
				"## CASE 07 malformed table and fenced table",
				"",
				"| not | a table |",
				"| --- | nope |",
				"| stays | literal |",
				"",
				"```md",
				"| code | table |",
				"| --- | --- |",
				"| not | parsed |",
				"```",
				"",
				"| after | fence |",
				"| --- | --- |",
				"| parsed | yes |",
			}, "\n")},
		}

		for _, item := range messages {
			html := RenderTeamsHTML(item)
			if item.Kind == TeamsRenderAssistant && !strings.Contains(item.Text, "CASE 07") && !strings.Contains(html, "<table>") {
				t.Fatalf("live table stress item did not render native table:\n%s", html)
			}
			if _, err := graph.SendHTML(ctx, chat.ID, html); err != nil {
				t.Fatalf("send live Teams table stress message kind=%s failed: %v", item.Kind, err)
			}
			time.Sleep(450 * time.Millisecond)
		}

		largeRows := []string{
			"## CASE 08 large table split into several Teams messages",
			"",
			"| Row | Status | Detail | Link |",
			"| --- | --- | --- | --- |",
		}
		for i := 0; i < 36; i++ {
			largeRows = append(largeRows, fmt.Sprintf("| row-%02d | **ok** | value `x|%02d` and <tag-%02d> | [safe](https://example.com/table/%02d) |", i, i, i, i))
		}
		chunks := PlanTeamsHTMLChunks(TeamsRenderInput{
			Kind: TeamsRenderAssistant,
			Text: strings.Join(largeRows, "\n"),
		}, TeamsRenderOptions{
			TargetLimitBytes: 2200,
			HardLimitBytes:   2600,
		})
		if len(chunks) < 2 {
			t.Fatalf("large live table stress input did not split; got %d chunk", len(chunks))
		}
		for _, chunk := range chunks {
			if !strings.Contains(chunk.HTML, "<table>") || !strings.Contains(chunk.HTML, "<th>Row</th>") {
				t.Fatalf("large table chunk lost native table/header:\n%s", chunk.HTML)
			}
			if _, err := graph.SendHTML(ctx, chat.ID, chunk.HTML); err != nil {
				t.Fatalf("send live Teams large table chunk %d/%d failed: %v", chunk.PartIndex, chunk.PartCount, err)
			}
			time.Sleep(450 * time.Millisecond)
		}

		mentionText := strings.TrimSpace(firstNonEmptyString(me.DisplayName, me.UserPrincipalName, "owner"))
		mention := `<at id="0">` + html.EscapeString(mentionText) + `</at>`
		footer := `<p><strong>🔧 Helper:</strong> Native table stress test finished. ` + mention + `</p>`
		if _, err := graph.SendHTMLWithMentions(ctx, chat.ID, footer, []ChatMention{{ID: 0, Text: mentionText, User: me}}); err != nil {
			t.Fatalf("send live Teams table stress footer failed: %v", err)
		}
	}

	want := []string{
		"Native table stress test",
		"CASE 01 basic alignment",
		"CASE 02 escaped pipes",
		"CASE 03 ragged rows",
		"CASE 04 unicode",
		"CASE 05 local paths",
		"CASE 06 raw HTML",
		"CASE 07 malformed table",
		"CASE 08 large table",
		"row-19",
		"row-29",
		"row-35",
		"Native table stress test finished.",
	}
	wantAny := []string{
		"Case\nResult\nEvidence",
		"bold\nok|pass\ndocs (https://example.com/a?x=1&y=2)",
		"blocked (javascript:alert(1))",
		"a | b\none cell",
		"x|y\none code cell",
		"pipe|label (https://example.com/a%7Cb?x=1&y=2)",
		"two | three | four",
		"empty-left\n\nafter empty",
		"中文 ✅\n-123.456",
		"café é\n0",
		"./internal/teams/markdown.go:132",
		"C:/Users/jason/table.md:9",
		"Image: plot (https://example.com/plot.png)",
		"Image: /tmp/plot.png",
		"<script>alert('x')</script>",
		"<img src=x onerror=alert(1)>",
		"</td></tr><tr><td>breakout",
		"<javascript:alert(1)>",
		"| not | a table |",
		"row-35\nok\nvalue x|35 and <tag-35>",
	}
	deadline := time.Now().Add(90 * time.Second)
	var got []string
	var lastErr error
	for time.Now().Before(deadline) {
		got, lastErr = liveRecentPlainMessagesAscending(ctx, readGraph, chat.ID, 50)
		if lastErr == nil && containsOrderedLiveHistoryMessages(got, want) && containsAllLivePlainText(got, wantAny) {
			for i, text := range got {
				if strings.Contains(text, "CASE ") || strings.Contains(text, "Native table stress") {
					t.Logf("LIVE_TABLE_STRESS_RECENT[%d]=%s", i, text)
				}
			}
			return
		}
		time.Sleep(3 * time.Second)
	}
	if lastErr != nil {
		t.Fatalf("read live table stress messages failed: %v", lastErr)
	}
	t.Fatalf("live table stress messages did not contain expected order/content.\nwant ordered subsequence: %#v\nwant content fragments: %#v\ngot: %#v", want, wantAny, got)
}

func resolveLiveReadBenchmarkChat(ctx context.Context, t *testing.T, readGraph *GraphClient, safetyGraph *GraphClient) (string, string) {
	t.Helper()
	if chatID := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_CHAT_ID")); chatID != "" {
		me := requireLiveJasonWeiSingleMemberChat(ctx, t, safetyGraph, chatID)
		chat, err := safetyGraph.GetChat(ctx, chatID)
		if err != nil {
			t.Fatalf("Graph chat lookup failed for benchmark target after safety check as %s: %v", me.DisplayName, err)
		}
		return chat.ID, chat.Topic
	}
	me, err := safetyGraph.Me(ctx)
	if err != nil {
		t.Fatalf("Graph /me safety check failed while selecting benchmark chat: %v", err)
	}
	chats, err := readGraph.ListChats(ctx, 50)
	if err != nil {
		t.Fatalf("list chats while selecting benchmark target: %v", err)
	}
	for _, candidate := range chats {
		chat, err := safetyGraph.GetChat(ctx, candidate.ID)
		if err != nil {
			continue
		}
		members, err := safetyGraph.ListChatMembers(ctx, candidate.ID)
		if err != nil {
			continue
		}
		if err := validateLiveJasonWeiSingleMemberChat(me, chat, members, candidate.ID); err != nil {
			continue
		}
		return chat.ID, chat.Topic
	}
	t.Fatalf("no Jason Wei-only single-member chat found in the latest 50 chats; set CODEX_HELPER_TEAMS_LIVE_CHAT_ID to a known safe chat")
	return "", ""
}

func liveReadBenchmarkIterations() int {
	raw := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_READ_BENCH_ITERATIONS"))
	if raw == "" {
		return 5
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return 5
	}
	if n > 20 {
		return 20
	}
	return n
}

func liveBenchmarkGraphReadOperation(ctx context.Context, t *testing.T, profile string, op string, iterations int, fn func(context.Context) error) {
	t.Helper()
	var samples []time.Duration
	var failures []string
	for i := 0; i < iterations; i++ {
		start := time.Now()
		err := fn(ctx)
		elapsed := time.Since(start)
		if err != nil {
			failures = append(failures, err.Error())
		} else {
			samples = append(samples, elapsed)
		}
		if i+1 < iterations {
			time.Sleep(250 * time.Millisecond)
		}
	}
	if len(samples) == 0 {
		t.Fatalf("READ_BENCH profile=%s op=%s all %d attempt(s) failed; first_error=%q", profile, op, iterations, firstString(failures))
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	total := time.Duration(0)
	for _, sample := range samples {
		total += sample
	}
	t.Logf("READ_BENCH profile=%s op=%s ok=%d failed=%d min=%s p50=%s p95=%s max=%s avg=%s",
		profile,
		op,
		len(samples),
		len(failures),
		formatBenchDuration(samples[0]),
		formatBenchDuration(percentileDuration(samples, 50)),
		formatBenchDuration(percentileDuration(samples, 95)),
		formatBenchDuration(samples[len(samples)-1]),
		formatBenchDuration(total/time.Duration(len(samples))),
	)
	if len(failures) > 0 {
		t.Logf("READ_BENCH_ERRORS profile=%s op=%s first_error=%q", profile, op, firstString(failures))
	}
}

func percentileDuration(samples []time.Duration, percentile int) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	if percentile <= 0 {
		return samples[0]
	}
	if percentile >= 100 {
		return samples[len(samples)-1]
	}
	index := (len(samples)*percentile + 99) / 100
	if index <= 0 {
		index = 1
	}
	if index > len(samples) {
		index = len(samples)
	}
	return samples[index-1]
}

func formatBenchDuration(value time.Duration) string {
	return value.Round(time.Millisecond).String()
}

func shortGraphID(id string) string {
	id = strings.TrimSpace(id)
	if len(id) <= 18 {
		return id
	}
	return id[:8] + "..." + id[len(id)-8:]
}

func firstString(values []string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func accessTokenScopesForLog(token string) string {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return ""
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return ""
	}
	var claims struct {
		Scopes string `json:"scp"`
	}
	if err := json.Unmarshal(payload, &claims); err != nil {
		return ""
	}
	return strings.TrimSpace(claims.Scopes)
}

func TestLiveGraphOutboundAttachmentOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_OUTBOUND_TEST")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_OUTBOUND_TEST=1 and CODEX_HELPER_TEAMS_LIVE_CHAT_ID to upload a live Teams file attachment")
	}
	chatID := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_CHAT_ID"))
	if chatID == "" {
		t.Fatal("CODEX_HELPER_TEAMS_LIVE_CHAT_ID is required for outbound attachment smoke")
	}
	cfg, err := DefaultFileWriteAuthConfig()
	if err != nil {
		t.Fatalf("DefaultFileWriteAuthConfig error: %v", err)
	}
	if _, err := readTokenCache(cfg.CachePath); err != nil {
		t.Fatalf("read Teams file-write token cache %s: %v", cfg.CachePath, err)
	}
	graph := NewGraphClient(NewAuthManager(cfg), io.Discard)
	chatCfg, err := DefaultAuthConfig()
	if err != nil {
		t.Fatalf("DefaultAuthConfig error: %v", err)
	}
	if _, err := readTokenCache(chatCfg.CachePath); err != nil {
		t.Fatalf("read Teams chat token cache %s: %v", chatCfg.CachePath, err)
	}
	chatGraph := NewGraphClient(NewAuthManager(chatCfg), io.Discard)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	requireLiveJasonWeiSingleMemberChat(ctx, t, chatGraph, chatID)
	requireLiveWriteOnce(t, "outbound-attachment")

	tmp := t.TempDir()
	local := filepath.Join(tmp, "codex-helper-live-outbound.txt")
	if err := os.WriteFile(local, []byte("codex-helper live outbound attachment smoke\n"), 0o600); err != nil {
		t.Fatalf("write local smoke file: %v", err)
	}
	file, err := PrepareOutboundAttachment(local, OutboundAttachmentOptions{
		AllowAnyPath:  true,
		GeneratedName: "codex-helper-live-outbound-" + time.Now().UTC().Format("20060102T150405") + ".txt",
	})
	if err != nil {
		t.Fatalf("PrepareOutboundAttachment error: %v", err)
	}
	result, err := SendOutboundAttachment(ctx, graph, chatID, file, OutboundAttachmentOptions{
		Message: "codex-helper live outbound attachment smoke",
	})
	if err != nil {
		t.Fatalf("SendOutboundAttachment error: %v", err)
	}
	if result.Message.ID == "" {
		t.Fatalf("outbound smoke message missing id: %#v", result.Message)
	}
}

func TestLiveBridgeHelperE2EOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_HELPER_E2E")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_HELPER_E2E=1 to create a Jason Wei-only live control/work chat and run helper routing")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before any live Teams chat read, send, mention, or file upload", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	requireLiveWriteOnce(t, "bridge-helper-e2e")

	cfg, err := DefaultAuthConfig()
	if err != nil {
		t.Fatalf("DefaultAuthConfig error: %v", err)
	}
	if _, err := readTokenCache(cfg.CachePath); err != nil {
		t.Fatalf("read Teams chat token cache %s: %v", cfg.CachePath, err)
	}
	ctx := context.Background()

	auth := NewAuthManager(cfg)
	graph := NewGraphClient(auth, io.Discard)
	readCfg, err := DefaultReadAuthConfig()
	if err != nil {
		t.Fatalf("DefaultReadAuthConfig error: %v", err)
	}
	if _, err := readTokenCache(readCfg.CachePath); err != nil {
		t.Fatalf("read Teams read token cache %s: %v", readCfg.CachePath, err)
	}
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
		t.Fatalf("open live helper store: %v", err)
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
	if !strings.HasPrefix(controlChat.Topic, "🏠 ") || !strings.Contains(controlChat.Topic, "Codex Control") {
		t.Fatalf("live control title = %q, want main-chat emoji and control marker", controlChat.Topic)
	}
	if _, err := store.RecordChatPollSuccess(ctx, controlChat.ID, time.Now().UTC(), true, false, 0); err != nil {
		t.Fatalf("seed live control poll cursor failed: %v", err)
	}

	nonce := safeLiveMarkerPart(strings.TrimSpace(os.Getenv(liveWriteOnceEnv)))
	listenOnce := func(label string) error {
		if err := bridge.Listen(ctx, BridgeOptions{
			RegistryPath:            registryPath,
			Store:                   store,
			HelperVersion:           "live-e2e",
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
		for attempts := 0; attempts < 6 && time.Now().Before(deadline); attempts++ {
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
		for attempts := 0; attempts < 6 && time.Now().Before(deadline); attempts++ {
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
	if _, err := graph.SendHTML(ctx, controlChat.ID, "<p>status</p>"); err != nil {
		t.Fatalf("send live status failed: %v", err)
	}
	waitForOutbox("control status", controlChat.ID, "no linked work chats yet")

	workDir := filepath.Join(tmp, "live-work")
	task := "live helper e2e " + nonce
	newCommand := "<p>new " + html.EscapeString(workDir) + " -- " + html.EscapeString(task) + "</p>"
	if _, err := graph.SendHTML(ctx, controlChat.ID, newCommand); err != nil {
		t.Fatalf("send live new failed: %v", err)
	}
	workSession := waitForSession("control new")
	requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, workSession.ChatID)
	if !strings.HasPrefix(workSession.Topic, "💬 ") || !strings.Contains(workSession.Topic, "Codex Work") {
		t.Fatalf("live work title = %q, want work-chat emoji and work marker", workSession.Topic)
	}
	if _, err := store.RecordChatPollSuccess(ctx, workSession.ChatID, time.Now().UTC(), true, false, 0); err != nil {
		t.Fatalf("seed live work poll cursor failed: %v", err)
	}
	waitForOutbox("work ready", workSession.ChatID, "Work chat is ready")

	if _, err := graph.SendHTML(ctx, workSession.ChatID, "<p>helper details</p>"); err != nil {
		t.Fatalf("send live work helper details failed: %v", err)
	}
	waitForOutbox("work details", workSession.ChatID, "Session:", "Working directory:")

	renameTitle := "live renamed " + nonce
	if _, err := graph.SendHTML(ctx, workSession.ChatID, "<p>helper rename "+html.EscapeString(renameTitle)+"</p>"); err != nil {
		t.Fatalf("send live work helper rename failed: %v", err)
	}
	waitForOutbox("work rename", workSession.ChatID, "renamed")
	renamedChat, err := graph.GetChat(ctx, workSession.ChatID)
	if err != nil {
		t.Fatalf("read renamed live work chat failed: %v", err)
	}
	if !strings.HasPrefix(renamedChat.Topic, "💬 ") || !strings.Contains(renamedChat.Topic, renameTitle) {
		t.Fatalf("renamed live work title = %q, want work emoji and requested title %q", renamedChat.Topic, renameTitle)
	}

	prompt := "live worker echo " + nonce
	if _, err := graph.SendHTML(ctx, workSession.ChatID, "<p>"+html.EscapeString(prompt)+"</p>"); err != nil {
		t.Fatalf("send live work prompt failed: %v", err)
	}
	waitForOutbox("work prompt ack", workSession.ChatID, "accepted")
	waitForOutbox("work prompt final", workSession.ChatID, "echo:")
}

func TestLiveBridgePublishExistingTranscriptOrderOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_PUBLISH_HISTORY_E2E")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_PUBLISH_HISTORY_E2E=1 to create one Jason Wei-only live work chat and verify imported history order")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before any live Teams chat read or send", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	requireLiveWriteOnce(t, "publish-history-e2e")

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

	nonce := safeLiveMarkerPart(strings.TrimSpace(os.Getenv(liveWriteOnceEnv)))
	tmp := t.TempDir()
	transcriptPath := filepath.Join(tmp, "session.jsonl")
	sessionID := "live-history-" + nonce
	records := []string{
		`{"id":"u1","role":"user","text":"synthetic live import test user prompt ` + nonce + `"}`,
		`{"id":"a1","role":"assistant","text":"synthetic live import test assistant reply ` + nonce + `"}`,
		`{"id":"s1","type":"status","text":"synthetic live import test codex status ` + nonce + `"}`,
		`{"id":"tool1","type":"tool","text":"synthetic live import test tool/status record ` + nonce + `"}`,
		`{"id":"u2","role":"user","text":"synthetic live import test second user prompt ` + nonce + `"}`,
		`{"id":"a2","role":"assistant","text":"synthetic live import test second assistant reply ` + nonce + `"}`,
		``,
	}
	if err := os.WriteFile(transcriptPath, []byte(strings.Join(records, "\n")), 0o600); err != nil {
		t.Fatalf("write live synthetic transcript: %v", err)
	}
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "live-history",
			Path: tmp,
			Sessions: []codexhistory.Session{{
				SessionID:   sessionID,
				FirstPrompt: "synthetic live import test " + nonce,
				ProjectPath: tmp,
				FilePath:    transcriptPath,
				ModifiedAt:  time.Now(),
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })

	registryPath := filepath.Join(tmp, "registry.json")
	store, err := teamstore.Open(filepath.Join(tmp, "state.json"))
	if err != nil {
		t.Fatalf("open live publish history store: %v", err)
	}
	bridge, err := NewBridge(ctx, auth, registryPath, io.Discard)
	if err != nil {
		t.Fatalf("NewBridge error: %v", err)
	}
	bridge.readGraph = readGraph
	bridge.store = store
	bridge.machine.Label = "live-test-synthetic-history-" + nonce
	if _, err := bridge.publishCodexSession(ctx, DashboardCommandTarget{Raw: sessionID}); err != nil {
		t.Fatalf("live publish existing transcript failed: %v", err)
	}
	if len(bridge.reg.Sessions) != 1 || strings.TrimSpace(bridge.reg.Sessions[0].ChatID) == "" {
		t.Fatalf("live publish did not register one work session: %#v", bridge.reg.Sessions)
	}
	workSession := bridge.reg.Sessions[0]
	requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, workSession.ChatID)
	if !strings.HasPrefix(workSession.Topic, "💬 ") || !strings.Contains(workSession.Topic, "Codex Work") {
		t.Fatalf("live publish work title = %q, want work-chat emoji and marker", workSession.Topic)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load live publish history store: %v", err)
	}
	for _, msg := range state.OutboxMessages {
		if msg.SessionID == workSession.ID && strings.HasPrefix(msg.TurnID, "import:") && msg.MentionOwner {
			t.Fatalf("historical import message unexpectedly mentions owner: %#v", msg)
		}
	}

	want := []string{
		"Helper: Imported Codex session history",
		"User:\nsynthetic live import test user prompt " + nonce,
		"Codex answer:\nsynthetic live import test assistant reply " + nonce,
		"Codex status:\nsynthetic live import test codex status " + nonce,
		"User:\nsynthetic live import test second user prompt " + nonce,
		"Codex answer:\nsynthetic live import test second assistant reply " + nonce,
		"Helper: Import complete. Imported 5 visible history item(s) and skipped 1 background tool event(s)",
	}
	deadline := time.Now().Add(6 * time.Minute)
	var got []string
	var lastErr error
	for attempts := 0; attempts < 12 && time.Now().Before(deadline); attempts++ {
		got, lastErr = liveRecentPlainMessagesAscending(ctx, readGraph, workSession.ChatID, 20)
		if lastErr == nil && containsOrderedLiveHistoryMessages(got, want) {
			for _, msg := range got {
				if strings.Contains(msg, "synthetic live import test tool/status record "+nonce) {
					t.Fatalf("historical tool record should not be imported, got recent messages: %#v", got)
				}
			}
			return
		}
		time.Sleep(15 * time.Second)
	}
	if lastErr != nil {
		t.Fatalf("read live imported history messages failed: %v", lastErr)
	}
	t.Fatalf("live imported history messages were not in expected order.\nwant subsequence: %#v\ngot: %#v", want, got)
}

func TestLiveBridgePublishLongLocalTranscriptOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_PUBLISH_LONG_TRANSCRIPT")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_PUBLISH_LONG_TRANSCRIPT=1 and CODEX_HELPER_TEAMS_LIVE_LONG_TRANSCRIPT_PATH to import one long local transcript into a Jason Wei-only Teams chat")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before any live Teams chat write", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	transcriptPath := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_LONG_TRANSCRIPT_PATH"))
	if transcriptPath == "" {
		t.Fatal("CODEX_HELPER_TEAMS_LIVE_LONG_TRANSCRIPT_PATH is required")
	}
	info, err := os.Stat(transcriptPath)
	if err != nil {
		t.Fatalf("stat long transcript %s: %v", transcriptPath, err)
	}
	requireLiveWriteOnce(t, "publish-long-local-transcript")

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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
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
	transcript, err := ReadSessionTranscript(transcriptPath)
	if err != nil {
		t.Fatalf("ReadSessionTranscript(%s): %v", transcriptPath, err)
	}
	transcriptPath, transcript = limitLiveLongTranscriptForTeams(t, transcriptPath, transcript)

	nonce := safeLiveMarkerPart(strings.TrimSpace(os.Getenv(liveWriteOnceEnv)))
	tmp := t.TempDir()
	registryPath := filepath.Join(tmp, "registry.json")
	store, err := teamstore.Open(filepath.Join(tmp, "state.json"))
	if err != nil {
		t.Fatalf("open live long transcript store: %v", err)
	}
	bridge, err := NewBridge(ctx, auth, registryPath, io.Discard)
	if err != nil {
		t.Fatalf("NewBridge error: %v", err)
	}
	bridge.readGraph = readGraph
	bridge.store = store
	bridge.machine.Label = "live-long-history-" + nonce

	sessionID := bridge.reg.NextSessionID()
	projectPath := filepath.Dir(transcriptPath)
	local := codexhistory.Session{
		SessionID:   firstNonEmptyString(transcript.ThreadID, "live-long-history-"+nonce),
		FirstPrompt: "long history import " + nonce,
		ProjectPath: projectPath,
		FilePath:    transcriptPath,
		ModifiedAt:  time.Now(),
	}
	title := WorkChatTitle(ChatTitleOptions{
		MachineLabel: bridge.machine.Label,
		Profile:      bridge.scope.Profile,
		SessionID:    sessionID,
		Topic:        local.DisplayTitle(),
		Cwd:          projectPath,
	})
	chat, err := bridge.graph.CreateSingleMemberGroupChat(ctx, bridge.user.ID, title)
	if err != nil {
		t.Fatalf("create live long transcript work chat: %v", err)
	}
	requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, chat.ID)

	now := time.Now()
	session := Session{
		ID:            sessionID,
		ChatID:        chat.ID,
		ChatURL:       chat.WebURL,
		Topic:         chat.Topic,
		Status:        "active",
		CodexThreadID: local.SessionID,
		Cwd:           projectPath,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	bridge.reg.Sessions = append(bridge.reg.Sessions, session)
	if err := bridge.ensureDurableSession(ctx, &session); err != nil {
		t.Fatalf("ensure durable live long transcript session: %v", err)
	}
	if err := bridge.importCodexTranscriptToTeams(ctx, session, local); err != nil {
		t.Fatalf("import live long transcript to Teams: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load live long transcript store: %v", err)
	}
	imported, mentionOwner, sent := 0, 0, 0
	for _, msg := range state.OutboxMessages {
		if msg.SessionID != session.ID || !strings.HasPrefix(msg.TurnID, "import:") {
			continue
		}
		imported++
		if msg.SentAt.IsZero() {
			t.Fatalf("import outbox message was not sent: %#v", msg)
		}
		sent++
		if msg.MentionOwner {
			mentionOwner++
		}
	}
	if imported == 0 || sent != imported {
		t.Fatalf("imported=%d sent=%d, want all import messages sent", imported, sent)
	}
	if mentionOwner != 0 {
		t.Fatalf("historical import set MentionOwner on %d message(s); old final answers must not mention the user", mentionOwner)
	}
	recent, err := liveRecentPlainMessagesAscending(ctx, readGraph, session.ChatID, 50)
	if err != nil {
		t.Fatalf("read recent live long transcript messages: %v", err)
	}
	if !strings.Contains(strings.Join(recent, "\n"), "Import complete") {
		t.Fatalf("recent live long transcript messages missing completion marker: %#v", recent)
	}
	t.Logf("LONG_LIVE_IMPORT_SUMMARY chat_url=%s chat_id=%s transcript=%s size=%d parsed_records=%d outbox_sent=%d mention_owner=%d recent_tail=%#v",
		session.ChatURL,
		shortGraphID(session.ChatID),
		transcriptPath,
		info.Size(),
		len(transcript.Records),
		sent,
		mentionOwner,
		recent[max(0, len(recent)-min(len(recent), 5)):],
	)
}

func limitLiveLongTranscriptForTeams(t *testing.T, transcriptPath string, transcript Transcript) (string, Transcript) {
	t.Helper()
	raw := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_LONG_TRANSCRIPT_LIMIT"))
	if raw == "" {
		return transcriptPath, transcript
	}
	limit, err := strconv.Atoi(raw)
	if err != nil || limit <= 0 {
		t.Fatalf("CODEX_HELPER_TEAMS_LIVE_LONG_TRANSCRIPT_LIMIT=%q must be a positive integer", raw)
	}
	importable := 0
	sourceLine := 0
	dedupe := newTranscriptDedupeState()
	for _, record := range transcript.Records {
		if strings.TrimSpace(record.Text) == "" || shouldSkipImportedTranscriptRecord(record) {
			continue
		}
		body := formatTranscriptRecordForTeams(record)
		if strings.TrimSpace(body) == "" || dedupe.shouldSkip(record, body) {
			continue
		}
		importable++
		sourceLine = record.SourceLine
		if importable >= limit {
			break
		}
	}
	if importable < limit || sourceLine <= 0 {
		t.Logf("LONG_LIVE_IMPORT_LIMIT requested=%d available_importable=%d; using full transcript", limit, importable)
		return transcriptPath, transcript
	}

	limitedPath := filepath.Join(t.TempDir(), "limited-session.jsonl")
	if err := copyTranscriptPrefixLines(transcriptPath, limitedPath, sourceLine); err != nil {
		t.Fatalf("write limited transcript prefix: %v", err)
	}
	limited, err := ReadSessionTranscript(limitedPath)
	if err != nil {
		t.Fatalf("ReadSessionTranscript(%s): %v", limitedPath, err)
	}
	t.Logf("LONG_LIVE_IMPORT_LIMIT requested_importable=%d source_line=%d original_records=%d limited_records=%d limited_path=%s",
		limit,
		sourceLine,
		len(transcript.Records),
		len(limited.Records),
		limitedPath,
	)
	return limitedPath, limited
}

func copyTranscriptPrefixLines(src string, dst string, maxLine int) error {
	if maxLine <= 0 {
		return fmt.Errorf("maxLine must be positive")
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer out.Close()
	reader := bufio.NewReaderSize(in, 64*1024)
	for lineNo := 1; lineNo <= maxLine; lineNo++ {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if _, writeErr := out.Write(line); writeErr != nil {
				return writeErr
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
	return nil
}

func TestLiveBridgePublishSubagentMarkerOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_PUBLISH_SUBAGENT_MARKER_E2E")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_PUBLISH_SUBAGENT_MARKER_E2E=1 to create one Jason Wei-only live work chat and verify subagent marker import")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before any live Teams chat read or send", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	requireLiveWriteOnce(t, "publish-subagent-marker-e2e")

	cfg, err := DefaultAuthConfig()
	if err != nil {
		t.Fatalf("DefaultAuthConfig error: %v", err)
	}
	readCfg, err := DefaultReadAuthConfig()
	if err != nil {
		t.Fatalf("DefaultReadAuthConfig error: %v", err)
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

	nonce := safeLiveMarkerPart(strings.TrimSpace(os.Getenv(liveWriteOnceEnv)))
	tmp := t.TempDir()
	parentPath := filepath.Join(tmp, "parent.jsonl")
	childPath := filepath.Join(tmp, "child.jsonl")
	parentID := "live-parent-" + nonce
	childID := "live-child-" + nonce
	parentText := strings.Join([]string{
		`{"timestamp":"2026-05-01T00:00:00Z","type":"session_meta","payload":{"id":"` + parentID + `","cwd":"` + filepath.ToSlash(tmp) + `","source":"cli"}}`,
		`{"timestamp":"2026-05-01T00:01:00Z","type":"response_item","payload":{"id":"live-parent-user","type":"message","role":"user","content":[{"type":"input_text","text":"live parent user ` + nonce + `"}]}}`,
		`{"timestamp":"2026-05-01T00:02:00Z","type":"response_item","payload":{"id":"live-parent-assistant","type":"message","role":"assistant","content":[{"type":"output_text","text":"live parent assistant ` + nonce + `"}]}}`,
	}, "\n") + "\n"
	childText := strings.Join([]string{
		`{"timestamp":"2026-05-01T00:03:00Z","type":"session_meta","payload":{"id":"` + childID + `","cwd":"` + filepath.ToSlash(tmp) + `","source":{"subagent":{"thread_spawn":{"parent_thread_id":"` + parentID + `","depth":1,"agent_nickname":"Reviewer","agent_role":"explorer"}}}}}`,
		`{"timestamp":"2026-05-01T00:04:00Z","type":"response_item","payload":{"id":"live-child-user","type":"message","role":"user","content":[{"type":"input_text","text":"live subagent user ` + nonce + `"}]}}`,
		`{"timestamp":"2026-05-01T00:05:00Z","type":"response_item","payload":{"id":"live-child-assistant","type":"message","role":"assistant","content":[{"type":"output_text","text":"live subagent assistant ` + nonce + `"}]}}`,
	}, "\n") + "\n"
	if err := os.WriteFile(parentPath, []byte(parentText), 0o600); err != nil {
		t.Fatalf("write live parent transcript: %v", err)
	}
	if err := os.WriteFile(childPath, []byte(childText), 0o600); err != nil {
		t.Fatalf("write live child transcript: %v", err)
	}
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "live-subagent",
			Path: tmp,
			Sessions: []codexhistory.Session{{
				SessionID:   parentID,
				FirstPrompt: "live parent user " + nonce,
				ProjectPath: tmp,
				FilePath:    parentPath,
				ModifiedAt:  time.Now(),
				Subagents: []codexhistory.SubagentSession{{
					AgentID:         "thread_spawn",
					ParentSessionID: parentID,
					SessionID:       childID,
					FirstPrompt:     "live subagent user " + nonce,
					FilePath:        childPath,
					CreatedAt:       time.Now().Add(time.Second),
					ModifiedAt:      time.Now().Add(2 * time.Second),
				}},
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })

	registryPath := filepath.Join(tmp, "registry.json")
	store, err := teamstore.Open(filepath.Join(tmp, "state.json"))
	if err != nil {
		t.Fatalf("open live subagent store: %v", err)
	}
	bridge, err := NewBridge(ctx, auth, registryPath, io.Discard)
	if err != nil {
		t.Fatalf("NewBridge error: %v", err)
	}
	bridge.readGraph = readGraph
	bridge.store = store
	bridge.machine.Label = "live-test-subagent-history-" + nonce
	if _, err := bridge.publishCodexSession(ctx, DashboardCommandTarget{Raw: parentID}); err != nil {
		t.Fatalf("live publish subagent transcript failed: %v", err)
	}
	if len(bridge.reg.Sessions) != 1 || strings.TrimSpace(bridge.reg.Sessions[0].ChatID) == "" {
		t.Fatalf("live publish subagent did not register one work session: %#v", bridge.reg.Sessions)
	}
	workSession := bridge.reg.Sessions[0]
	requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, workSession.ChatID)
	want := []string{
		"User:\nlive parent user " + nonce,
		"Codex answer:\nlive parent assistant " + nonce,
		"Helper: Subagent spawned",
		"Subagent: live subagent user " + nonce,
	}
	deadline := time.Now().Add(6 * time.Minute)
	var got []string
	var lastErr error
	for attempts := 0; attempts < 12 && time.Now().Before(deadline); attempts++ {
		got, lastErr = liveRecentPlainMessagesAscending(ctx, readGraph, workSession.ChatID, 20)
		if lastErr == nil && containsOrderedLiveHistoryMessages(got, want) {
			for _, msg := range got {
				if strings.Contains(msg, "User:\nlive subagent user "+nonce) || strings.Contains(msg, "Codex answer:\nlive subagent assistant "+nonce) {
					t.Fatalf("live subagent child transcript should not be expanded, got recent messages: %#v", got)
				}
			}
			return
		}
		time.Sleep(15 * time.Second)
	}
	if lastErr != nil {
		t.Fatalf("read live subagent imported messages failed: %v", lastErr)
	}
	t.Fatalf("live subagent marker messages were not in expected order.\nwant subsequence: %#v\ngot: %#v", want, got)
}

func TestLiveBridgeSendFileDurableOutboxOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_BRIDGE_OUTBOUND_TEST")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_BRIDGE_OUTBOUND_TEST=1 and CODEX_HELPER_TEAMS_LIVE_CHAT_ID to run a live bridge helper file durable outbox smoke")
	}
	chatID := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_CHAT_ID"))
	if chatID == "" {
		t.Fatal("CODEX_HELPER_TEAMS_LIVE_CHAT_ID is required for bridge outbound attachment smoke")
	}
	chatCfg, err := DefaultAuthConfig()
	if err != nil {
		t.Fatalf("DefaultAuthConfig error: %v", err)
	}
	if _, err := readTokenCache(chatCfg.CachePath); err != nil {
		t.Fatalf("read Teams chat token cache %s: %v", chatCfg.CachePath, err)
	}
	fileCfg, err := DefaultFileWriteAuthConfig()
	if err != nil {
		t.Fatalf("DefaultFileWriteAuthConfig error: %v", err)
	}
	if _, err := readTokenCache(fileCfg.CachePath); err != nil {
		t.Fatalf("read Teams file-write token cache %s: %v", fileCfg.CachePath, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	chatGraph := NewGraphClient(NewAuthManager(chatCfg), io.Discard)
	user := requireLiveJasonWeiSingleMemberChat(ctx, t, chatGraph, chatID)
	requireLiveWriteOnce(t, "bridge-send-file")

	root, err := DefaultOutboundRoot()
	if err != nil {
		t.Fatalf("DefaultOutboundRoot error: %v", err)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("mkdir outbound root: %v", err)
	}
	name := "codex-helper-live-bridge-outbound-" + time.Now().UTC().Format("20060102T150405") + ".txt"
	if err := os.WriteFile(filepath.Join(root, name), []byte("codex-helper live bridge helper file durable outbox smoke\n"), 0o600); err != nil {
		t.Fatalf("write outbound smoke file: %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(filepath.Join(root, name)) })

	fileGraph := NewGraphClient(NewAuthManager(fileCfg), io.Discard)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(chatGraph, store, &recordingExecutor{})
	bridge.user = user
	bridge.fileGraph = fileGraph
	bridge.reg.Sessions[0].ChatID = chatID
	bridge.reg.Sessions[0].ChatURL = ""
	bridge.reg.Sessions[0].Topic = "live bridge outbound smoke"

	if err := bridge.handleSessionMessage(ctx, chatID, bridgeTestMessageWithText("live-bridge-send-file-"+name, "helper file "+name), "helper file "+name); err != nil {
		t.Fatalf("bridge helper file live smoke failed: %v", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load store after live smoke: %v", err)
	}
	var sent OutboxMessageForLiveSmoke
	for _, msg := range state.OutboxMessages {
		if msg.Kind == "attachment" {
			sent = OutboxMessageForLiveSmoke{
				ID:             msg.ID,
				Status:         string(msg.Status),
				TeamsMessageID: msg.TeamsMessageID,
				DriveItemID:    msg.DriveItemID,
			}
		}
	}
	if sent.Status != string(teamstore.OutboxStatusSent) || sent.TeamsMessageID == "" || sent.DriveItemID == "" {
		t.Fatalf("live bridge attachment outbox not sent: %#v", sent)
	}
}

type OutboxMessageForLiveSmoke struct {
	ID             string
	Status         string
	TeamsMessageID string
	DriveItemID    string
}

func TestClaimLiveWriteOnceMarker(t *testing.T) {
	base := t.TempDir()
	if claimed, err := claimLiveWriteOnceMarker(base, "bridge/send file", "nonce value"); err != nil || !claimed {
		t.Fatalf("first claim = %v err=%v, want claimed", claimed, err)
	}
	if claimed, err := claimLiveWriteOnceMarker(base, "bridge/send file", "nonce value"); err != nil || claimed {
		t.Fatalf("second claim = %v err=%v, want already claimed without error", claimed, err)
	}
	if claimed, err := claimLiveWriteOnceMarker(base, "bridge/send file", "different nonce"); err != nil || !claimed {
		t.Fatalf("different nonce claim = %v err=%v, want claimed", claimed, err)
	}
}

func requireLiveJasonWeiSingleMemberChat(ctx context.Context, t *testing.T, graph *GraphClient, chatID string) User {
	t.Helper()
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before any live Teams chat read, send, mention, or file upload", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	me, err := graph.Me(ctx)
	if err != nil {
		t.Fatalf("Graph /me safety check failed: %v", err)
	}
	chat, err := graph.GetChat(ctx, chatID)
	if err != nil {
		t.Fatalf("Graph chat safety check failed for configured chat: %v", err)
	}
	members, err := graph.ListChatMembers(ctx, chatID)
	if err != nil {
		t.Fatalf("Graph chat member safety check failed for configured chat: %v", err)
	}
	if err := validateLiveJasonWeiSingleMemberChat(me, chat, members, chatID); err != nil {
		t.Fatalf("refusing live Teams operation: %v", err)
	}
	return me
}

func requireLiveCreatedJasonWeiSingleMemberChat(ctx context.Context, t *testing.T, graph *GraphClient, chatID string) User {
	t.Helper()
	me, err := graph.Me(ctx)
	if err != nil {
		t.Fatalf("Graph /me safety check failed: %v", err)
	}
	chat, err := graph.GetChat(ctx, chatID)
	if err != nil {
		t.Fatalf("Graph chat safety check failed for created chat: %v", err)
	}
	members, err := graph.ListChatMembers(ctx, chatID)
	if err != nil {
		t.Fatalf("Graph chat member safety check failed for created chat: %v", err)
	}
	if err := validateLiveJasonWeiSingleMemberChat(me, chat, members, chatID); err != nil {
		t.Fatalf("refusing live Teams operation: %v", err)
	}
	return me
}

func requireLiveMessageContaining(ctx context.Context, t *testing.T, graph *GraphClient, chatID string, parts ...string) {
	t.Helper()
	messages, err := graph.ListMessages(ctx, chatID, 20)
	if err != nil {
		t.Fatalf("Graph message read failed for %s: %v", chatID, err)
	}
	for _, msg := range messages {
		text := PlainTextFromTeamsHTML(msg.Body.Content)
		matched := true
		for _, part := range parts {
			if !strings.Contains(text, part) {
				matched = false
				break
			}
		}
		if matched {
			return
		}
	}
	t.Fatalf("no live Teams message in %s contained all parts %q among %d recent message(s)", chatID, parts, len(messages))
}

func requireLiveSentOutboxContaining(ctx context.Context, t *testing.T, store *teamstore.Store, chatID string, parts ...string) {
	t.Helper()
	ok, err := liveSentOutboxContains(ctx, store, chatID, parts...)
	if err != nil {
		t.Fatalf("load live helper store failed: %v", err)
	}
	if ok {
		return
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load live helper store failed: %v", err)
	}
	t.Fatalf("no sent live Teams outbox in %s contained all parts %q among %d outbox message(s)", chatID, parts, len(state.OutboxMessages))
}

func liveSentOutboxContains(ctx context.Context, store *teamstore.Store, chatID string, parts ...string) (bool, error) {
	state, err := store.Load(ctx)
	if err != nil {
		return false, err
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
			return true, nil
		}
	}
	return false, nil
}

func liveRecentPlainMessagesAscending(ctx context.Context, graph *GraphClient, chatID string, top int) ([]string, error) {
	messages, err := graph.ListMessages(ctx, chatID, top)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(messages, func(i, j int) bool {
		left := parseGraphTime(messages[i].CreatedDateTime)
		right := parseGraphTime(messages[j].CreatedDateTime)
		if !left.IsZero() && !right.IsZero() && !left.Equal(right) {
			return left.Before(right)
		}
		return messages[i].ID < messages[j].ID
	})
	out := make([]string, 0, len(messages))
	for _, msg := range messages {
		if text := strings.TrimSpace(PlainTextFromTeamsHTML(msg.Body.Content)); text != "" {
			out = append(out, text)
		}
	}
	return out, nil
}

func liveRecentHTMLMessagesAscending(ctx context.Context, graph *GraphClient, chatID string, top int) ([]string, error) {
	messages, err := graph.ListMessages(ctx, chatID, top)
	if err != nil {
		return nil, err
	}
	sort.SliceStable(messages, func(i, j int) bool {
		left := parseGraphTime(messages[i].CreatedDateTime)
		right := parseGraphTime(messages[j].CreatedDateTime)
		if !left.IsZero() && !right.IsZero() && !left.Equal(right) {
			return left.Before(right)
		}
		return messages[i].ID < messages[j].ID
	})
	out := make([]string, 0, len(messages))
	for _, msg := range messages {
		if html := strings.TrimSpace(msg.Body.Content); html != "" {
			out = append(out, html)
		}
	}
	return out, nil
}

func liveHTMLCodeBlockContainsAll(raw string, parts ...string) bool {
	for _, block := range liveHTMLCodeBlockPlainTexts(raw) {
		matched := true
		for _, part := range parts {
			if !strings.Contains(block, part) {
				matched = false
				break
			}
		}
		if matched {
			return true
		}
	}
	return false
}

func liveHTMLCodeBlockPlainTexts(raw string) []string {
	lower := strings.ToLower(raw)
	var out []string
	for search := 0; search < len(raw); {
		rel := strings.Index(lower[search:], "<code")
		if rel < 0 {
			break
		}
		start := search + rel
		nameEnd := start + len("<code")
		if nameEnd < len(lower) {
			next := lower[nameEnd]
			if next != '>' && next != ' ' && next != '\t' && next != '\n' && next != '\r' {
				search = nameEnd
				continue
			}
		}
		tagEndRel := strings.IndexByte(lower[nameEnd:], '>')
		if tagEndRel < 0 {
			break
		}
		contentStart := nameEnd + tagEndRel + 1
		endRel := strings.Index(lower[contentStart:], "</code>")
		if endRel < 0 {
			break
		}
		contentEnd := contentStart + endRel
		if text := strings.TrimSpace(PlainTextFromTeamsHTML(raw[contentStart:contentEnd])); text != "" {
			out = append(out, text)
		}
		search = contentEnd + len("</code>")
	}
	return out
}

func containsOrderedLiveHistoryMessages(got []string, want []string) bool {
	next := 0
	for _, text := range got {
		if next >= len(want) {
			return true
		}
		if strings.Contains(compactLivePlainTextBlankLines(text), compactLivePlainTextBlankLines(want[next])) {
			next++
		}
	}
	return next == len(want)
}

func containsAllLivePlainText(got []string, want []string) bool {
	joined := compactLivePlainTextBlankLines(strings.Join(got, "\n"))
	for _, part := range want {
		if !strings.Contains(joined, compactLivePlainTextBlankLines(part)) {
			return false
		}
	}
	return true
}

func compactLivePlainTextBlankLines(text string) string {
	text = strings.ReplaceAll(text, "\r\n", "\n")
	text = strings.ReplaceAll(text, "\r", "\n")
	var b strings.Builder
	prevNewline := false
	for _, r := range text {
		if r == '\n' {
			if prevNewline {
				continue
			}
			prevNewline = true
			b.WriteRune(r)
			continue
		}
		prevNewline = false
		b.WriteRune(r)
	}
	return b.String()
}

func liveHelperE2ERegistryPath(t *testing.T) string {
	t.Helper()
	if explicit := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_HELPER_E2E_REGISTRY")); explicit != "" {
		return expandHome(explicit)
	}
	base := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_HELPER_E2E_STATE_DIR"))
	if base == "" {
		cache, err := os.UserCacheDir()
		if err != nil {
			t.Fatalf("resolve user cache dir for live helper e2e registry: %v", err)
		}
		base = filepath.Join(cache, "codex-helper", "teams", "live-tests", "helper-e2e")
	}
	return filepath.Join(expandHome(base), "registry.json")
}

func pruneLiveHelperE2ERegistry(t *testing.T, registryPath string) {
	t.Helper()
	reg, err := LoadRegistry(registryPath)
	if err != nil {
		t.Fatalf("load live helper e2e registry %s: %v", registryPath, err)
	}
	controlState := ChatState{}
	if reg.ControlChatID != "" {
		controlState = reg.Chats[reg.ControlChatID]
	}
	reg.Sessions = nil
	reg.Chats = map[string]ChatState{}
	if reg.ControlChatID != "" {
		reg.Chats[reg.ControlChatID] = controlState
	}
	if err := SaveRegistry(registryPath, reg); err != nil {
		t.Fatalf("save pruned live helper e2e registry %s: %v", registryPath, err)
	}
}

func adoptLiveHelperE2EControlChat(ctx context.Context, t *testing.T, graph *GraphClient, readGraph *GraphClient, registryPath string, me User) {
	t.Helper()
	reg, err := LoadRegistry(registryPath)
	if err != nil {
		t.Fatalf("load live helper e2e registry %s: %v", registryPath, err)
	}
	if strings.TrimSpace(reg.ControlChatID) != "" {
		return
	}
	chats, err := readGraph.ListChats(ctx, 50)
	if err != nil {
		t.Fatalf("list live chats while adopting existing helper control chat: %v", err)
	}
	machine := machineLabel()
	for _, candidate := range chats {
		topic := strings.TrimSpace(candidate.Topic)
		if !strings.Contains(topic, "Codex Control") || (machine != "" && !strings.Contains(topic, machine)) {
			continue
		}
		chat, err := graph.GetChat(ctx, candidate.ID)
		if err != nil {
			continue
		}
		members, err := graph.ListChatMembers(ctx, candidate.ID)
		if err != nil {
			continue
		}
		if err := validateLiveJasonWeiSingleMemberChat(me, chat, members, candidate.ID); err != nil {
			continue
		}
		reg.ControlChatID = chat.ID
		reg.ControlChatURL = chat.WebURL
		reg.ControlChatTopic = chat.Topic
		if err := SaveRegistry(registryPath, reg); err != nil {
			t.Fatalf("save adopted live helper e2e control chat: %v", err)
		}
		t.Logf("reusing existing Jason-only live helper control chat %s", chat.ID)
		return
	}
}

func requireLiveHelperE2EControlBindingOrSkip(t *testing.T, registryPath string) {
	t.Helper()
	reg, err := LoadRegistry(registryPath)
	if err != nil {
		t.Fatalf("load live helper e2e registry %s: %v", registryPath, err)
	}
	if strings.TrimSpace(reg.ControlChatID) != "" {
		return
	}
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_ALLOW_CREATE_CONTROL")) == "1" {
		return
	}
	t.Skip("no existing Jason-only live helper control chat was found; set CODEX_HELPER_TEAMS_LIVE_ALLOW_CREATE_CONTROL=1 for the one-time main-chat creation test")
}

func requireLiveWriteOnce(t *testing.T, testName string) {
	t.Helper()
	nonce := strings.TrimSpace(os.Getenv(liveWriteOnceEnv))
	if nonce == "" {
		t.Fatalf("%s=<unique nonce> is required for live Teams writes/uploads; use a new nonce for each intentional live write run", liveWriteOnceEnv)
	}
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_ALLOW_REPEAT_WRITES")) != "1" {
		if testCount := strings.TrimSpace(os.Getenv("TEST_COUNT")); testCount != "" && testCount != "1" {
			t.Fatalf("refusing live Teams writes with TEST_COUNT=%s; run write tests once or set CODEX_HELPER_TEAMS_LIVE_ALLOW_REPEAT_WRITES=1 deliberately", testCount)
		}
	}
	base, err := os.UserCacheDir()
	if err != nil {
		t.Fatalf("resolve user cache dir for live write marker: %v", err)
	}
	claimed, err := claimLiveWriteOnceMarker(filepath.Join(base, "codex-helper", "teams", "live-write-once"), testName, nonce)
	if err != nil {
		t.Fatalf("claim live Teams write marker: %v", err)
	}
	if !claimed {
		t.Skipf("live Teams write nonce %q was already used for %s", nonce, testName)
	}
}

func claimLiveWriteOnceMarker(base string, testName string, nonce string) (bool, error) {
	if strings.TrimSpace(testName) == "" || strings.TrimSpace(nonce) == "" {
		return false, fmt.Errorf("test name and nonce are required")
	}
	if err := os.MkdirAll(base, 0o700); err != nil {
		return false, err
	}
	name := safeLiveMarkerPart(testName) + "-" + safeLiveMarkerPart(nonce) + ".marker"
	path := filepath.Join(base, name)
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer file.Close()
	_, err = fmt.Fprintf(file, "claimed_at=%s\ntest=%s\n", time.Now().UTC().Format(time.RFC3339Nano), testName)
	return true, err
}

func safeLiveMarkerPart(value string) string {
	value = strings.TrimSpace(value)
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	out := strings.Trim(b.String(), "_.-")
	if out == "" {
		return "marker"
	}
	if len(out) > 96 {
		return out[:96]
	}
	return out
}

func validateLiveJasonWeiSingleMemberChat(me User, chat Chat, members []ChatMember, chatID string) error {
	if normalizeLiveHumanName(me.DisplayName) != "jason wei" {
		return fmt.Errorf("logged-in user displayName %q is not Jason Wei", me.DisplayName)
	}
	if strings.TrimSpace(me.ID) == "" {
		return fmt.Errorf("logged-in user has no id")
	}
	if strings.TrimSpace(chat.ID) != strings.TrimSpace(chatID) {
		return fmt.Errorf("chat id mismatch: got %q, want %q", chat.ID, chatID)
	}
	if strings.TrimSpace(chat.ChatType) != "group" {
		return fmt.Errorf("chat %q is %q, not a single-member group chat", chatID, chat.ChatType)
	}
	if len(members) != 1 {
		return fmt.Errorf("chat %q has %d member(s), want exactly 1", chatID, len(members))
	}
	if strings.TrimSpace(members[0].UserID) == "" {
		return fmt.Errorf("chat %q only member has no userId; refusing ambiguous live target", chatID)
	}
	if strings.TrimSpace(members[0].UserID) != strings.TrimSpace(me.ID) {
		return fmt.Errorf("chat %q only member userId %q does not match logged-in user %q", chatID, members[0].UserID, me.ID)
	}
	return nil
}

func normalizeLiveHumanName(value string) string {
	return strings.Join(strings.Fields(strings.ToLower(strings.TrimSpace(value))), " ")
}

func newLiveSmokeGraph(token string) *GraphClient {
	return &GraphClient{
		auth: &staticLiveGraphAuth{token: token},
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
		baseURL:    graphBaseURL,
		maxRetries: defaultGraphRetries,
		backoffMin: defaultBackoffBase,
		backoffMax: defaultBackoffMax,
		sleep:      sleepContext,
		jitter:     jitterDuration,
	}
}

type staticLiveGraphAuth struct {
	token string
}

func (a *staticLiveGraphAuth) AccessToken(context.Context, io.Writer, bool) (string, error) {
	return a.token, nil
}

func (a *staticLiveGraphAuth) RefreshAccessToken(context.Context) (string, error) {
	return "", errors.New("live smoke static token refresh is disabled")
}
