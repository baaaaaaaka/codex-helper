package teams

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestSimulatedLiveTeamsRenderCasesRoundTripThroughGraphCI(t *testing.T) {
	graph, captured := newSimulatedLiveGraph(t)
	ctx := context.Background()

	formattingHTML := RenderTeamsHTML(TeamsRenderInput{
		Surface: TeamsRenderSurfaceOutbox,
		Kind:    TeamsRenderAssistant,
		Text: strings.Join([]string{
			"## SIM-FORMAT summary",
			"",
			"- **bold** `inline <tag>`",
			"",
			"```go",
			`fmt.Println("<teams>")`,
			"```",
		}, "\n"),
	})
	freezeHTML := renderTeamsFreezeNoticeHTML(
		"https://teams.microsoft.com/l/chat/control-chat/0",
		"r SIM-FREEZE",
		"SIM-FREEZE safe line",
	)
	oaiFilteredHTML := renderFinalOutboxBodyHTML(teamstore.OutboxMessage{
		Kind: "final",
		Body: strings.Join([]string{
			"SIM-OAI visible answer",
			"",
			"<oai-mem-citation>",
			"<citation_entries>",
			"MEMORY.md:1-2|note=[hidden]",
			"</citation_entries>",
			"<rollout_ids>",
			"00000000-0000-0000-0000-000000000000",
			"</rollout_ids>",
			"</oai-mem-citation>",
		}, "\n"),
	})
	for _, tc := range []struct {
		name string
		html string
	}{
		{name: "formatting", html: formattingHTML},
		{name: "freeze", html: freezeHTML},
		{name: "oai-memory-filter", html: oaiFilteredHTML},
	} {
		if _, err := graph.SendHTML(ctx, "chat-1", tc.html); err != nil {
			t.Fatalf("%s SendHTML error: %v", tc.name, err)
		}
	}

	var tableRows []string
	tableRows = append(tableRows, "| Case | Value | Notes |")
	tableRows = append(tableRows, "| --- | --- | --- |")
	for i := 0; i < 80; i++ {
		tableRows = append(tableRows, fmt.Sprintf("SIM-TABLE row %02d | `a|b` <x> | [safe](https://example.com/%02d)", i, i))
	}
	tableChunks := PlanTeamsHTMLChunks(TeamsRenderInput{
		Kind: TeamsRenderAssistant,
		Text: strings.Join(tableRows, "\n"),
	}, TeamsRenderOptions{
		TargetLimitBytes: 1400,
		HardLimitBytes:   1800,
	})
	if len(tableChunks) < 2 {
		t.Fatalf("simulated live table stress should split into multiple Teams messages, got %d", len(tableChunks))
	}
	for i, chunk := range tableChunks {
		if chunk.ByteLength > 1800 {
			t.Fatalf("table chunk %d byte length = %d, want <= 1800", i, chunk.ByteLength)
		}
		if _, err := graph.SendHTML(ctx, "chat-1", chunk.HTML); err != nil {
			t.Fatalf("table chunk %d SendHTML error: %v", i, err)
		}
	}

	messages, err := graph.ListMessages(ctx, "chat-1", 50)
	if err != nil {
		t.Fatalf("ListMessages error: %v", err)
	}
	if got, want := len(messages), 3+len(tableChunks); got != want {
		t.Fatalf("captured messages = %d, want %d; raw=%#v", got, want, captured())
	}

	var plain strings.Builder
	for _, msg := range messages {
		if msg.Body.ContentType != "html" {
			t.Fatalf("captured content type = %q, want html", msg.Body.ContentType)
		}
		plain.WriteString(PlainTextFromTeamsHTML(msg.Body.Content))
		plain.WriteByte('\n')
	}
	joined := plain.String()
	for _, want := range []string{
		"SIM-FORMAT summary",
		`fmt.Println("<teams>")`,
		"SIM-FREEZE safe line",
		"Step 2: Send: r SIM-FREEZE",
		"SIM-OAI visible answer",
		"SIM-TABLE row 00",
		"SIM-TABLE row 79",
		"safe (https://example.com/79)",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("simulated live Graph round-trip missing %q in:\n%s", want, joined)
		}
	}
	for _, forbidden := range []string{
		"oai-mem-citation",
		"MEMORY.md",
		"rollout_ids",
		"https://teams.microsoft.com/l/chat/control-chat/0",
	} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("simulated live Graph round-trip leaked %q in:\n%s", forbidden, joined)
		}
	}
}

func TestSimulatedLiveHelperE2EControlWorkAndStatusCI(t *testing.T) {
	var createdTopic string
	graph, sent := newBridgeCreateChatGraph(t, &createdTopic)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{
		Text:          "echo: SIM-HELPER prompt",
		CodexThreadID: "thread-sim-helper",
		CodexTurnID:   "turn-sim-helper",
	}}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.reg.Sessions = nil

	workDir := t.TempDir()
	if err := bridge.handleControlMessage(context.Background(), bridgeTestMessageWithText("control-new", "new "+workDir+" -- SIM-HELPER task"), "new "+workDir+" -- SIM-HELPER task"); err != nil {
		t.Fatalf("control new error: %v", err)
	}
	session := bridge.reg.SessionByID("s001")
	if session == nil || session.ChatID != "work-chat" || session.Cwd != workDir {
		t.Fatalf("created session mismatch: %#v", bridge.reg.Sessions)
	}
	if createdTopic == "" || strings.Contains(createdTopic, workDir) {
		t.Fatalf("created topic = %q, want non-empty safe title without full path", createdTopic)
	}

	if err := bridge.handleSessionMessage(context.Background(), session.ChatID, bridgeTestMessageWithText("work-prompt", "SIM-HELPER prompt"), "SIM-HELPER prompt"); err != nil {
		t.Fatalf("work prompt error: %v", err)
	}
	if err := bridge.handleSessionMessage(context.Background(), session.ChatID, bridgeTestMessageWithText("work-status", "helper status"), "helper status"); err != nil {
		t.Fatalf("work status error: %v", err)
	}

	joined := sentPlainJoined(*sent)
	requirePlainTextInOrder(t, joined,
		"Work chat created: s001",
		"Codex will start automatically",
		"search for: s001",
		"Codex is working. Request accepted.",
		"echo: SIM-HELPER prompt",
		"STATUS: Work chat",
	)
	if got := executor.prompts; len(got) != 1 || !strings.Contains(got[0], "SIM-HELPER prompt") || !strings.Contains(got[0], ArtifactManifestFenceInfo) {
		t.Fatalf("executor prompts = %#v, want one prompt with artifact handoff instructions", got)
	}

	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load state error: %v", err)
	}
	var completed int
	for _, turn := range state.Turns {
		if turn.SessionID == session.ID && turn.Status == teamstore.TurnStatusCompleted {
			completed++
		}
	}
	if completed != 1 {
		t.Fatalf("completed turns for %s = %d, want 1; turns=%#v", session.ID, completed, state.Turns)
	}
}

func newSimulatedLiveGraph(t *testing.T) (*GraphClient, func() []ChatMessage) {
	t.Helper()
	var mu sync.Mutex
	var messages []ChatMessage
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/chats/chat-1/messages":
			var payload struct {
				Body struct {
					ContentType string `json:"contentType"`
					Content     string `json:"content"`
				} `json:"body"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode message payload: %v", err)
			}
			if payload.Body.ContentType != "html" {
				t.Fatalf("contentType = %q, want html", payload.Body.ContentType)
			}
			mu.Lock()
			id := fmt.Sprintf("message-%02d", len(messages)+1)
			msg := ChatMessage{ID: id, ChatID: "chat-1", CreatedDateTime: "2026-05-14T00:00:00Z", MessageType: "message"}
			msg.Body.ContentType = payload.Body.ContentType
			msg.Body.Content = payload.Body.Content
			messages = append([]ChatMessage{msg}, messages...)
			mu.Unlock()
			if err := json.NewEncoder(w).Encode(msg); err != nil {
				t.Fatalf("encode message response: %v", err)
			}
		case r.Method == http.MethodGet && r.URL.Path == "/chats/chat-1/messages":
			mu.Lock()
			out := append([]ChatMessage(nil), messages...)
			mu.Unlock()
			if err := json.NewEncoder(w).Encode(map[string]any{"value": out}); err != nil {
				t.Fatalf("encode messages response: %v", err)
			}
		default:
			t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
		}
	}))
	t.Cleanup(server.Close)
	return newTestGraphClient(&fakeGraphAuth{token: "access"}, server, nil), func() []ChatMessage {
		mu.Lock()
		defer mu.Unlock()
		return append([]ChatMessage(nil), messages...)
	}
}
