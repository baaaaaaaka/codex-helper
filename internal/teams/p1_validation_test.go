package teams

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestP1TranscriptToolStatusArtifactAndHalfWrittenTail(t *testing.T) {
	input := strings.Join([]string{
		`{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"session-p1"}}`,
		`{"timestamp":"2026-01-01T00:01:00Z","type":"response_item","payload":{"id":"tool-1","type":"function_call","name":"shell","arguments":"{\"cmd\":\"go test ./internal/teams\"}"}}`,
		`{"timestamp":"2026-01-01T00:02:00Z","type":"response_item","payload":{"id":"status-1","type":"reasoning","summary":[{"text":"visible summary"}]}}`,
		`{"timestamp":"2026-01-01T00:03:00Z","type":"response_item","payload":{"id":"artifact-1","type":"artifact","path":"reports/final.txt","text":"reports/final.txt"}}`,
		`{"timestamp":"2026-01-01T00:04:00Z","type":"response_item","payload":{"id":"tail","type":"artifact","text":"half"}`,
	}, "\n")

	got, err := ParseCodexTranscript(strings.NewReader(input), TranscriptParseOptions{SourceName: "session.jsonl"})
	if err != nil {
		t.Fatalf("ParseCodexTranscript error: %v", err)
	}
	if len(got.Records) != 3 {
		t.Fatalf("records = %#v, want complete tool/status/artifact records only", got.Records)
	}
	want := []struct {
		kind TranscriptKind
		text string
	}{
		{kind: TranscriptKindTool, text: "go test ./internal/teams"},
		{kind: TranscriptKindStatus, text: "visible summary"},
		{kind: TranscriptKindArtifact, text: "reports/final.txt"},
	}
	for i, item := range want {
		record := got.Records[i]
		if record.Kind != item.kind || !strings.Contains(record.Text, item.text) {
			t.Fatalf("record[%d] = %#v, want kind %q containing %q", i, record, item.kind, item.text)
		}
		rendered := formatTranscriptRecordForTeams(record)
		if strings.Contains(rendered, ":\n") || !strings.Contains(rendered, item.text) {
			t.Fatalf("formatted record[%d] = %q, want role-free text %q", i, rendered, item.text)
		}
	}
	if len(got.Diagnostics) != 1 || got.Diagnostics[0].Kind != "invalid_json" || got.Diagnostics[0].SourceLine != 5 {
		t.Fatalf("diagnostics = %#v, want invalid_json on half-written tail line", got.Diagnostics)
	}
}

func TestP1RendererMatrixEscapesCRLFAndEmptyText(t *testing.T) {
	cases := []struct {
		name  string
		kind  TeamsRenderKind
		label string
	}{
		{name: "user", kind: TeamsRenderUser, label: "🧑‍💻 User"},
		{name: "assistant", kind: TeamsRenderAssistant, label: "🤖 ✅ Codex answer"},
		{name: "helper", kind: TeamsRenderHelper, label: "🔧 Helper"},
		{name: "status", kind: TeamsRenderStatus, label: "📌 Session status"},
		{name: "code", kind: TeamsRenderCode, label: "💻 Code"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RenderTeamsHTML(TeamsRenderInput{
				Surface: TeamsRenderSurfaceTranscript,
				Kind:    tc.kind,
				Text:    "first\r\n<&>\rsecond",
			})
			if !strings.Contains(got, "<strong>"+tc.label+":</strong>") {
				t.Fatalf("rendered HTML missing label %q: %s", tc.label, got)
			}
			if strings.Contains(got, "\r") {
				t.Fatalf("rendered HTML kept carriage return: %q", got)
			}
			if !strings.Contains(got, "&lt;&amp;&gt;") || strings.Contains(got, "<&>") {
				t.Fatalf("rendered HTML did not escape raw text: %s", got)
			}
			if tc.kind != TeamsRenderCode && !strings.Contains(got, "<br>") {
				t.Fatalf("rendered non-code HTML did not preserve line breaks: %s", got)
			}

			empty := RenderTeamsHTML(TeamsRenderInput{Kind: tc.kind})
			if !strings.Contains(empty, "<strong>"+tc.label+":</strong>") {
				t.Fatalf("empty rendered HTML missing label %q: %s", tc.label, empty)
			}
		})
	}
}

func TestP1ArtifactManifestWithoutFileWriteAuthLeavesFinalVisibleAndWarns(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", tmp)
	t.Setenv("CODEX_HELPER_TEAMS_FILE_WRITE_TOKEN_CACHE", filepath.Join(tmp, "missing-file-write-token.json"))

	resultText := "done\n```" + ArtifactManifestFenceInfo + "\n" +
		`{"version":1,"files":[{"path":"artifact.txt","name":"artifact.txt"}]}` + "\n```"
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{
		Text:          resultText,
		CodexThreadID: "thread-1",
		CodexTurnID:   "turn-1",
	}}
	bridge := newBridgeTestBridge(graph, store, executor)

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessage("artifact-no-auth"), "make artifact"); err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if len(*sent) != 3 {
		t.Fatalf("sent messages = %#v, want ack, visible final, and auth warning", *sent)
	}
	if strings.Contains((*sent)[1].Content, ArtifactManifestFenceInfo) || !strings.Contains((*sent)[1].Content, "done") {
		t.Fatalf("final response should hide helper artifact manifest and remain visible: %#v", *sent)
	}
	if !strings.Contains((*sent)[2].Content, "artifact manifest detected") || !strings.Contains((*sent)[2].Content, "not authenticated") {
		t.Fatalf("artifact warning = %#v, want unauthenticated helper warning", (*sent)[2])
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if len(state.ArtifactRecords) != 0 {
		t.Fatalf("artifact records = %#v, want none without file-write auth", state.ArtifactRecords)
	}
}

func TestP1TranscriptFormattingStripsArtifactManifestBlocks(t *testing.T) {
	text := "done\n```" + ArtifactManifestFenceInfo + "\n" +
		`{"version":1,"files":[{"path":"artifact.txt","name":"artifact.txt"}]}` + "\n```"
	rendered := formatTranscriptRecordForTeams(TranscriptRecord{Kind: TranscriptKindAssistant, Text: text})
	if !strings.Contains(rendered, "done") || strings.Contains(rendered, ArtifactManifestFenceInfo) || strings.Contains(rendered, `"files"`) {
		t.Fatalf("formatted transcript leaked artifact manifest: %q", rendered)
	}
	onlyManifest := formatTranscriptRecordForTeams(TranscriptRecord{Kind: TranscriptKindAssistant, Text: "```" + ArtifactManifestFenceInfo + "\n{}\n```"})
	if onlyManifest != "" {
		t.Fatalf("manifest-only transcript record should be hidden, got %q", onlyManifest)
	}
}

func TestP1TranscriptFormattingStripsOAIMemoryCitationBlocks(t *testing.T) {
	text := strings.Join([]string{
		"visible answer",
		"",
		"<oai-mem-citation>",
		"<citation_entries>",
		"MEMORY.md:1-3|note=[confirmed codex-helper repo context]",
		"</citation_entries>",
		"<rollout_ids>",
		"019d4393-5109-7b10-b5c2-05b2fe8635ba",
		"</rollout_ids>",
		"</oai-mem-citation>",
	}, "\n")
	rendered := formatTranscriptRecordForTeams(TranscriptRecord{Kind: TranscriptKindAssistant, Text: text})
	if rendered != "visible answer" {
		t.Fatalf("formatted assistant transcript = %q, want visible answer only", rendered)
	}
	userRendered := formatTranscriptRecordForTeams(TranscriptRecord{Kind: TranscriptKindUser, Text: text})
	if !strings.Contains(userRendered, "<oai-mem-citation>") || !strings.Contains(userRendered, "MEMORY.md") {
		t.Fatalf("formatted user transcript should preserve quoted citation text, got %q", userRendered)
	}
	html := renderFinalOutboxBodyHTML(teamstore.OutboxMessage{Kind: "final", Body: text})
	plain := PlainTextFromTeamsHTML(html)
	if !strings.Contains(plain, "visible answer") {
		t.Fatalf("rendered final outbox lost visible answer: %q", plain)
	}
	for _, forbidden := range []string{"oai-mem-citation", "citation_entries", "MEMORY.md", "rollout_ids"} {
		if strings.Contains(plain, forbidden) {
			t.Fatalf("rendered final outbox leaked %q: %q", forbidden, plain)
		}
	}
}

func TestP1TranscriptFormattingStripsHiddenHelperPrompt(t *testing.T) {
	rendered := formatTranscriptRecordForTeams(TranscriptRecord{
		Kind: TranscriptKindUser,
		Text: TeamsCodexPrompt("visible task"),
	})
	if rendered != "visible task" {
		t.Fatalf("formatted transcript = %q, want only visible user text", rendered)
	}
	if strings.Contains(rendered, "teams-outbound") || strings.Contains(rendered, ArtifactManifestFenceInfo) {
		t.Fatalf("formatted transcript leaked helper prompt: %q", rendered)
	}
}

func TestP1ArtifactManifestRejectedAfterFinalWhenAuthExists(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", tmp)
	cfg, err := DefaultFileWriteAuthConfig()
	if err != nil {
		t.Fatalf("DefaultFileWriteAuthConfig error: %v", err)
	}
	if err := writeTokenCache(cfg.CachePath, TokenCache{
		AccessToken:  "access",
		RefreshToken: "refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}); err != nil {
		t.Fatalf("write token cache: %v", err)
	}

	resultText := "done\n```" + ArtifactManifestFenceInfo + "\n" +
		`{"version":1,"files":[{"path":"../secret.txt","name":"secret.txt"}]}` + "\n```"
	graph, sent := newBridgeTestGraph(t)
	fileGraph, fileSent := newOutboundAttachmentGraph(t)
	store := newBridgeTestStore(t)
	executor := &recordingExecutor{result: ExecutionResult{
		Text:          resultText,
		CodexThreadID: "thread-1",
		CodexTurnID:   "turn-1",
	}}
	bridge := newBridgeTestBridge(graph, store, executor)
	bridge.fileGraph = fileGraph

	if err := bridge.handleSessionMessage(context.Background(), "chat-1", bridgeTestMessage("artifact-rejected"), "make artifact"); err != nil {
		t.Fatalf("handleSessionMessage error: %v", err)
	}
	if len(*sent) != 3 {
		t.Fatalf("sent messages = %#v, want ack, visible final, and rejection warning", *sent)
	}
	if strings.Contains((*sent)[1].Content, ArtifactManifestFenceInfo) || !strings.Contains((*sent)[1].Content, "done") {
		t.Fatalf("final response should hide helper artifact manifest and remain visible: %#v", *sent)
	}
	if !strings.Contains((*sent)[2].Content, "artifact manifest 1 rejected") || !strings.Contains((*sent)[2].Content, "path must stay") {
		t.Fatalf("artifact rejection warning = %#v", (*sent)[2])
	}
	if len(*fileSent) != 0 {
		t.Fatalf("file upload messages = %#v, want none for rejected manifest", *fileSent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if len(state.ArtifactRecords) != 0 {
		t.Fatalf("artifact records = %#v, want none for rejected manifest", state.ArtifactRecords)
	}
}

func TestP1SyncLinkedTranscriptIgnoresHalfWrittenTailAndCheckpointsCompleteRecord(t *testing.T) {
	transcriptPath := filepath.Join(t.TempDir(), "session.jsonl")
	initial := `{"id":"old","role":"assistant","text":"old answer"}` + "\n"
	if err := os.WriteFile(transcriptPath, []byte(initial), 0o600); err != nil {
		t.Fatalf("write transcript: %v", err)
	}
	prevDiscover := discoverCodexProjectsForTeams
	discoverCodexProjectsForTeams = func(_ context.Context, _ string) ([]codexhistory.Project, error) {
		return []codexhistory.Project{{
			Key:  "p1",
			Path: "/home/user/project/alpha",
			Sessions: []codexhistory.Session{{
				SessionID:   "thread-1",
				ProjectPath: "/home/user/project/alpha",
				FilePath:    transcriptPath,
				ModifiedAt:  time.Now(),
			}},
		}}, nil
	}
	t.Cleanup(func() { discoverCodexProjectsForTeams = prevDiscover })

	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	session := bridge.reg.SessionByChatID("chat-1")
	session.CodexThreadID = "thread-1"
	if err := bridge.ensureDurableSession(context.Background(), session); err != nil {
		t.Fatalf("ensureDurableSession error: %v", err)
	}

	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("initial sync error: %v", err)
	}
	if len(*sent) != 0 {
		t.Fatalf("initial sync should seed checkpoint without sending: %#v", *sent)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	checkpointID := transcriptCheckpointID("s001")
	if got := state.ImportCheckpoints[checkpointID].LastRecordID; got != "source:old" {
		t.Fatalf("seed checkpoint = %q, want source:old", got)
	}

	updated := initial +
		`{"id":"new","role":"assistant","text":"complete local answer"}` + "\n" +
		`{"id":"tail","role":"assistant","text":"half-written"`
	if err := os.WriteFile(transcriptPath, []byte(updated), 0o600); err != nil {
		t.Fatalf("write updated transcript: %v", err)
	}

	if err := bridge.syncLinkedTranscripts(context.Background()); err != nil {
		t.Fatalf("second sync error: %v", err)
	}
	if len(*sent) != 1 {
		t.Fatalf("sent messages = %#v, want only the complete appended record", *sent)
	}
	plain := PlainTextFromTeamsHTML((*sent)[0].Content)
	if !strings.Contains(plain, "complete local answer") || strings.Contains(plain, "half-written") {
		t.Fatalf("synced transcript text = %q, want complete record without bad tail", plain)
	}
	state, err = store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	checkpoint := state.ImportCheckpoints[checkpointID]
	if checkpoint.LastRecordID != "new" || checkpoint.LastSourceLine != 2 {
		t.Fatalf("checkpoint = %#v, want new at line 2", checkpoint)
	}
}

func TestP1ExpiredRateLimitBlockRecoversAndSendsQueuedOutbox(t *testing.T) {
	graph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})

	outbox, _, err := store.QueueOutbox(context.Background(), teamstore.OutboxMessage{
		ID:          "outbox:recover-after-rate-limit",
		TeamsChatID: "chat-1",
		Kind:        "helper",
		Body:        "send after rate-limit window",
	})
	if err != nil {
		t.Fatalf("QueueOutbox error: %v", err)
	}
	if _, err := store.SetChatRateLimit(context.Background(), "chat-1", time.Now().Add(-time.Minute), "429"); err != nil {
		t.Fatalf("SetChatRateLimit error: %v", err)
	}

	if err := bridge.sendQueuedOutbox(context.Background(), outbox); err != nil {
		t.Fatalf("sendQueuedOutbox error: %v", err)
	}
	if len(*sent) != 1 || !strings.Contains((*sent)[0].Content, "send after rate-limit window") {
		t.Fatalf("sent messages = %#v, want recovered queued outbox", *sent)
	}
	if _, ok, err := store.ChatRateLimit(context.Background(), "chat-1"); err != nil || ok {
		t.Fatalf("expired chat rate limit remains ok=%v err=%v", ok, err)
	}
	state, err := store.Load(context.Background())
	if err != nil {
		t.Fatalf("Load error: %v", err)
	}
	if got := state.OutboxMessages[outbox.ID].Status; got != teamstore.OutboxStatusSent {
		t.Fatalf("outbox status = %q, want sent", got)
	}
}

func TestP1DerivedCacheRebuildLeavesDurableCheckpointUsable(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	policy := DerivedCachePolicy{SchemaVersion: 2, TTL: time.Minute, StaleWhileRevalidate: time.Minute}
	decision := policy.Evaluate(now, CacheSourceSnapshot{
		Fingerprint: "source:new",
		MTime:       now,
	}, DerivedCacheRecord{
		SchemaVersion:     2,
		SourceFingerprint: "source:old",
		SourceMTime:       now,
		GeneratedAt:       now.Add(-10 * time.Second),
	})
	if !decision.Rebuild || decision.Reason != CacheReasonSourceFingerprintMismatch {
		t.Fatalf("cache decision = %#v, want rebuild on source fingerprint mismatch", decision)
	}

	checkpoint := DurableCheckpoint{
		Key:       transcriptCheckpointID("s001"),
		SourceID:  "thread-1",
		Cursor:    "source:new",
		Sequence:  2,
		UpdatedAt: now,
	}
	if got := EvaluateDurableCheckpoint(checkpoint); !got.UseCheckpoint || got.Reason != "usable" {
		t.Fatalf("checkpoint decision = %#v, want durable checkpoint usable across cache rebuild", got)
	}

	if CacheSourceFingerprint("a", "bc") == CacheSourceFingerprint("ab", "c") {
		t.Fatal("cache fingerprint should be length-delimited, not simple concatenation")
	}
}
