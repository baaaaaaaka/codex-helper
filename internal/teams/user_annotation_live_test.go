package teams

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLiveGraphUserAnnotationEditOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_USER_ANNOTATION_EDIT")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_USER_ANNOTATION_EDIT=1 to create a live single-member Teams chat and test user annotation edit")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before live Teams write tests", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	requireLiveWriteOnce(t, "teams-user-annotation-edit")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	graph := newLiveUserAnnotationEditGraph(t)
	me, err := graph.Me(ctx)
	if err != nil {
		t.Fatalf("Graph /me failed: %v", err)
	}
	nonce := safeLiveMarkerPart(strings.TrimSpace(os.Getenv(liveWriteOnceEnv)))
	chat, err := graph.CreateSingleMemberGroupChat(ctx, me.ID, "Codex User Annotation Edit "+nonce)
	if err != nil {
		t.Fatalf("create live user annotation edit chat failed: %v", err)
	}
	requireLiveCreatedJasonWeiSingleMemberChat(ctx, t, graph, chat.ID)
	if refreshed, err := graph.GetChat(ctx, chat.ID); err == nil && strings.TrimSpace(refreshed.WebURL) != "" {
		chat = refreshed
	}
	t.Logf("LIVE_USER_ANNOTATION_EDIT_CHAT_ID=%s", chat.ID)
	t.Logf("LIVE_USER_ANNOTATION_EDIT_CHAT_URL=%s", chat.WebURL)

	sent, err := graph.SendHTML(ctx, chat.ID, "<p>plain user annotation fixture "+htmlText(nonce)+"</p>")
	if err != nil {
		t.Fatalf("send live annotation fixture failed: %v", err)
	}
	verifyLiveUserAnnotationEdit(ctx, t, graph, chat.ID, sent.ID, me.ID, "graph-sent")
}

func TestLiveGraphExistingUserAnnotationEditOptIn(t *testing.T) {
	if strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_EXISTING_USER_ANNOTATION_EDIT")) != "1" {
		t.Skip("set CODEX_HELPER_TEAMS_LIVE_EXISTING_USER_ANNOTATION_EDIT=1 to PATCH a configured existing Teams message")
	}
	if got := strings.TrimSpace(os.Getenv(liveJasonWeiSafetyAckEnv)); got != liveJasonWeiSafetyAckValue {
		t.Fatalf("%s=%s is required before live Teams write tests", liveJasonWeiSafetyAckEnv, liveJasonWeiSafetyAckValue)
	}
	requireLiveWriteOnce(t, "teams-existing-user-annotation-edit")

	chatID := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_EXISTING_CHAT_ID"))
	messageID := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_EXISTING_MESSAGE_ID"))
	if chatID == "" || messageID == "" {
		t.Fatal("CODEX_HELPER_TEAMS_LIVE_EXISTING_CHAT_ID and CODEX_HELPER_TEAMS_LIVE_EXISTING_MESSAGE_ID are required")
	}
	fallbackText := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_LIVE_EXISTING_TEXT"))

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	graph := newLiveUserAnnotationEditGraph(t)
	me, err := graph.Me(ctx)
	if err != nil {
		t.Fatalf("Graph /me failed: %v", err)
	}
	verifyLiveUserAnnotationEditWithFallbackText(ctx, t, graph, chatID, messageID, me.ID, "existing-user-message", fallbackText)
}

func newLiveUserAnnotationEditGraph(t *testing.T) *GraphClient {
	t.Helper()
	writeCfg, err := DefaultEffectiveAuthConfig()
	if err != nil {
		t.Fatalf("DefaultEffectiveAuthConfig error: %v", err)
	}
	return NewGraphClient(NewAuthManager(writeCfg), io.Discard)
}

func verifyLiveUserAnnotationEdit(ctx context.Context, t *testing.T, graph *GraphClient, chatID string, messageID string, expectedUserID string, label string) {
	t.Helper()
	verifyLiveUserAnnotationEditWithFallbackText(ctx, t, graph, chatID, messageID, expectedUserID, label, "")
}

func verifyLiveUserAnnotationEditWithFallbackText(ctx context.Context, t *testing.T, graph *GraphClient, chatID string, messageID string, expectedUserID string, label string, fallbackText string) {
	t.Helper()
	original, err := graph.GetMessage(ctx, chatID, messageID)
	if err != nil {
		t.Fatalf("%s original GET failed: %v", label, err)
	}
	if got := strings.TrimSpace(original.From.User.ID); expectedUserID != "" && got != "" && !strings.EqualFold(got, expectedUserID) {
		t.Fatalf("%s message author = %q, want current user %q", label, got, expectedUserID)
	}
	annotated, ok := userAnnotatedMessageHTML(original, User{})
	if !ok && strings.TrimSpace(original.Body.Content) == "" && strings.TrimSpace(fallbackText) != "" {
		original.Body.ContentType = "html"
		original.Body.Content = "<p>" + htmlText(fallbackText) + "</p>"
		annotated, ok = userAnnotatedMessageHTML(original, User{})
	}
	if !ok {
		t.Fatalf("%s user annotation payload was not generated; body=%q attachments=%d", label, original.Body.Content, len(original.Attachments))
	}
	t.Logf("LIVE_USER_ANNOTATION_EDIT_TARGET label=%s chat=%s message=%s original_bytes=%d annotated_bytes=%d", label, shortGraphID(chatID), shortGraphID(messageID), len(original.Body.Content), len(annotated))
	if err := graph.UpdateChatMessageHTML(ctx, chatID, messageID, annotated); err != nil {
		t.Fatalf("%s user annotation PATCH failed: %v", label, err)
	}
	after, err := graph.GetMessage(ctx, chatID, messageID)
	if err != nil {
		t.Fatalf("%s after PATCH GET failed: %v", label, err)
	}
	if plain := PlainTextFromTeamsHTML(after.Body.Content); !strings.Contains(plain, incomingUserLabel()+":") {
		t.Fatalf("%s patched message missing user annotation label: %q", label, plain)
	}
}
