package teams

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestBridgeRequeuesPendingAttachmentWhenDriveItemIsGone(t *testing.T) {
	var metadataGETs int
	fileServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, "/me/drive/items/") {
			t.Fatalf("unexpected file Graph request: %s %s", r.Method, r.URL.String())
		}
		metadataGETs++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprint(w, `{"error":{"code":"itemNotFound","message":"item is gone"}}`)
	}))
	defer fileServer.Close()

	chatGraph, sent := newBridgeTestGraph(t)
	store := newBridgeTestStore(t)
	const outboxID = "outbox:stale-drive-item"
	if _, created, err := store.QueueOutbox(context.Background(), teamstore.OutboxMessage{
		ID: outboxID, TeamsChatID: "chat:stale-drive-item", Kind: "attachment",
		Body: "attachment body", AttachmentPath: "/private/staged/stale.bin",
		DriveItemID: "drive:stale", AttachmentMessagePostState: "pending",
		AttachmentUploadSessionPostState: "completed",
	}); err != nil || !created {
		t.Fatalf("queue stale DriveItem created=%v err=%v", created, err)
	}

	bridge := newBridgeTestBridge(chatGraph, store, &recordingExecutor{})
	bridge.fileGraph = &GraphClient{
		auth:       &fakeGraphAuth{token: "access"},
		client:     fileServer.Client(),
		baseURL:    fileServer.URL,
		maxRetries: 0,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}

	// The first pass observes the definitive missing-item response and leaves a
	// short durable retry gate. It must not issue the final Teams POST.
	if err := bridge.flushPendingOutboxForChat(context.Background(), "chat:stale-drive-item"); err == nil {
		t.Fatal("flush with missing DriveItem returned nil; want deferred Graph error")
	}
	current, err := store.OutboxMessageByID(context.Background(), outboxID)
	if err != nil {
		t.Fatalf("reload stale DriveItem outbox: %v", err)
	}
	if current.Status != teamstore.OutboxStatusQueued || current.DriveItemID != "" ||
		current.AttachmentUploadURL != "" || current.AttachmentUploadSessionPostState != "pending" ||
		current.AttachmentMessagePostState != "pending" || current.SendAttemptToken != "" || current.NextAttemptAt.IsZero() {
		t.Fatalf("stale DriveItem recovery = %#v, want queued upload retry with durable gate", current)
	}
	if metadataGETs != 1 || len(*sent) != 0 {
		t.Fatalf("missing DriveItem first pass = metadata GETs:%d chat sends:%d, want 1/0", metadataGETs, len(*sent))
	}

	// The gate prevents an immediate same-cycle/next-call 404 hot loop. A later
	// wake can safely create a new upload session from the staged local file.
	_ = bridge.flushPendingOutboxForChat(context.Background(), "chat:stale-drive-item")
	if metadataGETs != 1 || len(*sent) != 0 {
		t.Fatalf("gated stale DriveItem retry = metadata GETs:%d chat sends:%d, want 1/0", metadataGETs, len(*sent))
	}
}

func TestDefinitiveDriveItemMissingErrorIsNarrowlyScoped(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "item-not-found", err: &GraphStatusError{Method: http.MethodGet, Path: "/me/drive/items/item-1?$select=id", StatusCode: http.StatusNotFound, Code: "itemNotFound"}, want: true},
		{name: "gone-without-envelope", err: &GraphStatusError{Method: http.MethodGet, Path: "/me/drive/items/item-1", StatusCode: http.StatusGone}, want: true},
		{name: "permission-denied-code", err: &GraphStatusError{Method: http.MethodGet, Path: "/me/drive/items/item-1", StatusCode: http.StatusNotFound, Code: "accessDenied"}, want: false},
		{name: "wrong-method", err: &GraphStatusError{Method: http.MethodPost, Path: "/me/drive/items/item-1", StatusCode: http.StatusNotFound, Code: "itemNotFound"}, want: false},
		{name: "wrong-route", err: &GraphStatusError{Method: http.MethodGet, Path: "/chats/chat-1/messages", StatusCode: http.StatusNotFound, Code: "itemNotFound"}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := definitiveDriveItemMissingError(tc.err); got != tc.want {
				t.Fatalf("definitiveDriveItemMissingError = %v, want %v", got, tc.want)
			}
		})
	}
}
