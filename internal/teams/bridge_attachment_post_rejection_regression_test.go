package teams

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// A 401 is an explicit rejection before Graph accepted the attachment
// message. It must reset the exact started boundary and make the row
// replayable; treating it like an unknown result would leave every later
// attempt in marker recovery forever even though there is no remote message to
// reconcile.
func TestBridgeAttachmentMessagePost401ResetsBoundaryAndRetries(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			ctx := context.Background()
			var posts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != "/chats/chat-1/messages" {
					t.Fatalf("unexpected Graph request: %s %s", r.Method, r.URL.String())
				}
				if _, err := io.Copy(io.Discard, r.Body); err != nil {
					t.Fatalf("read attachment POST body: %v", err)
				}
				attempt := posts.Add(1)
				if attempt == 1 {
					http.Error(w, `{"error":{"code":"InvalidAuthenticationToken","message":"expired"}}`, http.StatusUnauthorized)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprint(w, `{"id":"teams-attachment-after-401","messageType":"message"}`)
			}))
			t.Cleanup(server.Close)

			auth := &fakeGraphAuth{token: "old", refreshedToken: "new"}
			graph := &GraphClient{
				auth:       auth,
				client:     server.Client(),
				baseURL:    server.URL,
				maxRetries: 0,
				sleep:      sleepContext,
				jitter:     func(delay time.Duration) time.Duration { return delay },
			}
			store := newBridgeTestStore(t)
			queued, created, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
				ID:                         "outbox:attachment-post-401",
				TeamsChatID:                "chat-1",
				Kind:                       "attachment",
				Body:                       "attachment rejected once",
				AttachmentPath:             "/private/attachment.bin",
				DriveItemID:                "drive-item-401",
				DriveItemName:              "attachment.bin",
				DriveItemETag:              `"{1176C944-0CB9-4304-974C-5837185EFD6A},1"`,
				DriveItemWebDav:            "https://sharepoint.test/attachment.bin",
				AttachmentMessagePostState: "pending",
			})
			if err != nil || !created {
				t.Fatalf("queue attachment created=%v err=%v", created, err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("migrate attachment outbox: %v", err)
				}
			}
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})

			err = bridge.sendQueuedOutboxWithOptions(ctx, queued, outboxSendOptions{IgnoreEarlierOutbox: true})
			if err == nil || !graphUnauthorizedError(err) {
				t.Fatalf("first attachment POST error=%v, want Graph 401", err)
			}
			current, err := store.OutboxMessageByID(ctx, queued.ID)
			if err != nil {
				t.Fatalf("reload attachment after 401: %v", err)
			}
			if current.Status != teamstore.OutboxStatusQueued || current.AttachmentMessagePostState != "pending" || current.AttachmentMessagePostAttemptToken != "" {
				t.Fatalf("attachment after explicit 401 rejection=%#v, want queued/pending without boundary token", current)
			}
			if got := posts.Load(); got != 1 {
				t.Fatalf("attachment POSTs after first rejection=%d, want 1", got)
			}

			if err := bridge.sendQueuedOutboxWithOptions(ctx, current, outboxSendOptions{IgnoreEarlierOutbox: true}); err != nil {
				t.Fatalf("second attachment POST after 401: %v", err)
			}
			current, err = store.OutboxMessageByID(ctx, queued.ID)
			if err != nil {
				t.Fatalf("reload attachment after successful retry: %v", err)
			}
			if got := posts.Load(); got != 2 {
				t.Fatalf("attachment POSTs after retry=%d, want exactly 2", got)
			}
			if current.Status != teamstore.OutboxStatusSent || current.TeamsMessageID != "teams-attachment-after-401" {
				t.Fatalf("attachment after successful retry=%#v, want sent with durable Teams ID", current)
			}
			if got := auth.refreshCalls; got != 1 {
				t.Fatalf("401 token refresh calls=%d, want 1", got)
			}
		})
	}
}

func TestAttachmentPostResetClassifierStaysNarrow(t *testing.T) {
	for name, err := range map[string]error{
		"nil":       nil,
		"transport": errors.New("connection reset"),
		"429":       &GraphStatusError{StatusCode: http.StatusTooManyRequests, Method: http.MethodPost},
		"408":       &GraphStatusError{StatusCode: http.StatusRequestTimeout, Method: http.MethodPost},
		"401":       &GraphStatusError{StatusCode: http.StatusUnauthorized, Method: http.MethodPost},
	} {
		t.Run(name, func(t *testing.T) {
			want := name == "401"
			if got := attachmentPostResponseAllowsPendingReset(err); got != want {
				t.Fatalf("attachment reset classifier=%v, want %v for %v", got, want, err)
			}
			if strings.Contains(name, "429") && attachmentPostResponseAllowsPendingReset(err) {
				t.Fatal("429 must remain in the ambiguous attachment boundary")
			}
		})
	}
}
