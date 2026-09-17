package teams

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestBridgeAttachmentUploadSessionUnknownPOSTIsDurablyNonReplayable(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	root, err := DefaultOutboundRoot()
	if err != nil {
		t.Fatalf("DefaultOutboundRoot: %v", err)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("mkdir outbound root: %v", err)
	}
	path := filepath.Join(root, "large.bin")
	if err := os.WriteFile(path, []byte{0x01, 0x02}, 0o600); err != nil {
		t.Fatalf("write upload file: %v", err)
	}
	size, hash, err := hashOutboundAttachmentFile(path, root, false, maxOutboundAttachmentBytes)
	if err != nil {
		t.Fatalf("hash upload file: %v", err)
	}

	var createSessionPosts atomic.Int32
	var recoveryReads atomic.Int32
	transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
		switch {
		case req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/createUploadSession"):
			createSessionPosts.Add(1)
			// The request crossed the local HTTP boundary, but the caller cannot
			// distinguish a lost response from provider rejection. The durable
			// upload-session marker must therefore make this POST non-replayable.
			return nil, errors.New("connection reset by peer")
		case req.Method == http.MethodGet && strings.Contains(req.URL.Path, "/chats/chat-1/messages"):
			recoveryReads.Add(1)
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(strings.NewReader(`{"value":[]}`)),
				Request:    req,
			}, nil
		default:
			return nil, fmt.Errorf("unexpected Graph request %s %s", req.Method, req.URL.String())
		}
	})
	graph := &GraphClient{
		auth:               &fakeGraphAuth{token: "access"},
		client:             &http.Client{Transport: transport},
		baseURL:            "https://graph.example.test",
		maxRetries:         0,
		transferMaxRetries: 0,
		singlePutMaxBytes:  1,
		transferChunkSize:  2,
		sleep:              sleepContext,
		jitter:             func(d time.Duration) time.Duration { return d },
	}
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
	bridge.fileGraph = graph
	ctx := context.Background()
	if _, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
		ID:                               "outbox:unknown-upload-session",
		TeamsChatID:                      "chat-1",
		Kind:                             "attachment",
		Body:                             "attachment",
		AttachmentPath:                   path,
		AttachmentName:                   "large.bin",
		AttachmentUploadName:             "large.bin",
		AttachmentContentType:            "application/octet-stream",
		AttachmentSize:                   size,
		AttachmentHash:                   hash,
		AttachmentUploadSessionPostState: "pending",
	}); err != nil {
		t.Fatalf("QueueOutbox: %v", err)
	}

	err = bridge.flushPendingOutboxForChat(ctx, "chat-1")
	if err == nil || !errors.Is(err, teamstore.ErrOutboxUploadSessionIndeterminate) {
		t.Fatalf("first flush error = %v, want durable indeterminate upload-session error", err)
	}
	if got := createSessionPosts.Load(); got != 1 {
		t.Fatalf("createUploadSession POSTs after first flush = %d, want 1", got)
	}
	current, err := store.OutboxMessageByID(ctx, "outbox:unknown-upload-session")
	if err != nil {
		t.Fatalf("reload ambiguous outbox: %v", err)
	}
	if current.Status != teamstore.OutboxStatusSending || !teamstore.OutboxSendIsAmbiguous(current) {
		t.Fatalf("ambiguous upload outbox = %#v, want Sending/ambiguous", current)
	}
	if current.AttachmentUploadSessionPostState != "started" || current.AttachmentUploadURL != "" {
		t.Fatalf("upload-session boundary after unknown POST = %#v, want started without URL", current)
	}

	// The durable retry gate also applies to the cold evidence lane. An
	// immediate direct sender call must not turn a long Retry-After into a hot
	// Graph GET loop.
	err = bridge.sendQueuedOutboxWithOptions(ctx, current, outboxSendOptions{IgnoreEarlierOutbox: true})
	if err == nil || !isOutboxDeliveryDeferred(err) {
		t.Fatalf("ambiguous recovery before retry gate = %v, want durable deferral", err)
	}
	if got := createSessionPosts.Load(); got != 1 {
		t.Fatalf("createUploadSession POSTs before ambiguous recovery = %d, want exactly 1", got)
	}
	if got := recoveryReads.Load(); got != 0 {
		t.Fatalf("ambiguous recovery read Graph before its durable retry gate: %d", got)
	}

	// Fast-forward only the durable test clock. Production reaches this state
	// when Retry-After expires; the test must not sleep for minutes just to
	// prove the post-expiry safety property.
	makeBridgeOutboxDueForTest(t, store, current.ID)
	current, err = store.OutboxMessageByID(ctx, current.ID)
	if err != nil {
		t.Fatalf("reload due ambiguous upload outbox: %v", err)
	}
	err = bridge.sendQueuedOutboxWithOptions(ctx, current, outboxSendOptions{IgnoreEarlierOutbox: true})
	if err == nil || !isOutboxDeliveryDeferred(err) {
		t.Fatalf("ambiguous recovery after retry gate = %v, want durable deferral", err)
	}
	if got := recoveryReads.Load(); got == 0 {
		t.Fatal("ambiguous recovery did not perform its bounded Graph evidence read after the gate")
	}
	current, err = store.OutboxMessageByID(ctx, current.ID)
	if err != nil {
		t.Fatalf("reload after ambiguous recovery: %v", err)
	}
	if current.Status != teamstore.OutboxStatusSending || !teamstore.OutboxSendIsAmbiguous(current) || current.AttachmentUploadSessionPostState != "started" {
		t.Fatalf("ambiguous recovery changed durable state: %#v", current)
	}
}

func TestBridgeAttachmentUploadSession429RecordsWriteGateWithoutReplay(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			tmp := t.TempDir()
			isolateTeamsUserDirsForTest(t, tmp)
			root, err := DefaultOutboundRoot()
			if err != nil {
				t.Fatalf("DefaultOutboundRoot: %v", err)
			}
			if err := os.MkdirAll(root, 0o700); err != nil {
				t.Fatalf("mkdir outbound root: %v", err)
			}
			path := filepath.Join(root, "large.bin")
			if err := os.WriteFile(path, []byte{0x01, 0x02}, 0o600); err != nil {
				t.Fatalf("write upload file: %v", err)
			}
			size, hash, err := hashOutboundAttachmentFile(path, root, false, maxOutboundAttachmentBytes)
			if err != nil {
				t.Fatalf("hash upload file: %v", err)
			}

			var createSessionPosts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/createUploadSession") {
					t.Errorf("unexpected Graph request: %s %s", r.Method, r.URL.String())
					http.Error(w, "unexpected Graph request", http.StatusBadRequest)
					return
				}
				createSessionPosts.Add(1)
				w.Header().Set("Retry-After", "60")
				w.Header().Set("X-CXP-RateLimit-Scope", "account")
				http.Error(w, `{"error":{"code":"TooManyRequests","message":"upload session throttle"}}`, http.StatusTooManyRequests)
			}))
			t.Cleanup(server.Close)
			graph := &GraphClient{
				auth:               &fakeGraphAuth{token: "access"},
				client:             server.Client(),
				baseURL:            server.URL,
				maxRetries:         0,
				transferMaxRetries: 0,
				singlePutMaxBytes:  1,
				transferChunkSize:  2,
				sleep: func(context.Context, time.Duration) error {
					t.Fatal("upload-session 429 must not sleep before durable gate")
					return nil
				},
				jitter: func(d time.Duration) time.Duration { return d },
			}
			store := newBridgeTestStore(t)
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			bridge.fileGraph = graph
			ctx := context.Background()
			if _, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
				ID:                               "outbox:upload-session-429",
				TeamsChatID:                      "chat-1",
				Kind:                             "attachment",
				Body:                             "attachment",
				AttachmentPath:                   path,
				AttachmentName:                   "large.bin",
				AttachmentUploadName:             "large.bin",
				AttachmentContentType:            "application/octet-stream",
				AttachmentSize:                   size,
				AttachmentHash:                   hash,
				AttachmentUploadSessionPostState: "pending",
			}); err != nil {
				t.Fatalf("QueueOutbox: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}

			err = bridge.flushPendingOutboxForChat(ctx, "chat-1")
			if err == nil || !isGraphRateLimitError(err) {
				t.Fatalf("flush error = %v, want upload-session 429", err)
			}
			if got := createSessionPosts.Load(); got != 1 {
				t.Fatalf("createUploadSession POSTs = %d, want 1", got)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("Load state: %v", err)
			}
			outbox := state.OutboxMessages["outbox:upload-session-429"]
			if outbox.Status != teamstore.OutboxStatusSending || !teamstore.OutboxSendIsAmbiguous(outbox) || outbox.AttachmentUploadSessionPostState != "started" {
				t.Fatalf("outbox after upload-session 429 = %#v, want Sending/ambiguous started", outbox)
			}
			if limit := state.ChatRateLimits["chat-1"]; !limit.BlockedUntil.After(time.Now()) || limit.PoisonOutboxID != outbox.ID {
				t.Fatalf("chat write gate after upload-session 429 = %#v", limit)
			}
			if limit := state.ChatRateLimits[graphWriteAccountRateLimitKey]; !limit.BlockedUntil.After(time.Now()) || limit.PoisonOutboxID != outbox.ID {
				t.Fatalf("account write gate after upload-session 429 = %#v", limit)
			}
		})
	}
}

func TestBridgeAttachmentChunk429KeepsDurableSessionReplayable(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			tmp := t.TempDir()
			isolateTeamsUserDirsForTest(t, tmp)
			root, err := DefaultOutboundRoot()
			if err != nil {
				t.Fatalf("DefaultOutboundRoot: %v", err)
			}
			if err := os.MkdirAll(root, 0o700); err != nil {
				t.Fatalf("mkdir outbound root: %v", err)
			}
			path := filepath.Join(root, "chunk-429.bin")
			if err := os.WriteFile(path, []byte("01234567"), 0o600); err != nil {
				t.Fatalf("write upload file: %v", err)
			}
			size, hash, err := hashOutboundAttachmentFile(path, root, false, maxOutboundAttachmentBytes)
			if err != nil {
				t.Fatalf("hash upload file: %v", err)
			}

			var creates, puts atomic.Int32
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch {
				case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/createUploadSession"):
					creates.Add(1)
					_, _ = fmt.Fprintf(w, `{"uploadUrl":%q,"expirationDateTime":%q}`, server.URL+"/upload-session", time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano))
				case r.Method == http.MethodPut && r.URL.Path == "/upload-session":
					puts.Add(1)
					w.Header().Set("Retry-After", "60")
					w.Header().Set("X-CXP-RateLimit-Scope", "account")
					http.Error(w, `{"error":{"code":"TooManyRequests","message":"upload chunk throttle"}}`, http.StatusTooManyRequests)
				default:
					t.Errorf("unexpected file Graph request: %s %s", r.Method, r.URL.String())
					http.Error(w, "unexpected Graph request", http.StatusBadRequest)
				}
			}))
			t.Cleanup(server.Close)
			graph := &GraphClient{
				auth: &fakeGraphAuth{token: "access"}, client: server.Client(), baseURL: server.URL,
				maxRetries: 0, transferMaxRetries: 0, singlePutMaxBytes: 1, transferChunkSize: 8,
				sleep: func(context.Context, time.Duration) error {
					t.Fatal("durable upload-session chunk 429 must not sleep in the worker")
					return nil
				},
				jitter: func(d time.Duration) time.Duration { return d },
			}
			store := newBridgeTestStore(t)
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
			bridge.fileGraph = graph
			ctx := context.Background()
			queued, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
				ID: "outbox:upload-chunk-429", TeamsChatID: "chat-1", Kind: "attachment", Body: "attachment",
				AttachmentPath: path, AttachmentName: "chunk-429.bin", AttachmentUploadName: "chunk-429.bin",
				AttachmentContentType: "application/octet-stream", AttachmentSize: size, AttachmentHash: hash,
				AttachmentUploadSessionPostState: "pending",
			})
			if err != nil {
				t.Fatalf("QueueOutbox: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}

			err = bridge.flushPendingOutboxForChat(ctx, "chat-1")
			if err == nil || !isGraphRateLimitError(err) {
				t.Fatalf("chunk 429 flush error = %v, want Graph rate limit", err)
			}
			if creates.Load() != 1 || puts.Load() != 1 {
				t.Fatalf("chunk 429 requests: creates=%d puts=%d, want 1/1", creates.Load(), puts.Load())
			}
			current, err := store.OutboxMessageByID(ctx, queued.ID)
			if err != nil {
				t.Fatalf("reload chunk-429 outbox: %v", err)
			}
			if current.Status != teamstore.OutboxStatusQueued || teamstore.OutboxSendIsAmbiguous(current) {
				t.Fatalf("chunk 429 outbox=%#v, want queued and non-ambiguous", current)
			}
			if current.AttachmentUploadSessionPostState != "ready" || current.AttachmentUploadURL == "" || current.AttachmentUploadOffset != 0 {
				t.Fatalf("chunk 429 lost resume witness: %#v", current)
			}
			state, err := store.Load(ctx)
			if err != nil {
				t.Fatalf("load chunk-429 rate-limit state: %v", err)
			}
			if limit := state.ChatRateLimits["chat-1"]; !limit.BlockedUntil.After(time.Now()) || limit.PoisonOutboxID != queued.ID {
				t.Fatalf("chunk 429 chat write gate=%#v", limit)
			}
		})
	}
}

func TestUploadSessionPostResetRejectsOnlyKnownPreRequestStatuses(t *testing.T) {
	tests := []struct {
		status int
		reset  bool
	}{
		{status: http.StatusBadRequest, reset: false},
		{status: http.StatusForbidden, reset: false},
		{status: http.StatusNotFound, reset: false},
		{status: http.StatusGone, reset: false},
		{status: http.StatusConflict, reset: false},
		{status: http.StatusTooManyRequests, reset: false},
		{status: http.StatusRequestTimeout, reset: false},
		{status: http.StatusTooEarly, reset: false},
		{status: http.StatusInternalServerError, reset: false},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("status_%d", test.status), func(t *testing.T) {
			err := &GraphStatusError{StatusCode: test.status, Message: http.StatusText(test.status)}
			if got := uploadSessionPostResponseAllowsPendingReset(err); got != test.reset {
				t.Fatalf("uploadSessionPostResponseAllowsPendingReset(%d) = %v, want %v", test.status, got, test.reset)
			}
		})
	}
}

func TestBridgeUploadSessionOwnerFenceStopsCreateBeforeHTTP(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	root, err := DefaultOutboundRoot()
	if err != nil {
		t.Fatalf("DefaultOutboundRoot: %v", err)
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("mkdir outbound root: %v", err)
	}
	path := filepath.Join(root, "owner-fenced.bin")
	if err := os.WriteFile(path, []byte{0x01, 0x02}, 0o600); err != nil {
		t.Fatalf("write owner-fenced upload file: %v", err)
	}
	size, hash, err := hashOutboundAttachmentFile(path, root, false, maxOutboundAttachmentBytes)
	if err != nil {
		t.Fatalf("hash owner-fenced upload file: %v", err)
	}

	ctx := context.Background()
	store := newBridgeTestStore(t)
	bridge := newBridgeTestBridge(nil, store, &recordingExecutor{})
	now := time.Now().UTC().Truncate(time.Microsecond)
	lease := teamstore.ControlLease{
		ScopeID: bridge.scope.ID, HolderMachineID: bridge.machine.ID,
		HolderKind: bridge.machine.Kind, Priority: bridge.machine.Priority,
		Generation: 1, Status: teamstore.ControlLeaseStatusActive,
		LeaseUntil: now.Add(time.Hour), LastHeartbeat: now, UpdatedAt: now,
	}
	if err := store.Update(ctx, func(state *teamstore.State) error {
		state.Scope = bridge.scope
		state.Machines[bridge.machine.ID] = bridge.machine
		state.ControlLease = lease
		state.OutboxMessages["outbox:owner-fenced-upload"] = teamstore.OutboxMessage{
			ID: "outbox:owner-fenced-upload", TeamsChatID: "chat-owner-fenced-upload",
			Kind: "attachment", Body: "owner fence", Status: teamstore.OutboxStatusQueued,
			AttachmentPath: path, AttachmentName: "owner-fenced.bin", AttachmentUploadName: "owner-fenced.bin",
			AttachmentContentType: "application/octet-stream", AttachmentSize: size, AttachmentHash: hash,
			AttachmentUploadSessionPostState: "pending", CreatedAt: now, UpdatedAt: now,
		}
		return nil
	}); err != nil {
		t.Fatalf("seed owner-fenced upload: %v", err)
	}
	bridge.setControlLease(lease)

	var createSessionPosts atomic.Int32
	takeoverMachine := bridge.machine
	takeoverMachine.ID += "-takeover"
	var takeoverErr error
	graph := &GraphClient{
		auth: &bridgeTakeoverGraphAuth{
			token: "access",
			onAccess: func() {
				takeoverErr = store.Update(ctx, func(state *teamstore.State) error {
					next := state.ControlLease
					next.HolderMachineID = takeoverMachine.ID
					next.Generation = lease.Generation + 1
					next.LeaseUntil = time.Now().UTC().Add(time.Hour)
					next.LastHeartbeat = time.Now().UTC()
					next.UpdatedAt = time.Now().UTC()
					state.Machines[takeoverMachine.ID] = takeoverMachine
					state.ControlLease = next
					return nil
				})
			},
		},
		client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			if req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/createUploadSession") {
				createSessionPosts.Add(1)
			}
			return nil, fmt.Errorf("unexpected Graph request after owner takeover: %s %s", req.Method, req.URL.String())
		})},
		baseURL: "https://graph.example.test", maxRetries: 0, transferMaxRetries: 0,
		singlePutMaxBytes: 1, transferChunkSize: 2,
		sleep: sleepContext, jitter: func(d time.Duration) time.Duration { return d },
	}
	bridge.graph = graph
	bridge.fileGraph = graph

	outbox, err := store.OutboxMessageByID(ctx, "outbox:owner-fenced-upload")
	if err != nil {
		t.Fatalf("load owner-fenced upload: %v", err)
	}
	err = bridge.sendQueuedOutboxWithOptions(ctx, outbox, outboxSendOptions{})
	if takeoverErr != nil {
		t.Fatalf("durable owner takeover in auth callback: %v", takeoverErr)
	}
	if err == nil || !errors.Is(err, teamstore.ErrControlLeaseNotHeld) {
		t.Fatalf("owner-fenced upload error = %v, want control-lease loss before POST", err)
	}
	if got := createSessionPosts.Load(); got != 0 {
		t.Fatalf("owner-fenced upload issued %d createUploadSession POST(s), want zero", got)
	}
	current, err := store.OutboxMessageByID(ctx, outbox.ID)
	if err != nil {
		t.Fatalf("reload owner-fenced upload: %v", err)
	}
	if current.Status != teamstore.OutboxStatusQueued || teamstore.OutboxSendIsAmbiguous(current) {
		t.Fatalf("owner-fenced upload durable state = %#v, want queued and replayable", current)
	}
	if current.AttachmentUploadSessionPostState != "pending" || current.AttachmentUploadSessionPostAttemptToken != "" {
		t.Fatalf("owner-fenced upload marker = %#v, want pending without boundary witness", current)
	}
}

// A chat-message 429 is a provider response, but it is not proof that a
// gateway did not accept an attachment POST.  Once the durable attachment
// boundary is marked started, a retry must therefore enter exact-marker
// recovery and never reset the row to a replayable pending POST.
func TestBridgeAttachmentMessagePost429DoesNotReplayStartedBoundary(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			ctx := context.Background()
			var posts, recoveryReads atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.Method == http.MethodPost && r.URL.Path == "/chats/chat-1/messages":
					posts.Add(1)
					w.Header().Set("Retry-After", "60")
					http.Error(w, `{"error":{"code":"TooManyRequests","message":"attachment post throttle"}}`, http.StatusTooManyRequests)
				case r.Method == http.MethodGet && r.URL.Path == "/chats/chat-1/messages":
					recoveryReads.Add(1)
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `{"value":[]}`)
				default:
					t.Errorf("unexpected Graph request: %s %s", r.Method, r.URL.String())
					http.Error(w, "unexpected Graph request", http.StatusBadRequest)
				}
			}))
			t.Cleanup(server.Close)
			graph := &GraphClient{
				auth:       &fakeGraphAuth{token: "access"},
				client:     server.Client(),
				baseURL:    server.URL,
				maxRetries: 0,
				sleep:      sleepContext,
				jitter:     func(d time.Duration) time.Duration { return d },
			}
			store := newBridgeTestStore(t)
			attachmentPath := filepath.Join(t.TempDir(), "attachment.bin")
			if err := os.WriteFile(attachmentPath, []byte("attachment"), 0o600); err != nil {
				t.Fatalf("write attachment fixture: %v", err)
			}
			queued, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
				ID:                         "outbox:attachment-post-429",
				TeamsChatID:                "chat-1",
				Kind:                       "attachment",
				Body:                       "attachment post must not replay",
				AttachmentPath:             attachmentPath,
				AttachmentName:             "attachment.bin",
				DriveItemID:                "drive-item-429",
				DriveItemName:              "attachment.bin",
				DriveItemETag:              `"{1176C944-0CB9-4304-974C-5837185EFD6A},1"`,
				DriveItemWebDav:            "https://contoso.sharepoint.com/attachment.bin",
				AttachmentMessagePostState: "pending",
			})
			if err != nil {
				t.Fatalf("QueueOutbox: %v", err)
			}
			if useSQLite {
				if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
					t.Fatalf("MigrateLargeStateToSQLite: %v", err)
				}
			}
			bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})

			err = bridge.sendQueuedOutboxWithOptions(ctx, queued, outboxSendOptions{
				IgnoreEarlierOutbox: true,
				RecordRateLimit:     true,
			})
			if err == nil || !isGraphRateLimitError(err) {
				t.Fatalf("first attachment send error = %v, want Graph 429", err)
			}
			if got := posts.Load(); got != 1 {
				t.Fatalf("attachment POSTs after first 429 = %d, want one", got)
			}
			current, err := store.OutboxMessageByID(ctx, queued.ID)
			if err != nil {
				t.Fatalf("reload attachment after first 429: %v", err)
			}
			if current.Status != teamstore.OutboxStatusSending || current.AttachmentMessagePostState != "started" {
				t.Fatalf("attachment after first 429 = %#v, want sending/started", current)
			}

			// Ignore the durable write gate only to exercise the next local
			// recovery decision immediately.  It must read for an exact marker,
			// then defer; it must not issue a second chat POST.
			err = bridge.sendQueuedOutboxWithOptions(ctx, current, outboxSendOptions{
				IgnoreEarlierOutbox: true,
				RecordRateLimit:     true,
			})
			if err == nil || !isOutboxDeliveryDeferred(err) {
				t.Fatalf("attachment recovery after 429 = %v, want durable deferral", err)
			}
			if got := posts.Load(); got != 1 {
				t.Fatalf("attachment POSTs after recovery = %d, want exactly one", got)
			}
			if recoveryReads.Load() == 0 {
				t.Fatal("attachment recovery after 429 did not perform an evidence read")
			}
			current, err = store.OutboxMessageByID(ctx, queued.ID)
			if err != nil {
				t.Fatalf("reload attachment after recovery: %v", err)
			}
			if current.AttachmentMessagePostState != "started" || current.TeamsMessageID != "" {
				t.Fatalf("attachment recovery changed post boundary = %#v, want started without message ID", current)
			}
		})
	}
}

// An ordinary chat-message POST has the same external ambiguity as the
// attachment message boundary. A provider/gateway may commit it and then
// return an ambiguous 408/409/425/429, so the durable row must stay in
// recovery and a subsequent flush must probe evidence rather than issue a
// second POST.
func TestBridgeOrdinaryMessagePostUnknownHTTPStatusDoesNotReturnToReplayableQueue(t *testing.T) {
	for _, statusCase := range []struct {
		name   string
		status int
	}{{name: "408", status: http.StatusRequestTimeout}, {name: "409", status: http.StatusConflict}, {name: "425", status: http.StatusTooEarly}, {name: "429", status: http.StatusTooManyRequests}} {
		for _, useSQLite := range []bool{false, true} {
			backend := "json"
			if useSQLite {
				backend = "sqlite"
			}
			t.Run(statusCase.name+"/"+backend, func(t *testing.T) {
				ctx := context.Background()
				var posts, recoveryReads atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch {
					case r.Method == http.MethodPost && r.URL.Path == "/chats/chat-1/messages":
						posts.Add(1)
						if statusCase.status == http.StatusTooManyRequests {
							w.Header().Set("Retry-After", "60")
						}
						http.Error(w, fmt.Sprintf(`{"error":{"code":"status-%d","message":"ordinary post unknown outcome"}}`, statusCase.status), statusCase.status)
					case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/chats/chat-1/messages"):
						recoveryReads.Add(1)
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, `{"value":[]}`)
					default:
						t.Errorf("unexpected Graph request: %s %s", r.Method, r.URL.String())
						http.Error(w, "unexpected Graph request", http.StatusBadRequest)
					}
				}))
				t.Cleanup(server.Close)
				graph := &GraphClient{
					auth: &fakeGraphAuth{token: "access"}, client: server.Client(), baseURL: server.URL,
					maxRetries: 0, sleep: sleepContext, jitter: func(d time.Duration) time.Duration { return d },
				}
				store := newBridgeTestStore(t)
				queued, _, err := store.QueueOutbox(ctx, teamstore.OutboxMessage{
					ID: "outbox:ordinary-post-429", TeamsChatID: "chat-1", Kind: "helper",
					Body: "ordinary post must not replay",
				})
				if err != nil {
					t.Fatalf("QueueOutbox: %v", err)
				}
				if useSQLite {
					if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
						t.Fatalf("MigrateLargeStateToSQLite: %v", err)
					}
				}
				bridge := newBridgeTestBridge(graph, store, &recordingExecutor{})
				firstErr := bridge.sendQueuedOutboxWithOptions(ctx, queued, outboxSendOptions{
					IgnoreEarlierOutbox: true, RecordRateLimit: true,
				})
				if firstErr == nil || !definitiveGraphSendFailure(firstErr) {
					t.Fatalf("first ordinary send error = %v, want definitive HTTP %d", firstErr, statusCase.status)
				}
				if got := posts.Load(); got != 1 {
					t.Fatalf("ordinary POSTs after first 429 = %d, want one", got)
				}
				current, err := store.OutboxMessageByID(ctx, queued.ID)
				if err != nil {
					t.Fatalf("reload ordinary outbox: %v", err)
				}
				if current.Status != teamstore.OutboxStatusSending || !teamstore.OutboxSendIsAmbiguous(current) {
					t.Fatalf("ordinary outbox after 429 = %#v, want sending/ambiguous", current)
				}

				// Deliberately omit the rate-limit shortcut so this call exercises the
				// recovery read. It must still terminate without a replacement POST.
				if err := bridge.sendQueuedOutboxWithOptions(ctx, current, outboxSendOptions{
					IgnoreEarlierOutbox: true, RecordRateLimit: true,
				}); err != nil && !isOutboxDeliveryDeferred(err) {
					t.Fatalf("ordinary recovery after 429 = %v, want no replay/deferred result", err)
				}
				if got := posts.Load(); got != 1 {
					t.Fatalf("ordinary POSTs after recovery = %d, want exactly one", got)
				}
				if recoveryReads.Load() == 0 {
					t.Fatal("ordinary 429 recovery did not perform an evidence read")
				}
				current, err = store.OutboxMessageByID(ctx, queued.ID)
				if err != nil {
					t.Fatalf("reload ordinary outbox after recovery: %v", err)
				}
				if current.Status == teamstore.OutboxStatusQueued || current.TeamsMessageID != "" {
					t.Fatalf("ordinary recovery reopened/reidentified outbox unsafely: %#v", current)
				}
			})
		}
	}
}
