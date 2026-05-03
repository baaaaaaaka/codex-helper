package teams

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type fakeGraphAuth struct {
	token          string
	refreshedToken string
	accessCalls    int
	refreshCalls   int
}

func (a *fakeGraphAuth) AccessToken(context.Context, io.Writer, bool) (string, error) {
	a.accessCalls++
	return a.token, nil
}

func (a *fakeGraphAuth) RefreshAccessToken(context.Context) (string, error) {
	a.refreshCalls++
	a.token = a.refreshedToken
	return a.token, nil
}

func TestGraphRefreshesTokenAndRetriesOnceOnUnauthorized(t *testing.T) {
	auth := &fakeGraphAuth{token: "old-access", refreshedToken: "new-access"}
	var attempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		attempts++
		if got := req.URL.String(); got != "/me?$select=id,displayName,userPrincipalName" {
			t.Fatalf("unexpected request path: %s", got)
		}
		switch attempts {
		case 1:
			if got := req.Header.Get("Authorization"); got != "Bearer old-access" {
				t.Fatal("unexpected first authorization header")
			}
			http.Error(w, `{"error":{"code":"InvalidAuthenticationToken","message":"expired"}}`, http.StatusUnauthorized)
		case 2:
			if got := req.Header.Get("Authorization"); got != "Bearer new-access" {
				t.Fatal("unexpected retry authorization header")
			}
			_, _ = w.Write([]byte(`{"id":"u1","displayName":"User One","userPrincipalName":"u1@example.test"}`))
		default:
			t.Fatalf("unexpected extra request: %d", attempts)
		}
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, nil)
	user, err := graph.Me(context.Background())
	if err != nil {
		t.Fatalf("me: %v", err)
	}
	if user.ID != "u1" {
		t.Fatalf("unexpected user: %+v", user)
	}
	if auth.accessCalls != 1 || auth.refreshCalls != 1 {
		t.Fatalf("unexpected auth calls: access=%d refresh=%d", auth.accessCalls, auth.refreshCalls)
	}
	if attempts != 2 {
		t.Fatalf("unexpected attempts: %d", attempts)
	}
}

func TestGraphRetriesTooManyRequestsAfterRetryAfter(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	var attempts int
	var sleeps []time.Duration
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		attempts++
		if attempts == 1 {
			w.Header().Set("Retry-After", "2")
			http.Error(w, `{"error":{"code":"TooManyRequests","message":"slow down"}}`, http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"id":"u1"}`))
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, &sleeps)
	graph.jitter = func(delay time.Duration) time.Duration {
		t.Fatalf("Retry-After delay should not be jittered")
		return delay
	}
	if _, err := graph.Me(context.Background()); err != nil {
		t.Fatalf("me: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("unexpected attempts: %d", attempts)
	}
	if len(sleeps) != 1 || sleeps[0] != 2*time.Second {
		t.Fatalf("unexpected sleeps: %v", sleeps)
	}
}

func TestTeamsBackgroundKeepaliveGraphNetworkDisconnectThenNextCallRecoversCI(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	var attempts int
	graph := &GraphClient{
		auth: auth,
		client: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			attempts++
			if attempts == 1 {
				return nil, fmt.Errorf("simulated network disconnect")
			}
			if got := req.URL.String(); got != "https://graph.example.test/me?$select=id,displayName,userPrincipalName" {
				t.Fatalf("unexpected request URL: %s", got)
			}
			return jsonResponse(http.StatusOK, `{"id":"u1","displayName":"User One","userPrincipalName":"u1@example.test"}`), nil
		})},
		baseURL:    "https://graph.example.test",
		maxRetries: 3,
		backoffMin: time.Millisecond,
		backoffMax: time.Millisecond,
		sleep:      func(context.Context, time.Duration) error { return nil },
		jitter:     func(d time.Duration) time.Duration { return d },
	}

	if _, err := graph.Me(context.Background()); err == nil || !strings.Contains(err.Error(), "simulated network disconnect") {
		t.Fatalf("first Graph call error = %v, want simulated network disconnect", err)
	}
	user, err := graph.Me(context.Background())
	if err != nil {
		t.Fatalf("second Graph call should recover after transport error: %v", err)
	}
	if attempts != 2 || user.ID != "u1" {
		t.Fatalf("attempts=%d user=%#v, want second call recovery", attempts, user)
	}
}

func TestTeamsBackgroundKeepaliveGraphLongRetryAfterIsInterruptibleCI(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	var sleeps []time.Duration
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Retry-After", "3600")
		http.Error(w, `{"error":{"code":"TooManyRequests","message":"long throttle"}}`, http.StatusTooManyRequests)
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, &sleeps)
	graph.sleep = func(ctx context.Context, delay time.Duration) error {
		sleeps = append(sleeps, delay)
		return context.Canceled
	}
	_, err := graph.Me(context.Background())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Graph long Retry-After error = %v, want context.Canceled", err)
	}
	if len(sleeps) != 1 || sleeps[0] != time.Hour {
		t.Fatalf("Graph long Retry-After sleeps = %v, want 1h", sleeps)
	}
}

func TestTeamsBackgroundKeepaliveGraphServerDelayRespectsContextDeadlineCI(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		<-req.Context().Done()
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := graph.Me(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Graph delayed server error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("Graph delayed server did not respect context promptly: %v", elapsed)
	}
}

func TestGraphRetriesTooManyRequestsAfterHTTPDateRetryAfter(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	var attempts int
	var sleeps []time.Duration
	retryAt := time.Now().Add(2 * time.Second).UTC()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		attempts++
		if attempts == 1 {
			w.Header().Set("Retry-After", retryAt.Format(http.TimeFormat))
			http.Error(w, `{"error":{"code":"TooManyRequests","message":"slow down"}}`, http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"id":"u1"}`))
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, &sleeps)
	graph.jitter = func(delay time.Duration) time.Duration {
		t.Fatalf("HTTP-date Retry-After delay should not be jittered")
		return delay
	}
	if _, err := graph.Me(context.Background()); err != nil {
		t.Fatalf("me: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("unexpected attempts: %d", attempts)
	}
	if len(sleeps) != 1 || sleeps[0] <= 0 || sleeps[0] > 3*time.Second {
		t.Fatalf("unexpected HTTP-date retry sleep: %v", sleeps)
	}
}

func TestGraphInvalidRetryAfterFallsBackToJitteredBackoff(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	var attempts int
	var sleeps []time.Duration
	var jitterCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		attempts++
		if attempts == 1 {
			w.Header().Set("Retry-After", "not-a-date")
			http.Error(w, `{"error":{"code":"TooManyRequests","message":"slow down"}}`, http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"id":"u1"}`))
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, &sleeps)
	graph.jitter = func(delay time.Duration) time.Duration {
		jitterCalls++
		return delay + time.Nanosecond
	}
	if _, err := graph.Me(context.Background()); err != nil {
		t.Fatalf("me: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("unexpected attempts: %d", attempts)
	}
	if len(sleeps) != 1 || jitterCalls != 1 || sleeps[0] != time.Millisecond+time.Nanosecond {
		t.Fatalf("unexpected fallback retry timing: sleeps=%v jitterCalls=%d", sleeps, jitterCalls)
	}
}

func TestGraphRetriesServerErrorsWithBoundedBackoff(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	var attempts int
	var sleeps []time.Duration
	var jitterCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		attempts++
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":{"code":"ServiceUnavailable","message":"secret raw-token message body"}}`))
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, &sleeps)
	graph.maxRetries = 2
	graph.jitter = func(delay time.Duration) time.Duration {
		jitterCalls++
		return delay + time.Nanosecond
	}
	_, err := graph.Me(context.Background())
	if err == nil {
		t.Fatal("expected server error")
	}
	if attempts != 3 {
		t.Fatalf("unexpected attempts: %d", attempts)
	}
	if len(sleeps) != 2 || jitterCalls != 2 {
		t.Fatalf("unexpected retry timing: sleeps=%v jitterCalls=%d", sleeps, jitterCalls)
	}
	errText := err.Error()
	if strings.Contains(errText, "secret") || strings.Contains(errText, "raw-token") || strings.Contains(errText, "message body") {
		t.Fatalf("Graph error leaked raw response body: %s", errText)
	}
	if !strings.Contains(errText, "code=ServiceUnavailable") {
		t.Fatalf("Graph error should retain safe error code, got: %s", errText)
	}
}

func TestGraphDoesNotRetryPostServerErrors(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	var attempts int
	var sleeps []time.Duration
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		attempts++
		if req.Method != http.MethodPost || req.URL.String() != "/chats/chat-1/messages" {
			t.Fatalf("unexpected request: %s %s", req.Method, req.URL.String())
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"error":{"code":"ServiceUnavailable","message":"send outcome unknown"}}`))
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, &sleeps)
	graph.maxRetries = 3
	_, err := graph.SendHTML(context.Background(), "chat-1", "hello")
	if err == nil {
		t.Fatal("expected server error")
	}
	if attempts != 1 {
		t.Fatalf("POST server-error attempts = %d, want 1", attempts)
	}
	if len(sleeps) != 0 {
		t.Fatalf("POST server-error sleeps = %v, want none", sleeps)
	}
}

func TestGraphRetriesPostTooManyRequestsAfterRetryAfter(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	var attempts int
	var sleeps []time.Duration
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		attempts++
		if req.Method != http.MethodPost || req.URL.String() != "/chats/chat-1/messages" {
			t.Fatalf("unexpected request: %s %s", req.Method, req.URL.String())
		}
		if attempts == 1 {
			w.Header().Set("Retry-After", "2")
			http.Error(w, `{"error":{"code":"TooManyRequests","message":"slow down"}}`, http.StatusTooManyRequests)
			return
		}
		_, _ = w.Write([]byte(`{"id":"m1","messageType":"message"}`))
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, &sleeps)
	msg, err := graph.SendHTML(context.Background(), "chat-1", "hello")
	if err != nil {
		t.Fatalf("SendHTML error: %v", err)
	}
	if msg.ID != "m1" {
		t.Fatalf("sent message id = %q, want m1", msg.ID)
	}
	if attempts != 2 {
		t.Fatalf("POST 429 attempts = %d, want 2", attempts)
	}
	if len(sleeps) != 1 || sleeps[0] != 2*time.Second {
		t.Fatalf("POST 429 sleeps = %v, want 2s", sleeps)
	}
}

func TestGraphStatusErrorRedactsDynamicPathValues(t *testing.T) {
	err := (&GraphStatusError{
		Method:     http.MethodGet,
		Path:       "/chats/19:secret-chat/messages/secret-message/hostedContents/secret-content/$value",
		StatusCode: http.StatusForbidden,
		Code:       "Forbidden",
	}).Error()
	for _, leaked := range []string{"19:secret-chat", "secret-message", "secret-content"} {
		if strings.Contains(err, leaked) {
			t.Fatalf("GraphStatusError leaked %q in %q", leaked, err)
		}
	}
	if !strings.Contains(err, "/chats/{chat-id}/messages/{message-id}/hostedContents/{hosted-content-id}/$value") {
		t.Fatalf("GraphStatusError did not preserve safe endpoint shape: %q", err)
	}
	uploadErr := (&GraphStatusError{
		Method:     http.MethodPut,
		Path:       "/me/drive/root:/Microsoft%20Teams%20Chat%20Files/private-file.txt:/content",
		StatusCode: http.StatusTooManyRequests,
		Code:       "TooManyRequests",
	}).Error()
	if strings.Contains(uploadErr, "private-file.txt") || !strings.Contains(uploadErr, "/me/drive/root:/{path}:/content") {
		t.Fatalf("upload GraphStatusError path redaction mismatch: %q", uploadErr)
	}
}

func TestGraphAllowlistRejectsUnexpectedEndpoints(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	graph := &GraphClient{auth: auth}
	if err := graph.do(context.Background(), http.MethodGet, "/users", nil, nil); err == nil {
		t.Fatal("expected allowlist rejection")
	}
	if auth.accessCalls != 0 {
		t.Fatalf("auth should not be called for allowlist rejection: %d", auth.accessCalls)
	}

	rejected := []struct {
		method string
		path   string
	}{
		{http.MethodPost, "/me"},
		{http.MethodGet, "/me?$expand=memberOf"},
		{http.MethodGet, "/me/chats?$top=51"},
		{http.MethodGet, "/me/chats?$select=id"},
		{http.MethodPost, "/me/chats?$top=1"},
		{http.MethodGet, "/chats/a?$select=*"},
		{http.MethodGet, "/chats/a/members?$select=id,roles,displayName,email,userId"},
		{http.MethodGet, "/chats/a/members?$top=1"},
		{http.MethodGet, "/chats/a/messages/message-id/extra"},
		{http.MethodGet, "/chats/../messages"},
		{http.MethodGet, "/chats/a/messages/../x"},
		{http.MethodGet, "/chats/a/messages/message-id?$top=1"},
		{http.MethodPatch, "/chats/a/messages/message-id?$top=1"},
		{http.MethodPatch, "/chats/a/messages"},
		{http.MethodGet, "/chats/a/messages/message-id/hostedContents/../$value"},
		{http.MethodGet, "/chats/a/messages/message-id/hostedContents/content-id/$value?$top=1"},
		{http.MethodGet, "/shares/u!abc/driveItem/content?$top=1"},
		{http.MethodGet, "/shares/u!abc/driveItem/content/extra"},
		{http.MethodGet, "/drives/drive-id/items/item-id/content"},
		{http.MethodGet, "/me/drive/root:/file.txt:/content"},
		{http.MethodPut, "/me/drive/root:/../file.txt:/content"},
		{http.MethodPut, "/me/drive/root:/folder/bad:name.txt:/content"},
		{http.MethodPut, "/me/drive/root:/folder/bad%2Fname.txt:/content"},
		{http.MethodPut, "/me/drive/root:/folder/bad%0Aname.txt:/content"},
		{http.MethodGet, "/me/drive/items/item-id"},
		{http.MethodGet, "/me/drive/items/item-id?$select=*"},
		{http.MethodGet, "/me/drive/items/item%0Aevil?$select=id,name,eTag,webUrl,webDavUrl"},
		{http.MethodGet, "/me/drive/items/item%2Fevil?$select=id,name,eTag,webUrl,webDavUrl"},
		{http.MethodGet, "/chats/a/messages?$top=51"},
		{http.MethodPost, "/chats/a/messages?$top=1"},
		{http.MethodGet, "/chats/a/messages?$filter=createdDateTime%20gt%202026-04-30T00%3A00%3A00Z"},
		{http.MethodGet, "/chats/a/messages?$orderby=createdDateTime%20desc&$filter=lastModifiedDateTime%20gt%202026-04-30T00%3A00%3A00Z"},
	}
	for _, tc := range rejected {
		if isAllowedGraphRequest(tc.method, tc.path) {
			t.Fatalf("request should have been rejected: %s %s", tc.method, tc.path)
		}
	}

	allowed := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/me?$select=id,displayName,userPrincipalName"},
		{http.MethodGet, "/me/chats?$top=50"},
		{http.MethodPost, "/chats"},
		{http.MethodPost, "/me/onlineMeetings"},
		{http.MethodGet, "/chats/chat-id?$select=id,topic,chatType,webUrl"},
		{http.MethodGet, "/chats/chat-id/members"},
		{http.MethodGet, "/chats/chat-id/messages?$top=50"},
		{http.MethodGet, "/chats/chat-id/messages/message-id"},
		{http.MethodPatch, "/chats/chat-id/messages/message-id"},
		{http.MethodGet, "/chats/chat-id/messages/message-id/hostedContents/content-id/$value"},
		{http.MethodGet, "/shares/u!abc/driveItem/content"},
		{http.MethodPut, "/me/drive/root:/Microsoft%20Teams%20Chat%20Files/file.txt:/content"},
		{http.MethodGet, "/me/drive/items/item-id?$select=id,name,eTag,webUrl,webDavUrl"},
		{http.MethodGet, "/chats/chat-id/messages?$top=50&$orderby=lastModifiedDateTime%20desc&$filter=lastModifiedDateTime%20gt%202026-04-30T00%3A00%3A00Z"},
		{http.MethodGet, "/chats/chat-id/messages?$top=50&$skiptoken=abc123"},
		{http.MethodPost, "/chats/chat-id/messages"},
	}
	for _, tc := range allowed {
		if !isAllowedGraphRequest(tc.method, tc.path) {
			t.Fatalf("request should have been allowed: %s %s", tc.method, tc.path)
		}
	}
}

func TestGraphListChats(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet || r.URL.Path != "/me/chats" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		if r.URL.Query().Get("$top") != "7" {
			t.Fatalf("list chats top query = %q", r.URL.RawQuery)
		}
		_, _ = fmt.Fprint(w, `{"value":[{"id":"chat-1","topic":"control","chatType":"group","webUrl":"https://teams.example/chat-1"}]}`)
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, nil)
	chats, err := graph.ListChats(context.Background(), 7)
	if err != nil {
		t.Fatalf("ListChats error: %v", err)
	}
	if len(chats) != 1 || chats[0].ID != "chat-1" || chats[0].ChatType != "group" || chats[0].WebURL == "" {
		t.Fatalf("unexpected chats: %#v", chats)
	}
}

func TestGraphCreateMeetingChatUsesOnlineMeetingThreadID(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	var sawCreate bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodPost || r.URL.Path != "/me/onlineMeetings" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		sawCreate = true
		var payload struct {
			Subject       string `json:"subject"`
			StartDateTime string `json:"startDateTime"`
			EndDateTime   string `json:"endDateTime"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode onlineMeeting payload: %v", err)
		}
		if payload.Subject != "💬 Codex Work - s001 - repo - host" || payload.StartDateTime == "" || payload.EndDateTime == "" {
			t.Fatalf("unexpected onlineMeeting payload: %#v", payload)
		}
		_, _ = fmt.Fprint(w, `{"id":"meeting-1","subject":"💬 Codex Work - s001 - repo - host","joinWebUrl":"https://teams.example/join","chatInfo":{"threadId":"19:meeting_abc@thread.v2"}}`)
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, nil)
	chat, err := graph.CreateMeetingChat(context.Background(), "💬 Codex Work - s001 - repo - host")
	if err != nil {
		t.Fatalf("CreateMeetingChat error: %v", err)
	}
	if !sawCreate || chat.ID != "19:meeting_abc@thread.v2" || chat.ChatType != "meeting" || chat.Topic != "💬 Codex Work - s001 - repo - host" {
		t.Fatalf("unexpected meeting chat: %#v sawCreate=%v", chat, sawCreate)
	}
	if !strings.Contains(chat.WebURL, "teams.microsoft.com/l/chat/19%3Ameeting_abc%40thread.v2/0") {
		t.Fatalf("chat WebURL = %q, want Teams chat link", chat.WebURL)
	}
}

func TestGraphAllowlistRejectsRawAndBytesBeforeToken(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	graph := &GraphClient{auth: auth}
	if _, _, err := graph.doRaw(context.Background(), http.MethodGet, "/users/user-id/photo/$value", 1024); err == nil {
		t.Fatal("expected raw allowlist rejection")
	}
	if _, err := graph.doBytes(context.Background(), http.MethodPut, "/me/drive/root:/bad%2Fname.txt:/content", []byte("data"), "text/plain", 1024); err == nil {
		t.Fatal("expected bytes allowlist rejection")
	}
	if auth.accessCalls != 0 {
		t.Fatalf("auth should not be called for rejected raw/bytes requests: %d", auth.accessCalls)
	}
}

func TestGraphAllowlistRejectsEncodedTraversalInDynamicIDs(t *testing.T) {
	rejected := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/chats/%2e%2e/messages?$top=10"},
		{http.MethodGet, "/chats/chat%2Fid/messages?$top=10"},
		{http.MethodGet, "/chats/chat-id/messages/%2e%2e"},
		{http.MethodGet, "/chats/chat-id/messages/message%2Fid"},
		{http.MethodGet, "/chats/chat-id/messages/message-id/hostedContents/%2e%2e/$value"},
		{http.MethodGet, "/chats/chat-id/messages/message-id/hostedContents/content%2Fid/$value"},
	}
	for _, tc := range rejected {
		if isAllowedGraphRequest(tc.method, tc.path) {
			t.Fatalf("encoded traversal/dynamic slash should have been rejected: %s %s", tc.method, tc.path)
		}
	}
}

func TestGraphGetChatAndListMembers(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	var sawGetChat, sawMembers bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/chats/chat-1":
			sawGetChat = true
			if r.URL.Query().Get("$select") != "id,topic,chatType,webUrl" {
				t.Fatalf("chat query = %q", r.URL.RawQuery)
			}
			_, _ = fmt.Fprint(w, `{"id":"chat-1","topic":"💬 work","chatType":"group","webUrl":"https://teams.example/chat"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/chats/chat-1/members":
			sawMembers = true
			if r.URL.RawQuery != "" {
				t.Fatalf("members query = %q", r.URL.RawQuery)
			}
			_, _ = fmt.Fprint(w, `{"value":[{"id":"member-1","roles":["owner"],"displayName":"Jason Wei","email":"jason@example.test","userId":"user-1"}]}`)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, nil)
	chat, err := graph.GetChat(context.Background(), "chat-1")
	if err != nil {
		t.Fatalf("GetChat error: %v", err)
	}
	if chat.ID != "chat-1" || chat.ChatType != "group" || chat.WebURL == "" {
		t.Fatalf("unexpected chat: %#v", chat)
	}
	members, err := graph.ListChatMembers(context.Background(), "chat-1")
	if err != nil {
		t.Fatalf("ListChatMembers error: %v", err)
	}
	if len(members) != 1 || members[0].UserID != "user-1" || members[0].DisplayName != "Jason Wei" {
		t.Fatalf("unexpected members: %#v", members)
	}
	if !sawGetChat || !sawMembers {
		t.Fatalf("missing request(s): chat=%v members=%v", sawGetChat, sawMembers)
	}
}

func TestGraphListMessagesAvoidsSlowTinyTop(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	var gotTop []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/chats/chat-id/messages" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		gotTop = append(gotTop, r.URL.Query().Get("$top"))
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"value":[]}`)
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, nil)
	for _, top := range []int{1, 0, 20, 99} {
		if _, err := graph.ListMessages(context.Background(), "chat-id", top); err != nil {
			t.Fatalf("ListMessages(%d) error: %v", top, err)
		}
	}
	want := []string{"10", "20", "20", "50"}
	if strings.Join(gotTop, ",") != strings.Join(want, ",") {
		t.Fatalf("message top requests = %#v, want %#v", gotTop, want)
	}
}

func TestLiveJasonWeiSingleMemberChatValidation(t *testing.T) {
	validUser := User{ID: "user-1", DisplayName: "Jason Wei", UserPrincipalName: "jason@example.test"}
	validChat := Chat{ID: "chat-1", ChatType: "group"}
	validMembers := []ChatMember{{ID: "member-1", DisplayName: "Jason Wei", UserID: "user-1"}}
	if err := validateLiveJasonWeiSingleMemberChat(validUser, validChat, validMembers, "chat-1"); err != nil {
		t.Fatalf("valid Jason Wei single-member group chat rejected: %v", err)
	}
	validMeetingChat := Chat{ID: "chat-1", ChatType: "meeting"}
	if err := validateLiveJasonWeiSingleMemberChat(validUser, validMeetingChat, validMembers, "chat-1"); err != nil {
		t.Fatalf("valid Jason Wei single-member meeting chat rejected: %v", err)
	}

	tests := []struct {
		name    string
		me      User
		chat    Chat
		members []ChatMember
		chatID  string
		want    string
	}{
		{
			name:    "non Jason user",
			me:      User{ID: "user-2", DisplayName: "Someone Else"},
			chat:    validChat,
			members: validMembers,
			chatID:  "chat-1",
			want:    "not Jason Wei",
		},
		{
			name:    "wrong chat id",
			me:      validUser,
			chat:    Chat{ID: "other-chat", ChatType: "group"},
			members: validMembers,
			chatID:  "chat-1",
			want:    "chat id mismatch",
		},
		{
			name:    "not group chat",
			me:      validUser,
			chat:    Chat{ID: "chat-1", ChatType: "oneOnOne"},
			members: validMembers,
			chatID:  "chat-1",
			want:    "not a single-member group or meeting chat",
		},
		{
			name:    "extra member",
			me:      validUser,
			chat:    validChat,
			members: []ChatMember{{UserID: "user-1"}, {UserID: "user-2"}},
			chatID:  "chat-1",
			want:    "2 member",
		},
		{
			name:    "missing member user id",
			me:      validUser,
			chat:    validChat,
			members: []ChatMember{{DisplayName: "Jason Wei"}},
			chatID:  "chat-1",
			want:    "no userId",
		},
		{
			name:    "wrong member user id",
			me:      validUser,
			chat:    validChat,
			members: []ChatMember{{UserID: "user-2"}},
			chatID:  "chat-1",
			want:    "does not match",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateLiveJasonWeiSingleMemberChat(tc.me, tc.chat, tc.members, tc.chatID)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("expected error containing %q, got %v", tc.want, err)
			}
		})
	}
}

func TestGraphUploadAndSendDriveItemAttachment(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	var sawUpload, sawMetadata, sawMessage bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodPut && r.URL.EscapedPath() == "/me/drive/root:/Microsoft%20Teams%20Chat%20Files/file.txt:/content":
			sawUpload = true
			if got := r.Header.Get("Content-Type"); got != "text/plain" {
				t.Fatalf("upload content type = %q", got)
			}
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Fatalf("read upload body: %v", err)
			}
			if string(body) != "file bytes" {
				t.Fatalf("upload body = %q", string(body))
			}
			_, _ = fmt.Fprint(w, `{"id":"item-1","name":"file.txt"}`)
		case r.Method == http.MethodGet && r.URL.Path == "/me/drive/items/item-1":
			sawMetadata = true
			if r.URL.Query().Get("$select") != "id,name,eTag,webUrl,webDavUrl" {
				t.Fatalf("metadata query = %q", r.URL.RawQuery)
			}
			_, _ = fmt.Fprint(w, `{"id":"item-1","name":"file.txt","eTag":"\"{1176C944-0CB9-4304-974C-5837185EFD6A},1\"","webDavUrl":"https://contoso.sharepoint.com/file.txt"}`)
		case r.Method == http.MethodPost && r.URL.Path == "/chats/chat-1/messages":
			sawMessage = true
			var payload struct {
				Body struct {
					Content string `json:"content"`
				} `json:"body"`
				Attachments []MessageAttachment `json:"attachments"`
			}
			if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
				t.Fatalf("decode message payload: %v", err)
			}
			if !strings.Contains(payload.Body.Content, `<attachment id="1176c944-0cb9-4304-974c-5837185efd6a"></attachment>`) {
				t.Fatalf("message body missing attachment tag: %q", payload.Body.Content)
			}
			if !strings.Contains(payload.Body.Content, "Codex: attached") {
				t.Fatalf("attachment message should be helper-prefixed, got %q", payload.Body.Content)
			}
			if len(payload.Attachments) != 1 || payload.Attachments[0].ContentType != "reference" || payload.Attachments[0].Name != "file.txt" || payload.Attachments[0].ContentURL == "" {
				t.Fatalf("unexpected attachments: %#v", payload.Attachments)
			}
			_, _ = fmt.Fprint(w, `{"id":"message-1","messageType":"message","attachments":[{"id":"1176c944-0cb9-4304-974c-5837185efd6a","contentType":"reference","name":"file.txt","contentUrl":"https://contoso.sharepoint.com/file.txt"}]}`)
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, nil)
	item, err := graph.UploadSmallDriveItem(context.Background(), DefaultOutboundUploadFolder(), "file.txt", []byte("file bytes"), "text/plain")
	if err != nil {
		t.Fatalf("UploadSmallDriveItem error: %v", err)
	}
	meta, err := graph.GetDriveItemMetadata(context.Background(), item.ID)
	if err != nil {
		t.Fatalf("GetDriveItemMetadata error: %v", err)
	}
	msg, err := graph.SendDriveItemAttachment(context.Background(), "chat-1", meta, "attached")
	if err != nil {
		t.Fatalf("SendDriveItemAttachment error: %v", err)
	}
	if msg.ID != "message-1" {
		t.Fatalf("message id = %q", msg.ID)
	}
	if !sawUpload || !sawMetadata || !sawMessage {
		t.Fatalf("missing request(s): upload=%v metadata=%v message=%v", sawUpload, sawMetadata, sawMessage)
	}
}

func TestGraphSendHTMLWithOwnerMention(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	var sawMessage bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/chats/chat-1/messages" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		sawMessage = true
		var payload struct {
			Body struct {
				Content string `json:"content"`
			} `json:"body"`
			Mentions []struct {
				ID          int    `json:"id"`
				MentionText string `json:"mentionText"`
				Mentioned   struct {
					User struct {
						ID               string `json:"id"`
						DisplayName      string `json:"displayName"`
						UserIdentityType string `json:"userIdentityType"`
					} `json:"user"`
				} `json:"mentioned"`
			} `json:"mentions"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode mention payload: %v", err)
		}
		if !strings.Contains(payload.Body.Content, `<at id="0">Owner &amp; One</at>`) {
			t.Fatalf("body missing escaped mention tag: %q", payload.Body.Content)
		}
		if len(payload.Mentions) != 1 || payload.Mentions[0].ID != 0 || payload.Mentions[0].MentionText != "Owner & One" || payload.Mentions[0].Mentioned.User.ID != "user-1" || payload.Mentions[0].Mentioned.User.UserIdentityType != "aadUser" {
			t.Fatalf("unexpected mentions payload: %#v", payload.Mentions)
		}
		_, _ = fmt.Fprint(w, `{"id":"message-1","messageType":"message"}`)
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, nil)
	html, mentions := HTMLMessageMentioningOwner("Codex", "completed", User{ID: "user-1", DisplayName: "Owner & One", UserPrincipalName: "owner@example.test"})
	msg, err := graph.SendHTMLWithMentions(context.Background(), "chat-1", html, mentions)
	if err != nil {
		t.Fatalf("SendHTMLWithMentions error: %v", err)
	}
	if msg.ID != "message-1" || !sawMessage {
		t.Fatalf("message result mismatch: msg=%#v saw=%v", msg, sawMessage)
	}
}

func TestGraphUpdateChatMessageHTML(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	var sawPatch bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch || r.URL.Path != "/chats/chat-1/messages/message-1" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		sawPatch = true
		var payload struct {
			Body struct {
				ContentType string `json:"contentType"`
				Content     string `json:"content"`
			} `json:"body"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Fatalf("decode update payload: %v", err)
		}
		if payload.Body.ContentType != "html" || payload.Body.Content != "<p>updated</p>" {
			t.Fatalf("unexpected update payload: %#v", payload.Body)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, nil)
	if err := graph.UpdateChatMessageHTML(context.Background(), "chat-1", "message-1", "<p>updated</p>"); err != nil {
		t.Fatalf("UpdateChatMessageHTML error: %v", err)
	}
	if !sawPatch {
		t.Fatal("missing PATCH request")
	}
}

func TestGraphGetHostedContentValueReturnsBytes(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/chats/chat-id/messages/message-id/hostedContents/content-id/$value" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.String())
		}
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write([]byte("png-bytes"))
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, nil)
	value, err := graph.GetHostedContentValue(context.Background(), "chat-id", "message-id", "content-id")
	if err != nil {
		t.Fatalf("GetHostedContentValue error: %v", err)
	}
	if string(value.Bytes) != "png-bytes" || value.ContentType != "image/png" {
		t.Fatalf("unexpected hosted content: %#v", value)
	}
}

func TestGraphGetSharedDriveItemContentReturnsBytes(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	rawURL := "https://contoso.sharepoint.com/sites/team/Shared%20Documents/file.txt"
	wantPath := "/shares/" + url.PathEscape(graphShareID(rawURL)) + "/driveItem/content"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.EscapedPath() != wantPath {
			t.Fatalf("unexpected request: %s %s want %s", r.Method, r.URL.String(), wantPath)
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("file-bytes"))
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, nil)
	value, err := graph.GetSharedDriveItemContent(context.Background(), rawURL)
	if err != nil {
		t.Fatalf("GetSharedDriveItemContent error: %v", err)
	}
	if string(value.Bytes) != "file-bytes" || value.ContentType != "text/plain" {
		t.Fatalf("unexpected shared file content: %#v", value)
	}
}

func TestHelperAttachmentMessageIsSelfIgnoring(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		{name: "empty", in: "", want: "Codex: file attached"},
		{name: "plain", in: "report ready", want: "Codex: report ready"},
		{name: "already helper", in: "Codex: report ready", want: "Codex: report ready"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := helperAttachmentMessage(tc.in)
			if got != tc.want {
				t.Fatalf("helperAttachmentMessage(%q) = %q, want %q", tc.in, got, tc.want)
			}
			if !IsHelperText(got) {
				t.Fatalf("attachment message should be ignored by helper text filter: %q", got)
			}
		})
	}
}

func TestGraphListMessagesWindowReturnsSafeContinuation(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	requests := 0
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		switch requests {
		case 1:
			if r.URL.Query().Get("$orderby") != "lastModifiedDateTime desc" || !strings.Contains(r.URL.Query().Get("$filter"), "lastModifiedDateTime gt ") {
				t.Fatalf("first request missing cursor query: %s", r.URL.RawQuery)
			}
			_, _ = fmt.Fprintf(w, `{"value":[{"id":"m1","messageType":"message"}],"@odata.nextLink":%q}`, server.URL+"/chats/chat-id/messages?$top=50&$skiptoken=next")
		case 2:
			if r.URL.Query().Get("$skiptoken") != "next" {
				t.Fatalf("second request missing skiptoken: %s", r.URL.RawQuery)
			}
			_, _ = fmt.Fprint(w, `{"value":[{"id":"m2","messageType":"message"}]}`)
		default:
			t.Fatalf("unexpected extra request: %s", r.URL.String())
		}
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, nil)
	window, err := graph.ListMessagesWindow(context.Background(), "chat-id", 50, time.Date(2026, 4, 30, 0, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatalf("ListMessagesWindow error: %v", err)
	}
	if got := len(window.Messages); got != 1 {
		t.Fatalf("message count = %d, want one page", got)
	}
	if !window.Truncated || !strings.Contains(window.NextPath, "$skiptoken=next") {
		t.Fatalf("window continuation = truncated %v next %q", window.Truncated, window.NextPath)
	}
	if requests != 1 {
		t.Fatalf("initial window requests = %d, want 1", requests)
	}
	continued, err := graph.ListMessagesWindowFromPath(context.Background(), window.NextPath)
	if err != nil {
		t.Fatalf("ListMessagesWindowFromPath error: %v", err)
	}
	if got := len(continued.Messages); got != 1 {
		t.Fatalf("continued message count = %d, want 1", got)
	}
	if continued.Truncated || continued.NextPath != "" {
		t.Fatalf("continued window should be complete: %#v", continued)
	}
}

func TestGraphNextLinkStripsProductionBasePath(t *testing.T) {
	graph := &GraphClient{baseURL: graphBaseURL}
	got, err := graph.relativeGraphPath("https://graph.microsoft.com/v1.0/chats/chat-id/messages?$top=10&$skiptoken=abc")
	if err != nil {
		t.Fatalf("relativeGraphPath error: %v", err)
	}
	want := "/chats/chat-id/messages?$top=10&$skiptoken=abc"
	if got != want {
		t.Fatalf("relativeGraphPath = %q, want %q", got, want)
	}
	if !isAllowedGraphRequest(http.MethodGet, got) {
		t.Fatalf("normalized nextLink should be allowlisted: %s", got)
	}
}

func TestGraphListMessagesWindowRejectsExternalNextLink(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"value":[],"@odata.nextLink":"https://evil.example/chats/chat-id/messages?$skiptoken=next"}`)
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, nil)
	_, err := graph.ListMessagesWindow(context.Background(), "chat-id", 50, time.Time{})
	if err == nil || !strings.Contains(err.Error(), "nextLink host mismatch") {
		t.Fatalf("expected nextLink host mismatch, got %v", err)
	}
}

func TestGraphListMessagesWindowFetchesOnePageAndReturnsContinuation(t *testing.T) {
	auth := &fakeGraphAuth{token: "access"}
	requests := 0
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		switch requests {
		case 1:
			payload := map[string]any{
				"value": []map[string]string{{
					"id":          "m1",
					"messageType": "message",
				}},
				"@odata.nextLink": server.URL + "/chats/chat-id/messages?$skiptoken=next",
			}
			if err := json.NewEncoder(w).Encode(payload); err != nil {
				t.Fatalf("encode page: %v", err)
			}
		case 2:
			if got := r.URL.Query().Get("$skiptoken"); got != "next" {
				t.Fatalf("continuation skiptoken = %q, want next", got)
			}
			_, _ = fmt.Fprint(w, `{"value":[{"id":"m2","messageType":"message"}]}`)
		default:
			t.Fatalf("unexpected extra request: %s", r.URL.String())
		}
	}))
	defer server.Close()

	graph := newTestGraphClient(auth, server, nil)
	window, err := graph.ListMessagesWindow(context.Background(), "chat-id", 50, time.Time{})
	if err != nil {
		t.Fatalf("ListMessagesWindow error: %v", err)
	}
	if requests != 1 {
		t.Fatalf("request count = %d, want 1", requests)
	}
	if got := len(window.Messages); got != 1 {
		t.Fatalf("message count = %d, want 1", got)
	}
	if !window.Truncated {
		t.Fatal("window should be marked truncated when Graph returns nextLink")
	}
	if !strings.Contains(window.NextPath, "$skiptoken=next") {
		t.Fatalf("next path = %q, want continuation skiptoken", window.NextPath)
	}
	continued, err := graph.ListMessagesWindowFromPath(context.Background(), window.NextPath)
	if err != nil {
		t.Fatalf("ListMessagesWindowFromPath error: %v", err)
	}
	if got := len(continued.Messages); got != 1 {
		t.Fatalf("continued message count = %d, want 1", got)
	}
}

func newTestGraphClient(auth graphAuth, server *httptest.Server, sleeps *[]time.Duration) *GraphClient {
	return &GraphClient{
		auth:       auth,
		client:     server.Client(),
		baseURL:    server.URL,
		maxRetries: 3,
		backoffMin: time.Millisecond,
		backoffMax: 10 * time.Millisecond,
		sleep: func(_ context.Context, delay time.Duration) error {
			if sleeps != nil {
				*sleeps = append(*sleeps, delay)
			}
			return nil
		},
		jitter: func(delay time.Duration) time.Duration {
			return delay
		},
	}
}

func jsonResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
