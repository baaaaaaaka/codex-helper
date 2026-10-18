package codexrunner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestRemoteBrokerRelaysProtocolAndApprovesAfterDelay(t *testing.T) {
	transport := newChannelAppServerTransport()
	delays := make(chan time.Duration, 1)
	release := make(chan struct{})
	handler := AppServerServerRequestHandlerFunc(func(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, bool, error) {
		delays <- DefaultApprovalDelay
		select {
		case <-ctx.Done():
			return nil, true, ctx.Err()
		case <-release:
		}
		return automaticApprovalResult(method, params)
	})
	broker, err := StartRemoteBroker(context.Background(), RemoteBrokerOptions{
		Starter: AppServerTransportStarterFunc(func(context.Context, AppServerStartRequest) (AppServerLineTransport, error) {
			return transport, nil
		}),
		ServerRequestHandler: handler,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = broker.Close(ctx)
	}()
	if strings.Contains(broker.URL(), "/_cxp/") || strings.TrimSpace(broker.AuthToken()) == "" {
		t.Fatalf("broker endpoint must use a bare Codex-compatible URL with a separate capability token: url=%q", broker.URL())
	}
	headers := http.Header{"Authorization": []string{"Bearer " + broker.AuthToken()}}
	connection, _, err := websocket.DefaultDialer.Dial(broker.URL(), headers)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()

	clientRequest := []byte(`{"id":1,"method":"initialize","params":{}}`)
	if err := connection.WriteMessage(websocket.TextMessage, clientRequest); err != nil {
		t.Fatal(err)
	}
	if got := <-transport.writes; string(got) != string(clientRequest) {
		t.Fatalf("app-server write = %s", got)
	}

	transport.reads <- []byte(`{"id":1,"result":{}}`)
	_, relayed, err := connection.ReadMessage()
	if err != nil || string(relayed) != `{"id":1,"result":{}}` {
		t.Fatalf("relayed=%s err=%v", relayed, err)
	}

	transport.reads <- []byte(`{"id":99,"method":"item/commandExecution/requestApproval","params":{}}`)
	if delay := <-delays; delay != DefaultApprovalDelay {
		t.Fatalf("delay=%s", delay)
	}
	select {
	case response := <-transport.writes:
		t.Fatalf("approval sent before release: %s", response)
	default:
	}
	close(release)
	select {
	case response := <-transport.writes:
		var decoded map[string]any
		if json.Unmarshal(response, &decoded) != nil || decoded["id"] != float64(99) {
			t.Fatalf("approval response=%s", response)
		}
	case <-time.After(time.Second):
		t.Fatal("approval response was not written")
	}
}

func TestRemoteBrokerManualModeRelaysApprovalRequestsAndResponsesByteTransparent(t *testing.T) {
	transport := newChannelAppServerTransport()
	broker, err := StartRemoteBroker(context.Background(), RemoteBrokerOptions{
		Starter: AppServerTransportStarterFunc(func(context.Context, AppServerStartRequest) (AppServerLineTransport, error) {
			return transport, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = broker.Close(ctx)
	}()
	headers := http.Header{"Authorization": []string{"Bearer " + broker.AuthToken()}}
	connection, _, err := websocket.DefaultDialer.Dial(broker.URL(), headers)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()

	request := []byte(`{"jsonrpc":"2.0","id":99,"method":"item/commandExecution/requestApproval","params":{"command":"touch sentinel"}}`)
	for attempt := 0; attempt < 2; attempt++ {
		transport.reads <- request
		_, relayed, readErr := connection.ReadMessage()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if string(relayed) != string(request) {
			t.Fatalf("manual request %d changed in transit: %s", attempt, relayed)
		}
	}
	select {
	case response := <-transport.writes:
		t.Fatalf("manual mode synthesized an approval response: %s", response)
	default:
	}

	response := []byte(`{"jsonrpc":"2.0","id":99,"result":{"decision":"accept"}}`)
	if err := connection.WriteMessage(websocket.TextMessage, response); err != nil {
		t.Fatal(err)
	}
	select {
	case relayed := <-transport.writes:
		if string(relayed) != string(response) {
			t.Fatalf("manual response changed in transit: %s", relayed)
		}
	case <-time.After(time.Second):
		t.Fatal("manual approval response was not returned to app-server")
	}
}

func TestRemoteBrokerAutomaticModeUsesFixedDelay(t *testing.T) {
	transport := newChannelAppServerTransport()
	broker, err := StartRemoteBroker(context.Background(), RemoteBrokerOptions{
		Starter: AppServerTransportStarterFunc(func(context.Context, AppServerStartRequest) (AppServerLineTransport, error) {
			return transport, nil
		}),
		ApprovalMode: ApprovalModeAutomatic,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = broker.Close(ctx)
	}()
	connection, _, err := websocket.DefaultDialer.Dial(broker.URL(), http.Header{
		"Authorization": []string{"Bearer " + broker.AuthToken()},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	started := time.Now()
	transport.reads <- []byte(`{"jsonrpc":"2.0","id":101,"method":"item/commandExecution/requestApproval","params":{}}`)
	select {
	case response := <-transport.writes:
		if elapsed := time.Since(started); elapsed < DefaultApprovalDelay {
			t.Fatalf("automatic approval arrived after %s, want at least %s", elapsed, DefaultApprovalDelay)
		}
		if !strings.Contains(string(response), `"decision":"accept"`) {
			t.Fatalf("automatic approval response = %s", response)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("automatic approval did not arrive")
	}
}

func TestRemoteBrokerRequiresBearerCapabilityAtRootURL(t *testing.T) {
	transport := newChannelAppServerTransport()
	broker, err := StartRemoteBroker(context.Background(), RemoteBrokerOptions{
		Starter: AppServerTransportStarterFunc(func(context.Context, AppServerStartRequest) (AppServerLineTransport, error) {
			return transport, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = broker.Close(ctx)
	}()

	for name, headers := range map[string]http.Header{
		"missing": nil,
		"wrong":   {"Authorization": []string{"Bearer wrong-token"}},
		"raw":     {"Authorization": []string{broker.AuthToken()}},
	} {
		t.Run(name, func(t *testing.T) {
			connection, response, dialErr := websocket.DefaultDialer.Dial(broker.URL(), headers)
			if connection != nil {
				_ = connection.Close()
			}
			if dialErr == nil || response == nil || response.StatusCode != http.StatusUnauthorized {
				t.Fatalf("dial error=%v response=%v, want HTTP 401", dialErr, response)
			}
		})
	}

	connection, response, err := websocket.DefaultDialer.Dial(broker.URL()+"/not-supported", http.Header{
		"Authorization": []string{"Bearer " + broker.AuthToken()},
	})
	if connection != nil {
		_ = connection.Close()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusNotFound {
		t.Fatalf("path dial error=%v response=%v, want HTTP 404", err, response)
	}
}

func TestRemoteBrokerIsServingBeforeStartReturns(t *testing.T) {
	for range 50 {
		transport := newChannelAppServerTransport()
		broker, err := StartRemoteBroker(context.Background(), RemoteBrokerOptions{
			Starter: AppServerTransportStarterFunc(func(context.Context, AppServerStartRequest) (AppServerLineTransport, error) {
				return transport, nil
			}),
		})
		if err != nil {
			t.Fatal(err)
		}
		headers := http.Header{"Authorization": []string{"Bearer " + broker.AuthToken()}}
		connection, response, dialErr := websocket.DefaultDialer.Dial(broker.URL(), headers)
		if dialErr != nil {
			_ = broker.Close(context.Background())
			t.Fatalf("immediate authenticated dial failed: %v (response=%v)", dialErr, response)
		}
		_ = connection.Close()
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		if err := broker.Close(closeCtx); err != nil {
			cancel()
			t.Fatal(err)
		}
		cancel()
	}
}

func TestRemoteBrokerApprovalCacheBoundsPreviouslyConcurrentRequests(t *testing.T) {
	broker := &RemoteBroker{approvals: make(map[string]*brokerApprovalState)}
	type pending struct {
		key   string
		state *brokerApprovalState
	}
	requests := make([]pending, 0, 2048)
	for index := 0; index < cap(requests); index++ {
		key := fmt.Sprintf("%d\x00approval", index)
		state, owner := broker.approvalState(key)
		if !owner {
			t.Fatalf("request %d unexpectedly reused approval state", index)
		}
		requests = append(requests, pending{key: key, state: state})
	}
	// Complete in reverse order so the first 1024 cache evictions exercise
	// states that were all created while still in flight.
	for index := len(requests) - 1; index >= 0; index-- {
		broker.finishApprovalState(requests[index].key, requests[index].state, []byte(`{"ok":true}`))
	}
	if got := len(broker.approvals); got != 1024 {
		t.Fatalf("approval cache size = %d, want 1024", got)
	}
	if got := len(broker.approvalOrder); got != 1024 {
		t.Fatalf("approval order size = %d, want 1024", got)
	}
}

func TestRemoteBrokerAutomaticModeStreamsLargeResponseAndStillApprovesRequests(t *testing.T) {
	transport := newStreamingAppServerTransport()
	var handled atomic.Int32
	handler := AppServerServerRequestHandlerFunc(func(_ context.Context, method string, params json.RawMessage) (json.RawMessage, bool, error) {
		if method != appServerMethodCommandExecutionApproval || !bytes.Contains(params, []byte(`"command":"echo"`)) {
			return nil, false, nil
		}
		handled.Add(1)
		return json.RawMessage(`{"decision":"accepted"}`), true, nil
	})
	_, connection := startStreamingRemoteBroker(t, transport, handler)

	payloadSize := defaultRemoteBrokerApprovalCaptureLimit + 4096
	largePayload := strings.Repeat("p", payloadSize)
	prefix := []byte(`{"id":7,"result":{"plugins":"`)
	suffix := []byte(`"}}`)
	chunks := [][]byte{prefix}
	for offset := 0; offset < len(largePayload); offset += appServerProcessStdoutChunkSize {
		end := offset + appServerProcessStdoutChunkSize
		if end > len(largePayload) {
			end = len(largePayload)
		}
		chunks = append(chunks, []byte(largePayload[offset:end]))
	}
	chunks = append(chunks, suffix)
	transport.records <- appServerStreamRecord{chunks: chunks}

	if err := connection.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	messageType, message, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("read large plugin response: %v", err)
	}
	if messageType != websocket.TextMessage {
		t.Fatalf("large response message type = %d, want text", messageType)
	}
	wantLength := len(prefix) + payloadSize + len(suffix)
	if len(message) != wantLength || !bytes.HasPrefix(message, prefix) || !bytes.HasSuffix(message, suffix) {
		t.Fatalf("large response length/boundaries = %d, want %d and valid JSON boundaries", len(message), wantLength)
	}
	for index, value := range message[len(prefix) : len(message)-len(suffix)] {
		if value != 'p' {
			t.Fatalf("large response payload differs at byte %d", index)
		}
	}

	transport.records <- appServerStreamRecord{chunks: [][]byte{[]byte(`{"params":{"command":"echo"},"id":55,"method":"item/commandExecution/requestApproval"}`)}}
	select {
	case response := <-transport.writes:
		var decoded struct {
			ID     json.RawMessage `json:"id"`
			Result struct {
				Decision string `json:"decision"`
			} `json:"result"`
		}
		if err := json.Unmarshal(response, &decoded); err != nil {
			t.Fatalf("decode automatic approval response %s: %v", response, err)
		}
		if string(decoded.ID) != "55" || decoded.Result.Decision != "accepted" {
			t.Fatalf("automatic approval response = %s", response)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("large ordinary response prevented a later approval request")
	}
	if handled.Load() != 1 {
		t.Fatalf("approval handler calls = %d, want 1", handled.Load())
	}
	if err := connection.SetReadDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := connection.ReadMessage(); err == nil {
		t.Fatal("automatically handled approval request was also forwarded to the TUI")
	}
}

func TestRemoteBrokerStreamsLargeProcessRecordPastLegacyLineLimit(t *testing.T) {
	oldLimit := appServerProcessStdoutLineLimit
	appServerProcessStdoutLineLimit = 256
	defer func() { appServerProcessStdoutLineLimit = oldLimit }()

	const payloadSize = defaultAppServerProcessStdoutLineLimit + 2048
	transport := startProcessHelper(t, AppServerProcessStarter{}, "stream-jsonl", fmt.Sprint(payloadSize))
	_, connection := startStreamingRemoteBroker(t, transport, AutomaticApprovalHandler{})
	if err := connection.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, message, err := connection.ReadMessage()
	if err != nil {
		t.Fatalf("read process-backed large response: %v", err)
	}
	prefix := []byte(`{"id":7,"result":{"plugins":"`)
	suffix := []byte(`"}}`)
	wantLength := len(prefix) + payloadSize + len(suffix)
	if len(message) != wantLength || !bytes.HasPrefix(message, prefix) || !bytes.HasSuffix(message, suffix) {
		t.Fatalf("process-backed response length/boundaries = %d, want %d", len(message), wantLength)
	}
	for index, value := range message[len(prefix) : len(message)-len(suffix)] {
		if value != 'p' {
			t.Fatalf("process-backed payload differs at byte %d", index)
		}
	}
	_, next, err := connection.ReadMessage()
	if err != nil || string(next) != `{"next":true}` {
		t.Fatalf("record after large process output = %s, err=%v", next, err)
	}
}

func TestRemoteBrokerAutomaticModeFallsBackToTUIAtBoundedLimits(t *testing.T) {
	tests := []struct {
		name       string
		prefixCap  int
		captureCap int
		request    func() []byte
	}{
		{
			name:      "method appears after a large undecided field",
			prefixCap: 48,
			request: func() []byte {
				return []byte(`{"id":1,"params":{"payload":"` + strings.Repeat("x", 1024) + `"},"method":"custom/approval"}`)
			},
		},
		{
			name:       "approval candidate exceeds capture budget",
			captureCap: 128,
			request: func() []byte {
				return []byte(`{"id":2,"method":"custom/approval","params":{"payload":"` + strings.Repeat("y", 1024) + `"}}`)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transport := newStreamingAppServerTransport()
			var calls atomic.Int32
			handler := AppServerServerRequestHandlerFunc(func(context.Context, string, json.RawMessage) (json.RawMessage, bool, error) {
				calls.Add(1)
				return json.RawMessage(`{}`), true, nil
			})
			_, connection := startStreamingRemoteBrokerWithLimits(t, transport, handler, test.prefixCap, test.captureCap)
			request := test.request()
			transport.records <- streamRecordInChunks(request, 17)
			if err := connection.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			_, relayed, err := connection.ReadMessage()
			if err != nil {
				t.Fatalf("read request handed back to TUI: %v", err)
			}
			if !bytes.Equal(relayed, request) {
				t.Fatalf("fallback request changed in transit (%d bytes)", len(relayed))
			}
			if calls.Load() != 0 {
				t.Fatalf("handler calls = %d, want 0 for bounded fallback", calls.Load())
			}
		})
	}
}

func TestRemoteBrokerSerializesApprovalBeforeFollowingMessages(t *testing.T) {
	transport := newStreamingAppServerTransport()
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	handler := AppServerServerRequestHandlerFunc(func(ctx context.Context, method string, _ json.RawMessage) (json.RawMessage, bool, error) {
		close(entered)
		select {
		case <-ctx.Done():
			return nil, true, ctx.Err()
		case <-release:
		}
		return json.RawMessage(`{"decision":"accept"}`), true, nil
	})
	_, connection := startStreamingRemoteBroker(t, transport, handler)
	transport.records <- appServerStreamRecord{chunks: [][]byte{[]byte(`{"id":9,"method":"approval","params":{}}`)}}
	transport.records <- appServerStreamRecord{chunks: [][]byte{[]byte(`{"id":10,"result":{"afterApproval":true}}`)}}

	select {
	case <-transport.completedRecords:
	case <-time.After(5 * time.Second):
		t.Fatal("broker did not finish reading the approval frame")
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("approval handler did not start")
	}
	select {
	case <-transport.completedRecords:
		t.Fatal("broker read a following frame while approval was unresolved")
	case <-time.After(100 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })

	select {
	case response := <-transport.writes:
		var decoded appServerMessage
		if err := json.Unmarshal(response, &decoded); err != nil || string(decoded.ID) != "9" {
			t.Fatalf("approval response = %s, err=%v", response, err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("approval response was not written")
	}
	select {
	case <-transport.completedRecords:
	case <-time.After(5 * time.Second):
		t.Fatal("broker did not resume reading after approval")
	}
	_, relayed, err := connection.ReadMessage()
	if err != nil || string(relayed) != `{"id":10,"result":{"afterApproval":true}}` {
		t.Fatalf("following frame = %s, err=%v", relayed, err)
	}
}

func TestRemoteBrokerAbortsPartialWebsocketMessageOnStdoutFailure(t *testing.T) {
	transport := newStreamingAppServerTransport()
	_, connection := startStreamingRemoteBroker(t, transport, nil)
	transport.records <- appServerStreamRecord{
		chunks: [][]byte{[]byte(`{"id":4,"result":{"partial":"not-a-complete-record`)},
		err:    errors.New("injected stdout failure"),
	}
	if err := connection.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, message, err := connection.ReadMessage()
	if err == nil {
		t.Fatalf("partial JSON record was published as a complete WebSocket message: %q", message)
	}
	if len(message) > 0 {
		t.Fatalf("incomplete message bytes were returned to TUI: %q", message)
	}
}

func TestRemoteBrokerCloseUnblocksActiveStreamingConnection(t *testing.T) {
	transport := newStreamingAppServerTransport()
	broker, connection := startStreamingRemoteBroker(t, transport, nil)
	closed := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		closed <- broker.Close(ctx)
	}()
	if err := connection.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := connection.ReadMessage(); err == nil {
		t.Fatal("active WebSocket remained open after broker Close")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("broker Close: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("broker Close did not finish after unblocking the WebSocket")
	}
}

func TestRemoteBrokerAutomaticModeForwardsRequestsTheHandlerDeclines(t *testing.T) {
	transport := newStreamingAppServerTransport()
	_, connection := startStreamingRemoteBroker(t, transport, AutomaticApprovalHandler{})
	request := []byte(`{"id":44,"method":"mcpServer/elicitation/request","params":{"request":{"_meta":{"codex_approval_kind":"ask_user"}}}}`)
	transport.records <- streamRecordInChunks(request, 5)
	if err := connection.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, relayed, err := connection.ReadMessage()
	if err != nil || !bytes.Equal(relayed, request) {
		t.Fatalf("declined approval request = %s, err=%v", relayed, err)
	}
	select {
	case response := <-transport.writes:
		t.Fatalf("handler-declined request generated a response: %s", response)
	default:
	}
}

func TestRemoteBrokerAutomaticModePreservesExactIDAndDoesNotApproveDuplicateMethod(t *testing.T) {
	transport := newStreamingAppServerTransport()
	_, connection := startStreamingRemoteBroker(t, transport, AutomaticApprovalHandler{})
	largeID := "9007199254740993123456789"
	request := []byte(`{"met\u0068od":"item/commandExecution/requestApproval","params":{"command":"echo"},"id":` + largeID + `}`)
	transport.records <- streamRecordInChunks(request, 3)
	select {
	case response := <-transport.writes:
		var decoded appServerMessage
		if err := json.Unmarshal(response, &decoded); err != nil || string(decoded.ID) != largeID {
			t.Fatalf("response id = %s, err=%v; want exact %s", decoded.ID, err, largeID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("request with escaped key and late id was not approved")
	}

	duplicate := []byte(`{"id":90,"method":"item/commandExecution/requestApproval","params":{},"method":"custom/not-approvable"}`)
	transport.records <- streamRecordInChunks(duplicate, 11)
	if err := connection.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	_, relayed, err := connection.ReadMessage()
	if err != nil || !bytes.Equal(relayed, duplicate) {
		t.Fatalf("ambiguous duplicate-key request = %s, err=%v", relayed, err)
	}
}

func TestRemoteBrokerAutomaticModeDoesNotForwardCandidateWhenHandlerFails(t *testing.T) {
	transport := newStreamingAppServerTransport()
	_, connection := startStreamingRemoteBroker(t, transport, AppServerServerRequestHandlerFunc(func(context.Context, string, json.RawMessage) (json.RawMessage, bool, error) {
		return nil, true, errors.New("approval policy failed")
	}))
	transport.records <- appServerStreamRecord{chunks: [][]byte{[]byte(`{"id":91,"method":"custom/approval","params":{}}`)}}
	if err := connection.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, message, err := connection.ReadMessage(); err == nil {
		t.Fatalf("handler failure should close the broker without forwarding candidate: %s", message)
	}
	select {
	case response := <-transport.writes:
		t.Fatalf("handler failure generated a response: %s", response)
	default:
	}
}

type channelAppServerTransport struct {
	reads     chan []byte
	writes    chan []byte
	closed    chan struct{}
	closeOnce sync.Once
}

type appServerStreamRecord struct {
	chunks [][]byte
	err    error
}

type streamingAppServerTransport struct {
	*channelAppServerTransport
	records          chan appServerStreamRecord
	completedRecords chan struct{}
}

func newStreamingAppServerTransport() *streamingAppServerTransport {
	return &streamingAppServerTransport{
		channelAppServerTransport: newChannelAppServerTransport(),
		records:                   make(chan appServerStreamRecord, 16),
		completedRecords:          make(chan struct{}, 16),
	}
}

func (t *streamingAppServerTransport) ReadRecord(ctx context.Context, writeChunk func([]byte) error) (bool, error) {
	select {
	case record, ok := <-t.records:
		if !ok {
			return false, io.EOF
		}
		for _, chunk := range record.chunks {
			if err := writeChunk(chunk); err != nil {
				return true, err
			}
		}
		select {
		case t.completedRecords <- struct{}{}:
		default:
		}
		return true, record.err
	case <-ctx.Done():
		return false, ctx.Err()
	case <-t.closed:
		return false, io.EOF
	}
}

func streamRecordInChunks(record []byte, chunkSize int) appServerStreamRecord {
	chunks := make([][]byte, 0, (len(record)+chunkSize-1)/chunkSize)
	for offset := 0; offset < len(record); offset += chunkSize {
		end := offset + chunkSize
		if end > len(record) {
			end = len(record)
		}
		chunks = append(chunks, record[offset:end])
	}
	return appServerStreamRecord{chunks: chunks}
}

func startStreamingRemoteBroker(t *testing.T, transport AppServerLineTransport, handler AppServerServerRequestHandler) (*RemoteBroker, *websocket.Conn) {
	return startStreamingRemoteBrokerWithLimits(t, transport, handler, 0, 0)
}

func startStreamingRemoteBrokerWithLimits(t *testing.T, transport AppServerLineTransport, handler AppServerServerRequestHandler, prefixLimit, captureLimit int) (*RemoteBroker, *websocket.Conn) {
	t.Helper()
	broker, err := StartRemoteBroker(context.Background(), RemoteBrokerOptions{
		Starter: AppServerTransportStarterFunc(func(context.Context, AppServerStartRequest) (AppServerLineTransport, error) {
			return transport, nil
		}),
		ServerRequestHandler: handler,
	})
	if err != nil {
		t.Fatal(err)
	}
	if prefixLimit > 0 {
		broker.classificationPrefixLimit = prefixLimit
	}
	if captureLimit > 0 {
		broker.approvalCaptureLimit = captureLimit
	}
	connection, _, err := websocket.DefaultDialer.Dial(broker.URL(), http.Header{
		"Authorization": []string{"Bearer " + broker.AuthToken()},
	})
	if err != nil {
		_ = broker.Close(context.Background())
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = broker.Close(ctx)
	})
	return broker, connection
}

func newChannelAppServerTransport() *channelAppServerTransport {
	return &channelAppServerTransport{
		reads:  make(chan []byte, 16),
		writes: make(chan []byte, 16),
		closed: make(chan struct{}),
	}
}

func (t *channelAppServerTransport) WriteLine(ctx context.Context, line []byte) error {
	select {
	case t.writes <- append([]byte(nil), line...):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-t.closed:
		return io.EOF
	}
}

func (t *channelAppServerTransport) ReadLine(ctx context.Context) ([]byte, error) {
	select {
	case line := <-t.reads:
		return append([]byte(nil), line...), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-t.closed:
		return nil, io.EOF
	}
}

func (t *channelAppServerTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}
