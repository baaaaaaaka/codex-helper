package codexrunner

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
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

type channelAppServerTransport struct {
	reads     chan []byte
	writes    chan []byte
	closed    chan struct{}
	closeOnce sync.Once
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
