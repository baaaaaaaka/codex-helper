package codexrunner

import (
	"context"
	"encoding/json"
	"io"
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
	connection, _, err := websocket.DefaultDialer.Dial(broker.URL(), nil)
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
