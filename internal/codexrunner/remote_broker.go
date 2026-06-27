package codexrunner

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// RemoteBroker exposes one original Codex app-server stdio transport as a
// loopback WebSocket endpoint for the original Codex TUI. Approval requests
// are resolved locally; all other protocol messages remain byte-transparent.
type RemoteBroker struct {
	transport AppServerLineTransport
	handler   AppServerServerRequestHandler
	listener  net.Listener
	server    *http.Server
	url       string
	path      string
	done      chan error
	closeOnce sync.Once
	closeErr  error

	clientMu sync.Mutex
	client   bool

	transportWriteMu sync.Mutex
	clientWriteMu    sync.Mutex
	approvalMu       sync.Mutex
	approvals        map[string]*brokerApprovalState
	approvalOrder    []string
}

type brokerApprovalState struct {
	done     chan struct{}
	response []byte
}

type RemoteBrokerOptions struct {
	Starter              AppServerTransportStarter
	StartRequest         AppServerStartRequest
	ServerRequestHandler AppServerServerRequestHandler
	ListenAddress        string
}

func StartRemoteBroker(ctx context.Context, options RemoteBrokerOptions) (*RemoteBroker, error) {
	starter := options.Starter
	if starter == nil {
		starter = PolicyAppServerStarter{}
	}
	transport, err := starter.StartAppServer(ctx, options.StartRequest)
	if err != nil {
		return nil, err
	}
	listen := strings.TrimSpace(options.ListenAddress)
	if listen == "" {
		listen = "127.0.0.1:0"
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		_ = transport.Close()
		return nil, fmt.Errorf("listen for app-server broker: %w", err)
	}
	handler := options.ServerRequestHandler
	if handler == nil {
		handler = AutomaticApprovalHandler{Delay: DefaultApprovalDelay}
	}
	token, err := newRemoteBrokerCapability()
	if err != nil {
		_ = listener.Close()
		_ = transport.Close()
		return nil, err
	}
	path := "/_cxp/" + token
	broker := &RemoteBroker{
		transport: transport,
		handler:   handler,
		listener:  listener,
		url:       "ws://" + listener.Addr().String() + path,
		path:      path,
		done:      make(chan error, 1),
		approvals: make(map[string]*brokerApprovalState),
	}
	broker.server = &http.Server{
		Handler:           broker,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}
	go func() {
		err := broker.server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		broker.done <- err
		close(broker.done)
	}()
	return broker, nil
}

func (b *RemoteBroker) URL() string {
	if b == nil {
		return ""
	}
	return b.url
}

func (b *RemoteBroker) Done() <-chan error {
	if b == nil {
		closed := make(chan error)
		close(closed)
		return closed
	}
	return b.done
}

func (b *RemoteBroker) ServeHTTP(w http.ResponseWriter, request *http.Request) {
	if request.URL.Path != b.path {
		http.NotFound(w, request)
		return
	}
	b.clientMu.Lock()
	if b.client {
		b.clientMu.Unlock()
		http.Error(w, "app-server broker already has a client", http.StatusConflict)
		return
	}
	b.client = true
	b.clientMu.Unlock()
	defer func() {
		b.clientMu.Lock()
		b.client = false
		b.clientMu.Unlock()
	}()

	upgrader := websocket.Upgrader{
		CheckOrigin: func(request *http.Request) bool {
			origin := strings.TrimSpace(request.Header.Get("Origin"))
			return origin == "" || strings.Contains(origin, request.Host)
		},
		EnableCompression: true,
	}
	connection, err := upgrader.Upgrade(w, request, nil)
	if err != nil {
		return
	}
	defer connection.Close()
	b.serveConnection(request.Context(), connection)
}

func (b *RemoteBroker) serveConnection(parent context.Context, connection *websocket.Conn) {
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	errors := make(chan error, 2)
	go b.relayClientToAppServer(ctx, connection, errors)
	go b.relayAppServerToClient(ctx, connection, errors)
	<-errors
}

func (b *RemoteBroker) relayClientToAppServer(ctx context.Context, connection *websocket.Conn, done chan<- error) {
	for {
		messageType, payload, err := connection.ReadMessage()
		if err != nil {
			sendBrokerError(ctx, done, err)
			return
		}
		if messageType != websocket.TextMessage {
			continue
		}
		if err := b.writeTransport(ctx, payload); err != nil {
			sendBrokerError(ctx, done, err)
			return
		}
	}
}

func (b *RemoteBroker) relayAppServerToClient(ctx context.Context, connection *websocket.Conn, done chan<- error) {
	var approvalWrites sync.WaitGroup
	defer approvalWrites.Wait()
	for {
		line, err := b.transport.ReadLine(ctx)
		if err != nil {
			sendBrokerError(ctx, done, err)
			return
		}
		line = bytes.TrimSpace(line)
		var message appServerMessage
		if json.Unmarshal(line, &message) == nil && appServerMessageIsServerRequest(message) {
			approvalWrites.Add(1)
			go func(request appServerMessage, raw []byte) {
				defer approvalWrites.Done()
				b.handleServerRequest(ctx, connection, request, raw, done)
			}(message, append([]byte(nil), line...))
			continue
		}
		if err := b.writeClient(connection, line); err != nil {
			sendBrokerError(ctx, done, err)
			return
		}
	}
}

func newRemoteBrokerCapability() (string, error) {
	raw := make([]byte, 24)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate app-server broker capability: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func (b *RemoteBroker) writeTransport(ctx context.Context, line []byte) error {
	b.transportWriteMu.Lock()
	defer b.transportWriteMu.Unlock()
	return b.transport.WriteLine(ctx, line)
}

func (b *RemoteBroker) writeClient(connection *websocket.Conn, line []byte) error {
	b.clientWriteMu.Lock()
	defer b.clientWriteMu.Unlock()
	return connection.WriteMessage(websocket.TextMessage, line)
}

func (b *RemoteBroker) handleServerRequest(ctx context.Context, connection *websocket.Conn, request appServerMessage, raw []byte, done chan<- error) {
	key := string(bytes.TrimSpace(request.ID)) + "\x00" + strings.TrimSpace(request.Method)
	state, owner := b.approvalState(key)
	if !owner {
		select {
		case <-ctx.Done():
			return
		case <-state.done:
			if len(state.response) > 0 {
				if err := b.writeTransport(ctx, state.response); err != nil {
					sendBrokerError(ctx, done, err)
				}
			}
		}
		return
	}

	result, handled, err := b.handler.HandleServerRequest(ctx, request.Method, request.Params)
	if err != nil {
		b.finishApprovalState(state, nil)
		sendBrokerError(ctx, done, err)
		return
	}
	if !handled {
		b.finishApprovalState(state, nil)
		if err := b.writeClient(connection, raw); err != nil {
			sendBrokerError(ctx, done, err)
		}
		return
	}
	response := appServerResultResponse{JSONRPC: "2.0", ID: request.ID, Result: result}
	encoded, err := json.Marshal(response)
	if err != nil {
		b.finishApprovalState(state, nil)
		sendBrokerError(ctx, done, err)
		return
	}
	b.finishApprovalState(state, encoded)
	if err := b.writeTransport(ctx, encoded); err != nil {
		sendBrokerError(ctx, done, err)
	}
}

func (b *RemoteBroker) approvalState(key string) (*brokerApprovalState, bool) {
	b.approvalMu.Lock()
	defer b.approvalMu.Unlock()
	if state, ok := b.approvals[key]; ok {
		return state, false
	}
	state := &brokerApprovalState{done: make(chan struct{})}
	b.approvals[key] = state
	b.approvalOrder = append(b.approvalOrder, key)
	if len(b.approvalOrder) > 1024 {
		drop := b.approvalOrder[0]
		b.approvalOrder = b.approvalOrder[1:]
		if previous := b.approvals[drop]; previous != state {
			select {
			case <-previous.done:
				delete(b.approvals, drop)
			default:
			}
		}
	}
	return state, true
}

func (b *RemoteBroker) finishApprovalState(state *brokerApprovalState, response []byte) {
	b.approvalMu.Lock()
	state.response = append([]byte(nil), response...)
	close(state.done)
	b.approvalMu.Unlock()
}

func sendBrokerError(ctx context.Context, destination chan<- error, err error) {
	select {
	case destination <- err:
	case <-ctx.Done():
	}
}

func (b *RemoteBroker) Close(ctx context.Context) error {
	if b == nil {
		return nil
	}
	b.closeOnce.Do(func() {
		if b.server != nil {
			b.closeErr = b.server.Shutdown(ctx)
		}
		if b.transport != nil {
			if err := b.transport.Close(); b.closeErr == nil {
				b.closeErr = err
			}
		}
	})
	return b.closeErr
}
