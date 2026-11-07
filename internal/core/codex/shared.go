package codex

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/gorilla/websocket"
)

// Endpoint is a capability for one native app-server. Never log Token.
type Endpoint struct {
	URL   string `json:"url"`
	Token string `json:"token"`
}

func (s *Server) Endpoint() Endpoint { return s.endpoint }

// socketStream adapts native WebSocket frames to the existing RPC reader.
// One RPC write is one frame; reads retain at most one frame.
type socketStream struct {
	c       *websocket.Conn
	frame   io.Reader
	newline bool
}

func (w *socketStream) Write(p []byte) (int, error) {
	_ = w.c.SetWriteDeadline(time.Now().Add(30 * time.Second))
	if err := w.c.WriteMessage(websocket.TextMessage, p); err != nil {
		return 0, err
	}
	return len(p), nil
}
func (w *socketStream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		if w.newline {
			w.newline = false
			p[0] = '\n'
			return 1, nil
		}
		if w.frame == nil {
			_, r, err := w.c.NextReader()
			if err != nil {
				return 0, err
			}
			w.frame = r
		}
		n, err := w.frame.Read(p)
		if err == io.EOF {
			w.frame = nil
			w.newline = true
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
}
func (w *socketStream) Close() error { return w.c.Close() }

func startShared(ctx context.Context, cfg Config, cmd *exec.Cmd, tail *startTail) (*Server, error) {
	dir, err := os.MkdirTemp(cfg.SharedDir, "app-server-*")
	if err != nil {
		return nil, err
	}
	cleanup := func() { _ = os.RemoveAll(dir) }
	ok := false
	defer func() {
		if !ok {
			cleanup()
		}
	}()
	secret := make([]byte, 32)
	if _, err = rand.Read(secret); err != nil {
		return nil, err
	}
	token := hex.EncodeToString(secret)
	path := filepath.Join(dir, "token")
	if err = os.WriteFile(path, []byte(token), 0600); err != nil {
		return nil, err
	}
	if cfg.SecureSharedFile != nil {
		for _, p := range []string{dir, path} {
			if err = cfg.SecureSharedFile(p); err != nil {
				return nil, err
			}
		}
	}
	// Native CLI accepts an address, not an inherited listener. Reserve an
	// ephemeral loopback port, then release it immediately before launch.
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	url := "ws://" + listener.Addr().String()
	_ = listener.Close()
	cmd.Args = append(cmd.Args, "--listen", url, "--ws-auth", "capability-token", "--ws-token-file", path)
	cmd.Stdout = cfg.Log
	configureSharedLifetime(cmd)
	if err = cmd.Start(); err != nil {
		return nil, fmt.Errorf("codex: start shared app-server: %w", err)
	}
	releaseProcess, err := attachSharedLifetime(cmd)
	if err != nil {
		_ = killGroup(cmd.Process.Pid)
		_ = cmd.Wait()
		return nil, fmt.Errorf("codex: protect shared app-server lifetime: %w", err)
	}
	processExit := make(chan error, 1)
	go func() {
		err := cmd.Wait()
		releaseProcess()
		processExit <- err
	}()
	s := &Server{cfg: cfg, cmd: cmd, pid: cmd.Process.Pid, endpoint: Endpoint{URL: url, Token: token}, cleanup: cleanup,
		waitProcess: func() error { return <-processExit },
		pending:     map[int64]chan rpcResult{}, subs: map[string]map[int]func(Notification){}, done: make(chan struct{}), fault: make(chan struct{})}
	// A bound startup retry, not a runtime poll. Proxy configuration must
	// never send the local capability through an HTTP proxy.
	dialer := websocket.Dialer{HandshakeTimeout: time.Second}
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case exitErr := <-processExit:
			return nil, fmt.Errorf("codex: shared app-server exited before listening: %v (app-server stderr: %s)", exitErr, tail.close())
		default:
		}
		conn, _, dialErr := dialer.DialContext(ctx, url, http.Header{"Authorization": []string{"Bearer " + token}})
		if dialErr == nil {
			stream := &socketStream{c: conn}
			s.in = stream
			go s.readLoop(stream)
			break
		}
		select {
		case <-ctx.Done():
			err = ctx.Err()
		case <-deadline.C:
			err = fmt.Errorf("codex: shared app-server did not listen: %w", dialErr)
		case <-time.After(20 * time.Millisecond):
			continue
		}
		_ = killGroup(s.pid)
		_ = <-processExit
		return nil, err
	}
	if err = s.initialize(ctx); err != nil {
		s.Kill()
		return nil, fmt.Errorf("%w (app-server stderr: %s)", err, tail.close())
	}
	tail.close()
	// Native Codex has loaded the token. Clients use the in-memory capability;
	// the startup credential need not remain on disk for the server lifetime.
	cleanup()
	ok = true
	return s, nil
}

// AnswerPendingRequests transfers outstanding approvals back to the helper
// when the last TUI leaves. Resolved requests are removed by the reader.
func (s *Server) AnswerPendingRequests(thread string) {
	s.mu.Lock()
	var requests []wireMsg
	for _, m := range s.serverRequests {
		var p struct {
			ThreadID string `json:"threadId"`
		}
		if json.Unmarshal(m.Params, &p) == nil && p.ThreadID == thread {
			requests = append(requests, m)
		}
	}
	s.mu.Unlock()
	for _, m := range requests {
		s.answer(m)
	}
}
