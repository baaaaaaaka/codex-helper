// Package codex is a thin client of `codex app-server` (JSON-RPC over
// stdio): one process per running conversation, kept warm for a while and
// run in its own process group, with a PID file so the next helper process
// can kill servers a crashed one left behind before it reconciles turns.
package codex

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// Config describes how to launch an app-server.
type Config struct {
	Bin  string   // codex binary
	Args []string // extra arguments after "app-server"
	Env  []string // extra environment (KEY=VALUE), appended to os.Environ()
	Dir  string   // working directory of the process
	// Log receives stderr of the app-server and protocol diagnostics.
	Log io.Writer
	// ClientName is sent in initialize.clientInfo (default "codex-helper").
	ClientName string
	// OnServerRequest answers app-server initiated requests; nil uses
	// AutoApprove and rejects everything else.
	OnServerRequest func(method string, params json.RawMessage) (json.RawMessage, bool, error)
	// QuestionRequest retains an interactive request instead of auto-answering it.
	QuestionRequest func(*Server, json.RawMessage, json.RawMessage) bool
	Observe         func(*Server, Notification)
	// IgnoreServerRequest leaves requests to an attached native TUI.
	IgnoreServerRequest func(json.RawMessage) bool
	// Configure, when set, adjusts the command before it starts (the
	// caller's environment and execution identity); an error fails Start.
	Configure func(cmd *exec.Cmd) error
	// ConfigFile is the Codex config the server reads when it starts
	// ($CODEX_HOME/config.toml: MCP servers, providers, features).
	// Pool.AcquireCurrent replaces an idle server started before it
	// changed. "" = not watched.
	ConfigFile string
	// SharedDir enables native authenticated WebSocket clients. Empty keeps stdio.
	SharedDir string
	// SecureSharedFile applies the launch identity to private endpoint files.
	SecureSharedFile func(string) error
}

// ErrExited is returned for calls on (or turns of) a server that exited.
var ErrExited = errors.New("codex: app-server exited")

// RPCError is a JSON-RPC error answer.
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("codex app-server error %d: %s", e.Code, e.Message)
}

// Notification is a server notification.
type Notification struct {
	Method string
	Params json.RawMessage
}

// Server is one running app-server process.
type Server struct {
	cfg         Config
	waitProcess func() error // shared startup observes process exit before RPC connects
	cmd         *exec.Cmd
	pid         int
	endpoint    Endpoint
	cleanup     func()

	wmu sync.Mutex
	in  io.WriteCloser

	mu                sync.Mutex
	serverRequests    map[string]wireMsg // requests awaiting a TUI or helper answer
	questionBackendID uint64
	nextID            int64
	pending           map[int64]chan rpcResult
	subs              map[string]map[int]func(Notification)
	threads           map[string]*ThreadInfo // threadstate.go
	subSeq            int
	exitErr           error

	fault chan struct{}
	done  chan struct{} // closed after the process exited and the reader stopped
	wg    sync.WaitGroup
}

type rpcResult struct {
	result json.RawMessage
	err    error
}

type wireMsg struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *RPCError       `json:"error,omitempty"`
}

// Start launches `<bin> app-server <args>` and performs the initialize
// handshake.
func Start(ctx context.Context, cfg Config) (*Server, error) {
	if cfg.Bin == "" {
		return nil, errors.New("codex: binary not configured")
	}
	if cfg.Log == nil {
		cfg.Log = io.Discard
	}
	if cfg.ClientName == "" {
		cfg.ClientName = "codex-helper"
	}
	cmd := exec.Command(cfg.Bin, append([]string{"app-server"}, cfg.Args...)...)
	cmd.Env = append(os.Environ(), cfg.Env...)
	cmd.Dir = cfg.Dir
	// The start-up stderr is kept (bounded) for the error of a server that
	// exits before it answers initialize (a missing Node runtime, a broken
	// install: F-TRN-16 classifies it); after initialize nothing is kept.
	startErr := &startTail{}
	cmd.Stderr = io.MultiWriter(cfg.Log, startErr)
	configure(cmd)
	if cfg.Configure != nil {
		if err := cfg.Configure(cmd); err != nil {
			return nil, fmt.Errorf("codex: prepare app-server: %w", err)
		}
	}
	if cfg.SharedDir != "" {
		return startShared(ctx, cfg, cmd, startErr)
	}
	in, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("codex: start app-server: %w", err)
	}
	s := &Server{
		cfg: cfg, cmd: cmd, pid: cmd.Process.Pid, in: in,
		pending: map[int64]chan rpcResult{}, subs: map[string]map[int]func(Notification){},
		done: make(chan struct{}), fault: make(chan struct{}),
	}
	go s.readLoop(out)
	if err := s.initialize(ctx); err != nil {
		s.Kill() // waits for the exit, so stderr is complete
		if tail := startErr.close(); tail != "" {
			return nil, fmt.Errorf("%w (app-server stderr: %s)", err, tail)
		}
		return nil, err
	}
	startErr.close()
	return s, nil
}

// startTail keeps the last startTailMax bytes of stderr until close.
type startTail struct {
	mu     sync.Mutex
	buf    []byte
	closed bool
}

const startTailMax = 600

func (t *startTail) Write(p []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.closed {
		t.buf = append(t.buf, p...)
		if len(t.buf) > startTailMax {
			t.buf = append(t.buf[:0], t.buf[len(t.buf)-startTailMax:]...)
		}
	}
	return len(p), nil
}

// close stops keeping stderr and returns what was kept, on one line.
func (t *startTail) close() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	out := strings.Join(strings.Fields(string(t.buf)), " ")
	t.buf = nil
	return out
}

// PID is the process id (also the process group id on unix).
func (s *Server) PID() int { return s.pid }

// Done is closed when the process has exited.
func (s *Server) Done() <-chan struct{} { return s.done }

// Exited reports whether the process has exited.
func (s *Server) Exited() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

func (s *Server) initialize(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	params := map[string]any{
		"clientInfo":   map[string]string{"name": s.cfg.ClientName, "version": "0"},
		"capabilities": map[string]any{"experimentalApi": true},
	}
	if err := s.Call(ctx, "initialize", params, nil); err != nil {
		return fmt.Errorf("codex: initialize: %w", err)
	}
	return s.write(wireMsg{Method: "initialized", Params: json.RawMessage(`{}`)})
}

func (s *Server) write(m wireMsg) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	s.wmu.Lock()
	defer s.wmu.Unlock()
	_, err = s.in.Write(append(data, '\n'))
	return err
}

// Call sends a request and decodes its result into out (nil to discard).
func (s *Server) Call(ctx context.Context, method string, params, out any) error {
	raw, err := json.Marshal(params)
	if err != nil {
		return err
	}
	ch := make(chan rpcResult, 1)
	s.mu.Lock()
	if s.exitErr != nil {
		s.mu.Unlock()
		return ErrExited
	}
	s.nextID++
	id := s.nextID
	s.pending[id] = ch
	s.mu.Unlock()
	drop := func() {
		s.mu.Lock()
		delete(s.pending, id)
		s.mu.Unlock()
	}
	if err := s.write(wireMsg{ID: json.RawMessage(fmt.Sprint(id)), Method: method, Params: raw}); err != nil {
		drop()
		if s.exitAfterWrite(ctx) {
			return ErrExited
		}
		return fmt.Errorf("codex: write %s: %w", method, err)
	}
	select {
	case r := <-ch:
		if r.err != nil {
			return r.err
		}
		if out != nil && len(r.result) > 0 {
			if err := json.Unmarshal(r.result, out); err != nil {
				return fmt.Errorf("codex: decode %s result: %w", method, err)
			}
		}
		return nil
	case <-ctx.Done():
		drop()
		return ctx.Err()
	}
}

// writeExitWait bounds how long a failed write waits for the server's exit.
const writeExitWait = 5 * time.Second

// exitAfterWrite reports whether the server exits within writeExitWait of
// a failed write. A write fails when the server closed its input, which it
// does by exiting (a missing Node runtime exits at once): whether the write
// or the reader sees that first is a race, and the error (shown to the
// owner by the setup notice, F-TRN-16) is the exit either way.
func (s *Server) exitAfterWrite(ctx context.Context) bool {
	t := time.NewTimer(writeExitWait)
	defer t.Stop()
	select {
	case <-s.done:
		return true
	case <-t.C:
	case <-ctx.Done():
	}
	return s.Exited()
}

// Subscribe registers fn for notifications whose params carry threadId ==
// threadID ("" receives every notification). fn runs on the reader
// goroutine and must not block. The returned func unsubscribes.
func (s *Server) Subscribe(threadID string, fn func(Notification)) func() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subSeq++
	n := s.subSeq
	if s.subs[threadID] == nil {
		s.subs[threadID] = map[int]func(Notification){}
	}
	s.subs[threadID][n] = fn
	return func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.subs[threadID], n)
		if len(s.subs[threadID]) == 0 {
			delete(s.subs, threadID)
		}
	}
}

func (s *Server) readLoop(out io.Reader) {
	r := bufio.NewReaderSize(out, 1<<20)
	for {
		line, err := r.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) > 0 {
			s.handleLine(line)
		}
		if err != nil {
			break
		}
	}
	s.failPending()
	var werr error
	if s.waitProcess != nil {
		werr = s.waitProcess()
	} else {
		werr = s.cmd.Wait()
	}
	s.wg.Wait() // server-request answers in flight
	s.mu.Lock()
	if werr == nil {
		werr = ErrExited
	}
	s.exitErr = werr
	pend := s.pending
	s.pending = map[int64]chan rpcResult{}
	s.mu.Unlock()
	for _, ch := range pend {
		ch <- rpcResult{err: ErrExited}
	}
	if s.cleanup != nil {
		s.cleanup()
	}
	close(s.done)
}

func (s *Server) handleLine(line []byte) {
	var m wireMsg
	if err := json.Unmarshal(line, &m); err != nil {
		fmt.Fprintf(s.cfg.Log, "codex: bad line from app-server: %v\n", err)
		return
	}
	switch {
	case m.Method != "" && len(m.ID) > 0:
		s.mu.Lock()
		if s.serverRequests == nil {
			s.serverRequests = map[string]wireMsg{}
		}
		s.serverRequests[string(m.ID)] = m
		s.mu.Unlock()
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.answer(m)
		}()
	case m.Method != "":
		var p struct {
			ThreadID string `json:"threadId"`
		}
		_ = json.Unmarshal(m.Params, &p)
		if m.Method == "serverRequest/resolved" {
			var resolved struct {
				RequestID json.RawMessage `json:"requestId"`
			}
			if json.Unmarshal(m.Params, &resolved) == nil {
				s.mu.Lock()
				delete(s.serverRequests, string(resolved.RequestID))
				s.mu.Unlock()
			}
		}
		s.track(m.Method, p.ThreadID, m.Params)
		n := Notification{Method: m.Method, Params: m.Params}
		if s.cfg.Observe != nil {
			s.cfg.Observe(s, n)
		}
		s.mu.Lock()
		var fns []func(Notification)
		for _, fn := range s.subs[p.ThreadID] {
			fns = append(fns, fn)
		}
		if p.ThreadID != "" {
			for _, fn := range s.subs[""] {
				fns = append(fns, fn)
			}
		}
		s.mu.Unlock()
		for _, fn := range fns {
			fn(n)
		}
	case len(m.ID) > 0:
		var id int64
		if err := json.Unmarshal(m.ID, &id); err != nil {
			return
		}
		s.mu.Lock()
		ch := s.pending[id]
		delete(s.pending, id)
		s.mu.Unlock()
		if ch == nil {
			return
		}
		if m.Error != nil {
			ch <- rpcResult{err: m.Error}
		} else {
			ch <- rpcResult{result: m.Result}
		}
	}
}

func (s *Server) answer(m wireMsg) {
	if m.Method == "item/tool/requestUserInput" && s.cfg.QuestionRequest != nil && s.cfg.QuestionRequest(s, m.ID, m.Params) {
		return
	}
	if s.cfg.IgnoreServerRequest != nil && s.cfg.IgnoreServerRequest(m.Params) {
		return
	}
	s.mu.Lock()
	_, pending := s.serverRequests[string(m.ID)]
	delete(s.serverRequests, string(m.ID))
	s.mu.Unlock()
	if !pending {
		return
	} // another client resolved it, or detach already answered
	handler := s.cfg.OnServerRequest
	if handler == nil {
		handler = AutoApprove
	}
	res, handled, err := handler(m.Method, m.Params)
	reply := wireMsg{ID: m.ID}
	switch {
	case err != nil:
		reply.Error = &RPCError{Code: -32603, Message: err.Error()}
	case !handled:
		reply.Error = &RPCError{Code: -32601, Message: m.Method + " is not supported by codex-helper"}
	default:
		reply.Result = res
	}
	if err := s.write(reply); err != nil && !s.Exited() {
		fmt.Fprintf(s.cfg.Log, "codex: answer %s: %v\n", m.Method, err)
	}
}

// Close ends the server. Stdio gets an EOF grace period; a shared backend
// needs an explicit termination signal because client disconnect is not exit.
func (s *Server) Close(grace time.Duration) {
	s.wmu.Lock()
	_ = s.in.Close()
	s.wmu.Unlock()
	if s.endpoint.URL == "" {
		select {
		case <-s.done:
			return
		case <-time.After(grace):
		}
	}
	_ = terminateGroup(s.pid)
	select {
	case <-s.done:
		return
	case <-time.After(2 * time.Second):
	}
	s.Kill()
}

// Kill kills the whole process group at once and waits for the reader.
func (s *Server) Kill() {
	_ = killGroup(s.pid)
	s.wmu.Lock()
	_ = s.in.Close()
	s.wmu.Unlock()
	select {
	case <-s.done:
	case <-time.After(10 * time.Second):
	}
}

// IsActiveWriter reports a turn/start rejection because another Codex
// process (the TUI) holds the thread. Provisional match until the real
// error shape is confirmed by the probe.
func IsActiveWriter(err error) bool {
	var re *RPCError
	if !errors.As(err, &re) {
		return false
	}
	low := strings.ToLower(re.Message)
	return strings.Contains(low, "active writer") || strings.Contains(low, "in use by another")
}

func (s *Server) failPending() {
	s.mu.Lock()
	s.exitErr = ErrExited
	pend := s.pending
	s.pending = map[int64]chan rpcResult{}
	s.mu.Unlock()
	for _, ch := range pend {
		ch <- rpcResult{err: ErrExited}
	}
	if s.fault != nil {
		close(s.fault)
	}
}

// Disconnected reports loss of the helper RPC connection, even if native TUI clients remain.
func (s *Server) Disconnected() bool {
	select {
	case <-s.fault:
		return true
	default:
		return false
	}
}
