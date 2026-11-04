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
	cfg Config
	cmd *exec.Cmd
	pid int

	wmu sync.Mutex
	in  io.WriteCloser

	mu      sync.Mutex
	nextID  int64
	pending map[int64]chan rpcResult
	subs    map[string]map[int]func(Notification)
	subSeq  int
	exitErr error

	done chan struct{} // closed after the process exited and the reader stopped
	wg   sync.WaitGroup
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
	cmd.Stderr = cfg.Log
	configure(cmd)
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
		done: make(chan struct{}),
	}
	go s.readLoop(out)
	if err := s.initialize(ctx); err != nil {
		s.Kill()
		return nil, err
	}
	return s, nil
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
		if s.Exited() {
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
	werr := s.cmd.Wait()
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
		n := Notification{Method: m.Method, Params: m.Params}
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

// Close ends the server: stdin is closed (Codex exits on EOF), then the
// process group is terminated after grace and killed 2 s later.
func (s *Server) Close(grace time.Duration) {
	s.wmu.Lock()
	_ = s.in.Close()
	s.wmu.Unlock()
	select {
	case <-s.done:
		return
	case <-time.After(grace):
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
