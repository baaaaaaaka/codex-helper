package realcodex

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// WriteCodexHome writes a throwaway CODEX_HOME whose only model provider is
// the fake Responses API at baseURL, with retries disabled.
func WriteCodexHome(dir, baseURL string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	cfg := fmt.Sprintf(`model = "probe-model"
model_provider = "probe"
approval_policy = "never"
sandbox_mode = "read-only"
check_for_update_on_startup = false

[model_providers.probe]
name = "Probe"
base_url = %q
env_key = "PROBE_API_KEY"
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
stream_idle_timeout_ms = 600000
`, baseURL)
	return os.WriteFile(filepath.Join(dir, "config.toml"), []byte(cfg), 0o600)
}

// CodexEnv is the environment for a codex process using home as CODEX_HOME.
func CodexEnv(home string) []string {
	env := []string{
		"CODEX_HOME=" + home,
		"HOME=" + filepath.Dir(home),
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"PROBE_API_KEY=probe-key",
		"RUST_LOG=warn",
		"NO_COLOR=1",
	}
	if v := os.Getenv("TMPDIR"); v != "" {
		env = append(env, "TMPDIR="+v)
	}
	return env
}

// RPCMessage is one JSON-RPC message from the app-server.
type RPCMessage struct {
	ID     json.RawMessage `json:"id,omitempty"`
	Method string          `json:"method,omitempty"`
	Params json.RawMessage `json:"params,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *RPCError       `json:"error,omitempty"`
	At     time.Time       `json:"-"`
	Raw    string          `json:"-"`
}

// RPCError is a JSON-RPC error object.
type RPCError struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Data    json.RawMessage `json:"data,omitempty"`
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("rpc error %d: %s (data %s)", e.Code, e.Message, string(e.Data))
}

// AppServer is a running `codex app-server` with a minimal client.
type AppServer struct {
	Cmd   *exec.Cmd
	stdin io.WriteCloser

	mu      sync.Mutex
	nextID  int
	pending map[string]chan RPCMessage
	notes   []RPCMessage
	noteCh  chan struct{}
	done    chan struct{}
	exitErr error
}

// StartAppServer launches bin app-server. stderr goes to logPath.
func StartAppServer(bin, home, logPath string, configure func(*exec.Cmd)) (*AppServer, error) {
	cmd := exec.Command(bin, "app-server")
	cmd.Env = CodexEnv(home)
	cmd.Dir = filepath.Dir(home)
	logf, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	cmd.Stderr = logf
	if configure != nil {
		configure(cmd)
	}
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	a := &AppServer{Cmd: cmd, stdin: stdin, pending: map[string]chan RPCMessage{}, noteCh: make(chan struct{}, 1), done: make(chan struct{})}
	go a.read(stdout, logf)
	return a, nil
}

func (a *AppServer) read(stdout io.Reader, logf *os.File) {
	sc := bufio.NewScanner(stdout)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		line := sc.Text()
		var m RPCMessage
		if json.Unmarshal([]byte(line), &m) != nil {
			continue
		}
		m.At, m.Raw = time.Now(), line
		a.mu.Lock()
		if len(m.ID) > 0 && m.Method == "" {
			if ch := a.pending[string(m.ID)]; ch != nil {
				delete(a.pending, string(m.ID))
				ch <- m
			}
		} else if len(m.ID) > 0 && m.Method != "" {
			// A server request (approval etc.): refuse it.
			go a.send(map[string]any{"id": m.ID, "error": map[string]any{"code": -32601, "message": "probe client does not handle " + m.Method}})
			a.notes = append(a.notes, m)
		} else {
			a.notes = append(a.notes, m)
		}
		a.mu.Unlock()
		select {
		case a.noteCh <- struct{}{}:
		default:
		}
	}
	a.exitErr = a.Cmd.Wait()
	logf.Close()
	close(a.done)
}

func (a *AppServer) send(v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.stdin == nil {
		return fmt.Errorf("stdin closed")
	}
	_, err = a.stdin.Write(append(data, '\n'))
	return err
}

// Call sends a request and waits for its response.
func (a *AppServer) Call(method string, params any, timeout time.Duration) (json.RawMessage, *RPCError, error) {
	a.mu.Lock()
	a.nextID++
	id := fmt.Sprint(a.nextID)
	ch := make(chan RPCMessage, 1)
	a.pending[id] = ch
	a.mu.Unlock()
	if err := a.send(map[string]any{"id": a.nextIDValue(id), "method": method, "params": params}); err != nil {
		return nil, nil, err
	}
	select {
	case m := <-ch:
		return m.Result, m.Error, nil
	case <-a.done:
		return nil, nil, fmt.Errorf("%s: app-server exited (%v)", method, a.exitErr)
	case <-time.After(timeout):
		return nil, nil, fmt.Errorf("%s: no response within %s", method, timeout)
	}
}

func (a *AppServer) nextIDValue(id string) any {
	var n int
	fmt.Sscan(id, &n)
	return n
}

// Notify sends a notification.
func (a *AppServer) Notify(method string, params any) error {
	return a.send(map[string]any{"method": method, "params": params})
}

// Initialize performs the initialize handshake the helper uses.
func (a *AppServer) Initialize() error {
	_, rerr, err := a.Call("initialize", map[string]any{
		"clientInfo":   map[string]any{"name": "codex-helper-probe", "version": "0"},
		"capabilities": map[string]any{"experimentalApi": true},
	}, 60*time.Second)
	if err != nil {
		return err
	}
	if rerr != nil {
		return rerr
	}
	return a.Notify("initialized", map[string]any{})
}

// CloseStdin closes the app-server's stdin (client-side EOF).
func (a *AppServer) CloseStdin() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.stdin == nil {
		return nil
	}
	err := a.stdin.Close()
	a.stdin = nil
	return err
}

// Done is closed when the process exited and stdout drained.
func (a *AppServer) Done() <-chan struct{} { return a.done }

// ExitErr is the process exit error (valid after Done).
func (a *AppServer) ExitErr() error { return a.exitErr }

// Notes returns a copy of all notifications so far.
func (a *AppServer) Notes() []RPCMessage {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]RPCMessage(nil), a.notes...)
}

// WaitNote waits for a notification matching pred.
func (a *AppServer) WaitNote(pred func(RPCMessage) bool, timeout time.Duration) (RPCMessage, bool) {
	deadline := time.After(timeout)
	seen := 0
	for {
		notes := a.Notes()
		for ; seen < len(notes); seen++ {
			if pred(notes[seen]) {
				return notes[seen], true
			}
		}
		select {
		case <-a.noteCh:
		case <-deadline:
			return RPCMessage{}, false
		case <-a.done:
			notes = a.Notes()
			for ; seen < len(notes); seen++ {
				if pred(notes[seen]) {
					return notes[seen], true
				}
			}
			return RPCMessage{}, false
		}
	}
}

// Kill kills the process if it is still running.
func (a *AppServer) Kill() {
	if a.Cmd.Process != nil {
		_ = a.Cmd.Process.Kill()
	}
	select {
	case <-a.done:
	case <-time.After(10 * time.Second):
	}
}

// IsMethod returns a predicate for notifications with that method (and,
// optionally, a params substring).
func IsMethod(method string, contains ...string) func(RPCMessage) bool {
	return func(m RPCMessage) bool {
		if m.Method != method {
			return false
		}
		for _, c := range contains {
			if !strings.Contains(string(m.Params), c) {
				return false
			}
		}
		return true
	}
}

// FindRollout returns the rollout file of threadID under home/sessions.
func FindRollout(home, threadID string) string {
	var found string
	_ = filepath.Walk(filepath.Join(home, "sessions"), func(p string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && strings.Contains(filepath.Base(p), threadID) {
			found = p
		}
		return nil
	})
	return found
}
