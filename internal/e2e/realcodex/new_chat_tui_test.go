//go:build linux

package realcodex

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
	_ "modernc.org/sqlite"
)

// A real helper, real CXP selector and real Codex TUI. Only the remote Graph
// and model services are fakes. Run inside the offline contract container.
func TestRealCodexNewChatTUI(t *testing.T) {
	native := codexBin(t)
	cxp := os.Getenv("E2E_CXP_BIN")
	if cxp == "" {
		t.Fatal("E2E_CXP_BIN is required when native contracts are enabled")
	}
	if _, err := exec.LookPath("script"); err != nil {
		t.Fatal("PTY supervisor script is required:", err)
	}
	fg := fakegraph.New(fakegraph.Options{})
	t.Cleanup(fg.Close)
	env := harness.NewEnv(t, fg)
	resp := NewResponses()
	t.Cleanup(resp.Close)
	if err := WriteCodexHome(env.CodexHome, resp.URL()); err != nil {
		t.Fatal(err)
	}
	config := env.Vars["CODEX_HELPER_CONFIG"]
	if err := os.MkdirAll(filepath.Dir(config), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config, []byte(`{"version":6,"proxyEnabled":false,"agentAutoApproveEnabled":false,"profiles":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	vars := map[string]string{}
	for k, v := range env.Vars {
		vars[k] = v
	}
	vars["PATH"] = "/usr/local/bin:/usr/bin:/bin"
	vars["PROBE_API_KEY"] = "probe-key"
	vars["TERM"] = "xterm-256color"
	vars["CXP_AUTO_UPDATE"] = "0"
	vars[harness.EnvSeam] = harness.Seam{}.Value(fg.URL(), "native-new-chat", 50*time.Millisecond)
	var processEnv []string
	for k, v := range vars {
		processEnv = append(processEnv, k+"="+v)
	}
	sort.Strings(processEnv)
	helper := harness.NewProcessSUT(harness.SUTConfig{Env: env}, harness.ProcessSpec{
		Name: "native-new-chat", Binary: cxp, Dir: env.Home,
		Args: []string{"teams", "run", "--codex-path", native, "--machine-registry=false", "--auto-update=false", "--auto-service=false"},
		Env:  func(string) []string { return processEnv },
	})
	ctx, cancel := context.WithTimeout(context.Background(), 110*time.Second)
	defer cancel()
	if err := helper.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		if err := helper.Stop(stopCtx); err != nil {
			t.Error(err)
		}
	})
	t.Cleanup(func() {
		if t.Failed() {
			t.Log("helper:\n" + helper.Output())
			for _, r := range resp.Requests() {
				t.Logf("model input: %q structured=%v tools=%v", r.UserText, len(r.Schema) > 0, r.ToolNames)
			}
			_ = filepath.WalkDir(env.CodexHome, func(p string, d os.DirEntry, err error) error {
				if err == nil && !d.IsDir() && strings.HasSuffix(d.Name(), ".log") {
					if b, e := os.ReadFile(p); e == nil {
						t.Log(p + ":\n" + tail(string(b), 4000))
					}
				}
				return nil
			})
		}
	})
	waitNative(t, ctx, "helper ready", func() bool {
		for _, m := range fg.AllMessages() {
			if strings.Contains(m.PlainText(), "control chat is ready") {
				return true
			}
		}
		return false
	})
	// Fresh empty windows must not create model requests or a Teams Work chat.
	emptyDir := env.WorkDir(t, "empty")
	empty := startNativePTY(t, ctx, cxp, []string{"history", "tui", "--codex-path", native, "--refresh-interval", "0"}, emptyDir, processEnv)
	openNativeNew(t, ctx, empty)
	empty.stop()
	if len(resp.Requests()) != 0 || len(fg.Chats()) != 1 {
		t.Fatalf("empty window created work: model=%d chats=%d", len(resp.Requests()), len(fg.Chats()))
	}
	// Keep two windows open together. The selector's current directory is the
	// project for each new thread; no rollout or fake user input is pre-seeded.
	dirs := []string{env.WorkDir(t, "first"), env.WorkDir(t, "second")}
	windows := make([]*nativePTY, 2)
	for i, dir := range dirs {
		windows[i] = startNativePTY(t, ctx, cxp, []string{"history", "tui", "--codex-path", native, "--refresh-interval", "0"}, dir, processEnv)
		openNativeNew(t, ctx, windows[i])
	}
	if len(resp.Requests()) != 0 {
		t.Fatal("model invoked before first user input")
	}
	// Every app-server must belong directly to this isolated helper. A TUI
	// silently falling back to its own competing backend must fail acceptance.
	processes, err := os.ReadDir("/proc")
	if err != nil {
		t.Fatal(err)
	}
	backends := 0
	for _, p := range processes {
		cmdline, err := os.ReadFile(filepath.Join("/proc", p.Name(), "cmdline"))
		if err != nil || !bytes.HasPrefix(cmdline, []byte(native+"\x00app-server\x00")) {
			continue
		}
		backends++
		status, err := os.ReadFile(filepath.Join("/proc", p.Name(), "status"))
		if err != nil {
			t.Fatal(err)
		}
		for _, line := range strings.Split(string(status), "\n") {
			if strings.HasPrefix(line, "PPid:") && strings.TrimSpace(strings.TrimPrefix(line, "PPid:")) != fmt.Sprint(helper.PID()) {
				t.Fatalf("competing backend outside helper: %s", line)
			}
		}
	}
	if backends < 2 {
		t.Fatalf("missing shared backends: %d", backends)
	}
	for i, w := range windows {
		if i == 0 {
			resp.ToolOnce("exec_command", map[string]any{"cmd": "printf native-approval-command", "sandbox_permissions": "require_escalated", "justification": "native approval contract"})
		}
		prompt := fmt.Sprintf("native-window-%d-first", i)
		w.submit(t, ctx, prompt)
		if i == 0 {
			waitNative(t, ctx, "native TUI approval", func() bool { return strings.Contains(w.text(), "Would you like to run") })
			if strings.Contains(windows[1].text(), "native-approval-command") {
				t.Fatal("approval routed to another window")
			}
			w.send(t, "\r") // approve only the harmless printf inside this offline container
		}
		waitNative(t, ctx, "first response in native TUI", func() bool { return strings.Contains(w.text(), "ANSWER("+prompt+")") })
	}
	windows[0].submit(t, ctx, "native-window-0-second")
	waitNative(t, ctx, "second response in same TUI", func() bool { return strings.Contains(windows[0].text(), "ANSWER(native-window-0-second)") })
	// Inspect only externally persisted state. No product imports or fake native
	// app-server take part in these assertions.
	var files []string
	waitNative(t, ctx, "two real rollouts", func() bool {
		files = nil
		_ = filepath.WalkDir(env.SessionsDir, func(p string, d os.DirEntry, err error) error {
			if err == nil && !d.IsDir() && strings.HasSuffix(p, ".jsonl") {
				files = append(files, p)
			}
			return nil
		})
		return len(files) == 2
	})
	ids := map[string]string{}
	for _, file := range files {
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var id, cwd string
		var inputs []string
		for _, line := range bytes.Split(b, []byte{'\n'}) {
			var row struct {
				Type    string
				Payload struct {
					ID, Cwd, Type string
					Item          struct {
						Type    string
						Content []struct{ Text string }
					}
				}
			}
			if json.Unmarshal(line, &row) != nil {
				continue
			}
			if row.Type == "session_meta" {
				id, cwd = row.Payload.ID, row.Payload.Cwd
			}
			if row.Type == "event_msg" && row.Payload.Type == "item_completed" && row.Payload.Item.Type == "UserMessage" {
				var parts []string
				for _, part := range row.Payload.Item.Content {
					parts = append(parts, part.Text)
				}
				inputs = append(inputs, strings.Join(parts, ""))
			}
		}
		want := []string{"native-window-1-first"}
		if cwd == dirs[0] {
			want = []string{"native-window-0-first", "native-window-0-second"}
		} else if cwd != dirs[1] {
			t.Fatalf("wrong cwd: %s", cwd)
		}
		if strings.Join(inputs, "|") != strings.Join(want, "|") || id == "" {
			t.Log("rollout tail:\n" + tail(string(b), 7000))
			t.Fatalf("hidden, lost or mixed user turns: cwd=%s id=%s inputs=%v", cwd, id, inputs)
		}
		ids[cwd] = id
	}
	if len(ids) != 2 {
		t.Fatal("windows shared a thread")
	}
	var core string
	_ = filepath.WalkDir(env.StateDir, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && d.Name() == "core.sqlite" {
			core = p
		}
		return nil
	})
	db, err := sql.Open("sqlite", "file:"+core+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for cwd, id := range ids {
		var n int
		if err := db.QueryRow(`SELECT count(*) FROM threads WHERE thread_id=? AND cwd=?`, id, cwd).Scan(&n); err != nil || n != 1 {
			t.Fatalf("helper thread binding %s: n=%d err=%v", id, n, err)
		}
	}
	// Close a real native window, then resume its persisted thread through CXP.
	windows[0].stop()
	resumed := startNativePTY(t, ctx, cxp, []string{"history", "open", ids[dirs[0]], "--codex-path", native}, dirs[0], processEnv)
	waitNative(t, ctx, "resumed native input", func() bool { return strings.Contains(resumed.text(), "›") })
	resumed.submit(t, ctx, "native-resumed-third")
	waitNative(t, ctx, "response after resume", func() bool { return strings.Contains(resumed.text(), "ANSWER(native-resumed-third)") })
	var actual []string
	for _, r := range resp.Requests() {
		if len(r.Schema) == 0 {
			actual = append(actual, r.UserText)
		}
	}
	// The tool causes one model continuation in the first user turn.
	if strings.Join(actual, "|") != "native-window-0-first|native-window-0-first|native-window-1-first|native-window-0-second|native-resumed-third" {
		t.Fatalf("hidden or repeated user input: %v", actual)
	}
	for _, v := range fg.Violations() {
		t.Errorf("Graph violation: %+v", v)
	}
}

func waitNative(t *testing.T, ctx context.Context, what string, ok func() bool) {
	t.Helper()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	deadline := time.NewTimer(20 * time.Second)
	defer deadline.Stop()
	for {
		if ok() {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("timeout waiting for " + what)
		case <-ctx.Done():
			t.Fatal("timeout waiting for " + what)
		case <-tick.C:
		}
	}
}

type nativePTY struct {
	cmd     *exec.Cmd
	in      io.WriteCloser
	mu      sync.Mutex
	inputMu sync.Mutex
	output  bytes.Buffer
	once    sync.Once
	done    chan struct{}
}

var terminalCSI = regexp.MustCompile("\x1b\\[[0-?]*[ -/]*[@-~]")

func (p *nativePTY) text() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return terminalCSI.ReplaceAllString(p.output.String(), "")
}
func (p *nativePTY) Write(b []byte) (int, error) {
	p.mu.Lock()
	p.output.Write(b)
	p.mu.Unlock()
	if bytes.Contains(b, []byte("\x1b[6n")) {
		p.inputMu.Lock()
		_, _ = p.in.Write([]byte("\x1b[1;1R"))
		p.inputMu.Unlock()
	}
	return len(b), nil
}
func (p *nativePTY) send(t *testing.T, s string) {
	t.Helper()
	p.inputMu.Lock()
	defer p.inputMu.Unlock()
	if _, err := io.WriteString(p.in, s); err != nil {
		t.Fatal(err)
	}
}
func (p *nativePTY) submit(t *testing.T, ctx context.Context, text string) {
	t.Helper()
	p.send(t, "\x1b[200~"+text+"\x1b[201~")
	waitNative(t, ctx, "native composer contains input", func() bool { return strings.Contains(p.text(), text) })
	p.send(t, "\r")
}
func (p *nativePTY) stop() {
	p.once.Do(func() { _ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL); _ = p.in.Close(); <-p.done })
}
func startNativePTY(t *testing.T, ctx context.Context, bin string, args []string, cwd string, env []string) *nativePTY {
	t.Helper()
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", "'\"'\"'") + "'" }
	line := "stty rows 40 cols 150; exec " + quote(bin)
	for _, a := range args {
		line += " " + quote(a)
	}
	cmd := exec.CommandContext(ctx, "script", "-qefc", line, "/dev/null")
	cmd.Dir, cmd.Env = cwd, env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	in, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	p := &nativePTY{cmd: cmd, in: in, done: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = p, p
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { _ = cmd.Wait(); close(p.done) }()
	t.Cleanup(p.stop)
	t.Cleanup(func() {
		if t.Failed() {
			t.Log("TUI " + cwd + ":\n" + tail(p.text(), 2500))
		}
	})
	return p
}
func openNativeNew(t *testing.T, ctx context.Context, p *nativePTY) {
	t.Helper()
	waitNative(t, ctx, "CXP selector", func() bool { return strings.Contains(p.text(), "Ctrl+N: new") })
	p.send(t, "\x0e") // the actual selector's New chat action
	waitNative(t, ctx, "native Codex input", func() bool { return strings.Contains(p.text(), "›") })
}
