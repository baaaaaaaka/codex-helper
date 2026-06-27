package codexcontract

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestInstalledCodexRemoteTUIHandshake(t *testing.T) {
	if os.Getenv("CODEX_REMOTE_TUI_CONTRACT_TEST") != "1" {
		t.Skip("set CODEX_REMOTE_TUI_CONTRACT_TEST=1 to probe the installed Codex remote TUI")
	}
	var ptyCommand string
	var err error
	if runtime.GOOS == "windows" {
		ptyCommand, err = exec.LookPath("winpty")
		if err != nil {
			t.Fatal("winpty is required for the Windows remote TUI contract probe")
		}
	} else {
		ptyCommand, err = exec.LookPath("script")
		if err != nil {
			t.Fatal("script is required for the remote TUI contract probe")
		}
	}
	command := strings.TrimSpace(os.Getenv("CXP_CONTRACT_CODEX"))
	if command == "" {
		command, err = exec.LookPath("codex")
		if err != nil {
			t.Fatalf("codex not found in PATH: %v", err)
		}
	}
	preflightCtx, preflightCancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer preflightCancel()
	if _, err := Probe(preflightCtx, command); err != nil {
		t.Fatalf("Codex remote TUI contract preflight: %v", err)
	}

	handshake := make(chan []byte, 1)
	upgrade := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		connection, err := upgrade.Upgrade(writer, request, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		messageType, payload, err := connection.ReadMessage()
		if err == nil && messageType == websocket.TextMessage {
			select {
			case handshake <- append([]byte(nil), payload...):
			default:
			}
		}
	}))
	defer server.Close()
	remoteURL := "ws" + strings.TrimPrefix(server.URL, "http")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	commandLine := shellQuote(command) + " -c features.tui_app_server=true --remote " + shellQuote(remoteURL)
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		windowsCommandLine := windowsCmdQuote(command) + ` -c "features.tui_app_server=true" --remote ` + windowsCmdQuote(remoteURL)
		cmd = exec.CommandContext(ctx, ptyCommand, "cmd.exe", "/d", "/s", "/c", windowsCommandLine)
	case "darwin":
		cmd = exec.CommandContext(ctx, ptyCommand, "-q", "/dev/null", "/bin/sh", "-lc", commandLine)
	default:
		cmd = exec.CommandContext(ctx, ptyCommand, "-qefc", commandLine, "/dev/null")
	}
	cmd.Env = append(os.Environ(),
		"TERM=xterm-256color",
		"CODEX_HOME="+t.TempDir(),
		"OPENAI_API_KEY=cxp-contract-key",
	)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	if err := cmd.Start(); err != nil {
		t.Fatalf("start Codex remote TUI under PTY: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	select {
	case payload := <-handshake:
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal(payload, &request); err != nil {
			t.Fatalf("decode remote TUI handshake %q: %v", payload, err)
		}
		if request.Method != "initialize" || len(request.ID) == 0 || len(request.Params) == 0 {
			t.Fatalf("remote TUI handshake = %s, want initialize request with id and params", payload)
		}
		cancel()
	case err := <-done:
		t.Fatalf("Codex remote TUI exited before initialize handshake: %v\n%s", err, output.String())
	case <-ctx.Done():
		t.Fatalf("timed out waiting for Codex remote TUI initialize handshake\n%s", output.String())
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		if cmd.Process != nil {
			_ = cmd.Process.Kill()
		}
		<-done
	}
}

func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "'\"'\"'") + "'"
}

func windowsCmdQuote(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `\"`) + `"`
}
