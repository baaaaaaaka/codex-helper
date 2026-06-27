package codexcontract

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestInstalledCodexRemoteTUIHandshake(t *testing.T) {
	if os.Getenv("CODEX_REMOTE_TUI_CONTRACT_TEST") != "1" {
		t.Skip("set CODEX_REMOTE_TUI_CONTRACT_TEST=1 to probe the installed Codex remote TUI")
	}
	command := strings.TrimSpace(os.Getenv("CXP_CONTRACT_CODEX"))
	if command == "" {
		var err error
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
	process, err := startRemoteTUIProcess(ctx, command, remoteURL, t.TempDir())
	if err != nil {
		t.Fatalf("start Codex remote TUI under PTY: %v", err)
	}
	defer process.Stop()
	done := make(chan error, 1)
	go func() { done <- process.Wait() }()

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
		process.Stop()
	case err := <-done:
		t.Fatalf("Codex remote TUI exited before initialize handshake: %v\n%s", err, process.Output())
	case <-ctx.Done():
		process.Stop()
		<-done
		t.Fatalf("timed out waiting for Codex remote TUI initialize handshake\n%s", process.Output())
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		process.Stop()
		<-done
	}
}

type remoteTUIProcess interface {
	Wait() error
	Stop()
	Output() string
}
