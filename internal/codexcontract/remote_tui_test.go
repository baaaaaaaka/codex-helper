package codexcontract

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
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
	process, err := startRemoteTUIProcess(ctx, command, remoteURL, "", "", t.TempDir())
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

func TestInstalledCodexRemoteTUIProductionBrokerHandshake(t *testing.T) {
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
		t.Fatalf("Codex production broker preflight: %v", err)
	}

	codexHome := t.TempDir()
	initialize := make(chan []byte, 1)
	base := &capturingAppServerStarter{initialize: initialize}
	var brokerLog bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	broker, err := codexrunner.StartRemoteBroker(ctx, codexrunner.RemoteBrokerOptions{
		Starter: codexrunner.PolicyAppServerStarter{Base: base},
		StartRequest: codexrunner.AppServerStartRequest{
			Command: command,
			Args:    []string{"app-server", "--analytics-default-enabled"},
			ExtraEnv: []string{
				"CODEX_HOME=" + codexHome,
				"OPENAI_API_KEY=cxp-contract-key",
			},
			Timeout: 20 * time.Second,
		},
		Log: &brokerLog,
	})
	if err != nil {
		t.Fatalf("start production remote broker: %v", err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		_ = broker.Close(closeCtx)
	}()
	if strings.Contains(strings.TrimPrefix(broker.URL(), "ws://"), "/") {
		t.Fatalf("production broker URL contains a path rejected by Codex: %q", broker.URL())
	}

	process, err := startRemoteTUIProcess(ctx, command, broker.URL(), codexrunner.RemoteBrokerAuthTokenEnv, broker.AuthToken(), codexHome)
	if err != nil {
		t.Fatalf("start Codex remote TUI against production broker: %v", err)
	}
	defer process.Stop()
	done := make(chan error, 1)
	go func() { done <- process.Wait() }()

	select {
	case payload := <-initialize:
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		if err := json.Unmarshal(payload, &request); err != nil {
			t.Fatalf("decode production broker initialize %q: %v", payload, err)
		}
		if request.Method != "initialize" || len(request.ID) == 0 || len(request.Params) == 0 {
			t.Fatalf("production broker handshake = %s", payload)
		}
		process.Stop()
	case err := <-done:
		t.Fatalf("Codex TUI exited before reaching production broker: %v\n%s\nbroker log:\n%s", err, process.Output(), brokerLog.String())
	case <-ctx.Done():
		process.Stop()
		<-done
		t.Fatalf("timed out waiting for production broker handshake\n%s\nbroker log:\n%s", process.Output(), brokerLog.String())
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		process.Stop()
		<-done
	}
}

type capturingAppServerStarter struct {
	initialize chan<- []byte
}

func (s *capturingAppServerStarter) StartAppServer(ctx context.Context, request codexrunner.AppServerStartRequest) (codexrunner.AppServerLineTransport, error) {
	transport, err := (codexrunner.AppServerProcessStarter{}).StartAppServer(ctx, request)
	if err != nil {
		return nil, err
	}
	return &capturingAppServerTransport{AppServerLineTransport: transport, initialize: s.initialize}, nil
}

type capturingAppServerTransport struct {
	codexrunner.AppServerLineTransport
	initialize chan<- []byte
	once       sync.Once
}

func (t *capturingAppServerTransport) WriteLine(ctx context.Context, line []byte) error {
	var request struct {
		Method string `json:"method"`
	}
	if json.Unmarshal(line, &request) == nil && request.Method == "initialize" {
		t.once.Do(func() {
			select {
			case t.initialize <- append([]byte(nil), line...):
			default:
			}
		})
	}
	return t.AppServerLineTransport.WriteLine(ctx, line)
}

type remoteTUIProcess interface {
	Wait() error
	Stop()
	Output() string
}
