package codexrunner

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/responsespolicy"
)

// PolicyAppServerStarter starts the local Responses policy server before the
// original Codex app-server and ties both lifetimes to one transport.
type PolicyAppServerStarter struct {
	Base          AppServerTransportStarter
	ServerOptions responsespolicy.ServerOptions
	// ReadyHook runs once after the original app-server successfully answers
	// initialize. It is used for post-activation housekeeping and never changes
	// protocol bytes or telemetry.
	ReadyHook func() error
}

func (s PolicyAppServerStarter) StartAppServer(ctx context.Context, request AppServerStartRequest) (AppServerLineTransport, error) {
	serverOptions := s.ServerOptions
	openAIUpstream := appServerOpenAIBaseURLOverride(request.Args)
	if serverOptions.OpenAIUpstream == "" && openAIUpstream != "" {
		serverOptions.OpenAIUpstream = openAIUpstream
		serverOptions.ChatGPTModelUpstream = openAIUpstream
	}
	policyServer, err := responsespolicy.StartServer(serverOptions)
	if err != nil {
		return nil, err
	}
	request.Args = append(request.Args, policyServer.CodexConfigArgs()...)
	base := s.Base
	if base == nil {
		base = AppServerProcessStarter{}
	}
	transport, err := base.StartAppServer(ctx, request)
	if err != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = policyServer.Close(shutdownCtx)
		return nil, err
	}
	return &policyAppServerTransport{
		AppServerLineTransport: transport,
		policyServer:           policyServer,
		readyHook:              s.ReadyHook,
		initializeIDs:          make(map[string]struct{}),
	}, nil
}

func appServerOpenAIBaseURLOverride(args []string) string {
	var openAI string
	for index := 0; index < len(args); index++ {
		arg := strings.TrimSpace(args[index])
		if arg != "-c" && arg != "--config" {
			continue
		}
		if index+1 >= len(args) {
			break
		}
		index++
		key, value, ok := strings.Cut(strings.TrimSpace(args[index]), "=")
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		if len(value) >= 2 && ((value[0] == '"' && value[len(value)-1] == '"') || (value[0] == '\'' && value[len(value)-1] == '\'')) {
			value = value[1 : len(value)-1]
			value = strings.ReplaceAll(value, `\"`, `"`)
			value = strings.ReplaceAll(value, `\\`, `\`)
		}
		switch strings.TrimSpace(key) {
		case "openai_base_url":
			openAI = value
		}
	}
	return openAI
}

type policyAppServerTransport struct {
	AppServerLineTransport
	policyServer  *responsespolicy.Server
	closeOnce     sync.Once
	closeErr      error
	readyHook     func() error
	readyOnce     sync.Once
	readyErr      error
	protocolMu    sync.Mutex
	initializeIDs map[string]struct{}
}

func (t *policyAppServerTransport) WriteLine(ctx context.Context, line []byte) error {
	var message appServerMessage
	if json.Unmarshal(bytes.TrimSpace(line), &message) == nil &&
		strings.TrimSpace(message.Method) == appServerMethodInitialize && len(bytes.TrimSpace(message.ID)) > 0 {
		t.protocolMu.Lock()
		t.initializeIDs[string(bytes.TrimSpace(message.ID))] = struct{}{}
		t.protocolMu.Unlock()
	}
	return t.AppServerLineTransport.WriteLine(ctx, line)
}

func (t *policyAppServerTransport) ReadLine(ctx context.Context) ([]byte, error) {
	line, err := t.AppServerLineTransport.ReadLine(ctx)
	if err != nil {
		return line, err
	}
	var message appServerMessage
	if json.Unmarshal(bytes.TrimSpace(line), &message) == nil && message.Error == nil && len(bytes.TrimSpace(message.ID)) > 0 {
		key := string(bytes.TrimSpace(message.ID))
		t.protocolMu.Lock()
		_, ready := t.initializeIDs[key]
		if ready {
			delete(t.initializeIDs, key)
		}
		t.protocolMu.Unlock()
		if ready && t.readyHook != nil {
			t.readyOnce.Do(func() { t.readyErr = t.readyHook() })
			if t.readyErr != nil {
				return nil, t.readyErr
			}
		}
	}
	return line, nil
}

func (t *policyAppServerTransport) Close() error {
	t.closeOnce.Do(func() {
		if t.AppServerLineTransport != nil {
			t.closeErr = t.AppServerLineTransport.Close()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := t.policyServer.Close(ctx); t.closeErr == nil {
			t.closeErr = err
		}
	})
	return t.closeErr
}
