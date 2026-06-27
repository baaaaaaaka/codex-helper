package responsespolicy

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

const (
	DefaultOpenAIUpstream       = "https://api.openai.com/v1"
	DefaultChatGPTModelUpstream = "https://chatgpt.com/backend-api/codex"
	DefaultChatGPTUpstream      = "https://chatgpt.com/backend-api"
)

type ServerOptions struct {
	ListenAddress  string
	OpenAIUpstream string
	// ChatGPTModelUpstream is the first-party Responses endpoint selected for
	// requests that carry ChatGPT-Account-ID.
	ChatGPTModelUpstream string
	ChatGPTUpstream      string
	Transport            http.RoundTripper
	ProxyURL             string
	Policy               *ShellEscalationPolicy
}

// Server hosts separate OpenAI and ChatGPT reverse-proxy routes on one
// loopback listener. Non-Responses paths, including analytics, are forwarded
// without payload mutation by Proxy.
type Server struct {
	listener net.Listener
	server   *http.Server
	baseURL  string
	token    string
	done     chan error
	close    sync.Once
	closeErr error
}

func StartServer(options ServerOptions) (*Server, error) {
	listen := strings.TrimSpace(options.ListenAddress)
	if listen == "" {
		listen = "127.0.0.1:0"
	}
	policy := options.Policy
	if policy == nil {
		policy = NewShellEscalationPolicy(0)
	}
	openAIUpstream := firstNonEmpty(options.OpenAIUpstream, DefaultOpenAIUpstream)
	chatGPTModelUpstream := firstNonEmpty(options.ChatGPTModelUpstream, DefaultChatGPTModelUpstream)
	chatGPTUpstream := firstNonEmpty(options.ChatGPTUpstream, DefaultChatGPTUpstream)
	transport, websocketProxy, err := policyProxyTransport(options.Transport, options.ProxyURL)
	if err != nil {
		return nil, err
	}
	openAIProxy, err := NewProxy(ProxyOptions{Upstream: openAIUpstream, Policy: policy, Transport: transport, WebSocketProxy: websocketProxy})
	if err != nil {
		return nil, err
	}
	chatGPTModelProxy, err := NewProxy(ProxyOptions{Upstream: chatGPTModelUpstream, Policy: policy, Transport: transport, WebSocketProxy: websocketProxy})
	if err != nil {
		return nil, err
	}
	chatGPTProxy, err := NewProxy(ProxyOptions{Upstream: chatGPTUpstream, Policy: policy, Transport: transport, WebSocketProxy: websocketProxy})
	if err != nil {
		return nil, err
	}
	token, err := newCapabilityToken()
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, fmt.Errorf("listen for Responses policy server: %w", err)
	}
	prefix := "/_cxp/" + token
	mux := http.NewServeMux()
	mux.Handle(prefix+"/gateway/", http.StripPrefix(prefix+"/gateway", http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if strings.TrimSpace(request.Header.Get("ChatGPT-Account-ID")) != "" {
			chatGPTModelProxy.ServeHTTP(w, request)
			return
		}
		openAIProxy.ServeHTTP(w, request)
	})))
	mux.Handle(prefix+"/chatgpt/", http.StripPrefix(prefix+"/chatgpt", chatGPTProxy))
	mux.HandleFunc(prefix+"/health", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	runtime := &Server{
		listener: listener,
		server: &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
			IdleTimeout:       2 * time.Minute,
		},
		baseURL: "http://" + listener.Addr().String(),
		token:   token,
		done:    make(chan error, 1),
	}
	go func() {
		err := runtime.server.Serve(listener)
		if err == http.ErrServerClosed {
			err = nil
		}
		runtime.done <- err
		close(runtime.done)
	}()
	return runtime, nil
}

func policyProxyTransport(configured http.RoundTripper, rawProxyURL string) (http.RoundTripper, func(*http.Request) (*url.URL, error), error) {
	rawProxyURL = strings.TrimSpace(rawProxyURL)
	if rawProxyURL == "" {
		return configured, nil, nil
	}
	proxyURL, err := url.Parse(rawProxyURL)
	if err != nil || proxyURL.Scheme == "" || proxyURL.Host == "" {
		return nil, nil, fmt.Errorf("invalid policy proxy URL %q", rawProxyURL)
	}
	if configured != nil {
		return nil, nil, fmt.Errorf("policy server cannot combine a custom transport with ProxyURL")
	}
	base, _ := http.DefaultTransport.(*http.Transport)
	transport := base.Clone()
	proxy := http.ProxyURL(proxyURL)
	transport.Proxy = proxy
	return transport, proxy, nil
}

func (s *Server) BaseURL() string {
	if s == nil {
		return ""
	}
	return s.baseURL
}

func (s *Server) capabilityBaseURL() string {
	if s == nil {
		return ""
	}
	return strings.TrimSuffix(s.BaseURL(), "/") + "/_cxp/" + s.token
}

func (s *Server) OpenAIBaseURL() string { return s.capabilityBaseURL() + "/gateway" }

func (s *Server) ChatGPTBaseURL() string { return s.capabilityBaseURL() + "/chatgpt" }

// CodexConfigArgs returns ordinary config overrides. It intentionally does not
// use full-access, bypass, or noninteractive approval flags.
func (s *Server) CodexConfigArgs() []string {
	return []string{
		"-c", `openai_base_url="` + tomlEscape(s.OpenAIBaseURL()) + `"`,
		"-c", `chatgpt_base_url="` + tomlEscape(s.ChatGPTBaseURL()) + `"`,
		"-c", `approval_policy="on-request"`,
		"-c", `approvals_reviewer="user"`,
		"-c", `sandbox_mode="read-only"`,
	}
}

func (s *Server) Close(ctx context.Context) error {
	if s == nil || s.server == nil {
		return nil
	}
	s.close.Do(func() { s.closeErr = s.server.Shutdown(ctx) })
	return s.closeErr
}

func (s *Server) Done() <-chan error {
	if s == nil {
		closed := make(chan error)
		close(closed)
		return closed
	}
	return s.done
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			return value
		}
	}
	return ""
}

func tomlEscape(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	return strings.ReplaceAll(value, `"`, `\"`)
}
