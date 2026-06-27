package responsespolicy

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	maxBufferedJSONResponse = 64 << 20
	maxWebSocketMessage     = 64 << 20
)

// Proxy transparently relays a Responses API endpoint and applies a
// ShellEscalationPolicy only on Responses request/response payloads. Other
// paths, including analytics endpoints, are relayed without content mutation.
type Proxy struct {
	upstream *url.URL
	policy   *ShellEscalationPolicy
	reverse  *httputil.ReverseProxy
	dialer   *websocket.Dialer
	upgrader websocket.Upgrader
}

type ProxyOptions struct {
	Upstream       string
	Policy         *ShellEscalationPolicy
	Transport      http.RoundTripper
	WebSocketProxy func(*http.Request) (*url.URL, error)
}

func NewProxy(options ProxyOptions) (*Proxy, error) {
	upstream, err := url.Parse(strings.TrimSpace(options.Upstream))
	if err != nil || upstream.Scheme == "" || upstream.Host == "" {
		return nil, fmt.Errorf("invalid Responses upstream %q", options.Upstream)
	}
	policy := options.Policy
	if policy == nil {
		policy = NewShellEscalationPolicy(0)
	}
	proxy := &Proxy{
		upstream: upstream,
		policy:   policy,
		dialer: &websocket.Dialer{
			Proxy:             firstProxy(options.WebSocketProxy, http.ProxyFromEnvironment),
			EnableCompression: true,
		},
		upgrader: websocket.Upgrader{
			CheckOrigin:       func(*http.Request) bool { return true },
			EnableCompression: true,
		},
	}
	proxy.reverse = &httputil.ReverseProxy{
		Rewrite:        proxy.rewriteRequest,
		ModifyResponse: proxy.modifyResponse,
		Transport:      options.Transport,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			// Upstream errors can contain proxy URLs or credential-bearing request
			// details. Keep the public loopback response deliberately generic.
			http.Error(w, "Responses upstream is unavailable", http.StatusBadGateway)
		},
	}
	return proxy, nil
}

func firstProxy(proxies ...func(*http.Request) (*url.URL, error)) func(*http.Request) (*url.URL, error) {
	for _, proxy := range proxies {
		if proxy != nil {
			return proxy
		}
	}
	return nil
}

func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if websocket.IsWebSocketUpgrade(r) {
		p.serveWebSocket(w, r, isResponsesPath(r.URL.Path))
		return
	}
	if isResponsesPath(r.URL.Path) && r.Body != nil && r.Method == http.MethodPost {
		raw, err := io.ReadAll(r.Body)
		_ = r.Body.Close()
		if err != nil {
			http.Error(w, "read Responses request", http.StatusBadRequest)
			return
		}
		if restored, changed := p.policy.RestoreRequest(raw); changed {
			raw = restored
		}
		r.Body = io.NopCloser(bytes.NewReader(raw))
		r.ContentLength = int64(len(raw))
		r.Header.Set("Content-Length", strconv.Itoa(len(raw)))
	}
	p.reverse.ServeHTTP(w, r)
}

func (p *Proxy) rewriteRequest(request *httputil.ProxyRequest) {
	request.SetURL(p.upstream)
	request.Out.Host = p.upstream.Host
	request.Out.Header.Set("Accept-Encoding", "identity")
}

func (p *Proxy) modifyResponse(response *http.Response) error {
	if response.Request == nil || !isResponsesPath(response.Request.URL.Path) || response.Body == nil {
		return nil
	}
	contentType := strings.ToLower(response.Header.Get("Content-Type"))
	response.Header.Del("Content-Length")
	response.ContentLength = -1
	if strings.Contains(contentType, "text/event-stream") {
		reader, writer := io.Pipe()
		upstreamBody := response.Body
		response.Body = reader
		go func() {
			err := transformSSE(upstreamBody, writer, p.policy)
			_ = upstreamBody.Close()
			_ = writer.CloseWithError(err)
		}()
		return nil
	}
	if !strings.Contains(contentType, "json") {
		return nil
	}
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxBufferedJSONResponse+1))
	_ = response.Body.Close()
	if err != nil {
		return err
	}
	if len(raw) > maxBufferedJSONResponse {
		return fmt.Errorf("Responses JSON body exceeds %d bytes", maxBufferedJSONResponse)
	}
	if rewritten, changed := p.policy.RewriteResponseEvent(raw); changed {
		raw = rewritten
	}
	response.Body = io.NopCloser(bytes.NewReader(raw))
	response.ContentLength = int64(len(raw))
	response.Header.Set("Content-Length", strconv.Itoa(len(raw)))
	return nil
}

func transformSSE(source io.Reader, destination io.Writer, policy *ShellEscalationPolicy) error {
	reader := bufio.NewReader(source)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			body, ending := splitLineEnding(line)
			trimmed := bytes.TrimSpace(body)
			if bytes.HasPrefix(trimmed, []byte("data:")) {
				payload := bytes.TrimSpace(bytes.TrimPrefix(trimmed, []byte("data:")))
				if len(payload) > 0 && !bytes.Equal(payload, []byte("[DONE]")) {
					if rewritten, changed := policy.RewriteResponseEvent(payload); changed {
						prefix := body[:bytes.Index(body, []byte("data:"))]
						line = append(append(append([]byte{}, prefix...), []byte("data: ")...), rewritten...)
						line = append(line, ending...)
					}
				}
			}
			if _, writeErr := destination.Write(line); writeErr != nil {
				return writeErr
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

func splitLineEnding(line []byte) ([]byte, []byte) {
	switch {
	case bytes.HasSuffix(line, []byte("\r\n")):
		return line[:len(line)-2], line[len(line)-2:]
	case bytes.HasSuffix(line, []byte("\n")):
		return line[:len(line)-1], line[len(line)-1:]
	default:
		return line, nil
	}
}

func (p *Proxy) serveWebSocket(w http.ResponseWriter, request *http.Request, applyResponsesPolicy bool) {
	upstreamURL := *p.upstream
	switch upstreamURL.Scheme {
	case "https":
		upstreamURL.Scheme = "wss"
	case "http":
		upstreamURL.Scheme = "ws"
	}
	upstreamURL.Path = joinURLPath(p.upstream.Path, request.URL.Path)
	upstreamURL.RawQuery = request.URL.RawQuery
	headers := cloneWebSocketHeaders(request.Header)
	subprotocols := websocket.Subprotocols(request)
	dialer := *p.dialer
	dialer.Subprotocols = subprotocols
	upstream, response, err := dialer.DialContext(request.Context(), upstreamURL.String(), headers)
	if err != nil {
		status := http.StatusBadGateway
		if response != nil && response.StatusCode >= 400 {
			status = response.StatusCode
		}
		http.Error(w, "Responses WebSocket upstream is unavailable", status)
		return
	}
	defer upstream.Close()
	upgrader := p.upgrader
	if selected := upstream.Subprotocol(); selected != "" {
		upgrader.Subprotocols = []string{selected}
	}
	downstream, err := upgrader.Upgrade(w, request, nil)
	if err != nil {
		return
	}
	defer downstream.Close()
	upstream.SetReadLimit(maxWebSocketMessage)
	downstream.SetReadLimit(maxWebSocketMessage)
	installWebSocketControlRelay(upstream, downstream)
	installWebSocketControlRelay(downstream, upstream)

	ctx, cancel := context.WithCancel(request.Context())
	defer cancel()
	errors := make(chan error, 2)
	var restoreRequest, rewriteResponse func([]byte) ([]byte, bool)
	if applyResponsesPolicy {
		restoreRequest = p.policy.RestoreRequest
		rewriteResponse = p.policy.RewriteResponseEvent
	}
	go relayWebSocket(ctx, upstream, downstream, restoreRequest, errors)
	go relayWebSocket(ctx, downstream, upstream, rewriteResponse, errors)
	<-errors
	cancel()
	_ = upstream.Close()
	_ = downstream.Close()
	select {
	case <-errors:
	case <-time.After(time.Second):
	}
}

func installWebSocketControlRelay(source, destination *websocket.Conn) {
	source.SetPingHandler(func(data string) error {
		return destination.WriteControl(websocket.PingMessage, []byte(data), time.Now().Add(time.Second))
	})
	source.SetPongHandler(func(data string) error {
		return destination.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(time.Second))
	})
	source.SetCloseHandler(func(code int, text string) error {
		return destination.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, text), time.Now().Add(time.Second))
	})
}

func relayWebSocket(ctx context.Context, destination, source *websocket.Conn, mutate func([]byte) ([]byte, bool), done chan<- error) {
	for {
		messageType, payload, err := source.ReadMessage()
		if err != nil {
			select {
			case done <- err:
			case <-ctx.Done():
			}
			return
		}
		if (messageType == websocket.TextMessage || messageType == websocket.BinaryMessage) && mutate != nil {
			if rewritten, changed := mutate(payload); changed {
				payload = rewritten
			}
		}
		if err := destination.WriteMessage(messageType, payload); err != nil {
			select {
			case done <- err:
			case <-ctx.Done():
			}
			return
		}
	}
}

func cloneWebSocketHeaders(source http.Header) http.Header {
	destination := make(http.Header)
	for key, values := range source {
		switch strings.ToLower(key) {
		case "connection", "upgrade", "sec-websocket-key", "sec-websocket-version", "sec-websocket-extensions", "sec-websocket-protocol", "host":
			continue
		}
		for _, value := range values {
			destination.Add(key, value)
		}
	}
	return destination
}

func isResponsesPath(path string) bool {
	path = strings.TrimSuffix(path, "/")
	return strings.HasSuffix(path, "/responses") || strings.HasSuffix(path, "/responses/compact")
}

func joinURLPath(basePath, requestPath string) string {
	return strings.TrimSuffix(basePath, "/") + "/" + strings.TrimPrefix(requestPath, "/")
}
