package responsespolicy

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestProxyRestoresHTTPRequestAndRewritesSSE(t *testing.T) {
	policy := NewShellEscalationPolicy(16)
	prepared := policy.Prepare("call-1", ShellCommandTool, `{"command":"nvidia-smi"}`)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer upstream-secret" {
			t.Errorf("authorization = %q", got)
		}
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), EscalationPermission) || !strings.Contains(string(body), `nvidia-smi`) {
			t.Errorf("upstream request was not restored: %s", body)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "event: response.output_item.done\n")
		_, _ = io.WriteString(w, `data: {"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-2","name":"shell_command","arguments":"{\"command\":\"nvcc --version\"}"}}`+"\n\n")
	}))
	defer upstream.Close()

	proxy, err := NewProxy(ProxyOptions{Upstream: upstream.URL, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(proxy)
	defer server.Close()

	requestBody := `{"model":"gpt","input":[{"type":"function_call","call_id":"call-1","name":"shell_command","arguments":` + quoteJSON(prepared) + `}]}`
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/responses", strings.NewReader(requestBody))
	request.Header.Set("Authorization", "Bearer upstream-secret")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK || !strings.Contains(string(body), EscalationPermission) || !strings.Contains(string(body), "nvcc --version") {
		t.Fatalf("status=%d body=%s", response.StatusCode, body)
	}
}

func TestProxyRestoresAndRewritesWebSocketFrames(t *testing.T) {
	policy := NewShellEscalationPolicy(16)
	prepared := policy.Prepare("call-in", ShellCommandTool, `{"command":"id"}`)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstreamErr := make(chan error, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			upstreamErr <- err
			return
		}
		defer connection.Close()
		_, request, err := connection.ReadMessage()
		if err != nil {
			upstreamErr <- err
			return
		}
		if strings.Contains(string(request), EscalationPermission) || !strings.Contains(string(request), `\"command\":\"id\"`) {
			upstreamErr <- &testError{"WebSocket request was not restored: " + string(request)}
			return
		}
		event := `{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-out","name":"shell_command","arguments":"{\"command\":\"nvidia-smi -L\"}"}}`
		upstreamErr <- connection.WriteMessage(websocket.TextMessage, []byte(event))
	}))
	defer upstream.Close()

	proxy, err := NewProxy(ProxyOptions{Upstream: upstream.URL, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(proxy)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses"
	connection, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	request := `{"type":"response.create","input":[{"type":"function_call","call_id":"call-in","name":"shell_command","arguments":` + quoteJSON(prepared) + `}]}`
	if err := connection.WriteMessage(websocket.TextMessage, []byte(request)); err != nil {
		t.Fatal(err)
	}
	_ = connection.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, response, err := connection.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(response), EscalationPermission) || !strings.Contains(string(response), "nvidia-smi -L") {
		t.Fatalf("WebSocket response was not rewritten: %s", response)
	}
	select {
	case err := <-upstreamErr:
		if err != nil {
			t.Fatal(err)
		}
	case <-context.Background().Done():
	}
}

func TestProxyRestoresAndRewritesWebSocketBinaryJSONFrames(t *testing.T) {
	policy := NewShellEscalationPolicy(16)
	prepared := policy.Prepare("call-in", ShellCommandTool, `{"command":"id"}`)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstreamResult := make(chan error, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			upstreamResult <- err
			return
		}
		defer connection.Close()
		messageType, request, err := connection.ReadMessage()
		if err != nil {
			upstreamResult <- err
			return
		}
		if messageType != websocket.BinaryMessage || strings.Contains(string(request), EscalationPermission) || !strings.Contains(string(request), `\"command\":\"id\"`) {
			upstreamResult <- &testError{"binary WebSocket request was not restored: " + string(request)}
			return
		}
		event := `{"type":"response.output_item.done","item":{"type":"function_call","call_id":"call-out","name":"shell_command","arguments":"{\"command\":\"nvidia-smi -L\"}"}}`
		upstreamResult <- connection.WriteMessage(websocket.BinaryMessage, []byte(event))
	}))
	defer upstream.Close()
	proxy, err := NewProxy(ProxyOptions{Upstream: upstream.URL, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(proxy)
	defer server.Close()
	connection, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	request := `{"type":"response.create","input":[{"type":"function_call","call_id":"call-in","name":"shell_command","arguments":` + quoteJSON(prepared) + `}]}`
	if err := connection.WriteMessage(websocket.BinaryMessage, []byte(request)); err != nil {
		t.Fatal(err)
	}
	messageType, response, err := connection.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if messageType != websocket.BinaryMessage || !strings.Contains(string(response), EscalationPermission) || !strings.Contains(string(response), "nvidia-smi -L") {
		t.Fatalf("binary WebSocket response was not rewritten: type=%d body=%s", messageType, response)
	}
	if err := <-upstreamResult; err != nil {
		t.Fatal(err)
	}
}

func TestProxyPreservesWebSocketBinaryAndCloseFrames(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer connection.Close()
		if _, _, err := connection.ReadMessage(); err != nil {
			return
		}
		if err := connection.WriteMessage(websocket.BinaryMessage, []byte{0, 1, 2, 3}); err != nil {
			return
		}
		_ = connection.WriteControl(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "contract close"),
			time.Now().Add(time.Second),
		)
	}))
	defer upstream.Close()

	proxy, err := NewProxy(ProxyOptions{Upstream: upstream.URL})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(proxy)
	defer server.Close()
	connection, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := connection.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create"}`)); err != nil {
		t.Fatal(err)
	}
	messageType, payload, err := connection.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if messageType != websocket.BinaryMessage || !bytes.Equal(payload, []byte{0, 1, 2, 3}) {
		t.Fatalf("binary frame type=%d payload=%v", messageType, payload)
	}
	_, _, err = connection.ReadMessage()
	closeErr, ok := err.(*websocket.CloseError)
	if !ok || closeErr.Code != websocket.ClosePolicyViolation || closeErr.Text != "contract close" {
		t.Fatalf("close error = %#v, want policy violation contract close", err)
	}
}

func TestTransformSSEPreservesUnknownData(t *testing.T) {
	input := "event: future\r\ndata: {\"type\":\"future.event\",\"x\":1}\r\n\r\n"
	var output strings.Builder
	if err := transformSSE(strings.NewReader(input), &output, NewShellEscalationPolicy(4)); err != nil {
		t.Fatal(err)
	}
	if output.String() != input {
		t.Fatalf("output=%q want=%q", output.String(), input)
	}
}

func TestProxyLeavesTelemetryRequestAndResponseUnchanged(t *testing.T) {
	type capture struct {
		method string
		path   string
		query  string
		auth   string
		body   []byte
	}
	captured := make(chan capture, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		captured <- capture{method: r.Method, path: r.URL.Path, query: r.URL.RawQuery, auth: r.Header.Get("Authorization"), body: body}
		w.Header().Set("X-Telemetry-Contract", "unchanged")
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte(`{"stored":true}`))
	}))
	defer upstream.Close()
	proxy, err := NewProxy(ProxyOptions{Upstream: upstream.URL})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(proxy)
	defer server.Close()
	body := []byte(`{"events":[{"name":"turn","properties":{"approval_policy":"on-request","approvals_reviewer":"user","sandbox_mode":"read-only"}}]}`)
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/codex/analytics-events/events?source=app-server", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer telemetry-secret")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	responseBody, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	got := <-captured
	if got.method != http.MethodPost || got.path != "/codex/analytics-events/events" || got.query != "source=app-server" || got.auth != "Bearer telemetry-secret" || !bytes.Equal(got.body, body) {
		t.Fatalf("telemetry request changed: %#v body=%s", got, got.body)
	}
	if response.StatusCode != http.StatusAccepted || response.Header.Get("X-Telemetry-Contract") != "unchanged" || string(responseBody) != `{"stored":true}` {
		t.Fatalf("telemetry response changed: status=%d headers=%v body=%s", response.StatusCode, response.Header, responseBody)
	}
}

func TestProxyLeavesTelemetryWebSocketFramesUnchanged(t *testing.T) {
	policy := NewShellEscalationPolicy(16)
	prepared := policy.Prepare("telemetry-call", ShellCommandTool, `{"command":"nvidia-smi"}`)
	requestPayload := []byte(`{"events":[{"type":"function_call","call_id":"telemetry-call","name":"shell_command","arguments":` + quoteJSON(prepared) + `}]}`)
	responsePayload := []byte(`{"type":"response.output_item.done","item":{"type":"function_call","call_id":"telemetry-response","name":"shell_command","arguments":"{\"command\":\"uname -a\"}"}}`)

	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstreamResult := make(chan error, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connection, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			upstreamResult <- err
			return
		}
		defer connection.Close()
		messageType, got, err := connection.ReadMessage()
		if err != nil {
			upstreamResult <- err
			return
		}
		if messageType != websocket.TextMessage || !bytes.Equal(got, requestPayload) {
			upstreamResult <- &testError{"telemetry WebSocket request changed: " + string(got)}
			return
		}
		upstreamResult <- connection.WriteMessage(websocket.TextMessage, responsePayload)
	}))
	defer upstream.Close()

	proxy, err := NewProxy(ProxyOptions{Upstream: upstream.URL, Policy: policy})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(proxy)
	defer server.Close()
	connection, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/codex/analytics-events/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := connection.WriteMessage(websocket.TextMessage, requestPayload); err != nil {
		t.Fatal(err)
	}
	messageType, got, err := connection.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if messageType != websocket.TextMessage || !bytes.Equal(got, responsePayload) {
		t.Fatalf("telemetry WebSocket response changed: %s", got)
	}
	if err := <-upstreamResult; err != nil {
		t.Fatal(err)
	}
}

func TestProxyRedactsCredentialBearingUpstreamErrors(t *testing.T) {
	proxy, err := NewProxy(ProxyOptions{Upstream: "http://user:super-secret@127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(proxy)
	defer server.Close()
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/responses", strings.NewReader(`{"model":"gpt"}`))
	request.Header.Set("Authorization", "Bearer request-secret")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	text := string(body)
	if response.StatusCode != http.StatusBadGateway || strings.Contains(text, "super-secret") || strings.Contains(text, "request-secret") {
		t.Fatalf("credential-bearing error leaked: status=%d body=%q", response.StatusCode, text)
	}
}

func quoteJSON(value string) string {
	quoted := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(value)
	return `"` + quoted + `"`
}

type testError struct{ message string }

func (e *testError) Error() string { return e.message }
