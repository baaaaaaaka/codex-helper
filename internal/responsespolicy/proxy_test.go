package responsespolicy

import (
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

func quoteJSON(value string) string {
	quoted := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(value)
	return `"` + quoted + `"`
}

type testError struct{ message string }

func (e *testError) Error() string { return e.message }
