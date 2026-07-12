package cli

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNativeResponsesCompatibilityProxyFlattensNamespacesAndRestoresCalls(t *testing.T) {
	var upstreamRequest map[string]any
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/responses" || r.Header.Get("Authorization") != "Bearer upstream-key" {
			t.Fatalf("unexpected upstream request path=%q auth=%q", r.URL.Path, r.Header.Get("Authorization"))
		}
		raw, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(raw, &upstreamRequest); err != nil {
			t.Fatalf("decode upstream request: %v", err)
		}
		tools, _ := upstreamRequest["tools"].([]any)
		if len(tools) != 2 {
			t.Fatalf("upstream tools = %#v", tools)
		}
		tool, _ := tools[0].(map[string]any)
		wireName, _ := tool["name"].(string)
		if tool["type"] != "function" || !strings.HasPrefix(wireName, "cxpns_") {
			t.Fatalf("flattened tool = %#v", tool)
		}
		if upstreamRequest["prompt_cache_key"] != "stable-thread" {
			t.Fatalf("prompt_cache_key changed: %#v", upstreamRequest)
		}
		custom, _ := tools[1].(map[string]any)
		customWireName, _ := custom["name"].(string)
		if custom["type"] != "function" || !strings.HasPrefix(customWireName, "cxpns_") {
			t.Fatalf("custom tool was not converted: %#v", custom)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"type":"response.output_item.added","item":{"type":"function_call","name":"`+wireName+`","call_id":"call_1","arguments":"{}"}}`+"\n\n")
		_, _ = io.WriteString(w, `data: {"type":"response.completed","response":{"usage":{"input_tokens":100,"input_tokens_details":{"cached_tokens":80}}}}`+"\n\n")
		_, _ = io.WriteString(w, `data: {"type":"response.output_item.added","item":{"type":"function_call","name":"`+customWireName+`","call_id":"call_2","arguments":"{\"input\":\"*** Begin Patch\"}"}}`+"\n\n")
		_, _ = io.WriteString(w, "data: [DONE]\n\n")
	}))
	defer upstream.Close()

	var usageLog bytes.Buffer
	baseURL, cleanup, err := startNativeResponsesCompatibilityProxy(upstream.URL, "upstream-key", "proxy-key", "", &usageLog)
	if err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	defer cleanup()
	body := `{"model":"m","prompt_cache_key":"stable-thread","tools":[{"type":"namespace","name":"mcp__docs","tools":[{"type":"function","name":"search","description":"search docs","parameters":{"type":"object"}}]},{"type":"custom","name":"apply_patch","description":"apply a patch"}],"stream":true}`
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/responses", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer proxy-key")
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("proxy status = %d", resp.StatusCode)
	}
	var output bytes.Buffer
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		output.Write(scanner.Bytes())
		output.WriteByte('\n')
	}
	got := output.String()
	if !strings.Contains(got, `"name":"search"`) || !strings.Contains(got, `"namespace":"mcp__docs"`) {
		t.Fatalf("namespace call was not restored:\n%s", got)
	}
	if !strings.Contains(got, `"cached_tokens":80`) {
		t.Fatalf("usage was not preserved:\n%s", got)
	}
	if !strings.Contains(usageLog.String(), "responses upstream cache usage: model=") || !strings.Contains(usageLog.String(), "input=100 cached=80 hit_rate=80.0%") {
		t.Fatalf("cache usage was not logged: %s", usageLog.String())
	}
	if !strings.Contains(got, `"type":"custom_tool_call"`) || !strings.Contains(got, `"name":"apply_patch"`) || !strings.Contains(got, `"input":"*** Begin Patch"`) {
		t.Fatalf("custom tool call was not restored:\n%s", got)
	}
}

func TestNativeResponsesCompatibilityProxyRejectsWrongProxyKey(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("upstream should not be reached")
	}))
	defer upstream.Close()
	baseURL, cleanup, err := startNativeResponsesCompatibilityProxy(upstream.URL, "upstream-key", "proxy-key", "", nil)
	if err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	defer cleanup()
	req, _ := http.NewRequest(http.MethodPost, baseURL+"/responses", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer wrong")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("proxy request: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", resp.StatusCode)
	}
}

func TestNativeResponsesCompatibilityProxyAvoidsDuplicateVersionPath(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Fatalf("upstream path=%q, want /v1/models", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"data":[]}`)
	}))
	defer upstream.Close()

	baseURL, cleanup, err := startNativeResponsesCompatibilityProxy(upstream.URL+"/v1", "upstream-key", "proxy-key", "", nil)
	if err != nil {
		t.Fatalf("start proxy: %v", err)
	}
	defer cleanup()
	req, _ := http.NewRequest(http.MethodGet, baseURL+"/models", nil)
	req.Header.Set("Authorization", "Bearer proxy-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET models: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
}

func TestNativeResponsesCompatibilityProxyNormalizesToolOutputContent(t *testing.T) {
	p := &nativeResponsesCompatibilityProxy{
		toWire:   map[nativeResponsesToolName]string{},
		fromWire: map[string]nativeResponsesToolName{},
	}
	raw, err := p.rewriteRequest([]byte(`{"input":[{"type":"reasoning","content":[{"type":"reasoning_text","text":"private chain"}]},{"type":"function_call_output","call_id":"call_1","output":[{"type":"input_text","text":"line one"},{"type":"input_text","text":"line two"}]},{"type":"mcp_tool_call_output","call_id":"call_2","output":{"content":[{"text":"mcp result"}]}},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"prior answer"}]}]}`))
	if err != nil {
		t.Fatalf("rewrite request: %v", err)
	}
	got := string(raw)
	if !strings.Contains(got, `"output":"line one\nline two"`) || !strings.Contains(got, `"type":"function_call_output"`) || !strings.Contains(got, `"output":"mcp result"`) {
		t.Fatalf("tool outputs were not normalized: %s", got)
	}
	if strings.Contains(got, `"type":"reasoning"`) || strings.Contains(got, "private chain") {
		t.Fatalf("unsupported replayed reasoning item was not removed: %s", got)
	}
	if strings.Contains(got, `"type":"output_text"`) || !strings.Contains(got, `"text":"prior answer","type":"input_text"`) {
		t.Fatalf("assistant replay content was not normalized: %s", got)
	}
}

func TestNativeResponsesCompatibilityProxyNormalizesSubagentMessage(t *testing.T) {
	p := &nativeResponsesCompatibilityProxy{
		toWire:   map[nativeResponsesToolName]string{},
		fromWire: map[string]nativeResponsesToolName{},
	}
	raw, err := p.rewriteRequest([]byte(`{"input":[{"type":"agent_message","author":"/root/gpt_search","recipient":"/root","content":[{"type":"input_text","text":"Message Type: FINAL_ANSWER\nPayload:\nThe cited result."}]}]}`))
	if err != nil {
		t.Fatalf("rewrite request: %v", err)
	}
	got := string(raw)
	for _, want := range []string{
		`"type":"message"`,
		`"role":"developer"`,
		`"text":"Message Type: FINAL_ANSWER\nPayload:\nThe cited result."`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("normalized subagent message missing %q: %s", want, got)
		}
	}
	if strings.Contains(got, `"type":"agent_message"`) || strings.Contains(got, `"author"`) || strings.Contains(got, `"recipient"`) {
		t.Fatalf("internal subagent envelope leaked upstream: %s", got)
	}
}
