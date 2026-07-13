package cli

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
)

func TestUnifiedModelGatewayMergesCatalogAndIsolatesCredentials(t *testing.T) {
	var mu sync.Mutex
	var officialAuth string
	var officialAccount string
	var thirdAuth string
	var thirdAccount string
	var thirdGatewayKey string
	var thirdModel string
	var thirdOfficialMetadata string
	official := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		officialAuth = r.Header.Get("Authorization")
		officialAccount = r.Header.Get("ChatGPT-Account-ID")
		mu.Unlock()
		if strings.HasSuffix(r.URL.Path, "/models") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-official","display_name":"Official","priority":9,"supported_reasoning_levels":[{"effort":"max"}],"future":{"kept":true}}]}`)
			return
		}
		writeTestResponsesSSE(w, "OFFICIAL_OK")
	}))
	defer official.Close()
	third := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Model string `json:"model"`
		}
		_ = json.NewDecoder(r.Body).Decode(&request)
		mu.Lock()
		thirdAuth = r.Header.Get("Authorization")
		thirdAccount = r.Header.Get("ChatGPT-Account-ID")
		thirdGatewayKey = r.Header.Get(cxpUnifiedGatewayHeader)
		thirdModel = request.Model
		thirdOfficialMetadata = r.Header.Get("OpenAI-Organization")
		mu.Unlock()
		writeTestResponsesSSE(w, "THIRD_OK")
	}))
	defer third.Close()

	resolved := modelprofile.Resolved{
		Name: "third",
		Profile: config.ModelProfile{
			Provider:  "responses-compatible",
			Model:     "cxp/model",
			BaseURL:   third.URL + "/v1",
			APIKeyRef: "env:THIRD_KEY",
			Revision:  1,
		},
		Provider: modelprofile.ProviderSpec{
			ID:              "responses-compatible",
			DisplayName:     "Third",
			DefaultModel:    "cxp/model",
			Models:          []modelprofile.ModelSpec{{ID: "cxp/model", UpstreamID: "vendor/model", SupportsReason: true}},
			BaseURL:         third.URL + "/v1",
			AdapterProfile:  "openai-responses",
			UsesAdapter:     true,
			DirectResponses: true,
			SupportsReason:  true,
		},
		Model: modelprofile.ModelSpec{ID: "cxp/model", UpstreamID: "vendor/model", SupportsReason: true},
	}
	catalogPath := filepath.Join(t.TempDir(), "catalog.json")
	gateway, cleanup, err := newUnifiedModelGateway(unifiedModelGatewayOptions{
		OfficialBaseURL: official.URL,
		LocalKey:        "local-secret",
		CatalogPath:     catalogPath,
		Providers:       []modelprofile.Resolved{resolved},
		APIKeys:         map[string]string{"third": "third-secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	server := httptest.NewServer(gateway)
	defer server.Close()
	readyRequest, _ := http.NewRequest(http.MethodGet, server.URL+"/_cxp/ready", nil)
	setUnifiedTestHeaders(readyRequest)
	readyResponse, err := http.DefaultClient.Do(readyRequest)
	if err != nil {
		t.Fatal(err)
	}
	_ = readyResponse.Body.Close()
	if readyResponse.StatusCode != http.StatusOK {
		t.Fatalf("gateway readiness with third-party fallback catalog = %d, want 200", readyResponse.StatusCode)
	}

	modelsRequest, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/models?client_version=0.144.1", nil)
	setUnifiedTestHeaders(modelsRequest)
	modelsResponse, err := http.DefaultClient.Do(modelsRequest)
	if err != nil {
		t.Fatal(err)
	}
	modelsBody, _ := io.ReadAll(modelsResponse.Body)
	_ = modelsResponse.Body.Close()
	if modelsResponse.StatusCode != http.StatusOK {
		t.Fatalf("models status = %d, body=%s", modelsResponse.StatusCode, modelsBody)
	}
	readyRequest, _ = http.NewRequest(http.MethodGet, server.URL+"/_cxp/ready", nil)
	setUnifiedTestHeaders(readyRequest)
	readyResponse, err = http.DefaultClient.Do(readyRequest)
	if err != nil {
		t.Fatal(err)
	}
	_ = readyResponse.Body.Close()
	if readyResponse.StatusCode != http.StatusOK {
		t.Fatalf("gateway readiness after model merge = %d, want 200", readyResponse.StatusCode)
	}
	var catalog struct {
		Models []map[string]any `json:"models"`
	}
	if err := json.Unmarshal(modelsBody, &catalog); err != nil {
		t.Fatal(err)
	}
	if len(catalog.Models) != 2 || catalog.Models[0]["slug"] != "gpt-official" || catalog.Models[1]["slug"] != "cxp/model" {
		t.Fatalf("merged catalog = %#v", catalog.Models)
	}
	if catalog.Models[0]["future"].(map[string]any)["kept"] != true {
		t.Fatalf("official future field was not preserved: %#v", catalog.Models[0])
	}

	officialResponse := doUnifiedTestResponse(t, server.URL, "gpt-official")
	if !strings.Contains(officialResponse, "OFFICIAL_OK") {
		t.Fatalf("official response = %q", officialResponse)
	}
	thirdResponse := doUnifiedTestResponse(t, server.URL, "cxp/model")
	if !strings.Contains(thirdResponse, "THIRD_OK") {
		t.Fatalf("third response = %q", thirdResponse)
	}
	mu.Lock()
	defer mu.Unlock()
	if officialAuth != "Bearer official-secret" || officialAccount != "account-1" {
		t.Fatalf("official credentials = auth %q account %q", officialAuth, officialAccount)
	}
	if thirdAuth != "Bearer third-secret" || thirdAccount != "" || thirdGatewayKey != "" {
		t.Fatalf("third credentials = auth %q account %q gateway %q", thirdAuth, thirdAccount, thirdGatewayKey)
	}
	if thirdModel != "vendor/model" {
		t.Fatalf("third upstream model = %q, want vendor/model", thirdModel)
	}
	if thirdOfficialMetadata != "" {
		t.Fatalf("third upstream received official metadata %q", thirdOfficialMetadata)
	}
}

func TestUnifiedModelGatewayRejectsUnknownModel(t *testing.T) {
	official := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-official","priority":1}]}`)
	}))
	defer official.Close()
	gateway, cleanup, err := newUnifiedModelGateway(unifiedModelGatewayOptions{
		OfficialBaseURL: official.URL,
		LocalKey:        "local-secret",
		CatalogPath:     filepath.Join(t.TempDir(), "catalog.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	server := httptest.NewServer(gateway)
	defer server.Close()
	modelsRequest, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/models", nil)
	setUnifiedTestHeaders(modelsRequest)
	response, err := http.DefaultClient.Do(modelsRequest)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/responses", strings.NewReader(`{"model":"unknown"}`))
	setUnifiedTestHeaders(request)
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown model status = %d, want 400", response.StatusCode)
	}
}

func TestSanitizeOfficialResponsesHistoryDropsThirdPartyReasoningOnly(t *testing.T) {
	raw := []byte(`{
		"model":"gpt-official",
		"input":[
			{"type":"message","role":"developer","content":[{"type":"input_text","text":"rules"}]},
			{"type":"reasoning","id":"rs_official_1","summary":[],"encrypted_content":"opaque-official-token"},
			{"type":"reasoning","id":"rs_resp_old_adapter","summary":[],"encrypted_content":"plaintext-old","content":[{"type":"reasoning_text","text":"plaintext-old"}]},
			{"type":"reasoning","id":"rs_vendor_plain","summary":[],"content":[{"type":"reasoning_text","text":"vendor thought"}]},
			{"type":"message","role":"assistant","content":[{"type":"output_text","text":"visible answer"}]},
			{"type":"function_call","call_id":"call_1","name":"read_file","arguments":"{}"},
			{"type":"function_call_output","call_id":"call_1","output":"tool result"},
			{"type":"message","role":"user","content":[{"type":"input_text","text":"continue"}]}
		],
		"tools":[{"type":"function","name":"read_file","parameters":{"type":"object"}}]
	}`)

	sanitized, dropped, err := sanitizeOfficialResponsesHistory(raw)
	if err != nil {
		t.Fatal(err)
	}
	if dropped != 2 {
		t.Fatalf("dropped reasoning items = %d, want 2", dropped)
	}
	var request struct {
		Input []map[string]any `json:"input"`
		Tools []map[string]any `json:"tools"`
	}
	if err := json.Unmarshal(sanitized, &request); err != nil {
		t.Fatal(err)
	}
	if len(request.Input) != 6 {
		t.Fatalf("sanitized input length = %d, want 6: %s", len(request.Input), sanitized)
	}
	if request.Input[1]["id"] != "rs_official_1" || request.Input[1]["encrypted_content"] != "opaque-official-token" {
		t.Fatalf("official reasoning was not preserved: %#v", request.Input[1])
	}
	wantTypes := []string{"message", "reasoning", "message", "function_call", "function_call_output", "message"}
	for index, want := range wantTypes {
		if got := request.Input[index]["type"]; got != want {
			t.Fatalf("input %d type = %#v, want %q", index, got, want)
		}
	}
	if len(request.Tools) != 1 || request.Tools[0]["name"] != "read_file" {
		t.Fatalf("official tools changed during history sanitization: %#v", request.Tools)
	}
	if strings.Contains(string(sanitized), "plaintext-old") || strings.Contains(string(sanitized), "vendor thought") {
		t.Fatalf("third-party reasoning leaked into official request: %s", sanitized)
	}
}

func TestSanitizeOfficialResponsesHistoryLeavesOfficialOnlyBodyByteStable(t *testing.T) {
	raw := []byte(`{"model":"gpt-official","input":[{"type":"reasoning","id":"rs_official","summary":[],"encrypted_content":"opaque"},{"role":"user","content":"continue"}]}`)
	got, dropped, err := sanitizeOfficialResponsesHistory(raw)
	if err != nil {
		t.Fatal(err)
	}
	if dropped != 0 || string(got) != string(raw) {
		t.Fatalf("official-only request changed: dropped=%d got=%s", dropped, got)
	}
}

func TestSanitizeOfficialResponsesHistoryPreservesOpaqueOfficialReasoningWithContent(t *testing.T) {
	raw := []byte(`{"model":"gpt-official","input":[{"type":"reasoning","id":"rs_official_future","encrypted_content":"opaque","content":[{"type":"reasoning_text","text":"display summary"}]},{"role":"user","content":"continue"}]}`)
	got, dropped, err := sanitizeOfficialResponsesHistory(raw)
	if err != nil {
		t.Fatal(err)
	}
	if dropped != 0 || string(got) != string(raw) {
		t.Fatalf("opaque official reasoning changed: dropped=%d got=%s", dropped, got)
	}
}

func TestSanitizeThirdPartyResponsesHistoryRemovesOnlyOpaqueEncryptedContent(t *testing.T) {
	body := []byte(`{"model":"vendor/model","input":[{"type":"message","role":"user","content":"keep user"},{"type":"reasoning","id":"rs_official","encrypted_content":"opaque-official-ciphertext","summary":[{"type":"summary_text","text":"keep visible summary"}]},{"type":"reasoning","id":"rs_resp_legacy","encrypted_content":"legacy-mislabeled-plaintext","content":[{"type":"reasoning_text","text":"keep portable legacy content"}]},{"type":"message","role":"assistant","content":"keep assistant"}]}`)
	sanitized, stripped, err := sanitizeThirdPartyResponsesHistory(body)
	if err != nil {
		t.Fatal(err)
	}
	if stripped != 2 {
		t.Fatalf("stripped = %d, want 2", stripped)
	}
	text := string(sanitized)
	for _, absent := range []string{"opaque-official-ciphertext", "legacy-mislabeled-plaintext", "encrypted_content"} {
		if strings.Contains(text, absent) {
			t.Fatalf("sanitized history retained %q: %s", absent, text)
		}
	}
	for _, present := range []string{"keep user", "keep visible summary", "keep portable legacy content", "keep assistant"} {
		if !strings.Contains(text, present) {
			t.Fatalf("sanitized history lost %q: %s", present, text)
		}
	}
}

func TestUnifiedModelGatewaySanitizesHistoryBeforeOfficialUpstream(t *testing.T) {
	var received string
	official := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/models") {
			_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-official","priority":1}]}`)
			return
		}
		raw, _ := io.ReadAll(r.Body)
		received = string(raw)
		writeTestResponsesSSE(w, "OFFICIAL_OK")
	}))
	defer official.Close()
	gateway, cleanup, err := newUnifiedModelGateway(unifiedModelGatewayOptions{
		OfficialBaseURL: official.URL,
		LocalKey:        "local-secret",
		CatalogPath:     filepath.Join(t.TempDir(), "catalog.json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	server := httptest.NewServer(gateway)
	defer server.Close()
	modelsRequest, _ := http.NewRequest(http.MethodGet, server.URL+"/v1/models", nil)
	setUnifiedTestHeaders(modelsRequest)
	modelsResponse, err := http.DefaultClient.Do(modelsRequest)
	if err != nil {
		t.Fatal(err)
	}
	_ = modelsResponse.Body.Close()

	body := `{"model":"gpt-official","stream":true,"input":[{"type":"reasoning","id":"rs_resp_third","content":[{"type":"reasoning_text","text":"private"}]},{"type":"message","role":"assistant","content":[{"type":"output_text","text":"visible"}]},{"type":"message","role":"user","content":[{"type":"input_text","text":"continue"}]}]}`
	request, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/responses", strings.NewReader(body))
	setUnifiedTestHeaders(request)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	responseBody, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("official response status = %d, body=%s", response.StatusCode, responseBody)
	}
	if strings.Contains(received, "private") || strings.Contains(received, "rs_resp_third") {
		t.Fatalf("official upstream received third-party reasoning: %s", received)
	}
	for _, want := range []string{"visible", "continue"} {
		if !strings.Contains(received, want) {
			t.Fatalf("official upstream lost portable context %q: %s", want, received)
		}
	}
}

func TestUnifiedTerminalResponseWriterFailsIncompleteSSE(t *testing.T) {
	recorder := httptest.NewRecorder()
	recorder.Header().Set("Content-Type", "text/event-stream")
	guard := &unifiedTerminalResponseWriter{ResponseWriter: recorder, etag: `"catalog"`}
	guard.WriteHeader(http.StatusOK)
	_, _ = guard.Write([]byte("event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"partial\"}\n\n"))
	guard.ensureTerminal()
	if !strings.Contains(recorder.Body.String(), "upstream_stream_incomplete") || !strings.Contains(recorder.Body.String(), "response.failed") {
		t.Fatalf("incomplete SSE was not terminated with a failure:\n%s", recorder.Body.String())
	}
	if recorder.Header().Get("X-Models-Etag") != `"catalog"` {
		t.Fatalf("X-Models-Etag = %q", recorder.Header().Get("X-Models-Etag"))
	}
}

func TestUnifiedTerminalResponseWriterPreservesCompletedSSE(t *testing.T) {
	recorder := httptest.NewRecorder()
	recorder.Header().Set("Content-Type", "text/event-stream")
	guard := &unifiedTerminalResponseWriter{ResponseWriter: recorder}
	guard.WriteHeader(http.StatusOK)
	_, _ = guard.Write([]byte("event: response.completed\ndata: {\"type\":\"response.completed\"}\n\n"))
	guard.ensureTerminal()
	if strings.Contains(recorder.Body.String(), "upstream_stream_incomplete") {
		t.Fatalf("completed SSE received a synthetic failure:\n%s", recorder.Body.String())
	}
}

func setUnifiedTestHeaders(request *http.Request) {
	request.Header.Set(cxpUnifiedGatewayHeader, "local-secret")
	request.Header.Set("Authorization", "Bearer official-secret")
	request.Header.Set("ChatGPT-Account-ID", "account-1")
	request.Header.Set("OpenAI-Organization", "official-org")
	request.Header.Set("Content-Type", "application/json")
}

func doUnifiedTestResponse(t *testing.T, baseURL string, model string) string {
	t.Helper()
	request, _ := http.NewRequest(http.MethodPost, baseURL+"/v1/responses", strings.NewReader(`{"model":"`+model+`","stream":true}`))
	setUnifiedTestHeaders(request)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != http.StatusOK {
		t.Fatalf("response status = %d, body=%s", response.StatusCode, body)
	}
	return string(body)
}

func writeTestResponsesSSE(w http.ResponseWriter, text string) {
	w.Header().Set("Content-Type", "text/event-stream")
	_, _ = io.WriteString(w, "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\""+text+"\"}\n\n")
	_, _ = io.WriteString(w, "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp-test\",\"status\":\"completed\",\"output\":[]}}\n\n")
}
