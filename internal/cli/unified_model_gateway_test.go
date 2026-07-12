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
