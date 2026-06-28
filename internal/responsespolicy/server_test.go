package responsespolicy

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestServerRoutesResponsesAndLeavesNonResponsesTrafficUnchanged(t *testing.T) {
	openAIPaths := make(chan string, 1)
	openAIBodies := make(chan string, 1)
	openAI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		openAIPaths <- r.URL.Path
		openAIBodies <- string(body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":"openai"}`)
	}))
	defer openAI.Close()
	chatGPTModelPaths := make(chan string, 1)
	chatGPTModel := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chatGPTModelPaths <- r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":"chatgpt-model"}`)
	}))
	defer chatGPTModel.Close()
	server, err := StartServer(ServerOptions{
		OpenAIUpstream:       openAI.URL + "/v1",
		ChatGPTModelUpstream: chatGPTModel.URL + "/backend-api/codex",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Close(ctx)
	}()

	response, err := http.Get(server.OpenAIBaseURL() + "/models")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if got := <-openAIPaths; got != "/v1/models" {
		t.Fatalf("OpenAI path = %q", got)
	}
	<-openAIBodies

	request, _ := http.NewRequest(http.MethodPost, server.OpenAIBaseURL()+"/responses", strings.NewReader(`{"model":"gpt"}`))
	request.Header.Set("ChatGPT-Account-ID", "account")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if got := <-chatGPTModelPaths; got != "/backend-api/codex/responses" {
		t.Fatalf("ChatGPT model path = %q", got)
	}

	analytics := `{"events":[{"name":"conversation"}]}`
	response, err = http.Post(server.OpenAIBaseURL()+"/analytics-events/events", "application/json", strings.NewReader(analytics))
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if got := <-openAIPaths; got != "/v1/analytics-events/events" {
		t.Fatalf("non-Responses path = %q", got)
	}
	if got := <-openAIBodies; got != analytics {
		t.Fatalf("non-Responses body changed: %s", got)
	}

	args := strings.Join(server.CodexConfigArgs(), " ")
	for _, want := range []string{"openai_base_url", `approval_policy="on-request"`, `approvals_reviewer="user"`, `sandbox_mode="read-only"`} {
		if !strings.Contains(args, want) {
			t.Fatalf("config args missing %q: %s", want, args)
		}
	}
	if strings.Contains(args, "chatgpt_base_url") {
		t.Fatalf("config args must preserve the official ChatGPT HTTPS origin: %s", args)
	}

	response, err = http.Get(server.capabilityBaseURL() + "/chatgpt/api/codex/config/bundle")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		t.Fatalf("general ChatGPT reverse-proxy route status = %d, want 404", response.StatusCode)
	}
}
