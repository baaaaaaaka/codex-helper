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

func TestServerRoutesOpenAIChatGPTAndLeavesAnalyticsUnchanged(t *testing.T) {
	openAIPaths := make(chan string, 1)
	openAI := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		openAIPaths <- r.URL.Path
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
	chatGPTBodies := make(chan string, 1)
	chatGPTPaths := make(chan string, 1)
	chatGPT := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		chatGPTPaths <- r.URL.Path
		chatGPTBodies <- string(body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"accepted":true}`)
	}))
	defer chatGPT.Close()

	server, err := StartServer(ServerOptions{
		OpenAIUpstream:       openAI.URL + "/v1",
		ChatGPTModelUpstream: chatGPTModel.URL + "/backend-api/codex",
		ChatGPTUpstream:      chatGPT.URL,
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
	response, err = http.Post(server.ChatGPTBaseURL()+"/analytics-events/events", "application/json", strings.NewReader(analytics))
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if got := <-chatGPTPaths; got != "/analytics-events/events" {
		t.Fatalf("ChatGPT path = %q", got)
	}
	if got := <-chatGPTBodies; got != analytics {
		t.Fatalf("analytics body changed: %s", got)
	}

	args := strings.Join(server.CodexConfigArgs(), " ")
	for _, want := range []string{"openai_base_url", "chatgpt_base_url", `approval_policy="on-request"`, `approvals_reviewer="user"`, `sandbox_mode="read-only"`} {
		if !strings.Contains(args, want) {
			t.Fatalf("config args missing %q: %s", want, args)
		}
	}
}
