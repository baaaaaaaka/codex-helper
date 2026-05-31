package responsesadapter

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

type compactRequest struct {
	Model        string          `json:"model,omitempty"`
	Input        json.RawMessage `json:"input,omitempty"`
	Instructions string          `json:"instructions,omitempty"`
}

type compactResponse struct {
	Output []compactOutputItem `json:"output"`
}

type compactOutputItem struct {
	Type    string        `json:"type"`
	Role    string        `json:"role,omitempty"`
	Content []contentPart `json:"content,omitempty"`
}

func (f *Facade) handleCompact(w http.ResponseWriter, r *http.Request) {
	if f.Adapter == nil && f.Router == nil {
		writeJSON(w, http.StatusInternalServerError, errorBody("adapter is not configured"))
		return
	}
	var req compactRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody("invalid request JSON"))
		return
	}
	req.Model = firstNonEmpty(req.Model, f.DefaultModel)
	runtime, err := f.resolveRuntime(r, ResponsesRequest{Model: req.Model})
	if err != nil {
		writeJSON(w, routeErrorStatus(err), errorBody(err.Error()))
		return
	}
	req.Model = firstNonEmpty(runtime.Model, req.Model)
	if runtime.Adapter == nil {
		writeJSON(w, http.StatusInternalServerError, errorBody("adapter is not configured"))
		return
	}
	parsedInput, err := parseInput(req.Input)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody(err.Error()))
		return
	}
	transcript := compactTranscript(parsedInput.Messages)
	if strings.TrimSpace(transcript) == "" {
		writeJSON(w, http.StatusOK, compactResponse{Output: nil})
		return
	}
	scope := f.resolveScope(r, ResponsesRequest{Model: req.Model})
	scope.Provider = firstNonEmpty(scope.Provider, runtime.ProviderID, f.ProviderID, "adapter")
	scope.Model = req.Model
	scope.KeyFingerprint = firstNonEmpty(scope.KeyFingerprint, runtime.KeyFingerprint, f.KeyFingerprint)
	scope.BaseURLHash = firstNonEmpty(scope.BaseURLHash, runtime.BaseURLHash, f.BaseURLHash)
	scope.ProfileVersion = firstNonEmpty(scope.ProfileVersion, runtime.ProfileVersion, f.ProfileVersion)
	scope = scope.withDefaults()
	providerReq := ProviderRequest{
		Model:     req.Model,
		InputText: transcript,
		Scope:     scope,
		Messages: []ProviderMessage{
			{Role: "system", Content: firstNonEmpty(req.Instructions, "Summarize the conversation into a compact replacement history.")},
			{Role: "user", Content: transcript},
		},
	}
	stream, err := runtime.Adapter.Stream(r.Context(), providerReq)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, errorBody(err.Error()))
		return
	}
	result, ok := f.collectProviderResult(r.Context(), stream)
	if !ok {
		writeJSON(w, http.StatusBadGateway, errorBody("provider stream ended before completion"))
		return
	}
	summary := strings.TrimSpace(result.text)
	if summary == "" {
		summary = "(no summary available)"
	}
	writeJSON(w, http.StatusOK, compactResponse{
		Output: []compactOutputItem{{
			Type:    "message",
			Role:    "user",
			Content: []contentPart{{Type: "input_text", Text: summary}},
		}},
	})
}

func compactTranscript(messages []ProviderMessage) string {
	var b strings.Builder
	for _, message := range messages {
		role := firstNonEmpty(message.Role, "user")
		if strings.TrimSpace(message.Content) != "" {
			_, _ = fmt.Fprintf(&b, "%s: %s\n", role, message.Content)
		}
		if strings.TrimSpace(message.ReasoningContent) != "" {
			_, _ = fmt.Fprintf(&b, "%s reasoning: %s\n", role, message.ReasoningContent)
		}
		for _, call := range message.ToolCalls {
			_, _ = fmt.Fprintf(&b, "assistant tool_call %s %s: %s\n", call.ID, call.Name, call.Arguments)
		}
		if message.Role == "tool" {
			_, _ = fmt.Fprintf(&b, "tool %s: %s\n", message.ToolCallID, message.Content)
		}
	}
	return strings.TrimSpace(b.String())
}
