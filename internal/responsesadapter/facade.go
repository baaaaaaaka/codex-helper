package responsesadapter

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/responsespolicy"
	"github.com/gorilla/websocket"
)

type ScopeResolver func(*http.Request, ResponsesRequest) Scope

type IDGenerator func(prefix string) (string, error)

var (
	activeTurnWaitTimeout = 90 * time.Second
	activeTurnPollDelay   = 50 * time.Millisecond
)

type Facade struct {
	Adapter        ProviderAdapter
	Router         ProviderRouter
	Store          ResponseStore
	InstanceID     string
	ProviderID     string
	DefaultModel   string
	Models         []ModelInfo
	KeyFingerprint string
	BaseURLHash    string
	ProfileVersion string
	ScopeResolver  ScopeResolver
	NewID          IDGenerator
	// WebSocketRequestHook observes request arrival without exposing payloads.
	// It is intended for health/contract probes, not request logging.
	WebSocketRequestHook func()
	// ShellPolicy applies execution-target escalation only at the local Codex
	// boundary. Provider history and requests retain the original arguments.
	ShellPolicy *responsespolicy.ShellEscalationPolicy
}

func (f *Facade) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimSuffix(r.URL.Path, "/")
	switch {
	case websocket.IsWebSocketUpgrade(r) && (path == "/responses" || path == "/v1/responses"):
		f.handleResponsesWebSocket(w, r, path)
	case r.Method == http.MethodGet && (path == "/_codex_proxy/health" || path == "/_cxp/health" || path == "/v1/_cxp/health"):
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "status": "ok", "instanceId": f.InstanceID})
	case r.Method == http.MethodGet && (path == "/health" || path == "/v1/health"):
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	case r.Method == http.MethodGet && (path == "/models" || path == "/v1/models"):
		f.handleModels(w)
	case r.Method == http.MethodPost && (path == "/responses/compact" || path == "/v1/responses/compact"):
		f.handleCompact(w, r)
	case r.Method == http.MethodPost && (path == "/responses" || path == "/v1/responses"):
		f.handleResponses(w, r)
	default:
		writeJSON(w, http.StatusNotFound, map[string]any{"error": map[string]string{"message": "not found"}})
	}
}

func (f *Facade) handleResponsesWebSocket(w http.ResponseWriter, r *http.Request, path string) {
	upgrader := websocket.Upgrader{
		CheckOrigin:       func(*http.Request) bool { return true },
		EnableCompression: true,
	}
	connection, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer connection.Close()
	connection.SetReadLimit(64 << 20)
	var warmupResponseID string
	var warmupInput []any
	for {
		messageType, raw, err := connection.ReadMessage()
		if err != nil {
			return
		}
		if messageType != websocket.TextMessage && messageType != websocket.BinaryMessage {
			continue
		}
		var requestBody map[string]any
		if json.Unmarshal(raw, &requestBody) != nil {
			_ = connection.WriteJSON(map[string]any{
				"type":  "error",
				"error": map[string]string{"message": "invalid request JSON"},
			})
			continue
		}
		if f.WebSocketRequestHook != nil {
			f.WebSocketRequestHook()
		}
		if generate, ok := requestBody["generate"].(bool); ok && !generate {
			responseID, err := f.newID("resp")
			if err != nil {
				return
			}
			warmupResponseID = responseID
			warmupInput, _ = requestBody["input"].([]any)
			warmupInput = append([]any(nil), warmupInput...)
			if err := connection.WriteJSON(map[string]any{
				"type":     "response.created",
				"response": map[string]any{"id": responseID},
			}); err != nil {
				return
			}
			if err := connection.WriteJSON(map[string]any{
				"type": "response.completed",
				"response": map[string]any{
					"id": responseID,
					"usage": map[string]any{
						"input_tokens": 0, "input_tokens_details": nil,
						"output_tokens": 0, "output_tokens_details": nil,
						"total_tokens": 0,
					},
				},
			}); err != nil {
				return
			}
			continue
		}
		if previous, _ := requestBody["previous_response_id"].(string); previous != "" && previous == warmupResponseID {
			incremental, _ := requestBody["input"].([]any)
			requestBody["input"] = append(append([]any(nil), warmupInput...), incremental...)
			delete(requestBody, "previous_response_id")
			warmupResponseID = ""
			warmupInput = nil
		}
		delete(requestBody, "type")
		requestBody["stream"] = true
		encoded, err := json.Marshal(requestBody)
		if err != nil {
			return
		}
		request, err := http.NewRequestWithContext(r.Context(), http.MethodPost, path, bytes.NewReader(encoded))
		if err != nil {
			return
		}
		request.Header = r.Header.Clone()
		reader, writer := io.Pipe()
		stream := newWebSocketStreamResponseWriter(writer)
		handlerDone := make(chan struct{})
		go func() {
			defer close(handlerDone)
			f.handleResponses(stream, request)
			stream.finish()
			_ = writer.Close()
		}()
		select {
		case <-stream.ready:
		case <-r.Context().Done():
			_ = reader.CloseWithError(r.Context().Err())
			<-handlerDone
			return
		}
		if stream.statusCode() >= http.StatusBadRequest {
			rawBody, _ := io.ReadAll(reader)
			<-handlerDone
			var body any
			if json.Unmarshal(rawBody, &body) != nil {
				body = map[string]string{"message": http.StatusText(stream.statusCode())}
			}
			if err := connection.WriteJSON(map[string]any{"type": "error", "error": body}); err != nil {
				return
			}
			continue
		}
		if err := writeSSEAsWebSocketEvents(connection, reader); err != nil {
			_ = reader.CloseWithError(err)
			<-handlerDone
			return
		}
		<-handlerDone
	}
}

type websocketStreamResponseWriter struct {
	header http.Header
	pipe   *io.PipeWriter
	ready  chan struct{}
	once   sync.Once
	mu     sync.Mutex
	status int
}

func newWebSocketStreamResponseWriter(pipe *io.PipeWriter) *websocketStreamResponseWriter {
	return &websocketStreamResponseWriter{header: make(http.Header), pipe: pipe, ready: make(chan struct{})}
}

func (w *websocketStreamResponseWriter) Header() http.Header { return w.header }

func (w *websocketStreamResponseWriter) WriteHeader(status int) {
	w.mu.Lock()
	if w.status == 0 {
		w.status = status
		w.once.Do(func() { close(w.ready) })
	}
	w.mu.Unlock()
}

func (w *websocketStreamResponseWriter) Write(payload []byte) (int, error) {
	w.WriteHeader(http.StatusOK)
	return w.pipe.Write(payload)
}

func (w *websocketStreamResponseWriter) Flush() {}

func (w *websocketStreamResponseWriter) finish() {
	w.WriteHeader(http.StatusOK)
}

func (w *websocketStreamResponseWriter) statusCode() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func writeSSEAsWebSocketEvents(connection *websocket.Conn, stream io.Reader) error {
	scanner := bufio.NewScanner(stream)
	scanner.Buffer(make([]byte, 64<<10), 64<<20)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if !bytes.HasPrefix(line, []byte("data:")) {
			continue
		}
		payload := bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:")))
		if len(payload) == 0 || bytes.Equal(payload, []byte("[DONE]")) {
			continue
		}
		if err := connection.WriteMessage(websocket.TextMessage, append([]byte(nil), payload...)); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func (f *Facade) handleModels(w http.ResponseWriter) {
	if f.Router != nil {
		writeModels(w, f.Router.Models(), f.ProviderID)
		return
	}
	models := f.Models
	if len(models) == 0 {
		model := strings.TrimSpace(f.DefaultModel)
		if model == "" {
			model = "adapter-default"
		}
		models = []ModelInfo{{ID: model, OwnedBy: firstNonEmpty(f.ProviderID, "adapter")}}
	}
	writeModels(w, models, f.ProviderID)
}

func writeModels(w http.ResponseWriter, models []ModelInfo, defaultOwner string) {
	data := make([]map[string]string, 0, len(models))
	for _, model := range models {
		data = append(data, map[string]string{
			"id":       model.ID,
			"object":   "model",
			"owned_by": firstNonEmpty(model.OwnedBy, defaultOwner, "adapter"),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": data})
}

func (f *Facade) handleResponses(w http.ResponseWriter, r *http.Request) {
	if f.Adapter == nil && f.Router == nil {
		writeJSON(w, http.StatusInternalServerError, errorBody("adapter is not configured"))
		return
	}
	store := f.Store
	if store == nil {
		store = NewMemoryStore()
		f.Store = store
	}
	var req ResponsesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody("invalid request JSON"))
		return
	}
	req.Model = firstNonEmpty(req.Model, f.DefaultModel)
	runtime, err := f.resolveRuntime(r, req)
	if err != nil {
		writeJSON(w, routeErrorStatus(err), errorBody(err.Error()))
		return
	}
	publicModel := firstNonEmpty(runtime.PublicModel, req.Model)
	upstreamModel := firstNonEmpty(runtime.Model, publicModel)
	req.Model = publicModel
	if runtime.Adapter == nil {
		writeJSON(w, http.StatusInternalServerError, errorBody("adapter is not configured"))
		return
	}
	responseID, err := f.newID("resp")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorBody(err.Error()))
		return
	}
	scope := f.resolveScope(r, req)
	scope.Provider = firstNonEmpty(scope.Provider, runtime.ProviderID, f.ProviderID, "adapter")
	scope.Model = publicModel
	scope.KeyFingerprint = firstNonEmpty(scope.KeyFingerprint, runtime.KeyFingerprint, f.KeyFingerprint)
	scope.BaseURLHash = firstNonEmpty(scope.BaseURLHash, runtime.BaseURLHash, f.BaseURLHash)
	scope.ProfileVersion = firstNonEmpty(scope.ProfileVersion, runtime.ProfileVersion, f.ProfileVersion)
	scope = scope.withDefaults()
	release, err := beginTurnWithWait(r.Context(), store, scope, responseID)
	if err != nil {
		if errors.Is(err, ErrActiveTurn) {
			writeJSON(w, http.StatusConflict, errorBody("conversation already has an active turn"))
			return
		}
		writeJSON(w, http.StatusInternalServerError, errorBody(err.Error()))
		return
	}
	defer release()
	history, err := f.resolveHistory(store, req.PreviousResponseID, scope)
	if err != nil {
		status := http.StatusNotFound
		if errors.Is(err, ErrScopeMismatch) {
			status = http.StatusConflict
		}
		writeJSON(w, status, errorBody(err.Error()))
		return
	}
	tools, toolWarnings, err := NormalizeTools(req.Tools)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody(err.Error()))
		return
	}
	parsedInput, err := parseInput(req.Input)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody(err.Error()))
		return
	}
	f.restoreProviderToolArguments(parsedInput.Messages)
	parsedInput.Messages, err = applyCachedReasoning(store, scope, parsedInput.Messages)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, errorBody(err.Error()))
		return
	}
	if err := validateToolMessageLinks(parsedInput.Messages, history); err != nil {
		writeJSON(w, http.StatusBadRequest, errorBody(err.Error()))
		return
	}
	providerReq := ProviderRequest{
		Model:              upstreamModel,
		Instructions:       req.Instructions,
		InputText:          parsedInput.Text,
		InputMessages:      parsedInput.Messages,
		Messages:           buildProviderMessages(history, parsedInput.Messages),
		Tools:              tools,
		ToolWarnings:       toolWarnings,
		ToolChoice:         req.ToolChoice,
		ParallelToolCalls:  req.ParallelToolCalls,
		MaxOutputTokens:    req.MaxOutputTokens,
		ReasoningEffort:    reasoningEffort(req.Reasoning),
		Temperature:        req.Temperature,
		TopP:               req.TopP,
		PreviousResponseID: req.PreviousResponseID,
		Scope:              scope,
		History:            history,
	}
	stream, err := runtime.Adapter.Stream(r.Context(), providerReq)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, errorBody(err.Error()))
		return
	}
	if req.Stream {
		f.streamResponse(w, r.Context(), responseID, req, providerReq, stream)
		return
	}
	f.completeResponse(w, r.Context(), responseID, req, providerReq, stream)
}

func (f *Facade) restoreProviderToolArguments(messages []ProviderMessage) {
	if f == nil || f.ShellPolicy == nil {
		return
	}
	for messageIndex := range messages {
		for callIndex := range messages[messageIndex].ToolCalls {
			call := &messages[messageIndex].ToolCalls[callIndex]
			call.Arguments = f.ShellPolicy.Restore(call.ID, call.Name, call.Arguments)
		}
	}
}

func (f *Facade) prepareProviderToolCalls(records []ToolCallRecord) []ToolCallRecord {
	if f == nil || f.ShellPolicy == nil || len(records) == 0 {
		return records
	}
	prepared := append([]ToolCallRecord(nil), records...)
	for index := range prepared {
		call := &prepared[index]
		call.Arguments = f.ShellPolicy.Prepare(call.ID, call.Name, call.Arguments)
	}
	return prepared
}

func beginTurnWithWait(ctx context.Context, store ResponseStore, scope Scope, responseID string) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	deadline := time.NewTimer(activeTurnWaitTimeout)
	defer deadline.Stop()
	for {
		release, err := store.BeginTurn(scope, responseID)
		if !errors.Is(err, ErrActiveTurn) {
			return release, err
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, err
		case <-time.After(activeTurnPollDelay):
		}
	}
}

func (f *Facade) resolveHistory(store ResponseStore, previousResponseID string, scope Scope) ([]ResponseRecord, error) {
	if strings.TrimSpace(previousResponseID) == "" {
		return nil, nil
	}
	return store.ResolveChain(previousResponseID, scope)
}

func (f *Facade) resolveScope(r *http.Request, req ResponsesRequest) Scope {
	if f.ScopeResolver != nil {
		return f.ScopeResolver(r, req)
	}
	thread := firstNonEmpty(
		r.Header.Get("x-codex-thread-id"),
		r.Header.Get("x-codex-conversation-id"),
		r.Header.Get("x-codex-parent-thread-id"),
		req.PromptCacheKey,
	)
	return Scope{
		Tenant:         r.Header.Get("x-adapter-tenant"),
		User:           r.Header.Get("x-adapter-user"),
		Provider:       f.ProviderID,
		Model:          req.Model,
		Thread:         thread,
		Branch:         r.Header.Get("x-adapter-branch"),
		KeyFingerprint: r.Header.Get("x-adapter-key-fingerprint"),
		BaseURLHash:    r.Header.Get("x-adapter-base-url-hash"),
		ProfileVersion: r.Header.Get("x-adapter-profile-version"),
	}
}

func (f *Facade) newID(prefix string) (string, error) {
	if f.NewID != nil {
		return f.NewID(prefix)
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("rand: %w", err)
	}
	return prefix + "_" + hex.EncodeToString(b[:]), nil
}

func extractInputText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return ""
	}
	var parts []string
	for _, itemRaw := range items {
		if text := extractMessageText(itemRaw); text != "" {
			parts = append(parts, text)
		}
	}
	return strings.Join(parts, "\n")
}

func extractMessageText(raw json.RawMessage) string {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return ""
	}
	if contentRaw, ok := obj["content"]; ok {
		if text := extractContentText(contentRaw); text != "" {
			return text
		}
	}
	return ""
}

func extractContentText(raw json.RawMessage) string {
	text, _ := extractMessageContent(raw)
	return text
}

func extractMessageContent(raw json.RawMessage) (string, []ProviderContentPart) {
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s, nil
	}
	var parts []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &parts); err != nil {
		return "", nil
	}
	var textOut []string
	var contentParts []ProviderContentPart
	for _, part := range parts {
		partType := rawString(part["type"])
		for _, key := range []string{"text", "input_text"} {
			if value, ok := part[key]; ok {
				var text string
				if err := json.Unmarshal(value, &text); err == nil && text != "" {
					textOut = append(textOut, text)
					if partType == "input_text" || partType == "text" {
						contentParts = append(contentParts, ProviderContentPart{Type: "text", Text: text})
					}
				}
			}
		}
		if partType == "input_image" || partType == "image_url" {
			url, detail := imageURLPart(part["image_url"])
			if url == "" {
				url = rawString(part["url"])
			}
			if detail == "" {
				detail = rawString(part["detail"])
			}
			if url != "" {
				contentParts = append(contentParts, ProviderContentPart{Type: "image_url", ImageURL: url, Detail: detail})
			}
			textOut = append(textOut, "image attachment omitted")
		}
	}
	if !hasImagePart(contentParts) {
		contentParts = nil
	}
	return strings.Join(textOut, "\n"), contentParts
}

func imageURLPart(raw json.RawMessage) (string, string) {
	if url := rawString(raw); url != "" {
		return url, ""
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return "", ""
	}
	return rawString(obj["url"]), rawString(obj["detail"])
}

func hasImagePart(parts []ProviderContentPart) bool {
	for _, part := range parts {
		if part.Type == "image_url" && part.ImageURL != "" {
			return true
		}
	}
	return false
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func errorBody(message string) map[string]any {
	return map[string]any{"error": map[string]string{"message": message}}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func reasoningEffort(reasoning *ReasoningInput) string {
	if reasoning == nil {
		return ""
	}
	return strings.TrimSpace(reasoning.Effort)
}
