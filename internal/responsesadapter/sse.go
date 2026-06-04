package responsesadapter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
)

type responseObject struct {
	ID                 string       `json:"id"`
	Object             string       `json:"object"`
	Status             string       `json:"status"`
	Model              string       `json:"model"`
	PreviousResponseID string       `json:"previous_response_id,omitempty"`
	Output             []outputItem `json:"output"`
	OutputText         string       `json:"output_text,omitempty"`
	Usage              *Usage       `json:"usage,omitempty"`
}

type outputItem struct {
	ID               string             `json:"id"`
	Type             string             `json:"type"`
	Role             string             `json:"role,omitempty"`
	Status           string             `json:"status,omitempty"`
	CallID           string             `json:"call_id,omitempty"`
	Name             string             `json:"name,omitempty"`
	Arguments        string             `json:"arguments,omitempty"`
	Summary          []reasoningSummary `json:"summary,omitempty"`
	EncryptedContent string             `json:"encrypted_content,omitempty"`
	Content          []contentPart      `json:"content,omitempty"`
}

type contentPart struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

type reasoningSummary struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

func (i outputItem) MarshalJSON() ([]byte, error) {
	item := map[string]any{
		"type": i.Type,
	}
	if i.ID != "" {
		item["id"] = i.ID
	}
	switch i.Type {
	case "message":
		item["role"] = i.Role
		content := i.Content
		if content == nil {
			content = []contentPart{}
		}
		item["content"] = content
	case "reasoning":
		summary := i.Summary
		if summary == nil {
			summary = []reasoningSummary{}
		}
		item["summary"] = summary
		if i.EncryptedContent != "" {
			item["encrypted_content"] = i.EncryptedContent
		}
		if len(i.Content) > 0 {
			item["content"] = i.Content
		}
	case "function_call":
		if i.Status != "" {
			item["status"] = i.Status
		}
		item["call_id"] = i.CallID
		item["name"] = i.Name
		item["arguments"] = i.Arguments
	default:
		if i.Role != "" {
			item["role"] = i.Role
		}
		if i.Status != "" {
			item["status"] = i.Status
		}
		if i.CallID != "" {
			item["call_id"] = i.CallID
		}
		if i.Name != "" {
			item["name"] = i.Name
		}
		if i.Arguments != "" {
			item["arguments"] = i.Arguments
		}
		if len(i.Summary) > 0 {
			item["summary"] = i.Summary
		}
		if i.EncryptedContent != "" {
			item["encrypted_content"] = i.EncryptedContent
		}
		if len(i.Content) > 0 {
			item["content"] = i.Content
		}
	}
	return json.Marshal(item)
}

func (f *Facade) completeResponse(w http.ResponseWriter, ctx context.Context, responseID string, req ResponsesRequest, providerReq ProviderRequest, stream <-chan ProviderEvent) {
	result, ok := f.collectProviderResult(ctx, stream)
	if !ok {
		writeJSON(w, http.StatusBadGateway, errorBody("provider stream ended before completion"))
		return
	}
	response := buildResponseObject(responseID, req, req.Model, result.text, result.messageOutputIndex, result.reasoningText, result.reasoningOutputIndex, result.usage, result.toolCalls)
	if f.Store != nil {
		if err := f.Store.Store(ResponseRecord{
			ID:                 responseID,
			PreviousResponseID: req.PreviousResponseID,
			Scope:              providerReq.Scope,
			InputText:          providerReq.InputText,
			InputMessages:      providerReq.InputMessages,
			OutputText:         result.text,
			ReasoningText:      result.reasoningText,
			ToolCalls:          result.toolCalls,
			Status:             ResponseStatusCompleted,
			Model:              req.Model,
			Usage:              result.usage,
		}); err != nil {
			writeJSON(w, http.StatusInternalServerError, errorBody(err.Error()))
			return
		}
	}
	writeJSON(w, http.StatusOK, response)
}

func (f *Facade) streamResponse(w http.ResponseWriter, ctx context.Context, responseID string, req ResponsesRequest, providerReq ProviderRequest, stream <-chan ProviderEvent) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeJSON(w, http.StatusInternalServerError, errorBody("streaming is not supported by this response writer"))
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	sequence := 0
	writeEvent := func(name string, payload map[string]any) {
		sequence++
		payload["type"] = name
		payload["sequence_number"] = sequence
		writeSSE(w, name, payload)
		flusher.Flush()
	}

	startedMessageItem := false
	messageOutputIndex := -1
	nextOutputIndex := 0
	text := ""
	reasoning := ""
	startedReasoningItem := false
	reasoningOutputIndex := -1
	var usage *Usage
	toolCalls := newToolCallAccumulator(responseID)
	created := buildResponseObject(responseID, req, req.Model, "", 0, "", -1, nil, nil)
	created.Status = string(ResponseStatusInProgress)
	writeEvent("response.created", map[string]any{"response": created})

	ensureReasoningItem := func() {
		if startedReasoningItem {
			return
		}
		startedReasoningItem = true
		reasoningOutputIndex = nextOutputIndex
		nextOutputIndex++
		writeEvent("response.output_item.added", map[string]any{
			"output_index": reasoningOutputIndex,
			"item":         buildReasoningItem(responseID, ""),
		})
	}

	ensureMessageItem := func() {
		if startedMessageItem {
			return
		}
		startedMessageItem = true
		messageOutputIndex = nextOutputIndex
		nextOutputIndex++
		writeEvent("response.output_item.added", map[string]any{
			"output_index": messageOutputIndex,
			"item":         buildMessageItem(responseID, ""),
		})
	}

	emitToolCallAdded := func(call *toolCallState) {
		writeEvent("response.output_item.added", map[string]any{
			"output_index": call.OutputIndex,
			"item":         buildToolCallItem(call.record("in_progress")),
		})
	}

	emitToolCallDelta := func(call *toolCallState, delta string) {
		if delta == "" {
			return
		}
		writeEvent("response.function_call_arguments.delta", map[string]any{
			"output_index": call.OutputIndex,
			"item_id":      call.ItemID,
			"delta":        delta,
		})
	}

	emitFailure := func(message string) {
		writeEvent("response.failed", map[string]any{"response": failedResponseObject(responseID, req, req.Model, text, messageOutputIndex, reasoning, reasoningOutputIndex, usage, toolCalls.records(), message)})
	}

	for {
		select {
		case <-ctx.Done():
			emitFailure("client cancelled")
			return
		case event, ok := <-stream:
			if !ok {
				emitFailure("provider stream ended before completion")
				return
			}
			switch event.Kind {
			case ProviderEventReasoningDelta:
				ensureReasoningItem()
				reasoning += event.Delta
				writeEvent("response.reasoning_text.delta", map[string]any{
					"output_index":  reasoningOutputIndex,
					"item_id":       reasoningItemID(responseID),
					"content_index": 0,
					"delta":         event.Delta,
				})
			case ProviderEventTextDelta:
				ensureMessageItem()
				text += event.Delta
				writeEvent("response.output_text.delta", map[string]any{
					"output_index":  messageOutputIndex,
					"content_index": 0,
					"delta":         event.Delta,
				})
			case ProviderEventToolCallDelta:
				call, added, err := toolCalls.apply(event.ToolCall, &nextOutputIndex)
				if err != nil {
					emitFailure(err.Error())
					return
				}
				if added {
					emitToolCallAdded(call)
				}
				if call != nil && call.Added {
					emitToolCallDelta(call, event.ToolCall.ArgumentsDelta)
				}
			case ProviderEventUsage:
				usage = event.Usage
			case ProviderEventError:
				msg := "provider error"
				if event.Err != nil {
					msg = event.Err.Error()
				}
				emitFailure(msg)
				return
			case ProviderEventDone:
				if err := toolCalls.validateComplete(); err != nil {
					emitFailure(err.Error())
					return
				}
				if !startedMessageItem && toolCalls.len() == 0 && !startedReasoningItem {
					ensureMessageItem()
				}
				for _, doneItem := range buildDoneItems(responseID, messageOutputIndex, text, reasoningOutputIndex, reasoning, toolCalls.records(), startedMessageItem, startedReasoningItem) {
					writeEvent("response.output_item.done", map[string]any{
						"output_index": doneItem.outputIndex,
						"item":         doneItem.item,
					})
				}
				records := toolCalls.records()
				response := buildResponseObject(responseID, req, req.Model, text, messageOutputIndex, reasoning, reasoningOutputIndex, usage, records)
				if f.Store != nil {
					if err := f.Store.Store(ResponseRecord{
						ID:                 responseID,
						PreviousResponseID: req.PreviousResponseID,
						Scope:              providerReq.Scope,
						InputText:          providerReq.InputText,
						InputMessages:      providerReq.InputMessages,
						OutputText:         text,
						ReasoningText:      reasoning,
						ToolCalls:          records,
						Status:             ResponseStatusCompleted,
						Model:              req.Model,
						Usage:              usage,
					}); err != nil {
						writeEvent("response.failed", map[string]any{"response": failedResponseObject(responseID, req, req.Model, text, messageOutputIndex, reasoning, reasoningOutputIndex, usage, records, err.Error())})
						return
					}
				}
				writeEvent("response.completed", map[string]any{"response": response})
				return
			}
		}
	}
}

type providerResult struct {
	text                 string
	messageOutputIndex   int
	reasoningText        string
	reasoningOutputIndex int
	usage                *Usage
	toolCalls            []ToolCallRecord
}

func (f *Facade) collectProviderResult(ctx context.Context, stream <-chan ProviderEvent) (providerResult, bool) {
	result := providerResult{messageOutputIndex: -1, reasoningOutputIndex: -1}
	toolCalls := newToolCallAccumulator("")
	nextOutputIndex := 0
	startedMessage := false
	startedReasoning := false
	for {
		select {
		case <-ctx.Done():
			return providerResult{}, false
		case event, ok := <-stream:
			if !ok {
				return providerResult{}, false
			}
			switch event.Kind {
			case ProviderEventReasoningDelta:
				if !startedReasoning {
					startedReasoning = true
					result.reasoningOutputIndex = nextOutputIndex
					nextOutputIndex++
				}
				result.reasoningText += event.Delta
			case ProviderEventTextDelta:
				if !startedMessage {
					startedMessage = true
					result.messageOutputIndex = nextOutputIndex
					nextOutputIndex++
				}
				result.text += event.Delta
			case ProviderEventToolCallDelta:
				if _, _, err := toolCalls.apply(event.ToolCall, &nextOutputIndex); err != nil {
					return providerResult{}, false
				}
			case ProviderEventUsage:
				result.usage = event.Usage
			case ProviderEventError:
				return providerResult{}, false
			case ProviderEventDone:
				if err := toolCalls.validateComplete(); err != nil {
					return providerResult{}, false
				}
				result.toolCalls = toolCalls.records()
				return result, true
			}
		}
	}
}

func buildResponseObject(responseID string, req ResponsesRequest, model string, text string, messageOutputIndex int, reasoning string, reasoningOutputIndex int, usage *Usage, toolCalls []ToolCallRecord) responseObject {
	return responseObject{
		ID:                 responseID,
		Object:             "response",
		Status:             string(ResponseStatusCompleted),
		Model:              model,
		PreviousResponseID: req.PreviousResponseID,
		Output:             buildOutputItems(responseID, text, messageOutputIndex, reasoning, reasoningOutputIndex, toolCalls),
		OutputText:         text,
		Usage:              usage,
	}
}

func failedResponseObject(responseID string, req ResponsesRequest, model string, text string, messageOutputIndex int, reasoning string, reasoningOutputIndex int, usage *Usage, toolCalls []ToolCallRecord, message string) map[string]any {
	response := buildResponseObject(responseID, req, model, text, messageOutputIndex, reasoning, reasoningOutputIndex, usage, toolCalls)
	response.Status = string(ResponseStatusFailed)
	return map[string]any{
		"id":                   response.ID,
		"object":               response.Object,
		"status":               response.Status,
		"model":                response.Model,
		"previous_response_id": response.PreviousResponseID,
		"output":               response.Output,
		"output_text":          response.OutputText,
		"usage":                response.Usage,
		"error": map[string]string{
			"message": message,
		},
	}
}

func buildOutputItems(responseID string, text string, messageOutputIndex int, reasoning string, reasoningOutputIndex int, toolCalls []ToolCallRecord) []outputItem {
	doneItems := make([]doneItem, 0, 1+len(toolCalls))
	if reasoning != "" {
		if reasoningOutputIndex < 0 {
			reasoningOutputIndex = 0
		}
		doneItems = append(doneItems, doneItem{outputIndex: reasoningOutputIndex, item: buildReasoningItem(responseID, reasoning)})
	}
	if text != "" || (len(toolCalls) == 0 && reasoning == "") {
		if messageOutputIndex < 0 {
			messageOutputIndex = 0
		}
		doneItems = append(doneItems, doneItem{outputIndex: messageOutputIndex, item: buildMessageItem(responseID, text)})
	}
	for _, call := range sortedToolCalls(toolCalls) {
		doneItems = append(doneItems, doneItem{outputIndex: call.OutputIndex, item: buildToolCallItem(call)})
	}
	sort.SliceStable(doneItems, func(i, j int) bool {
		return doneItems[i].outputIndex < doneItems[j].outputIndex
	})
	items := make([]outputItem, 0, len(doneItems))
	for _, doneItem := range doneItems {
		items = append(items, doneItem.item)
	}
	return items
}

func buildMessageItem(responseID string, text string) outputItem {
	item := outputItem{
		ID:      "msg_" + responseID,
		Type:    "message",
		Role:    "assistant",
		Content: []contentPart{{Type: "output_text", Text: text}},
	}
	return item
}

func buildReasoningItem(responseID string, text string) outputItem {
	item := outputItem{
		ID:      reasoningItemID(responseID),
		Type:    "reasoning",
		Summary: []reasoningSummary{},
	}
	if text != "" {
		item.EncryptedContent = text
		item.Content = []contentPart{{Type: "reasoning_text", Text: text}}
	}
	return item
}

func reasoningItemID(responseID string) string {
	return "rs_" + responseID
}

func buildToolCallItem(call ToolCallRecord) outputItem {
	return outputItem{
		ID:        call.ItemID,
		Type:      "function_call",
		Status:    firstNonEmpty(call.Status, "completed"),
		CallID:    call.ID,
		Name:      call.Name,
		Arguments: call.Arguments,
	}
}

type doneItem struct {
	outputIndex int
	item        outputItem
}

func buildDoneItems(responseID string, messageOutputIndex int, text string, reasoningOutputIndex int, reasoning string, toolCalls []ToolCallRecord, includeMessage bool, includeReasoning bool) []doneItem {
	items := make([]doneItem, 0, 1+len(toolCalls))
	if includeReasoning {
		items = append(items, doneItem{outputIndex: reasoningOutputIndex, item: buildReasoningItem(responseID, reasoning)})
	}
	if includeMessage {
		items = append(items, doneItem{outputIndex: messageOutputIndex, item: buildMessageItem(responseID, text)})
	}
	for _, call := range sortedToolCalls(toolCalls) {
		items = append(items, doneItem{outputIndex: call.OutputIndex, item: buildToolCallItem(call)})
	}
	sort.SliceStable(items, func(i, j int) bool {
		return items[i].outputIndex < items[j].outputIndex
	})
	return items
}

func sortedToolCalls(toolCalls []ToolCallRecord) []ToolCallRecord {
	out := append([]ToolCallRecord(nil), toolCalls...)
	sort.SliceStable(out, func(i, j int) bool {
		return out[i].OutputIndex < out[j].OutputIndex
	})
	return out
}

type toolCallAccumulator struct {
	responseID string
	byIndex    map[int]*toolCallState
	order      []*toolCallState
}

type toolCallState struct {
	Index       int
	OutputIndex int
	ItemID      string
	CallID      string
	Name        string
	Arguments   string
	Added       bool
}

func newToolCallAccumulator(responseID string) *toolCallAccumulator {
	return &toolCallAccumulator{
		responseID: responseID,
		byIndex:    make(map[int]*toolCallState),
	}
}

func (a *toolCallAccumulator) len() int {
	if a == nil {
		return 0
	}
	return len(a.order)
}

func (a *toolCallAccumulator) apply(delta *ProviderToolCallDelta, nextOutputIndex *int) (*toolCallState, bool, error) {
	if delta == nil {
		return nil, false, fmt.Errorf("tool call delta is missing")
	}
	call := a.ensure(delta.Index, nextOutputIndex)
	if delta.ID != "" {
		if call.Added && call.CallID != "" && call.CallID != delta.ID {
			return nil, false, fmt.Errorf("tool call %d changed id from %q to %q", call.Index, call.CallID, delta.ID)
		}
		call.CallID = delta.ID
	}
	if delta.Name != "" {
		if call.Added && call.Name != "" && call.Name != delta.Name {
			return nil, false, fmt.Errorf("tool call %d changed name from %q to %q", call.Index, call.Name, delta.Name)
		}
		call.Name = delta.Name
	}
	call.Arguments += delta.ArgumentsDelta

	added := false
	if !call.Added && call.Name != "" {
		call.Added = true
		added = true
	}
	return call, added, nil
}

func (a *toolCallAccumulator) ensure(index int, nextOutputIndex *int) *toolCallState {
	if call, ok := a.byIndex[index]; ok {
		return call
	}
	outputIndex := 0
	if nextOutputIndex != nil {
		outputIndex = *nextOutputIndex
		*nextOutputIndex = *nextOutputIndex + 1
	}
	itemID := fmt.Sprintf("fc_%s_%d", firstNonEmpty(a.responseID, "response"), index)
	callID := fmt.Sprintf("call_%s_%d", firstNonEmpty(a.responseID, "response"), index)
	call := &toolCallState{
		Index:       index,
		OutputIndex: outputIndex,
		ItemID:      itemID,
		CallID:      callID,
	}
	a.byIndex[index] = call
	a.order = append(a.order, call)
	return call
}

func (a *toolCallAccumulator) validateComplete() error {
	if a == nil {
		return nil
	}
	for _, call := range a.order {
		if call.Name == "" {
			return fmt.Errorf("tool call %d is missing a function name", call.Index)
		}
	}
	return nil
}

func (a *toolCallAccumulator) records() []ToolCallRecord {
	if a == nil {
		return nil
	}
	records := make([]ToolCallRecord, 0, len(a.order))
	for _, call := range a.order {
		records = append(records, call.record("completed"))
	}
	sort.SliceStable(records, func(i, j int) bool {
		return records[i].OutputIndex < records[j].OutputIndex
	})
	return records
}

func (c *toolCallState) record(status string) ToolCallRecord {
	arguments := c.Arguments
	if status == "completed" {
		arguments = sanitizeToolArguments(arguments)
	}
	return ToolCallRecord{
		Index:       c.Index,
		OutputIndex: c.OutputIndex,
		ItemID:      c.ItemID,
		ID:          c.CallID,
		Name:        c.Name,
		Arguments:   arguments,
		Status:      status,
	}
}

func sanitizeToolArguments(arguments string) string {
	arguments = strings.TrimSpace(arguments)
	if arguments == "" {
		return "{}"
	}
	if json.Valid([]byte(arguments)) {
		return arguments
	}
	return "{}"
}

func writeSSE(w http.ResponseWriter, event string, payload map[string]any) {
	data, err := json.Marshal(payload)
	if err != nil {
		data = []byte(`{"type":"response.failed","error":{"message":"failed to encode SSE payload"}}`)
	}
	_, _ = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event, data)
}
