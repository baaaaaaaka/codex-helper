package responsesadapter

import (
	"context"
	"encoding/json"
	"strings"
)

type Scope struct {
	Tenant         string
	User           string
	Provider       string
	Model          string
	Thread         string
	Branch         string
	KeyFingerprint string
	BaseURLHash    string
	ProfileVersion string
}

func (s Scope) key() string {
	parts := []string{s.Tenant, s.User, s.Provider, s.Model, s.Thread, s.Branch, s.KeyFingerprint, s.BaseURLHash, s.ProfileVersion}
	for i, part := range parts {
		parts[i] = strings.TrimSpace(part)
	}
	return strings.Join(parts, "\x00")
}

func (s Scope) withDefaults() Scope {
	if s.Tenant == "" {
		s.Tenant = "local"
	}
	if s.User == "" {
		s.User = "local"
	}
	if s.Provider == "" {
		s.Provider = "default"
	}
	if s.Model == "" {
		s.Model = "default"
	}
	if s.Thread == "" {
		s.Thread = "default"
	}
	if s.Branch == "" {
		s.Branch = "main"
	}
	if s.KeyFingerprint == "" {
		s.KeyFingerprint = "no-key"
	}
	if s.BaseURLHash == "" {
		s.BaseURLHash = "default-url"
	}
	if s.ProfileVersion == "" {
		s.ProfileVersion = "v1"
	}
	return s
}

type ResponsesRequest struct {
	Model              string          `json:"model,omitempty"`
	PromptCacheKey     string          `json:"prompt_cache_key,omitempty"`
	Instructions       string          `json:"instructions,omitempty"`
	Input              json.RawMessage `json:"input,omitempty"`
	Tools              json.RawMessage `json:"tools,omitempty"`
	ToolChoice         json.RawMessage `json:"tool_choice,omitempty"`
	ParallelToolCalls  *bool           `json:"parallel_tool_calls,omitempty"`
	MaxOutputTokens    *int            `json:"max_output_tokens,omitempty"`
	Reasoning          *ReasoningInput `json:"reasoning,omitempty"`
	Temperature        *float64        `json:"temperature,omitempty"`
	TopP               *float64        `json:"top_p,omitempty"`
	Stream             bool            `json:"stream,omitempty"`
	PreviousResponseID string          `json:"previous_response_id,omitempty"`
}

type ReasoningInput struct {
	Effort string `json:"effort,omitempty"`
}

type ProviderRequest struct {
	Model              string
	Instructions       string
	InputText          string
	InputMessages      []ProviderMessage
	Messages           []ProviderMessage
	Tools              []ChatTool
	ToolWarnings       []ToolWarning
	ToolChoice         json.RawMessage
	ParallelToolCalls  *bool
	MaxOutputTokens    *int
	ReasoningEffort    string
	Temperature        *float64
	TopP               *float64
	PreviousResponseID string
	Scope              Scope
	History            []ResponseRecord
}

type ProviderMessage struct {
	Role             string
	Content          string
	ContentParts     []ProviderContentPart
	ReasoningContent string
	ToolCallID       string
	ToolCalls        []ToolCallRecord
}

type ProviderContentPart struct {
	Type     string
	Text     string
	ImageURL string
	Detail   string
}

type ProviderAdapter interface {
	Stream(context.Context, ProviderRequest) (<-chan ProviderEvent, error)
}

type ProviderEventKind string

const (
	ProviderEventTextDelta      ProviderEventKind = "text_delta"
	ProviderEventReasoningDelta ProviderEventKind = "reasoning_delta"
	ProviderEventToolCallDelta  ProviderEventKind = "tool_call_delta"
	ProviderEventUsage          ProviderEventKind = "usage"
	ProviderEventDone           ProviderEventKind = "done"
	ProviderEventError          ProviderEventKind = "error"
)

type ProviderEvent struct {
	Kind     ProviderEventKind
	Delta    string
	ToolCall *ProviderToolCallDelta
	Usage    *Usage
	Err      error
}

type ProviderToolCallDelta struct {
	Index          int
	ID             string
	Name           string
	ArgumentsDelta string
}

type ToolCallRecord struct {
	Index       int
	OutputIndex int
	ItemID      string
	ID          string
	Name        string
	Arguments   string
	Status      string
}

type Usage struct {
	InputTokens     int
	OutputTokens    int
	TotalTokens     int
	CachedTokens    int
	ReasoningTokens int
}

type usageWire struct {
	InputTokens        int `json:"input_tokens,omitempty"`
	OutputTokens       int `json:"output_tokens,omitempty"`
	TotalTokens        int `json:"total_tokens,omitempty"`
	InputTokensDetails struct {
		CachedTokens int `json:"cached_tokens,omitempty"`
	} `json:"input_tokens_details,omitempty"`
	OutputTokensDetails struct {
		ReasoningTokens int `json:"reasoning_tokens,omitempty"`
	} `json:"output_tokens_details,omitempty"`
	CachedTokens    int `json:"cached_tokens,omitempty"`
	ReasoningTokens int `json:"reasoning_tokens,omitempty"`
}

func (u Usage) MarshalJSON() ([]byte, error) {
	wire := usageWire{
		InputTokens:  u.InputTokens,
		OutputTokens: u.OutputTokens,
		TotalTokens:  u.TotalTokens,
	}
	wire.InputTokensDetails.CachedTokens = u.CachedTokens
	wire.OutputTokensDetails.ReasoningTokens = u.ReasoningTokens
	return json.Marshal(wire)
}

func (u *Usage) UnmarshalJSON(raw []byte) error {
	var wire usageWire
	if err := json.Unmarshal(raw, &wire); err != nil {
		return err
	}
	u.InputTokens = wire.InputTokens
	u.OutputTokens = wire.OutputTokens
	u.TotalTokens = wire.TotalTokens
	u.CachedTokens = wire.InputTokensDetails.CachedTokens
	if u.CachedTokens == 0 {
		u.CachedTokens = wire.CachedTokens
	}
	u.ReasoningTokens = wire.OutputTokensDetails.ReasoningTokens
	if u.ReasoningTokens == 0 {
		u.ReasoningTokens = wire.ReasoningTokens
	}
	return nil
}

type ModelInfo struct {
	ID         string `json:"id"`
	OwnedBy    string `json:"owned_by,omitempty"`
	UpstreamID string `json:"-"`
}

type ChatTool struct {
	Type     string       `json:"type"`
	Function ChatFunction `json:"function"`
}

type ChatFunction struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
	Strict      *bool           `json:"strict,omitempty"`
}

type ToolWarning struct {
	Type   string
	Name   string
	Reason string
}

type ResponseStatus string

const (
	ResponseStatusInProgress ResponseStatus = "in_progress"
	ResponseStatusCompleted  ResponseStatus = "completed"
	ResponseStatusFailed     ResponseStatus = "failed"
	ResponseStatusCancelled  ResponseStatus = "cancelled"
)

type ResponseRecord struct {
	ID                 string
	PreviousResponseID string
	Scope              Scope
	InputText          string
	InputMessages      []ProviderMessage
	OutputText         string
	ReasoningText      string
	ToolCalls          []ToolCallRecord
	Status             ResponseStatus
	Model              string
	Usage              *Usage
}
