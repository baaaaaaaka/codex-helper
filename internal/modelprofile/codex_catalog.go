package modelprofile

import (
	"encoding/json"
	"fmt"
	"strings"
)

type codexCatalog struct {
	Models []codexModelInfo `json:"models"`
}

type codexModelInfo struct {
	Slug                          string                 `json:"slug"`
	DisplayName                   string                 `json:"display_name"`
	Description                   string                 `json:"description"`
	DefaultReasoningLevel         string                 `json:"default_reasoning_level"`
	SupportedReasoningLevels      []codexReasoningPreset `json:"supported_reasoning_levels"`
	ShellType                     string                 `json:"shell_type"`
	Visibility                    string                 `json:"visibility"`
	SupportedInAPI                bool                   `json:"supported_in_api"`
	Priority                      int                    `json:"priority"`
	AdditionalSpeedTiers          []string               `json:"additional_speed_tiers"`
	ServiceTiers                  []codexServiceTier     `json:"service_tiers"`
	AvailabilityNUX               any                    `json:"availability_nux"`
	Upgrade                       any                    `json:"upgrade"`
	BaseInstructions              string                 `json:"base_instructions"`
	SupportsReasoningSummaries    bool                   `json:"supports_reasoning_summaries"`
	DefaultReasoningSummary       string                 `json:"default_reasoning_summary"`
	SupportVerbosity              bool                   `json:"support_verbosity"`
	DefaultVerbosity              any                    `json:"default_verbosity"`
	ApplyPatchToolType            string                 `json:"apply_patch_tool_type"`
	WebSearchToolType             string                 `json:"web_search_tool_type"`
	TruncationPolicy              codexTruncationPolicy  `json:"truncation_policy"`
	SupportsParallelToolCalls     bool                   `json:"supports_parallel_tool_calls"`
	SupportsImageDetailOriginal   bool                   `json:"supports_image_detail_original"`
	ContextWindow                 int                    `json:"context_window"`
	MaxContextWindow              int                    `json:"max_context_window"`
	EffectiveContextWindowPercent int                    `json:"effective_context_window_percent"`
	ExperimentalSupportedTools    []string               `json:"experimental_supported_tools"`
	InputModalities               []string               `json:"input_modalities"`
	SupportsSearchTool            bool                   `json:"supports_search_tool"`
	MultiAgentVersion             *string                `json:"multi_agent_version"`
	ToolMode                      string                 `json:"tool_mode,omitempty"`
}

type codexReasoningPreset struct {
	Effort      string `json:"effort"`
	Description string `json:"description"`
}

type codexServiceTier struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Description string `json:"description"`
}

type codexTruncationPolicy struct {
	Mode  string `json:"mode"`
	Limit int    `json:"limit"`
}

func CodexModelCatalogJSON(provider ProviderSpec) ([]byte, error) {
	models := provider.ModelCatalog()
	if len(models) == 0 {
		return nil, fmt.Errorf("provider %q has no models for Codex catalog", provider.ID)
	}
	catalog := codexCatalog{Models: make([]codexModelInfo, 0, len(models))}
	for _, model := range models {
		publicID := model.PublicID()
		if publicID == "" {
			continue
		}
		contextWindow := model.ContextWindow
		if contextWindow <= 0 {
			contextWindow = 128000
		}
		maxContextWindow := model.MaxContextWindow
		if maxContextWindow <= 0 {
			maxContextWindow = contextWindow
		}
		effectiveContextPercent := model.EffectiveContextPercent
		if effectiveContextPercent <= 0 || effectiveContextPercent > 100 {
			effectiveContextPercent = 90
		}
		parallelTools := model.SupportsTools
		switch strings.ToLower(strings.TrimSpace(model.ToolPolicy.Parallel)) {
		case "disabled":
			parallelTools = false
		case "enabled":
			parallelTools = true
		}
		catalog.Models = append(catalog.Models, codexModelInfo{
			Slug:                          publicID,
			DisplayName:                   model.Label(),
			Description:                   firstNonEmpty(model.Description, provider.DisplayName+" model"),
			DefaultReasoningLevel:         defaultReasoningLevel(provider, model),
			SupportedReasoningLevels:      supportedReasoningLevels(provider, model),
			ShellType:                     "shell_command",
			Visibility:                    "list",
			SupportedInAPI:                true,
			Priority:                      model.Priority,
			AdditionalSpeedTiers:          []string{},
			ServiceTiers:                  []codexServiceTier{},
			AvailabilityNUX:               nil,
			Upgrade:                       nil,
			BaseInstructions:              "You are Codex, a coding agent. Follow the user's instructions and use available tools carefully.",
			SupportsReasoningSummaries:    model.SupportsReason,
			DefaultReasoningSummary:       "none",
			SupportVerbosity:              false,
			DefaultVerbosity:              nil,
			ApplyPatchToolType:            "freeform",
			WebSearchToolType:             "text_and_image",
			TruncationPolicy:              codexTruncationPolicy{Mode: "tokens", Limit: 10000},
			SupportsParallelToolCalls:     parallelTools,
			SupportsImageDetailOriginal:   model.SupportsVision,
			ContextWindow:                 contextWindow,
			MaxContextWindow:              maxContextWindow,
			EffectiveContextWindowPercent: effectiveContextPercent,
			ExperimentalSupportedTools:    []string{},
			InputModalities:               inputModalities(model),
			SupportsSearchTool:            model.SupportsSearch,
			MultiAgentVersion:             codexMultiAgentVersion(provider, model),
			ToolMode:                      codexToolMode(provider, model),
		})
	}
	if len(catalog.Models) == 0 {
		return nil, fmt.Errorf("provider %q has no valid models for Codex catalog", provider.ID)
	}
	return json.MarshalIndent(catalog, "", "  ")
}

// codexMultiAgentVersion advertises the Codex collaboration protocol only for
// models that have a configured web-search fallback. The fallback is
// implemented by a named Codex agent, so the model catalog must expose the
// collaboration version before Codex will register spawn_agent. A provider
// with hosted/native search does not need this capability, and an explicitly
// disabled fallback must remain fail-closed.
func codexMultiAgentVersion(provider ProviderSpec, model ModelSpec) *string {
	if !provider.DisableHostedWebSearch {
		return nil
	}
	if model.SearchPolicy.Fallback.Enabled != nil && !*model.SearchPolicy.Fallback.Enabled {
		return nil
	}
	// The chat adapter cannot preserve the encrypted inter-agent payloads used
	// by multi-agent v2. Advertise the plaintext v1 protocol until a Responses
	// route explicitly proves that it can carry those opaque payloads.
	version := "v1"
	return &version
}

func codexToolMode(provider ProviderSpec, model ModelSpec) string {
	if codexMultiAgentVersion(provider, model) == nil {
		return ""
	}
	return "code_mode_only"
}

func defaultReasoningLevel(provider ProviderSpec, model ModelSpec) string {
	if model.SupportsReason {
		configured := strings.ToLower(strings.TrimSpace(provider.DefaultReasoningEffort))
		for _, effort := range supportedReasoningLevels(provider, model) {
			if effort.Effort == configured {
				return configured
			}
		}
		return "medium"
	}
	return "none"
}

func supportedReasoningLevels(provider ProviderSpec, model ModelSpec) []codexReasoningPreset {
	if !model.SupportsReason {
		return []codexReasoningPreset{}
	}
	efforts := normalizeStringList(provider.SupportedReasoningEfforts)
	if len(efforts) == 0 {
		efforts = []string{"low", "medium", "high", "xhigh"}
	}
	descriptions := map[string]string{
		"none":    "No additional reasoning",
		"minimal": "Minimal reasoning for the fastest response",
		"low":     "Fast responses with lighter reasoning",
		"medium":  "Balances speed and reasoning depth",
		"high":    "Greater reasoning depth for complex tasks",
		"xhigh":   "Extra high reasoning depth for complex tasks",
		"max":     "Maximum reasoning depth for the hardest problems",
		"ultra":   "Maximum reasoning with automatic task delegation",
	}
	levels := make([]codexReasoningPreset, 0, len(efforts))
	for _, effort := range efforts {
		description := descriptions[effort]
		if description == "" {
			description = "Provider-defined reasoning effort"
		}
		levels = append(levels, codexReasoningPreset{Effort: effort, Description: description})
	}
	return levels
}

func inputModalities(model ModelSpec) []string {
	if model.SupportsVision {
		return []string{"text", "image"}
	}
	return []string{"text"}
}
