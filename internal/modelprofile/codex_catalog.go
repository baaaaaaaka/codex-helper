package modelprofile

import (
	"encoding/json"
	"fmt"
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
		catalog.Models = append(catalog.Models, codexModelInfo{
			Slug:                          publicID,
			DisplayName:                   model.Label(),
			Description:                   firstNonEmpty(model.Description, provider.DisplayName+" model"),
			DefaultReasoningLevel:         defaultReasoningLevel(model),
			SupportedReasoningLevels:      supportedReasoningLevels(model),
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
			SupportsParallelToolCalls:     true,
			SupportsImageDetailOriginal:   model.SupportsVision,
			ContextWindow:                 contextWindow,
			MaxContextWindow:              maxContextWindow,
			EffectiveContextWindowPercent: 90,
			ExperimentalSupportedTools:    []string{},
			InputModalities:               inputModalities(model),
			SupportsSearchTool:            model.SupportsSearch,
		})
	}
	if len(catalog.Models) == 0 {
		return nil, fmt.Errorf("provider %q has no valid models for Codex catalog", provider.ID)
	}
	return json.MarshalIndent(catalog, "", "  ")
}

func defaultReasoningLevel(model ModelSpec) string {
	if model.SupportsReason {
		return "medium"
	}
	return "none"
}

func supportedReasoningLevels(model ModelSpec) []codexReasoningPreset {
	if !model.SupportsReason {
		return []codexReasoningPreset{}
	}
	return []codexReasoningPreset{
		{Effort: "low", Description: "Fast responses with lighter reasoning"},
		{Effort: "medium", Description: "Balances speed and reasoning depth"},
		{Effort: "high", Description: "Greater reasoning depth for complex tasks"},
	}
}

func inputModalities(model ModelSpec) []string {
	if model.SupportsVision {
		return []string{"text", "image"}
	}
	return []string{"text"}
}
