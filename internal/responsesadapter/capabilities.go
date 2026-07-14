package responsesadapter

import (
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

type ProviderProfile struct {
	ID                                     string
	IncludeUsageStreamOptions              bool
	MergeSystemMessages                    bool
	OmitEmptyAssistantContentWithToolCalls bool
	DefaultReasoningEffort                 string
	EnableThinking                         bool
	ForceParallelToolCalls                 *bool
	StripSamplingWhenThinking              bool
	DropNonAutoToolChoice                  bool
	ReasoningEffortMap                     map[string]string
	ThinkingMode                           string
	ReasoningContentPolicy                 string
	ImagePolicy                            string
	AudioPolicy                            string
	VideoPolicy                            string
	HistoryPolicy                          string
	TemperaturePolicy                      string
	TopPPolicy                             string
	ValidateToolArguments                  bool
	ThinkingEnabledRequest                 map[string]any
	ThinkingDisabledRequest                map[string]any
	ReasoningResponseField                 string
}

func ProfileForProvider(provider string) ProviderProfile {
	id := strings.ToLower(strings.TrimSpace(provider))
	if id == "" {
		id = "generic"
	}
	profile := ProviderProfile{
		ID:                                     id,
		IncludeUsageStreamOptions:              true,
		MergeSystemMessages:                    true,
		OmitEmptyAssistantContentWithToolCalls: true,
	}
	switch {
	case strings.Contains(id, "glm"):
		profile.DefaultReasoningEffort = "high"
		profile.ReasoningEffortMap = map[string]string{"xhigh": "max"}
	}
	return profile
}

func (p ProviderProfile) withDefaults() ProviderProfile {
	if p.ID == "" {
		return ProfileForProvider("generic")
	}
	return p
}

func (p ProviderProfile) reasoningEffort(requested string) string {
	requested = strings.ToLower(strings.TrimSpace(requested))
	if mapped := strings.ToLower(strings.TrimSpace(p.ReasoningEffortMap[requested])); mapped != "" {
		return mapped
	}
	switch requested {
	case "xhigh":
		return "high"
	case "none", "minimal", "low", "medium", "high":
		return requested
	case "":
		return p.DefaultReasoningEffort
	default:
		return requested
	}
}

// WithReasoningOverrides applies model-profile configuration after the generic
// adapter defaults. Provider-specific behavior belongs in the external model
// catalog rather than in this binary.
func (p ProviderProfile) WithReasoningOverrides(defaultEffort string, effortMap map[string]string) ProviderProfile {
	if value := strings.ToLower(strings.TrimSpace(defaultEffort)); value != "" {
		p.DefaultReasoningEffort = value
	}
	if len(effortMap) > 0 {
		p.ReasoningEffortMap = make(map[string]string, len(effortMap))
		for key, value := range effortMap {
			key = strings.ToLower(strings.TrimSpace(key))
			value = strings.ToLower(strings.TrimSpace(value))
			if key != "" && value != "" {
				p.ReasoningEffortMap[key] = value
			}
		}
	}
	return p
}

func (p ProviderProfile) WithModelPolicies(reasoning config.ModelReasoningPolicy, tools config.ModelToolPolicy, messages config.ModelMessagePolicy, sampling config.ModelSamplingPolicy) ProviderProfile {
	p.ThinkingEnabledRequest = cloneAnyMap(reasoning.EnabledRequest)
	p.ThinkingDisabledRequest = cloneAnyMap(reasoning.DisabledRequest)
	p.ReasoningResponseField = strings.TrimSpace(reasoning.ResponseField)
	if value := strings.ToLower(strings.TrimSpace(reasoning.ThinkingMode)); value != "" {
		p.ThinkingMode = value
		p.EnableThinking = value != "disabled" && value != "provider-default"
	}
	if value := strings.ToLower(strings.TrimSpace(reasoning.HistoryPolicy)); value != "" {
		p.HistoryPolicy = value
		if value == "never" || value == "omit" || value == "drop" || value == "text-only" {
			p.ReasoningContentPolicy = "drop"
		} else if value == "always" || value == "tool-calls-only" || value == "preserve" || value == "keep" {
			p.ReasoningContentPolicy = "preserve"
		}
	}
	if value := strings.ToLower(strings.TrimSpace(messages.Images)); value != "" {
		p.ImagePolicy = value
	}
	if value := strings.ToLower(strings.TrimSpace(messages.Audio)); value != "" {
		p.AudioPolicy = value
	}
	if value := strings.ToLower(strings.TrimSpace(messages.Video)); value != "" {
		p.VideoPolicy = value
	}
	if reasoning.StripSamplingWhenEnabled != nil {
		p.StripSamplingWhenThinking = *reasoning.StripSamplingWhenEnabled
	}
	if messages.MergeSystemMessages != nil {
		p.MergeSystemMessages = *messages.MergeSystemMessages
	}
	switch strings.ToLower(strings.TrimSpace(tools.EmptyAssistantContent)) {
	case "omit":
		p.OmitEmptyAssistantContentWithToolCalls = true
	case "empty-string", "preserve":
		p.OmitEmptyAssistantContentWithToolCalls = false
	}
	switch strings.ToLower(strings.TrimSpace(tools.ToolChoice)) {
	case "auto-only":
		p.DropNonAutoToolChoice = true
	case "full":
		p.DropNonAutoToolChoice = false
	}
	switch strings.ToLower(strings.TrimSpace(tools.Parallel)) {
	case "enabled":
		value := true
		p.ForceParallelToolCalls = &value
	case "disabled":
		value := false
		p.ForceParallelToolCalls = &value
	}
	if tools.ValidateArguments != nil {
		p.ValidateToolArguments = *tools.ValidateArguments
	}
	p.TemperaturePolicy = strings.ToLower(strings.TrimSpace(sampling.Temperature))
	p.TopPPolicy = strings.ToLower(strings.TrimSpace(sampling.TopP))
	return p.WithReasoningOverrides(reasoning.DefaultEffort, reasoning.EffortMap)
}

func cloneAnyMap(src map[string]any) map[string]any {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]any, len(src))
	for key, value := range src {
		out[key] = value
	}
	return out
}

func (p ProviderProfile) thinkingType(model, effort string) string {
	switch strings.ToLower(strings.TrimSpace(p.ThinkingMode)) {
	case "disabled":
		return "disabled"
	case "always":
		return "enabled"
	case "effort-dependent":
		switch strings.ToLower(strings.TrimSpace(effort)) {
		case "none", "minimal":
			return "disabled"
		default:
			return "enabled"
		}
	case "provider-default":
		return ""
	}
	if p.shouldEnableThinking(model) {
		return "enabled"
	}
	return ""
}

func (p ProviderProfile) shouldEnableThinking(model string) bool {
	if !p.EnableThinking {
		return false
	}
	model = strings.ToLower(strings.TrimSpace(model))
	_ = model
	return true
}

func (p ProviderProfile) shouldStripSampling(model string) bool {
	if !p.StripSamplingWhenThinking || !p.shouldEnableThinking(model) {
		return false
	}
	return true
}

func (p ProviderProfile) shouldForwardImages(model string, parts []ProviderContentPart, role string, _ string) bool {
	if role != "user" || !hasProviderMediaPart(parts) {
		return false
	}
	_ = model
	for _, part := range parts {
		var policy string
		switch part.Type {
		case "image_url":
			policy = p.ImagePolicy
		case "audio":
			policy = p.AudioPolicy
		case "video":
			policy = p.VideoPolicy
		default:
			continue
		}
		switch strings.ToLower(strings.TrimSpace(policy)) {
		case "allow", "enabled", "forward", "multimodal":
			continue
		default:
			return false
		}
	}
	return true
}

func (p ProviderProfile) shouldSendReasoningContent(model string) bool {
	_ = model
	switch strings.ToLower(strings.TrimSpace(p.ReasoningContentPolicy)) {
	case "drop", "omit", "text-only":
		return false
	default:
		return true
	}
}

func hasProviderImagePart(parts []ProviderContentPart) bool {
	for _, part := range parts {
		if part.Type == "image_url" && strings.TrimSpace(part.ImageURL) != "" {
			return true
		}
	}
	return false
}

func hasProviderMediaPart(parts []ProviderContentPart) bool {
	for _, part := range parts {
		if (part.Type == "image_url" && strings.TrimSpace(part.ImageURL) != "") ||
			(part.Type == "audio" && (strings.TrimSpace(part.AudioURL) != "" || strings.TrimSpace(part.AudioData) != "")) ||
			(part.Type == "video" && strings.TrimSpace(part.VideoURL) != "") {
			return true
		}
	}
	return false
}
