package responsesadapter

import "strings"

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
	case strings.Contains(id, "deepseek"):
		profile.DefaultReasoningEffort = "high"
		profile.EnableThinking = true
		profile.StripSamplingWhenThinking = true
	case strings.Contains(id, "mimo"):
		forceParallel := true
		profile.DefaultReasoningEffort = "high"
		profile.EnableThinking = true
		profile.ForceParallelToolCalls = &forceParallel
		profile.StripSamplingWhenThinking = true
		profile.DropNonAutoToolChoice = true
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
	switch requested {
	case "xhigh":
		if strings.Contains(p.ID, "deepseek") {
			return "max"
		}
		return "high"
	case "none", "minimal":
		return "low"
	case "low", "medium", "high":
		return requested
	case "":
		return p.DefaultReasoningEffort
	default:
		return requested
	}
}

func (p ProviderProfile) shouldEnableThinking(model string) bool {
	if !p.EnableThinking {
		return false
	}
	model = strings.ToLower(strings.TrimSpace(model))
	switch {
	case strings.Contains(p.ID, "deepseek"):
		return strings.Contains(model, "deepseek-v4")
	case strings.Contains(p.ID, "mimo"):
		return strings.HasPrefix(model, "mimo-v2.5") || strings.Contains(model, "mimo-v2-pro") || strings.Contains(model, "mimo-v2-omni")
	default:
		return true
	}
}

func (p ProviderProfile) shouldStripSampling(model string) bool {
	if !p.StripSamplingWhenThinking || !p.shouldEnableThinking(model) {
		return false
	}
	model = strings.ToLower(strings.TrimSpace(model))
	if strings.Contains(p.ID, "mimo") {
		return strings.HasPrefix(model, "mimo-v2.5")
	}
	return true
}

func (p ProviderProfile) shouldForwardImages(model string, parts []ProviderContentPart, role string, _ string) bool {
	if role != "user" || !hasProviderImagePart(parts) {
		return false
	}
	model = strings.ToLower(strings.TrimSpace(model))
	switch {
	case strings.Contains(p.ID, "mimo"):
		return model == "mimo-v2.5" || strings.Contains(model, "mimo-v2-omni")
	case strings.Contains(p.ID, "deepseek"):
		return strings.Contains(model, "deepseek-v4-pro")
	default:
		return false
	}
}

func (p ProviderProfile) shouldSendReasoningContent(model string) bool {
	model = strings.ToLower(strings.TrimSpace(model))
	if strings.Contains(p.ID, "deepseek") && strings.Contains(model, "deepseek-reasoner") {
		return false
	}
	return true
}

func hasProviderImagePart(parts []ProviderContentPart) bool {
	for _, part := range parts {
		if part.Type == "image_url" && strings.TrimSpace(part.ImageURL) != "" {
			return true
		}
	}
	return false
}
