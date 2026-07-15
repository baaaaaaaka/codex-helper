package config

import "strings"

// QualifiedModelID is the stable selector used by catalog sources. A model
// name is only unique within a provider; keeping the provider in the runtime
// identity prevents two subscriptions from overwriting one another.
func QualifiedModelID(provider, model string) string {
	provider = strings.Trim(strings.TrimSpace(provider), "/")
	model = strings.Trim(strings.TrimSpace(model), "/")
	if provider == "" {
		return model
	}
	if model == "" {
		return provider
	}
	if strings.EqualFold(model, provider) || strings.HasPrefix(strings.ToLower(model), strings.ToLower(provider)+"/") {
		return model
	}
	return provider + "/" + model
}

// SplitQualifiedModelID splits a provider/model selector. A bare model is
// returned with an empty provider and is resolved only when unambiguous.
func SplitQualifiedModelID(ref string) (provider, model string, qualified bool) {
	ref = strings.Trim(strings.TrimSpace(ref), "/")
	if ref == "" {
		return "", "", false
	}
	provider, model, qualified = strings.Cut(ref, "/")
	if !qualified || strings.TrimSpace(provider) == "" || strings.TrimSpace(model) == "" {
		return "", ref, false
	}
	return strings.TrimSpace(provider), strings.Trim(strings.TrimSpace(model), "/"), true
}
