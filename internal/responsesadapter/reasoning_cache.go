package responsesadapter

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
)

type ReasoningCacheStore interface {
	LookupReasoning(scope Scope, message ProviderMessage) (string, error)
}

func applyCachedReasoning(store ResponseStore, scope Scope, messages []ProviderMessage) ([]ProviderMessage, error) {
	cache, ok := store.(ReasoningCacheStore)
	if !ok || len(messages) == 0 {
		return messages, nil
	}
	out := make([]ProviderMessage, len(messages))
	copy(out, messages)
	for i := range out {
		if out[i].Role != "assistant" || strings.TrimSpace(out[i].ReasoningContent) != "" {
			continue
		}
		reasoning, err := cache.LookupReasoning(scope, out[i])
		if err != nil {
			return nil, err
		}
		if strings.TrimSpace(reasoning) != "" {
			out[i].ReasoningContent = reasoning
		}
	}
	return out, nil
}

func reasoningKeysForRecord(scope Scope, record ResponseRecord) []string {
	return reasoningCacheKeys(scope, ProviderMessage{
		Role:      "assistant",
		Content:   record.OutputText,
		ToolCalls: record.ToolCalls,
	})
}

func reasoningCacheKeys(scope Scope, message ProviderMessage) []string {
	scope = scope.withDefaults()
	var keys []string
	for _, call := range message.ToolCalls {
		if strings.TrimSpace(call.ID) != "" {
			keys = append(keys, scope.key()+"\x00call\x00"+strings.TrimSpace(call.ID))
		}
	}
	if strings.TrimSpace(message.Content) != "" {
		keys = append(keys, scope.key()+"\x00content\x00"+shortHash(message.Content))
	}
	return keys
}

func shortHash(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])[:24]
}
