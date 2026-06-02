package responsesadapter

import (
	"encoding/json"
	"testing"
)

func TestUsageMarshalIncludesZeroCachedTokens(t *testing.T) {
	raw, err := json.Marshal(Usage{InputTokens: 3, OutputTokens: 2, TotalTokens: 5})
	if err != nil {
		t.Fatalf("Marshal usage: %v", err)
	}
	var usage map[string]any
	if err := json.Unmarshal(raw, &usage); err != nil {
		t.Fatalf("Unmarshal usage: %v\n%s", err, raw)
	}
	inputDetails, ok := usage["input_tokens_details"].(map[string]any)
	if !ok {
		t.Fatalf("usage missing input_tokens_details: %s", raw)
	}
	if inputDetails["cached_tokens"] != float64(0) {
		t.Fatalf("cached_tokens = %#v, want 0 in %s", inputDetails["cached_tokens"], raw)
	}
	if _, ok := usage["cached_tokens"]; ok {
		t.Fatalf("usage unexpectedly emitted legacy top-level cached_tokens: %s", raw)
	}
}
