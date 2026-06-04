package responsesadapter

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func TestLiveThirdPartyPromptCacheResponsesFlow(t *testing.T) {
	if !liveEnvEnabled("CODEX_HELPER_LIVE_THIRDPARTY_CACHE") {
		t.Skip("set CODEX_HELPER_LIVE_THIRDPARTY_CACHE=1 to run live third-party prompt-cache test")
	}

	cases := []struct {
		provider string
		model    string
		baseURL  string
		keyEnv   string
	}{
		{provider: "deepseek", model: "deepseek-v4-flash", baseURL: "https://api.deepseek.com/v1", keyEnv: "DEEPSEEK_API_KEY"},
		{provider: "mimo", model: "mimo-v2.5", baseURL: "https://api.xiaomimimo.com/v1", keyEnv: "MIMO_API_KEY"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.provider, func(t *testing.T) {
			apiKey := strings.TrimSpace(os.Getenv(tc.keyEnv))
			if apiKey == "" {
				t.Skipf("%s is not set", tc.keyEnv)
			}

			facade := livePromptCacheFacade(tc.provider, tc.model, tc.baseURL, apiKey)
			cacheKey := "live-thirdparty-cache-" + tc.provider
			anchor := livePromptCacheAnchor(tc.provider)

			var previousID string
			var sawCacheHit bool
			for round := 1; round <= 3; round++ {
				body := map[string]any{
					"model":             tc.model,
					"max_output_tokens": 96,
					"prompt_cache_key":  cacheKey,
					"input":             livePromptCacheInput(anchor, tc.provider, round),
				}
				if previousID != "" {
					body["previous_response_id"] = previousID
				}
				response := postLiveResponses(t, facade, body)
				previousID = response.ID
				if response.Usage == nil || response.Usage.InputTokens <= 0 {
					t.Fatalf("%s round %d usage = %#v, want populated input tokens", tc.provider, round, response.Usage)
				}
				hitRate := 0.0
				if response.Usage.InputTokens > 0 {
					hitRate = 100 * float64(response.Usage.CachedTokens) / float64(response.Usage.InputTokens)
				}
				t.Logf("LIVE_THIRDPARTY_PROMPT_CACHE provider=%s round=%02d response=%s input=%d cached=%d hit_rate=%.1f%% output=%q",
					tc.provider, round, response.ID, response.Usage.InputTokens, response.Usage.CachedTokens, hitRate, trimForLiveCacheLog(response.OutputText))
				if round > 1 && response.Usage.CachedTokens > 0 {
					sawCacheHit = true
				}
			}
			if !sawCacheHit {
				t.Fatalf("%s did not report cached prompt tokens on follow-up rounds", tc.provider)
			}
		})
	}
}

func livePromptCacheAnchor(provider string) string {
	var b strings.Builder
	for i := 0; i < 220; i++ {
		_, _ = fmt.Fprintf(&b, "cache-anchor-%s-%03d: keep this exact prefix stable for prompt cache measurement.\n", provider, i)
	}
	return b.String()
}

func livePromptCacheFacade(provider string, model string, baseURL string, apiKey string) *Facade {
	nextID := 0
	return &Facade{
		Adapter: OpenAIChatAdapter{
			BaseURL:    baseURL,
			APIKey:     apiKey,
			Profile:    ProfileForProvider(provider),
			HTTPClient: &http.Client{Timeout: 90 * time.Second},
		},
		Store:        NewMemoryStore(),
		ProviderID:   provider,
		DefaultModel: model,
		NewID: func(prefix string) (string, error) {
			nextID++
			return fmt.Sprintf("%s_live_%s_%02d", prefix, provider, nextID), nil
		},
	}
}

func livePromptCacheInput(anchor string, provider string, round int) string {
	if round == 1 {
		return "This is a prompt-cache probe for " + provider + ". Read the stable anchor below and reply with exactly CACHE-ROUND-01.\n\n" + anchor
	}
	return fmt.Sprintf("Continue the same prompt-cache probe for %s. Reply with exactly CACHE-ROUND-%02d.", provider, round)
}

func trimForLiveCacheLog(text string) string {
	text = strings.TrimSpace(strings.ReplaceAll(text, "\n", " "))
	if len(text) > 120 {
		return text[:120] + "..."
	}
	return text
}
