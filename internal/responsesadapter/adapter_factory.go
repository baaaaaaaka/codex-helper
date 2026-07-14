package responsesadapter

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

// AdapterOptions is the safe boundary between a catalog route and a compiled
// wire converter. ConversionProfile is a registry key, never a URL, template
// or script supplied by the catalog.
type AdapterOptions struct {
	AdapterID          string
	ConversionProfile  string
	StrictConversion   bool
	BaseURL            string
	APIKey             string
	Headers            map[string]string
	Endpoints          map[string]string
	AuthType           string
	AuthHeader         string
	Profile            ProviderProfile
	RetryStatuses      []int
	MaxRetries         *int
	HonorRetryAfter    *bool
	RetryTransport     *bool
	MaxOutputTokens    int
	StreamMode         string
	ReasoningDeltaPath string
	CachedTokensPath   string
	UsageField         string
	HTTP               config.ModelHTTPPolicy
	Stream             config.ModelStreamPolicy
	ProxyURL           string
	Status             func(string)
}

// NewConfiguredAdapter constructs the converter registered for a catalog
// interface. OpenAI-compatible routes retain the existing adapter; the two
// DeepSeek profiles use dedicated typed converters.
func NewConfiguredAdapter(opts AdapterOptions) (ProviderAdapter, error) {
	adapterID := strings.ToLower(strings.TrimSpace(opts.AdapterID))
	profile := strings.ToLower(strings.TrimSpace(opts.ConversionProfile))
	if profile == "" {
		switch adapterID {
		case "deepseek-anthropic":
			profile = "deepseek-anthropic-v1"
		case "deepseek-beta":
			profile = "deepseek-beta-v1"
		}
	}
	if err := validateAdapterProfilePair(adapterID, profile); err != nil {
		return nil, err
	}
	maxRetries := 0
	maxRetriesSet := false
	if opts.MaxRetries != nil {
		maxRetries = *opts.MaxRetries
		maxRetriesSet = true
	}
	streamMode := strings.TrimSpace(opts.StreamMode)
	if streamMode == "" {
		streamMode = strings.TrimSpace(opts.Stream.UpstreamMode)
	}
	client, err := adapterHTTPClient(opts.ProxyURL, opts.HTTP, opts.Stream)
	if err != nil {
		return nil, err
	}
	switch profile {
	case "", "openai-chat-v1":
		adapter := OpenAIChatAdapter{
			BaseURL: opts.BaseURL, APIKey: opts.APIKey, Profile: opts.Profile,
			RetryStatuses: append([]int(nil), opts.RetryStatuses...), MaxRetries: maxRetries, MaxRetriesSet: maxRetriesSet,
			HonorRetryAfter: opts.HonorRetryAfter, RetryTransportErrors: opts.RetryTransport,
			MaxOutputTokens: opts.MaxOutputTokens, Headers: cloneAdapterStringMap(opts.Headers), AuthType: opts.AuthType, AuthHeader: opts.AuthHeader,
			StreamMode: streamMode, ReasoningDeltaPath: opts.ReasoningDeltaPath, CachedTokensPath: opts.CachedTokensPath, UsageField: opts.UsageField,
			ResponseHeaderTimeout: time.Duration(opts.HTTP.ResponseHeaderTimeoutSeconds) * time.Second,
			StreamIdleTimeout:     time.Duration(opts.Stream.IdleTimeoutSeconds) * time.Second,
			HTTPClient:            client, Status: opts.Status,
		}
		if opts.HTTP.MaxConcurrentRequests > 0 {
			adapter.RequestGate = make(chan struct{}, opts.HTTP.MaxConcurrentRequests)
		}
		return adapter, nil
	case "deepseek-anthropic-v1":
		return AnthropicAdapter{
			BaseURL: opts.BaseURL, APIKey: opts.APIKey, Headers: cloneAdapterStringMap(opts.Headers), Endpoints: cloneAdapterStringMap(opts.Endpoints), AuthType: opts.AuthType, AuthHeader: opts.AuthHeader,
			HTTPClient: client, MaxRetries: maxRetries, MaxRetriesSet: maxRetriesSet, RetryStatuses: append([]int(nil), opts.RetryStatuses...), HonorRetryAfter: opts.HonorRetryAfter, RetryTransportErrors: opts.RetryTransport,
			ResponseHeaderTimeout: time.Duration(opts.HTTP.ResponseHeaderTimeoutSeconds) * time.Second, StreamIdleTimeout: time.Duration(opts.Stream.IdleTimeoutSeconds) * time.Second,
			MaxOutputTokens: opts.MaxOutputTokens, StreamMode: streamMode, Strict: opts.StrictConversion, Status: opts.Status,
			RequestGate: requestGate(opts.HTTP.MaxConcurrentRequests),
		}, nil
	case "deepseek-beta-v1":
		chat, chatErr := NewConfiguredAdapter(AdapterOptions{
			AdapterID: "openai-chat", BaseURL: opts.BaseURL, APIKey: opts.APIKey, Headers: opts.Headers, Endpoints: opts.Endpoints, AuthType: opts.AuthType, AuthHeader: opts.AuthHeader,
			Profile: opts.Profile, RetryStatuses: opts.RetryStatuses, MaxRetries: opts.MaxRetries, HonorRetryAfter: opts.HonorRetryAfter, RetryTransport: opts.RetryTransport,
			MaxOutputTokens: opts.MaxOutputTokens, StreamMode: streamMode, ReasoningDeltaPath: opts.ReasoningDeltaPath, CachedTokensPath: opts.CachedTokensPath, UsageField: opts.UsageField,
			HTTP: opts.HTTP, Stream: opts.Stream, ProxyURL: opts.ProxyURL, Status: opts.Status,
		})
		if chatErr != nil {
			return nil, chatErr
		}
		chatAdapter, typeOK := chat.(OpenAIChatAdapter)
		if !typeOK {
			return nil, fmt.Errorf("deepseek beta chat adapter has unexpected type %T", chat)
		}
		return DeepSeekBetaAdapter{Chat: chatAdapter, BaseURL: opts.BaseURL, APIKey: opts.APIKey, Headers: cloneAdapterStringMap(opts.Headers), Endpoints: cloneAdapterStringMap(opts.Endpoints), AuthType: opts.AuthType, AuthHeader: opts.AuthHeader, HTTPClient: client, StreamMode: streamMode, MaxOutputTokens: opts.MaxOutputTokens, ResponseHeaderTimeout: time.Duration(opts.HTTP.ResponseHeaderTimeoutSeconds) * time.Second, StreamIdleTimeout: time.Duration(opts.Stream.IdleTimeoutSeconds) * time.Second, Status: opts.Status, RequestGate: requestGate(opts.HTTP.MaxConcurrentRequests)}, nil
	default:
		return nil, fmt.Errorf("unsupported wire conversion profile %q for adapter %q", opts.ConversionProfile, opts.AdapterID)
	}
}

func validateAdapterProfilePair(adapterID, profile string) error {
	switch profile {
	case "deepseek-anthropic-v1":
		if adapterID != "" && adapterID != "deepseek-anthropic" {
			return fmt.Errorf("wire conversion profile %q is incompatible with adapter %q", profile, adapterID)
		}
	case "deepseek-beta-v1":
		if adapterID != "" && adapterID != "deepseek-beta" {
			return fmt.Errorf("wire conversion profile %q is incompatible with adapter %q", profile, adapterID)
		}
	}
	return nil
}

// ValidateConversionProfile is used even when a caller supplies a custom
// adapter instance, so a catalog conversion selector can never be accepted
// and silently ignored.
func ValidateConversionProfile(profile string) error {
	switch strings.ToLower(strings.TrimSpace(profile)) {
	case "", "openai-chat-v1", "deepseek-anthropic-v1", "deepseek-beta-v1":
		return nil
	default:
		return fmt.Errorf("unsupported wire conversion profile %q", profile)
	}
}

func adapterHTTPClient(proxyURL string, policy config.ModelHTTPPolicy, stream config.ModelStreamPolicy) (*http.Client, error) {
	proxy := http.ProxyFromEnvironment
	if strings.TrimSpace(proxyURL) != "" {
		parsed, err := url.Parse(proxyURL)
		if err != nil {
			return nil, fmt.Errorf("parse upstream proxy url: %w", err)
		}
		proxy = http.ProxyURL(parsed)
	}
	client := NewUpstreamHTTPClientWithResponseHeaderTimeout(proxy, time.Duration(policy.ResponseHeaderTimeoutSeconds)*time.Second)
	if policy.TimeoutSeconds > 0 {
		client.Timeout = time.Duration(policy.TimeoutSeconds) * time.Second
	}
	_ = stream
	return client, nil
}

func cloneAdapterStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func requestGate(maxConcurrent int) chan struct{} {
	if maxConcurrent <= 0 {
		return nil
	}
	return make(chan struct{}, maxConcurrent)
}
