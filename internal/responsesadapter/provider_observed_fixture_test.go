package responsesadapter

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOpenAIChatAdapterParsesObservedProviderSSEFixtures(t *testing.T) {
	tests := []struct {
		name             string
		profile          string
		model            string
		sse              string
		wantText         string
		wantReasoning    string
		wantToolName     string
		wantToolArgs     string
		wantCachedTokens int
		wantReasonTokens int
	}{
		{
			name:          "deepseek v4 flash text with reasoning before content",
			profile:       "deepseek",
			model:         "deepseek-v4-flash",
			wantText:      "cxp-live-ok",
			wantReasoning: "reply exactly",
			sse: strings.Join([]string{
				`data: {"choices":[{"delta":{"role":"assistant","content":null,"reasoning_content":""}}],"usage":null}`,
				"",
				`data: {"choices":[{"delta":{"content":null,"reasoning_content":"Need to reply exactly."}}],"usage":null}`,
				"",
				`data: {"choices":[{"delta":{"content":"cxp"}}],"usage":null}`,
				"",
				`data: {"choices":[{"delta":{"content":"-live-ok"},"finish_reason":"stop"}],"usage":null}`,
				"",
				`data: {"choices":[],"usage":{"prompt_tokens":11,"completion_tokens":8,"total_tokens":19,"prompt_tokens_details":{"cached_tokens":6},"completion_tokens_details":{"reasoning_tokens":5}}}`,
				"",
				`data: [DONE]`,
				"",
			}, "\n"),
			wantCachedTokens: 6,
			wantReasonTokens: 5,
		},
		{
			name:          "deepseek v4 flash streams split tool arguments",
			profile:       "deepseek",
			model:         "deepseek-v4-flash",
			wantReasoning: "tool call",
			wantToolName:  "get_weather",
			wantToolArgs:  `{"city": "Paris"}`,
			sse: strings.Join([]string{
				`data: {"choices":[{"delta":{"role":"assistant","content":null,"reasoning_content":""}}],"usage":null}`,
				"",
				`data: {"choices":[{"delta":{"content":null,"reasoning_content":"Need a tool call."}}],"usage":null}`,
				"",
				`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_ds","type":"function","function":{"name":"get_weather","arguments":""}}]}}],"usage":null}`,
				"",
				`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{"}}]}}],"usage":null}`,
				"",
				`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"city\""}}]}}],"usage":null}`,
				"",
				`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":": "}}]}}],"usage":null}`,
				"",
				`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"Paris\""}}]}}],"usage":null}`,
				"",
				`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"}"}}]},"finish_reason":"tool_calls"}],"usage":null}`,
				"",
				`data: {"choices":[],"usage":{"prompt_tokens":20,"completion_tokens":12,"total_tokens":32,"prompt_tokens_details":{"cached_tokens":10},"completion_tokens_details":{"reasoning_tokens":7}}}`,
				"",
				`data: [DONE]`,
				"",
			}, "\n"),
			wantCachedTokens: 10,
			wantReasonTokens: 7,
		},
		{
			name:          "mimo v2.5 multimodal returns long reasoning then final text",
			profile:       "mimo",
			model:         "mimo-v2.5",
			wantText:      "Red",
			wantReasoning: "dominant color",
			sse: strings.Join([]string{
				`data: {"choices":[{"delta":{"content":"","role":"assistant","tool_calls":null,"reasoning_content":null},"finish_reason":null,"index":0}],"created":1780203233,"model":"mimo-v2.5","object":"chat.completion.chunk"}`,
				"",
				`data: {"choices":[{"delta":{"content":null,"role":null,"tool_calls":null,"reasoning_content":"The dominant color is red."},"finish_reason":null,"index":0}],"created":1780203233,"model":"mimo-v2.5","object":"chat.completion.chunk"}`,
				"",
				`data: {"choices":[{"delta":{"content":"Red","role":null,"tool_calls":null,"reasoning_content":null},"finish_reason":null,"index":0}],"created":1780203234,"model":"mimo-v2.5","object":"chat.completion.chunk"}`,
				"",
				`data: {"choices":[{"delta":{"content":null,"role":null,"tool_calls":null,"reasoning_content":null},"finish_reason":"stop","index":0}],"created":1780203234,"model":"mimo-v2.5","object":"chat.completion.chunk","usage":null}`,
				"",
				`data: {"choices":[],"created":1780203234,"model":"mimo-v2.5","object":"chat.completion.chunk","usage":{"completion_tokens":39,"prompt_tokens":272,"total_tokens":311,"completion_tokens_details":{"reasoning_tokens":35},"prompt_tokens_details":{"cached_tokens":256,"image_tokens":8}}}`,
				"",
				`data: [DONE]`,
				"",
			}, "\n"),
			wantCachedTokens: 256,
			wantReasonTokens: 35,
		},
		{
			name:          "mimo v2.5 emits complete tool call in one chunk",
			profile:       "mimo",
			model:         "mimo-v2.5",
			wantReasoning: "city parameter",
			wantToolName:  "get_weather",
			wantToolArgs:  `{"city": "Paris"}`,
			sse: strings.Join([]string{
				`data: {"choices":[{"delta":{"content":"","role":"assistant","tool_calls":null,"reasoning_content":null},"finish_reason":null,"index":0}],"model":"mimo-v2.5","object":"chat.completion.chunk"}`,
				"",
				`data: {"choices":[{"delta":{"content":null,"role":null,"tool_calls":null,"reasoning_content":"Use the city parameter."},"finish_reason":null,"index":0}],"model":"mimo-v2.5","object":"chat.completion.chunk"}`,
				"",
				`data: {"choices":[{"delta":{"content":null,"role":null,"tool_calls":[{"index":0,"id":"call_mimo","function":{"arguments":"{\"city\": \"Paris\"}","name":"get_weather"},"type":"function"}],"reasoning_content":null},"finish_reason":null,"index":0}],"model":"mimo-v2.5","object":"chat.completion.chunk"}`,
				"",
				`data: {"choices":[{"delta":{"content":null,"role":null,"tool_calls":null,"reasoning_content":null},"finish_reason":"tool_calls","index":0}],"model":"mimo-v2.5","object":"chat.completion.chunk","usage":null}`,
				"",
				`data: {"choices":[],"model":"mimo-v2.5","object":"chat.completion.chunk","usage":{"completion_tokens":56,"prompt_tokens":496,"total_tokens":552,"completion_tokens_details":{"reasoning_tokens":33},"prompt_tokens_details":{"cached_tokens":192}}}`,
				"",
				`data: [DONE]`,
				"",
			}, "\n"),
			wantCachedTokens: 192,
			wantReasonTokens: 33,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte(tc.sse))
			}))
			defer server.Close()

			adapter := OpenAIChatAdapter{
				BaseURL:    server.URL + "/v1",
				Profile:    ProfileForProvider(tc.profile),
				HTTPClient: server.Client(),
				MaxRetries: -1,
			}
			stream, err := adapter.Stream(context.Background(), ProviderRequest{Model: tc.model, InputText: "fixture"})
			if err != nil {
				t.Fatalf("Stream: %v", err)
			}
			events := collectEvents(stream)
			if got := eventText(events); got != tc.wantText {
				t.Fatalf("text = %q want %q events=%#v", got, tc.wantText, events)
			}
			if got := eventReasoning(events); !strings.Contains(got, tc.wantReasoning) {
				t.Fatalf("reasoning = %q want containing %q events=%#v", got, tc.wantReasoning, events)
			}
			if tc.wantToolName != "" {
				name, args := eventToolCall(events)
				if name != tc.wantToolName || args != tc.wantToolArgs {
					t.Fatalf("tool name=%q args=%q want %q %q events=%#v", name, args, tc.wantToolName, tc.wantToolArgs, events)
				}
			}
			usage := lastUsage(events)
			if usage == nil || usage.CachedTokens != tc.wantCachedTokens || usage.ReasoningTokens != tc.wantReasonTokens {
				t.Fatalf("usage = %#v want cached=%d reasoning=%d events=%#v", usage, tc.wantCachedTokens, tc.wantReasonTokens, events)
			}
			if len(events) == 0 || events[len(events)-1].Kind != ProviderEventDone {
				t.Fatalf("events = %#v", events)
			}
		})
	}
}

func TestFacadeCompletesObservedProviderToolCallFixtures(t *testing.T) {
	tests := []struct {
		name        string
		profile     string
		model       string
		fixture     string
		wantToolArg string
	}{
		{
			name:    "deepseek split arguments",
			profile: "deepseek",
			model:   "deepseek-v4-flash",
			fixture: strings.Join([]string{
				`data: {"choices":[{"delta":{"content":null,"reasoning_content":"Need a tool."}}]}`,
				"",
				`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_ds","type":"function","function":{"name":"get_weather","arguments":""}}]}}]}`,
				"",
				`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\""}}]}}]}`,
				"",
				`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":": \"Paris\"}"}}]},"finish_reason":"tool_calls"}]}`,
				"",
				`data: [DONE]`,
				"",
			}, "\n"),
			wantToolArg: `{"city": "Paris"}`,
		},
		{
			name:    "mimo complete arguments",
			profile: "mimo",
			model:   "mimo-v2.5",
			fixture: strings.Join([]string{
				`data: {"choices":[{"delta":{"content":null,"reasoning_content":"Use the tool."}}]}`,
				"",
				`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_mimo","function":{"arguments":"{\"city\": \"Paris\"}","name":"get_weather"},"type":"function"}]},"finish_reason":"tool_calls"}]}`,
				"",
				`data: [DONE]`,
				"",
			}, "\n"),
			wantToolArg: `{"city": "Paris"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte(tc.fixture))
			}))
			defer server.Close()
			facade := &Facade{
				Adapter:      OpenAIChatAdapter{BaseURL: server.URL + "/v1", Profile: ProfileForProvider(tc.profile), HTTPClient: server.Client(), MaxRetries: -1},
				Store:        NewMemoryStore(),
				ProviderID:   tc.profile,
				DefaultModel: tc.model,
				NewID:        func(prefix string) (string, error) { return prefix + "_fixture", nil },
			}
			raw, err := json.Marshal(map[string]any{
				"model":       tc.model,
				"input":       "call get_weather for Paris",
				"tool_choice": "auto",
				"tools": []any{
					map[string]any{
						"type":        "function",
						"name":        "get_weather",
						"description": "Get weather by city",
						"parameters":  map[string]any{"type": "object", "properties": map[string]any{"city": map[string]any{"type": "string"}}},
					},
				},
			})
			if err != nil {
				t.Fatalf("marshal request: %v", err)
			}
			req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(string(raw)))
			rec := httptest.NewRecorder()
			facade.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
			}
			var response responseObject
			if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
				t.Fatalf("decode response: %v", err)
			}
			item := firstOutputItemOfType(response.Output, "function_call")
			if item == nil {
				t.Fatalf("missing function_call output: %#v", response.Output)
			}
			if item.Name != "get_weather" || item.Arguments != tc.wantToolArg {
				t.Fatalf("tool item = %#v want args %q", item, tc.wantToolArg)
			}
			if firstOutputItemOfType(response.Output, "reasoning") == nil {
				t.Fatalf("missing reasoning item: %#v", response.Output)
			}
		})
	}
}

func TestMockedLiveProviderResponsesFlowsCI(t *testing.T) {
	tests := []struct {
		name              string
		profile           string
		model             string
		body              map[string]any
		fixture           string
		wantText          string
		wantTool          bool
		wantRequestSubstr []string
	}{
		{
			name:    "deepseek text",
			profile: "deepseek",
			model:   "deepseek-v4-flash",
			body: map[string]any{
				"model":             "deepseek-v4-flash",
				"max_output_tokens": 192,
				"input":             "Reply with exactly: cxp-live-ok",
			},
			fixture: strings.Join([]string{
				`data: {"choices":[{"delta":{"role":"assistant","content":null,"reasoning_content":"reply exactly"}}]}`,
				"",
				`data: {"choices":[{"delta":{"content":"cxp-live-ok"},"finish_reason":"stop"}]}`,
				"",
				`data: {"choices":[],"usage":{"prompt_tokens":11,"completion_tokens":8,"total_tokens":19,"prompt_tokens_details":{"cached_tokens":6},"completion_tokens_details":{"reasoning_tokens":5}}}`,
				"",
				`data: [DONE]`,
				"",
			}, "\n"),
			wantText:          "cxp-live-ok",
			wantRequestSubstr: []string{"deepseek-v4-flash", "Reply with exactly: cxp-live-ok", "stream_options"},
		},
		{
			name:    "deepseek tool",
			profile: "deepseek",
			model:   "deepseek-v4-flash",
			body:    liveToolCallRequest("deepseek-v4-flash", 320),
			fixture: strings.Join([]string{
				`data: {"choices":[{"delta":{"content":null,"reasoning_content":"Need a tool."}}]}`,
				"",
				`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_ds","type":"function","function":{"name":"get_weather","arguments":""}}]}}]}`,
				"",
				`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"city\""}}]}}]}`,
				"",
				`data: {"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":": \"Paris\"}"}}]},"finish_reason":"tool_calls"}]}`,
				"",
				`data: {"choices":[],"usage":{"prompt_tokens":20,"completion_tokens":12,"total_tokens":32,"prompt_tokens_details":{"cached_tokens":10},"completion_tokens_details":{"reasoning_tokens":7}}}`,
				"",
				`data: [DONE]`,
				"",
			}, "\n"),
			wantTool:          true,
			wantRequestSubstr: []string{"deepseek-v4-flash", "get_weather", "tool_choice"},
		},
		{
			name:    "mimo multimodal",
			profile: "mimo",
			model:   "mimo-v2.5",
			body: map[string]any{
				"model":             "mimo-v2.5",
				"max_output_tokens": 256,
				"input": []any{
					map[string]any{
						"type": "message",
						"role": "user",
						"content": []any{
							map[string]any{"type": "input_text", "text": "What is the dominant color in this image? Answer with one English word."},
							map[string]any{"type": "input_image", "image_url": redPNGDataURL()},
						},
					},
				},
			},
			fixture: strings.Join([]string{
				`data: {"choices":[{"delta":{"content":"","role":"assistant","tool_calls":null,"reasoning_content":null},"finish_reason":null,"index":0}],"model":"mimo-v2.5","object":"chat.completion.chunk"}`,
				"",
				`data: {"choices":[{"delta":{"content":null,"role":null,"tool_calls":null,"reasoning_content":"The dominant color is red."},"finish_reason":null,"index":0}],"model":"mimo-v2.5","object":"chat.completion.chunk"}`,
				"",
				`data: {"choices":[{"delta":{"content":"Red","role":null,"tool_calls":null,"reasoning_content":null},"finish_reason":"stop","index":0}],"model":"mimo-v2.5","object":"chat.completion.chunk"}`,
				"",
				`data: {"choices":[],"model":"mimo-v2.5","object":"chat.completion.chunk","usage":{"completion_tokens":39,"prompt_tokens":272,"total_tokens":311,"completion_tokens_details":{"reasoning_tokens":35},"prompt_tokens_details":{"cached_tokens":256,"image_tokens":8}}}`,
				"",
				`data: [DONE]`,
				"",
			}, "\n"),
			wantText:          "red",
			wantRequestSubstr: []string{"mimo-v2.5", "image_url", "data:image/png;base64", "What is the dominant color"},
		},
		{
			name:    "mimo tool",
			profile: "mimo",
			model:   "mimo-v2.5",
			body:    liveToolCallRequest("mimo-v2.5", 256),
			fixture: strings.Join([]string{
				`data: {"choices":[{"delta":{"content":"","role":"assistant","tool_calls":null,"reasoning_content":null},"finish_reason":null,"index":0}],"model":"mimo-v2.5","object":"chat.completion.chunk"}`,
				"",
				`data: {"choices":[{"delta":{"content":null,"role":null,"tool_calls":[{"index":0,"id":"call_mimo","function":{"arguments":"{\"city\": \"Paris\"}","name":"get_weather"},"type":"function"}],"reasoning_content":"Use the city parameter."},"finish_reason":"tool_calls","index":0}],"model":"mimo-v2.5","object":"chat.completion.chunk"}`,
				"",
				`data: {"choices":[],"model":"mimo-v2.5","object":"chat.completion.chunk","usage":{"completion_tokens":56,"prompt_tokens":496,"total_tokens":552,"completion_tokens_details":{"reasoning_tokens":33},"prompt_tokens_details":{"cached_tokens":192}}}`,
				"",
				`data: [DONE]`,
				"",
			}, "\n"),
			wantTool:          true,
			wantRequestSubstr: []string{"mimo-v2.5", "get_weather", "tool_choice"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var upstreamBody string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				raw, err := io.ReadAll(r.Body)
				if err != nil {
					t.Errorf("read upstream body: %v", err)
				}
				upstreamBody = string(raw)
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = w.Write([]byte(tc.fixture))
			}))
			defer server.Close()
			facade := &Facade{
				Adapter:      OpenAIChatAdapter{BaseURL: server.URL + "/v1", Profile: ProfileForProvider(tc.profile), HTTPClient: server.Client(), MaxRetries: -1},
				Store:        NewMemoryStore(),
				ProviderID:   tc.profile,
				DefaultModel: tc.model,
				NewID:        func(prefix string) (string, error) { return prefix + "_mocked_live_" + tc.profile, nil },
			}
			response := postLiveResponses(t, facade, tc.body)
			if tc.wantText != "" && !strings.Contains(strings.ToLower(response.OutputText), strings.ToLower(tc.wantText)) {
				t.Fatalf("output_text = %q output=%#v, want %q", response.OutputText, response.Output, tc.wantText)
			}
			if tc.wantTool {
				assertLiveWeatherToolCall(t, response)
			}
			if response.Usage == nil || response.Usage.TotalTokens == 0 || response.Usage.CachedTokens == 0 {
				t.Fatalf("usage = %#v, want populated usage with cached tokens", response.Usage)
			}
			for _, want := range tc.wantRequestSubstr {
				if !strings.Contains(upstreamBody, want) {
					t.Fatalf("upstream body missing %q:\n%s", want, upstreamBody)
				}
			}
		})
	}
}

func eventReasoning(events []ProviderEvent) string {
	var out strings.Builder
	for _, event := range events {
		if event.Kind == ProviderEventReasoningDelta {
			out.WriteString(event.Delta)
		}
	}
	return out.String()
}

func eventToolCall(events []ProviderEvent) (string, string) {
	var name string
	var args strings.Builder
	for _, event := range events {
		if event.Kind != ProviderEventToolCallDelta || event.ToolCall == nil {
			continue
		}
		if event.ToolCall.Name != "" {
			name = event.ToolCall.Name
		}
		args.WriteString(event.ToolCall.ArgumentsDelta)
	}
	return name, args.String()
}

func lastUsage(events []ProviderEvent) *Usage {
	var usage *Usage
	for _, event := range events {
		if event.Kind == ProviderEventUsage {
			usage = event.Usage
		}
	}
	return usage
}

func firstOutputItemOfType(items []outputItem, itemType string) *outputItem {
	for i := range items {
		if items[i].Type == itemType {
			return &items[i]
		}
	}
	return nil
}
