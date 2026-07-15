package responsesadapter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

// structuredOutputFormat extracts the two wire formats supported by the
// Responses API. Unknown formats are left to the upstream adapter.
func structuredOutputFormat(raw json.RawMessage) string {
	var value struct {
		Type string `json:"type"`
	}
	if len(bytes.TrimSpace(raw)) == 0 || json.Unmarshal(raw, &value) != nil {
		return ""
	}
	switch strings.ToLower(strings.TrimSpace(value.Type)) {
	case "json_object":
		return "json_object"
	case "json_schema":
		return "json_schema"
	default:
		return ""
	}
}

func structuredOutputMode(policy config.ModelStructuredOutputPolicy, format string) string {
	switch format {
	case "json_object":
		return strings.ToLower(strings.TrimSpace(policy.JSONObject))
	case "json_schema":
		return strings.ToLower(strings.TrimSpace(policy.JSONSchema))
	default:
		return ""
	}
}

func validateStructuredOutputRequest(raw json.RawMessage, policy config.ModelStructuredOutputPolicy) error {
	format := structuredOutputFormat(raw)
	if format == "" {
		return nil
	}
	if mode := structuredOutputMode(policy, format); mode == "unsupported" {
		return fmt.Errorf("structured output format %s is unsupported by the selected model", format)
	}
	return nil
}

// validateStructuredOutputResponse deliberately implements the provider
// independent part of the contract: the result must be actual JSON and, for
// json_schema, an object containing every declared required property. Full
// JSON-Schema validation remains provider-specific and is not guessed here.
func validateStructuredOutputResponse(raw json.RawMessage, text string, policy config.ModelStructuredOutputPolicy) error {
	format := structuredOutputFormat(raw)
	if format == "" || structuredOutputMode(policy, format) != "native" {
		return nil
	}
	text = strings.TrimSpace(text)
	if text == "" {
		return fmt.Errorf("provider returned no message for native %s structured output", format)
	}
	var value any
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return fmt.Errorf("provider returned invalid %s structured output: %w", format, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("provider returned invalid %s structured output: multiple JSON values", format)
		}
		return fmt.Errorf("provider returned invalid %s structured output: %w", format, err)
	}
	if format == "json_object" {
		if _, ok := value.(map[string]any); !ok {
			return fmt.Errorf("provider returned %s structured output that is not a JSON object", format)
		}
		return nil
	}
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("provider returned json_schema output that is not a JSON object")
	}
	var envelope struct {
		JSONSchema struct {
			Schema struct {
				Required []string `json:"required"`
			} `json:"schema"`
		} `json:"json_schema"`
	}
	if json.Unmarshal(raw, &envelope) == nil {
		for _, required := range envelope.JSONSchema.Schema.Required {
			if _, exists := object[required]; !exists {
				return fmt.Errorf("provider json_schema output is missing required property %q", required)
			}
		}
	}
	return nil
}
