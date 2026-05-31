package responsesadapter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

func NormalizeTools(raw json.RawMessage) ([]ChatTool, []ToolWarning, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, nil, fmt.Errorf("invalid tools: %w", err)
	}
	seen := map[string]bool{}
	var tools []ChatTool
	var warnings []ToolWarning
	for _, item := range items {
		normalized, toolWarnings := normalizeTool(item)
		warnings = append(warnings, toolWarnings...)
		for _, tool := range normalized {
			nameKey := strings.ToLower(tool.Function.Name)
			if seen[nameKey] {
				warnings = append(warnings, ToolWarning{Type: tool.Type, Name: tool.Function.Name, Reason: "duplicate tool name dropped"})
				continue
			}
			seen[nameKey] = true
			tools = append(tools, tool)
		}
	}
	return tools, warnings, nil
}

func normalizeTool(raw json.RawMessage) ([]ChatTool, []ToolWarning) {
	var input struct {
		Type        string            `json:"type"`
		Name        string            `json:"name"`
		Description string            `json:"description"`
		Parameters  json.RawMessage   `json:"parameters"`
		Strict      json.RawMessage   `json:"strict"`
		Tools       []json.RawMessage `json:"tools"`
		Function    *struct {
			Name        string          `json:"name"`
			Description string          `json:"description"`
			Parameters  json.RawMessage `json:"parameters"`
			Strict      json.RawMessage `json:"strict"`
		} `json:"function"`
	}
	if err := json.Unmarshal(raw, &input); err != nil {
		return nil, []ToolWarning{{Reason: "invalid tool JSON dropped"}}
	}
	toolType := strings.TrimSpace(input.Type)
	switch toolType {
	case "function":
		fn := ChatFunction{
			Name:        input.Name,
			Description: input.Description,
			Parameters:  input.Parameters,
			Strict:      strictBool(input.Strict),
		}
		if input.Function != nil {
			fn.Name = input.Function.Name
			fn.Description = input.Function.Description
			fn.Parameters = input.Function.Parameters
			fn.Strict = strictBool(input.Function.Strict)
		}
		fn.Name = strings.TrimSpace(fn.Name)
		if fn.Name == "" {
			return nil, []ToolWarning{{Type: toolType, Reason: "function tool missing name dropped"}}
		}
		return []ChatTool{{Type: "function", Function: fn}}, nil
	case "local_shell":
		return []ChatTool{{
			Type: "function",
			Function: ChatFunction{
				Name:        "shell",
				Description: "Run a shell command.",
				Parameters:  shellToolSchema(),
			},
		}}, nil
	case "custom":
		name := strings.TrimSpace(input.Name)
		if name == "" {
			name = "custom_tool"
		}
		return []ChatTool{{
			Type: "function",
			Function: ChatFunction{
				Name:        name,
				Description: firstNonEmpty(input.Description, "Accepts freeform custom tool input."),
				Parameters:  customToolSchema(),
			},
		}}, nil
	case "tool_search":
		name := firstNonEmpty(input.Name, "tool_search")
		parameters := input.Parameters
		if len(bytes.TrimSpace(parameters)) == 0 {
			parameters = json.RawMessage(`{"type":"object","additionalProperties":true}`)
		}
		return []ChatTool{{
			Type: "function",
			Function: ChatFunction{
				Name:        name,
				Description: firstNonEmpty(input.Description, "Search available tools."),
				Parameters:  parameters,
			},
		}}, nil
	case "namespace":
		var tools []ChatTool
		var warnings []ToolWarning
		for _, child := range input.Tools {
			childTools, childWarnings := normalizeTool(child)
			tools = append(tools, childTools...)
			warnings = append(warnings, childWarnings...)
		}
		if len(tools) == 0 && len(warnings) == 0 {
			warnings = append(warnings, ToolWarning{Type: toolType, Name: input.Name, Reason: "namespace contained no supported tools dropped"})
		}
		return tools, warnings
	default:
		return nil, []ToolWarning{{Type: toolType, Name: input.Name, Reason: "unsupported tool type dropped"}}
	}
}

func strictBool(raw json.RawMessage) *bool {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	var value bool
	if err := json.Unmarshal(trimmed, &value); err != nil {
		return nil
	}
	return &value
}

func shellToolSchema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"command":{"type":"string","description":"Command to run."}},"required":["command"],"additionalProperties":false}`)
}

func customToolSchema() json.RawMessage {
	return json.RawMessage(`{"type":"object","properties":{"input":{"type":"string","description":"Freeform tool input."}},"required":["input"],"additionalProperties":false}`)
}
