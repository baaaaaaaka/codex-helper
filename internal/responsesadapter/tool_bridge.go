package responsesadapter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

func NormalizeTools(raw json.RawMessage) ([]ChatTool, []ToolWarning, error) {
	return NormalizeToolsWithMode(raw, "function")
}

func NormalizeToolsWithMode(raw json.RawMessage, customToolMode string) ([]ChatTool, []ToolWarning, error) {
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
		normalized, toolWarnings := normalizeToolWithNamespace(item, "")
		warnings = append(warnings, toolWarnings...)
		for _, tool := range normalized {
			nameKey := toolIdentityKey(tool.Namespace, tool.Function.Name)
			if seen[nameKey] {
				warnings = append(warnings, ToolWarning{Type: tool.Type, Name: tool.Function.Name, Reason: "duplicate tool name dropped"})
				continue
			}
			seen[nameKey] = true
			tools = append(tools, tool)
		}
	}
	mode := strings.ToLower(strings.TrimSpace(customToolMode))
	if mode == "shell-fallback" || mode == "omit" {
		hasPatch := false
		filtered := tools[:0]
		for _, tool := range tools {
			if tool.SourceType == "custom" {
				if strings.EqualFold(tool.Function.Name, "apply_patch") {
					hasPatch = true
				}
				warnings = append(warnings, ToolWarning{Type: "custom", Name: tool.Function.Name, Reason: "custom tool omitted by compatibility mode"})
				continue
			}
			filtered = append(filtered, tool)
		}
		tools = filtered
		if mode == "shell-fallback" && hasPatch {
			for index := range tools {
				if tools[index].SourceType == "local_shell" || strings.EqualFold(tools[index].Function.Name, "shell") {
					tools[index].Function.Description += " To edit files, invoke the apply_patch command through this shell tool instead of only describing the patch."
					break
				}
			}
		}
	}
	return tools, warnings, nil
}

func normalizeTool(raw json.RawMessage) ([]ChatTool, []ToolWarning) {
	return normalizeToolWithNamespace(raw, "")
}

func normalizeToolWithNamespace(raw json.RawMessage, namespace string) ([]ChatTool, []ToolWarning) {
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
		return []ChatTool{{Type: "function", Function: fn, SourceType: "function", Namespace: namespace}}, nil
	case "local_shell":
		return []ChatTool{{
			Type: "function", SourceType: "local_shell", Namespace: namespace,
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
			Type: "function", SourceType: "custom", Namespace: namespace,
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
			Type: "function", SourceType: "tool_search", Namespace: namespace,
			Function: ChatFunction{
				Name:        name,
				Description: firstNonEmpty(input.Description, "Search available tools."),
				Parameters:  parameters,
			},
		}}, nil
	case "namespace":
		var tools []ChatTool
		var warnings []ToolWarning
		childNamespace := joinToolNamespace(namespace, input.Name)
		for _, child := range input.Tools {
			childTools, childWarnings := normalizeToolWithNamespace(child, childNamespace)
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

func joinToolNamespace(parent, name string) string {
	parent = strings.TrimSpace(parent)
	name = strings.TrimSpace(name)
	if parent == "" {
		return name
	}
	if name == "" {
		return parent
	}
	return parent + "." + name
}

func toolIdentityKey(namespace, name string) string {
	return strings.ToLower(strings.TrimSpace(namespace)) + "\x00" + strings.ToLower(strings.TrimSpace(name))
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
