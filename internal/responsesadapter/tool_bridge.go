package responsesadapter

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
)

func NormalizeTools(raw json.RawMessage) ([]ChatTool, []ToolWarning, error) {
	return NormalizeToolsWithPolicy(raw, "function", "drop")
}

func NormalizeToolsWithMode(raw json.RawMessage, customToolMode string) ([]ChatTool, []ToolWarning, error) {
	return NormalizeToolsWithPolicy(raw, customToolMode, "drop")
}

// NormalizeToolsWithPolicy is the catalog-aware entry point. A provider that
// declares a native feature but has no registered wire adapter must use
// policy=error; silently dropping web_search (or another native tool) would
// make the upstream model fabricate a result while the HTTP request still
// appears successful.
func NormalizeToolsWithPolicy(raw json.RawMessage, customToolMode string, unsupportedToolPolicy string) ([]ChatTool, []ToolWarning, error) {
	tools, _, warnings, err := NormalizeRequestTools(raw, customToolMode, unsupportedToolPolicy, nil)
	return tools, warnings, err
}

// NormalizeRequestTools separates ordinary function tools from provider-owned
// native tools. Native tools are accepted only when a catalog route supplies a
// typed mapping; otherwise the existing fail-closed policy is preserved.
func NormalizeRequestTools(raw json.RawMessage, customToolMode string, unsupportedToolPolicy string, specs []NativeToolSpec) ([]ChatTool, []ProviderNativeTool, []ToolWarning, error) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, nil, nil, nil
	}
	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, nil, nil, fmt.Errorf("invalid tools: %w", err)
	}
	seen := map[string]bool{}
	var tools []ChatTool
	var nativeTools []ProviderNativeTool
	var warnings []ToolWarning
	for _, item := range items {
		normalized, nestedNative, itemWarnings := normalizeRequestTool(item, specs)
		warnings = append(warnings, itemWarnings...)
		for _, native := range nestedNative {
			key := "native:" + strings.ToLower(native.InputType)
			if seen[key] {
				warnings = append(warnings, ToolWarning{Type: native.InputType, Name: native.Name, Reason: "duplicate native tool dropped"})
				continue
			}
			seen[key] = true
			nativeTools = append(nativeTools, native)
		}
		for _, tool := range normalized {
			nameKey := strings.ToLower(strings.TrimSpace(tool.Namespace) + "\x00" + strings.TrimSpace(tool.Function.Name))
			if seen[nameKey] {
				warnings = append(warnings, ToolWarning{Type: tool.Type, Name: tool.Function.Name, Reason: "duplicate tool name dropped"})
				continue
			}
			seen[nameKey] = true
			tools = append(tools, tool)
		}
	}
	if strings.EqualFold(strings.TrimSpace(unsupportedToolPolicy), "error") {
		for _, warning := range warnings {
			if strings.Contains(warning.Reason, "unsupported tool type") {
				return nil, nil, warnings, fmt.Errorf("unsupported upstream tool %q: %s", warning.Type, warning.Reason)
			}
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
	return tools, nativeTools, warnings, nil
}

func normalizeRequestTool(raw json.RawMessage, specs []NativeToolSpec) ([]ChatTool, []ProviderNativeTool, []ToolWarning) {
	return normalizeRequestToolInNamespace(raw, specs, "")
}

func normalizeRequestToolInNamespace(raw json.RawMessage, specs []NativeToolSpec, namespace string) ([]ChatTool, []ProviderNativeTool, []ToolWarning) {
	if len(specs) > 0 {
		if native, ok, warning := normalizeNativeTool(raw, specs); ok {
			return nil, []ProviderNativeTool{native}, nil
		} else if warning != nil {
			return nil, nil, []ToolWarning{*warning}
		}
	}
	var object struct {
		Type  string            `json:"type"`
		Name  string            `json:"name"`
		Tools []json.RawMessage `json:"tools"`
	}
	if err := json.Unmarshal(raw, &object); err == nil && strings.EqualFold(strings.TrimSpace(object.Type), "namespace") {
		nestedNamespace := strings.TrimSpace(object.Name)
		var tools []ChatTool
		var natives []ProviderNativeTool
		var warnings []ToolWarning
		for _, child := range object.Tools {
			childTools, childNatives, childWarnings := normalizeRequestToolInNamespace(child, specs, nestedNamespace)
			tools = append(tools, childTools...)
			natives = append(natives, childNatives...)
			warnings = append(warnings, childWarnings...)
		}
		if len(tools) == 0 && len(natives) == 0 && len(warnings) == 0 {
			warnings = append(warnings, ToolWarning{Type: "namespace", Reason: "namespace contained no supported tools dropped"})
		}
		return tools, natives, warnings
	}
	tools, warnings := normalizeTool(raw)
	for index := range tools {
		if namespace != "" {
			tools[index].Namespace = namespace
		}
	}
	return tools, nil, warnings
}

func normalizeNativeTool(raw json.RawMessage, specs []NativeToolSpec) (ProviderNativeTool, bool, *ToolWarning) {
	if len(specs) == 0 {
		return ProviderNativeTool{}, false, nil
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return ProviderNativeTool{}, false, nil
	}
	inputType := strings.TrimSpace(rawString(object["type"]))
	if inputType == "" {
		return ProviderNativeTool{}, false, nil
	}
	for _, spec := range specs {
		matched := false
		for _, candidate := range spec.InputTypes {
			if strings.EqualFold(strings.TrimSpace(candidate), inputType) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		if strings.TrimSpace(spec.UpstreamType) == "" {
			warning := ToolWarning{Type: inputType, Reason: "native tool mapping has no upstream type"}
			return ProviderNativeTool{}, false, &warning
		}
		fields := map[string]any{}
		for _, field := range spec.AllowedFields {
			key := strings.TrimSpace(field)
			if key == "" || key == "type" {
				continue
			}
			value, ok := object[key]
			if !ok {
				continue
			}
			var decoded any
			if json.Unmarshal(value, &decoded) == nil {
				fields[key] = decoded
			}
		}
		return ProviderNativeTool{InputType: inputType, UpstreamType: strings.TrimSpace(spec.UpstreamType), Name: strings.TrimSpace(spec.Name), Fields: fields}, true, nil
	}
	return ProviderNativeTool{}, false, nil
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
		return []ChatTool{{Type: "function", Function: fn, SourceType: "function"}}, nil
	case "local_shell":
		return []ChatTool{{
			Type: "function", SourceType: "local_shell",
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
			Type: "function", SourceType: "custom",
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
			Type: "function", SourceType: "tool_search",
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
