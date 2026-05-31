package responsesadapter

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNormalizeToolsConvertsResponsesFunctionTools(t *testing.T) {
	raw := json.RawMessage(`[
		{"type":"function","name":"read_file","description":"Read a file","parameters":{"type":"object"},"strict":null},
		{"type":"function","name":"write_file","parameters":{"type":"object"},"strict":false}
	]`)

	tools, warnings, err := NormalizeTools(raw)
	if err != nil {
		t.Fatalf("NormalizeTools error: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v", warnings)
	}
	if len(tools) != 2 {
		t.Fatalf("tools = %#v", tools)
	}
	if tools[0].Function.Name != "read_file" || tools[0].Function.Strict != nil {
		t.Fatalf("first tool = %#v", tools[0])
	}
	if tools[1].Function.Name != "write_file" || tools[1].Function.Strict == nil || *tools[1].Function.Strict {
		t.Fatalf("second tool = %#v", tools[1])
	}
	encoded, err := json.Marshal(tools[0])
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(encoded), "strict") {
		t.Fatalf("strict:null should be omitted, encoded = %s", encoded)
	}
}

func TestNormalizeToolsAcceptsChatStyleFunctionTools(t *testing.T) {
	raw := json.RawMessage(`[
		{"type":"function","function":{"name":"search","description":"Search","parameters":{"type":"object","properties":{"q":{"type":"string"}}}}}
	]`)

	tools, warnings, err := NormalizeTools(raw)
	if err != nil {
		t.Fatalf("NormalizeTools error: %v", err)
	}
	if len(warnings) != 0 || len(tools) != 1 {
		t.Fatalf("tools=%#v warnings=%#v", tools, warnings)
	}
	if tools[0].Function.Name != "search" || !strings.Contains(string(tools[0].Function.Parameters), `"q"`) {
		t.Fatalf("tool = %#v", tools[0])
	}
}

func TestNormalizeToolsMapsLocalShellAndCustomTools(t *testing.T) {
	raw := json.RawMessage(`[
		{"type":"local_shell"},
		{"type":"custom","name":"apply_patch","description":"Apply a patch"}
	]`)

	tools, warnings, err := NormalizeTools(raw)
	if err != nil {
		t.Fatalf("NormalizeTools error: %v", err)
	}
	if len(warnings) != 0 || len(tools) != 2 {
		t.Fatalf("tools=%#v warnings=%#v", tools, warnings)
	}
	if tools[0].Function.Name != "shell" || !strings.Contains(string(tools[0].Function.Parameters), `"command"`) {
		t.Fatalf("shell tool = %#v", tools[0])
	}
	if tools[1].Function.Name != "apply_patch" || !strings.Contains(string(tools[1].Function.Parameters), `"input"`) {
		t.Fatalf("custom tool = %#v", tools[1])
	}
}

func TestNormalizeToolsFlattensNamespaceTools(t *testing.T) {
	raw := json.RawMessage(`[
		{"type":"function","name":"exec_command","parameters":{"type":"object"}},
		{"type":"web_search_preview"},
		{"type":"image_generation"},
		{"type":"namespace","name":"mcp__codex_apps__github","tools":[
			{"type":"function","name":"mcp__codex_apps__github_add_comment_to_issue","parameters":{"type":"object"}},
			{"type":"function","name":"mcp__codex_apps__github_close_issue","parameters":{"type":"object"}},
			{"type":"code_interpreter"}
		]}
	]`)

	tools, warnings, err := NormalizeTools(raw)
	if err != nil {
		t.Fatalf("NormalizeTools error: %v", err)
	}
	names := make([]string, 0, len(tools))
	for _, tool := range tools {
		if tool.Type != "function" {
			t.Fatalf("tool type = %#v", tool)
		}
		names = append(names, tool.Function.Name)
	}
	wantNames := []string{
		"exec_command",
		"mcp__codex_apps__github_add_comment_to_issue",
		"mcp__codex_apps__github_close_issue",
	}
	if strings.Join(names, ",") != strings.Join(wantNames, ",") {
		t.Fatalf("names = %#v, want %#v; warnings=%#v", names, wantNames, warnings)
	}
	if len(warnings) != 3 {
		t.Fatalf("warnings = %#v", warnings)
	}
}

func TestNormalizeToolsPreservesToolSearchAsFunction(t *testing.T) {
	raw := json.RawMessage(`[
		{"type":"tool_search","description":"Search registered tools","parameters":{"type":"object","properties":{"query":{"type":"string"}}}},
		{"type":"function","name":"read_file","parameters":{"type":"object"}},
		{"type":"local_shell"}
	]`)

	tools, warnings, err := NormalizeTools(raw)
	if err != nil {
		t.Fatalf("NormalizeTools error: %v", err)
	}
	if len(warnings) != 0 {
		t.Fatalf("warnings = %#v", warnings)
	}
	if len(tools) != 3 {
		t.Fatalf("tools = %#v", tools)
	}
	if tools[0].Function.Name != "tool_search" || !strings.Contains(string(tools[0].Function.Parameters), `"query"`) {
		t.Fatalf("tool_search = %#v", tools[0])
	}
}

func TestNormalizeToolsDropsUnsupportedDuplicateAndNamelessTools(t *testing.T) {
	raw := json.RawMessage(`[
		{"type":"web_search_preview"},
		{"type":"function","name":"read_file","parameters":{"type":"object"}},
		{"type":"function","name":"read_file","parameters":{"type":"object"}},
		{"type":"function","parameters":{"type":"object"}}
	]`)

	tools, warnings, err := NormalizeTools(raw)
	if err != nil {
		t.Fatalf("NormalizeTools error: %v", err)
	}
	if len(tools) != 1 || tools[0].Function.Name != "read_file" {
		t.Fatalf("tools = %#v", tools)
	}
	if len(warnings) != 3 {
		t.Fatalf("warnings = %#v", warnings)
	}
	for _, want := range []string{"unsupported tool type dropped", "duplicate tool name dropped", "function tool missing name dropped"} {
		found := false
		for _, warning := range warnings {
			if warning.Reason == want {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("missing warning %q in %#v", want, warnings)
		}
	}
}
