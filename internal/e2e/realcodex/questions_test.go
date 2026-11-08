//go:build !windows

package realcodex

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRealCodexSharedQuestionsSync(t *testing.T) {
	p := newProbe(t)
	p.resp.ToolOnce("request_user_input", map[string]any{"questions": []any{map[string]any{"id": "environment", "header": "Environment", "question": "Which environment?", "options": []any{map[string]any{"label": "STAGING", "description": "Test data"}, map[string]any{"label": "PRODUCTION", "description": "Live data"}}}}})
	connect := p.sharedServer()
	a, b := connect(), connect()
	thread := sharedThread(a, p.dir)
	params := turnStartParams(thread, "Ask a question", "question-sync")
	params["collaborationMode"] = map[string]any{"mode": "plan", "settings": map[string]any{"model": "probe-model", "reasoning_effort": "low", "developer_instructions": nil}}
	a.call("turn/start", params)
	request := a.wait("item/tool/requestUserInput", "Which environment?")
	b.call("thread/resume", map[string]any{"threadId": thread, "excludeTurns": true})
	other := b.wait("item/tool/requestUserInput", "Which environment?")
	if string(request.ID) != string(other.ID) {
		t.Fatalf("different native requests: %s / %s", request.ID, other.ID)
	}
	if err := b.c.WriteJSON(map[string]any{"id": other.ID, "result": map[string]any{"answers": map[string]any{"environment": map[string]any{"answers": []string{"STAGING"}}}}}); err != nil {
		t.Fatal(err)
	}
	a.wait("serverRequest/resolved", "requestId")
	a.wait("turn/completed", thread)
	p.evidence("questions.sync", request)
}

func TestRealCodexSharedQuestionsAsync(t *testing.T) {
	p := newProbe(t)
	// Native Codex advertises the async tool only for model catalogs that
	// list this capability. The generic offline probe model has none.
	catalog, _ := json.Marshal(map[string]any{"models": []any{map[string]any{
		"slug": "probe-model", "display_name": "Offline question probe", "supported_reasoning_levels": []any{},
		"shell_type": "shell_command", "visibility": "list", "supported_in_api": true, "priority": 0,
		"support_verbosity": false, "base_instructions": "Use the provided test tools.",
		"truncation_policy":            map[string]any{"mode": "tokens", "limit": 10000},
		"experimental_supported_tools": []string{"request_user_input_async"},
	}}})
	path := filepath.Join(p.home, "models.json")
	if err := os.WriteFile(path, catalog, 0600); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(p.home, "config.toml")
	config, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	config = append([]byte(fmt.Sprintf("model_catalog_json = %q\n", path)), config...)
	if err := os.WriteFile(configPath, config, 0600); err != nil {
		t.Fatal(err)
	}
	p.resp.ToolOnce("request_user_input_async", map[string]any{"questions": []any{map[string]any{"title": "Which environment?", "options": []string{"STAGING", "PRODUCTION"}}}})
	connect := p.sharedServer()
	a, b := connect(), connect()
	thread := sharedThread(a, p.dir)
	turn := sharedStart(a, thread, "Ask an async question", "question-async")
	m := a.wait("item/completed", `"delivery":"async"`)
	var params struct {
		Item struct {
			ID        string
			Questions []struct{ Title string }
		}
	}
	if err := json.Unmarshal(m.Params, &params); err != nil || len(params.Item.Questions) != 1 {
		t.Fatalf("native async item: %s / %v", m.Params, err)
	}
	a.wait("turn/completed", turn)
	b.call("thread/resume", map[string]any{"threadId": thread, "excludeTurns": true})
	nativeID, _ := json.Marshal([]any{"request_user_input_async", params.Item.ID, 0})
	reply, _ := json.Marshal([]map[string]string{{"questionItemId": string(nativeID), "question": params.Item.Questions[0].Title, "answer": "STAGING"}})
	envelope := "<send_user_message_question_reply>\n" + string(reply) + "\n</send_user_message_question_reply>"
	followup := sharedStart(b, thread, envelope, "question-answer")
	b.wait("turn/completed", followup)
	seen := false
	for _, r := range p.resp.Requests() {
		if strings.Contains(r.UserText, "STAGING") {
			seen = true
		}
	}
	if !seen {
		t.Fatal(fmt.Sprintf("native answer was not presented to model: %+v", p.resp.Requests()))
	}
	p.evidence("questions.async", m)
}
