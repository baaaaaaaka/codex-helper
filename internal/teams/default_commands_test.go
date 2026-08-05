package teams

import (
	"context"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
)

type testGlobalDefaultManager struct {
	effort string
	source string
}

func (m *testGlobalDefaultManager) HandleDefaultCommand(context.Context, DefaultCommand) (string, error) {
	return "handled", nil
}

func (m *testGlobalDefaultManager) ResolveDefaultReasoningEffort(context.Context, modelprofile.Snapshot) (string, string, error) {
	return m.effort, m.source, nil
}

func TestParseDefaultCommandExtensibleGrammar(t *testing.T) {
	tests := []struct {
		input   string
		setting string
		action  DefaultCommandAction
		value   string
	}{
		{input: "", action: DefaultCommandStatus},
		{input: "status", action: DefaultCommandStatus},
		{input: "model", setting: "model", action: DefaultCommandStatus},
		{input: "model list", setting: "model", action: DefaultCommandList},
		{input: "model set official:gpt-test", setting: "model", action: DefaultCommandSet, value: "official:gpt-test"},
		{input: "effort use xhigh", setting: "effort", action: DefaultCommandSet, value: "xhigh"},
		{input: "effort reset", setting: "effort", action: DefaultCommandReset},
		{input: "sandbox status", setting: "sandbox", action: DefaultCommandStatus},
	}
	for _, test := range tests {
		command, err := parseDefaultCommand(test.input)
		if err != nil {
			t.Fatalf("parseDefaultCommand(%q): %v", test.input, err)
		}
		if command.Setting != test.setting || command.Action != test.action || command.Value != test.value {
			t.Fatalf("parseDefaultCommand(%q) = %#v", test.input, command)
		}
	}
}

func TestParseDefaultCommandRejectsIncompleteOrAmbiguousMutation(t *testing.T) {
	for _, input := range []string{"model set", "effort reset extra", "model unknown value", "status extra"} {
		if command, err := parseDefaultCommand(input); err == nil {
			t.Fatalf("parseDefaultCommand(%q) = %#v, nil error", input, command)
		}
	}
}

func TestDefaultCommandWorkChatMessageIsExplicitlyControlOnly(t *testing.T) {
	got := defaultCommandWorkChatMessage()
	if got == "" || !containsAllStrings(got, "only be used in the Control chat", "model", "effort") {
		t.Fatalf("work-chat guidance = %q", got)
	}
}

func containsAllStrings(value string, needles ...string) bool {
	for _, needle := range needles {
		if !strings.Contains(value, needle) {
			return false
		}
	}
	return true
}

func TestGlobalDefaultsApplyOnlyWhenControlAndWorkSessionsAreCreated(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeCreateChatGraph(t, nil)
	bridge := newBridgeTestBridge(graph, store, testEffortCatalogExecutor())
	bridge.defaultManager = &testGlobalDefaultManager{effort: "high", source: "global_default"}
	bridge.modelProfileResolver = func(context.Context, string) (modelprofile.Snapshot, error) {
		return modelprofile.Snapshot{Name: "global", Provider: modelprofile.DefaultProvider, Model: "gpt-global", Revision: 1}, nil
	}

	control, err := bridge.ensureControlFallbackSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if control.ReasoningEffort != "high" || control.ReasoningEffortSource != "global_default" {
		t.Fatalf("new control defaults = %#v", control)
	}

	source := bridge.reg.SessionByID("s001")
	message, err := bridge.forkWorkChatWithModelProfile(ctx, source, "")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(message, "Forked Work chat") {
		t.Fatalf("fork message = %q", message)
	}
	created := bridge.reg.SessionByID("s002")
	if created == nil || created.ReasoningEffort != "high" || created.ReasoningEffortSource != "global_default" {
		t.Fatalf("new work defaults = %#v", created)
	}
	if source.ReasoningEffort != "" || source.ReasoningEffortSource != "" {
		t.Fatalf("existing work chat was mutated = %#v", source)
	}
}

func TestRuntimeDefaultSourceIsCapturedOnlyForNewSessions(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeCreateChatGraph(t, nil)
	bridge := newBridgeTestBridge(graph, store, testEffortCatalogExecutor())
	bridge.defaultManager = &testGlobalDefaultManager{}
	bridge.modelProfileResolver = func(context.Context, string) (modelprofile.Snapshot, error) {
		return modelprofile.Snapshot{Name: "default", Provider: modelprofile.DefaultProvider, Model: "gpt-default", Revision: 1}, nil
	}

	source := bridge.reg.SessionByID("s001")
	if _, err := bridge.forkWorkChatWithModelProfile(ctx, source, ""); err != nil {
		t.Fatal(err)
	}
	created := bridge.reg.SessionByID("s002")
	if created == nil || created.ReasoningEffort != "" || created.ReasoningEffortSource != reasoningEffortSourceRuntimeDefault {
		t.Fatalf("new runtime-default work chat = %#v", created)
	}
	if source.ReasoningEffort != "" || source.ReasoningEffortSource != "" {
		t.Fatalf("existing legacy chat was mutated = %#v", source)
	}
}
