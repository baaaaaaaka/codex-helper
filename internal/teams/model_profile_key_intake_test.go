package teams

import (
	"context"
	"strings"
	"testing"
)

func TestParseModelProfileKeyIntakeSetupOptionsModelFlag(t *testing.T) {
	for _, tc := range []struct {
		name        string
		arg         string
		provider    string
		profileName string
		model       string
		sshProxy    string
		setDefault  bool
	}{
		{
			name:        "equals",
			arg:         "mimo mimo25 --model=pro --teams-key-intake --set-default",
			provider:    "mimo",
			profileName: "mimo25",
			model:       "pro",
			setDefault:  true,
		},
		{
			name:        "provider flag before positional profile",
			arg:         "--provider mimo mimo25 --model base --ssh-proxy work --teams-key-intake",
			provider:    "mimo",
			profileName: "mimo25",
			model:       "base",
			sshProxy:    "work",
		},
		{
			name:        "no ssh proxy clears value",
			arg:         "deepseek deepseek-work --model v4-pro --ssh-proxy=none --teams-key-intake",
			provider:    "deepseek",
			profileName: "deepseek-work",
			model:       "v4-pro",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseModelProfileKeyIntakeSetupOptions(tc.arg)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if !got.TeamsKeyIntake || got.Provider != tc.provider || got.ProfileName != tc.profileName || got.Model != tc.model || got.SSHProxy != tc.sshProxy || got.SetDefault != tc.setDefault {
				t.Fatalf("parsed options = %#v", got)
			}
		})
	}
}

func TestParseModelProfileKeyIntakeSetupOptionsSimpleModel(t *testing.T) {
	got, err := parseModelProfileKeyIntakeSetupOptions("mimo-v2.5-pro")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got.Provider != "mimo-v2.5-pro" || got.ProfileName != "" || got.Model != "" || got.SimpleModel {
		t.Fatalf("removed built-in selector parsed as legacy simple model = %#v", got)
	}
}

func TestParseModelProfileKeyIntakeSetupOptionsRejectsMissingModelValue(t *testing.T) {
	_, err := parseModelProfileKeyIntakeSetupOptions("mimo mimo25 --model")
	if err == nil || !strings.Contains(err.Error(), "--model requires a value") {
		t.Fatalf("parse err=%v, want missing model value", err)
	}
}

func TestBridgeModelProfileTeamsKeyIntakeSimpleModelStoresCredentialScope(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, store, nil)
	bridge.modelProfileManager = &fakeModelProfileManager{}
	oldCode := newModelProfileKeyIntakeCode
	newModelProfileKeyIntakeCode = func() (string, error) { return "MIMO25P1", nil }
	t.Cleanup(func() { newModelProfileKeyIntakeCode = oldCode })

	out, err := bridge.startModelProfileKeyIntake(ctx, bridgeTestMessage("setup-compat"), "responses-compatible compat-work --model example/reasoning-model --teams-key-intake")
	if err != nil {
		t.Fatalf("startModelProfileKeyIntake: %v", err)
	}
	if !strings.Contains(out, "example/reasoning-model") || !strings.Contains(out, "model key confirm MIMO25P1") {
		t.Fatalf("intake output:\n%s", out)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load store: %v", err)
	}
	if len(state.ModelProfileKeyIntakes) != 1 {
		t.Fatalf("pending intakes = %#v", state.ModelProfileKeyIntakes)
	}
	for _, intake := range state.ModelProfileKeyIntakes {
		if intake.Provider != "responses-compatible" || intake.ProfileName != "compat-work" || intake.Model != "example/reasoning-model" || intake.CredentialScope != "" || intake.SetDefault {
			t.Fatalf("stored intake = %#v", intake)
		}
	}
}

func TestBridgeModelProfileTeamsKeyIntakeRejectsInvalidModelBeforePendingIntake(t *testing.T) {
	ctx := context.Background()
	store := newBridgeTestStore(t)
	graph, _ := newBridgeTestGraph(t)
	bridge := newBridgeTestBridge(graph, store, nil)
	bridge.modelProfileManager = &fakeModelProfileManager{}

	_, err := bridge.startModelProfileKeyIntake(ctx, bridgeTestMessage("setup-invalid-model"), "mimo mimo25 --model nope --teams-key-intake")
	if err == nil || !strings.Contains(err.Error(), "unknown model") {
		t.Fatalf("startModelProfileKeyIntake err=%v, want unknown model", err)
	}
	state, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("Load store: %v", err)
	}
	if len(state.ModelProfileKeyIntakes) != 0 {
		t.Fatalf("pending intakes = %#v, want none", state.ModelProfileKeyIntakes)
	}
}
