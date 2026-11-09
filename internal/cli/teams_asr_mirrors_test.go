package cli

import (
	"reflect"
	"testing"
)

func TestTeamsASRMirrorConfigAndServiceEnvironment(t *testing.T) {
	raw := "https://one.example/voice, https://two.example/voice"
	t.Setenv(envTeamsASRMirrors, raw)
	cfg := teamsASRManagedConfig()
	if !reflect.DeepEqual(cfg.MirrorBaseURLs, []string{"https://one.example/voice", "https://two.example/voice"}) {
		t.Fatalf("mirrors=%v", cfg.MirrorBaseURLs)
	}
	if got := teamsASRServiceEnvironmentOverrides("", nil)[envTeamsASRMirrors]; got != raw {
		t.Fatalf("service dropped mirrors: %q", got)
	}
}
