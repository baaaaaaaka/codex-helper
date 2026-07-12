package modelprofile

import (
	"reflect"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestHasConfiguredThirdPartyModels(t *testing.T) {
	tests := []struct {
		name string
		cfg  config.Config
		want bool
	}{
		{name: "empty"},
		{name: "official only", cfg: config.Config{ModelProfiles: map[string]config.ModelProfile{
			"official": {Provider: DefaultProvider},
		}}},
		{name: "third party", cfg: config.Config{ModelProfiles: map[string]config.ModelProfile{
			"work": {Provider: "chat-compatible"},
		}}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := HasConfiguredThirdPartyModels(tt.cfg); got != tt.want {
				t.Fatalf("HasConfiguredThirdPartyModels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResolveConfiguredThirdPartyModelsStableOrder(t *testing.T) {
	cfg := config.Config{ModelProfiles: map[string]config.ModelProfile{
		"zeta": {
			Provider:  "chat-compatible",
			Model:     "vendor/zeta",
			BaseURL:   "https://third.example/v1",
			APIKeyRef: "env:THIRD_KEY",
			Revision:  1,
		},
		"Alpha": {
			Provider:  "responses-compatible",
			Model:     "vendor/alpha",
			BaseURL:   "https://responses.example/v1",
			APIKeyRef: "env:RESPONSES_KEY",
			Revision:  1,
		},
	}}
	got, err := ResolveConfiguredThirdPartyModels(cfg)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(got))
	for _, profile := range got {
		names = append(names, profile.Name)
	}
	if want := []string{"Alpha", "zeta"}; !reflect.DeepEqual(gotNames(names), want) {
		t.Fatalf("profile order = %#v, want %#v", names, want)
	}
}

func gotNames(in []string) []string {
	return in
}

func TestResolveConfiguredThirdPartyModelsRejectsBrokenProfile(t *testing.T) {
	cfg := config.Config{ModelProfiles: map[string]config.ModelProfile{
		"broken": {Provider: "chat-compatible", Model: "vendor/model", Revision: 1},
	}}
	if _, err := ResolveConfiguredThirdPartyModels(cfg); err == nil {
		t.Fatal("ResolveConfiguredThirdPartyModels() error = nil, want invalid configuration error")
	}
}
