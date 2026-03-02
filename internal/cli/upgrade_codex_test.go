package cli

import (
	"strings"
	"testing"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestRootProfileArg(t *testing.T) {
	cases := []struct {
		name    string
		args    []string
		want    string
		wantErr string
	}{
		{
			name: "no args",
			args: nil,
			want: "",
		},
		{
			name: "one profile arg",
			args: []string{"profile-a"},
			want: "profile-a",
		},
		{
			name:    "too many args before dash",
			args:    []string{"profile-a", "profile-b"},
			wantErr: "unexpected args before --",
		},
		{
			name:    "args after dash",
			args:    []string{"profile-a", "--", "echo"},
			wantErr: "unexpected args after --",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := &cobra.Command{}
			if err := cmd.Flags().Parse(tc.args); err != nil {
				t.Fatalf("parse args: %v", err)
			}
			got, err := rootProfileArg(cmd)
			if tc.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErr)
				}
				if !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %q", tc.wantErr, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("expected profile %q, got %q", tc.want, got)
			}
		})
	}
}

func TestUpgradeUsesProxy(t *testing.T) {
	enabled := true
	disabled := false

	cases := []struct {
		name string
		cfg  config.Config
		want bool
	}{
		{
			name: "unset with no profiles",
			cfg:  config.Config{},
			want: false,
		},
		{
			name: "unset with profiles",
			cfg: config.Config{
				Profiles: []config.Profile{{ID: "p1", Name: "p1"}},
			},
			want: true,
		},
		{
			name: "explicit false with profiles",
			cfg: config.Config{
				ProxyEnabled: &disabled,
				Profiles:     []config.Profile{{ID: "p1", Name: "p1"}},
			},
			want: false,
		},
		{
			name: "explicit true with no profiles",
			cfg: config.Config{
				ProxyEnabled: &enabled,
			},
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := upgradeUsesProxy(tc.cfg)
			if got != tc.want {
				t.Fatalf("expected %v, got %v", tc.want, got)
			}
		})
	}
}
