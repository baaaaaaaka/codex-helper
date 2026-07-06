package cli

import (
	"os/user"
	"testing"
)

func TestDefaultTeamsServiceWSLLinuxUserNameIgnoresSpoofedEnvironment(t *testing.T) {
	previous := teamsServiceCurrentUser
	teamsServiceCurrentUser = func() (*user.User, error) {
		return &user.User{Username: "account-user", HomeDir: "/home/account-user"}, nil
	}
	t.Cleanup(func() { teamsServiceCurrentUser = previous })
	t.Setenv("USER", "spoofed-user")
	t.Setenv("LOGNAME", "spoofed-logname")
	t.Setenv("USERNAME", "spoofed-windows-name")
	if got := defaultTeamsServiceWSLLinuxUserName(); got != "account-user" {
		t.Fatalf("WSL Linux username = %q, want account-user", got)
	}
}
