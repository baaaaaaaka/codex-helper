package cli

import (
	"bytes"
	"strings"
	"testing"
)

func TestTeamsServiceParkArgumentsAndHelp(t *testing.T) {
	for _, args := range [][]string{{}, {"s001"}, {"s001", "maybe"}, {"s001", "off", "extra"}} {
		cmd := newTeamsServiceParkCmd()
		cmd.SetArgs(args)
		if err := cmd.Execute(); err == nil {
			t.Fatalf("accepted invalid args %q", args)
		}
	}
	for _, action := range []string{"on", "off", "status", "OFF"} {
		cmd := newTeamsServiceParkCmd()
		cmd.SetArgs([]string{"s001", action})
		if err := cmd.Args(cmd, []string{"s001", action}); err != nil {
			t.Fatal(err)
		}
	}
	cmd := newTeamsServiceCmd(&rootOptions{})
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"park", "--help"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "<on|off|status>") {
		t.Fatalf("missing park help: %s", out.String())
	}
}
