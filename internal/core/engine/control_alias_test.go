package engine

import (
	"testing"
)

// The control chat accepts v0.1.20's short forms of the commands the new
// core has (v0.1.20 dashboard_commands.go controlNaturalCommandName): the
// owner sent `st` routinely, and the live canary on the real account
// answered it with "Unknown command".
func TestControlChatAcceptsV0120Aliases(t *testing.T) {
	r := newRig(t)
	ctl := r.control()
	for _, c := range []struct{ text, want string }{
		{"st", "Control status:"},
		{"helper st", "Control status:"},
		{"h", "Send `help advanced` for all commands."},
		{"menu", "Send `help advanced` for all commands."},
		{"s", "No local Codex sessions found"},
		{"session", "No local Codex sessions found"},
		{"history", "No local Codex sessions found"},
	} {
		before := len(r.own(ctl, c.want))
		r.ch.PostUser(ctl, c.text)
		r.wait("reply to `"+c.text+"`", func() bool { return len(r.own(ctl, c.want)) > before })
	}
	if n := len(r.own(ctl, "unknown control command")); n != 0 {
		t.Fatalf("%d aliases answered with unknown control command", n)
	}
	dir := t.TempDir()
	r.ch.PostUser(ctl, "create "+dir)
	r.wait("work chat from `create`", func() bool { return len(r.own(ctl, "Work chat created")) == 1 })
}
