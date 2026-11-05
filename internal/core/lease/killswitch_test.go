package lease

import (
	"os"
	"path/filepath"
	"testing"
)

// The kill-switch check opens the directory before it looks the file up:
// on NFS that revalidates the directory's cached attributes, so a DISABLED
// created by another host is seen at once instead of after the attribute
// cache expires (up to 60 s; measured in TAKEOVER-REPORT §9, drill 5).
// Every check uses it: a standby's, status's, and the owner's heartbeat.
func TestKillSwitchRevalidatesTheDirectory(t *testing.T) {
	dir := t.TempDir()
	var opened []string
	orig := openDir
	openDir = func(d string) (*os.File, error) {
		opened = append(opened, d)
		return orig(d)
	}
	defer func() { openDir = orig }()
	if KillSwitch(dir) {
		t.Fatal("no DISABLED yet")
	}
	if err := os.WriteFile(filepath.Join(dir, DisabledFile), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if !KillSwitch(dir) {
		t.Fatal("DISABLED not seen")
	}
	if len(opened) != 2 || opened[0] != dir {
		t.Fatalf("directory opens: %q", opened)
	}
	opened = nil
	if in, err := Read(filepath.Join(dir, "core.sqlite")); err != nil || !in.Disabled || len(opened) != 1 {
		t.Fatalf("Read: %+v %v opens=%q", in, err, opened)
	}
}
