package skillcmd

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
)

func newTest(t *testing.T) *Command {
	d := t.TempDir()
	return New(Options{
		ConfigDir: filepath.Join(d, "config"),
		CacheDir:  filepath.Join(d, "cache"),
		CodexDir:  filepath.Join(d, "codex"),
		HomeDir:   filepath.Join(d, "home"),
	})
}

func TestListEmpty(t *testing.T) {
	got := newTest(t).Command(context.Background(), "")
	if !strings.Contains(got, "No skill subscriptions") {
		t.Fatalf("list: %q", got)
	}
}

func TestLocalOnlyActions(t *testing.T) {
	c := newTest(t)
	for _, a := range []string{"push", "remove x", "rm x"} {
		got := c.Command(context.Background(), a)
		if !strings.Contains(got, "local `cxp skills") {
			t.Fatalf("%s: %q", a, got)
		}
	}
}

func TestUsageAndAddParsing(t *testing.T) {
	c := newTest(t)
	if got := c.Command(context.Background(), "bogus"); !strings.HasPrefix(got, "Usage:") {
		t.Fatalf("bogus: %q", got)
	}
	if got := c.Command(context.Background(), "add --force x"); !strings.HasPrefix(got, "Usage:") {
		t.Fatalf("add flag: %q", got)
	}
	if got := c.Command(context.Background(), "add"); !strings.HasPrefix(got, "Usage:") {
		t.Fatalf("add empty: %q", got)
	}
	if raw, err := parseAdd("<https://github.com/o/r>"); err != nil || raw != "https://github.com/o/r" {
		t.Fatalf("parseAdd: %q %v", raw, err)
	}
}

func TestRepliesCarryNoLocalPaths(t *testing.T) {
	d := t.TempDir()
	c := New(Options{ConfigDir: filepath.Join(d, "config"), CacheDir: filepath.Join(d, "cache"), CodexDir: filepath.Join(d, "codex"), HomeDir: d})
	for _, a := range []string{"", "list", "sync", "help"} {
		if got := c.Command(context.Background(), a); strings.Contains(got, d) {
			t.Fatalf("%q leaks a path: %q", a, got)
		}
	}
}
