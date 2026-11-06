package codex

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A changed config file replaces the idle server of a key at the next
// AcquireCurrent (SPEC R-24, F-TRN-12); a leased server is never replaced
// under its lease, and an unchanged file keeps the warm server.
func TestAcquireCurrentReplacesIdleServerAfterConfigChange(t *testing.T) {
	_, url := newBrain(t)
	home := t.TempDir()
	cfg := fakeConfig(t, url, home)
	cfg.ConfigFile = filepath.Join(home, "config.toml")
	write := func(s string) {
		t.Helper()
		if err := os.WriteFile(cfg.ConfigFile, []byte(s), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write("[mcp_servers.alpha]\ncommand = \"alpha\"\n")
	p := NewPool(PoolConfig{Idle: 10 * time.Minute})
	defer p.Close(5 * time.Second)
	ctx := tctx(t)

	l1, err := p.AcquireCurrent(ctx, "conv-1", cfg)
	if err != nil {
		t.Fatal(err)
	}
	first := l1.Server()
	l1.Release()
	l2, err := p.AcquireCurrent(ctx, "conv-1", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if l2.Server() != first {
		t.Fatal("unchanged config: the warm server was replaced")
	}

	// Changed while leased (a turn runs): kept, not interrupted.
	write("[mcp_servers.alpha]\ncommand = \"alpha\"\n[mcp_servers.beta]\ncommand = \"beta\"\n")
	l3, err := p.AcquireCurrent(ctx, "conv-1", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if l3.Server() != first || first.Exited() {
		t.Fatal("a leased server was replaced")
	}
	l3.Release()
	l2.Release()

	// Idle now: the next turn start gets a new server; the old one exited.
	l4, err := p.AcquireCurrent(ctx, "conv-1", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if l4.Server() == first || !first.Exited() {
		t.Fatalf("stale idle server kept (same=%v exited=%v)", l4.Server() == first, first.Exited())
	}
	second := l4.Server()
	l4.Release()

	// Plain Acquire (forks, lookups) never replaces a server.
	write("# removed\n")
	l5, err := p.Acquire(ctx, "conv-1", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if l5.Server() != second {
		t.Fatal("Acquire replaced a server")
	}
	l5.Release()

	// A removed file is a change too.
	if err := os.Remove(cfg.ConfigFile); err != nil {
		t.Fatal(err)
	}
	l6, err := p.AcquireCurrent(ctx, "conv-1", cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer l6.Release()
	if l6.Server() == second {
		t.Fatal("removed config: the server was kept")
	}
	if p.Size() != 1 {
		t.Fatalf("pool size %d, want 1", p.Size())
	}
}

func TestConfigStamp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.toml")
	if configStamp("") != "" {
		t.Fatal("unwatched file has a stamp")
	}
	absent := configStamp(path)
	if err := os.WriteFile(path, []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	a := configStamp(path)
	if err := os.WriteFile(path, []byte("b"), 0o600); err != nil {
		t.Fatal(err)
	}
	b := configStamp(path)
	if absent == a || a == b || absent != "absent" {
		t.Fatalf("stamps absent=%q a=%q b=%q", absent, a, b)
	}
}
