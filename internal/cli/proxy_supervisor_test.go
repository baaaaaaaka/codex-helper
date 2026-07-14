package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/stack"
)

func TestProxySupervisorBoundsChildRestartAttempts(t *testing.T) {
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	inst := config.Instance{ID: "supervised-1", ProfileID: "p1", OwnerToken: "owner-1", BrokerEpoch: "epoch-1"}
	if err := store.Save(config.Config{Version: config.CurrentVersion, Instances: []config.Instance{inst}}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	oldExecutable, oldCommand, oldWait, oldNow := proxyExecutable, proxySupervisorCommand, proxySupervisorWait, proxySupervisorNow
	t.Cleanup(func() {
		proxyExecutable, proxySupervisorCommand, proxySupervisorWait, proxySupervisorNow = oldExecutable, oldCommand, oldWait, oldNow
	})
	proxyExecutable = func() (string, error) { return "/bin/sh", nil }
	starts := 0
	proxySupervisorCommand = func(string, ...string) *exec.Cmd {
		starts++
		return exec.Command("sh", "-c", "exit 1")
	}
	proxySupervisorWait = func(context.Context, time.Duration) error { return nil }
	proxySupervisorNow = func() time.Time { return time.Unix(int64(starts), 0) }

	err = runProxySupervisor(context.Background(), store, inst.ID, inst.OwnerToken)
	if err != nil {
		t.Fatalf("runProxySupervisor error = %v, want clean circuit-breaker exit", err)
	}
	if starts != proxySupervisorRestartBurst {
		t.Fatalf("child starts = %d, want %d", starts, proxySupervisorRestartBurst)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Instances[0].RecoveryBudget.Blocked {
		t.Fatalf("supervisor did not persist blocked budget: %#v", cfg.Instances[0].RecoveryBudget)
	}
}

func TestProxySupervisorStopsAfterDurableBudgetBlock(t *testing.T) {
	store, err := config.NewStore(filepath.Join(t.TempDir(), "config.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	inst := config.Instance{ID: "blocked-supervisor", ProfileID: "p1", OwnerToken: "owner-1", BrokerEpoch: "epoch-1"}
	if err := store.Save(config.Config{Version: config.CurrentVersion, Instances: []config.Instance{inst}}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	now := time.Unix(100, 0)
	for i := 0; i < proxySupervisorRestartBurst; i++ {
		blocked, err := recordProxySupervisorAttempt(store, inst.ID, inst.OwnerToken, now, errors.New("daemon exited"))
		if err != nil {
			t.Fatalf("record attempt %d: %v", i, err)
		}
		if i < proxySupervisorRestartBurst-1 && blocked {
			t.Fatalf("attempt %d unexpectedly blocked", i)
		}
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !cfg.Instances[0].RecoveryBudget.Blocked {
		t.Fatalf("budget was not durably blocked: %#v", cfg.Instances[0].RecoveryBudget)
	}

	oldExecutable, oldCommand := proxyExecutable, proxySupervisorCommand
	t.Cleanup(func() { proxyExecutable, proxySupervisorCommand = oldExecutable, oldCommand })
	proxyExecutable = func() (string, error) { return "/bin/sh", nil }
	starts := 0
	proxySupervisorCommand = func(string, ...string) *exec.Cmd {
		starts++
		return exec.Command("sh", "-c", "exit 1")
	}
	if err := runProxySupervisor(context.Background(), store, inst.ID, inst.OwnerToken); err != nil {
		t.Fatalf("blocked supervisor returned error: %v", err)
	}
	if starts != 0 {
		t.Fatalf("blocked supervisor started %d child processes", starts)
	}
}

func TestProxyStartSupervisedLaunchesSupervisorInsteadOfBareDaemon(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	if err := store.Save(config.Config{
		Version:  config.CurrentVersion,
		Profiles: []config.Profile{{ID: "p1", Name: "profile", Host: "host", Port: 22, User: "alice"}},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	oldExecutable, oldCommand := proxyExecutable, proxyCommand
	t.Cleanup(func() { proxyExecutable, proxyCommand = oldExecutable, oldCommand })
	executable := filepath.Join(t.TempDir(), "codex-proxy")
	if err := os.WriteFile(executable, []byte("helper"), 0o755); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	proxyExecutable = func() (string, error) { return executable, nil }
	var got []string
	proxyCommand = func(_ string, args ...string) *exec.Cmd {
		got = append([]string(nil), args...)
		return exec.Command("sh", "-c", "exit 0")
	}
	cmd := newProxyStartCmd(&rootOptions{configPath: store.Path()})
	cmd.SetContext(context.Background())
	cmd.SetArgs([]string{"--supervised"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	joined := strings.Join(got, " ")
	if !strings.Contains(joined, "proxy supervisor run") || strings.Contains(joined, "proxy daemon") {
		t.Fatalf("supervised launch args = %v", got)
	}
}

func TestManagedProxyDaemonLeavesOwnerRecordForSupervisorRestart(t *testing.T) {
	lockCLITestHooks(t)
	store := newTempStore(t)
	inst := config.Instance{ID: "managed-1", ProfileID: "p1", Kind: config.InstanceKindDaemon, BrokerID: "managed-1", BrokerEpoch: "epoch-1", OwnerToken: "owner-1"}
	if err := store.Save(config.Config{
		Version:   config.CurrentVersion,
		Profiles:  []config.Profile{{ID: "p1", Host: "host", Port: 22, User: "alice"}},
		Instances: []config.Instance{inst},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	oldStart := proxyStartStack
	t.Cleanup(func() { proxyStartStack = oldStart })
	fatal := errors.New("managed child failed")
	proxyStartStack = func(config.Profile, string, stack.Options) (proxyStartedStack, error) {
		ch := make(chan error, 1)
		ch <- fatal
		return proxyStartedStack{httpPort: 18080, socksPort: 19080, fatalCh: ch, close: func(context.Context) error { return nil }}, nil
	}
	err := runProxyDaemonWithOwnerTokenMode(context.Background(), store, inst.ID, inst.OwnerToken, true)
	if !errors.Is(err, fatal) {
		t.Fatalf("managed daemon error = %v, want %v", err, fatal)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(cfg.Instances) != 1 || cfg.Instances[0].OwnerToken != inst.OwnerToken {
		t.Fatalf("managed daemon removed owner record: %+v", cfg.Instances)
	}
}
