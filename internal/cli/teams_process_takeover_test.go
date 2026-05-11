package cli

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

type teamsServiceCommandRunnerFunc func(context.Context, string, ...string) ([]byte, error)

func (fn teamsServiceCommandRunnerFunc) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	return fn(ctx, name, args...)
}

func TestTeamsServiceRetireLocalDuplicateProcessesFiltersAndTerminates(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
	})
	spec := teamsServiceSpec{
		Executable:   filepath.Join(tmp, "codex-proxy"),
		RegistryPath: filepath.Join(tmp, "registry.json"),
		Environment:  map[string]string{},
	}
	teamsServiceListLocalProcesses = func() ([]teamsServiceLocalProcess, error) {
		return []teamsServiceLocalProcess{
			{PID: 1001, Args: []string{"/home/alice/.cache/codex-helper/dev/bin/codex-proxy-teams-dev", "teams", "run", "--auto-service=false"}, Env: map[string]string{}},
			{PID: 1002, Args: []string{"/home/alice/go/bin/codex-proxy", "teams", "service", "watchdog", "--loop"}, Env: map[string]string{}},
			{PID: 1007, Args: []string{"/home/alice/.local/bin/cxp", "teams", "run", "--auto-service=false"}, Env: map[string]string{}},
			{PID: os.Getpid(), Args: []string{"/home/alice/go/bin/codex-proxy", "teams", "run", "--auto-service=false"}, Env: map[string]string{}},
			{PID: 1003, Args: []string{"/home/alice/go/bin/codex-proxy", "teams", "run", "--once"}, Env: map[string]string{}},
			{PID: 1004, Args: []string{"/home/alice/go/bin/codex-proxy", "proxy", "daemon"}, Env: map[string]string{}},
			{PID: 1005, Args: []string{"/home/alice/go/bin/codex-proxy", "teams", "run", "--registry", filepath.Join(tmp, "other.json")}, Env: map[string]string{}},
			{PID: 1006, Args: []string{"/home/alice/go/bin/codex-proxy", "teams", "run"}, Env: map[string]string{"CODEX_HELPER_TEAMS_PROFILE": "work"}},
			{PID: 1008, Args: []string{"/home/alice/bin/not-cxp", "teams", "run"}, Env: map[string]string{}},
		}, nil
	}
	var terminated []int
	teamsServiceTerminateLocalProcess = func(pid int, grace time.Duration) error {
		terminated = append(terminated, pid)
		if grace != 0 {
			t.Fatalf("grace = %s, want test hook zero", grace)
		}
		return nil
	}

	result, err := teamsServiceRetireLocalDuplicateProcesses(context.Background(), spec)
	if err != nil {
		t.Fatalf("retire local duplicate processes: %v", err)
	}
	if result.Matched != 3 || result.Retired != 3 {
		t.Fatalf("result = %#v, want 3 matched and retired", result)
	}
	want := []int{1001, 1002, 1007}
	if !reflect.DeepEqual(terminated, want) {
		t.Fatalf("terminated = %#v, want %#v", terminated, want)
	}
}

func TestTeamsServiceRetireLocalDuplicateProcessesDefaultRegistryTakesLegacyExplicitRegistry(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
	})
	spec := teamsServiceSpec{Executable: filepath.Join(tmp, "codex-proxy"), Environment: map[string]string{}}
	teamsServiceListLocalProcesses = func() ([]teamsServiceLocalProcess, error) {
		return []teamsServiceLocalProcess{
			{PID: 2001, Args: []string{"/home/alice/go/bin/codex-proxy", "teams", "run", "--registry", filepath.Join(tmp, "legacy-registry.json")}, Env: map[string]string{}},
		}, nil
	}
	var terminated []int
	teamsServiceTerminateLocalProcess = func(pid int, _ time.Duration) error {
		terminated = append(terminated, pid)
		return nil
	}

	result, err := teamsServiceRetireLocalDuplicateProcesses(context.Background(), spec)
	if err != nil {
		t.Fatalf("retire local duplicate processes: %v", err)
	}
	if result.Matched != 1 || result.Retired != 1 || !reflect.DeepEqual(terminated, []int{2001}) {
		t.Fatalf("result=%#v terminated=%#v, want legacy explicit registry retired", result, terminated)
	}
}

func TestTeamsServiceRetireLocalDuplicateProcessesFailureIsFatal(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
	})
	spec := teamsServiceSpec{Executable: filepath.Join(tmp, "codex-proxy"), Environment: map[string]string{}}
	teamsServiceListLocalProcesses = func() ([]teamsServiceLocalProcess, error) {
		return []teamsServiceLocalProcess{
			{PID: 3001, Args: []string{"/home/alice/go/bin/codex-proxy", "teams", "run"}, Env: map[string]string{}},
		}, nil
	}
	teamsServiceTerminateLocalProcess = func(pid int, _ time.Duration) error {
		return errors.New("access denied")
	}

	result, err := teamsServiceRetireLocalDuplicateProcesses(context.Background(), spec)
	if err == nil || !strings.Contains(err.Error(), "pid 3001") || !strings.Contains(err.Error(), "access denied") {
		t.Fatalf("error = %v, want pid-specific fatal cleanup error", err)
	}
	if result.Matched != 1 || result.Retired != 0 {
		t.Fatalf("result = %#v, want failed matched process not retired", result)
	}
}

func TestTeamsServiceBootstrapRetiresLocalDuplicateProcessesBeforeRepair(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	var events []string
	runner := teamsServiceCommandRunnerFunc(func(ctx context.Context, name string, args ...string) ([]byte, error) {
		events = append(events, "powershell:"+strings.Join(args, " "))
		return nil, nil
	})
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	teamsServiceListLocalProcesses = func() ([]teamsServiceLocalProcess, error) {
		return []teamsServiceLocalProcess{
			{PID: 4001, Args: []string{"/home/alice/.cache/codex-helper/dev/bin/codex-proxy-teams-dev", "teams", "run", "--auto-service=false"}, Env: map[string]string{}},
		}, nil
	}
	teamsServiceTerminateLocalProcess = func(pid int, _ time.Duration) error {
		events = append(events, "terminate:"+strconv.Itoa(pid))
		return nil
	}

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(filepath.Join(tmp, "registry.json")))
	cmd.SetArgs([]string{"bootstrap", "--yes"})
	cmd.SetIn(strings.NewReader(""))
	var out bytes.Buffer
	cmd.SetOut(&out)
	if err := cmd.Execute(); err != nil {
		t.Fatalf("bootstrap: %v\n%s", err, out.String())
	}
	if len(events) == 0 || events[0] != "terminate:4001" {
		t.Fatalf("events = %#v, want duplicate process retired before Scheduled Task repair", events)
	}
}

func TestTeamsServiceBootstrapFailsWhenDuplicateProcessCannotBeRetired(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	runner := teamsServiceCommandRunnerFunc(func(ctx context.Context, name string, args ...string) ([]byte, error) {
		t.Fatal("Scheduled Task repair should not run when duplicate process takeover fails")
		return nil, nil
	})
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	teamsServiceListLocalProcesses = func() ([]teamsServiceLocalProcess, error) {
		return []teamsServiceLocalProcess{
			{PID: 4501, Args: []string{"/home/alice/go/bin/codex-proxy", "teams", "run", "--auto-service=false"}, Env: map[string]string{}},
		}, nil
	}
	teamsServiceTerminateLocalProcess = func(pid int, _ time.Duration) error {
		return errors.New("still alive")
	}

	cmd := newTeamsServiceCmd(&rootOptions{}, stringPtr(filepath.Join(tmp, "registry.json")))
	cmd.SetArgs([]string{"bootstrap", "--yes"})
	var out bytes.Buffer
	cmd.SetOut(&out)
	err := cmd.Execute()
	if err == nil || !strings.Contains(err.Error(), "could not stop old local Teams helper process") || !strings.Contains(err.Error(), "pid 4501") {
		t.Fatalf("bootstrap error = %v, want duplicate process takeover failure", err)
	}
}

func TestTeamsServiceUpgradeRetiresLocalDuplicateProcessesBeforeRestart(t *testing.T) {
	lockCLITestHooks(t)

	tmp := t.TempDir()
	var events []string
	runner := teamsServiceCommandRunnerFunc(func(ctx context.Context, name string, args ...string) ([]byte, error) {
		events = append(events, "powershell:"+strings.Join(args, " "))
		return nil, nil
	})
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:           "linux",
		exe:            filepath.Join(tmp, "codex-proxy"),
		cwd:            tmp,
		windowsTaskDir: filepath.Join(tmp, "wsl-task"),
		isWSL:          true,
		wslDistro:      "Debian",
		wslLinuxUser:   "alice",
		runner:         runner,
	})
	teamsServiceListLocalProcesses = func() ([]teamsServiceLocalProcess, error) {
		return []teamsServiceLocalProcess{
			{PID: 5001, Args: []string{"/home/alice/.cache/codex-helper/dev/bin/codex-proxy-teams-dev", "teams", "run", "--auto-service=false"}, Env: map[string]string{}},
			{PID: 5002, Args: []string{"/home/alice/go/bin/codex-proxy", "teams", "service", "watchdog", "--loop"}, Env: map[string]string{}},
		}, nil
	}
	teamsServiceTerminateLocalProcess = func(pid int, _ time.Duration) error {
		events = append(events, "terminate:"+strconv.Itoa(pid))
		return nil
	}

	finalizer, err := stopTeamsServiceForHelperUpgrade(context.Background(), strings.NewReader(""), &bytes.Buffer{}, nil, stringPtr(filepath.Join(tmp, "registry.json")))
	if err != nil {
		t.Fatalf("stop service for upgrade: %v", err)
	}
	if finalizer == nil {
		t.Fatal("finalizer is nil")
	}
	if len(events) < 3 || !strings.HasPrefix(events[0], "powershell:") || events[1] != "terminate:5001" || events[2] != "terminate:5002" {
		t.Fatalf("events = %#v, want service stop before duplicate process retirement", events)
	}
}
