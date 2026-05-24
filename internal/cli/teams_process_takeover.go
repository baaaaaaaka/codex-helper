package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type teamsServiceLocalProcess struct {
	PID  int
	Args []string
	Env  map[string]string
}

type teamsServiceLocalProcessRetireResult struct {
	Matched int
	Retired int
}

var (
	teamsServiceListLocalProcesses     = defaultTeamsServiceListLocalProcesses
	teamsServiceTerminateLocalProcess  = defaultTeamsServiceTerminateLocalProcess
	teamsServiceLocalProcessGraceDelay = 3 * time.Second
)

func teamsServiceRetireLocalDuplicateProcesses(ctx context.Context, spec teamsServiceSpec) (teamsServiceLocalProcessRetireResult, error) {
	return teamsServiceRetireLocalProcesses(ctx, spec, map[string]bool{"run": true, "watchdog": true})
}

func teamsServiceRetireLocalBridgeProcesses(ctx context.Context, spec teamsServiceSpec) (teamsServiceLocalProcessRetireResult, error) {
	return teamsServiceRetireLocalProcesses(ctx, spec, map[string]bool{"run": true})
}

func teamsServiceRetireLocalProcesses(ctx context.Context, spec teamsServiceSpec, kinds map[string]bool) (teamsServiceLocalProcessRetireResult, error) {
	var result teamsServiceLocalProcessRetireResult
	if ctx == nil {
		ctx = context.Background()
	}
	if teamsServiceGOOS() != "linux" || !teamsServiceIsWSL() {
		return result, nil
	}
	processes, err := teamsServiceListLocalProcesses()
	if err != nil {
		return result, err
	}
	var failures []string
	for _, proc := range processes {
		if ctx.Err() != nil {
			return result, ctx.Err()
		}
		if !teamsServiceShouldRetireLocalProcess(proc, spec, kinds) {
			continue
		}
		result.Matched++
		if err := teamsServiceTerminateLocalProcess(proc.PID, teamsServiceLocalProcessGraceDelay); err != nil {
			failures = append(failures, fmt.Sprintf("pid %d: %v", proc.PID, err))
			continue
		}
		result.Retired++
	}
	if len(failures) > 0 {
		return result, fmt.Errorf("could not stop old local Teams helper process(es): %s", strings.Join(failures, "; "))
	}
	return result, nil
}

func teamsServiceShouldRetireLocalProcess(proc teamsServiceLocalProcess, spec teamsServiceSpec, kinds map[string]bool) bool {
	if proc.PID <= 0 || proc.PID == os.Getpid() {
		return false
	}
	kind := teamsServiceLocalProcessKind(proc.Args)
	if kind == "" || !kinds[kind] {
		return false
	}
	if teamsServiceArgsContainFlag(proc.Args, "--once") {
		return false
	}
	if !teamsServiceLocalProcessProfilesMatch(proc.Env, spec.Environment) {
		return false
	}
	return teamsServiceLocalProcessRegistryMatches(proc.Args, spec.RegistryPath)
}

func teamsServiceLocalProcessKind(args []string) string {
	for i, arg := range args {
		if arg != "teams" || i+1 >= len(args) || !teamsServiceLooksLikeCodexProxyArgs(args, i) {
			continue
		}
		switch args[i+1] {
		case "run", "listen":
			return "run"
		case "service":
			if i+2 < len(args) && args[i+2] == "watchdog" {
				return "watchdog"
			}
		}
	}
	return ""
}

func teamsServiceLooksLikeCodexProxyArgs(args []string, teamsIndex int) bool {
	if teamsIndex <= 0 || teamsIndex > len(args) {
		return false
	}
	exe := filepath.Base(args[teamsIndex-1])
	exe = strings.ToLower(strings.TrimSpace(exe))
	return strings.Contains(exe, "codex-proxy") || exe == "cxp" || exe == "cxp.exe"
}

func teamsServiceLocalProcessProfilesMatch(procEnv map[string]string, specEnv map[string]string) bool {
	for _, key := range []string{"CODEX_HELPER_TEAMS_PROFILE", "CODEX_HELPER_TEAMS_AUTH_PROFILE"} {
		want := teamsServiceProfileValue(specEnv[key])
		got := teamsServiceProfileValue(procEnv[key])
		if want != got {
			return false
		}
	}
	return true
}

func teamsServiceProfileValue(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "default"
	}
	return value
}

func teamsServiceLocalProcessRegistryMatches(args []string, current string) bool {
	current = teamsServiceCleanRegistryPath(current)
	other := teamsServiceCleanRegistryPath(teamsServiceRegistryArg(args))
	if current != "" && other != "" {
		return current == other
	}
	return true
}

func teamsServiceRegistryArg(args []string) string {
	for i := 0; i < len(args); i++ {
		arg := strings.TrimSpace(args[i])
		if arg == "--registry" && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(arg, "--registry=") {
			return strings.TrimPrefix(arg, "--registry=")
		}
	}
	return ""
}

func teamsServiceArgsContainFlag(args []string, flag string) bool {
	for _, arg := range args {
		if arg == flag || strings.HasPrefix(arg, flag+"=") {
			return true
		}
	}
	return false
}

func teamsServiceCleanRegistryPath(path string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	return filepath.Clean(path)
}

func defaultTeamsServiceListLocalProcesses() ([]teamsServiceLocalProcess, error) {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return nil, nil
	}
	out := make([]teamsServiceLocalProcess, 0)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		pid, err := strconv.Atoi(entry.Name())
		if err != nil || pid <= 0 {
			continue
		}
		args, err := readProcNULFileFields(filepath.Join("/proc", entry.Name(), "cmdline"))
		if err != nil || len(args) == 0 {
			continue
		}
		envFields, _ := readProcNULFileFields(filepath.Join("/proc", entry.Name(), "environ"))
		out = append(out, teamsServiceLocalProcess{PID: pid, Args: args, Env: envMapFromFields(envFields)})
	}
	return out, nil
}

func readProcNULFileFields(path string) ([]string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	parts := bytes.Split(raw, []byte{0})
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if len(part) == 0 {
			continue
		}
		out = append(out, string(part))
	}
	return out, nil
}

func envMapFromFields(fields []string) map[string]string {
	out := make(map[string]string, len(fields))
	for _, field := range fields {
		key, value, ok := strings.Cut(field, "=")
		if !ok {
			continue
		}
		out[key] = value
	}
	return out
}

func defaultTeamsServiceTerminateLocalProcess(pid int, grace time.Duration) error {
	if pid <= 0 || pid == os.Getpid() {
		return nil
	}
	process, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	return terminateProcess(process, grace)
}
