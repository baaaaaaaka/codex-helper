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

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
)

type teamsServiceLocalProcess struct {
	PID       int
	StartTime string
	Args      []string
	Env       map[string]string
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
	if teamsServiceGOOS() != "linux" {
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
	return teamsServiceLocalProcessMatchesSpec(proc, spec, kinds)
}

func teamsServiceLocalProcessMatchesSpec(proc teamsServiceLocalProcess, spec teamsServiceSpec, kinds map[string]bool) bool {
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
	if !teamsServiceLocalProcessStateRootMatches(proc.Env, spec.Environment) {
		return false
	}
	return teamsServiceLocalProcessRegistryMatches(proc.Args, proc.Env, spec.RegistryPath, spec.Environment)
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

func teamsServiceLocalProcessStateRootMatches(procEnv map[string]string, specEnv map[string]string) bool {
	current, currentOK := teamsServiceLocalProcessStateRoot(specEnv)
	other, otherOK := teamsServiceLocalProcessStateRoot(procEnv)
	return currentOK && otherOK && current == other
}

func teamsServiceLocalProcessStateRoot(env map[string]string) (string, bool) {
	home := strings.TrimSpace(env["HOME"])
	if home == "" {
		home, _ = os.UserHomeDir()
	}
	if override := strings.TrimSpace(env[appdirs.EnvStateDir]); override != "" {
		return teamsServiceLocalProcessAbsoluteStateRoot(override, home)
	}
	if base := strings.TrimSpace(env["XDG_STATE_HOME"]); base != "" {
		base, ok := teamsServiceLocalProcessAbsoluteStateRoot(base, home)
		if !ok {
			return "", false
		}
		return filepath.Join(base, appdirs.AppName), true
	}
	home, ok := teamsServiceLocalProcessAbsoluteStateRoot(home, home)
	if !ok {
		return "", false
	}
	return filepath.Join(home, ".local", "state", appdirs.AppName), true
}

func teamsServiceLocalProcessAbsoluteStateRoot(path string, home string) (string, bool) {
	path = strings.TrimSpace(path)
	home = strings.TrimSpace(home)
	switch {
	case path == "~" && home != "":
		path = home
	case strings.HasPrefix(path, "~/") && home != "":
		path = filepath.Join(home, strings.TrimPrefix(path, "~/"))
	}
	if path == "" || !filepath.IsAbs(path) {
		return "", false
	}
	return filepath.Clean(path), true
}

func teamsServiceLocalProcessRegistryMatches(args []string, procEnv map[string]string, current string, specEnv map[string]string) bool {
	currentCandidates := teamsServiceLocalProcessRegistryCandidates(current, specEnv)
	otherCandidates := teamsServiceLocalProcessRegistryCandidates(teamsServiceRegistryArg(args), procEnv)
	for _, currentCandidate := range currentCandidates {
		for _, otherCandidate := range otherCandidates {
			if currentCandidate == otherCandidate {
				return true
			}
		}
	}
	return false
}

func teamsServiceLocalProcessRegistryCandidates(explicit string, env map[string]string) []string {
	explicit = strings.TrimSpace(explicit)
	if explicit != "" {
		if !filepath.IsAbs(explicit) {
			return nil
		}
		return []string{filepath.Clean(explicit)}
	}
	stateRoot, ok := teamsServiceLocalProcessStateRoot(env)
	if !ok {
		return nil
	}
	candidates := []string{filepath.Join(stateRoot, "teams", "registry.json")}
	if legacyRoot, ok := teamsServiceLocalProcessLegacyCacheRoot(env); ok {
		candidates = append(candidates, filepath.Join(legacyRoot, appdirs.AppName, "teams-registry.json"))
	}
	return candidates
}

func teamsServiceLocalProcessLegacyCacheRoot(env map[string]string) (string, bool) {
	home := strings.TrimSpace(env["HOME"])
	if home == "" {
		home, _ = os.UserHomeDir()
	}
	if base := strings.TrimSpace(env["XDG_CACHE_HOME"]); base != "" {
		return teamsServiceLocalProcessAbsoluteStateRoot(base, home)
	}
	home, ok := teamsServiceLocalProcessAbsoluteStateRoot(home, home)
	if !ok {
		return "", false
	}
	return filepath.Join(home, ".cache"), true
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
		return nil, err
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
		// Exact takeover validation reads start time only for the selected owner
		// PID. Avoid another /proc read for every unrelated process during the
		// service scans that share this process inventory.
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
