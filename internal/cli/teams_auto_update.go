package cli

import (
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/teams"
	"github.com/baaaaaaaka/codex-helper/internal/update"
)

type teamsReleaseAutoUpdater struct {
	repo              string
	includePrerelease bool
}

var teamsAutoUpdateResolveInstallPath = resolveManagedInstallPathForTeamsAutoUpdate
var teamsAutoUpdateListReleases = update.ListReleases
var teamsAutoUpdateFetchReleaseIndex = update.FetchReleaseIndex
var teamsAutoUpdateExecutable = func() (string, error) { return teamsServiceExecutable() }

func newTeamsReleaseAutoUpdater(repo string, includePrerelease bool) teams.HelperAutoUpdater {
	return teamsReleaseAutoUpdater{repo: repo, includePrerelease: includePrerelease}
}

func (u teamsReleaseAutoUpdater) Check(ctx context.Context, check teams.HelperAutoUpdateCheck) (teams.HelperAutoUpdateDecision, error) {
	now := check.Now
	if now.IsZero() {
		now = time.Now()
	}
	if check.Manual && !check.IncludePrerelease {
		return u.checkManualStable(ctx, check, now)
	}
	releases, err := u.indexedOrListedReleases(ctx)
	if err != nil {
		return teams.HelperAutoUpdateDecision{
			NextCheckAt:  now.Add(update.DefaultAutoUpdateCheckInterval),
			BackoffUntil: now.Add(update.DefaultAutoUpdateCheckInterval),
			LastError:    err.Error(),
		}, err
	}
	installedVersion := check.InstalledVersion
	if check.Manual && (strings.TrimSpace(installedVersion) == "" || strings.EqualFold(strings.TrimSpace(installedVersion), "dev")) {
		installedVersion = "0.0.0"
	}
	selected := update.SelectAutoUpdateCandidate(releases, update.AutoUpdateSelectionOptions{
		InstalledVersion:  installedVersion,
		Now:               now,
		IncludePrerelease: u.includePrerelease || check.IncludePrerelease,
		IgnorePriority:    check.Manual,
	})
	decision := teams.HelperAutoUpdateDecision{
		NextCheckAt: selected.NextCheckAt,
	}
	if selected.Candidate != nil {
		decision.Candidate = &teams.HelperAutoUpdateCandidate{
			TagName:     selected.Candidate.TagName,
			Version:     selected.Candidate.Version,
			Priority:    string(selected.Candidate.Priority),
			PublishedAt: selected.Candidate.PublishedAt,
			EligibleAt:  selected.Candidate.EligibleAt,
			Asset:       selected.Candidate.Asset,
		}
	}
	return decision, nil
}

func (u teamsReleaseAutoUpdater) indexedOrListedReleases(ctx context.Context) ([]update.GitHubRelease, error) {
	indexReleases, indexErr := teamsAutoUpdateFetchReleaseIndex(ctx, update.ReleaseIndexOptions{
		Repo:    u.repo,
		Timeout: 8 * time.Second,
	})
	if indexErr == nil {
		return indexReleases, nil
	}
	releases, listErr := teamsAutoUpdateListReleases(ctx, update.ReleaseListOptions{
		Repo:    u.repo,
		Timeout: 8 * time.Second,
	})
	if listErr != nil {
		return nil, fmt.Errorf("release index failed: %v; release list failed: %w", indexErr, listErr)
	}
	return releases, nil
}

func (u teamsReleaseAutoUpdater) checkManualStable(ctx context.Context, check teams.HelperAutoUpdateCheck, now time.Time) (teams.HelperAutoUpdateDecision, error) {
	installedVersion := check.InstalledVersion
	if strings.TrimSpace(installedVersion) == "" || strings.EqualFold(strings.TrimSpace(installedVersion), "dev") {
		installedVersion = "0.0.0"
	}
	next := now.Add(update.DefaultAutoUpdateCheckInterval)
	indexReleases, indexErr := teamsAutoUpdateFetchReleaseIndex(ctx, update.ReleaseIndexOptions{
		Repo:    u.repo,
		Timeout: 8 * time.Second,
	})
	if indexErr == nil {
		selected := update.SelectAutoUpdateCandidate(indexReleases, update.AutoUpdateSelectionOptions{
			InstalledVersion: installedVersion,
			Now:              now,
			IgnorePriority:   true,
		})
		decision := teams.HelperAutoUpdateDecision{NextCheckAt: selected.NextCheckAt}
		if selected.Candidate != nil {
			decision.Candidate = &teams.HelperAutoUpdateCandidate{
				TagName:     selected.Candidate.TagName,
				Version:     selected.Candidate.Version,
				Priority:    "manual",
				PublishedAt: selected.Candidate.PublishedAt,
				EligibleAt:  selected.Candidate.EligibleAt,
				Asset:       selected.Candidate.Asset,
			}
		}
		return decision, nil
	}
	status := checkForUpdate(ctx, update.CheckOptions{
		Repo:             u.repo,
		InstalledVersion: installedVersion,
		Timeout:          8 * time.Second,
	})
	if !status.Supported {
		err := fmt.Errorf("helper update check failed: %s", strings.TrimSpace(status.Error))
		if strings.TrimSpace(status.Error) == "" {
			err = fmt.Errorf("helper update check failed")
		}
		err = fmt.Errorf("release index failed: %v; %w", indexErr, err)
		return teams.HelperAutoUpdateDecision{
			NextCheckAt:  next,
			BackoffUntil: next,
			LastError:    err.Error(),
		}, err
	}
	decision := teams.HelperAutoUpdateDecision{NextCheckAt: next}
	if !status.UpdateAvailable {
		return decision, nil
	}
	tag := strings.TrimSpace(status.RemoteTag)
	if tag == "" {
		tag = "v" + strings.TrimPrefix(strings.TrimSpace(status.RemoteVersion), "v")
	}
	decision.Candidate = &teams.HelperAutoUpdateCandidate{
		TagName:  tag,
		Version:  strings.TrimPrefix(strings.TrimSpace(status.RemoteVersion), "v"),
		Priority: "manual",
		Asset:    status.Asset,
	}
	return decision, nil
}

func (u teamsReleaseAutoUpdater) Apply(ctx context.Context, candidate teams.HelperAutoUpdateCandidate) (teams.HelperAutoUpdateApplyResult, error) {
	return u.ApplyWithOptions(ctx, candidate, teams.HelperAutoUpdateApplyOptions{})
}

func (u teamsReleaseAutoUpdater) ApplyWithOptions(ctx context.Context, candidate teams.HelperAutoUpdateCandidate, applyOpts teams.HelperAutoUpdateApplyOptions) (teams.HelperAutoUpdateApplyResult, error) {
	if err := preflightPersistedTeamsServiceForUpdate(); err != nil {
		return teams.HelperAutoUpdateApplyResult{}, err
	}
	installPath, err := teamsAutoUpdateResolveInstallPath("")
	if err != nil {
		return teams.HelperAutoUpdateApplyResult{}, err
	}
	lock, ok, err := acquireHelperInstallLock(ctx, installPath)
	if err != nil {
		return teams.HelperAutoUpdateApplyResult{}, err
	}
	if !ok {
		return teams.HelperAutoUpdateApplyResult{}, fmt.Errorf("another helper auto-update is already updating %s", installPath)
	}
	defer func() { _ = lock.Unlock() }()
	activationPending, activationReason := teamsAutoUpdateShouldDeferActivation(installPath)
	res, err := performUpdate(ctx, update.UpdateOptions{
		Repo:               u.repo,
		Version:            candidate.TagName,
		InstallPath:        installPath,
		Timeout:            120 * time.Second,
		ValidateBinary:     true,
		PendingReplacement: teamsPendingReplacementMode(applyOpts),
	})
	if err != nil {
		return teams.HelperAutoUpdateApplyResult{}, err
	}
	activationPending, activationReason, err = teamsAutoUpdateActivationAfterApply(activationPending, activationReason, res)
	if err != nil {
		return teams.HelperAutoUpdateApplyResult{}, err
	}
	if res.RestartRequired {
		if err := ensureCXPShimForInstallPath(res.InstallPath); err != nil {
			return teams.HelperAutoUpdateApplyResult{}, err
		}
		if teamsServiceGOOS() == "windows" && strings.TrimSpace(res.PendingReplacePath) != "" {
			// Best effort: the primary activation remains authoritative. A running
			// legacy cxp.exe may still be locked, which must not invalidate an
			// otherwise recoverable pending primary update.
			_ = refreshWindowsStableCXPExecutableFromSource(res.InstallPath, res.PendingReplacePath)
		}
	} else {
		if err := finalizeHelperUpdateResult(res, io.Discard); err != nil {
			return teams.HelperAutoUpdateApplyResult{}, err
		}
	}
	installBundledSkillsFromHelper(ctx, helperUpdateExecutionPath(res), io.Discard)
	return teams.HelperAutoUpdateApplyResult{
		Version:            res.Version,
		InstallPath:        res.InstallPath,
		RestartRequired:    res.RestartRequired,
		PendingReplacePath: res.PendingReplacePath,
		ActivationPending:  activationPending,
		ActivationReason:   activationReason,
	}, nil
}

// preflightPersistedTeamsServiceForUpdate rejects service specifications that
// cannot launch before replacing or activating a helper runtime. Keep this
// deliberately structural: it does not open Teams state, run doctor, or start a
// dry-run supervisor.
func preflightPersistedTeamsServiceForUpdate() error {
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		return fmt.Errorf("select persisted Teams service backend: %w", err)
	}
	path, err := backend.Path()
	if err != nil {
		return fmt.Errorf("resolve persisted Teams service configuration path: %w", err)
	}
	if wslBackend, ok := backend.(teamsServiceWSLWindowsTaskBackend); ok {
		fallbackPath, fallbackErr := wslBackend.startupFallbackMarkerPath()
		if fallbackErr != nil {
			return fmt.Errorf("resolve persisted Teams WSL Startup fallback configuration path: %w", fallbackErr)
		}
		if _, fallbackErr := os.Stat(fallbackPath); fallbackErr == nil {
			// The WSL backend routes lifecycle actions through the Startup
			// fallback whenever its marker exists, even if a stale main Task
			// marker is also present. Preflight the effective contract.
			path = fallbackPath
		} else if !errors.Is(fallbackErr, os.ErrNotExist) {
			return fmt.Errorf(
				"inspect persisted Teams WSL Startup fallback configuration %s: %w",
				fallbackPath,
				fallbackErr,
			)
		}
	}
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("inspect persisted Teams service configuration %s: %w", path, err)
	}
	contract, err := readPersistedTeamsServiceLaunchContract(backend.ID(), path)
	if err != nil {
		return fmt.Errorf("read persisted Teams service configuration %s: %w", path, err)
	}
	workingDir := strings.TrimSpace(contract.WorkingDir)
	if workingDir == "" {
		return fmt.Errorf("persisted Teams service WorkingDir is empty")
	}
	if info, err := os.Stat(workingDir); err != nil {
		return fmt.Errorf("persisted Teams service WorkingDir %s is unavailable: %w", workingDir, err)
	} else if !info.IsDir() {
		return fmt.Errorf("persisted Teams service WorkingDir %s is not a directory", workingDir)
	}
	executable := strings.TrimSpace(contract.Executable)
	if executable == "" {
		return fmt.Errorf("persisted Teams service executable is empty; run `cxp teams service repair` before updating")
	}
	if info, err := os.Stat(executable); err != nil {
		return fmt.Errorf("persisted Teams service executable %s is unavailable: %w", executable, err)
	} else if info.IsDir() {
		return fmt.Errorf("persisted Teams service executable %s is a directory", executable)
	} else if teamsServiceGOOS() != "windows" && runtime.GOOS != "windows" && info.Mode().Perm()&0o111 == 0 {
		// A Windows test host can simulate a WSL backend, but its filesystem
		// does not expose meaningful Unix execute bits. Real WSL runs on a
		// Linux Go runtime and still takes this check.
		return fmt.Errorf("persisted Teams service executable %s is not executable", executable)
	}
	if registryPath := strings.TrimSpace(contract.RegistryPath); registryPath != "" && !filepath.IsAbs(registryPath) {
		return fmt.Errorf("persisted Teams service registry path must be absolute: %s", registryPath)
	}
	return nil
}

type persistedTeamsServiceLaunchContract struct {
	Executable   string
	WorkingDir   string
	RegistryPath string
}

func readPersistedTeamsServiceLaunchContract(backendID string, path string) (persistedTeamsServiceLaunchContract, error) {
	if backendID == teamsServiceLocalSupervisorID {
		cfg, err := readTeamsServiceLocalSupervisorConfig(path)
		if err != nil {
			return persistedTeamsServiceLaunchContract{}, err
		}
		return persistedTeamsServiceLaunchContract{
			Executable:   cfg.Spec.Executable,
			WorkingDir:   cfg.Spec.WorkingDir,
			RegistryPath: cfg.Spec.RegistryPath,
		}, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return persistedTeamsServiceLaunchContract{}, err
	}
	text := string(data)
	switch backendID {
	case "systemd-user":
		var contract persistedTeamsServiceLaunchContract
		for _, line := range strings.Split(text, "\n") {
			switch {
			case strings.HasPrefix(strings.TrimSpace(line), "WorkingDirectory="):
				contract.WorkingDir = unquotePersistedTeamsServiceValue(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "WorkingDirectory=")))
			case strings.HasPrefix(strings.TrimSpace(line), "ExecStart="):
				fields, err := splitPersistedTeamsServiceSystemdArgs(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(line), "ExecStart=")))
				if err != nil {
					return persistedTeamsServiceLaunchContract{}, err
				}
				if len(fields) > 0 {
					contract.Executable = fields[0]
				}
				contract.RegistryPath = registryArgumentValue(fields)
			}
		}
		return contract, nil
	case "launchagent":
		values := parsePersistedTeamsServicePlistStrings(text)
		arguments := values["ProgramArguments"]
		return persistedTeamsServiceLaunchContract{
			Executable:   firstString(arguments),
			WorkingDir:   firstString(values["WorkingDirectory"]),
			RegistryPath: registryArgumentValue(arguments),
		}, nil
	case "windows-task-scheduler":
		return readPersistedWindowsTaskLaunchContract(text)
	case "wsl-windows-task-scheduler":
		arguments := ""
		for _, line := range strings.Split(text, "\n") {
			if value, ok := strings.CutPrefix(strings.TrimSpace(line), "Arguments="); ok {
				arguments = value
				break
			}
		}
		args, err := splitWindowsCommandLine(arguments)
		if err != nil {
			return persistedTeamsServiceLaunchContract{}, err
		}
		contract := persistedTeamsServiceLaunchContract{RegistryPath: registryArgumentValue(args)}
		for i := 0; i+1 < len(args); i++ {
			if args[i] == "--cd" {
				contract.WorkingDir = args[i+1]
			}
		}
		executable, err := persistedWSLTeamsExecutable(args)
		if err != nil {
			return persistedTeamsServiceLaunchContract{}, err
		}
		contract.Executable = executable
		return contract, nil
	default:
		return persistedTeamsServiceLaunchContract{}, fmt.Errorf("unsupported Teams service backend %q", backendID)
	}
}

func persistedWSLTeamsExecutable(args []string) (string, error) {
	execIndex := -1
	for i, arg := range args {
		if arg == "--exec" {
			execIndex = i
			break
		}
	}
	if execIndex < 0 || execIndex+2 >= len(args) || args[execIndex+1] != "env" {
		return "", fmt.Errorf("unknown persisted WSL Teams service format; run `cxp teams service repair`")
	}
	for i := execIndex + 2; i < len(args); i++ {
		if strings.Contains(args[i], "=") {
			continue
		}
		if i+2 >= len(args) || args[i+1] != "teams" || args[i+2] != "run" {
			return "", fmt.Errorf("unknown persisted WSL Teams service command; run `cxp teams service repair`")
		}
		return args[i], nil
	}
	return "", fmt.Errorf("persisted WSL Teams service executable is missing; run `cxp teams service repair`")
}

func readPersistedWindowsTaskLaunchContract(text string) (persistedTeamsServiceLaunchContract, error) {
	command := strings.TrimSpace(persistedTeamsServiceXMLValue(text, "Command"))
	arguments := strings.TrimSpace(persistedTeamsServiceXMLValue(text, "Arguments"))
	contract := persistedTeamsServiceLaunchContract{
		WorkingDir: persistedTeamsServiceXMLValue(text, "WorkingDirectory"),
	}
	switch strings.ToLower(filepath.Base(command)) {
	case "":
		return persistedTeamsServiceLaunchContract{}, fmt.Errorf("persisted Windows Teams task command is missing; run `cxp teams service repair`")
	case "wscript.exe", "wscript":
		args, err := splitWindowsCommandLine(arguments)
		if err != nil || len(args) != 3 ||
			!strings.EqualFold(args[0], "//B") ||
			!strings.EqualFold(args[1], "//Nologo") {
			return persistedTeamsServiceLaunchContract{}, fmt.Errorf("unknown persisted Windows Teams launcher format; run `cxp teams service repair`")
		}
		psPath, err := persistedWindowsPowerShellPathFromVBS(args[2])
		if err != nil {
			return persistedTeamsServiceLaunchContract{}, err
		}
		return persistedWindowsPowerShellLaunchContract(psPath, contract)
	case "powershell.exe", "powershell":
		executable, argumentLine, err := persistedTeamsExecutableFromPowerShell(arguments)
		if err != nil {
			return persistedTeamsServiceLaunchContract{}, err
		}
		contract.Executable = executable
		if args, splitErr := splitWindowsCommandLine(argumentLine); splitErr == nil {
			contract.RegistryPath = registryArgumentValue(args)
		}
		return contract, nil
	default:
		contract.Executable = command
		if args, err := splitWindowsCommandLine(arguments); err == nil {
			contract.RegistryPath = registryArgumentValue(args)
		}
		return contract, nil
	}
}

func persistedWindowsPowerShellPathFromVBS(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read persisted Windows Teams VBS launcher %s: %w", path, err)
	}
	const marker = `-File " & Q("`
	text := string(data)
	start := strings.Index(text, marker)
	if start < 0 {
		return "", fmt.Errorf("unknown persisted Windows Teams VBS launcher format; run `cxp teams service repair`")
	}
	start += len(marker)
	end := strings.Index(text[start:], `")`)
	if end < 0 {
		return "", fmt.Errorf("unknown persisted Windows Teams VBS launcher format; run `cxp teams service repair`")
	}
	return strings.ReplaceAll(text[start:start+end], `""`, `"`), nil
}

func persistedWindowsPowerShellLaunchContract(path string, contract persistedTeamsServiceLaunchContract) (persistedTeamsServiceLaunchContract, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return persistedTeamsServiceLaunchContract{}, fmt.Errorf("read persisted Windows Teams PowerShell launcher %s: %w", path, err)
	}
	executable, argumentLine, err := persistedTeamsExecutableFromPowerShell(string(data))
	if err != nil {
		return persistedTeamsServiceLaunchContract{}, err
	}
	contract.Executable = executable
	if args, splitErr := splitWindowsCommandLine(argumentLine); splitErr == nil {
		contract.RegistryPath = registryArgumentValue(args)
	}
	return contract, nil
}

func persistedTeamsExecutableFromPowerShell(text string) (string, string, error) {
	executable, ok := persistedPowerShellSingleQuotedValue(text, "Start-Process -FilePath ")
	if !ok {
		return "", "", fmt.Errorf("unknown persisted Windows Teams PowerShell launcher format; run `cxp teams service repair`")
	}
	argumentLine, _ := persistedPowerShellSingleQuotedValue(text, "$argumentLine = ")
	return executable, argumentLine, nil
}

func persistedPowerShellSingleQuotedValue(text string, marker string) (string, bool) {
	start := strings.Index(text, marker)
	if start < 0 {
		return "", false
	}
	rest := strings.TrimSpace(text[start+len(marker):])
	if rest == "" || rest[0] != '\'' {
		return "", false
	}
	var value strings.Builder
	for i := 1; i < len(rest); i++ {
		if rest[i] != '\'' {
			value.WriteByte(rest[i])
			continue
		}
		if i+1 < len(rest) && rest[i+1] == '\'' {
			value.WriteByte('\'')
			i++
			continue
		}
		return value.String(), true
	}
	return "", false
}

func splitPersistedTeamsServiceSystemdArgs(line string) ([]string, error) {
	var out []string
	for line = strings.TrimSpace(line); line != ""; line = strings.TrimSpace(line) {
		if line[0] == '"' {
			quoted, err := strconv.QuotedPrefix(line)
			if err != nil {
				return nil, fmt.Errorf("parse systemd ExecStart: %w", err)
			}
			value, err := strconv.Unquote(quoted)
			if err != nil {
				return nil, fmt.Errorf("parse systemd ExecStart: %w", err)
			}
			out = append(out, value)
			line = line[len(quoted):]
			continue
		}
		end := strings.IndexAny(line, " \t")
		if end < 0 {
			out = append(out, line)
			break
		}
		out = append(out, line[:end])
		line = line[end:]
	}
	return out, nil
}

func unquotePersistedTeamsServiceValue(value string) string {
	if unquoted, err := strconv.Unquote(value); err == nil {
		return unquoted
	}
	return value
}

func parsePersistedTeamsServicePlistStrings(text string) map[string][]string {
	out := make(map[string][]string)
	var key string
	inArray := false
	for _, raw := range strings.Split(text, "\n") {
		line := strings.TrimSpace(raw)
		if strings.HasPrefix(line, "<key>") && strings.HasSuffix(line, "</key>") {
			key = html.UnescapeString(strings.TrimSuffix(strings.TrimPrefix(line, "<key>"), "</key>"))
			inArray = false
			continue
		}
		if line == "<array>" {
			inArray = true
			continue
		}
		if line == "</array>" {
			inArray = false
			key = ""
			continue
		}
		if key != "" && strings.HasPrefix(line, "<string>") && strings.HasSuffix(line, "</string>") {
			value := html.UnescapeString(strings.TrimSuffix(strings.TrimPrefix(line, "<string>"), "</string>"))
			out[key] = append(out[key], value)
			if !inArray {
				key = ""
			}
		}
	}
	return out
}

func persistedTeamsServiceXMLValue(text string, tag string) string {
	open := "<" + tag + ">"
	close := "</" + tag + ">"
	start := strings.Index(text, open)
	if start < 0 {
		return ""
	}
	start += len(open)
	end := strings.Index(text[start:], close)
	if end < 0 {
		return ""
	}
	return html.UnescapeString(strings.TrimSpace(text[start : start+end]))
}

func registryArgumentValue(args []string) string {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == "--registry" {
			return args[i+1]
		}
	}
	return ""
}

func firstString(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func teamsAutoUpdateActivationAfterApply(preUpdatePending bool, preUpdateReason string, res update.ApplyResult) (bool, string, error) {
	if !res.RuntimeActivated {
		return preUpdatePending, preUpdateReason, nil
	}
	if res.RestartRequired || strings.TrimSpace(res.PendingReplacePath) != "" {
		return false, "", fmt.Errorf(
			"managed runtime update returned contradictory activation state: runtime_activated=%t restart_required=%t pending_replace_path=%q",
			res.RuntimeActivated,
			res.RestartRequired,
			res.PendingReplacePath,
		)
	}
	// RuntimeActivated is the authoritative postcondition for an immutable
	// runtime update. finalizeHelperUpdateResult verifies the published runtime
	// exactly and requires the stable compatibility entry to remain runnable;
	// the entry's physical version string may be older than an explicitly
	// selected prerelease.
	// The pre-update executable can legitimately remain the old immutable
	// runtime until the normal fresh-launch restart crosses the stable entry.
	return false, "", nil
}

func teamsPendingReplacementMode(applyOpts teams.HelperAutoUpdateApplyOptions) update.PendingReplacementMode {
	if applyOpts.OwnsPendingReplacement && teamsServiceGOOS() == "windows" {
		return update.PendingReplacementReturnOnly
	}
	return update.PendingReplacementScheduleDeferredMove
}

func teamsAutoUpdateShouldDeferActivation(stableInstallPath string) (bool, string) {
	raw, err := teamsAutoUpdateExecutable()
	if err != nil {
		return true, "helper update installed, but activation is pending because the running helper executable path could not be inspected: " + err.Error()
	}
	resolved, err := helperpath.StableRunnablePathFromSources(raw, teamsServiceArgv0(), helperpath.Options{GOOS: teamsServiceGOOS()})
	if err != nil {
		class := helperpath.Classify(raw)
		if class.Transient {
			return true, "helper update installed to " + stableInstallPath + ", but activation is pending because the running helper executable is transient: " + class.Reason
		}
		return true, "helper update installed, but activation is pending because the running helper executable path is not stable: " + err.Error()
	}
	class := helperpath.Classify(raw)
	if class.Transient {
		return true, "helper update installed to " + stableInstallPath + ", but activation is pending because the running helper executable is transient: " + class.Reason
	}
	if strings.TrimSpace(stableInstallPath) != "" && !sameHelperInstallLocation(resolved.Path, stableInstallPath, teamsServiceGOOS()) {
		return true, "helper update installed to " + stableInstallPath + ", but activation is pending because the running helper executable is " + resolved.Path
	}
	return false, ""
}

func sameHelperExecutablePath(a string, b string, goos string) bool {
	a = filepath.Clean(strings.TrimSpace(a))
	b = filepath.Clean(strings.TrimSpace(b))
	if a == "" || b == "" {
		return a == b
	}
	if strings.EqualFold(strings.TrimSpace(goos), "windows") {
		return strings.EqualFold(a, b)
	}
	return a == b
}
