package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/codexbinary"
	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/userpath"
)

var (
	teamsUserPathResolver userpath.Resolver = userpath.DefaultResolver{}
	teamsFindNativeCodex                    = codexbinary.FindNativeBinaryWithRetry
	teamsCodexPackageRoot                   = codexbinary.PackageRootForWrapper
)

func resolveTeamsCodexUserPath(ctx context.Context, cfg config.Config, paths effectivePaths, extraEnv []string, workDir string) (userpath.Result, error) {
	target := userpath.TargetIdentity{Home: paths.Home}
	if identity := paths.ExecIdentity; identity != nil {
		target.UID = identity.UID
		target.GID = identity.GID
		target.Groups = append([]uint32(nil), identity.Groups...)
		target.GroupsKnown = identity.GroupsKnown
		target.Username = identity.Username
		target.Home = identity.Home
	}
	serviceEnvironment := mergeCLIEnvironment(os.Environ(), extraEnv)
	expectedSID, err := teamsUserPathExpectedSID(ctx, cfg.TeamsCodexPath.Mode, serviceEnvironment)
	if err != nil {
		return userpath.Result{}, fmt.Errorf("resolve Teams service target identity: %w", err)
	}
	target.SID = expectedSID
	target.WSLDistro, _ = explicitEnvironmentValue(serviceEnvironment, "WSL_DISTRO_NAME")
	helperExecutable, _ := explicitEnvironmentValue(extraEnv, envTeamsHelperCLIPath)
	if strings.TrimSpace(helperExecutable) == "" {
		if executable, err := teamsChildExecutablePath(); err == nil {
			helperExecutable = executable
		}
	}
	configure := func(command *exec.Cmd) error {
		updated, err := applyExecIdentity(command, command.Env, paths.ExecIdentity)
		if err != nil {
			return err
		}
		command.Env = updated
		return nil
	}
	result, err := teamsUserPathResolver.Resolve(ctx, userpath.Request{
		Policy: userpath.Policy{
			Mode:          userpath.Mode(cfg.TeamsCodexPath.Mode),
			ExplicitPath:  cfg.TeamsCodexPath.ExplicitPath,
			ShellOverride: cfg.TeamsCodexPath.ShellOverride,
		},
		Target:             target,
		ServiceEnvironment: serviceEnvironment,
		HelperExecutable:   helperExecutable,
		WorkingDir:         workDir,
		Timeout:            15 * time.Second,
		ConfigureCommand:   configure,
	})
	if err != nil {
		return userpath.Result{}, fmt.Errorf("resolve Teams Codex user PATH: %w", err)
	}
	return result, nil
}

// applyTeamsUserPathRuntime converts an npm Codex wrapper to its native binary
// whenever possible. Managed Node remains available to the installer and
// wrapper probes, but is removed from the PATH exposed to the final app-server.
func applyTeamsUserPathRuntime(runtimeContract resolvedCodexRuntime, pathResult userpath.Result, log io.Writer) resolvedCodexRuntime {
	runtimeContract.WrapperCommand = runtimeContract.Command
	// service is the exact compatibility mode for pre-policy installations.
	// Preserve the JavaScript wrapper and managed Node environment as well as
	// the service PATH; otherwise migration would still change runtime behavior
	// even though the configured PATH source was kept compatible.
	if pathResult.Mode == userpath.ModeService {
		if _, vendorPath, err := teamsFindNativeCodex(runtimeContract.Command); err == nil && strings.TrimSpace(vendorPath) != "" {
			runtimeContract.VendorPathDir = vendorPath
			runtimeContract.Environment = setEnvValue(runtimeContract.Environment, "PATH", prependPathDir(vendorPath, envValue(runtimeContract.Environment, "PATH")))
		}
		return runtimeContract
	}
	userPath := pathResult.Path
	if native, vendorPath, err := teamsFindNativeCodex(runtimeContract.Command); err == nil {
		runtimeContract.Command = native
		runtimeContract.VendorPathDir = vendorPath
		if strings.TrimSpace(vendorPath) != "" {
			userPath = prependPathDir(vendorPath, userPath)
		}
		runtimeContract.Environment = setEnvValue(runtimeContract.Environment, "PATH", userPath)
		runtimeContract.Environment = removeTeamsLauncherEnvironment(runtimeContract.Environment)
		if packageRoot, rootErr := teamsCodexPackageRoot(runtimeContract.WrapperCommand); rootErr == nil {
			runtimeContract.Environment = setEnvValue(runtimeContract.Environment, "CODEX_MANAGED_PACKAGE_ROOT", packageRoot)
			managedKey := "CODEX_MANAGED_BY_NPM"
			if strings.Contains(filepath.ToSlash(packageRoot), "/.bun/") {
				managedKey = "CODEX_MANAGED_BY_BUN"
			}
			runtimeContract.Environment = setEnvValue(runtimeContract.Environment, managedKey, "1")
		} else {
			runtimeContract.Environment = setEnvValue(runtimeContract.Environment, "CODEX_MANAGED_BY_NPM", "1")
			if log != nil {
				_, _ = fmt.Fprintf(log, "Teams Codex PATH warning: native Codex package root could not be resolved from %s: %v\n", runtimeContract.WrapperCommand, rootErr)
			}
		}
		return runtimeContract
	}
	if commandLooksNative(runtimeContract.Command) {
		runtimeContract.Environment = setEnvValue(runtimeContract.Environment, "PATH", userPath)
		runtimeContract.Environment = removeTeamsLauncherEnvironment(runtimeContract.Environment)
		return runtimeContract
	}
	if log != nil {
		_, _ = fmt.Fprintf(log, "Teams Codex PATH warning: could not resolve native binary for %s; retaining the launcher PATH for wrapper compatibility\n", runtimeContract.Command)
	}
	return runtimeContract
}

func removeTeamsLauncherEnvironment(environment []string) []string {
	blocked := []string{"CODEX_NODE_INSTALL_ROOT", "CODEX_NODE_MAJOR", "CODEX_NPM_PREFIX", "NPM_CONFIG_CACHE", "npm_config_cache", "CODEX_MANAGED_PACKAGE_ROOT", "CODEX_MANAGED_BY_NPM", "CODEX_MANAGED_BY_BUN"}
	out := make([]string, 0, len(environment))
	for _, entry := range environment {
		key, _, ok := strings.Cut(entry, "=")
		remove := false
		for _, blockedKey := range blocked {
			if ok && envKeyEqual(key, blockedKey) {
				remove = true
				break
			}
		}
		if remove {
			continue
		}
		out = append(out, entry)
	}
	return out
}

func commandLooksNative(path string) bool {
	path = strings.TrimSpace(path)
	if path == "" {
		return false
	}
	file, err := os.Open(path)
	if err != nil {
		return false
	}
	defer file.Close()
	header := make([]byte, 4)
	if _, err := io.ReadFull(file, header); err != nil {
		return false
	}
	switch string(header) {
	case "\x7fELF",
		"\xfe\xed\xfa\xce", "\xce\xfa\xed\xfe", // 32-bit Mach-O, either byte order
		"\xfe\xed\xfa\xcf", "\xcf\xfa\xed\xfe", // 64-bit Mach-O, either byte order
		"\xca\xfe\xba\xbe", "\xbe\xba\xfe\xca", // universal Mach-O
		"\xca\xfe\xba\xbf", "\xbf\xba\xfe\xca": // universal Mach-O with 64-bit arch records
		return true
	}
	return runtime.GOOS == "windows" && header[0] == 'M' && header[1] == 'Z' && strings.EqualFold(filepath.Ext(path), ".exe")
}
