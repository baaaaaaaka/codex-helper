package candidateupdate

import (
	"context"
	"debug/buildinfo"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"golang.org/x/mod/semver"
)

const (
	legacyRuntimeBridgeFirst = "v0.1.13-rc.28"
	legacyRuntimeBridgeLast  = "v0.1.13-rc.35"
	legacyCommandPath        = "github.com/baaaaaaaka/codex-helper/cmd/codex-proxy"
	legacyModulePath         = "github.com/baaaaaaaka/codex-helper"
)

// LegacyVersionBridge is the one-time escape hatch for runtime updaters that
// validate a stable entry with inherited CXP_RUNTIME markers. It runs only for
// the exact downloaded-candidate --version shape and never on ordinary version
// queries.
func LegacyVersionBridge(args []string, buildVersion string) error {
	if len(args) != 2 || args[1] != "--version" {
		return nil
	}
	base := filepath.Base(args[0])
	if !strings.HasPrefix(base, ".codex-proxy_") {
		return nil
	}
	current, ok := helperruntime.Current()
	if !ok || !legacyRuntimeNeedsBridge(current.Version) {
		return nil
	}
	targetVersion, ok := helperruntime.NormalizeVersion(buildVersion)
	if !ok || semver.Compare(targetVersion, legacyRuntimeBridgeLast) <= 0 {
		return nil
	}
	executable, err := helperpath.RawExecutable()
	if err != nil {
		return fmt.Errorf("inspect downloaded candidate: %w", err)
	}
	executable, err = filepath.Abs(filepath.Clean(executable))
	if err != nil {
		return err
	}
	expectedDir := filepath.Dir(helperruntime.VersionPath(current.Root, targetVersion, runtime.GOOS))
	if !samePath(filepath.Dir(executable), expectedDir) {
		return fmt.Errorf("legacy runtime bridge candidate directory %s, want %s", filepath.Dir(executable), expectedDir)
	}
	parent, err := directParentExecutable()
	if err != nil {
		return fmt.Errorf("inspect legacy updater parent: %w", err)
	}
	if err := requireSameFile(parent, current.RuntimePath, "legacy updater parent", "running immutable runtime"); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return helperruntime.WithRootLock(ctx, current.Root, func() error {
		active, err := helperruntime.ReadActive(current.Root)
		if err != nil {
			return fmt.Errorf("read active runtime before legacy bridge: %w", err)
		}
		if active != current.Version {
			return fmt.Errorf("legacy updater runtime %s is stale; active runtime is already %s", current.Version, active)
		}
		installed, err := helperruntime.InstallVersion(current.Root, executable, targetVersion, runtime.GOOS, false)
		if err != nil {
			return fmt.Errorf("prepublish bridge runtime: %w", err)
		}
		candidateHash, err := fileSHA256(installed)
		if err != nil {
			return fmt.Errorf("hash bridge runtime: %w", err)
		}
		pending := Pending{
			Schema:         ProtocolVersion,
			TargetVersion:  targetVersion,
			TargetSHA256:   candidateHash,
			PreviousActive: current.Version,
			TargetActive:   installed,
			RuntimeRoot:    current.Root,
			EntryPath:      current.EntryPath,
			RequestID:      fmt.Sprintf("legacy-%d-%s", os.Getppid(), strings.TrimPrefix(targetVersion, "v")),
		}
		if err := writePending(filepath.Join(current.Root, PendingFileName), pending); err != nil {
			return fmt.Errorf("write legacy bridge recovery state: %w", err)
		}
		if err := replaceLegacyStableEntry(executable, current.EntryPath, current.RuntimePath, current.Root); err != nil {
			return err
		}
		if current.Version != targetVersion {
			if err := helperruntime.SetPrevious(current.Root, current.Version); err != nil {
				return fmt.Errorf("record bridge previous runtime: %w", err)
			}
		}
		if err := helperruntime.Activate(current.Root, targetVersion); err != nil {
			return fmt.Errorf("activate bridge runtime: %w", err)
		}
		if same, err := sameFileContent(executable, installed); err != nil || !same {
			if err != nil {
				return fmt.Errorf("verify bridge runtime: %w", err)
			}
			return fmt.Errorf("bridge runtime contents changed during activation")
		}
		return nil
	})
}

func legacyRuntimeNeedsBridge(version string) bool {
	version, ok := helperruntime.NormalizeVersion(version)
	if !ok {
		return false
	}
	return semver.Compare(version, legacyRuntimeBridgeFirst) >= 0 && semver.Compare(version, legacyRuntimeBridgeLast) <= 0
}

func requireSameFile(left string, right string, leftRole string, rightRole string) error {
	leftInfo, err := os.Stat(left)
	if err != nil {
		return fmt.Errorf("inspect %s %s: %w", leftRole, left, err)
	}
	rightInfo, err := os.Stat(right)
	if err != nil {
		return fmt.Errorf("inspect %s %s: %w", rightRole, right, err)
	}
	if !os.SameFile(leftInfo, rightInfo) {
		return fmt.Errorf("%s %s is not %s %s", leftRole, left, rightRole, right)
	}
	return nil
}

func stableReplacementTarget(entry string, runningRuntime string, root string) (string, string, error) {
	entry = filepath.Clean(strings.TrimSpace(entry))
	info, err := os.Lstat(entry)
	if err != nil {
		return "", "", fmt.Errorf("inspect stable cxp entry %s: %w", entry, err)
	}
	target := entry
	if info.Mode()&os.ModeSymlink != 0 {
		target, err = filepath.EvalSymlinks(entry)
		if err != nil {
			return "", "", fmt.Errorf("resolve stable cxp entry %s: %w", entry, err)
		}
	}
	target, err = filepath.Abs(filepath.Clean(target))
	if err != nil {
		return "", "", err
	}
	versionsRoot := filepath.Join(filepath.Clean(root), "versions")
	if pathWithin(target, versionsRoot) {
		return "", "", fmt.Errorf("refusing to replace immutable runtime through stable entry %s", entry)
	}
	if same, err := sameFileContent(target, runningRuntime); err != nil {
		return "", "", fmt.Errorf("compare stable entry with running runtime: %w", err)
	} else if !same {
		version, verifyErr := verifiedAffectedPhysicalVersion(target)
		if verifyErr != nil {
			return "", "", fmt.Errorf("stable entry target %s does not match the running runtime and is not a verified affected launcher: %w", target, verifyErr)
		}
		if !legacyRuntimeNeedsBridge(version) {
			return "", "", fmt.Errorf("stable entry target %s has unsupported partial-state version %s", target, version)
		}
	}
	expectedHash, err := fileSHA256(target)
	if err != nil {
		return "", "", fmt.Errorf("hash stable entry target: %w", err)
	}
	return target, expectedHash, nil
}

func verifiedAffectedPhysicalVersion(path string) (string, error) {
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read Go build info: %w", err)
	}
	if info.Path != legacyCommandPath || info.Main.Path != legacyModulePath {
		return "", fmt.Errorf("unexpected command=%q module=%q", info.Path, info.Main.Path)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, path, "--version")
	cmd.Env = append(helperruntime.LauncherEnvironment(os.Environ()), helperruntime.EnvDisable+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("probe physical version: %w: %s", err, strings.TrimSpace(string(out)))
	}
	for _, field := range strings.Fields(string(out)) {
		if version, ok := helperruntime.NormalizeVersion(field); ok {
			return version, nil
		}
	}
	return "", fmt.Errorf("physical version output is invalid: %q", strings.TrimSpace(string(out)))
}

func sameFileContent(left string, right string) (bool, error) {
	leftHash, err := fileSHA256(left)
	if err != nil {
		return false, err
	}
	rightHash, err := fileSHA256(right)
	if err != nil {
		return false, err
	}
	return strings.EqualFold(leftHash, rightHash), nil
}

func copyExecutableAtomically(source string, target string, expectedHash string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	info, err := in.Stat()
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("source %s is not a regular file", source)
	}
	if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return err
	}
	out, err := os.CreateTemp(filepath.Dir(target), ".cxp-bridge-")
	if err != nil {
		return err
	}
	tmp := out.Name()
	cleanup := true
	defer func() {
		_ = out.Close()
		if cleanup {
			_ = os.Remove(tmp)
		}
	}()
	if err := out.Chmod(info.Mode().Perm() | 0o700); err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	if err := out.Sync(); err != nil {
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	currentHash, err := fileSHA256(target)
	if err != nil {
		return fmt.Errorf("revalidate stable entry before replacement: %w", err)
	}
	if !strings.EqualFold(currentHash, expectedHash) {
		return fmt.Errorf("stable entry changed while preparing replacement")
	}
	if err := os.Rename(tmp, target); err != nil {
		return err
	}
	cleanup = false
	return syncDirectory(filepath.Dir(target))
}
