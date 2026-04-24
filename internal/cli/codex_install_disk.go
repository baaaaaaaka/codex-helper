package cli

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

const (
	codexInstallDiskMinFreeKBDefault int64 = 524288
	envCodexInstallMinFreeKB               = "CODEX_PROXY_CODEX_INSTALL_MIN_FREE_KB"
)

var codexInstallDiskFreeBytes = platformDiskFreeBytes

type codexInstallDiskTarget struct {
	label string
	path  string
}

func ensureCodexInstallDiskSpace(out io.Writer, installerEnv []string, extraTargets []codexInstallDiskTarget) error {
	return ensureCodexInstallDiskSpaceForTargets(out, installerEnv, codexInstallDiskTargets(installerEnv, extraTargets, true))
}

func ensureCodexInstallDiskSpaceForTargets(out io.Writer, installerEnv []string, targets []codexInstallDiskTarget) error {
	minKB := codexInstallMinFreeKB(installerEnv)
	if minKB <= 0 {
		return nil
	}
	minBytes := uint64(minKB) * 1024
	seen := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		path := strings.TrimSpace(target.path)
		if path == "" {
			continue
		}
		key := normalizeExecutablePath(path)
		if runtime.GOOS == "windows" {
			key = strings.ToLower(key)
		}
		if key == "" {
			continue
		}
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}

		freeBytes, err := codexInstallDiskFreeBytes(path)
		if err != nil {
			if out != nil {
				_, _ = fmt.Fprintf(out, "warning: could not reliably check free disk space for %s (%s): %v\n", target.label, path, err)
			}
			continue
		}
		if freeBytes < minBytes {
			reason := fmt.Sprintf(
				"Not enough disk space for %s (%s): %s available, need at least %s.",
				target.label,
				path,
				formatMiB(freeBytes),
				formatMiB(minBytes),
			)
			writeCodexInstallFailureBanner(out, reason)
			return fmt.Errorf("not enough disk space for codex install: %s", reason)
		}
	}
	return nil
}

func codexInstallMinFreeKB(installerEnv []string) int64 {
	raw := strings.TrimSpace(envValue(installerEnv, envCodexInstallMinFreeKB))
	if raw == "" {
		return codexInstallDiskMinFreeKBDefault
	}
	parsed, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return codexInstallDiskMinFreeKBDefault
	}
	return parsed
}

func codexInstallDiskTargets(installerEnv []string, extraTargets []codexInstallDiskTarget, includeManaged bool) []codexInstallDiskTarget {
	targets := make([]codexInstallDiskTarget, 0, 5+len(extraTargets))
	if tmp := strings.TrimSpace(envTempDir(installerEnv)); tmp != "" {
		targets = append(targets, codexInstallDiskTarget{label: "temporary directory", path: tmp})
	}
	for _, target := range extraTargets {
		targets = append(targets, target)
	}
	if !includeManaged {
		return targets
	}
	if npmPrefix := managedCodexInstallPrefix(installerEnv); npmPrefix != "" {
		targets = append(targets, codexInstallDiskTarget{label: "managed npm prefix", path: npmPrefix})
	}
	if nodeRoot := managedNodeInstallRoot(installerEnv); nodeRoot != "" {
		targets = append(targets, codexInstallDiskTarget{label: "managed Node.js install root", path: nodeRoot})
	}
	return targets
}

func managedCodexInstallPrefix(installerEnv []string) string {
	if prefix := strings.TrimSpace(envValue(installerEnv, "CODEX_NPM_PREFIX")); prefix != "" {
		return prefix
	}
	if runtime.GOOS == "windows" {
		base := strings.TrimSpace(envValue(installerEnv, "LOCALAPPDATA"))
		if base == "" {
			base = strings.TrimSpace(envTempDir(installerEnv))
		}
		if base == "" {
			return ""
		}
		return filepath.Join(base, "codex-proxy", "npm-global")
	}
	home := strings.TrimSpace(envValue(installerEnv, "HOME"))
	if home == "" {
		home = preferredHomeDir()
	}
	if home == "" {
		return ""
	}
	return filepath.Join(home, ".local", "share", "codex-proxy", "npm-global")
}

func managedNodeInstallRoot(installerEnv []string) string {
	if root := strings.TrimSpace(envValue(installerEnv, "CODEX_NODE_INSTALL_ROOT")); root != "" {
		return root
	}
	if runtime.GOOS == "windows" {
		base := strings.TrimSpace(envValue(installerEnv, "LOCALAPPDATA"))
		if base == "" {
			base = strings.TrimSpace(envTempDir(installerEnv))
		}
		if base == "" {
			return ""
		}
		return filepath.Join(base, "codex-proxy", "node")
	}
	home := strings.TrimSpace(envValue(installerEnv, "HOME"))
	if home == "" {
		home = preferredHomeDir()
	}
	if home == "" {
		return ""
	}
	return filepath.Join(home, ".cache", "codex-proxy", "node")
}

func existingDiskCheckPath(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("empty path")
	}
	if !filepath.IsAbs(path) {
		if abs, err := filepath.Abs(path); err == nil {
			path = abs
		}
	}
	path = filepath.Clean(path)
	for {
		if _, err := os.Stat(path); err == nil {
			return path, nil
		} else if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(path)
		if parent == path {
			return "", fmt.Errorf("no existing ancestor for %s", path)
		}
		path = parent
	}
}

func writeCodexInstallFailureBanner(out io.Writer, reason string) {
	if out == nil {
		return
	}
	_, _ = fmt.Fprintln(out)
	_, _ = fmt.Fprintln(out, "============================================================")
	_, _ = fmt.Fprintln(out, "  CODEX CLI INSTALL FAILED")
	_, _ = fmt.Fprintln(out, "============================================================")
	_, _ = fmt.Fprintf(out, "Reason: %s\n", reason)
}

func formatMiB(bytes uint64) string {
	mib := bytes / (1024 * 1024)
	return fmt.Sprintf("%d MiB", mib)
}
