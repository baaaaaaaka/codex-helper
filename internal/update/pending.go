package update

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

type BinaryVersion struct {
	Path    string
	Version string
	Output  string
}

type PendingReplacement struct {
	Path    string
	Version string
	ModTime time.Time
}

func ProbeBinaryVersion(ctx context.Context, path string, timeout time.Duration) (BinaryVersion, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return BinaryVersion{}, fmt.Errorf("binary path is empty")
	}
	info, err := os.Stat(path)
	if err != nil {
		return BinaryVersion{}, err
	}
	if info.IsDir() {
		return BinaryVersion{}, fmt.Errorf("binary path is a directory: %s", path)
	}
	cmdCtx := ctx
	cancel := func() {}
	if timeout > 0 {
		cmdCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()
	out, err := exec.CommandContext(cmdCtx, path, "--version").CombinedOutput()
	output := trimValidationOutput(string(out))
	if err != nil {
		return BinaryVersion{Path: path, Output: output}, fmt.Errorf("probe binary version: %w: %s", err, output)
	}
	version := VersionFromOutput(output)
	if version == "" {
		return BinaryVersion{Path: path, Output: output}, fmt.Errorf("probe binary version: could not parse version from %q", output)
	}
	return BinaryVersion{Path: path, Version: version, Output: output}, nil
}

func VersionFromOutput(output string) string {
	for _, field := range strings.Fields(strings.TrimSpace(output)) {
		candidate := strings.TrimPrefix(strings.TrimSpace(field), "v")
		if looksLikeVersion(candidate) {
			return candidate
		}
	}
	return ""
}

func looksLikeVersion(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	if strings.EqualFold(value, "dev") {
		return true
	}
	parts := strings.SplitN(value, ".", 3)
	if len(parts) < 2 {
		return false
	}
	for _, part := range parts[:2] {
		if part == "" {
			return false
		}
		for _, r := range part {
			if r < '0' || r > '9' {
				return false
			}
		}
	}
	return true
}

func FindPendingReplacements(installPath string) ([]PendingReplacement, error) {
	return FindPendingReplacementsForPlatform(installPath, runtime.GOOS, runtime.GOARCH)
}

func FindPendingReplacementsForPlatform(installPath string, goos string, goarch string) ([]PendingReplacement, error) {
	installPath = strings.TrimSpace(installPath)
	if installPath == "" {
		return nil, fmt.Errorf("install path is empty")
	}
	dir := filepath.Dir(installPath)
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	assetPrefix := "codex-proxy_"
	osName := strings.ToLower(strings.TrimSpace(goos))
	archName := strings.ToLower(strings.TrimSpace(goarch))
	if osName == "" || archName == "" {
		return nil, nil
	}
	assetSuffix := "_" + osName + "_" + archName
	if osName == "windows" {
		assetSuffix += ".exe"
	}
	var out []PendingReplacement
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !strings.HasPrefix(name, "."+assetPrefix) {
			continue
		}
		if strings.HasSuffix(name, ".activation.json") || strings.HasSuffix(name, ".activation.json.tmp") {
			continue
		}
		rest := strings.TrimPrefix(name, "."+assetPrefix)
		idx := strings.Index(rest, assetSuffix+".")
		if idx <= 0 {
			continue
		}
		version := strings.TrimSpace(rest[:idx])
		if version == "" {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		out = append(out, PendingReplacement{
			Path:    filepath.Join(dir, name),
			Version: version,
			ModTime: info.ModTime(),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if cmp := compareVersion(out[i].Version, out[j].Version); cmp != versionCompareInvalid && cmp != 0 {
			return cmp > 0
		}
		if out[i].ModTime.Equal(out[j].ModTime) {
			return out[i].Path > out[j].Path
		}
		return out[i].ModTime.After(out[j].ModTime)
	})
	return out, nil
}

func LatestPendingReplacement(installPath string, targetVersion string) (PendingReplacement, bool, error) {
	pending, err := FindPendingReplacements(installPath)
	if err != nil {
		return PendingReplacement{}, false, err
	}
	targetVersion = strings.TrimPrefix(strings.TrimSpace(targetVersion), "v")
	for _, candidate := range pending {
		if targetVersion == "" || strings.EqualFold(strings.TrimPrefix(candidate.Version, "v"), targetVersion) {
			return candidate, true, nil
		}
	}
	return PendingReplacement{}, false, nil
}
