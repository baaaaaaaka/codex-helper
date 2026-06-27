//go:build linux

package cli

import (
	"crypto/sha256"
	"debug/buildinfo"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/managedinstall"
	"github.com/baaaaaaaka/codex-helper/internal/update"
)

const legacyUpdaterCommandPath = "github.com/baaaaaaaka/codex-helper/cmd/codex-proxy"
const legacyUpdaterModulePath = "github.com/baaaaaaaka/codex-helper"
const legacyUpdaterBridgeFromVersion = "0.1.10-rc.24"
const legacyUpdaterBridgeUntilVersion = "0.1.13-rc.7"

type legacyUpdateVersionPreflightOptions struct {
	args             []string
	executable       string
	parentExecutable string
	defaultTarget    string
	recordPath       string
	envInstallPath   string
	envInstallDir    string
	inspectParent    func(parentPath string, targetPath string) (bool, error)
	repairSelfLoop   func(candidatePath string, parentPath string, targetPath string) (bool, error)
	saveRecord       func(path string, record managedinstall.Record) error
}

type legacyUpdaterSelfLoopRepairOptions struct {
	releaseVersion func(path string, role string) (string, error)
	sameContents   func(left string, right string) (bool, error)
	copyExecutable func(src string, dst string) error
}

func legacyUpdaterVersionPreflight() error {
	if len(os.Args) != 2 || os.Args[1] != "--version" || !legacyUpdaterTempBaseCandidate(filepath.Base(os.Args[0])) {
		return nil
	}
	executable, err := helperpath.RawExecutable()
	if err != nil {
		return fmt.Errorf("inspect downloaded helper executable: %w", err)
	}
	defaultTarget, _ := managedinstall.DefaultInstallPath(managedinstall.Options{GOOS: runtime.GOOS})
	recordPath, _ := managedinstall.DefaultRecordPath()
	return legacyUpdaterVersionPreflightWithOptions(legacyUpdateVersionPreflightOptions{
		args:             append([]string(nil), os.Args...),
		executable:       executable,
		parentExecutable: fmt.Sprintf("/proc/%d/exe", os.Getppid()),
		defaultTarget:    defaultTarget,
		recordPath:       recordPath,
		envInstallPath:   os.Getenv(update.EnvInstallPath),
		envInstallDir:    os.Getenv(update.EnvInstallDir),
		inspectParent:    inspectLegacyUpdaterParent,
	})
}

func legacyUpdaterVersionPreflightWithOptions(opts legacyUpdateVersionPreflightOptions) error {
	if len(opts.args) != 2 || opts.args[1] != "--version" {
		return nil
	}
	targetPath, candidate, err := legacyUpdaterTempTarget(opts.executable)
	if !candidate {
		return nil
	}
	if err != nil {
		return err
	}
	repairSelfLoop := opts.repairSelfLoop
	if repairSelfLoop == nil {
		repairSelfLoop = repairLegacyUpdaterSelfLoop
	}
	if _, err := repairSelfLoop(opts.executable, opts.parentExecutable, targetPath); err != nil {
		return fmt.Errorf("recover legacy helper self-loop: %w", err)
	}

	record, recordExists, recordErr := loadLegacyUpdaterInstallRecord(opts.recordPath)
	dangerousEarlyAlias := firstDangerousLegacyUpdaterAlias([]string{
		strings.TrimSpace(opts.envInstallPath),
		legacyUpdaterInstallDirCandidate(opts.envInstallDir),
	}, targetPath)
	dangerousRecordOrDefaultAlias := firstDangerousLegacyUpdaterAlias(append(
		append([]string{record.TargetPath}, record.Shims...),
		strings.TrimSpace(opts.defaultTarget),
	), targetPath)
	if dangerousEarlyAlias == "" && dangerousRecordOrDefaultAlias == "" {
		return nil
	}
	if dangerousEarlyAlias == "" && recordErr == nil && recordExists && verifyLegacyUpdaterAliasPlan(targetPath, opts, record) == nil {
		return nil
	}

	inspectParent := opts.inspectParent
	if inspectParent == nil {
		inspectParent = inspectLegacyUpdaterParent
	}
	needsBridge, err := inspectParent(opts.parentExecutable, targetPath)
	if err != nil {
		return fmt.Errorf("verify legacy updater parent: %w", err)
	}
	if !needsBridge {
		return nil
	}
	if dangerousEarlyAlias != "" {
		return fmt.Errorf("legacy updater would process unsafe environment alias %s before the managed install record", dangerousEarlyAlias)
	}
	if recordErr != nil {
		return fmt.Errorf("load managed install record: %w", recordErr)
	}
	if strings.TrimSpace(opts.recordPath) == "" {
		return fmt.Errorf("managed install record path is unavailable")
	}

	shimPath := filepath.Join(filepath.Dir(targetPath), "cxp")
	if err := ensureLegacyUpdaterGuardShim(shimPath, targetPath); err != nil {
		return err
	}
	if !recordExists {
		record = managedinstall.Record{}
	}
	record.TargetPath = targetPath
	record.TargetSource = string(managedinstall.SourceCurrentExecutable)
	record.TargetState = string(managedinstall.StateManaged)
	record.GOOS = runtime.GOOS
	record.GOARCH = runtime.GOARCH
	record.UpdatedAt = ""
	record.Shims = prependLegacyUpdaterGuardShim(shimPath, record.Shims)
	saveRecord := opts.saveRecord
	if saveRecord == nil {
		saveRecord = managedinstall.SaveRecord
	}
	if err := saveRecord(opts.recordPath, record); err != nil {
		return fmt.Errorf("save managed install record guard: %w", err)
	}

	saved, err := managedinstall.LoadRecord(opts.recordPath)
	if err != nil {
		return fmt.Errorf("verify managed install record guard: %w", err)
	}
	if err := verifyLegacyUpdaterAliasPlan(targetPath, opts, saved); err != nil {
		return err
	}
	return nil
}

func legacyUpdaterTempBaseCandidate(base string) bool {
	return strings.HasPrefix(strings.TrimSpace(base), ".codex-proxy_")
}

func legacyUpdaterTempTarget(executable string) (string, bool, error) {
	executable = filepath.Clean(strings.TrimSpace(executable))
	base := filepath.Base(executable)
	if !legacyUpdaterTempBaseCandidate(base) {
		return "", false, nil
	}
	marker := "_linux_" + runtime.GOARCH + "."
	rest := strings.TrimPrefix(base, ".codex-proxy_")
	markerIndex := strings.LastIndex(rest, marker)
	if markerIndex <= 0 {
		return "", true, fmt.Errorf("downloaded helper temporary name %q does not match the updater asset format", base)
	}
	versionPart := rest[:markerIndex]
	suffix := rest[markerIndex+len(marker):]
	if !legacyUpdaterTempNamePartSafe(versionPart, true) || !legacyUpdaterTempNamePartSafe(suffix, false) {
		return "", true, fmt.Errorf("downloaded helper temporary name %q contains an unsafe version or suffix", base)
	}
	info, err := os.Lstat(executable)
	if err != nil {
		return "", true, fmt.Errorf("inspect downloaded helper temporary file: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return "", true, fmt.Errorf("downloaded helper temporary file %s is not a regular executable", executable)
	}
	targetPath := filepath.Join(filepath.Dir(executable), helperpath.BinaryName(runtime.GOOS))
	targetInfo, err := os.Lstat(targetPath)
	if err != nil {
		if os.IsNotExist(err) {
			return targetPath, true, nil
		}
		return "", true, fmt.Errorf("inspect legacy helper target %s: %w", targetPath, err)
	}
	if !targetInfo.Mode().IsRegular() || targetInfo.Mode().Perm()&0o111 == 0 {
		selfLoop, selfLoopErr := legacyUpdaterSameEntrySelfLoop(targetPath)
		if selfLoopErr != nil {
			return "", true, fmt.Errorf("inspect legacy helper target %s: %w", targetPath, selfLoopErr)
		}
		if selfLoop {
			return targetPath, true, nil
		}
		return "", true, fmt.Errorf("legacy helper target %s is not a regular executable", targetPath)
	}
	return targetPath, true, nil
}

func legacyUpdaterSameEntrySelfLoop(path string) (bool, error) {
	path = cleanAbsolutePath(path)
	if path == "" {
		return false, nil
	}
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return false, nil
	}
	target, err := os.Readlink(path)
	if err != nil {
		return false, err
	}
	if !filepath.IsAbs(target) {
		target = filepath.Join(filepath.Dir(path), target)
	}
	target = cleanAbsolutePath(target)
	if target == path {
		return true, nil
	}
	if filepath.Base(target) != filepath.Base(path) {
		return false, nil
	}
	pathParent, pathErr := filepath.EvalSymlinks(filepath.Dir(path))
	targetParent, targetErr := filepath.EvalSymlinks(filepath.Dir(target))
	if pathErr != nil || targetErr != nil {
		return false, nil
	}
	if cleanAbsolutePath(pathParent) == cleanAbsolutePath(targetParent) {
		return true, nil
	}
	pathParentInfo, pathStatErr := os.Stat(pathParent)
	targetParentInfo, targetStatErr := os.Stat(targetParent)
	return pathStatErr == nil && targetStatErr == nil && os.SameFile(pathParentInfo, targetParentInfo), nil
}

func repairLegacyUpdaterSelfLoop(candidatePath string, parentPath string, targetPath string) (bool, error) {
	return repairLegacyUpdaterSelfLoopWithOptions(candidatePath, parentPath, targetPath, legacyUpdaterSelfLoopRepairOptions{
		releaseVersion: verifiedLegacyUpdaterReleaseVersion,
		sameContents:   sameFileContents,
		copyExecutable: copyExecutableAtomically,
	})
}

func repairLegacyUpdaterSelfLoopWithOptions(candidatePath string, parentPath string, targetPath string, opts legacyUpdaterSelfLoopRepairOptions) (bool, error) {
	selfLoop, err := legacyUpdaterSameEntrySelfLoop(targetPath)
	if err != nil {
		return false, fmt.Errorf("inspect target %s: %w", targetPath, err)
	}
	if !selfLoop {
		return false, nil
	}
	if opts.releaseVersion == nil || opts.sameContents == nil || opts.copyExecutable == nil {
		return true, fmt.Errorf("self-loop recovery dependencies are unavailable")
	}
	if err := requireLegacyUpdaterRegularExecutable(candidatePath, "downloaded candidate", false); err != nil {
		return true, err
	}
	if err := requireLegacyUpdaterRegularExecutable(parentPath, "direct parent", true); err != nil {
		return true, err
	}
	previousPath := targetPath + ".prev"
	if err := requireLegacyUpdaterRegularExecutable(previousPath, "previous helper", false); err != nil {
		return true, err
	}

	candidateVersion, err := opts.releaseVersion(candidatePath, "downloaded candidate")
	if err != nil {
		return true, err
	}
	parentVersion, err := opts.releaseVersion(parentPath, "direct parent")
	if err != nil {
		return true, err
	}
	previousVersion, err := opts.releaseVersion(previousPath, "previous helper")
	if err != nil {
		return true, err
	}
	if cmp, ok := update.CompareVersions(parentVersion, previousVersion); !ok || cmp != 0 {
		return true, fmt.Errorf("direct parent version %q does not match previous helper version %q", parentVersion, previousVersion)
	}
	same, err := opts.sameContents(parentPath, previousPath)
	if err != nil {
		return true, fmt.Errorf("compare direct parent with previous helper: %w", err)
	}
	if !same {
		return true, fmt.Errorf("direct parent does not match previous helper contents")
	}
	if _, ok := update.CompareVersions(candidateVersion, candidateVersion); !ok {
		return true, fmt.Errorf("downloaded candidate has unknown release version %q", candidateVersion)
	}

	if err := opts.copyExecutable(previousPath, targetPath); err != nil {
		return true, fmt.Errorf("restore %s from %s: %w", targetPath, previousPath, err)
	}
	if err := requireLegacyUpdaterRegularExecutable(targetPath, "restored helper target", false); err != nil {
		return true, err
	}
	restoredMatches, err := opts.sameContents(parentPath, targetPath)
	if err != nil {
		return true, fmt.Errorf("verify restored helper target: %w", err)
	}
	if !restoredMatches {
		return true, fmt.Errorf("restored helper target does not match the direct parent contents")
	}
	return true, nil
}

func requireLegacyUpdaterRegularExecutable(path string, role string, followSymlink bool) error {
	var (
		info os.FileInfo
		err  error
	)
	if followSymlink {
		info, err = os.Stat(path)
	} else {
		info, err = os.Lstat(path)
	}
	if err != nil {
		return fmt.Errorf("inspect %s %s: %w", role, path, err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("%s %s is not a regular executable", role, path)
	}
	return nil
}

func verifiedLegacyUpdaterReleaseVersion(path string, role string) (string, error) {
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read %s Go build info: %w", role, err)
	}
	if info.Path != legacyUpdaterCommandPath || info.Main.Path != legacyUpdaterModulePath {
		return "", fmt.Errorf("%s is not codex-helper: command=%q module=%q", role, info.Path, info.Main.Path)
	}
	version := strings.TrimSpace(info.Main.Version)
	if _, ok := update.CompareVersions(version, version); !ok {
		return "", fmt.Errorf("%s codex-helper build has unknown module version %q", role, version)
	}
	return version, nil
}

func legacyUpdaterTempNamePartSafe(value string, allowVersionPunctuation bool) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			continue
		}
		if allowVersionPunctuation && (r == '.' || r == '+') {
			continue
		}
		return false
	}
	return true
}

func loadLegacyUpdaterInstallRecord(path string) (managedinstall.Record, bool, error) {
	if strings.TrimSpace(path) == "" {
		return managedinstall.Record{}, false, nil
	}
	record, err := managedinstall.LoadRecord(path)
	if err == nil {
		return record, true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return managedinstall.Record{}, false, nil
	}
	return managedinstall.Record{}, false, err
}

func legacyUpdaterInstallDirCandidate(value string) string {
	value = filepath.Clean(strings.TrimSpace(value))
	if value == "" || value == "." {
		return ""
	}
	base := filepath.Base(value)
	if base == helperpath.BinaryName(runtime.GOOS) || base == "cxp" {
		return value
	}
	return filepath.Join(value, helperpath.BinaryName(runtime.GOOS))
}

func firstDangerousLegacyUpdaterAlias(paths []string, targetPath string) string {
	for _, path := range paths {
		if legacyUpdaterAliasOverwritesTarget(path, targetPath) {
			return filepath.Clean(strings.TrimSpace(path))
		}
	}
	return ""
}

func legacyUpdaterAliasOverwritesTarget(path string, targetPath string) bool {
	path = cleanAbsolutePath(path)
	targetPath = cleanAbsolutePath(targetPath)
	if path == "" || targetPath == "" || path == targetPath {
		return false
	}
	if filepath.Base(path) != filepath.Base(targetPath) {
		return false
	}
	pathParent, pathErr := filepath.EvalSymlinks(filepath.Dir(path))
	targetParent, targetErr := filepath.EvalSymlinks(filepath.Dir(targetPath))
	if pathErr != nil || targetErr != nil {
		return false
	}
	if cleanAbsolutePath(pathParent) == cleanAbsolutePath(targetParent) {
		return true
	}
	pathParentInfo, pathStatErr := os.Stat(pathParent)
	targetParentInfo, targetStatErr := os.Stat(targetParent)
	return pathStatErr == nil && targetStatErr == nil && os.SameFile(pathParentInfo, targetParentInfo)
}

func cleanAbsolutePath(path string) string {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "" || path == "." {
		return ""
	}
	if abs, err := filepath.Abs(path); err == nil {
		return filepath.Clean(abs)
	}
	return path
}

func inspectLegacyUpdaterParent(parentPath string, targetPath string) (bool, error) {
	parentInfo, err := os.Stat(parentPath)
	if err != nil {
		return false, fmt.Errorf("inspect direct parent executable %s: %w", parentPath, err)
	}
	targetInfo, err := os.Stat(targetPath)
	if err != nil {
		return false, fmt.Errorf("inspect stable helper target %s: %w", targetPath, err)
	}
	info, err := buildinfo.ReadFile(parentPath)
	if err != nil {
		return false, fmt.Errorf("read direct parent Go build info: %w", err)
	}
	if info.Path != legacyUpdaterCommandPath || info.Main.Path != legacyUpdaterModulePath {
		return false, fmt.Errorf("direct parent is not codex-helper: command=%q module=%q", info.Path, info.Main.Path)
	}
	if !os.SameFile(parentInfo, targetInfo) {
		targetBuild, targetErr := buildinfo.ReadFile(targetPath)
		if targetErr != nil {
			return false, fmt.Errorf("direct parent differs from the stable target and target build info is unavailable: %w", targetErr)
		}
		if targetBuild.Path != legacyUpdaterCommandPath || targetBuild.Main.Path != legacyUpdaterModulePath {
			return false, fmt.Errorf("stable target is not codex-helper: command=%q module=%q", targetBuild.Path, targetBuild.Main.Path)
		}
	}
	needsBridge, known := legacyUpdaterVersionNeedsBridge(info.Main.Version)
	if !known {
		return false, fmt.Errorf("direct parent codex-helper build has unknown module version %q", info.Main.Version)
	}
	return needsBridge, nil
}

func legacyUpdaterVersionNeedsBridge(version string) (bool, bool) {
	// f7a90cc introduced alias convergence in rc.24; a2092c8 made its
	// location comparison safe for symlinked parent directories in rc.7.
	from, fromOK := update.CompareVersions(version, legacyUpdaterBridgeFromVersion)
	until, untilOK := update.CompareVersions(version, legacyUpdaterBridgeUntilVersion)
	if !fromOK || !untilOK {
		return false, false
	}
	return from >= 0 && until < 0, true
}

func ensureLegacyUpdaterGuardShim(shimPath string, targetPath string) error {
	info, err := os.Lstat(shimPath)
	switch {
	case err == nil && info.Mode()&os.ModeSymlink != 0:
		target, readErr := os.Readlink(shimPath)
		if readErr != nil {
			return fmt.Errorf("inspect cxp guard shim %s: %w", shimPath, readErr)
		}
		resolved := target
		if !filepath.IsAbs(resolved) {
			resolved = filepath.Join(filepath.Dir(shimPath), resolved)
		}
		if sameHelperInstallLocation(resolved, targetPath, runtime.GOOS) {
			return nil
		}
		verified, verifyErr := isCodexHelperBinary(resolved)
		if verifyErr != nil || !verified {
			return fmt.Errorf("cxp path %s is a user-owned or unverifiable symlink to %s", shimPath, target)
		}
	case err == nil && info.IsDir():
		return fmt.Errorf("cxp path %s is a directory", shimPath)
	case err == nil && info.Mode().IsRegular():
		equal, compareErr := sameFileContents(shimPath, targetPath)
		if compareErr != nil {
			return fmt.Errorf("inspect existing cxp file %s: %w", shimPath, compareErr)
		}
		if !equal {
			return fmt.Errorf("cxp path %s is a user-owned regular file", shimPath)
		}
	case err == nil:
		return fmt.Errorf("cxp path %s has unsupported file type %s", shimPath, info.Mode().Type())
	case !os.IsNotExist(err):
		return fmt.Errorf("inspect cxp path %s: %w", shimPath, err)
	}
	if err := replaceRelativeSymlinkAtomically(shimPath, helperpath.BinaryName(runtime.GOOS)); err != nil {
		return fmt.Errorf("prepare cxp guard shim %s: %w", shimPath, err)
	}
	return nil
}

func isCodexHelperBinary(path string) (bool, error) {
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		return false, err
	}
	return info.Path == legacyUpdaterCommandPath && info.Main.Path == legacyUpdaterModulePath, nil
}

func sameFileContents(left string, right string) (bool, error) {
	leftSum, err := fileSHA256(left)
	if err != nil {
		return false, err
	}
	rightSum, err := fileSHA256(right)
	if err != nil {
		return false, err
	}
	return leftSum == rightSum, nil
}

func fileSHA256(path string) ([sha256.Size]byte, error) {
	var zero [sha256.Size]byte
	f, err := os.Open(path)
	if err != nil {
		return zero, err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return zero, err
	}
	var sum [sha256.Size]byte
	copy(sum[:], h.Sum(nil))
	return sum, nil
}

func replaceRelativeSymlinkAtomically(linkPath string, relativeTarget string) error {
	dir := filepath.Dir(linkPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(linkPath)+".legacy-guard-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Remove(tmpPath); err != nil {
		return err
	}
	if err := os.Symlink(relativeTarget, tmpPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, linkPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

func prependLegacyUpdaterGuardShim(guard string, shims []string) []string {
	out := []string{guard}
	seen := map[string]bool{cleanAbsolutePath(guard): true}
	for _, shim := range shims {
		shim = filepath.Clean(strings.TrimSpace(shim))
		key := cleanAbsolutePath(shim)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, shim)
	}
	return out
}

func verifyLegacyUpdaterAliasPlan(targetPath string, opts legacyUpdateVersionPreflightOptions, record managedinstall.Record) error {
	aliases := legacyUpdaterAliases(targetPath, opts, record)
	for _, alias := range aliases {
		if legacyUpdaterAliasOverwritesTarget(alias, targetPath) {
			return fmt.Errorf("legacy updater alias plan still contains self-looping path %s", alias)
		}
	}
	guardKey := managedinstall.ComparisonKey(filepath.Join(filepath.Dir(targetPath), "cxp"), runtime.GOOS)
	targetKey := managedinstall.ComparisonKey(targetPath, runtime.GOOS)
	if guardKey == "" || targetKey == "" || guardKey != targetKey {
		return fmt.Errorf("cxp guard shim does not resolve to the stable helper target")
	}
	return nil
}

func legacyUpdaterAliases(targetPath string, opts legacyUpdateVersionPreflightOptions, record managedinstall.Record) []string {
	var aliases []string
	add := func(path string) {
		path = filepath.Clean(strings.TrimSpace(path))
		if path == "" || path == "." || cleanAbsolutePath(path) == cleanAbsolutePath(targetPath) {
			return
		}
		aliases = append(aliases, path)
		if filepath.Base(path) == helperpath.BinaryName(runtime.GOOS) {
			shim := filepath.Join(filepath.Dir(path), "cxp")
			if cleanAbsolutePath(shim) != cleanAbsolutePath(targetPath) {
				aliases = append(aliases, shim)
			}
		}
	}
	add(opts.envInstallPath)
	add(legacyUpdaterInstallDirCandidate(opts.envInstallDir))
	add(record.TargetPath)
	for _, shim := range record.Shims {
		add(shim)
	}
	add(opts.defaultTarget)

	seen := map[string]bool{}
	out := make([]string, 0, len(aliases))
	for _, alias := range aliases {
		key := managedinstall.ComparisonKey(alias, runtime.GOOS)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, alias)
	}
	return out
}
