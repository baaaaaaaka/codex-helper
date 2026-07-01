package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"github.com/baaaaaaaka/codex-helper/internal/managedinstall"
	"github.com/baaaaaaaka/codex-helper/internal/update"
)

var materializeManagedTeamsInstallTarget = defaultMaterializeManagedTeamsInstallTarget

func resolveManagedInstallPathForCLI(explicit string) (string, error) {
	target, err := resolveManagedInstallTargetForCLI(explicit)
	if err != nil {
		return "", err
	}
	return target.Path, nil
}

func resolveManagedInstallTargetForCLI(explicit string) (managedinstall.Target, error) {
	raw, _ := executablePath()
	recordPath, _ := managedinstall.DefaultRecordPath()
	if strings.TrimSpace(explicit) != "" {
		target, err := managedinstall.Resolve(managedinstall.Options{
			ExplicitPath:  explicit,
			RawExecutable: raw,
			Argv0:         restartArgv0(),
			GOOS:          runtime.GOOS,
		})
		if err != nil {
			return managedinstall.Target{}, err
		}
		return target, nil
	}

	var warnings []string
	if target, ok := resolveCurrentHelperInstallTargetForCLI(raw, restartArgv0(), recordPath, &warnings); ok {
		if canonical, ok := canonicalManagedInstallTargetForSameLocation(target.Path, recordPath, &warnings); ok {
			return canonical, nil
		}
		return target, nil
	}
	if target, ok := resolveKnownHelperInstallCandidateForCLI(os.Getenv(update.EnvInstallPath), managedinstall.SourceEnvInstallPath, managedinstall.StateExplicit, update.EnvInstallPath, false, recordPath, &warnings); ok {
		return target, nil
	}
	if envDirCandidate := legacyInstallDirCandidateForCLI(os.Getenv(update.EnvInstallDir)); strings.TrimSpace(envDirCandidate) != "" {
		if target, ok := resolveKnownHelperInstallCandidateForCLI(envDirCandidate, managedinstall.SourceEnvInstallDir, managedinstall.StateExplicit, update.EnvInstallDir, false, recordPath, &warnings); ok {
			return target, nil
		}
	}
	if strings.TrimSpace(recordPath) != "" {
		if record, err := managedinstall.LoadRecord(recordPath); err == nil && strings.TrimSpace(record.TargetPath) != "" {
			if target, ok := resolveKnownHelperInstallCandidateForCLI(record.TargetPath, managedinstall.SourceRecord, managedinstall.StateManaged, "install record", false, recordPath, &warnings); ok {
				return target, nil
			}
		} else if err != nil && !os.IsNotExist(err) {
			warnings = append(warnings, "install record ignored: "+err.Error())
		}
	}
	defaultPath, defaultErr := managedinstall.DefaultInstallPath(managedinstall.Options{GOOS: runtime.GOOS})
	if defaultErr == nil {
		if target, ok := resolveKnownHelperInstallCandidateForCLI(defaultPath, managedinstall.SourceDefault, managedinstall.StateManaged, "default per-user install target", true, recordPath, &warnings); ok {
			return target, nil
		}
	} else {
		warnings = append(warnings, "default install target unavailable: "+defaultErr.Error())
	}

	if len(warnings) > 0 {
		return managedinstall.Target{}, fmt.Errorf("resolve managed install target: %s", strings.Join(warnings, "; "))
	}
	return managedinstall.Target{}, fmt.Errorf("resolve managed install target: no candidate paths")
}

func resolveCurrentHelperInstallTargetForCLI(raw string, argv0 string, recordPath string, warnings *[]string) (managedinstall.Target, bool) {
	for _, source := range []struct {
		path   string
		reason string
	}{
		{path: raw, reason: "current executable"},
		{path: argv0, reason: "argv0 fallback"},
	} {
		for _, candidate := range currentHelperInstallCandidatesForCLI(source.path) {
			if target, ok := resolveKnownHelperInstallCandidateForCLI(candidate, managedinstall.SourceCurrentExecutable, managedinstall.StateUnmanagedCurrentExec, source.reason, false, recordPath, warnings); ok {
				return target, true
			}
		}
	}
	return managedinstall.Target{}, false
}

type managedInstallPathCandidate struct {
	path         string
	source       managedinstall.Source
	state        managedinstall.TargetState
	reason       string
	allowMissing bool
}

func canonicalManagedInstallTargetForSameLocation(installPath string, recordPath string, warnings *[]string) (managedinstall.Target, bool) {
	for _, candidate := range managedInstallPathCandidatesForCLI(recordPath) {
		if !managedinstall.IsCanonicalTargetPath(candidate.path, runtime.GOOS) {
			continue
		}
		if !sameHelperInstallLocation(candidate.path, installPath, runtime.GOOS) {
			continue
		}
		target, ok := resolveKnownHelperInstallCandidateForCLI(candidate.path, candidate.source, candidate.state, candidate.reason, candidate.allowMissing, recordPath, warnings)
		if ok {
			return target, true
		}
	}
	return managedinstall.Target{}, false
}

func managedInstallPathCandidatesForCLI(recordPath string) []managedInstallPathCandidate {
	var out []managedInstallPathCandidate
	if defaultPath, err := managedinstall.DefaultInstallPath(managedinstall.Options{GOOS: runtime.GOOS}); err == nil {
		out = append(out, managedInstallPathCandidate{
			path:   defaultPath,
			source: managedinstall.SourceDefault,
			state:  managedinstall.StateManaged,
			reason: "default per-user install target",
		})
	}
	if strings.TrimSpace(recordPath) != "" {
		if record, err := managedinstall.LoadRecord(recordPath); err == nil && strings.TrimSpace(record.TargetPath) != "" {
			out = append(out, managedInstallPathCandidate{
				path:   record.TargetPath,
				source: managedinstall.SourceRecord,
				state:  managedinstall.StateManaged,
				reason: "install record",
			})
		}
	}
	if envPath := strings.TrimSpace(os.Getenv(update.EnvInstallPath)); envPath != "" {
		out = append(out, managedInstallPathCandidate{
			path:   envPath,
			source: managedinstall.SourceEnvInstallPath,
			state:  managedinstall.StateExplicit,
			reason: update.EnvInstallPath,
		})
	}
	if envDirCandidate := legacyInstallDirCandidateForCLI(os.Getenv(update.EnvInstallDir)); strings.TrimSpace(envDirCandidate) != "" {
		out = append(out, managedInstallPathCandidate{
			path:   envDirCandidate,
			source: managedinstall.SourceEnvInstallDir,
			state:  managedinstall.StateExplicit,
			reason: update.EnvInstallDir,
		})
	}
	return out
}

func canonicalManagedInstallPathForSameLocation(installPath string, recordPath string) string {
	installPath = managedinstall.CanonicalTargetPathForEntry(installPath, runtime.GOOS)
	for _, candidate := range managedInstallPathCandidatesForCLI(recordPath) {
		if !managedinstall.IsCanonicalTargetPath(candidate.path, runtime.GOOS) {
			continue
		}
		if sameHelperInstallLocation(candidate.path, installPath, runtime.GOOS) {
			return strings.TrimSpace(candidate.path)
		}
	}
	return installPath
}

func currentHelperInstallCandidatesForCLI(path string) []string {
	return currentHelperInstallCandidatesForGOOS(path, runtime.GOOS, os.Stat)
}

func currentHelperInstallCandidatesForGOOS(path string, goos string, stat func(string) (os.FileInfo, error)) []string {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil
	}
	resolved, err := helperpath.StableRunnablePathFromSources(path, "", helperpath.Options{GOOS: goos, Stat: stat})
	if err != nil {
		return []string{managedinstall.CanonicalTargetPathForEntry(path, goos)}
	}
	candidate := resolved.Path
	var out []string
	base := filepath.Base(candidate)
	if managedinstall.IsShimEntryPath(candidate, goos) || strings.EqualFold(base, "cxp.exe") {
		out = append(out, filepath.Join(filepath.Dir(candidate), helperpath.BinaryName(goos)))
		return dedupeStringsForGOOS(out, goos)
	}
	out = append(out, candidate)
	return dedupeStringsForGOOS(out, goos)
}

func resolveKnownHelperInstallCandidateForCLI(path string, source managedinstall.Source, state managedinstall.TargetState, reason string, allowMissing bool, recordPath string, warnings *[]string) (managedinstall.Target, bool) {
	path = strings.TrimSpace(path)
	if path == "" {
		return managedinstall.Target{}, false
	}
	resolved, err := helperpath.StableInstallTargetFromSources(path, "", "", "", helperpath.Options{GOOS: runtime.GOOS})
	if err != nil {
		*warnings = append(*warnings, err.Error())
		return managedinstall.Target{}, false
	}
	resolved.Path = managedinstall.CanonicalTargetPathForEntry(resolved.Path, runtime.GOOS)
	probe := helperpath.ProbePath(resolved.Path, helperpath.Options{GOOS: runtime.GOOS})
	if !probe.Exists {
		if !allowMissing {
			*warnings = append(*warnings, fmt.Sprintf("install target %s does not exist", resolved.Path))
			return managedinstall.Target{}, false
		}
		return managedinstall.Target{
			Path:          resolved.Path,
			Source:        source,
			State:         state,
			Reason:        reason,
			ComparisonKey: managedinstall.ComparisonKey(resolved.Path, runtime.GOOS),
			RecordPath:    recordPath,
			Warnings:      append([]string(nil), (*warnings)...),
		}, true
	}
	if probe.IsDir {
		*warnings = append(*warnings, fmt.Sprintf("install target %s is a directory", resolved.Path))
		return managedinstall.Target{}, false
	}
	if !probe.Executable {
		*warnings = append(*warnings, fmt.Sprintf("install target %s is not executable", resolved.Path))
		return managedinstall.Target{}, false
	}
	if !probe.PlausibleHelperEntry {
		*warnings = append(*warnings, fmt.Sprintf("install target %s is not a known helper entry", resolved.Path))
		return managedinstall.Target{}, false
	}
	target, err := resolveRunnableHelperInstallCandidate(resolved.Path, source, state, reason, recordPath, runtime.GOOS, os.Stat)
	if err != nil {
		*warnings = append(*warnings, fmt.Sprintf("install target %s is not a runnable codex-helper binary: %v", resolved.Path, err))
		return managedinstall.Target{}, false
	}
	target.Warnings = append([]string(nil), (*warnings)...)
	return target, true
}

func resolveRunnableHelperInstallCandidate(path string, source managedinstall.Source, state managedinstall.TargetState, reason string, recordPath string, goos string, stat func(string) (os.FileInfo, error)) (managedinstall.Target, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return managedinstall.Target{}, fmt.Errorf("empty install target")
	}
	resolved, err := helperpath.StableInstallTargetFromSources(path, "", "", "", helperpath.Options{GOOS: goos, Stat: stat})
	if err != nil {
		return managedinstall.Target{}, err
	}
	resolved.Path = managedinstall.CanonicalTargetPathForEntry(resolved.Path, goos)
	probe := helperpath.ProbePath(resolved.Path, helperpath.Options{GOOS: goos, Stat: stat})
	if !probe.Exists {
		return managedinstall.Target{}, fmt.Errorf("install target %s does not exist", resolved.Path)
	}
	if probe.IsDir {
		return managedinstall.Target{}, fmt.Errorf("install target %s is a directory", resolved.Path)
	}
	if !probe.Executable {
		return managedinstall.Target{}, fmt.Errorf("install target %s is not executable", resolved.Path)
	}
	if !probe.PlausibleHelperEntry {
		return managedinstall.Target{}, fmt.Errorf("install target %s is not a known helper entry", resolved.Path)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := update.ProbeBinaryVersion(ctx, resolved.Path, 5*time.Second); err != nil {
		return managedinstall.Target{}, err
	}
	return managedinstall.Target{
		Path:          resolved.Path,
		Source:        source,
		State:         state,
		Reason:        reason,
		ComparisonKey: managedinstall.ComparisonKey(resolved.Path, goos),
		RecordPath:    recordPath,
	}, nil
}

func legacyInstallDirCandidateForCLI(value string) string {
	return legacyInstallDirCandidateForGOOS(value, runtime.GOOS, os.Stat)
}

func legacyInstallDirCandidateForGOOS(value string, goos string, stat func(string) (os.FileInfo, error)) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	base := filepath.Base(filepath.Clean(value))
	probe := helperpath.ProbePath(value, helperpath.Options{GOOS: goos, Stat: stat})
	if probe.PlausibleHelperEntry ||
		strings.EqualFold(base, helperpath.BinaryName(goos)) ||
		strings.EqualFold(base, "cxp") ||
		strings.EqualFold(base, "cxp.exe") {
		return value
	}
	return filepath.Join(value, helperpath.BinaryName(goos))
}

func dedupeStrings(values []string) []string {
	return dedupeStringsForGOOS(values, runtime.GOOS)
}

func dedupeStringsForGOOS(values []string, goos string) []string {
	seen := map[string]bool{}
	var out []string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		key := managedinstall.ComparisonKey(value, goos)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, value)
	}
	return out
}

func resolveManagedInstallPathForTeams(explicit string) (string, error) {
	target, err := resolveManagedTeamsInstallTarget(explicit, false, true)
	if err != nil {
		return "", err
	}
	return target.Path, nil
}

func resolveManagedInstallPathForTeamsAutoUpdate(explicit string) (string, error) {
	if strings.TrimSpace(explicit) != "" {
		return resolveManagedInstallPathForTeams(explicit)
	}
	if err := repairManagedTeamsInstallSelfLoopsBeforeAutoUpdate(); err != nil {
		return "", err
	}
	if target, ok := resolveCurrentHelperInstallTargetForTeamsAutoUpdate(); ok {
		var warnings []string
		if canonical, ok := canonicalManagedTeamsInstallTargetForSameLocation(target.Path, target.RecordPath, &warnings); ok {
			return canonical.Path, nil
		}
		return target.Path, nil
	}
	return resolveManagedInstallPathForTeams(explicit)
}

func resolveCurrentHelperInstallTargetForTeamsAutoUpdate() (managedinstall.Target, bool) {
	raw, _ := teamsServiceExecutable()
	argv0 := teamsServiceArgv0()
	goos := teamsServiceGOOS()
	stat := teamsServiceStat
	recordPath, _ := managedinstall.DefaultRecordPath()
	for _, source := range []struct {
		path   string
		reason string
	}{
		{path: raw, reason: "running Teams helper executable"},
		{path: argv0, reason: "Teams helper argv0 fallback"},
	} {
		for _, candidate := range currentHelperInstallCandidatesForGOOS(source.path, goos, stat) {
			target, err := resolveRunnableHelperInstallCandidate(candidate, managedinstall.SourceCurrentExecutable, managedinstall.StateUnmanagedCurrentExec, source.reason, recordPath, goos, stat)
			if err == nil {
				return target, true
			}
		}
	}
	return managedinstall.Target{}, false
}

func resolveManagedTeamsServiceExecutable(requireExisting bool) (managedinstall.Target, error) {
	allowMissingDefault := !requireExisting && !strings.EqualFold(teamsServiceGOOS(), "windows") && managedTeamsMaterializationSourceAvailable()
	target, err := resolveManagedTeamsInstallTarget("", requireExisting, allowMissingDefault)
	if err != nil {
		if raw, rawErr := teamsServiceExecutable(); rawErr == nil {
			if class := helperpath.Classify(raw); class.Kind == helperpath.KindGoBuildTemp {
				return managedinstall.Target{}, fmt.Errorf("cannot install Teams service from temporary helper executable path %q: %s", class.Clean, class.Reason)
			}
		}
	}
	return target, err
}

func managedTeamsMaterializationSourceAvailable() bool {
	raw, err := teamsServiceExecutable()
	if err != nil || strings.TrimSpace(raw) == "" {
		return false
	}
	running, err := helperpath.StableRunnablePathFromSources(raw, teamsServiceArgv0(), helperpath.Options{GOOS: teamsServiceGOOS(), Stat: teamsServiceStat})
	if err != nil {
		return false
	}
	probe := helperpath.ProbePath(running.Path, helperpath.Options{GOOS: teamsServiceGOOS(), Stat: teamsServiceStat})
	if !probe.Exists || probe.IsDir || !probe.Executable || !probe.PlausibleHelperEntry {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = update.ProbeBinaryVersion(ctx, running.Path, 5*time.Second)
	return err == nil
}

func resolveManagedTeamsInstallTarget(explicit string, requireExisting bool, allowMissingDefault bool) (managedinstall.Target, error) {
	raw, _ := teamsServiceExecutable()
	opts := managedinstall.Options{
		ExplicitPath:                explicit,
		EnvPath:                     os.Getenv(update.EnvInstallPath),
		EnvDir:                      os.Getenv(update.EnvInstallDir),
		RawExecutable:               raw,
		Argv0:                       teamsServiceArgv0(),
		GOOS:                        teamsServiceGOOS(),
		RequireExisting:             requireExisting,
		AllowMissingDefault:         allowMissingDefault,
		FallbackOnInvalidEnvPath:    true,
		PreferRecordBeforeLegacyEnv: true,
		Stat:                        teamsServiceStat,
	}
	target, err := managedinstall.Resolve(opts)
	if err != nil {
		return managedinstall.Target{}, err
	}
	if teamsManagedInstallTargetShouldSkipNonRunnableEnv(target, explicit) {
		opts.EnvPath = ""
		opts.EnvDir = ""
		return managedinstall.Resolve(opts)
	}
	if teamsManagedInstallTargetShouldPreferDefaultOverGoBinRecord(target, explicit) {
		if defaultTarget, ok := resolveManagedTeamsDefaultInstallTarget(requireExisting, allowMissingDefault, target.RecordPath); ok {
			return defaultTarget, nil
		}
	}
	return target, nil
}

func teamsManagedInstallTargetShouldPreferDefaultOverGoBinRecord(target managedinstall.Target, explicit string) bool {
	if strings.TrimSpace(explicit) != "" {
		return false
	}
	if target.Source != managedinstall.SourceRecord {
		return false
	}
	if !teamsManagedInstallPathLooksLikeHomeGoBin(target.Path, teamsServiceGOOS()) {
		return false
	}
	if strings.TrimSpace(target.RecordPath) == "" {
		return true
	}
	record, err := managedinstall.LoadRecord(target.RecordPath)
	if err != nil {
		return true
	}
	source := strings.TrimSpace(record.TargetSource)
	return source == "" || source == string(managedinstall.SourceCurrentExecutable)
}

func teamsManagedInstallPathLooksLikeHomeGoBin(path string, goos string) bool {
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return false
	}
	return sameHelperExecutablePath(path, filepath.Join(home, "go", "bin", helperpath.BinaryName(goos)), goos)
}

func resolveManagedTeamsDefaultInstallTarget(requireExisting bool, allowMissingDefault bool, recordPath string) (managedinstall.Target, bool) {
	goos := teamsServiceGOOS()
	defaultPath, err := managedinstall.DefaultInstallPath(managedinstall.Options{GOOS: goos})
	if err != nil {
		return managedinstall.Target{}, false
	}
	resolved, err := helperpath.StableInstallTargetFromSources(defaultPath, "", "", "", helperpath.Options{GOOS: goos, Stat: teamsServiceStat})
	if err != nil {
		return managedinstall.Target{}, false
	}
	probe := helperpath.ProbePath(resolved.Path, helperpath.Options{GOOS: goos, Stat: teamsServiceStat})
	if !probe.PlausibleHelperEntry {
		return managedinstall.Target{}, false
	}
	if requireExisting || !allowMissingDefault {
		if !probe.Exists || probe.IsDir || !probe.Executable {
			return managedinstall.Target{}, false
		}
	}
	return managedinstall.Target{
		Path:          resolved.Path,
		Source:        managedinstall.SourceDefault,
		State:         managedinstall.StateManaged,
		Reason:        "default per-user install target",
		ComparisonKey: managedinstall.ComparisonKey(resolved.Path, goos),
		RecordPath:    recordPath,
	}, true
}

func teamsManagedInstallTargetShouldSkipNonRunnableEnv(target managedinstall.Target, explicit string) bool {
	if strings.TrimSpace(explicit) != "" {
		return false
	}
	if target.Source != managedinstall.SourceEnvInstallPath && target.Source != managedinstall.SourceEnvInstallDir {
		return false
	}
	_, err := resolveRunnableHelperInstallCandidate(target.Path, target.Source, target.State, target.Reason, target.RecordPath, teamsServiceGOOS(), teamsServiceStat)
	return err != nil
}

func canonicalManagedTeamsInstallTargetForSameLocation(installPath string, recordPath string, warnings *[]string) (managedinstall.Target, bool) {
	for _, candidate := range managedTeamsInstallPathCandidates(recordPath) {
		if !managedinstall.IsCanonicalTargetPath(candidate.path, teamsServiceGOOS()) {
			continue
		}
		if !sameHelperInstallLocation(candidate.path, installPath, teamsServiceGOOS()) {
			continue
		}
		target, err := resolveRunnableHelperInstallCandidate(candidate.path, candidate.source, candidate.state, candidate.reason, recordPath, teamsServiceGOOS(), teamsServiceStat)
		if err != nil {
			if warnings != nil {
				*warnings = append(*warnings, fmt.Sprintf("install target %s is not a runnable codex-helper binary: %v", candidate.path, err))
			}
			continue
		}
		if warnings != nil {
			target.Warnings = append(target.Warnings, (*warnings)...)
		}
		return target, true
	}
	return managedinstall.Target{}, false
}

func managedTeamsInstallPathCandidates(recordPath string) []managedInstallPathCandidate {
	goos := teamsServiceGOOS()
	var out []managedInstallPathCandidate
	if defaultPath, err := managedinstall.DefaultInstallPath(managedinstall.Options{GOOS: goos}); err == nil {
		out = append(out, managedInstallPathCandidate{
			path:   defaultPath,
			source: managedinstall.SourceDefault,
			state:  managedinstall.StateManaged,
			reason: "default per-user install target",
		})
	}
	if strings.TrimSpace(recordPath) != "" {
		if record, err := managedinstall.LoadRecord(recordPath); err == nil && strings.TrimSpace(record.TargetPath) != "" {
			out = append(out, managedInstallPathCandidate{
				path:   record.TargetPath,
				source: managedinstall.SourceRecord,
				state:  managedinstall.StateManaged,
				reason: "install record",
			})
		}
	}
	if envPath := strings.TrimSpace(os.Getenv(update.EnvInstallPath)); envPath != "" {
		out = append(out, managedInstallPathCandidate{
			path:   envPath,
			source: managedinstall.SourceEnvInstallPath,
			state:  managedinstall.StateExplicit,
			reason: update.EnvInstallPath,
		})
	}
	if envDirCandidate := legacyInstallDirCandidateForGOOS(os.Getenv(update.EnvInstallDir), goos, teamsServiceStat); strings.TrimSpace(envDirCandidate) != "" {
		out = append(out, managedInstallPathCandidate{
			path:   envDirCandidate,
			source: managedinstall.SourceEnvInstallDir,
			state:  managedinstall.StateExplicit,
			reason: update.EnvInstallDir,
		})
	}
	return out
}

func repairManagedTeamsInstallSelfLoopsBeforeAutoUpdate() error {
	recordPath, _ := managedinstall.DefaultRecordPath()
	var errs []string
	for _, candidate := range managedTeamsInstallPathCandidates(recordPath) {
		repaired, err := repairHelperInstallSelfLoopFromPrevious(candidate.path, teamsServiceGOOS())
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		if repaired {
			break
		}
	}
	if len(errs) > 0 {
		if _, ok := resolveCurrentHelperInstallTargetForTeamsAutoUpdate(); ok {
			return nil
		}
		return fmt.Errorf("repair managed helper self-loop before update: %s", strings.Join(errs, "; "))
	}
	return nil
}

func defaultMaterializeManagedTeamsInstallTarget(ctx context.Context, target managedinstall.Target) error {
	if target.State != managedinstall.StateManaged {
		return nil
	}
	if target.Source != managedinstall.SourceDefault && target.Source != managedinstall.SourceRecord {
		return nil
	}
	if strings.EqualFold(teamsServiceGOOS(), "windows") {
		return nil
	}
	raw, err := teamsServiceExecutable()
	if err != nil || strings.TrimSpace(raw) == "" {
		return nil
	}
	running, err := helperpath.StableRunnablePathFromSources(raw, teamsServiceArgv0(), helperpath.Options{GOOS: teamsServiceGOOS(), Stat: teamsServiceStat})
	if err != nil {
		return nil
	}
	targetIsSymlink := false
	if info, err := os.Lstat(target.Path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		targetIsSymlink = true
	}
	if sameHelperInstallLocation(running.Path, target.Path, teamsServiceGOOS()) && !targetIsSymlink {
		return nil
	}
	runningProbe := helperpath.ProbePath(running.Path, helperpath.Options{GOOS: teamsServiceGOOS(), Stat: teamsServiceStat})
	if !runningProbe.Exists || runningProbe.IsDir || !runningProbe.Executable || !runningProbe.PlausibleHelperEntry {
		return nil
	}
	targetProbe := helperpath.ProbePath(target.Path, helperpath.Options{GOOS: teamsServiceGOOS(), Stat: teamsServiceStat})
	if targetProbe.IsDir || !targetProbe.PlausibleHelperEntry {
		return nil
	}
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	runningVersion, runningErr := update.ProbeBinaryVersion(probeCtx, running.Path, 5*time.Second)
	if runningErr != nil {
		return nil
	}
	shouldCopy := targetIsSymlink || !targetProbe.Exists || !targetProbe.Executable
	targetVersion := update.BinaryVersion{}
	if !shouldCopy {
		var targetErr error
		targetVersion, targetErr = update.ProbeBinaryVersion(probeCtx, target.Path, 5*time.Second)
		shouldCopy = targetErr != nil
		if targetErr == nil {
			cmp, ok := update.CompareVersions(runningVersion.Version, targetVersion.Version)
			shouldCopy = ok && cmp > 0
		}
	}
	shims := materializedManagedInstallShims(target.Path)
	if !shouldCopy {
		if err := ensureCXPShimForInstallPath(target.Path); err != nil {
			return err
		}
		if strings.TrimSpace(targetVersion.Version) != "" {
			saveManagedInstallRecordBestEffort(target, targetVersion.Version, shims)
		}
		return nil
	}
	if err := copyExecutableAtomically(running.Path, target.Path); err != nil {
		return fmt.Errorf("materialize managed Teams install target %s from running helper %s: %w", target.Path, running.Path, err)
	}
	for _, shim := range shims {
		if err := materializeManagedInstallShim(ctx, running.Path, target.Path, shim, runningVersion.Version); err != nil {
			return err
		}
	}
	saveManagedInstallRecordBestEffort(target, runningVersion.Version, shims)
	return nil
}

func materializedManagedInstallShims(targetPath string) []string {
	if strings.EqualFold(teamsServiceGOOS(), "windows") {
		return nil
	}
	targetPath = managedinstall.CanonicalTargetPathForEntry(targetPath, teamsServiceGOOS())
	return []string{filepath.Join(filepath.Dir(targetPath), "cxp")}
}

func finalizeHelperEntrypointsAfterUpgrade(installPath string, version string, out io.Writer) error {
	if err := ensureCXPShimForInstallPath(installPath); err != nil {
		_, _ = fmt.Fprintf(out, "Warning: failed to install cxp shim after upgrade: %v\n", err)
	}
	if strings.EqualFold(runtime.GOOS, "windows") {
		if err := refreshWindowsStableCXPExecutable(installPath); err != nil {
			return fmt.Errorf("refresh stable cxp executable after legacy upgrade: %w", err)
		}
	}
	for _, err := range repairKnownHelperEntrypointsForInstallPath(installPath) {
		_, _ = fmt.Fprintf(out, "Warning: failed to unify helper entrypoint after upgrade: %v\n", err)
	}
	if strings.EqualFold(runtime.GOOS, "windows") {
		for _, err := range migrateWindowsPowerShellCXPProfiles() {
			_, _ = fmt.Fprintf(out, "Warning: failed to migrate a legacy PowerShell cxp alias: %v\n", err)
		}
	}
	saveCLIManagedInstallRecordBestEffort(installPath, version)
	if err := verifyCXPEntrypointAfterUpgrade(installPath, version); err != nil {
		return err
	}
	return nil
}

func finalizeHelperUpdateResult(res update.ApplyResult, out io.Writer) error {
	if !res.RuntimeActivated {
		return finalizeHelperEntrypointsAfterUpgrade(res.InstallPath, res.Version, out)
	}
	if strings.EqualFold(runtime.GOOS, "windows") {
		if err := ensureCXPShimForInstallPath(res.InstallPath); err != nil {
			return fmt.Errorf("finalize stable cxp compatibility entrypoint: %w", err)
		}
		for _, err := range migrateWindowsPowerShellCXPProfiles() {
			_, _ = fmt.Fprintf(out, "Warning: failed to migrate a legacy PowerShell cxp alias: %v\n", err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := verifyHelperEntrypointVersion(ctx, res.RuntimePath, res.Version, "published cxp runtime"); err != nil {
		return err
	}
	if err := verifyFreshHelperEntrypointVersion(ctx, res.InstallPath, res.Version, "stable cxp entrypoint"); err != nil {
		return err
	}
	// Keep the legacy install record valid for old updaters while all new
	// executions converge through the stable cxp entry and active runtime.
	saveCLIManagedInstallRecordBestEffort(res.InstallPath, res.Version)
	return nil
}

func helperUpdateExecutionPath(res update.ApplyResult) string {
	if strings.TrimSpace(res.RuntimePath) != "" {
		return res.RuntimePath
	}
	return firstNonEmptyString(res.PendingReplacePath, res.InstallPath)
}

type helperEntrypointAlias struct {
	path        string
	description string
	create      bool
}

func repairKnownHelperEntrypointsForInstallPath(installPath string) []error {
	installPath = strings.TrimSpace(installPath)
	if installPath == "" {
		return nil
	}
	var errs []error
	for _, alias := range knownHelperEntrypointAliasesForInstallPath(installPath) {
		if err := repairHelperEntrypointAlias(installPath, alias); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

func knownHelperEntrypointAliasesForInstallPath(installPath string) []helperEntrypointAlias {
	var out []helperEntrypointAlias
	add := func(path string, description string, create bool) {
		path = strings.TrimSpace(path)
		if path == "" {
			return
		}
		if sameHelperInstallLocation(path, installPath, runtime.GOOS) {
			return
		}
		out = append(out, helperEntrypointAlias{path: path, description: description, create: create})
		if strings.EqualFold(filepath.Base(path), helperpath.BinaryName(runtime.GOOS)) {
			shimName := "cxp"
			if strings.EqualFold(runtime.GOOS, "windows") {
				shimName = "cxp.cmd"
			}
			shimPath := filepath.Join(filepath.Dir(path), shimName)
			if !sameHelperInstallLocation(shimPath, installPath, runtime.GOOS) {
				out = append(out, helperEntrypointAlias{path: shimPath, description: description + " shim", create: create})
			}
		}
	}
	add(os.Getenv(update.EnvInstallPath), update.EnvInstallPath, true)
	if envDirCandidate := legacyInstallDirCandidateForCLI(os.Getenv(update.EnvInstallDir)); envDirCandidate != "" {
		add(envDirCandidate, update.EnvInstallDir, true)
	}
	if recordPath, err := managedinstall.DefaultRecordPath(); err == nil {
		if record, err := managedinstall.LoadRecord(recordPath); err == nil {
			add(record.TargetPath, "install record", true)
			for _, shim := range record.Shims {
				add(shim, "install record shim", true)
			}
		}
	}
	if defaultPath, err := managedinstall.DefaultInstallPath(managedinstall.Options{GOOS: runtime.GOOS}); err == nil {
		add(defaultPath, "default per-user install target", true)
	}
	if raw, err := executablePath(); err == nil {
		for _, candidate := range currentHelperInstallCandidatesForCLI(raw) {
			add(candidate, "current executable", false)
		}
	}
	for _, candidate := range currentHelperInstallCandidatesForCLI(restartArgv0()) {
		add(candidate, "argv0 fallback", false)
	}
	return dedupeHelperEntrypointAliases(out)
}

func dedupeHelperEntrypointAliases(values []helperEntrypointAlias) []helperEntrypointAlias {
	seen := map[string]bool{}
	var out []helperEntrypointAlias
	for _, value := range values {
		key := managedinstall.ComparisonKey(value.path, runtime.GOOS)
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, value)
	}
	return out
}

func repairHelperEntrypointAlias(installPath string, alias helperEntrypointAlias) error {
	path := strings.TrimSpace(alias.path)
	if path == "" || sameHelperInstallLocation(path, installPath, runtime.GOOS) {
		return nil
	}
	probe := helperpath.ProbePath(path, helperpath.Options{GOOS: runtime.GOOS})
	if probe.Exists && (probe.IsDir || !probe.PlausibleHelperEntry) {
		return nil
	}
	if !probe.Exists && !alias.create {
		return nil
	}
	if probe.Exists && !probe.Executable {
		return nil
	}
	if strings.EqualFold(runtime.GOOS, "windows") {
		return repairWindowsHelperEntrypointAlias(installPath, path)
	}
	if err := replaceSymlinkAtomically(path, installPath); err != nil {
		if copyErr := copyExecutableAtomically(installPath, path); copyErr != nil {
			return fmt.Errorf("%s %s -> %s: symlink failed: %v; copy failed: %w", alias.description, path, installPath, err, copyErr)
		}
	}
	return nil
}

func repairWindowsHelperEntrypointAlias(installPath string, path string) error {
	base := filepath.Base(path)
	if strings.EqualFold(base, "cxp.cmd") {
		return os.WriteFile(path, []byte(windowsCXPShimContent()), 0o755)
	}
	return copyExecutableAtomically(installPath, path)
}

func sameHelperInstallLocation(a string, b string, goos string) bool {
	if sameHelperExecutablePath(a, b, goos) {
		return true
	}
	if sameExistingHelperFile(a, b) {
		return true
	}
	aKey, aOK := helperInstallLocationKey(a, goos)
	bKey, bOK := helperInstallLocationKey(b, goos)
	return aOK && bOK && aKey == bKey
}

func symlinkChainReferencesPath(start string, needle string, goos string) bool {
	current := filepath.Clean(strings.TrimSpace(start))
	needle = filepath.Clean(strings.TrimSpace(needle))
	if current == "" || current == "." || needle == "" || needle == "." {
		return false
	}
	seen := map[string]bool{}
	for i := 0; i < 64; i++ {
		if sameHelperExecutablePath(current, needle, goos) {
			return true
		}
		currentKey, currentOK := helperInstallLocationKey(current, goos)
		needleKey, needleOK := helperInstallLocationKey(needle, goos)
		if currentOK && needleOK && currentKey == needleKey {
			return true
		}
		key := managedinstall.ComparisonKey(current, goos)
		if seen[key] {
			return true
		}
		seen[key] = true
		info, err := os.Lstat(current)
		if err != nil {
			return false
		}
		if info.Mode()&os.ModeSymlink == 0 {
			return false
		}
		target, err := os.Readlink(current)
		if err != nil {
			return true
		}
		if !filepath.IsAbs(target) {
			target = filepath.Join(filepath.Dir(current), target)
		}
		current = filepath.Clean(target)
	}
	return true
}

func sameExistingHelperFile(a string, b string) bool {
	aInfo, aErr := os.Stat(strings.TrimSpace(a))
	bInfo, bErr := os.Stat(strings.TrimSpace(b))
	return aErr == nil && bErr == nil && os.SameFile(aInfo, bInfo)
}

func repairHelperInstallSelfLoopFromPrevious(path string, goos string) (bool, error) {
	path = managedinstall.CanonicalTargetPathForEntry(path, goos)
	if path == "" || !managedinstall.IsCanonicalTargetPath(path, goos) {
		return false, nil
	}
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.Mode()&os.ModeSymlink == 0 {
		return false, nil
	}
	target, err := os.Readlink(path)
	if err != nil {
		return false, err
	}
	resolvedTarget := target
	if !filepath.IsAbs(resolvedTarget) {
		resolvedTarget = filepath.Join(filepath.Dir(path), resolvedTarget)
	}
	if !sameHelperInstallLocation(resolvedTarget, path, goos) {
		return false, nil
	}
	previous := path + ".prev"
	probe := helperpath.ProbePath(previous, helperpath.Options{GOOS: goos})
	if !probe.Exists || probe.IsDir || !probe.Executable {
		return true, fmt.Errorf("managed helper target %s is a self-referential symlink and previous binary %s is not usable", path, previous)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := update.ProbeBinaryVersion(ctx, previous, 5*time.Second); err != nil {
		return true, fmt.Errorf("managed helper target %s is a self-referential symlink and previous binary %s is not runnable: %w", path, previous, err)
	}
	if err := copyExecutableAtomically(previous, path); err != nil {
		return true, fmt.Errorf("repair managed helper self-loop %s from %s: %w", path, previous, err)
	}
	return true, nil
}

func helperInstallLocationKey(path string, goos string) (string, bool) {
	path = filepath.Clean(strings.TrimSpace(path))
	if path == "" || path == "." {
		return "", false
	}
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	if real, err := filepath.EvalSymlinks(path); err == nil && strings.TrimSpace(real) != "" {
		return managedinstall.ComparisonKey(real, goos), true
	}
	parent := filepath.Dir(path)
	if parent == "" || parent == "." || parent == path {
		return managedinstall.ComparisonKey(path, goos), true
	}
	if realParent, err := filepath.EvalSymlinks(parent); err == nil && strings.TrimSpace(realParent) != "" {
		return managedinstall.ComparisonKey(filepath.Join(realParent, filepath.Base(path)), goos), true
	}
	return managedinstall.ComparisonKey(path, goos), true
}

func saveCLIManagedInstallRecordBestEffort(installPath string, version string) {
	recordPath, err := managedinstall.DefaultRecordPath()
	if err != nil {
		return
	}
	installPath = canonicalManagedInstallPathForSameLocation(installPath, recordPath)
	installPath = managedinstall.CanonicalTargetPathForEntry(installPath, runtime.GOOS)
	shims := []string{}
	if !strings.EqualFold(runtime.GOOS, "windows") {
		shims = append(shims, filepath.Join(filepath.Dir(installPath), "cxp"))
	} else {
		shims = append(shims, filepath.Join(filepath.Dir(installPath), "cxp.cmd"))
	}
	record := managedinstall.Record{
		TargetPath:   installPath,
		TargetSource: string(managedinstall.SourceCurrentExecutable),
		TargetState:  string(managedinstall.StateManaged),
		Version:      strings.TrimPrefix(strings.TrimSpace(version), "v"),
		GOOS:         runtime.GOOS,
		GOARCH:       runtime.GOARCH,
		Shims:        existingManagedInstallShimsForGOOS(shims, runtime.GOOS),
	}
	_ = managedinstall.SaveRecord(recordPath, record)
}

func existingManagedInstallShimsForGOOS(shims []string, goos string) []string {
	var out []string
	for _, shim := range shims {
		if strings.TrimSpace(shim) == "" {
			continue
		}
		if probe := helperpath.ProbePath(shim, helperpath.Options{GOOS: goos}); probe.Exists && !probe.IsDir && probe.PlausibleHelperEntry {
			out = append(out, shim)
		}
	}
	return out
}

func ensureCXPShimForInstallPath(installPath string) error {
	return ensureCXPShimForInstallPathForGOOS(installPath, runtime.GOOS)
}

func ensureCXPShimForInstallPathForGOOS(installPath string, goos string) error {
	if strings.EqualFold(goos, "windows") {
		return ensureWindowsCXPShimForInstallPath(installPath)
	}
	installPath = managedinstall.CanonicalTargetPathForEntry(installPath, goos)
	if installPath == "" || !strings.EqualFold(filepath.Base(installPath), helperpath.BinaryName(goos)) {
		return nil
	}
	shimPath := filepath.Join(filepath.Dir(installPath), "cxp")
	if info, err := os.Lstat(shimPath); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			target, readErr := os.Readlink(shimPath)
			if readErr != nil {
				return readErr
			}
			resolvedTarget := target
			if !filepath.IsAbs(resolvedTarget) {
				resolvedTarget = filepath.Join(filepath.Dir(shimPath), resolvedTarget)
			}
			if sameHelperInstallLocation(resolvedTarget, installPath, goos) {
				return nil
			}
			if err := replaceSymlinkAtomically(shimPath, installPath); err != nil {
				if copyErr := copyExecutableAtomically(installPath, shimPath); copyErr != nil {
					return fmt.Errorf("repair cxp shim %s -> %s: symlink failed: %v; copy failed: %w", shimPath, installPath, err, copyErr)
				}
			}
			return nil
		}
		probe := helperpath.ProbePath(shimPath, helperpath.Options{GOOS: goos})
		if probe.Exists && !probe.IsDir && probe.Executable && probe.PlausibleHelperEntry {
			repair, err := cxpShimNeedsRepair(installPath, shimPath)
			if err != nil {
				return err
			}
			if repair {
				if err := replaceSymlinkAtomically(shimPath, installPath); err != nil {
					if copyErr := copyExecutableAtomically(installPath, shimPath); copyErr != nil {
						return fmt.Errorf("repair cxp regular shim %s -> %s: symlink failed: %v; copy failed: %w", shimPath, installPath, err, copyErr)
					}
				}
			}
			return nil
		}
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(shimPath), 0o755); err != nil {
		return err
	}
	if err := replaceSymlinkAtomically(shimPath, installPath); err != nil {
		if copyErr := copyExecutableAtomically(installPath, shimPath); copyErr != nil {
			return fmt.Errorf("create cxp shim %s -> %s: symlink failed: %v; copy failed: %w", shimPath, installPath, err, copyErr)
		}
	}
	return nil
}

func replaceSymlinkAtomically(linkPath string, targetPath string) error {
	if sameHelperInstallLocation(linkPath, targetPath, runtime.GOOS) {
		return nil
	}
	if symlinkChainReferencesPath(targetPath, linkPath, runtime.GOOS) {
		return fmt.Errorf("refusing to create symlink %s -> %s because the target symlink chain references the link path", linkPath, targetPath)
	}
	dir := filepath.Dir(linkPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(linkPath)+".symlink-*")
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
	if err := os.Symlink(targetPath, tmpPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, linkPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

func ensureWindowsCXPShimForInstallPath(installPath string) error {
	installPath = managedinstall.CanonicalTargetPathForEntry(installPath, "windows")
	if installPath == "" || !strings.EqualFold(filepath.Base(installPath), helperpath.BinaryName("windows")) {
		return nil
	}
	stableExe := filepath.Join(filepath.Dir(installPath), helperruntime.BinaryName("windows"))
	if _, err := os.Stat(stableExe); os.IsNotExist(err) {
		if err := copyExecutableAtomically(installPath, stableExe); err != nil {
			return fmt.Errorf("create stable cxp executable %s from %s: %w", stableExe, installPath, err)
		}
	} else if err != nil {
		return err
	}
	shimPath := filepath.Join(filepath.Dir(installPath), "cxp.cmd")
	expected := windowsCXPShimContent()
	if info, err := os.Stat(shimPath); err == nil {
		if info.IsDir() {
			return nil
		}
		data, readErr := os.ReadFile(shimPath)
		if readErr != nil {
			return readErr
		}
		if string(data) == expected {
			return nil
		}
		if isManagedWindowsCXPShim(data) {
			return os.WriteFile(shimPath, []byte(expected), 0o755)
		}
		if !strings.Contains(strings.ToLower(string(data)), "codex-proxy.exe") {
			return nil
		}
		return os.WriteFile(shimPath, []byte(expected), 0o755)
	} else if !os.IsNotExist(err) {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(shimPath), 0o755); err != nil {
		return err
	}
	return os.WriteFile(shimPath, []byte(expected), 0o755)
}

// refreshWindowsStableCXPExecutable converges a stable cxp.exe that predates
// the immutable runtime launcher. This is intentionally separate from
// ensureWindowsCXPShimForInstallPath: runtime-owned upgrades only switch the
// active immutable runtime and must not try to replace their running launcher.
// A canonical cxp.cmd is the ownership marker that permits replacing cxp.exe;
// custom user command shims and their executables remain untouched.
func refreshWindowsStableCXPExecutable(installPath string) error {
	return refreshWindowsStableCXPExecutableFromSource(installPath, installPath)
}

// refreshWindowsStableCXPExecutableFromSource converges an owned cxp.exe from
// a validated helper binary that may not have reached installPath yet. Windows
// upgrades can leave the primary replacement pending when a short-lived file
// lock prevents MoveFileEx from replacing codex-proxy.exe. The downloaded
// binary remains available until the upgrading process exits, so it is also
// the safest source for keeping the stable cxp entrypoint on the same version.
func refreshWindowsStableCXPExecutableFromSource(installPath string, sourcePath string) error {
	installPath = managedinstall.CanonicalTargetPathForEntry(installPath, "windows")
	if installPath == "" || !strings.EqualFold(filepath.Base(installPath), helperpath.BinaryName("windows")) {
		return nil
	}
	sourcePath = strings.TrimSpace(sourcePath)
	if sourcePath == "" {
		return fmt.Errorf("stable cxp executable source is empty")
	}
	shimPath := filepath.Join(filepath.Dir(installPath), "cxp.cmd")
	shimData, err := os.ReadFile(shimPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if string(shimData) != windowsCXPShimContent() {
		return nil
	}
	stableExe := filepath.Join(filepath.Dir(installPath), helperruntime.BinaryName("windows"))
	stableInfo, err := os.Stat(stableExe)
	if err != nil {
		if os.IsNotExist(err) {
			return copyExecutableAtomically(sourcePath, stableExe)
		}
		return err
	}
	if stableInfo.IsDir() {
		return fmt.Errorf("stable cxp executable path is a directory: %s", stableExe)
	}
	same, err := filesHaveEqualSHA256(sourcePath, stableExe)
	if err != nil {
		return err
	}
	if same {
		return nil
	}
	if err := copyExecutableAtomically(sourcePath, stableExe); err != nil {
		return fmt.Errorf("replace stale stable cxp executable %s from %s: %w", stableExe, sourcePath, err)
	}
	return nil
}

func filesHaveEqualSHA256(leftPath string, rightPath string) (bool, error) {
	left, err := fileSHA256Portable(leftPath)
	if err != nil {
		return false, err
	}
	right, err := fileSHA256Portable(rightPath)
	if err != nil {
		return false, err
	}
	return left == right, nil
}

func fileSHA256Portable(path string) ([sha256.Size]byte, error) {
	var sum [sha256.Size]byte
	file, err := os.Open(path)
	if err != nil {
		return sum, err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return sum, err
	}
	copy(sum[:], hash.Sum(nil))
	return sum, nil
}

func windowsCXPShimContent() string {
	return "@echo off\r\n\"%~dp0cxp.exe\" %*\r\n"
}

func isManagedWindowsCXPShim(data []byte) bool {
	content := strings.TrimPrefix(string(data), "\xef\xbb\xbf")
	content = strings.ReplaceAll(content, "\r\n", "\n")
	return strings.EqualFold(strings.TrimSpace(content), "@echo off\n\"%~dp0cxp.exe\" %*")
}

func migrateWindowsPowerShellCXPProfiles() []error {
	const envProfile = "CODEX_PROXY_PROFILE_PATH"
	var candidates []string
	add := func(path string) {
		path = filepath.Clean(strings.TrimSpace(path))
		if path != "" && path != "." {
			candidates = append(candidates, path)
		}
	}
	add(os.Getenv(envProfile))
	for _, root := range []string{os.Getenv("USERPROFILE"), os.Getenv("HOME"), os.Getenv("OneDrive"), os.Getenv("OneDriveConsumer")} {
		root = strings.TrimSpace(root)
		if root == "" {
			continue
		}
		for _, family := range []string{"WindowsPowerShell", "PowerShell"} {
			add(filepath.Join(root, "Documents", family, "Microsoft.PowerShell_profile.ps1"))
			add(filepath.Join(root, "Documents", family, "profile.ps1"))
		}
	}
	seen := map[string]bool{}
	var errs []error
	for _, path := range candidates {
		key := managedinstall.ComparisonKey(path, "windows")
		if key == "" || seen[key] {
			continue
		}
		seen[key] = true
		if err := rewriteLegacyCXPProfile(path); err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", path, err))
		}
	}
	return errs
}

func rewriteLegacyCXPProfile(path string) error {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	legacy := []byte("Set-Alias -Name cxp -Value codex-proxy")
	updated, changed := replaceExactPowerShellProfileLine(data, legacy, []byte("Set-Alias -Name cxp -Value cxp.exe"))
	if !changed {
		return nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	return writeFileAtomically(path, updated, info.Mode().Perm())
}

func replaceExactPowerShellProfileLine(data []byte, oldLine []byte, newLine []byte) ([]byte, bool) {
	parts := bytes.SplitAfter(data, []byte{'\n'})
	var out bytes.Buffer
	out.Grow(len(data))
	changed := false
	for index, part := range parts {
		if len(part) == 0 {
			continue
		}
		bodyEnd := len(part)
		if bodyEnd > 0 && part[bodyEnd-1] == '\n' {
			bodyEnd--
		}
		if bodyEnd > 0 && part[bodyEnd-1] == '\r' {
			bodyEnd--
		}
		bodyStart := 0
		if index == 0 && bodyEnd >= 3 && bytes.Equal(part[:3], []byte{0xef, 0xbb, 0xbf}) {
			bodyStart = 3
		}
		if bytes.Equal(part[bodyStart:bodyEnd], oldLine) {
			out.Write(part[:bodyStart])
			out.Write(newLine)
			out.Write(part[bodyEnd:])
			changed = true
			continue
		}
		out.Write(part)
	}
	if !changed {
		return data, false
	}
	return out.Bytes(), true
}

func verifyCXPEntrypointAfterUpgrade(installPath string, targetVersion string) error {
	return verifyCXPEntrypointAfterUpgradeForGOOS(installPath, targetVersion, runtime.GOOS)
}

func verifyCXPEntrypointAfterUpgradeForGOOS(installPath string, targetVersion string, goos string) error {
	installPath = managedinstall.CanonicalTargetPathForEntry(installPath, goos)
	if !managedinstall.IsCanonicalTargetPath(installPath, goos) {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := verifyHelperEntrypointVersion(ctx, installPath, targetVersion, "managed helper target"); err != nil {
		return err
	}
	shimName := "cxp"
	if strings.EqualFold(goos, "windows") {
		shimName = "cxp.cmd"
	}
	shimPath := filepath.Join(filepath.Dir(installPath), shimName)
	if err := verifyHelperEntrypointVersion(ctx, shimPath, targetVersion, "cxp entrypoint"); err != nil {
		if repairErr := ensureCXPShimForInstallPathForGOOS(installPath, goos); repairErr != nil {
			return fmt.Errorf("%w; repair failed: %v", err, repairErr)
		}
		if strings.EqualFold(goos, "windows") {
			if repairErr := refreshWindowsStableCXPExecutable(installPath); repairErr != nil {
				return fmt.Errorf("%w; stable executable repair failed: %v", err, repairErr)
			}
		}
		if retryErr := verifyHelperEntrypointVersion(ctx, shimPath, targetVersion, "cxp entrypoint"); retryErr != nil {
			return fmt.Errorf("%w; after repair: %v", err, retryErr)
		}
	}
	return nil
}

func verifyHelperEntrypointVersion(ctx context.Context, path string, targetVersion string, description string) error {
	return verifyHelperVersionWithProbe(ctx, path, targetVersion, description, update.ProbePhysicalBinaryVersion)
}

func verifyFreshHelperEntrypointVersion(ctx context.Context, path string, targetVersion string, description string) error {
	return verifyHelperVersionWithProbe(ctx, path, targetVersion, description, update.ProbeFreshEntryVersion)
}

func verifyHelperVersionWithProbe(ctx context.Context, path string, targetVersion string, description string, probe func(context.Context, string, time.Duration) (update.BinaryVersion, error)) error {
	version, err := probe(ctx, path, 5*time.Second)
	if err != nil {
		return fmt.Errorf("%s %s is not runnable after upgrade: %w", description, path, err)
	}
	if !update.VersionMatchesTarget(version.Version, targetVersion) {
		return fmt.Errorf("%s %s version = %s, want %s", description, path, version.Version, strings.TrimPrefix(strings.TrimSpace(targetVersion), "v"))
	}
	return nil
}

func cxpShimNeedsRepair(installPath string, shimPath string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	installVersion, installErr := update.ProbeBinaryVersion(ctx, installPath, 5*time.Second)
	shimVersion, shimErr := update.ProbeBinaryVersion(ctx, shimPath, 5*time.Second)
	if shimErr != nil {
		return installErr == nil, nil
	}
	if installErr != nil {
		return false, nil
	}
	cmp, ok := update.CompareVersions(installVersion.Version, shimVersion.Version)
	return ok && cmp > 0, nil
}

func materializeManagedInstallShim(ctx context.Context, runningPath string, targetPath string, shimPath string, runningVersion string) error {
	targetPath = managedinstall.CanonicalTargetPathForEntry(targetPath, teamsServiceGOOS())
	if info, err := os.Lstat(shimPath); err == nil && info.Mode()&os.ModeSymlink != 0 {
		linkTarget, readErr := os.Readlink(shimPath)
		if readErr != nil {
			return readErr
		}
		resolvedTarget := linkTarget
		if !filepath.IsAbs(resolvedTarget) {
			resolvedTarget = filepath.Join(filepath.Dir(shimPath), resolvedTarget)
		}
		if sameHelperInstallLocation(resolvedTarget, targetPath, teamsServiceGOOS()) {
			return nil
		}
		if err := replaceSymlinkAtomically(shimPath, targetPath); err != nil {
			if copyErr := copyExecutableAtomically(targetPath, shimPath); copyErr != nil {
				return fmt.Errorf("repair managed Teams install shim %s -> %s: symlink failed: %v; copy failed: %w", shimPath, targetPath, err, copyErr)
			}
		}
		return nil
	}
	probe := helperpath.ProbePath(shimPath, helperpath.Options{GOOS: teamsServiceGOOS(), Stat: teamsServiceStat})
	if !probe.Exists {
		if err := os.MkdirAll(filepath.Dir(shimPath), 0o755); err != nil {
			return err
		}
		if err := replaceSymlinkAtomically(shimPath, targetPath); err != nil {
			return fmt.Errorf("materialize managed Teams install shim %s -> %s: %w", shimPath, targetPath, err)
		}
		return nil
	}
	if probe.IsDir || !probe.Executable || !probe.PlausibleHelperEntry {
		return nil
	}
	probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	shimVersion, err := update.ProbeBinaryVersion(probeCtx, shimPath, 5*time.Second)
	shouldCopy := err != nil
	if err == nil {
		cmp, ok := update.CompareVersions(runningVersion, shimVersion.Version)
		shouldCopy = ok && cmp > 0
	}
	if !shouldCopy {
		return nil
	}
	if err := copyExecutableAtomically(runningPath, shimPath); err != nil {
		return fmt.Errorf("materialize managed Teams install shim %s from running helper %s: %w", shimPath, runningPath, err)
	}
	return nil
}

func saveManagedInstallRecordBestEffort(target managedinstall.Target, version string, shims []string) {
	if strings.TrimSpace(target.RecordPath) == "" {
		return
	}
	target.Path = managedinstall.CanonicalTargetPathForEntry(target.Path, teamsServiceGOOS())
	record := managedinstall.Record{
		TargetPath:   target.Path,
		TargetSource: string(target.Source),
		TargetState:  string(target.State),
		Version:      version,
		GOOS:         teamsServiceGOOS(),
		GOARCH:       runtime.GOARCH,
		Shims:        existingManagedInstallShims(shims),
	}
	_ = managedinstall.SaveRecord(target.RecordPath, record)
}

func existingManagedInstallShims(shims []string) []string {
	var out []string
	for _, shim := range shims {
		if strings.TrimSpace(shim) == "" {
			continue
		}
		if probe := helperpath.ProbePath(shim, helperpath.Options{GOOS: teamsServiceGOOS(), Stat: teamsServiceStat}); probe.Exists && !probe.IsDir && probe.PlausibleHelperEntry {
			out = append(out, shim)
		}
	}
	return out
}

func copyExecutableAtomically(src string, dst string) error {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return err
	}
	if srcInfo.IsDir() {
		return fmt.Errorf("source is a directory: %s", src)
	}
	dstIsSymlink := false
	if dstInfo, err := os.Lstat(dst); err == nil && dstInfo.Mode()&os.ModeSymlink != 0 {
		dstIsSymlink = true
	}
	if dstInfo, err := os.Stat(dst); err == nil && os.SameFile(srcInfo, dstInfo) && !dstIsSymlink {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp, err := os.CreateTemp(filepath.Dir(dst), "."+filepath.Base(dst)+".materialize-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if _, err := io.Copy(tmp, in); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(srcInfo.Mode().Perm() | 0o700); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := replaceStagedFile(tmpPath, dst); err != nil {
		return err
	}
	cleanup = false
	return nil
}
