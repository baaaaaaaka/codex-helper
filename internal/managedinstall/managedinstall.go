package managedinstall

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
)

const (
	EnvInstallPath = "CODEX_PROXY_INSTALL_PATH"
	EnvInstallDir  = "CODEX_PROXY_INSTALL_DIR"

	RecordSchemaVersion = 1
)

type Source string

const (
	SourceExplicit          Source = "explicit"
	SourceEnvInstallPath    Source = "env_install_path"
	SourceEnvInstallDir     Source = "env_install_dir"
	SourceRecord            Source = "record"
	SourceDefault           Source = "default"
	SourceCurrentExecutable Source = "current_executable"
)

type TargetState string

const (
	StateManaged              TargetState = "managed"
	StateExplicit             TargetState = "explicit"
	StateUnmanagedCurrentExec TargetState = "unmanaged_current_executable"
)

type Record struct {
	SchemaVersion int      `json:"schema_version"`
	TargetPath    string   `json:"target_path"`
	TargetSource  string   `json:"target_source,omitempty"`
	TargetState   string   `json:"target_state,omitempty"`
	Repo          string   `json:"repo,omitempty"`
	Version       string   `json:"version,omitempty"`
	GOOS          string   `json:"goos,omitempty"`
	GOARCH        string   `json:"goarch,omitempty"`
	Shims         []string `json:"shims,omitempty"`
	UpdatedAt     string   `json:"updated_at,omitempty"`
}

type Target struct {
	Path          string
	Source        Source
	State         TargetState
	Reason        string
	ComparisonKey string
	RecordPath    string
	Warnings      []string
}

type Options struct {
	ExplicitPath  string
	EnvPath       string
	EnvDir        string
	RawExecutable string
	Argv0         string

	GOOS       string
	GOARCH     string
	HomeDir    string
	ConfigDir  string
	RecordPath string

	RequireExisting             bool
	AllowMissingDefault         bool
	FallbackOnInvalidEnvPath    bool
	PreferRecordBeforeLegacyEnv bool

	Stat func(string) (os.FileInfo, error)
}

func Resolve(opts Options) (Target, error) {
	opts = opts.withDefaults()
	var warnings []string
	recordPath, recordPathErr := defaultRecordPath(opts)
	if recordPathErr == nil {
		opts.RecordPath = recordPath
	}

	try := func(path string, source Source, state TargetState, reason string, allowMissing bool) (Target, bool) {
		target, err := resolveCandidate(path, source, state, reason, opts, allowMissing)
		if err != nil {
			warnings = append(warnings, err.Error())
			return Target{}, false
		}
		target.RecordPath = opts.RecordPath
		target.Warnings = append(target.Warnings, warnings...)
		return target, true
	}

	if strings.TrimSpace(opts.ExplicitPath) != "" {
		if target, ok := try(opts.ExplicitPath, SourceExplicit, StateExplicit, "explicit install path", true); ok {
			return target, nil
		}
		return Target{}, fmt.Errorf("resolve explicit install path: %s", warnings[len(warnings)-1])
	}
	if strings.TrimSpace(opts.EnvPath) != "" {
		if target, ok := try(opts.EnvPath, SourceEnvInstallPath, StateExplicit, EnvInstallPath, !opts.FallbackOnInvalidEnvPath); ok {
			return target, nil
		}
		if !opts.FallbackOnInvalidEnvPath {
			return Target{}, fmt.Errorf("resolve %s: %s", EnvInstallPath, warnings[len(warnings)-1])
		}
	}
	if !opts.PreferRecordBeforeLegacyEnv && strings.TrimSpace(opts.EnvDir) != "" {
		if target, ok := try(resolveLegacyInstallDirCandidate(opts.EnvDir, opts), SourceEnvInstallDir, StateExplicit, EnvInstallDir, true); ok {
			return target, nil
		}
		return Target{}, fmt.Errorf("resolve %s: %s", EnvInstallDir, warnings[len(warnings)-1])
	}

	if recordPathErr == nil {
		if record, err := LoadRecord(opts.RecordPath); err == nil {
			if strings.TrimSpace(record.TargetPath) != "" {
				if target, ok := try(record.TargetPath, SourceRecord, StateManaged, "install record", !opts.RequireExisting); ok {
					return target, nil
				}
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			warnings = append(warnings, "install record ignored: "+err.Error())
		}
	} else {
		warnings = append(warnings, "install record unavailable: "+recordPathErr.Error())
	}

	if defaultPath, err := DefaultInstallPath(opts); err == nil {
		allowMissingDefault := opts.AllowMissingDefault && !opts.RequireExisting
		if target, ok := try(defaultPath, SourceDefault, StateManaged, "default per-user install target", allowMissingDefault); ok {
			return target, nil
		}
	} else {
		warnings = append(warnings, "default install target unavailable: "+err.Error())
	}

	if opts.PreferRecordBeforeLegacyEnv && strings.TrimSpace(opts.EnvDir) != "" {
		if target, ok := try(resolveLegacyInstallDirCandidate(opts.EnvDir, opts), SourceEnvInstallDir, StateExplicit, EnvInstallDir, true); ok {
			return target, nil
		}
	}

	if strings.TrimSpace(opts.RawExecutable) != "" || strings.TrimSpace(opts.Argv0) != "" {
		resolved, err := helperpath.StableInstallTargetFromSources("", "", opts.RawExecutable, opts.Argv0, helperpath.Options{
			GOOS: opts.GOOS,
			Stat: opts.Stat,
		})
		if err != nil {
			return Target{}, err
		}
		key := ComparisonKey(resolved.Path, opts.GOOS)
		return Target{
			Path:          resolved.Path,
			Source:        SourceCurrentExecutable,
			State:         StateUnmanagedCurrentExec,
			Reason:        resolved.Reason,
			ComparisonKey: key,
			RecordPath:    opts.RecordPath,
			Warnings:      warnings,
		}, nil
	}

	if len(warnings) > 0 {
		return Target{}, fmt.Errorf("resolve managed install target: %s", strings.Join(warnings, "; "))
	}
	return Target{}, fmt.Errorf("resolve managed install target: no candidate paths")
}

func LoadRecord(path string) (Record, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		var err error
		path, err = DefaultRecordPath()
		if err != nil {
			return Record{}, err
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Record{}, err
	}
	data = bytes.TrimPrefix(data, []byte{0xef, 0xbb, 0xbf})
	var record Record
	if err := json.Unmarshal(data, &record); err != nil {
		return Record{}, fmt.Errorf("read install record %s: %w", path, err)
	}
	if record.SchemaVersion > RecordSchemaVersion {
		return Record{}, fmt.Errorf("install record %s requires schema %d, supported %d", path, record.SchemaVersion, RecordSchemaVersion)
	}
	return record, nil
}

func SaveRecord(path string, record Record) error {
	path = strings.TrimSpace(path)
	if path == "" {
		var err error
		path, err = DefaultRecordPath()
		if err != nil {
			return err
		}
	}
	if record.SchemaVersion == 0 {
		record.SchemaVersion = RecordSchemaVersion
	}
	if record.UpdatedAt == "" {
		record.UpdatedAt = time.Now().UTC().Format(time.RFC3339)
	}
	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, filepath.Base(path)+".tmp-*")
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
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpPath, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	cleanup = false
	return nil
}

func DefaultRecordPath() (string, error) {
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "codex-helper", "install.json"), nil
}

func DefaultInstallPath(opts Options) (string, error) {
	opts = opts.withDefaults()
	home := strings.TrimSpace(opts.HomeDir)
	if home == "" {
		var err error
		home, err = os.UserHomeDir()
		if err != nil {
			return "", err
		}
	}
	if strings.TrimSpace(home) == "" {
		return "", fmt.Errorf("empty home dir")
	}
	return filepath.Join(home, ".local", "bin", helperpath.BinaryName(opts.GOOS)), nil
}

func ComparisonKey(path string, goos string) string {
	clean := filepath.Clean(strings.TrimSpace(path))
	if abs, err := filepath.Abs(clean); err == nil {
		clean = abs
	}
	if real, err := filepath.EvalSymlinks(clean); err == nil && strings.TrimSpace(real) != "" {
		clean = filepath.Clean(real)
	}
	if strings.EqualFold(strings.TrimSpace(goos), "windows") {
		return strings.ToLower(clean)
	}
	return clean
}

func (opts Options) withDefaults() Options {
	if strings.TrimSpace(opts.GOOS) == "" {
		opts.GOOS = runtime.GOOS
	}
	if strings.TrimSpace(opts.GOARCH) == "" {
		opts.GOARCH = runtime.GOARCH
	}
	if opts.Stat == nil {
		opts.Stat = os.Stat
	}
	return opts
}

func defaultRecordPath(opts Options) (string, error) {
	if strings.TrimSpace(opts.RecordPath) != "" {
		return filepath.Clean(opts.RecordPath), nil
	}
	if strings.TrimSpace(opts.ConfigDir) != "" {
		return filepath.Join(opts.ConfigDir, "codex-helper", "install.json"), nil
	}
	return DefaultRecordPath()
}

func resolveCandidate(path string, source Source, state TargetState, reason string, opts Options, allowMissing bool) (Target, error) {
	if source == SourceEnvInstallPath || source == SourceRecord || source == SourceDefault {
		clean := filepath.Clean(strings.TrimSpace(path))
		if info, err := opts.Stat(clean); err == nil && info.IsDir() {
			return Target{}, fmt.Errorf("install target %s is a directory", clean)
		}
	}
	resolved, err := helperpath.StableInstallTargetFromSources(path, "", "", "", helperpath.Options{
		GOOS: opts.GOOS,
		Stat: opts.Stat,
	})
	if err != nil {
		return Target{}, err
	}
	probe := helperpath.ProbePath(resolved.Path, helperpath.Options{GOOS: opts.GOOS, Stat: opts.Stat})
	if source != SourceExplicit && source != SourceEnvInstallPath && !probe.PlausibleHelperEntry {
		return Target{}, fmt.Errorf("install target %s is not a known helper entry", resolved.Path)
	}
	if opts.RequireExisting || !allowMissing {
		if !probe.Exists {
			return Target{}, fmt.Errorf("install target %s does not exist", resolved.Path)
		}
		if probe.IsDir {
			return Target{}, fmt.Errorf("install target %s is a directory", resolved.Path)
		}
		if !probe.Executable {
			return Target{}, fmt.Errorf("install target %s is not executable", resolved.Path)
		}
	}
	return Target{
		Path:          resolved.Path,
		Source:        source,
		State:         state,
		Reason:        reason,
		ComparisonKey: ComparisonKey(resolved.Path, opts.GOOS),
	}, nil
}

func resolveLegacyInstallDirCandidate(value string, opts Options) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	base := filepath.Base(filepath.Clean(value))
	if helperpath.ProbePath(value, helperpath.Options{GOOS: opts.GOOS, Stat: opts.Stat}).PlausibleHelperEntry ||
		strings.EqualFold(base, helperpath.BinaryName(opts.GOOS)) ||
		strings.EqualFold(base, "cxp") ||
		strings.EqualFold(base, "cxp.exe") {
		return value
	}
	return filepath.Join(value, helperpath.BinaryName(opts.GOOS))
}
