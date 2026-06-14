package managedinstall

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestResolveUsesExistingDefaultBeforeCurrentExecutable(t *testing.T) {
	home := t.TempDir()
	configDir := filepath.Join(home, ".config")
	defaultTarget := filepath.Join(home, ".local", "bin", "codex-proxy")
	goBinTarget := filepath.Join(home, "go", "bin", "codex-proxy")
	writeExecutable(t, defaultTarget)
	writeExecutable(t, goBinTarget)

	target, err := Resolve(Options{
		RawExecutable: goBinTarget,
		HomeDir:       home,
		ConfigDir:     configDir,
		GOOS:          "linux",
		Stat:          executableStatForTest,
	})
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}
	if target.Path != defaultTarget || target.Source != SourceDefault || target.State != StateManaged {
		t.Fatalf("target = %#v, want default managed %q", target, defaultTarget)
	}
}

func TestResolveFallsBackToCurrentExecutableWhenDefaultMissing(t *testing.T) {
	home := t.TempDir()
	goBinTarget := filepath.Join(home, "go", "bin", "codex-proxy")
	writeExecutable(t, goBinTarget)

	target, err := Resolve(Options{
		RawExecutable: goBinTarget,
		HomeDir:       home,
		ConfigDir:     filepath.Join(home, ".config"),
		GOOS:          "linux",
		Stat:          executableStatForTest,
	})
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}
	if target.Path != goBinTarget || target.Source != SourceCurrentExecutable || target.State != StateUnmanagedCurrentExec {
		t.Fatalf("target = %#v, want current executable fallback %q", target, goBinTarget)
	}
}

func TestResolveCanReturnMissingDefaultWhenCallerCanMaterialize(t *testing.T) {
	home := t.TempDir()
	goBinTarget := filepath.Join(home, "go", "bin", "codex-proxy")
	writeExecutable(t, goBinTarget)
	defaultTarget := filepath.Join(home, ".local", "bin", "codex-proxy")

	target, err := Resolve(Options{
		RawExecutable:       goBinTarget,
		HomeDir:             home,
		ConfigDir:           filepath.Join(home, ".config"),
		GOOS:                "linux",
		AllowMissingDefault: true,
		Stat:                executableStatForTest,
	})
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}
	if target.Path != defaultTarget || target.Source != SourceDefault || target.State != StateManaged {
		t.Fatalf("target = %#v, want missing default managed %q", target, defaultTarget)
	}
}

func TestResolveCanIgnoreStaleEnvInstallPath(t *testing.T) {
	home := t.TempDir()
	defaultTarget := filepath.Join(home, ".local", "bin", "codex-proxy")
	writeExecutable(t, defaultTarget)

	target, err := Resolve(Options{
		EnvPath:                  filepath.Join(home, "missing", "codex-proxy"),
		HomeDir:                  home,
		ConfigDir:                filepath.Join(home, ".config"),
		GOOS:                     "linux",
		FallbackOnInvalidEnvPath: true,
		Stat:                     executableStatForTest,
	})
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}
	if target.Path != defaultTarget || target.Source != SourceDefault {
		t.Fatalf("target = %#v, want default target %q", target, defaultTarget)
	}
	if len(target.Warnings) == 0 {
		t.Fatalf("target warnings empty, want stale env path warning")
	}
}

func TestResolveSkipsDefaultPathOccupiedByDirectory(t *testing.T) {
	home := t.TempDir()
	defaultTarget := filepath.Join(home, ".local", "bin", "codex-proxy")
	if err := os.MkdirAll(defaultTarget, 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", defaultTarget, err)
	}
	goBinTarget := filepath.Join(home, "go", "bin", "codex-proxy")
	writeExecutable(t, goBinTarget)

	target, err := Resolve(Options{
		RawExecutable:       goBinTarget,
		HomeDir:             home,
		ConfigDir:           filepath.Join(home, ".config"),
		GOOS:                "linux",
		AllowMissingDefault: true,
		Stat:                executableStatForTest,
	})
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}
	if target.Path != goBinTarget || target.Source != SourceCurrentExecutable {
		t.Fatalf("target = %#v, want current executable fallback %q", target, goBinTarget)
	}
	if len(target.Warnings) == 0 {
		t.Fatalf("target warnings empty, want blocked default warning")
	}
}

func TestResolveAllowsExplicitNonStandardBasename(t *testing.T) {
	home := t.TempDir()
	targetPath := filepath.Join(home, "custom-helper")

	target, err := Resolve(Options{
		ExplicitPath: targetPath,
		HomeDir:      home,
		ConfigDir:    filepath.Join(home, ".config"),
		GOOS:         "linux",
		Stat:         executableStatForTest,
	})
	if err != nil {
		t.Fatalf("Resolve explicit non-standard basename error: %v", err)
	}
	if target.Path != targetPath || target.Source != SourceExplicit || target.State != StateExplicit {
		t.Fatalf("target = %#v, want explicit target %q", target, targetPath)
	}
}

func TestResolveTeamsModePrefersRecordBeforeLegacyEnvDir(t *testing.T) {
	home := t.TempDir()
	recordTarget := filepath.Join(home, ".local", "bin", "codex-proxy")
	legacyTarget := filepath.Join(home, "go", "bin", "codex-proxy")
	writeExecutable(t, recordTarget)
	writeExecutable(t, legacyTarget)
	recordPath := filepath.Join(home, ".config", "codex-helper", "install.json")
	if err := SaveRecord(recordPath, Record{TargetPath: recordTarget}); err != nil {
		t.Fatalf("SaveRecord: %v", err)
	}

	target, err := Resolve(Options{
		EnvDir:                      legacyTarget,
		RawExecutable:               legacyTarget,
		HomeDir:                     home,
		ConfigDir:                   filepath.Join(home, ".config"),
		RecordPath:                  recordPath,
		GOOS:                        "linux",
		PreferRecordBeforeLegacyEnv: true,
		Stat:                        executableStatForTest,
	})
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}
	if target.Path != recordTarget || target.Source != SourceRecord {
		t.Fatalf("target = %#v, want record target %q", target, recordTarget)
	}
}

func TestResolveCLIHonorsLegacyEnvDirBeforeRecord(t *testing.T) {
	home := t.TempDir()
	recordTarget := filepath.Join(home, ".local", "bin", "codex-proxy")
	envDir := filepath.Join(home, "custom", "bin")
	envTarget := filepath.Join(envDir, "codex-proxy")
	writeExecutable(t, recordTarget)
	writeExecutable(t, envTarget)
	recordPath := filepath.Join(home, ".config", "codex-helper", "install.json")
	if err := SaveRecord(recordPath, Record{TargetPath: recordTarget}); err != nil {
		t.Fatalf("SaveRecord: %v", err)
	}

	target, err := Resolve(Options{
		EnvDir:     envDir,
		HomeDir:    home,
		ConfigDir:  filepath.Join(home, ".config"),
		RecordPath: recordPath,
		GOOS:       "linux",
		Stat:       executableStatForTest,
	})
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}
	if target.Path != envTarget || target.Source != SourceEnvInstallDir {
		t.Fatalf("target = %#v, want env dir target %q", target, envTarget)
	}
}

func TestResolveTeamsModeRequiresExistingLegacyEnvDir(t *testing.T) {
	home := t.TempDir()
	currentTarget := filepath.Join(home, "go", "bin", "codex-proxy")
	writeExecutable(t, currentTarget)
	missingEnvDir := filepath.Join(home, "missing-bin")

	target, err := Resolve(Options{
		EnvDir:                      missingEnvDir,
		RawExecutable:               currentTarget,
		HomeDir:                     home,
		ConfigDir:                   filepath.Join(home, ".config"),
		GOOS:                        "linux",
		PreferRecordBeforeLegacyEnv: true,
		Stat:                        executableStatForTest,
	})
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}
	if target.Path != currentTarget || target.Source != SourceCurrentExecutable {
		t.Fatalf("target = %#v, want current executable fallback %q", target, currentTarget)
	}
	if len(target.Warnings) == 0 {
		t.Fatalf("target warnings empty, want missing legacy env warning")
	}
}

func TestResolveRequireExistingRejectsMissingRecordAndUsesDefault(t *testing.T) {
	home := t.TempDir()
	defaultTarget := filepath.Join(home, ".local", "bin", "codex-proxy")
	writeExecutable(t, defaultTarget)
	recordPath := filepath.Join(home, ".config", "codex-helper", "install.json")
	if err := SaveRecord(recordPath, Record{TargetPath: filepath.Join(home, "missing", "codex-proxy")}); err != nil {
		t.Fatalf("SaveRecord: %v", err)
	}

	target, err := Resolve(Options{
		HomeDir:         home,
		ConfigDir:       filepath.Join(home, ".config"),
		RecordPath:      recordPath,
		GOOS:            "linux",
		RequireExisting: true,
		Stat:            executableStatForTest,
	})
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}
	if target.Path != defaultTarget || target.Source != SourceDefault {
		t.Fatalf("target = %#v, want default target %q", target, defaultTarget)
	}
	if len(target.Warnings) == 0 {
		t.Fatalf("target warnings empty, want stale record warning")
	}
}

func TestResolveWindowsDefaultUsesExeBasename(t *testing.T) {
	home := t.TempDir()
	defaultTarget := filepath.Join(home, ".local", "bin", "codex-proxy.exe")
	goBinTarget := filepath.Join(home, "go", "bin", "codex-proxy.exe")
	writeExecutable(t, defaultTarget)
	writeExecutable(t, goBinTarget)

	target, err := Resolve(Options{
		RawExecutable: goBinTarget,
		HomeDir:       home,
		ConfigDir:     filepath.Join(home, ".config"),
		GOOS:          "windows",
	})
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}
	if target.Path != defaultTarget || target.Source != SourceDefault || target.State != StateManaged {
		t.Fatalf("target = %#v, want Windows default managed %q", target, defaultTarget)
	}
}

func TestLoadRecordToleratesUTF8BOM(t *testing.T) {
	path := filepath.Join(t.TempDir(), "install.json")
	data := append([]byte{0xef, 0xbb, 0xbf}, []byte(`{"schema_version":1,"target_path":"/tmp/codex-proxy"}`)...)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write record: %v", err)
	}

	record, err := LoadRecord(path)
	if err != nil {
		t.Fatalf("LoadRecord with BOM error: %v", err)
	}
	if record.TargetPath != "/tmp/codex-proxy" {
		t.Fatalf("record = %#v, want target path", record)
	}
}

func TestSaveRecordWritesJSONWithoutBOM(t *testing.T) {
	path := filepath.Join(t.TempDir(), "install.json")
	if err := SaveRecord(path, Record{TargetPath: "/tmp/codex-proxy"}); err != nil {
		t.Fatalf("SaveRecord: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read record: %v", err)
	}
	if hasUTF8BOM(data) {
		t.Fatalf("SaveRecord wrote UTF-8 BOM: % x", data[:3])
	}
	var record Record
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatalf("saved record is not plain JSON: %v\n%s", err, data)
	}
}

func writeExecutable(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("WriteFile(%q): %v", path, err)
	}
}

type executableFileInfoForTest struct {
	os.FileInfo
}

func (info executableFileInfoForTest) Mode() os.FileMode {
	return info.FileInfo.Mode() | 0o111
}

func executableStatForTest(path string) (os.FileInfo, error) {
	info, err := os.Stat(path)
	if err != nil || info.IsDir() {
		return info, err
	}
	return executableFileInfoForTest{FileInfo: info}, nil
}

func hasUTF8BOM(data []byte) bool {
	return len(data) >= 3 && data[0] == 0xef && data[1] == 0xbb && data[2] == 0xbf
}
