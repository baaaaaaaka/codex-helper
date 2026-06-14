package managedinstall

import (
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
	})
	if err != nil {
		t.Fatalf("Resolve error: %v", err)
	}
	if target.Path != envTarget || target.Source != SourceEnvInstallDir {
		t.Fatalf("target = %#v, want env dir target %q", target, envTarget)
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

func writeExecutable(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("MkdirAll(%q): %v", filepath.Dir(path), err)
	}
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatalf("WriteFile(%q): %v", path, err)
	}
}
