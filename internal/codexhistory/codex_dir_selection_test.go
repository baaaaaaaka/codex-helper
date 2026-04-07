package codexhistory

import (
	"os"
	"os/user"
	"path/filepath"
	"runtime"
	"testing"
)

func setResolveCodexDirHooksForTest(t *testing.T) {
	t.Helper()

	prevGetenv := resolveCodexDirGetenv
	prevUserHome := resolveCodexDirUserHomeDir
	prevLookupByID := resolveCodexDirLookupUserByID
	prevLookupByName := resolveCodexDirLookupUserByName
	prevRunningAsRoot := resolveCodexDirRunningAsRoot
	prevStat := resolveCodexDirStat
	prevSameFile := resolveCodexDirSameFile
	prevEvalSymlinks := resolveCodexDirEvalSymlinks
	t.Cleanup(func() {
		resolveCodexDirGetenv = prevGetenv
		resolveCodexDirUserHomeDir = prevUserHome
		resolveCodexDirLookupUserByID = prevLookupByID
		resolveCodexDirLookupUserByName = prevLookupByName
		resolveCodexDirRunningAsRoot = prevRunningAsRoot
		resolveCodexDirStat = prevStat
		resolveCodexDirSameFile = prevSameFile
		resolveCodexDirEvalSymlinks = prevEvalSymlinks
	})
}

func TestResolveCodexDirSelection_UsesCODEXHOMEEnv(t *testing.T) {
	setResolveCodexDirHooksForTest(t)
	env := map[string]string{
		EnvCodexDir:  "",
		envCodexHome: "/env/codex-home",
	}
	resolveCodexDirGetenv = func(key string) string { return env[key] }

	got, err := ResolveCodexDirSelection("")
	if err != nil {
		t.Fatalf("ResolveCodexDirSelection: %v", err)
	}
	want := filepath.Clean("/env/codex-home")
	if got.Dir != want {
		t.Fatalf("Dir = %q, want %q", got.Dir, want)
	}
	if got.Source != "env:"+envCodexHome {
		t.Fatalf("Source = %q, want %q", got.Source, "env:"+envCodexHome)
	}
}

func TestResolveCodexDirSelection_UsesTrustedUserHomeHintWhenRunningAsRoot(t *testing.T) {
	setResolveCodexDirHooksForTest(t)

	candidateHome := t.TempDir()
	env := map[string]string{
		EnvCodexDir:     "",
		envCodexHome:    "",
		EnvUserHomeHint: candidateHome,
	}
	resolveCodexDirGetenv = func(key string) string { return env[key] }
	resolveCodexDirRunningAsRoot = func() bool { return true }
	resolveCodexDirUserHomeDir = func() (string, error) { return "/root", nil }

	got, err := ResolveCodexDirSelection("")
	if err != nil {
		t.Fatalf("ResolveCodexDirSelection: %v", err)
	}
	want := filepath.Join(candidateHome, ".codex")
	if got.Dir != want {
		t.Fatalf("Dir = %q, want %q", got.Dir, want)
	}
	if got.Home != candidateHome {
		t.Fatalf("Home = %q, want %q", got.Home, candidateHome)
	}
	if got.Source != "env:"+EnvUserHomeHint {
		t.Fatalf("Source = %q, want %q", got.Source, "env:"+EnvUserHomeHint)
	}
}

func TestResolveCodexDirSelection_UsesTrustedUserHomeHintWhenNotRoot(t *testing.T) {
	setResolveCodexDirHooksForTest(t)

	candidateHome := t.TempDir()
	env := map[string]string{
		EnvCodexDir:     "",
		envCodexHome:    "",
		EnvUserHomeHint: candidateHome,
	}
	resolveCodexDirGetenv = func(key string) string { return env[key] }
	resolveCodexDirRunningAsRoot = func() bool { return false }
	resolveCodexDirUserHomeDir = func() (string, error) { return "/home/current", nil }

	got, err := ResolveCodexDirSelection("")
	if err != nil {
		t.Fatalf("ResolveCodexDirSelection: %v", err)
	}
	want := filepath.Join(candidateHome, ".codex")
	if got.Dir != want {
		t.Fatalf("Dir = %q, want %q", got.Dir, want)
	}
	if got.Home != candidateHome {
		t.Fatalf("Home = %q, want %q", got.Home, candidateHome)
	}
	if got.Source != "env:"+EnvUserHomeHint {
		t.Fatalf("Source = %q, want %q", got.Source, "env:"+EnvUserHomeHint)
	}
}

func TestResolveCodexDirSelection_KeepsAliasRootHomeWhenHintPointsToSameHome(t *testing.T) {
	setResolveCodexDirHooksForTest(t)
	if runtime.GOOS == "windows" {
		t.Skip("symlink alias test is unix-focused")
	}

	realHome := t.TempDir()
	currentHome := filepath.Join(t.TempDir(), "root-home")
	candidateHome := filepath.Join(t.TempDir(), "user-home")
	if err := os.Symlink(realHome, currentHome); err != nil {
		t.Fatalf("symlink current home: %v", err)
	}
	if err := os.Symlink(realHome, candidateHome); err != nil {
		t.Fatalf("symlink candidate home: %v", err)
	}
	if err := os.MkdirAll(filepath.Join(realHome, ".codex"), 0o755); err != nil {
		t.Fatalf("mkdir .codex: %v", err)
	}

	env := map[string]string{
		EnvCodexDir:     "",
		envCodexHome:    "",
		EnvUserHomeHint: candidateHome,
	}
	resolveCodexDirGetenv = func(key string) string { return env[key] }
	resolveCodexDirRunningAsRoot = func() bool { return true }
	resolveCodexDirUserHomeDir = func() (string, error) { return currentHome, nil }

	got, err := ResolveCodexDirSelection("")
	if err != nil {
		t.Fatalf("ResolveCodexDirSelection: %v", err)
	}
	want := filepath.Join(currentHome, ".codex")
	if got.Dir != want {
		t.Fatalf("Dir = %q, want %q", got.Dir, want)
	}
	if got.Home != currentHome {
		t.Fatalf("Home = %q, want %q", got.Home, currentHome)
	}
	if got.Source != "alias:env:"+EnvUserHomeHint {
		t.Fatalf("Source = %q, want %q", got.Source, "alias:env:"+EnvUserHomeHint)
	}
}

func TestResolveCodexDirSelection_UsesConsistentSUDOUserAndUID(t *testing.T) {
	setResolveCodexDirHooksForTest(t)

	candidateHome := t.TempDir()
	env := map[string]string{
		EnvCodexDir:  "",
		envCodexHome: "",
		"SUDO_UID":   "1000",
		"SUDO_USER":  "baka",
	}
	resolveCodexDirGetenv = func(key string) string { return env[key] }
	resolveCodexDirRunningAsRoot = func() bool { return true }
	resolveCodexDirUserHomeDir = func() (string, error) { return "/root", nil }
	resolveCodexDirLookupUserByID = func(uid string) (*user.User, error) {
		if uid != "1000" {
			t.Fatalf("unexpected uid lookup: %q", uid)
		}
		return &user.User{Uid: uid, HomeDir: candidateHome}, nil
	}
	resolveCodexDirLookupUserByName = func(name string) (*user.User, error) {
		if name != "baka" {
			t.Fatalf("unexpected user lookup: %q", name)
		}
		return &user.User{Uid: "1000", Username: name, HomeDir: candidateHome}, nil
	}

	got, err := ResolveCodexDirSelection("")
	if err != nil {
		t.Fatalf("ResolveCodexDirSelection: %v", err)
	}
	want := filepath.Join(candidateHome, ".codex")
	if got.Dir != want {
		t.Fatalf("Dir = %q, want %q", got.Dir, want)
	}
	if got.Source != "env:SUDO_USER+SUDO_UID" {
		t.Fatalf("Source = %q, want %q", got.Source, "env:SUDO_USER+SUDO_UID")
	}
}

func TestResolveCodexDirSelection_DoesNotGuessFromHOMEOrUSERWhenRunningAsRoot(t *testing.T) {
	setResolveCodexDirHooksForTest(t)

	env := map[string]string{
		EnvCodexDir:  "",
		envCodexHome: "",
		"HOME":       "/home/baka",
		"USER":       "baka",
		"LOGNAME":    "baka",
	}
	resolveCodexDirGetenv = func(key string) string { return env[key] }
	resolveCodexDirRunningAsRoot = func() bool { return true }
	resolveCodexDirUserHomeDir = func() (string, error) { return "/root", nil }

	got, err := ResolveCodexDirSelection("")
	if err != nil {
		t.Fatalf("ResolveCodexDirSelection: %v", err)
	}
	want := filepath.Join("/root", ".codex")
	if got.Dir != want {
		t.Fatalf("Dir = %q, want %q", got.Dir, want)
	}
	if got.Source != "default" {
		t.Fatalf("Source = %q, want %q", got.Source, "default")
	}
}

func TestResolveCodexDirSelection_FallsBackWhenSUDOUserAndUIDDoNotMatch(t *testing.T) {
	setResolveCodexDirHooksForTest(t)

	env := map[string]string{
		EnvCodexDir:  "",
		envCodexHome: "",
		"SUDO_UID":   "1000",
		"SUDO_USER":  "baka",
	}
	resolveCodexDirGetenv = func(key string) string { return env[key] }
	resolveCodexDirRunningAsRoot = func() bool { return true }
	resolveCodexDirUserHomeDir = func() (string, error) { return "/root", nil }
	resolveCodexDirLookupUserByID = func(uid string) (*user.User, error) {
		return &user.User{Uid: uid, HomeDir: "/home/baka"}, nil
	}
	resolveCodexDirLookupUserByName = func(name string) (*user.User, error) {
		return &user.User{Uid: "2000", Username: name, HomeDir: "/home/baka"}, nil
	}

	got, err := ResolveCodexDirSelection("")
	if err != nil {
		t.Fatalf("ResolveCodexDirSelection: %v", err)
	}
	want := filepath.Join("/root", ".codex")
	if got.Dir != want {
		t.Fatalf("Dir = %q, want %q", got.Dir, want)
	}
	if got.Source != "default" {
		t.Fatalf("Source = %q, want %q", got.Source, "default")
	}
}
