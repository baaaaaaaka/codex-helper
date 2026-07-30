package teams

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

const teamsPackageTestRootEnv = "CXP_TEAMS_PACKAGE_TEST_ROOT"

func TestMain(m *testing.M) {
	// Subprocess helpers inherit the parent's already-isolated environment.
	// Reusing it keeps cross-process migration fixtures in the same roots and
	// prevents child test binaries from deleting a root they do not own.
	if os.Getenv(teamsPackageTestRootEnv) != "" {
		os.Exit(m.Run())
	}
	tmp, err := os.MkdirTemp("", "codex-helper-teams-tests-")
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "create Teams test temp dir: %v\n", err)
		os.Exit(2)
	}
	isolatedEnv := map[string]string{
		teamsPackageTestRootEnv: tmp,
		"HOME":                  filepath.Join(tmp, "home"),
		"USERPROFILE":           filepath.Join(tmp, "home"),
		"APPDATA":               filepath.Join(tmp, "appdata"),
		"LOCALAPPDATA":          filepath.Join(tmp, "localappdata"),
		"XDG_CONFIG_HOME":       filepath.Join(tmp, "config"),
		"XDG_CACHE_HOME":        filepath.Join(tmp, "cache"),
		"XDG_DATA_HOME":         filepath.Join(tmp, "data"),
		"XDG_STATE_HOME":        filepath.Join(tmp, "state"),
	}
	for name, value := range isolatedEnv {
		if name != teamsPackageTestRootEnv {
			if err := os.MkdirAll(value, 0o700); err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "create isolated %s: %v\n", name, err)
				_ = os.RemoveAll(tmp)
				os.Exit(2)
			}
		}
		if err := os.Setenv(name, value); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "isolate %s: %v\n", name, err)
			_ = os.RemoveAll(tmp)
			os.Exit(2)
		}
	}
	for _, name := range []string{
		"CODEX_HOME",
		"CODEX_DIR",
		"CODEX_CONFIG_DIR",
		"CODEX_HELPER_STATE_DIR",
	} {
		if err := os.Unsetenv(name); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "clear ambient %s: %v\n", name, err)
			_ = os.RemoveAll(tmp)
			os.Exit(2)
		}
	}

	code := m.Run()
	_ = os.RemoveAll(tmp)
	os.Exit(code)
}

func TestTeamsPackageTestMainIsolatesEveryUserDirectoryCI(t *testing.T) {
	root := os.Getenv(teamsPackageTestRootEnv)
	if root == "" {
		t.Fatal("TestMain must provide an isolated Teams package test root")
	}
	for _, name := range []string{
		"HOME",
		"USERPROFILE",
		"APPDATA",
		"LOCALAPPDATA",
		"XDG_CONFIG_HOME",
		"XDG_CACHE_HOME",
		"XDG_DATA_HOME",
		"XDG_STATE_HOME",
	} {
		value := os.Getenv(name)
		rel, err := filepath.Rel(root, value)
		if err != nil || rel == "." || rel == ".." || filepath.IsAbs(rel) {
			t.Fatalf("TestMain left %s outside %q: %q", name, root, value)
		}
	}
	for _, name := range []string{"CODEX_HOME", "CODEX_DIR", "CODEX_CONFIG_DIR", "CODEX_HELPER_STATE_DIR"} {
		if value := os.Getenv(name); value != "" {
			t.Fatalf("TestMain retained ambient %s=%q", name, value)
		}
	}
}

func isolateTeamsUserDirsForTest(t *testing.T, tmp string) (string, string) {
	t.Helper()
	t.Setenv("HOME", tmp)
	t.Setenv("USERPROFILE", tmp)
	t.Setenv("APPDATA", filepath.Join(tmp, "AppData", "Roaming"))
	t.Setenv("LOCALAPPDATA", filepath.Join(tmp, "AppData", "Local"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(tmp, "config"))
	t.Setenv("XDG_CACHE_HOME", tmp)
	t.Setenv("XDG_STATE_HOME", filepath.Join(tmp, "state"))
	t.Setenv("CODEX_HELPER_STATE_DIR", "")
	t.Setenv("CODEX_HELPER_TEAMS_TENANT_ID", "tenant")
	t.Setenv("CODEX_HELPER_TEAMS_CLIENT_ID", "chat-client")
	t.Setenv("CODEX_HELPER_TEAMS_READ_CLIENT_ID", "read-client")
	t.Setenv("CODEX_HELPER_TEAMS_FILE_WRITE_CLIENT_ID", "file-client")
	configBase, err := os.UserConfigDir()
	if err != nil {
		t.Fatalf("os.UserConfigDir: %v", err)
	}
	cacheBase, err := os.UserCacheDir()
	if err != nil {
		t.Fatalf("os.UserCacheDir: %v", err)
	}
	return configBase, cacheBase
}
