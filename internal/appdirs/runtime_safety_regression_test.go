package appdirs

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestTeamsRuntimeSafetyTestPathGuardRejectsUserDirsOutsideIsolationRootCI(t *testing.T) {
	testRoot := filepath.Join(t.TempDir(), "allowed")
	outside := filepath.Join(t.TempDir(), "caller-owned")
	t.Setenv("CODEX_HELPER_TEST_USER_DIR_ROOT", testRoot)
	t.Setenv("HOME", outside)
	t.Setenv("USERPROFILE", outside)
	t.Setenv("APPDATA", filepath.Join(outside, "appdata"))
	t.Setenv("LOCALAPPDATA", filepath.Join(outside, "localappdata"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(outside, "config"))
	t.Setenv("XDG_CACHE_HOME", filepath.Join(outside, "cache"))
	t.Setenv("XDG_DATA_HOME", filepath.Join(outside, "data"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(outside, "state"))

	tests := []struct {
		name    string
		resolve func() (string, error)
	}{
		{name: "state", resolve: func() (string, error) { return StatePath("teams", "state.json") }},
		{name: "legacy_config", resolve: func() (string, error) { return LegacyConfigPath("teams", "local-supervisor.json") }},
		{name: "legacy_cache", resolve: func() (string, error) { return LegacyCachePath("teams", "cache.json") }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path, err := tc.resolve()
			if err == nil || !strings.Contains(strings.ToLower(err.Error()), "test") ||
				!strings.Contains(strings.ToLower(err.Error()), "outside") {
				t.Fatalf("resolved caller-owned path %q with error %v; test path guard must reject it before any write", path, err)
			}
		})
	}
}
