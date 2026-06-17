package cli

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/baaaaaaaka/codex-helper/internal/responsesadapter"
)

func TestDefaultResponsesStorePathMigratesSQLiteFamily(t *testing.T) {
	tmp := t.TempDir()
	legacyBase, newBase := isolateResponsesStoreDirsForTest(t, tmp)
	if err := os.MkdirAll(filepath.Dir(legacyBase), 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	for suffix, body := range map[string]string{
		"":     "db",
		"-wal": "wal",
		"-shm": "shm",
	} {
		if err := os.WriteFile(legacyBase+suffix, []byte(body), 0o600); err != nil {
			t.Fatalf("write legacy sqlite%s: %v", suffix, err)
		}
	}

	got := defaultResponsesStorePath()
	if got != newBase {
		t.Fatalf("defaultResponsesStorePath = %q, want %q", got, newBase)
	}
	for suffix, body := range map[string]string{"": "db", "-wal": "wal", "-shm": "shm"} {
		data, err := os.ReadFile(newBase + suffix)
		if err != nil {
			t.Fatalf("read migrated sqlite%s: %v", suffix, err)
		}
		if string(data) != body {
			t.Fatalf("migrated sqlite%s = %q, want %q", suffix, data, body)
		}
	}
}

func TestDefaultResponsesStorePathFallsBackWhenNewSidecarIsNonRegularCI(t *testing.T) {
	tmp := t.TempDir()
	legacyBase, newBase := isolateResponsesStoreDirsForTest(t, tmp)
	if err := os.MkdirAll(filepath.Dir(legacyBase), 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(newBase), 0o700); err != nil {
		t.Fatalf("mkdir new: %v", err)
	}
	for suffix, body := range map[string]string{
		"":     "db",
		"-wal": "wal",
	} {
		if err := os.WriteFile(legacyBase+suffix, []byte(body), 0o600); err != nil {
			t.Fatalf("write legacy sqlite%s: %v", suffix, err)
		}
	}
	if err := os.WriteFile(newBase, []byte("db"), 0o600); err != nil {
		t.Fatalf("write new base: %v", err)
	}
	if err := os.MkdirAll(newBase+"-wal", 0o700); err != nil {
		t.Fatalf("mkdir non-regular new wal: %v", err)
	}

	got := defaultResponsesStorePath()
	if got != legacyBase {
		t.Fatalf("defaultResponsesStorePath = %q, want legacy fallback %q", got, legacyBase)
	}
}

func TestDefaultResponsesStorePathSubprocessMigrationStressCI(t *testing.T) {
	if os.Getenv("CODEX_HELPER_RESPONSES_MIGRATION_WORKER") == "1" {
		t.Skip("parent stress only")
	}
	tmp := t.TempDir()
	home := filepath.Join(tmp, "home")
	cacheHome := filepath.Join(tmp, "cache")
	stateHome := filepath.Join(tmp, "state")
	localAppData := filepath.Join(tmp, "localappdata")
	appData := filepath.Join(tmp, "appdata")
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("XDG_CACHE_HOME", cacheHome)
	t.Setenv("XDG_STATE_HOME", stateHome)
	t.Setenv("LOCALAPPDATA", localAppData)
	t.Setenv("APPDATA", appData)
	legacyBase, newBase := responsesStorePathsForTest(t)
	if err := os.MkdirAll(filepath.Dir(legacyBase), 0o700); err != nil {
		t.Fatalf("mkdir legacy: %v", err)
	}
	for suffix, body := range map[string]string{
		"":     "db",
		"-wal": "wal",
		"-shm": "shm",
	} {
		if err := os.WriteFile(legacyBase+suffix, []byte(body), 0o600); err != nil {
			t.Fatalf("write legacy sqlite%s: %v", suffix, err)
		}
	}

	type proc struct {
		cmd *exec.Cmd
		out bytes.Buffer
	}
	procs := make([]proc, 6)
	for i := range procs {
		cmd := exec.Command(os.Args[0], "-test.run=TestDefaultResponsesStorePathSubprocessMigrationWorkerCI", "-test.v")
		cmd.Env = append(os.Environ(),
			"CODEX_HELPER_RESPONSES_MIGRATION_WORKER=1",
			"CODEX_HELPER_RESPONSES_TEST_HOME="+home,
			"CODEX_HELPER_RESPONSES_TEST_USERPROFILE="+home,
			"CODEX_HELPER_RESPONSES_TEST_CACHE_HOME="+cacheHome,
			"CODEX_HELPER_RESPONSES_TEST_STATE_HOME="+stateHome,
			"CODEX_HELPER_RESPONSES_TEST_LOCALAPPDATA="+localAppData,
			"CODEX_HELPER_RESPONSES_TEST_APPDATA="+appData,
		)
		cmd.Stdout = &procs[i].out
		cmd.Stderr = &procs[i].out
		procs[i].cmd = cmd
		if err := cmd.Start(); err != nil {
			t.Fatalf("start worker %d: %v", i, err)
		}
	}
	for i := range procs {
		if err := procs[i].cmd.Wait(); err != nil {
			t.Fatalf("worker %d failed: %v\n%s", i, err, procs[i].out.String())
		}
	}
	for suffix, body := range map[string]string{"": "db", "-wal": "wal", "-shm": "shm"} {
		data, err := os.ReadFile(newBase + suffix)
		if err != nil || string(data) != body {
			t.Fatalf("migrated sqlite%s = %q err=%v", suffix, data, err)
		}
	}
}

func TestDefaultResponsesStorePathSubprocessMigrationWorkerCI(t *testing.T) {
	if os.Getenv("CODEX_HELPER_RESPONSES_MIGRATION_WORKER") != "1" {
		t.Skip("subprocess worker only")
	}
	if home := os.Getenv("CODEX_HELPER_RESPONSES_TEST_HOME"); home != "" {
		if err := os.Setenv("HOME", home); err != nil {
			t.Fatalf("set HOME: %v", err)
		}
	}
	if userProfile := os.Getenv("CODEX_HELPER_RESPONSES_TEST_USERPROFILE"); userProfile != "" {
		if err := os.Setenv("USERPROFILE", userProfile); err != nil {
			t.Fatalf("set USERPROFILE: %v", err)
		}
	}
	if cacheHome := os.Getenv("CODEX_HELPER_RESPONSES_TEST_CACHE_HOME"); cacheHome != "" {
		if err := os.Setenv("XDG_CACHE_HOME", cacheHome); err != nil {
			t.Fatalf("set XDG_CACHE_HOME: %v", err)
		}
	}
	if stateHome := os.Getenv("CODEX_HELPER_RESPONSES_TEST_STATE_HOME"); stateHome != "" {
		if err := os.Setenv("XDG_STATE_HOME", stateHome); err != nil {
			t.Fatalf("set XDG_STATE_HOME: %v", err)
		}
	}
	if localAppData := os.Getenv("CODEX_HELPER_RESPONSES_TEST_LOCALAPPDATA"); localAppData != "" {
		if err := os.Setenv("LOCALAPPDATA", localAppData); err != nil {
			t.Fatalf("set LOCALAPPDATA: %v", err)
		}
	}
	if appData := os.Getenv("CODEX_HELPER_RESPONSES_TEST_APPDATA"); appData != "" {
		if err := os.Setenv("APPDATA", appData); err != nil {
			t.Fatalf("set APPDATA: %v", err)
		}
	}
	got := defaultResponsesStorePath()
	if !strings.HasSuffix(filepath.ToSlash(got), "/state/codex-helper/responses/adapter.sqlite") {
		t.Fatalf("defaultResponsesStorePath = %q, want migrated responses path", got)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if _, err := os.Stat(got + suffix); err != nil {
			legacy, legacyPathErr := appdirs.LegacyCachePath("responses-adapter.sqlite")
			if legacyPathErr == nil {
				legacy += suffix
			}
			_, legacyErr := os.Stat(legacy)
			t.Fatalf("migrated sqlite%s missing: %v; got=%q XDG_CACHE_HOME=%q XDG_STATE_HOME=%q LOCALAPPDATA=%q legacy=%q legacyPathErr=%v legacyErr=%v", suffix, err, got, os.Getenv("XDG_CACHE_HOME"), os.Getenv("XDG_STATE_HOME"), os.Getenv("LOCALAPPDATA"), legacy, legacyPathErr, legacyErr)
		}
	}
}

func isolateResponsesStoreDirsForTest(t *testing.T, tmp string) (string, string) {
	t.Helper()
	home := filepath.Join(tmp, "home")
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("XDG_CACHE_HOME", filepath.Join(tmp, "cache"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(tmp, "state"))
	t.Setenv("LOCALAPPDATA", filepath.Join(tmp, "localappdata"))
	t.Setenv("APPDATA", filepath.Join(tmp, "appdata"))
	return responsesStorePathsForTest(t)
}

func responsesStorePathsForTest(t *testing.T) (string, string) {
	t.Helper()
	legacyBase, err := appdirs.LegacyCachePath("responses-adapter.sqlite")
	if err != nil {
		t.Fatalf("legacy responses path: %v", err)
	}
	newBase, err := appdirs.StatePath("responses", "adapter.sqlite")
	if err != nil {
		t.Fatalf("state responses path: %v", err)
	}
	return legacyBase, newBase
}

func TestResponsesRegistryFromFileLoadsProvidersAndProxyKeys(t *testing.T) {
	t.Setenv("MIMO_TEST_KEY", "tp-mimo")
	t.Setenv("DEEPSEEK_TEST_KEY", "sk-ds")
	path := filepath.Join(t.TempDir(), "providers.json")
	if err := os.WriteFile(path, []byte(`{
		"default_provider":"mimo",
		"providers":[
			{"id":"mimo","profile":"mimo","api_key_env":"MIMO_TEST_KEY","default_model":"mimo-v2.5","models":["mimo-v2.5","mimo-v2.5-pro"]},
			{"id":"deepseek","profile":"deepseek","api_key_env":"DEEPSEEK_TEST_KEY","default_model":"deepseek-v4-flash","models":["deepseek-v4-flash"]}
		],
		"proxy_keys":{"mi-key":"mimo"}
	}`), 0o600); err != nil {
		t.Fatalf("write providers json: %v", err)
	}

	registry, err := responsesRegistryFromFile(path, "ds-key:deepseek,all-key:*", "salt")
	if err != nil {
		t.Fatalf("responsesRegistryFromFile: %v", err)
	}

	mimoReq := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	mimoReq.Header.Set("Authorization", "Bearer mi-key")
	runtime, err := registry.Resolve(mimoReq, responsesadapter.ResponsesRequest{Model: "mimo-v2.5-pro"})
	if err != nil {
		t.Fatalf("resolve mimo: %v", err)
	}
	if runtime.ProviderID != "mimo" || runtime.BaseURLHash != responsesadapter.BaseURLHash("https://token-plan-cn.xiaomimimo.com/v1") {
		t.Fatalf("mimo runtime = %#v", runtime)
	}
	if runtime.KeyFingerprint != responsesadapter.KeyFingerprint("tp-mimo", "salt") {
		t.Fatalf("mimo key fingerprint = %q", runtime.KeyFingerprint)
	}

	dsReq := httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
	dsReq.Header.Set("Authorization", "Bearer ds-key")
	runtime, err = registry.Resolve(dsReq, responsesadapter.ResponsesRequest{})
	if err != nil {
		t.Fatalf("resolve deepseek default: %v", err)
	}
	if runtime.ProviderID != "deepseek" || runtime.Model != "deepseek-v4-flash" {
		t.Fatalf("deepseek runtime = %#v", runtime)
	}
}

func TestParseResponsesProxyKeysRejectsMalformedEntries(t *testing.T) {
	if got, err := parseResponsesProxyKeys("a:mimo,b:*"); err != nil || got["a"] != "mimo" || got["b"] != "*" {
		t.Fatalf("parse valid = %#v err = %v", got, err)
	}
	if _, err := parseResponsesProxyKeys("missing-provider"); err == nil {
		t.Fatal("malformed proxy key entry should fail")
	}
}

func TestResponsesFacadeFromOptionsSetsScopeIdentity(t *testing.T) {
	store := responsesadapter.NewMemoryStore()
	facade, err := responsesFacadeFromOptions(responsesServeOptions{
		baseURL:   "https://api.deepseek.com/v1",
		provider:  "deepseek",
		model:     "deepseek-v4-flash",
		scopeSalt: "salt",
	}, "sk-test", store)
	if err != nil {
		t.Fatalf("responsesFacadeFromOptions: %v", err)
	}
	if facade.Store != store {
		t.Fatal("facade did not use provided store")
	}
	if facade.KeyFingerprint != responsesadapter.KeyFingerprint("sk-test", "salt") {
		t.Fatalf("key fingerprint = %q", facade.KeyFingerprint)
	}
	if facade.BaseURLHash != responsesadapter.BaseURLHash("https://api.deepseek.com/v1") {
		t.Fatalf("base URL hash = %q", facade.BaseURLHash)
	}
}
