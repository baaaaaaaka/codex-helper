package teams

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

func setTeamsAuthIDsForTest(t *testing.T) {
	t.Helper()
	t.Setenv("CODEX_HELPER_TEAMS_TENANT_ID", "tenant")
	t.Setenv("CODEX_HELPER_TEAMS_CLIENT_ID", "chat-client")
	t.Setenv("CODEX_HELPER_TEAMS_READ_CLIENT_ID", "read-client")
	t.Setenv("CODEX_HELPER_TEAMS_FILE_WRITE_CLIENT_ID", "file-client")
}

func TestWriteTokenCacheUsesPrivatePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not meaningful on Windows")
	}
	path := filepath.Join(t.TempDir(), "cache", "teams-token.json")
	if err := writeTokenCache(path, TokenCache{
		AccessToken:  "access",
		RefreshToken: "refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}); err != nil {
		t.Fatalf("write token cache: %v", err)
	}
	assertMode(t, filepath.Dir(path), 0o700)
	assertMode(t, path, 0o600)

	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("chmod token cache: %v", err)
	}
	if _, err := readTokenCache(path); err != nil {
		t.Fatalf("read token cache: %v", err)
	}
	assertMode(t, path, 0o600)
}

func TestTokenCacheStatusUsesSafeReader(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not meaningful on Windows")
	}
	path := filepath.Join(t.TempDir(), "teams-token.json")
	if err := os.WriteFile(path, []byte(`{"access_token":"access","expires_at":`+strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)+`}`), 0o644); err != nil {
		t.Fatalf("write token cache: %v", err)
	}
	status, err := TokenCacheStatus(path)
	if err != nil {
		t.Fatalf("TokenCacheStatus error: %v", err)
	}
	if !strings.Contains(status, "present") {
		t.Fatalf("status = %q", status)
	}
	assertMode(t, path, 0o600)

	link := filepath.Join(t.TempDir(), "link-token.json")
	if err := os.Symlink(path, link); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := TokenCacheStatus(link); err == nil || !strings.Contains(err.Error(), "must not be a symlink") {
		t.Fatalf("expected symlink rejection, got %v", err)
	}
}

func TestTokenCacheStatusDoesNotChmodNonTokenJSON(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not meaningful on Windows")
	}
	path := filepath.Join(t.TempDir(), "ordinary.json")
	if err := os.WriteFile(path, []byte(`{"hello":"world"}`), 0o644); err != nil {
		t.Fatalf("write ordinary json: %v", err)
	}

	_, err := TokenCacheStatus(path)
	if err == nil || !strings.Contains(err.Error(), "does not look like") {
		t.Fatalf("expected non-token cache error, got %v", err)
	}
	assertMode(t, path, 0o644)
}

func TestRemoveTokenCacheRefusesNonTokenJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ordinary.json")
	if err := os.WriteFile(path, []byte(`{"hello":"world"}`), 0o600); err != nil {
		t.Fatalf("write ordinary json: %v", err)
	}

	err := RemoveTokenCache(path)
	if err == nil || !strings.Contains(err.Error(), "does not look like") {
		t.Fatalf("expected non-token cache error, got %v", err)
	}
	if _, statErr := os.Stat(path); statErr != nil {
		t.Fatalf("ordinary file should not be removed, stat err = %v", statErr)
	}

	if err := writeTokenCache(path, TokenCache{AccessToken: "access", ExpiresAt: time.Now().Add(time.Hour).Unix()}); err != nil {
		t.Fatalf("write token cache: %v", err)
	}
	if err := RemoveTokenCache(path); err != nil {
		t.Fatalf("RemoveTokenCache valid cache error: %v", err)
	}
	if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
		t.Fatalf("token cache should be removed, stat err = %v", statErr)
	}
}

func TestTokenCacheAcceptsPreviouslyGrantedBroadScopes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "teams-token.json")
	if err := os.WriteFile(path, []byte(`{"access_token":"access","expires_at":`+strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10)+`,"scope":"openid profile offline_access User.Read Files.ReadWrite.All"}`), 0o600); err != nil {
		t.Fatalf("write token cache: %v", err)
	}
	status, err := TokenCacheStatus(path)
	if err != nil {
		t.Fatalf("TokenCacheStatus error: %v", err)
	}
	if !strings.Contains(status, "present") {
		t.Fatalf("status = %q, want present", status)
	}
}

func TestAccessTokenRefreshKeepsExistingRefreshToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "teams-token.json")
	if err := writeTokenCache(path, TokenCache{
		AccessToken:  "expired-access",
		RefreshToken: "old-refresh",
		ExpiresAt:    time.Now().Add(-time.Hour).Unix(),
	}); err != nil {
		t.Fatalf("write token cache: %v", err)
	}

	auth := NewAuthManager(AuthConfig{
		TenantID:  "tenant",
		ClientID:  "client",
		Scopes:    defaultScopes,
		CachePath: path,
	})
	auth.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		if got := req.URL.Path; got != "/tenant/oauth2/v2.0/token" {
			t.Fatalf("unexpected token endpoint: %s", got)
		}
		if err := req.ParseForm(); err != nil {
			t.Fatalf("parse token form: %v", err)
		}
		if got := req.Form.Get("grant_type"); got != "refresh_token" {
			t.Fatalf("unexpected grant type: %s", got)
		}
		if got := req.Form.Get("refresh_token"); got != "old-refresh" {
			t.Fatal("unexpected refresh token in request")
		}
		return jsonResponse(http.StatusOK, `{"access_token":"new-access","expires_in":3600,"token_type":"Bearer"}`), nil
	})}

	got, err := auth.AccessToken(context.Background(), nil, false)
	if err != nil {
		t.Fatalf("access token: %v", err)
	}
	if got != "new-access" {
		t.Fatal("unexpected access token")
	}
	cached, err := readTokenCache(path)
	if err != nil {
		t.Fatalf("read refreshed cache: %v", err)
	}
	if cached.RefreshToken != "old-refresh" {
		t.Fatal("refresh token was not preserved")
	}
}

func TestNonInteractiveAuthDoesNotStartDeviceLogin(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing-token.json")
	auth := nonInteractiveAuth{
		AuthManager: NewAuthManager(AuthConfig{
			TenantID:  "tenant",
			ClientID:  "client",
			Scopes:    defaultFileWriteScopes,
			CachePath: path,
		}),
		action: "Teams file upload",
	}
	auth.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		t.Fatalf("non-interactive auth should not call OAuth endpoint, got %s", req.URL.String())
		return nil, nil
	})}

	_, err := auth.AccessToken(context.Background(), nil, false)
	if err == nil {
		t.Fatal("expected missing cache error")
	}
	if !strings.Contains(err.Error(), "auth cache is missing") || !strings.Contains(err.Error(), "teams auth file-write") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestServiceAuthDoesNotStartDeviceLogin(t *testing.T) {
	t.Setenv("CODEX_HELPER_TEAMS_SERVICE", "1")
	path := filepath.Join(t.TempDir(), "missing-token.json")
	auth := NewAuthManager(AuthConfig{
		TenantID:  "tenant",
		ClientID:  "client",
		Scopes:    defaultScopes,
		CachePath: path,
	})
	auth.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		t.Fatalf("service auth should not call OAuth endpoint, got %s", req.URL.String())
		return nil, nil
	})}

	_, err := auth.AccessToken(context.Background(), nil, false)
	if err == nil {
		t.Fatal("expected missing cache error")
	}
	if !strings.Contains(err.Error(), "auth cache is missing") || !strings.Contains(err.Error(), "teams auth") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNonInteractiveAuthRejectsExpiredAccessTokenWithoutRefresh(t *testing.T) {
	path := filepath.Join(t.TempDir(), "teams-token.json")
	if err := writeTokenCache(path, TokenCache{
		AccessToken: "expired-access",
		ExpiresAt:   time.Now().Add(-time.Hour).Unix(),
	}); err != nil {
		t.Fatalf("write token cache: %v", err)
	}
	auth := nonInteractiveAuth{
		AuthManager: NewAuthManager(AuthConfig{
			TenantID:  "tenant",
			ClientID:  "client",
			Scopes:    defaultFileWriteScopes,
			CachePath: path,
		}),
		action: "Teams file upload",
	}
	auth.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		t.Fatalf("non-interactive auth should not call OAuth endpoint without a refresh token, got %s", req.URL.String())
		return nil, nil
	})}

	_, err := auth.AccessToken(context.Background(), nil, false)
	if err == nil {
		t.Fatal("expected expired cache error")
	}
	if !strings.Contains(err.Error(), "expired and has no refresh token") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNonInteractiveAuthUsesValidCachedAccessToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "teams-token.json")
	if err := writeTokenCache(path, TokenCache{
		AccessToken: "valid-access",
		ExpiresAt:   time.Now().Add(time.Hour).Unix(),
	}); err != nil {
		t.Fatalf("write token cache: %v", err)
	}
	auth := nonInteractiveAuth{
		AuthManager: NewAuthManager(AuthConfig{
			TenantID:  "tenant",
			ClientID:  "client",
			Scopes:    defaultFileWriteScopes,
			CachePath: path,
		}),
		action: "Teams file upload",
	}
	auth.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		t.Fatalf("non-interactive auth should not call OAuth endpoint with a valid token, got %s", req.URL.String())
		return nil, nil
	})}

	got, err := auth.AccessToken(context.Background(), nil, false)
	if err != nil {
		t.Fatalf("AccessToken error: %v", err)
	}
	if got != "valid-access" {
		t.Fatalf("AccessToken = %q", got)
	}
}

func TestWriteTokenCacheDoesNotChmodExistingDirectory(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("POSIX permission bits are not meaningful on Windows")
	}
	dir := filepath.Join(t.TempDir(), "existing")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatalf("mkdir existing cache dir: %v", err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("chmod existing cache dir: %v", err)
	}
	path := filepath.Join(dir, "teams-token.json")
	if err := writeTokenCache(path, TokenCache{
		AccessToken:  "access",
		RefreshToken: "refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
	}); err != nil {
		t.Fatalf("write token cache: %v", err)
	}
	assertMode(t, dir, 0o755)
	assertMode(t, path, 0o600)
}

func TestDefaultAuthConfigRejectsUnexpectedScopes(t *testing.T) {
	setTeamsAuthIDsForTest(t)
	t.Setenv("CODEX_HELPER_TEAMS_SCOPES", "openid profile offline_access User.Read Files.Read.All")
	t.Setenv("CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES", "")

	_, err := DefaultAuthConfig()
	if err == nil {
		t.Fatal("expected unexpected scope error")
	}
	if !strings.Contains(err.Error(), "Files.Read.All") {
		t.Fatalf("error should name unexpected scope, got %v", err)
	}
}

func TestDefaultAuthConfigAllowsDocumentedScopes(t *testing.T) {
	setTeamsAuthIDsForTest(t)
	t.Setenv("CODEX_HELPER_TEAMS_SCOPES", "openid profile offline_access User.Read Chat.ReadWrite Chat.Create")
	t.Setenv("CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES", "")

	cfg, err := DefaultAuthConfig()
	if err != nil {
		t.Fatalf("DefaultAuthConfig error: %v", err)
	}
	if cfg.Scopes != "openid profile offline_access User.Read Chat.ReadWrite Chat.Create" {
		t.Fatalf("scopes = %q", cfg.Scopes)
	}
}

func TestDefaultAuthConfigRequiresConfiguredTenantAndClientID(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	t.Setenv("CODEX_HELPER_TEAMS_TENANT_ID", "")
	t.Setenv("CODEX_HELPER_TEAMS_CLIENT_ID", "")
	t.Setenv("CODEX_HELPER_TEAMS_AUTH_CONFIG", filepath.Join(tmp, "missing-auth.json"))

	_, err := DefaultAuthConfig()
	if err == nil {
		t.Fatal("expected missing Teams auth config error")
	}
	if !strings.Contains(err.Error(), "tenant id is not configured") {
		t.Fatalf("missing config error = %v", err)
	}
}

func TestDefaultAuthConfigUsesLocalAuthConfigFile(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	configPath := filepath.Join(tmp, "teams-auth.json")
	t.Setenv("CODEX_HELPER_TEAMS_AUTH_CONFIG", configPath)
	t.Setenv("CODEX_HELPER_TEAMS_TENANT_ID", "")
	t.Setenv("CODEX_HELPER_TEAMS_CLIENT_ID", "")
	t.Setenv("CODEX_HELPER_TEAMS_READ_CLIENT_ID", "")
	t.Setenv("CODEX_HELPER_TEAMS_FILE_WRITE_CLIENT_ID", "")
	if err := SaveTeamsAuthConfigFile(configPath, TeamsAuthConfigFile{
		TenantID: "tenant-from-file",
		Read: TeamsAuthCredentialConfig{
			ClientID: "read-from-file",
		},
		ChatWrite: TeamsAuthCredentialConfig{
			ClientID: "chat-from-file",
		},
	}); err != nil {
		t.Fatalf("SaveTeamsAuthConfigFile error: %v", err)
	}

	chatCfg, err := DefaultAuthConfig()
	if err != nil {
		t.Fatalf("DefaultAuthConfig error: %v", err)
	}
	if chatCfg.TenantID != "tenant-from-file" || chatCfg.ClientID != "chat-from-file" {
		t.Fatalf("chat config = %#v", chatCfg)
	}
	readCfg, err := DefaultReadAuthConfig()
	if err != nil {
		t.Fatalf("DefaultReadAuthConfig error: %v", err)
	}
	if readCfg.TenantID != "tenant-from-file" || readCfg.ClientID != "read-from-file" {
		t.Fatalf("read config = %#v", readCfg)
	}
	fileCfg, err := DefaultFileWriteAuthConfig()
	if err != nil {
		t.Fatalf("DefaultFileWriteAuthConfig error: %v", err)
	}
	if fileCfg.TenantID != "tenant-from-file" || fileCfg.ClientID != "chat-from-file" {
		t.Fatalf("file config should fall back to chat client id: %#v", fileCfg)
	}
	fullCfg, err := DefaultFullAuthConfig()
	if err != nil {
		t.Fatalf("DefaultFullAuthConfig error: %v", err)
	}
	if fullCfg.TenantID != "tenant-from-file" || fullCfg.ClientID != "chat-from-file" {
		t.Fatalf("full config should fall back to chat client id: %#v", fullCfg)
	}
}

func TestDefaultFullAuthConfigUsesSeparateCacheAndScopes(t *testing.T) {
	tmp := t.TempDir()
	_, cacheBase := isolateTeamsUserDirsForTest(t, tmp)
	setTeamsAuthIDsForTest(t)
	t.Setenv("CODEX_HELPER_TEAMS_FULL_SCOPES", "")
	t.Setenv("CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE", "")
	t.Setenv("CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES", "")

	cfg, err := DefaultFullAuthConfig()
	if err != nil {
		t.Fatalf("DefaultFullAuthConfig error: %v", err)
	}
	if cfg.Scopes != defaultFullScopes {
		t.Fatalf("full scopes = %q, want %q", cfg.Scopes, defaultFullScopes)
	}
	want := filepath.Join(cacheBase, "codex-helper", "teams", "profiles", "default", fullTokenCacheName)
	if cfg.CachePath != want {
		t.Fatalf("full cache path = %q, want %q", cfg.CachePath, want)
	}
}

func TestDefaultFileWriteAuthConfigUsesSeparateCacheAndScopes(t *testing.T) {
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	setTeamsAuthIDsForTest(t)
	t.Setenv("CODEX_HELPER_TEAMS_FILE_WRITE_SCOPES", "")
	t.Setenv("CODEX_HELPER_TEAMS_FILE_WRITE_TOKEN_CACHE", "")
	t.Setenv("CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES", "")

	cfg, err := DefaultFileWriteAuthConfig()
	if err != nil {
		t.Fatalf("DefaultFileWriteAuthConfig error: %v", err)
	}
	if cfg.Scopes != defaultFileWriteScopes {
		t.Fatalf("file-write scopes = %q", cfg.Scopes)
	}
	if !strings.HasSuffix(cfg.CachePath, fileWriteTokenCacheName) {
		t.Fatalf("file-write cache path = %q", cfg.CachePath)
	}
	if strings.Contains(cfg.CachePath, "teams-chat-write-token.json") {
		t.Fatalf("file-write cache should be separate from chat token cache: %q", cfg.CachePath)
	}
}

func TestEffectiveAuthConfigsFallBackToFullTokenCache(t *testing.T) {
	tmp := t.TempDir()
	_, cacheBase := isolateTeamsUserDirsForTest(t, tmp)
	setTeamsAuthIDsForTest(t)
	t.Setenv("CODEX_HELPER_TEAMS_READ_TOKEN_CACHE", "")
	t.Setenv("CODEX_HELPER_TEAMS_TOKEN_CACHE", "")
	t.Setenv("CODEX_HELPER_TEAMS_FILE_WRITE_TOKEN_CACHE", "")
	fullPath := filepath.Join(cacheBase, "codex-helper", "teams", "profiles", "default", fullTokenCacheName)
	if err := writeTokenCache(fullPath, TokenCache{
		AccessToken:  "full-access",
		RefreshToken: "full-refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
		Scope:        defaultFullScopes,
	}); err != nil {
		t.Fatalf("write full token cache: %v", err)
	}

	readCfg, err := DefaultEffectiveReadAuthConfig()
	if err != nil {
		t.Fatalf("DefaultEffectiveReadAuthConfig error: %v", err)
	}
	if readCfg.CachePath != fullPath || readCfg.Scopes != defaultFullScopes {
		t.Fatalf("effective read config = %#v, want full cache/scopes", readCfg)
	}
	chatCfg, err := DefaultEffectiveAuthConfig()
	if err != nil {
		t.Fatalf("DefaultEffectiveAuthConfig error: %v", err)
	}
	if chatCfg.CachePath != fullPath || chatCfg.Scopes != defaultFullScopes {
		t.Fatalf("effective chat config = %#v, want full cache/scopes", chatCfg)
	}
	fileCfg, err := DefaultEffectiveFileWriteAuthConfig()
	if err != nil {
		t.Fatalf("DefaultEffectiveFileWriteAuthConfig error: %v", err)
	}
	if fileCfg.CachePath != fullPath || fileCfg.Scopes != defaultFullScopes {
		t.Fatalf("effective file config = %#v, want full cache/scopes", fileCfg)
	}
}

func TestEffectiveAuthConfigsPreferDedicatedTokenCache(t *testing.T) {
	tmp := t.TempDir()
	_, cacheBase := isolateTeamsUserDirsForTest(t, tmp)
	setTeamsAuthIDsForTest(t)
	readPath := filepath.Join(cacheBase, "codex-helper", "teams", "profiles", "default", readTokenCacheName)
	fullPath := filepath.Join(cacheBase, "codex-helper", "teams", "profiles", "default", fullTokenCacheName)
	if err := writeTokenCache(readPath, TokenCache{
		AccessToken: "read-access",
		ExpiresAt:   time.Now().Add(time.Hour).Unix(),
		Scope:       defaultReadScopes,
	}); err != nil {
		t.Fatalf("write read token cache: %v", err)
	}
	if err := writeTokenCache(fullPath, TokenCache{
		AccessToken: "full-access",
		ExpiresAt:   time.Now().Add(time.Hour).Unix(),
		Scope:       defaultFullScopes,
	}); err != nil {
		t.Fatalf("write full token cache: %v", err)
	}

	cfg, err := DefaultEffectiveReadAuthConfig()
	if err != nil {
		t.Fatalf("DefaultEffectiveReadAuthConfig error: %v", err)
	}
	if cfg.CachePath != readPath {
		t.Fatalf("effective read cache path = %q, want dedicated %q", cfg.CachePath, readPath)
	}
}

func TestEffectiveAuthConfigsHonorExplicitTokenCacheOverride(t *testing.T) {
	tmp := t.TempDir()
	_, cacheBase := isolateTeamsUserDirsForTest(t, tmp)
	setTeamsAuthIDsForTest(t)
	explicitRead := filepath.Join(tmp, "explicit-read-token.json")
	fullPath := filepath.Join(cacheBase, "codex-helper", "teams", "profiles", "default", fullTokenCacheName)
	if err := writeTokenCache(fullPath, TokenCache{
		AccessToken: "full-access",
		ExpiresAt:   time.Now().Add(time.Hour).Unix(),
		Scope:       defaultFullScopes,
	}); err != nil {
		t.Fatalf("write full token cache: %v", err)
	}
	t.Setenv("CODEX_HELPER_TEAMS_READ_TOKEN_CACHE", explicitRead)

	cfg, err := DefaultEffectiveReadAuthConfig()
	if err != nil {
		t.Fatalf("DefaultEffectiveReadAuthConfig error: %v", err)
	}
	if cfg.CachePath != explicitRead {
		t.Fatalf("effective read cache path = %q, want explicit override %q", cfg.CachePath, explicitRead)
	}
}

func TestEffectiveFileWriteAuthConfigPromotesBroadChatTokenCache(t *testing.T) {
	tmp := t.TempDir()
	_, cacheBase := isolateTeamsUserDirsForTest(t, tmp)
	setTeamsAuthIDsForTest(t)
	chatPath := filepath.Join(cacheBase, "codex-helper", "teams", "profiles", "default", chatWriteTokenCacheName)
	if err := writeTokenCache(chatPath, TokenCache{
		AccessToken:  "chat-access",
		RefreshToken: "chat-refresh",
		ExpiresAt:    time.Now().Add(time.Hour).Unix(),
		Scope:        "User.Read Chat.ReadWrite OnlineMeetings.ReadWrite Files.ReadWrite",
	}); err != nil {
		t.Fatalf("write chat token cache: %v", err)
	}

	cfg, err := DefaultEffectiveFileWriteAuthConfig()
	if err != nil {
		t.Fatalf("DefaultEffectiveFileWriteAuthConfig error: %v", err)
	}
	if cfg.CachePath != chatPath {
		t.Fatalf("effective file cache path = %q, want broad chat cache %q", cfg.CachePath, chatPath)
	}
	if cfg.Scopes != defaultFullScopes {
		t.Fatalf("effective file scopes = %q, want full scopes %q", cfg.Scopes, defaultFullScopes)
	}
}

func TestEffectiveFileWriteAuthConfigDoesNotPromoteNarrowChatTokenCache(t *testing.T) {
	tmp := t.TempDir()
	_, cacheBase := isolateTeamsUserDirsForTest(t, tmp)
	setTeamsAuthIDsForTest(t)
	chatPath := filepath.Join(cacheBase, "codex-helper", "teams", "profiles", "default", chatWriteTokenCacheName)
	fullPath := filepath.Join(cacheBase, "codex-helper", "teams", "profiles", "default", fullTokenCacheName)
	if err := writeTokenCache(chatPath, TokenCache{
		AccessToken: "chat-access",
		ExpiresAt:   time.Now().Add(time.Hour).Unix(),
		Scope:       "User.Read Chat.ReadWrite OnlineMeetings.ReadWrite",
	}); err != nil {
		t.Fatalf("write chat token cache: %v", err)
	}

	cfg, err := DefaultEffectiveFileWriteAuthConfig()
	if err != nil {
		t.Fatalf("DefaultEffectiveFileWriteAuthConfig error: %v", err)
	}
	if cfg.CachePath != fullPath {
		t.Fatalf("effective file cache path = %q, want missing full cache path %q", cfg.CachePath, fullPath)
	}
}

func TestDefaultReadAuthConfigUsesReadClientCacheAndScopes(t *testing.T) {
	tmp := t.TempDir()
	_, cacheBase := isolateTeamsUserDirsForTest(t, tmp)
	t.Setenv("CODEX_HELPER_TEAMS_READ_CLIENT_ID", "")
	t.Setenv("CODEX_HELPER_TEAMS_READ_SCOPES", "")
	t.Setenv("CODEX_HELPER_TEAMS_READ_TOKEN_CACHE", "")
	t.Setenv("CODEX_HELPER_TEAMS_READ_CLIENT_ID", "read-client-configured")
	t.Setenv("CODEX_HELPER_TEAMS_CLIENT_ID", "write-client-override")
	t.Setenv("CODEX_HELPER_TEAMS_TENANT_ID", "tenant")
	t.Setenv("CODEX_HELPER_TEAMS_SCOPES", "openid profile offline_access User.Read Chat.ReadWrite")
	t.Setenv("CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES", "")

	cfg, err := DefaultReadAuthConfig()
	if err != nil {
		t.Fatalf("DefaultReadAuthConfig error: %v", err)
	}
	if cfg.ClientID != "read-client-configured" {
		t.Fatalf("read client id = %q, want configured read client", cfg.ClientID)
	}
	if cfg.Scopes != defaultReadScopes {
		t.Fatalf("read scopes = %q, want %q", cfg.Scopes, defaultReadScopes)
	}
	want := filepath.Join(cacheBase, "codex-helper", "teams", "profiles", "default", readTokenCacheName)
	if cfg.CachePath != want {
		t.Fatalf("read cache path = %q, want %q", cfg.CachePath, want)
	}
}

func TestDefaultReadAuthConfigAllowsTeamsCLIReadScopes(t *testing.T) {
	setTeamsAuthIDsForTest(t)
	t.Setenv("CODEX_HELPER_TEAMS_READ_SCOPES", "openid profile offline_access User.Read Chat.Read Channel.ReadBasic.All ChannelMessage.Read.All")
	t.Setenv("CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES", "")

	cfg, err := DefaultReadAuthConfig()
	if err != nil {
		t.Fatalf("DefaultReadAuthConfig error: %v", err)
	}
	if !strings.Contains(cfg.Scopes, "ChannelMessage.Read.All") {
		t.Fatalf("read scopes = %q", cfg.Scopes)
	}
}

func TestDefaultAuthConfigUsesProfileScopedCache(t *testing.T) {
	tmp := t.TempDir()
	_, cacheBase := isolateTeamsUserDirsForTest(t, tmp)
	setTeamsAuthIDsForTest(t)
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "")
	t.Setenv("CODEX_HELPER_TEAMS_AUTH_PROFILE", "")
	t.Setenv("CODEX_HELPER_TEAMS_TOKEN_CACHE", "")

	cfg, err := DefaultAuthConfig()
	if err != nil {
		t.Fatalf("DefaultAuthConfig error: %v", err)
	}
	want := filepath.Join(cacheBase, "codex-helper", "teams", "profiles", "default", chatWriteTokenCacheName)
	if cfg.CachePath != want {
		t.Fatalf("cache path = %q, want %q", cfg.CachePath, want)
	}
}

func TestDefaultAuthConfigKeepsExistingLegacyDefaultCache(t *testing.T) {
	tmp := t.TempDir()
	_, cacheBase := isolateTeamsUserDirsForTest(t, tmp)
	setTeamsAuthIDsForTest(t)
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "")
	t.Setenv("CODEX_HELPER_TEAMS_AUTH_PROFILE", "")
	t.Setenv("CODEX_HELPER_TEAMS_TOKEN_CACHE", "")
	legacy := filepath.Join(cacheBase, "codex-helper", chatWriteTokenCacheName)
	if err := os.MkdirAll(filepath.Dir(legacy), 0o700); err != nil {
		t.Fatalf("mkdir legacy cache dir: %v", err)
	}
	if err := os.WriteFile(legacy, []byte(`{}`), 0o600); err != nil {
		t.Fatalf("write legacy cache: %v", err)
	}

	cfg, err := DefaultAuthConfig()
	if err != nil {
		t.Fatalf("DefaultAuthConfig error: %v", err)
	}
	if cfg.CachePath != legacy {
		t.Fatalf("cache path = %q, want legacy %q", cfg.CachePath, legacy)
	}
}

func TestDefaultAuthConfigUsesLegacyDefaultCacheWithBroadCachedScopes(t *testing.T) {
	tmp := t.TempDir()
	_, cacheBase := isolateTeamsUserDirsForTest(t, tmp)
	setTeamsAuthIDsForTest(t)
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "")
	t.Setenv("CODEX_HELPER_TEAMS_AUTH_PROFILE", "")
	t.Setenv("CODEX_HELPER_TEAMS_TOKEN_CACHE", "")
	t.Setenv("CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES", "")
	legacy := filepath.Join(cacheBase, "codex-helper", chatWriteTokenCacheName)
	if err := os.MkdirAll(filepath.Dir(legacy), 0o700); err != nil {
		t.Fatalf("mkdir legacy cache dir: %v", err)
	}
	if err := os.WriteFile(legacy, []byte(`{"access_token":"legacy","expires_at":9999999999,"scope":"Files.ReadWrite.All"}`), 0o600); err != nil {
		t.Fatalf("write legacy cache: %v", err)
	}

	cfg, err := DefaultAuthConfig()
	if err != nil {
		t.Fatalf("DefaultAuthConfig error: %v", err)
	}
	if cfg.CachePath != legacy {
		t.Fatalf("cache path = %q, want legacy %q", cfg.CachePath, legacy)
	}
}

func TestDefaultAuthConfigUsesTeamsProfileForCache(t *testing.T) {
	tmp := t.TempDir()
	_, cacheBase := isolateTeamsUserDirsForTest(t, tmp)
	setTeamsAuthIDsForTest(t)
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "research/teams")
	t.Setenv("CODEX_HELPER_TEAMS_AUTH_PROFILE", "")
	t.Setenv("CODEX_HELPER_TEAMS_TOKEN_CACHE", "")

	cfg, err := DefaultAuthConfig()
	if err != nil {
		t.Fatalf("DefaultAuthConfig error: %v", err)
	}
	want := filepath.Join(cacheBase, "codex-helper", "teams", "profiles", "research_teams", chatWriteTokenCacheName)
	if cfg.CachePath != want {
		t.Fatalf("cache path = %q, want %q", cfg.CachePath, want)
	}
}

func TestDefaultFileWriteAuthConfigUsesAuthProfileOverride(t *testing.T) {
	tmp := t.TempDir()
	_, cacheBase := isolateTeamsUserDirsForTest(t, tmp)
	setTeamsAuthIDsForTest(t)
	t.Setenv("CODEX_HELPER_TEAMS_PROFILE", "teams-profile")
	t.Setenv("CODEX_HELPER_TEAMS_AUTH_PROFILE", "auth-profile")
	t.Setenv("CODEX_HELPER_TEAMS_FILE_WRITE_TOKEN_CACHE", "")

	cfg, err := DefaultFileWriteAuthConfig()
	if err != nil {
		t.Fatalf("DefaultFileWriteAuthConfig error: %v", err)
	}
	want := filepath.Join(cacheBase, "codex-helper", "teams", "profiles", "auth-profile", fileWriteTokenCacheName)
	if cfg.CachePath != want {
		t.Fatalf("file-write cache path = %q, want %q", cfg.CachePath, want)
	}
}

func TestDefaultAuthConfigUnsafeScopeOverride(t *testing.T) {
	setTeamsAuthIDsForTest(t)
	t.Setenv("CODEX_HELPER_TEAMS_SCOPES", "openid profile offline_access User.Read Files.Read.All")
	t.Setenv("CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES", "1")

	if _, err := DefaultAuthConfig(); err != nil {
		t.Fatalf("DefaultAuthConfig with unsafe override error: %v", err)
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("unexpected mode for %s: got %03o want %03o", path, got, want)
	}
}
