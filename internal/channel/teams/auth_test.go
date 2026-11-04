package teams

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

func writeCache(t *testing.T, path string, tok TokenCache) {
	t.Helper()
	if err := WriteTokenCache(path, tok); err != nil {
		t.Fatal(err)
	}
}

func readCache(t *testing.T, path string) TokenCache {
	t.Helper()
	tok, err := readTokenCache(path)
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func authAdapter(t *testing.T, fg *fakegraph.Server, cachePath string) (*Adapter, *DelegatedAuth) {
	t.Helper()
	auth := NewDelegatedAuth(AuthConfig{
		TenantID: fakegraph.TenantID, ClientID: "client", Scopes: DefaultScopes,
		CachePath: cachePath, LoginURL: fg.URL(),
	}, &http.Client{Transport: loginHostTransport{http.DefaultTransport}})
	a, err := New(Config{GraphURL: fg.URL() + "/v1.0", Tokens: auth, Sleep: noSleep})
	if err != nil {
		t.Fatal(err)
	}
	return a, auth
}

func TestDelegatedAuthRefreshesOn401ForReads(t *testing.T) {
	fg := fakegraph.New(fakegraph.Options{})
	t.Cleanup(fg.Close)
	_, refresh := fg.IssueTokens("cached")
	path := filepath.Join(t.TempDir(), "teams-full-token.json")
	writeCache(t, path, TokenCache{AccessToken: "revoked", RefreshToken: refresh, ExpiresAt: time.Now().Add(time.Hour).Unix()})
	a, _ := authAdapter(t, fg, path)
	chat := fg.CreateMeetingChatAsUser("auth").ID
	if _, err := a.Receive(context.Background(), chat, ""); err != nil {
		t.Fatal(err)
	}
	tok := readCache(t, path)
	if tok.AccessToken == "revoked" || tok.RefreshToken == refresh || tok.RefreshToken == "" {
		t.Fatalf("cache not refreshed: %+v", tok)
	}
	st, _ := os.Stat(path)
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("cache mode %v", st.Mode())
	}
	noViolations(t, fg)
}

func TestDelegatedAuth401OnWriteIsRejectedNotRetried(t *testing.T) {
	ctx := context.Background()
	fg := fakegraph.New(fakegraph.Options{})
	t.Cleanup(fg.Close)
	access, refresh := fg.IssueTokens("cached")
	path := filepath.Join(t.TempDir(), "tok.json")
	writeCache(t, path, TokenCache{AccessToken: access, RefreshToken: refresh, ExpiresAt: time.Now().Add(time.Hour).Unix()})
	a, _ := authAdapter(t, fg, path)
	chat := fg.CreateMeetingChatAsUser("auth").ID
	p := renderOne(t, a, channel.RichDoc{Label: "L", Markdown: "x"})
	// Revoke the access token behind the adapter's back.
	writeCache(t, path, TokenCache{AccessToken: "revoked", RefreshToken: refresh, ExpiresAt: time.Now().Add(time.Hour).Unix()})
	a2, _ := authAdapter(t, fg, path)
	_, err := a2.Send(ctx, chat, p, "t1")
	var r *channel.Rejected
	if !errors.As(err, &r) || r.Permanent || channel.IsAmbiguous(err) {
		t.Fatalf("err = %v, want transient Rejected", err)
	}
	if len(fg.Messages(chat)) != 0 || countRequests(fg, routePost) != 1 {
		t.Fatal("401 write was retried or stored")
	}
	if _, err := a2.Send(ctx, chat, p, "t2"); err != nil {
		t.Fatalf("send after refresh: %v", err)
	}
}

func TestDelegatedAuthProactiveRefreshAndFailures(t *testing.T) {
	ctx := context.Background()
	fg := fakegraph.New(fakegraph.Options{})
	t.Cleanup(fg.Close)
	_, refresh := fg.IssueTokens("cached")
	dir := t.TempDir()

	path := filepath.Join(dir, "expired.json")
	writeCache(t, path, TokenCache{AccessToken: "old", RefreshToken: refresh, ExpiresAt: time.Now().Add(time.Minute).Unix()})
	_, auth := authAdapter(t, fg, path)
	tok, err := auth.Token(ctx)
	if err != nil || tok == "old" {
		t.Fatalf("token=%q err=%v", tok, err)
	}
	again, _ := auth.Token(ctx)
	if again != tok {
		t.Fatal("fresh token was refreshed again")
	}

	var ar *channel.AuthRequired
	bad := filepath.Join(dir, "bad.json")
	writeCache(t, bad, TokenCache{RefreshToken: "unknown", ExpiresAt: 1})
	_, auth = authAdapter(t, fg, bad)
	if _, err := auth.Token(ctx); !errors.As(err, &ar) {
		t.Fatalf("invalid_grant: %v", err)
	}
	_, auth = authAdapter(t, fg, filepath.Join(dir, "missing.json"))
	if _, err := auth.Token(ctx); !errors.As(err, &ar) {
		t.Fatalf("missing cache: %v", err)
	}
	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(path, link); err == nil {
		_, auth = authAdapter(t, fg, link)
		if _, err := auth.Token(ctx); !errors.As(err, &ar) {
			t.Fatalf("symlink cache: %v", err)
		}
		if err := WriteTokenCache(link, TokenCache{}); err == nil {
			t.Fatal("wrote through a symlink")
		}
	}
	fg.AddFault(fakegraph.Fault{Name: "login503", Kind: fakegraph.FaultStatus, Host: "login.microsoftonline.com", Status: 503, Times: 1})
	writeCache(t, path, TokenCache{AccessToken: "old", RefreshToken: readCache(t, path).RefreshToken, ExpiresAt: 1})
	_, auth = authAdapter(t, fg, path)
	if _, err := auth.Token(ctx); err == nil || errors.As(err, &ar) {
		t.Fatalf("login 503 must be transient, got %v", err)
	}
	if _, err := auth.Token(ctx); err != nil {
		t.Fatalf("after transient failure: %v", err)
	}
	noViolations(t, fg)
}

func TestResolveAuthConfigReadsExistingCaches(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	cache := filepath.Join(root, "cache")
	cfgPath := filepath.Join(root, "teams-auth.json")
	t.Setenv("CODEX_HELPER_STATE_DIR", state)
	t.Setenv("XDG_CACHE_HOME", cache)
	t.Setenv("CODEX_HELPER_TEAMS_AUTH_CONFIG", cfgPath)
	for _, k := range []string{"CODEX_HELPER_TEAMS_AUTH_PROFILE", "CODEX_HELPER_TEAMS_PROFILE", "CODEX_HELPER_TEAMS_FULL_TENANT_ID",
		"CODEX_HELPER_TEAMS_TENANT_ID", "CODEX_HELPER_TEAMS_FULL_CLIENT_ID", "CODEX_HELPER_TEAMS_CLIENT_ID",
		"CODEX_HELPER_TEAMS_FULL_SCOPES", "CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE"} {
		t.Setenv(k, "")
	}

	if _, err := ResolveAuthConfig(""); err == nil {
		t.Fatal("expected error without tenant/client")
	}
	raw, _ := json.Marshal(map[string]any{
		"tenant_id":  "tenant-a",
		"chat_write": map[string]string{"client_id": "chat-client"},
		"profiles":   map[string]any{"work": map[string]any{"full": map[string]string{"client_id": "work-client"}}},
	})
	if err := os.WriteFile(cfgPath, raw, 0o600); err != nil {
		t.Fatal(err)
	}

	primary := filepath.Join(state, "teams", "profiles", "default", "teams-full-token.json")
	cfg, err := ResolveAuthConfig("")
	if err != nil || cfg.TenantID != "tenant-a" || cfg.ClientID != "chat-client" || cfg.Scopes != DefaultScopes || cfg.CachePath != primary {
		t.Fatalf("no caches: %+v %v", cfg, err)
	}

	// Pre-profile default cache from older builds.
	legacy := filepath.Join(cache, "codex-helper", "teams-full-token.json")
	writeCache(t, legacy, TokenCache{RefreshToken: "r"})
	if cfg, _ := ResolveAuthConfig(""); cfg.CachePath != legacy {
		t.Fatalf("legacy cache not used: %s", cfg.CachePath)
	}

	// Chat-write cache only counts when it already carries the full scopes.
	os.Remove(legacy)
	chatW := filepath.Join(state, "teams", "profiles", "default", "teams-chat-write-token.json")
	writeCache(t, chatW, TokenCache{RefreshToken: "r", Scope: "User.Read Chat.ReadWrite"})
	if cfg, _ := ResolveAuthConfig(""); cfg.CachePath != primary {
		t.Fatalf("reduced-scope chat cache used: %s", cfg.CachePath)
	}
	writeCache(t, chatW, TokenCache{RefreshToken: "r", Scope: "openid User.Read Chat.ReadWrite OnlineMeetings.ReadWrite Files.ReadWrite.All"})
	if cfg, _ := ResolveAuthConfig(""); cfg.CachePath != chatW {
		t.Fatalf("full-scope chat cache not used: %s", cfg.CachePath)
	}
	writeCache(t, primary, TokenCache{RefreshToken: "r"})
	if cfg, _ := ResolveAuthConfig(""); cfg.CachePath != primary {
		t.Fatalf("full cache must win: %s", cfg.CachePath)
	}

	// Profiles and env overrides.
	cfg, _ = ResolveAuthConfig("work")
	if cfg.ClientID != "work-client" || cfg.CachePath != filepath.Join(state, "teams", "profiles", "work", "teams-full-token.json") {
		t.Fatalf("profile: %+v", cfg)
	}
	t.Setenv("CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE", filepath.Join(root, "explicit.json"))
	t.Setenv("CODEX_HELPER_TEAMS_CLIENT_ID", "env-client")
	cfg, _ = ResolveAuthConfig("")
	if cfg.CachePath != filepath.Join(root, "explicit.json") || cfg.ClientID != "env-client" {
		t.Fatalf("env: %+v", cfg)
	}
	if safePathPart("a b/ü") != "a_b__" || safePathPart(" ") != "default" {
		t.Fatalf("safePathPart = %q", safePathPart("a b/ü"))
	}
}
