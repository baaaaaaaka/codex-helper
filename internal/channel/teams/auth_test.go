package teams

import (
	"context"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

func writeCache(t *testing.T, path string, tok TokenCache) {
	t.Helper()
	if err := WriteTokenCache(testTokenLocation(path), tok); err != nil {
		t.Fatal(err)
	}
}

func readCache(t *testing.T, path string) TokenCache {
	t.Helper()
	tok, err := ReadTokenCache(testTokenLocation(path))
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func authAdapter(t *testing.T, fg *fakegraph.Server, cachePath string) (*Adapter, *DelegatedAuth) {
	t.Helper()
	auth := NewDelegatedAuth(AuthConfig{
		TenantID: fakegraph.TenantID, ClientID: "client", Scopes: DefaultScopes,
		Cache: testTokenLocation(cachePath), LoginURL: fg.URL(),
	}, &http.Client{Transport: loginHostTransport{http.DefaultTransport}})
	t.Cleanup(func() { auth.Close() })
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
	st, err := os.Stat(testTokenLocation(path).Database)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && st.Mode().Perm() != 0o600 { // no Unix modes on Windows
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
		auth = NewDelegatedAuth(AuthConfig{Cache: TokenLocation{Database: link, Slot: "full"}}, nil)
		t.Cleanup(func() { auth.Close() })
		if _, err := auth.Token(ctx); !errors.As(err, &ar) {
			t.Fatalf("symlink cache: %v", err)
		}
		if err := WriteTokenCache(TokenLocation{Database: link, Slot: "link"}, TokenCache{}); err == nil {
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
	t.Setenv("HOME", filepath.Join(root, "home"))
	t.Setenv("LocalAppData", cache)
	t.Setenv("CODEX_HELPER_TEAMS_AUTH_CONFIG", cfgPath)
	for _, k := range []string{"CODEX_HELPER_TEAMS_AUTH_PROFILE", "CODEX_HELPER_TEAMS_PROFILE", "CODEX_HELPER_TEAMS_FULL_TENANT_ID", "CODEX_HELPER_TEAMS_TENANT_ID", "CODEX_HELPER_TEAMS_FULL_CLIENT_ID", "CODEX_HELPER_TEAMS_CLIENT_ID", "CODEX_HELPER_TEAMS_FULL_SCOPES", "CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE", "CODEX_HELPER_TEAMS_READ_TOKEN_CACHE"} {
		t.Setenv(k, "")
	}
	raw := `{"tenant_id":"tenant-a","chat_write":{"client_id":"chat-client"},"profiles":{"work":{"full":{"client_id":"work-client"}}}}`
	if err := os.WriteFile(cfgPath, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	legacy, err := appdirs.LegacyCachePath("teams-full-token.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.MkdirAll(filepath.Dir(legacy), 0700); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(legacy, []byte(`{"refresh_token":"old-refresh","expires_at":9007199254740993}`), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := ResolveAuthConfig("")
	primary := TokenLocation{Database: filepath.Join(state, "teams", "profiles", "default", "teams-tokens.sqlite"), Slot: "full"}
	if err != nil || cfg.TenantID != "tenant-a" || cfg.ClientID != "chat-client" || cfg.Scopes != DefaultScopes || cfg.Cache != primary {
		t.Fatalf("config %#v/%v", cfg, err)
	}
	tok, err := ReadTokenCache(cfg.Cache)
	if err != nil || tok.RefreshToken != "old-refresh" || tok.ExpiresAt != 9007199254740993 {
		t.Fatalf("legacy token %#v/%v", tok, err)
	}
	if _, err = os.Stat(legacy + ".migrated"); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(legacy, []byte("poisoned"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = RemoveTokenCache(context.Background(), primary); err != nil {
		t.Fatal(err)
	}
	if cfg, err = ResolveAuthConfig(""); err != nil || cfg.Cache != primary {
		t.Fatalf("native authority %#v/%v", cfg, err)
	}
	chat := primary
	chat.Slot = "chat_write"
	if err = WriteTokenCache(chat, TokenCache{RefreshToken: "r", Scope: "User.Read Chat.ReadWrite"}); err != nil {
		t.Fatal(err)
	}
	if cfg, err = ResolveAuthConfig(""); err != nil || cfg.Cache != primary {
		t.Fatalf("reduced scope %#v/%v", cfg, err)
	}
	if err = WriteTokenCache(chat, TokenCache{RefreshToken: "r", Scope: "openid User.Read Chat.ReadWrite OnlineMeetings.ReadWrite Files.ReadWrite.All"}); err != nil {
		t.Fatal(err)
	}
	if cfg, err = ResolveAuthConfig(""); err != nil || cfg.Cache != chat {
		t.Fatalf("full scope chat %#v/%v", cfg, err)
	}
	if err = WriteTokenCache(primary, TokenCache{RefreshToken: "r"}); err != nil {
		t.Fatal(err)
	}
	if cfg, err = ResolveAuthConfig(""); err != nil || cfg.Cache != primary {
		t.Fatalf("full precedence %#v/%v", cfg, err)
	}
	cfg, err = ResolveAuthConfig("work")
	if err != nil || cfg.ClientID != "work-client" || cfg.Cache.Database != filepath.Join(state, "teams", "profiles", "work", "teams-tokens.sqlite") {
		t.Fatalf("profile %#v/%v", cfg, err)
	}
	t.Setenv("CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE", filepath.Join(root, "explicit.json"))
	t.Setenv("CODEX_HELPER_TEAMS_CLIENT_ID", "env-client")
	cfg, err = ResolveAuthConfig("")
	if err != nil || cfg.Cache.Database != filepath.Join(root, "teams-tokens.sqlite") || cfg.Cache.Slot != "explicit.json" || cfg.ClientID != "env-client" {
		t.Fatalf("override %#v/%v", cfg, err)
	}
	if safePathPart("a b/ü") != "a_b__" || safePathPart(" ") != "default" {
		t.Fatal("safePathPart")
	}
}

func TestResolveReadAuthConfigFallsBackOrUsesSeparateCache(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	configPath := filepath.Join(root, "teams-auth.json")
	t.Setenv("CODEX_HELPER_STATE_DIR", state)
	t.Setenv("XDG_CACHE_HOME", filepath.Join(root, "cache"))
	t.Setenv("HOME", filepath.Join(root, "home"))
	t.Setenv("LocalAppData", filepath.Join(root, "cache"))
	t.Setenv("CODEX_HELPER_TEAMS_AUTH_CONFIG", configPath)
	for _, key := range []string{
		"CODEX_HELPER_TEAMS_AUTH_PROFILE", "CODEX_HELPER_TEAMS_PROFILE",
		"CODEX_HELPER_TEAMS_TENANT_ID", "CODEX_HELPER_TEAMS_CLIENT_ID",
		"CODEX_HELPER_TEAMS_FULL_TENANT_ID", "CODEX_HELPER_TEAMS_FULL_CLIENT_ID",
		"CODEX_HELPER_TEAMS_FULL_SCOPES", "CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE",
		"CODEX_HELPER_TEAMS_READ_TENANT_ID", "CODEX_HELPER_TEAMS_READ_CLIENT_ID",
		"CODEX_HELPER_TEAMS_READ_SCOPES", "CODEX_HELPER_TEAMS_READ_TOKEN_CACHE",
	} {
		t.Setenv(key, "")
	}
	writeConfig := func(readID string) {
		t.Helper()
		if err := UpdateAuthConfig(configPath, map[string]string{"tenant_id": "tenant-a", "client_id": "write-client", "read_client_id": readID}); err != nil {
			t.Fatal(err)
		}
	}

	writeConfig("")
	fullCfg, err := ResolveAuthConfig("")
	if err != nil {
		t.Fatal(err)
	}
	readCfg, err := ResolveReadAuthConfig("")
	if err != nil || readCfg.ClientID != fullCfg.ClientID || readCfg.Cache != fullCfg.Cache {
		t.Fatalf("read config should reuse full auth: read=%+v full=%+v err=%v", readCfg, fullCfg, err)
	}

	writeConfig("read-client")
	readCfg, err = ResolveReadAuthConfig("")
	if err != nil {
		t.Fatal(err)
	}
	wantReadCache := TokenLocation{Database: filepath.Join(state, "teams", "profiles", "default", "teams-tokens.sqlite"), Slot: "read"}
	if readCfg.ClientID != "read-client" || readCfg.Scopes != DefaultReadScopes || readCfg.Cache != wantReadCache || readCfg.Cache == fullCfg.Cache {
		t.Fatalf("separate read config = %+v, want cache %s", readCfg, wantReadCache)
	}

	if err := WriteTokenCache(wantReadCache, TokenCache{RefreshToken: "read-refresh"}); err != nil {
		t.Fatal(err)
	}
	readCfg, err = ResolveReadAuthConfig("")
	if err != nil || readCfg.Cache != wantReadCache {
		t.Fatalf("existing read cache not reused: %+v %v", readCfg, err)
	}
}

// Two token sources on one cache (service + CLI, or two machines on a shared
// home) must not both spend the refresh token: the second one waits for the
// cache lock, re-reads the cache and uses the token the first one stored.
func TestDelegatedAuthRefreshIsSerialisedAcrossInstances(t *testing.T) {
	fg := fakegraph.New(fakegraph.Options{})
	t.Cleanup(fg.Close)
	_, refresh := fg.IssueTokens("cached")
	path := filepath.Join(t.TempDir(), "teams-full-token.json")
	writeCache(t, path, TokenCache{AccessToken: "expired", RefreshToken: refresh, ExpiresAt: time.Now().Add(-time.Hour).Unix()})
	fg.AddFault(fakegraph.Fault{Name: "slow-login", Kind: fakegraph.FaultLatency, Host: "login.microsoftonline.com", Latency: 300 * time.Millisecond})
	const n = 4
	toks := make([]string, n)
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		_, auth := authAdapter(t, fg, path) // separate instances: only the file lock is shared
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			toks[i], errs[i] = auth.Token(context.Background())
		}(i)
	}
	wg.Wait()
	for i := range toks {
		if errs[i] != nil || toks[i] != toks[0] || toks[i] == "expired" {
			t.Fatalf("instance %d: %q %v (first %q)", i, toks[i], errs[i], toks[0])
		}
	}
	if got := countRequests(fg, "POST /{tenant}/oauth2/v2.0/token"); got != 1 {
		t.Fatalf("%d refreshes, want 1", got)
	}
	if _, err := os.Stat(testTokenLocation(path).lockPath()); err != nil {
		t.Fatalf("lock file: %v", err)
	}

	// Refresh(stale) after a 401: an instance holding the token another
	// instance already replaced picks up the new one without refreshing.
	_, a1 := authAdapter(t, fg, path)
	_, a2 := authAdapter(t, fg, path)
	t1, _ := a1.Token(context.Background())
	t2, err := a2.Refresh(context.Background(), t1)
	if err != nil || t2 == t1 {
		t.Fatalf("refresh: %q %v", t2, err)
	}
	t3, err := a1.Refresh(context.Background(), t1)
	if err != nil || t3 != t2 || countRequests(fg, "POST /{tenant}/oauth2/v2.0/token") != 2 {
		t.Fatalf("second instance refreshed again: %q vs %q, %v", t3, t2, err)
	}
}

// F-SET-2: scopes broader than v0.1.20's allowlist fail closed unless the
// operator opts in.
func TestResolveAuthConfigRefusesBroaderScopes(t *testing.T) {
	root := t.TempDir()
	t.Setenv("CODEX_HELPER_STATE_DIR", filepath.Join(root, "state"))
	t.Setenv("XDG_CACHE_HOME", filepath.Join(root, "cache"))
	t.Setenv("CODEX_HELPER_TEAMS_AUTH_CONFIG", filepath.Join(root, "teams-auth.json"))
	t.Setenv("CODEX_HELPER_TEAMS_TENANT_ID", "tenant")
	t.Setenv("CODEX_HELPER_TEAMS_CLIENT_ID", "client")
	t.Setenv("CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE", "")
	t.Setenv("CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES", "")
	t.Setenv("CODEX_HELPER_TEAMS_FULL_SCOPES", "User.Read Chat.ReadWrite Mail.Read Mail.Read Sites.FullControl.All")
	_, err := ResolveAuthConfig("")
	want := "unexpected Teams Graph scope(s): Mail.Read, Sites.FullControl.All; set CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES=1 to override"
	if err == nil || err.Error() != want {
		t.Fatalf("err = %v, want %q", err, want)
	}
	t.Setenv("CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES", "yes")
	if _, err := ResolveAuthConfig(""); err != nil {
		t.Fatalf("opt-in: %v", err)
	}
	t.Setenv("CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES", "")
	t.Setenv("CODEX_HELPER_TEAMS_FULL_SCOPES", "openid profile offline_access User.Read Chat.Read Chat.Create ChatMessage.Send Files.Read")
	if _, err := ResolveAuthConfig(""); err != nil {
		t.Fatalf("allowed scopes: %v", err)
	}
	if err := ValidateScopes(DefaultScopes); err != nil {
		t.Fatalf("defaults: %v", err)
	}
}

func testTokenLocation(path string) TokenLocation {
	slot := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	switch slot {
	case "teams-full-token":
		slot = "full"
	case "teams-read-token":
		slot = "read"
	case "teams-chat-write-token":
		slot = "chat_write"
	}
	return TokenLocation{Database: filepath.Join(filepath.Dir(path), "teams-tokens.sqlite"), Slot: slot}
}
