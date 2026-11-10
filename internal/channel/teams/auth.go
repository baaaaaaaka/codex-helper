package teams

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/gofrs/flock"
)

// Delegated authentication: one full token for writes and, optionally, a
// separate read token. When the read client is absent or matches the full
// client, the full token also handles reads.
//
// Profiles use one native token database; full/read credentials are distinct
// rows. Historical paths are consulted only by the atomic migration module.

const (
	DefaultReadScopes = "openid profile offline_access User.Read Chat.Read Files.Read"
	DefaultScopes     = "openid profile offline_access User.Read Chat.ReadWrite OnlineMeetings.ReadWrite Files.ReadWrite"
	defaultLoginURL   = "https://login.microsoftonline.com"
	accessTokenSkew   = 2 * time.Minute
)

// TokenSource supplies delegated Graph access tokens.
type TokenSource interface {
	// Token returns an access token that is valid for at least a couple of
	// minutes, refreshing it if needed.
	Token(ctx context.Context) (string, error)
	// Refresh forces a new token unless the current one already differs
	// from stale (another request refreshed it meanwhile).
	Refresh(ctx context.Context, stale string) (string, error)
}

// AuthConfig identifies the delegated login.
type AuthConfig struct {
	TenantID string
	ClientID string
	Scopes   string
	Cache    TokenLocation
	LoginURL string // default https://login.microsoftonline.com (tests point it at a fake)
}

// TokenCache holds credential fields. JSON tags belong to the OAuth/import boundary.
type TokenCache struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresAt    int64  `json:"expires_at"`
	ExpiresIn    int64  `json:"expires_in,omitempty"`
	Scope        string `json:"scope,omitempty"`
	TokenType    string `json:"token_type,omitempty"`
}

type authFile struct {
	TenantID  string                 `json:"tenant_id,omitempty"`
	Read      authCred               `json:"read,omitempty"`
	ChatWrite authCred               `json:"chat_write,omitempty"`
	Full      authCred               `json:"full,omitempty"`
	Profiles  map[string]authProfile `json:"profiles,omitempty"`
}

type authProfile struct {
	TenantID  string   `json:"tenant_id,omitempty"`
	Read      authCred `json:"read,omitempty"`
	ChatWrite authCred `json:"chat_write,omitempty"`
	Full      authCred `json:"full,omitempty"`
}

type authCred struct {
	ClientID string `json:"client_id,omitempty"`
	Scopes   string `json:"scopes,omitempty"`
}

// AuthProfile returns the active auth profile name.
func AuthProfile() string {
	for _, k := range []string{"CODEX_HELPER_TEAMS_AUTH_PROFILE", "CODEX_HELPER_TEAMS_PROFILE"} {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return "default"
}

// AuthConfigPath returns the native Teams auth configuration database.
func AuthConfigPath() (string, error) {
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_AUTH_CONFIG")); v != "" {
		return authDatabasePath(expandHome(v)), nil
	}
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "codex-helper", "teams-auth.sqlite"), nil
}

// ResolveAuthConfig builds the delegated auth config for profile ("" = the
// active profile) from teams-auth.json, the environment and the existing
// token caches. It never contacts the network.
func ResolveAuthConfig(profile string) (AuthConfig, error) {
	file, path, err := loadAuthConfigFile(profile)
	if err != nil {
		return AuthConfig{}, err
	}
	return resolveFullAuthConfig(profile, file, path)
}

func resolveFullAuthConfig(profile string, file authFile, path string) (AuthConfig, error) {
	if profile == "" {
		profile = AuthProfile()
	}
	p := file.Profiles[profile]
	cfg := AuthConfig{
		TenantID: first(os.Getenv("CODEX_HELPER_TEAMS_FULL_TENANT_ID"), os.Getenv("CODEX_HELPER_TEAMS_TENANT_ID"), p.TenantID, file.TenantID),
		ClientID: first(os.Getenv("CODEX_HELPER_TEAMS_FULL_CLIENT_ID"), os.Getenv("CODEX_HELPER_TEAMS_CLIENT_ID"),
			p.Full.ClientID, file.Full.ClientID, p.ChatWrite.ClientID, file.ChatWrite.ClientID),
		Scopes: first(os.Getenv("CODEX_HELPER_TEAMS_FULL_SCOPES"), p.Full.Scopes, file.Full.Scopes, DefaultScopes),
	}
	if cfg.TenantID == "" || cfg.ClientID == "" {
		return AuthConfig{}, &channel.AuthRequired{Err: fmt.Errorf("Teams tenant id and client id are not configured (set them in %s)", path)}
	}
	if !UnsafeScopesAllowed() {
		if err := ValidateScopes(cfg.Scopes); err != nil {
			return AuthConfig{}, err
		}
	}
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE")); v != "" {
		var err error
		cfg.Cache, err = resolveTokenOverride(v, "full")
		return cfg, err
	}
	var err error
	cfg.Cache, err = resolveProfileToken(profile, "full")
	if err != nil {
		return cfg, err
	}
	tok, err := ReadTokenCache(cfg.Cache)
	if err == nil && (tok.AccessToken != "" || tok.RefreshToken != "") {
		return cfg, nil
	}
	if err != nil && !os.IsNotExist(err) {
		return cfg, err
	}
	alternate := cfg.Cache
	alternate.Slot = "chat_write"
	tok, err = ReadTokenCache(alternate)
	if err != nil && !os.IsNotExist(err) {
		return cfg, err
	}
	if err == nil && (tok.AccessToken != "" || tok.RefreshToken != "") && coversScopes(tok.Scope, "User.Read Chat.ReadWrite OnlineMeetings.ReadWrite Files.ReadWrite") {
		cfg.Cache = alternate
	}
	return cfg, nil
}

// ResolveReadAuthConfig returns a separate read credential when configured.
// Otherwise, it reuses the full credential and token cache.
func ResolveReadAuthConfig(profile string) (AuthConfig, error) {
	if profile == "" {
		profile = AuthProfile()
	}
	file, path, err := loadAuthConfigFile(profile)
	if err != nil {
		return AuthConfig{}, err
	}
	fullCfg, err := resolveFullAuthConfig(profile, file, path)
	if err != nil {
		return AuthConfig{}, err
	}
	p := file.Profiles[profile]
	clientID := first(os.Getenv("CODEX_HELPER_TEAMS_READ_CLIENT_ID"), p.Read.ClientID, file.Read.ClientID)
	if clientID == "" || strings.TrimSpace(clientID) == strings.TrimSpace(fullCfg.ClientID) {
		return fullCfg, nil
	}
	cfg := AuthConfig{
		TenantID: first(os.Getenv("CODEX_HELPER_TEAMS_READ_TENANT_ID"), os.Getenv("CODEX_HELPER_TEAMS_TENANT_ID"), p.TenantID, file.TenantID, fullCfg.TenantID),
		ClientID: strings.TrimSpace(clientID),
		Scopes:   first(os.Getenv("CODEX_HELPER_TEAMS_READ_SCOPES"), p.Read.Scopes, file.Read.Scopes, DefaultReadScopes),
	}
	if cfg.TenantID == "" {
		return AuthConfig{}, &channel.AuthRequired{Err: fmt.Errorf("Teams read tenant id is not configured (set it in %s)", path)}
	}
	if !UnsafeScopesAllowed() {
		if err := ValidateScopes(cfg.Scopes); err != nil {
			return AuthConfig{}, err
		}
	}
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_READ_TOKEN_CACHE")); v != "" {
		cfg.Cache, err = resolveTokenOverride(v, "read")
	} else {
		cfg.Cache, err = resolveProfileToken(profile, "read")
	}
	if err != nil {
		return cfg, err
	}
	if cfg.Cache == fullCfg.Cache {
		return AuthConfig{}, errors.New("Teams read and full credentials must use different token rows when their client IDs differ")
	}
	return cfg, nil
}

func loadAuthConfigFile(profile string) (authFile, string, error) {
	if profile == "" {
		profile = AuthProfile()
	}
	path, err := AuthConfigPath()
	if err != nil {
		return authFile{}, "", err
	}
	file, err := loadNativeAuthConfig(path, profile)
	return file, path, err
}

// allowedScopes is v0.1.20's set of delegated Graph scopes the helper may
// request. A configured scope outside it fails closed (F-SET-2) unless
// CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES opts in.
var allowedScopes = map[string]bool{
	"openid":                   true,
	"profile":                  true,
	"offline_access":           true,
	"User.Read":                true,
	"Files.Read":               true,
	"Chat.Read":                true,
	"Chat.ReadWrite":           true,
	"Chat.Create":              true,
	"ChatMessage.Send":         true,
	"ChatMember.ReadWrite":     true,
	"Channel.ReadBasic.All":    true,
	"ChannelMessage.Read.All":  true,
	"OnlineMeetings.ReadWrite": true,
	"Files.ReadWrite":          true,
}

// ValidateScopes refuses Graph scopes broader than the helper needs, with
// v0.1.20's message.
func ValidateScopes(scopes string) error {
	var unexpected []string
	seen := map[string]bool{}
	for _, scope := range strings.Fields(scopes) {
		if seen[scope] {
			continue
		}
		seen[scope] = true
		if !allowedScopes[scope] {
			unexpected = append(unexpected, scope)
		}
	}
	if len(unexpected) > 0 {
		return fmt.Errorf("unexpected Teams Graph scope(s): %s; set CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES=1 to override", strings.Join(unexpected, ", "))
	}
	return nil
}

// UnsafeScopesAllowed reports the CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES
// opt-out.
func UnsafeScopesAllowed() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES"))) {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

func coversScopes(have, need string) bool {
	set := map[string]bool{}
	for _, s := range strings.Fields(have) {
		set[strings.ToLower(s)] = true
	}
	for _, s := range strings.Fields(need) {
		s = strings.ToLower(s)
		if !set[s] && !set[s+".all"] && !(s == "files.readwrite" && set["files.readwrite.all"]) {
			return false
		}
	}
	return true
}

// DelegatedAuth is the TokenSource backed by a token cache file and the
// refresh_token grant. It never starts an interactive login.
type DelegatedAuth struct {
	cfg  AuthConfig
	http *http.Client
	now  func() time.Time

	mu    sync.Mutex
	cur   TokenCache
	store *tokenStore
}

// NewDelegatedAuth returns a TokenSource for cfg. client may be nil.
func NewDelegatedAuth(cfg AuthConfig, client *http.Client) *DelegatedAuth {
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	return &DelegatedAuth{cfg: cfg, http: client, now: time.Now}
}

func (a *DelegatedAuth) Token(ctx context.Context) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.fresh(a.cur) {
		return a.cur.AccessToken, nil
	}
	// Re-read the row: the login command or another process may have
	// refreshed it.
	if err := a.load(); err != nil {
		return "", err
	}
	if a.fresh(a.cur) {
		return a.cur.AccessToken, nil
	}
	return a.refreshLocked(ctx, a.cur.AccessToken)
}

func (a *DelegatedAuth) Refresh(ctx context.Context, stale string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.load(); err != nil {
		return "", err
	}
	if a.cur.AccessToken != "" && a.cur.AccessToken != stale && a.fresh(a.cur) {
		return a.cur.AccessToken, nil
	}
	return a.refreshLocked(ctx, stale)
}

func (a *DelegatedAuth) fresh(t TokenCache) bool {
	return t.AccessToken != "" && t.ExpiresAt > a.now().Add(accessTokenSkew).Unix()
}

func (a *DelegatedAuth) load() error {
	if a.store == nil {
		var err error
		a.store, err = openExistingTokens(a.cfg.Cache.Database)
		if err != nil {
			return &channel.AuthRequired{Err: err}
		}
	}
	tok, err := readNativeToken(a.store.db, a.cfg.Cache.Slot)
	if errors.Is(err, os.ErrNotExist) {
		return &channel.AuthRequired{Err: errors.New("Teams token cache is missing; run `cxp teams auth login`")}
	}
	if err != nil {
		return &channel.AuthRequired{Err: err}
	}
	a.cur = tok
	return nil
}

// refreshLocked retains the existing cross-process refresh lock across OAuth
// I/O, then commits the new token with one point write. No SQLite transaction
// spans the network request. Readers reload after acquiring the lock so a
// rotated refresh token is never consumed twice by competing processes.
func (a *DelegatedAuth) refreshLocked(ctx context.Context, stale string) (string, error) {
	unlock, err := lockFile(ctx, a.cfg.Cache.lockPath())
	if err != nil {
		return "", &channel.Rejected{Err: fmt.Errorf("lock Teams token cache: %w", err)}
	}
	defer unlock()
	if err := a.load(); err != nil {
		return "", err
	}
	if a.cur.AccessToken != "" && a.cur.AccessToken != stale && a.fresh(a.cur) {
		return a.cur.AccessToken, nil
	}
	if a.cur.RefreshToken == "" {
		return "", &channel.AuthRequired{Err: errors.New("Teams token cache has no refresh token; run `cxp teams auth login`")}
	}
	tok, err := postForm(ctx, a.http, a.cfg, "token", url.Values{
		"grant_type":    {"refresh_token"},
		"client_id":     {a.cfg.ClientID},
		"refresh_token": {a.cur.RefreshToken},
		"scope":         {a.cfg.Scopes},
	}, a.now)
	if err != nil {
		var oe *oauthError
		if errors.As(err, &oe) && !oe.transient() {
			return "", &channel.AuthRequired{Err: err}
		}
		return "", err
	}
	if tok.RefreshToken == "" {
		tok.RefreshToken = a.cur.RefreshToken
	}
	if _, err := a.store.db.Exec(tokenPut, tokenArgs(a.cfg.Cache.Slot, tok)...); err != nil {
		return "", fmt.Errorf("save Teams token cache: %w", err)
	}
	a.cur = tok
	return tok.AccessToken, nil
}

// lockFile takes an exclusive flock on path, waiting until ctx ends.
func lockFile(ctx context.Context, path string) (func(), error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	l := flock.New(path)
	ok, err := l.TryLockContext(ctx, 25*time.Millisecond)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("lock not acquired")
	}
	_ = os.Chmod(path, 0o600)
	return func() { _ = l.Unlock() }, nil
}

// DeviceLogin runs the device-code flow, prints the instructions to out and
// stores the token in cfg.Cache. It is the only interactive path.
func DeviceLogin(ctx context.Context, cfg AuthConfig, client *http.Client, out io.Writer) error {
	if client == nil {
		client = &http.Client{Timeout: 60 * time.Second}
	}
	var dc struct {
		DeviceCode      string `json:"device_code"`
		UserCode        string `json:"user_code"`
		VerificationURI string `json:"verification_uri"`
		Message         string `json:"message"`
		ExpiresIn       int64  `json:"expires_in"`
		Interval        int64  `json:"interval"`
	}
	if err := postFormRaw(ctx, client, cfg, "devicecode", url.Values{"client_id": {cfg.ClientID}, "scope": {cfg.Scopes}}, &dc); err != nil {
		return err
	}
	msg := dc.Message
	if msg == "" {
		msg = fmt.Sprintf("Open %s and enter code %s", dc.VerificationURI, dc.UserCode)
	}
	fmt.Fprintln(out, msg)
	interval := time.Duration(max(dc.Interval, 5)) * time.Second
	deadline := time.Now().Add(time.Duration(max(dc.ExpiresIn, 60)) * time.Second)
	for time.Now().Before(deadline) {
		if err := sleepCtx(ctx, interval); err != nil {
			return err
		}
		tok, err := postForm(ctx, client, cfg, "token", url.Values{
			"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
			"client_id":   {cfg.ClientID},
			"device_code": {dc.DeviceCode},
		}, time.Now)
		var oe *oauthError
		switch {
		case err == nil:
			unlock, lerr := lockFile(ctx, cfg.Cache.lockPath())
			if lerr != nil {
				return lerr
			}
			defer unlock()
			return WriteTokenCache(cfg.Cache, tok)
		case errors.As(err, &oe) && oe.Code == "authorization_pending":
		case errors.As(err, &oe) && oe.Code == "slow_down":
			interval += 5 * time.Second
		default:
			return err
		}
	}
	return errors.New("device login expired")
}

type oauthError struct {
	Status int
	Code   string
}

func (e *oauthError) Error() string { return fmt.Sprintf("oauth HTTP %d %s", e.Status, e.Code) }

// transient: the server could not answer now; the refresh token may still be good.
func (e *oauthError) transient() bool {
	return e.Status == 429 || e.Status >= 500 || e.Code == "temporarily_unavailable" || e.Code == "slow_down"
}

func postForm(ctx context.Context, client *http.Client, cfg AuthConfig, endpoint string, v url.Values, now func() time.Time) (TokenCache, error) {
	var raw struct {
		AccessToken  string      `json:"access_token"`
		RefreshToken string      `json:"refresh_token"`
		ExpiresIn    json.Number `json:"expires_in"`
		Scope        string      `json:"scope"`
		TokenType    string      `json:"token_type"`
	}
	if err := postFormRaw(ctx, client, cfg, endpoint, v, &raw); err != nil {
		return TokenCache{}, err
	}
	if raw.AccessToken == "" {
		return TokenCache{}, &oauthError{Status: 200, Code: "no_access_token"}
	}
	exp, _ := raw.ExpiresIn.Int64()
	if exp <= 0 {
		exp = 3600
	}
	return TokenCache{
		AccessToken: raw.AccessToken, RefreshToken: raw.RefreshToken,
		ExpiresIn: exp, ExpiresAt: now().Unix() + exp, Scope: raw.Scope, TokenType: raw.TokenType,
	}, nil
}

func postFormRaw(ctx context.Context, client *http.Client, cfg AuthConfig, endpoint string, v url.Values, out any) error {
	base := strings.TrimRight(first(cfg.LoginURL, defaultLoginURL), "/")
	target := fmt.Sprintf("%s/%s/oauth2/v2.0/%s", base, url.PathEscape(cfg.TenantID), endpoint)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, strings.NewReader(v.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		var e struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(data, &e)
		return &oauthError{Status: resp.StatusCode, Code: e.Error}
	}
	return json.Unmarshal(data, out)
}

// Close releases the retained credential connection after all callers drain.
func (a *DelegatedAuth) Close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.store == nil {
		return nil
	}
	err := a.store.db.Close()
	a.store = nil
	return err
}

// safePathPart matches the profile directory naming of earlier releases.
func safePathPart(s string) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(s) {
		if r == '_' || r == '-' || r >= '0' && r <= '9' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "default"
	}
	return b.String()
}

func expandHome(p string) string {
	if p == "~" || strings.HasPrefix(p, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, strings.TrimPrefix(p, "~"))
		}
	}
	return p
}

func first(vals ...string) string {
	for _, v := range vals {
		if v = strings.TrimSpace(v); v != "" {
			return v
		}
	}
	return ""
}
