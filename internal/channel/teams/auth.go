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

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/gofrs/flock"
)

// Delegated authentication: one Microsoft Entra delegated token for
// everything the adapter does (read, write, files, meetings).
//
// Files read, in order (first usable wins):
//
//  1. $CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE, if set (and nothing else).
//  2. <state>/teams/profiles/<profile>/teams-full-token.json
//  3. <user cache>/codex-helper/teams/profiles/<profile>/teams-full-token.json (pre-state-dir builds)
//  4. default profile only: <user cache>/codex-helper/teams-full-token.json (pre-profile builds)
//  5. the same three locations for teams-chat-write-token.json, but only if
//     the cached token's scope already covers the full feature set (older
//     builds logged in once with full scopes under the chat-write name).
//
// Refreshed tokens are written back to the file they were read from
// (0600, atomic rename, symlinks refused). Configuration comes from
// $CODEX_HELPER_TEAMS_AUTH_CONFIG or <user config>/codex-helper/teams-auth.json
// (tenant_id, full.client_id/scopes, falling back to chat_write.client_id),
// with CODEX_HELPER_TEAMS_{FULL_,}{TENANT_ID,CLIENT_ID} and
// CODEX_HELPER_TEAMS_FULL_SCOPES env overrides.

const (
	DefaultScopes      = "openid profile offline_access User.Read Chat.ReadWrite OnlineMeetings.ReadWrite Files.ReadWrite"
	defaultLoginURL    = "https://login.microsoftonline.com"
	fullCacheName      = "teams-full-token.json"
	chatWriteCacheName = "teams-chat-write-token.json"
	accessTokenSkew    = 2 * time.Minute
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
	TenantID  string
	ClientID  string
	Scopes    string
	CachePath string
	LoginURL  string // default https://login.microsoftonline.com (tests point it at a fake)
}

// TokenCache is the on-disk format shared with earlier releases.
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
	ChatWrite authCred               `json:"chat_write,omitempty"`
	Full      authCred               `json:"full,omitempty"`
	Profiles  map[string]authProfile `json:"profiles,omitempty"`
}

type authProfile struct {
	TenantID  string   `json:"tenant_id,omitempty"`
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

// AuthConfigPath returns the teams-auth.json location.
func AuthConfigPath() (string, error) {
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_AUTH_CONFIG")); v != "" {
		return expandHome(v), nil
	}
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "codex-helper", "teams-auth.json"), nil
}

// ResolveAuthConfig builds the delegated auth config for profile ("" = the
// active profile) from teams-auth.json, the environment and the existing
// token caches. It never contacts the network.
func ResolveAuthConfig(profile string) (AuthConfig, error) {
	if profile == "" {
		profile = AuthProfile()
	}
	var file authFile
	path, err := AuthConfigPath()
	if err != nil {
		return AuthConfig{}, err
	}
	if raw, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(raw, &file); err != nil {
			return AuthConfig{}, fmt.Errorf("read Teams auth config %s: %w", path, err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return AuthConfig{}, err
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
		cfg.CachePath = expandHome(v)
		return cfg, nil
	}
	cands, err := cacheCandidates(profile)
	if err != nil {
		return AuthConfig{}, err
	}
	cfg.CachePath = cands[0].path
	for _, c := range cands {
		tok, err := readTokenCache(c.path)
		if err != nil || (tok.AccessToken == "" && tok.RefreshToken == "") {
			continue
		}
		if c.reduced && !coversScopes(tok.Scope, "User.Read Chat.ReadWrite OnlineMeetings.ReadWrite Files.ReadWrite") {
			continue
		}
		cfg.CachePath = c.path
		break
	}
	return cfg, nil
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

type cacheCandidate struct {
	path    string
	reduced bool // chat-write cache: only usable if its scopes cover the full set
}

func cacheCandidates(profile string) ([]cacheCandidate, error) {
	safe := safePathPart(profile)
	var out []cacheCandidate
	for _, name := range []string{fullCacheName, chatWriteCacheName} {
		reduced := name == chatWriteCacheName
		p, err := appdirs.StatePath("teams", "profiles", safe, name)
		if err != nil {
			return nil, err
		}
		out = append(out, cacheCandidate{p, reduced})
		if p, err := appdirs.LegacyCachePath("teams", "profiles", safe, name); err == nil {
			out = append(out, cacheCandidate{p, reduced})
		}
		if profile == "default" {
			if p, err := appdirs.LegacyCachePath(name); err == nil {
				out = append(out, cacheCandidate{p, reduced})
			}
		}
	}
	return out, nil
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

	mu  sync.Mutex
	cur TokenCache
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
	// Re-read the file: the login command or another process may have
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
	tok, err := readTokenCache(a.cfg.CachePath)
	if errors.Is(err, os.ErrNotExist) {
		return &channel.AuthRequired{Err: errors.New("Teams token cache is missing; run `cxp teams auth login`")}
	}
	if err != nil {
		return &channel.AuthRequired{Err: err}
	}
	a.cur = tok
	return nil
}

// refreshLocked refreshes under the cross-process cache lock
// (<cache>.lock, flock). Holding it, the cache is read again: another
// process (the service, the CLI, a second machine on a shared home) may
// have refreshed while we waited, and refreshing again with the rotated
// refresh token would waste it. Binaries older than this lock never take
// it; they still replace the cache atomically, so the worst case with them
// is one extra refresh, as before.
func (a *DelegatedAuth) refreshLocked(ctx context.Context, stale string) (string, error) {
	unlock, err := lockFile(ctx, a.cfg.CachePath+".lock")
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
	if err := WriteTokenCache(a.cfg.CachePath, tok); err != nil {
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
// stores the token in cfg.CachePath. It is the only interactive path.
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
			unlock, lerr := lockFile(ctx, cfg.CachePath+".lock")
			if lerr != nil {
				return lerr
			}
			defer unlock()
			return WriteTokenCache(cfg.CachePath, tok)
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

// ReadTokenCache reads a token cache (for status output; callers never
// print the tokens).
func ReadTokenCache(path string) (TokenCache, error) { return readTokenCache(path) }

func readTokenCache(path string) (TokenCache, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return TokenCache{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return TokenCache{}, fmt.Errorf("Teams token cache %s is not a regular file", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return TokenCache{}, err
	}
	var tok TokenCache
	if err := json.Unmarshal(data, &tok); err != nil {
		return TokenCache{}, fmt.Errorf("Teams token cache %s: %w", path, err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		_ = os.Chmod(path, 0o600)
	}
	return tok, nil
}

// WriteTokenCache stores tok at path atomically with mode 0600.
func WriteTokenCache(path string, tok TokenCache) error {
	if info, err := os.Lstat(path); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("Teams token cache %s must not be a symlink", path)
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(tok, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".teams-token-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
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
