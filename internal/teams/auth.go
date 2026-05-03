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
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

const (
	defaultReadScopes       = "openid profile offline_access User.Read Chat.Read Files.Read"
	defaultScopes           = "openid profile offline_access User.Read Chat.ReadWrite OnlineMeetings.ReadWrite"
	defaultFileWriteScopes  = "openid profile offline_access User.Read Chat.ReadWrite Files.ReadWrite"
	defaultFullScopes       = "openid profile offline_access User.Read Chat.ReadWrite OnlineMeetings.ReadWrite Files.ReadWrite"
	readTokenCacheName      = "teams-read-token.json"
	chatWriteTokenCacheName = "teams-chat-write-token.json"
	fileWriteTokenCacheName = "teams-file-write-token.json"
	fullTokenCacheName      = "teams-full-token.json"
)

type AuthConfig struct {
	TenantID  string
	ClientID  string
	Scopes    string
	CachePath string
}

type TeamsAuthConfigFile struct {
	TenantID  string                          `json:"tenant_id,omitempty"`
	Read      TeamsAuthCredentialConfig       `json:"read,omitempty"`
	ChatWrite TeamsAuthCredentialConfig       `json:"chat_write,omitempty"`
	FileWrite TeamsAuthCredentialConfig       `json:"file_write,omitempty"`
	Full      TeamsAuthCredentialConfig       `json:"full,omitempty"`
	Profiles  map[string]TeamsAuthProfileFile `json:"profiles,omitempty"`
}

type TeamsAuthProfileFile struct {
	TenantID  string                    `json:"tenant_id,omitempty"`
	Read      TeamsAuthCredentialConfig `json:"read,omitempty"`
	ChatWrite TeamsAuthCredentialConfig `json:"chat_write,omitempty"`
	FileWrite TeamsAuthCredentialConfig `json:"file_write,omitempty"`
	Full      TeamsAuthCredentialConfig `json:"full,omitempty"`
}

type TeamsAuthCredentialConfig struct {
	ClientID string `json:"client_id,omitempty"`
	Scopes   string `json:"scopes,omitempty"`
}

func (c TeamsAuthConfigFile) profile(name string) TeamsAuthProfileFile {
	name = strings.TrimSpace(name)
	if name == "" || c.Profiles == nil {
		return TeamsAuthProfileFile{}
	}
	return c.Profiles[name]
}

func DefaultTeamsAuthConfigPath() (string, error) {
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_AUTH_CONFIG")); v != "" {
		return expandHome(v), nil
	}
	base, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "codex-helper", "teams-auth.json"), nil
}

func LoadTeamsAuthConfigFile(path string) (TeamsAuthConfigFile, error) {
	if strings.TrimSpace(path) == "" {
		var err error
		path, err = DefaultTeamsAuthConfigPath()
		if err != nil {
			return TeamsAuthConfigFile{}, err
		}
	}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return TeamsAuthConfigFile{}, nil
	}
	if err != nil {
		return TeamsAuthConfigFile{}, err
	}
	var cfg TeamsAuthConfigFile
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return TeamsAuthConfigFile{}, fmt.Errorf("read Teams auth config %s: %w", path, err)
	}
	return cfg, nil
}

func SaveTeamsAuthConfigFile(path string, cfg TeamsAuthConfigFile) error {
	if strings.TrimSpace(path) == "" {
		var err error
		path, err = DefaultTeamsAuthConfigPath()
		if err != nil {
			return err
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

type TokenCache struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresAt    int64  `json:"expires_at"`
	ExpiresIn    int64  `json:"expires_in,omitempty"`
	Scope        string `json:"scope,omitempty"`
	TokenType    string `json:"token_type,omitempty"`
}

type AuthManager struct {
	cfg    AuthConfig
	client *http.Client
}

type nonInteractiveAuth struct {
	*AuthManager
	action       string
	loginCommand string
}

func DefaultReadAuthConfig() (AuthConfig, error) {
	cachePath, err := defaultReadTokenCachePath()
	if err != nil {
		return AuthConfig{}, err
	}
	fileCfg, err := loadDefaultTeamsAuthConfigFile()
	if err != nil {
		return AuthConfig{}, err
	}
	profileCfg := fileCfg.profile(defaultTeamsAuthProfile())
	cfg := AuthConfig{
		TenantID:  firstNonEmptyString(profileCfg.TenantID, fileCfg.TenantID),
		ClientID:  firstNonEmptyString(profileCfg.Read.ClientID, fileCfg.Read.ClientID),
		Scopes:    firstNonEmptyString(profileCfg.Read.Scopes, fileCfg.Read.Scopes, defaultReadScopes),
		CachePath: cachePath,
	}
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_READ_TENANT_ID")); v != "" {
		cfg.TenantID = v
	} else if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_TENANT_ID")); v != "" {
		cfg.TenantID = v
	}
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_READ_CLIENT_ID")); v != "" {
		cfg.ClientID = v
	}
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_READ_SCOPES")); v != "" {
		cfg.Scopes = v
	}
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_READ_TOKEN_CACHE")); v != "" {
		cfg.CachePath = expandHome(v)
	}
	if err := requireTeamsAuthIDs(cfg, "read", "CODEX_HELPER_TEAMS_READ_CLIENT_ID"); err != nil {
		return AuthConfig{}, err
	}
	if !unsafeTeamsScopesAllowed() {
		if err := validateTeamsScopes(cfg.Scopes); err != nil {
			return AuthConfig{}, err
		}
	}
	return cfg, nil
}

func DefaultEffectiveReadAuthConfig() (AuthConfig, error) {
	cfg, err := DefaultReadAuthConfig()
	if err != nil {
		return AuthConfig{}, err
	}
	return withFullAuthFallback(cfg, "CODEX_HELPER_TEAMS_READ_TOKEN_CACHE")
}

func loadDefaultTeamsAuthConfigFile() (TeamsAuthConfigFile, error) {
	path, err := DefaultTeamsAuthConfigPath()
	if err != nil {
		return TeamsAuthConfigFile{}, err
	}
	return LoadTeamsAuthConfigFile(path)
}

func requireTeamsAuthIDs(cfg AuthConfig, label string, clientEnv string) error {
	configPath, pathErr := DefaultTeamsAuthConfigPath()
	if pathErr != nil {
		configPath = "$XDG_CONFIG_HOME/codex-helper/teams-auth.json"
	}
	if strings.TrimSpace(cfg.TenantID) == "" {
		return fmt.Errorf("Teams %s tenant id is not configured; set CODEX_HELPER_TEAMS_TENANT_ID or write %s with `codex-proxy teams auth config --tenant-id <tenant-id> ...`", label, configPath)
	}
	if strings.TrimSpace(cfg.ClientID) == "" {
		if strings.TrimSpace(clientEnv) == "CODEX_HELPER_TEAMS_FILE_WRITE_CLIENT_ID" {
			return fmt.Errorf("Teams %s client id is not configured; set %s or CODEX_HELPER_TEAMS_CLIENT_ID, or write %s with `codex-proxy teams auth config --file-write-client-id <client-id> ...`", label, clientEnv, configPath)
		}
		return fmt.Errorf("Teams %s client id is not configured; set %s or write %s with `codex-proxy teams auth config --%s <client-id> ...`", label, clientEnv, configPath, authConfigClientFlag(label))
	}
	return nil
}

func authConfigClientFlag(label string) string {
	switch strings.TrimSpace(label) {
	case "read":
		return "read-client-id"
	case "chat write":
		return "chat-client-id"
	case "full":
		return "full-client-id"
	default:
		return "client-id"
	}
}

func DefaultAuthConfig() (AuthConfig, error) {
	cachePath, err := defaultTokenCachePath()
	if err != nil {
		return AuthConfig{}, err
	}
	fileCfg, err := loadDefaultTeamsAuthConfigFile()
	if err != nil {
		return AuthConfig{}, err
	}
	profileCfg := fileCfg.profile(defaultTeamsAuthProfile())
	cfg := AuthConfig{
		TenantID:  firstNonEmptyString(profileCfg.TenantID, fileCfg.TenantID),
		ClientID:  firstNonEmptyString(profileCfg.ChatWrite.ClientID, fileCfg.ChatWrite.ClientID),
		Scopes:    firstNonEmptyString(profileCfg.ChatWrite.Scopes, fileCfg.ChatWrite.Scopes, defaultScopes),
		CachePath: cachePath,
	}
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_TENANT_ID")); v != "" {
		cfg.TenantID = v
	}
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_CLIENT_ID")); v != "" {
		cfg.ClientID = v
	}
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_SCOPES")); v != "" {
		cfg.Scopes = v
	}
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_TOKEN_CACHE")); v != "" {
		cfg.CachePath = expandHome(v)
	}
	if err := requireTeamsAuthIDs(cfg, "chat write", "CODEX_HELPER_TEAMS_CLIENT_ID"); err != nil {
		return AuthConfig{}, err
	}
	if !unsafeTeamsScopesAllowed() {
		if err := validateTeamsScopes(cfg.Scopes); err != nil {
			return AuthConfig{}, err
		}
	}
	return cfg, nil
}

func DefaultEffectiveAuthConfig() (AuthConfig, error) {
	cfg, err := DefaultAuthConfig()
	if err != nil {
		return AuthConfig{}, err
	}
	return withFullAuthFallback(cfg, "CODEX_HELPER_TEAMS_TOKEN_CACHE")
}

func DefaultFileWriteAuthConfig() (AuthConfig, error) {
	cachePath, err := defaultFileWriteTokenCachePath()
	if err != nil {
		return AuthConfig{}, err
	}
	fileCfg, err := loadDefaultTeamsAuthConfigFile()
	if err != nil {
		return AuthConfig{}, err
	}
	profileCfg := fileCfg.profile(defaultTeamsAuthProfile())
	cfg := AuthConfig{
		TenantID: firstNonEmptyString(profileCfg.TenantID, fileCfg.TenantID),
		ClientID: firstNonEmptyString(
			profileCfg.FileWrite.ClientID,
			fileCfg.FileWrite.ClientID,
			profileCfg.ChatWrite.ClientID,
			fileCfg.ChatWrite.ClientID,
		),
		Scopes:    firstNonEmptyString(profileCfg.FileWrite.Scopes, fileCfg.FileWrite.Scopes, defaultFileWriteScopes),
		CachePath: cachePath,
	}
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_FILE_WRITE_TENANT_ID")); v != "" {
		cfg.TenantID = v
	} else if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_TENANT_ID")); v != "" {
		cfg.TenantID = v
	}
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_FILE_WRITE_CLIENT_ID")); v != "" {
		cfg.ClientID = v
	} else if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_CLIENT_ID")); v != "" {
		cfg.ClientID = v
	}
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_FILE_WRITE_SCOPES")); v != "" {
		cfg.Scopes = v
	}
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_FILE_WRITE_TOKEN_CACHE")); v != "" {
		cfg.CachePath = expandHome(v)
	}
	if err := requireTeamsAuthIDs(cfg, "file write", "CODEX_HELPER_TEAMS_FILE_WRITE_CLIENT_ID"); err != nil {
		return AuthConfig{}, err
	}
	if !unsafeTeamsScopesAllowed() {
		if err := validateTeamsScopes(cfg.Scopes); err != nil {
			return AuthConfig{}, err
		}
	}
	return cfg, nil
}

func DefaultEffectiveFileWriteAuthConfig() (AuthConfig, error) {
	cfg, err := DefaultFileWriteAuthConfig()
	if err != nil {
		return AuthConfig{}, err
	}
	return withFullAuthFallback(cfg, "CODEX_HELPER_TEAMS_FILE_WRITE_TOKEN_CACHE")
}

func DefaultFullAuthConfig() (AuthConfig, error) {
	cachePath, err := defaultFullTokenCachePath()
	if err != nil {
		return AuthConfig{}, err
	}
	fileCfg, err := loadDefaultTeamsAuthConfigFile()
	if err != nil {
		return AuthConfig{}, err
	}
	profileCfg := fileCfg.profile(defaultTeamsAuthProfile())
	cfg := AuthConfig{
		TenantID: firstNonEmptyString(profileCfg.TenantID, fileCfg.TenantID),
		ClientID: firstNonEmptyString(
			profileCfg.Full.ClientID,
			fileCfg.Full.ClientID,
			profileCfg.ChatWrite.ClientID,
			fileCfg.ChatWrite.ClientID,
		),
		Scopes:    firstNonEmptyString(profileCfg.Full.Scopes, fileCfg.Full.Scopes, defaultFullScopes),
		CachePath: cachePath,
	}
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_FULL_TENANT_ID")); v != "" {
		cfg.TenantID = v
	} else if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_TENANT_ID")); v != "" {
		cfg.TenantID = v
	}
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_FULL_CLIENT_ID")); v != "" {
		cfg.ClientID = v
	} else if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_CLIENT_ID")); v != "" {
		cfg.ClientID = v
	}
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_FULL_SCOPES")); v != "" {
		cfg.Scopes = v
	}
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE")); v != "" {
		cfg.CachePath = expandHome(v)
	}
	if err := requireTeamsAuthIDs(cfg, "full", "CODEX_HELPER_TEAMS_FULL_CLIENT_ID"); err != nil {
		return AuthConfig{}, err
	}
	if !unsafeTeamsScopesAllowed() {
		if err := validateTeamsScopes(cfg.Scopes); err != nil {
			return AuthConfig{}, err
		}
	}
	return cfg, nil
}

type authCacheState int

const (
	authCacheMissing authCacheState = iota
	authCacheEmpty
	authCacheUsable
)

func withFullAuthFallback(cfg AuthConfig, explicitTokenCacheEnv string) (AuthConfig, error) {
	if strings.TrimSpace(os.Getenv(explicitTokenCacheEnv)) != "" {
		return cfg, nil
	}
	state, err := classifyAuthCache(cfg.CachePath)
	if err != nil || state == authCacheUsable {
		return cfg, nil
	}
	fullCfg, fullErr := DefaultFullAuthConfig()
	if fullErr != nil {
		return cfg, nil
	}
	fullState, fullReadErr := classifyAuthCache(fullCfg.CachePath)
	if fullReadErr != nil || fullState == authCacheUsable {
		return fullCfg, nil
	}
	if promotedCfg, ok := cachedFullAuthAliasFromChatToken(fullCfg); ok {
		return promotedCfg, nil
	}
	if state == authCacheMissing {
		return fullCfg, nil
	}
	return cfg, nil
}

func cachedFullAuthAliasFromChatToken(fullCfg AuthConfig) (AuthConfig, bool) {
	chatCfg, err := DefaultAuthConfig()
	if err != nil {
		return AuthConfig{}, false
	}
	if filepath.Clean(chatCfg.CachePath) == filepath.Clean(fullCfg.CachePath) {
		return AuthConfig{}, false
	}
	if !tokenCacheCoversFeatureScopes(chatCfg.CachePath, "User.Read Chat.ReadWrite OnlineMeetings.ReadWrite Files.ReadWrite") {
		return AuthConfig{}, false
	}
	fullCfg.CachePath = chatCfg.CachePath
	return fullCfg, true
}

func tokenCacheCoversFeatureScopes(path string, required string) bool {
	tok, err := readTokenCache(path)
	if err != nil {
		return false
	}
	have := make(map[string]bool)
	for _, scope := range strings.Fields(tok.Scope) {
		have[scope] = true
	}
	for _, scope := range strings.Fields(required) {
		if !scopeCoveredByToken(scope, have) {
			return false
		}
	}
	return true
}

func scopeCoveredByToken(scope string, have map[string]bool) bool {
	if have[scope] {
		return true
	}
	switch scope {
	case "Chat.Read":
		return have["Chat.ReadWrite"]
	case "Files.Read":
		return have["Files.ReadWrite"] || have["Files.Read.All"] || have["Files.ReadWrite.All"]
	case "Files.ReadWrite":
		return have["Files.ReadWrite.All"]
	case "User.Read":
		return have["User.Read.All"]
	default:
		return false
	}
}

func classifyAuthCache(path string) (authCacheState, error) {
	tok, err := readTokenCache(path)
	if errors.Is(err, os.ErrNotExist) {
		return authCacheMissing, nil
	}
	if err != nil {
		return authCacheEmpty, err
	}
	if tok.AccessToken != "" || tok.RefreshToken != "" {
		return authCacheUsable, nil
	}
	return authCacheEmpty, nil
}

func NewAuthManager(cfg AuthConfig) *AuthManager {
	return NewAuthManagerWithHTTPClient(cfg, nil)
}

func NewAuthManagerWithHTTPClient(cfg AuthConfig, client *http.Client) *AuthManager {
	if client == nil {
		client = &http.Client{
			Timeout: 30 * time.Second,
		}
	}
	return &AuthManager{
		cfg:    cfg,
		client: client,
	}
}

func (a *AuthManager) TenantID() string {
	if a == nil {
		return ""
	}
	return strings.TrimSpace(a.cfg.TenantID)
}

func newNonInteractiveAuthManager(cfg AuthConfig, action string, loginCommand ...string) graphAuth {
	return newNonInteractiveAuthManagerWithHTTPClient(cfg, nil, action, loginCommand...)
}

func newNonInteractiveAuthManagerWithHTTPClient(cfg AuthConfig, client *http.Client, action string, loginCommand ...string) graphAuth {
	command := "codex-proxy teams auth"
	if len(loginCommand) > 0 && strings.TrimSpace(loginCommand[0]) != "" {
		command = strings.TrimSpace(loginCommand[0])
	}
	return nonInteractiveAuth{AuthManager: NewAuthManagerWithHTTPClient(cfg, client), action: action, loginCommand: command}
}

func (a nonInteractiveAuth) AccessToken(ctx context.Context, out io.Writer, forceLogin bool) (string, error) {
	tok, err := readTokenCache(a.cfg.CachePath)
	if errors.Is(err, os.ErrNotExist) {
		return "", a.reauthRequiredError("auth cache is missing")
	}
	if err != nil {
		return "", err
	}
	if !forceLogin && tok.AccessToken != "" && tok.ExpiresAt > time.Now().Add(2*time.Minute).Unix() {
		return tok.AccessToken, nil
	}
	if tok.RefreshToken == "" {
		return "", a.reauthRequiredError("auth cache is expired and has no refresh token")
	}
	refreshed, err := a.refreshCachedToken(ctx, tok)
	if err != nil {
		return "", fmt.Errorf("%s token refresh failed: %w; run `%s` locally", a.displayAction(), err, a.loginInstruction())
	}
	return refreshed.AccessToken, nil
}

func (a nonInteractiveAuth) RefreshAccessToken(ctx context.Context) (string, error) {
	tok, err := readTokenCache(a.cfg.CachePath)
	if errors.Is(err, os.ErrNotExist) {
		return "", a.reauthRequiredError("auth cache is missing")
	}
	if err != nil {
		return "", err
	}
	if tok.RefreshToken == "" {
		return "", a.reauthRequiredError("auth cache has no refresh token")
	}
	refreshed, err := a.refreshCachedToken(ctx, tok)
	if err != nil {
		return "", fmt.Errorf("%s token refresh failed: %w; run `%s` locally", a.displayAction(), err, a.loginInstruction())
	}
	return refreshed.AccessToken, nil
}

func (a nonInteractiveAuth) reauthRequiredError(reason string) error {
	return fmt.Errorf("%s %s; run `%s` locally", a.displayAction(), reason, a.loginInstruction())
}

func (a nonInteractiveAuth) displayAction() string {
	if strings.TrimSpace(a.action) == "" {
		return "Teams auth"
	}
	return strings.TrimSpace(a.action)
}

func (a nonInteractiveAuth) loginInstruction() string {
	if strings.TrimSpace(a.loginCommand) != "" {
		return strings.TrimSpace(a.loginCommand)
	}
	if strings.Contains(strings.ToLower(a.action), "file") {
		return "codex-proxy teams auth file-write"
	}
	if strings.Contains(strings.ToLower(a.action), "read") {
		return "codex-proxy teams auth read"
	}
	return "codex-proxy teams auth"
}

func loginCommandForAuthCache(cachePath string, fallback string) string {
	if filepath.Base(strings.TrimSpace(cachePath)) == fullTokenCacheName {
		return "codex-proxy teams auth full"
	}
	return strings.TrimSpace(fallback)
}

func (a *AuthManager) AccessToken(ctx context.Context, out io.Writer, forceLogin bool) (string, error) {
	serviceMode := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_SERVICE")) != ""
	refreshFailed := error(nil)
	if !forceLogin {
		tok, err := readTokenCache(a.cfg.CachePath)
		if err == nil && tok.AccessToken != "" && tok.ExpiresAt > time.Now().Add(2*time.Minute).Unix() {
			return tok.AccessToken, nil
		}
		if err == nil && tok.RefreshToken != "" {
			refreshed, refreshErr := a.refreshCachedToken(ctx, tok)
			if refreshErr == nil {
				return refreshed.AccessToken, nil
			}
			refreshFailed = refreshErr
			if out != nil && !serviceMode {
				_, _ = fmt.Fprintln(out, "Teams token refresh failed, starting device login.")
			}
		}
		if serviceMode {
			loginCommand := loginCommandForAuthCache(a.cfg.CachePath, "codex-proxy teams auth")
			if errors.Is(err, os.ErrNotExist) {
				return "", fmt.Errorf("Teams chat access auth cache is missing; run `%s` in a foreground terminal before starting the service", loginCommand)
			}
			if err != nil {
				return "", err
			}
			if refreshFailed != nil {
				return "", fmt.Errorf("Teams chat access token refresh failed: %w; run `%s` in a foreground terminal", refreshFailed, loginCommand)
			}
			return "", fmt.Errorf("Teams chat access auth cache is expired and has no refresh token; run `%s` in a foreground terminal before starting the service", loginCommand)
		}
	}
	tok, err := a.deviceLogin(ctx, out)
	if err != nil {
		return "", err
	}
	if err := validateTokenCacheScopes(tok); err != nil {
		return "", err
	}
	if err := writeTokenCache(a.cfg.CachePath, tok); err != nil {
		return "", err
	}
	return tok.AccessToken, nil
}

func (a *AuthManager) RefreshAccessToken(ctx context.Context) (string, error) {
	tok, err := readTokenCache(a.cfg.CachePath)
	if err != nil {
		return "", err
	}
	refreshed, err := a.refreshCachedToken(ctx, tok)
	if err != nil {
		return "", err
	}
	return refreshed.AccessToken, nil
}

func (a *AuthManager) refreshCachedToken(ctx context.Context, tok TokenCache) (TokenCache, error) {
	if tok.RefreshToken == "" {
		return TokenCache{}, errors.New("teams token cache has no refresh token")
	}
	refreshed, err := a.refresh(ctx, tok.RefreshToken)
	if err != nil {
		return TokenCache{}, err
	}
	if refreshed.AccessToken == "" {
		return TokenCache{}, errors.New("teams token refresh returned no access token")
	}
	if err := validateTokenCacheScopes(refreshed); err != nil {
		return TokenCache{}, err
	}
	if refreshed.RefreshToken == "" {
		refreshed.RefreshToken = tok.RefreshToken
	}
	if err := writeTokenCache(a.cfg.CachePath, refreshed); err != nil {
		return TokenCache{}, err
	}
	return refreshed, nil
}

func (a *AuthManager) deviceLogin(ctx context.Context, out io.Writer) (TokenCache, error) {
	var dc struct {
		DeviceCode      string `json:"device_code"`
		UserCode        string `json:"user_code"`
		VerificationURI string `json:"verification_uri"`
		Message         string `json:"message"`
		ExpiresIn       int64  `json:"expires_in"`
		Interval        int64  `json:"interval"`
	}
	if err := a.form(ctx, "devicecode", url.Values{
		"client_id": {a.cfg.ClientID},
		"scope":     {a.cfg.Scopes},
	}, &dc); err != nil {
		return TokenCache{}, err
	}
	if out != nil {
		if dc.Message != "" {
			_, _ = fmt.Fprintln(out, dc.Message)
		} else {
			_, _ = fmt.Fprintf(out, "Open %s and enter code %s\n", dc.VerificationURI, dc.UserCode)
		}
		if openWindowsBrowser(dc.VerificationURI) {
			_, _ = fmt.Fprintln(out, "Attempted to open the Microsoft device login page in Windows.")
		}
	}
	interval := time.Duration(dc.Interval) * time.Second
	if interval <= 0 {
		interval = 5 * time.Second
	}
	deadline := time.Now().Add(time.Duration(dc.ExpiresIn) * time.Second)
	if dc.ExpiresIn <= 0 {
		deadline = time.Now().Add(15 * time.Minute)
	}
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return TokenCache{}, ctx.Err()
		case <-time.After(interval):
		}
		var raw map[string]any
		err := a.form(ctx, "token", url.Values{
			"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
			"client_id":   {a.cfg.ClientID},
			"device_code": {dc.DeviceCode},
		}, &raw)
		if err == nil {
			return tokenFromMap(raw), nil
		}
		var authErr oauthError
		if errors.As(err, &authErr) {
			switch authErr.Code {
			case "authorization_pending":
				continue
			case "slow_down":
				interval += 5 * time.Second
				continue
			}
		}
		return TokenCache{}, err
	}
	return TokenCache{}, errors.New("device login expired")
}

func (a *AuthManager) refresh(ctx context.Context, refreshToken string) (TokenCache, error) {
	var raw map[string]any
	if err := a.form(ctx, "token", url.Values{
		"grant_type":    {"refresh_token"},
		"client_id":     {a.cfg.ClientID},
		"refresh_token": {refreshToken},
		"scope":         {a.cfg.Scopes},
	}, &raw); err != nil {
		return TokenCache{}, err
	}
	return tokenFromMap(raw), nil
}

type oauthError struct {
	Code        string
	Description string
}

func (e oauthError) Error() string {
	return e.Code
}

func (a *AuthManager) form(ctx context.Context, endpoint string, values url.Values, out any) error {
	body := strings.NewReader(values.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.authorityURL(endpoint), body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := a.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode >= 400 {
		var payload struct {
			Error            string `json:"error"`
			ErrorDescription string `json:"error_description"`
		}
		if json.Unmarshal(data, &payload) == nil && payload.Error != "" {
			return oauthError{Code: payload.Error, Description: payload.ErrorDescription}
		}
		return fmt.Errorf("oauth %s failed: HTTP %d", endpoint, resp.StatusCode)
	}
	if err := json.Unmarshal(data, out); err != nil {
		return err
	}
	return nil
}

func (a *AuthManager) authorityURL(endpoint string) string {
	return fmt.Sprintf("https://login.microsoftonline.com/%s/oauth2/v2.0/%s", url.PathEscape(a.cfg.TenantID), endpoint)
}

func tokenFromMap(raw map[string]any) TokenCache {
	tok := TokenCache{}
	if v, ok := raw["access_token"].(string); ok {
		tok.AccessToken = v
	}
	if v, ok := raw["refresh_token"].(string); ok {
		tok.RefreshToken = v
	}
	if v, ok := raw["scope"].(string); ok {
		tok.Scope = v
	}
	if v, ok := raw["token_type"].(string); ok {
		tok.TokenType = v
	}
	if v, ok := raw["expires_in"].(float64); ok {
		tok.ExpiresIn = int64(v)
		tok.ExpiresAt = time.Now().Unix() + int64(v)
	}
	if tok.ExpiresAt == 0 {
		tok.ExpiresAt = time.Now().Add(time.Hour).Unix()
	}
	return tok
}

func validateTeamsScopes(scopes string) error {
	allowed := map[string]bool{
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
	var unexpected []string
	seen := make(map[string]bool)
	for _, scope := range strings.Fields(scopes) {
		if seen[scope] {
			continue
		}
		seen[scope] = true
		if !allowed[scope] {
			unexpected = append(unexpected, scope)
		}
	}
	if len(unexpected) > 0 {
		return fmt.Errorf("unexpected Teams Graph scope(s): %s; set CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES=1 to override", strings.Join(unexpected, ", "))
	}
	return nil
}

func unsafeTeamsScopesAllowed() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_ALLOW_UNSAFE_SCOPES"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func readTokenCache(path string) (TokenCache, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return TokenCache{}, err
	}
	var tok TokenCache
	if err := json.Unmarshal(data, &tok); err != nil {
		return TokenCache{}, err
	}
	if !tokenCacheLooksManaged(data, tok) {
		return TokenCache{}, fmt.Errorf("teams token cache does not look like a codex-helper Teams token cache")
	}
	if err := validateTokenCacheScopes(tok); err != nil {
		return TokenCache{}, err
	}
	if err := ensureTokenCacheFilePrivate(path); err != nil {
		return TokenCache{}, err
	}
	return tok, nil
}

func readLegacyRefreshTokenCache(path string) (TokenCache, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return TokenCache{}, err
	}
	var tok TokenCache
	if err := json.Unmarshal(data, &tok); err != nil {
		return TokenCache{}, err
	}
	if !tokenCacheLooksManaged(data, tok) {
		return TokenCache{}, fmt.Errorf("teams token cache does not look like a codex-helper Teams token cache")
	}
	if err := ensureTokenCacheFilePrivate(path); err != nil {
		return TokenCache{}, err
	}
	if strings.TrimSpace(tok.RefreshToken) == "" {
		return TokenCache{}, fmt.Errorf("legacy Teams token cache has no refresh token")
	}
	return TokenCache{RefreshToken: tok.RefreshToken}, nil
}

func TokenCacheStatus(path string) (string, error) {
	tok, err := readTokenCache(path)
	if errors.Is(err, os.ErrNotExist) {
		return "missing", nil
	}
	if err != nil {
		return "", err
	}
	now := time.Now().Unix()
	switch {
	case tok.AccessToken == "" && tok.RefreshToken == "":
		return "empty", nil
	case tok.ExpiresAt > now:
		return fmt.Sprintf("present, access token expires at %s", time.Unix(tok.ExpiresAt, 0).Format(time.RFC3339)), nil
	case tok.RefreshToken != "":
		return "present, access token expired, refresh token cached", nil
	default:
		return "present, access token expired", nil
	}
}

func validateTokenCacheScopes(tok TokenCache) error {
	// Cached delegated tokens may contain extra scopes from an older approved
	// login. Do not force a new device-code login just because the tenant
	// returns a wider scope set on refresh; endpoint-level Graph restrictions
	// and chat safety checks are the enforcement boundary here.
	return nil
}

func RemoveTokenCache(path string) error {
	if _, err := readTokenCache(path); err != nil {
		return err
	}
	return os.Remove(path)
}

func tokenCacheLooksManaged(data []byte, tok TokenCache) bool {
	if tok.AccessToken != "" || tok.RefreshToken != "" || tok.ExpiresAt != 0 || tok.ExpiresIn != 0 || tok.Scope != "" || tok.TokenType != "" {
		return true
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return false
	}
	if len(raw) == 0 {
		return true
	}
	for _, key := range []string{"access_token", "refresh_token", "expires_at", "expires_in", "scope", "token_type"} {
		if _, ok := raw[key]; ok {
			return true
		}
	}
	return false
}

func writeTokenCache(path string, tok TokenCache) error {
	dir := filepath.Dir(path)
	_, statErr := os.Stat(dir)
	createdDir := os.IsNotExist(statErr)
	if statErr != nil && !createdDir {
		return statErr
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if createdDir && dir != "." {
		if err := os.Chmod(dir, 0o700); err != nil {
			return err
		}
	}
	data, err := json.MarshalIndent(tok, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".teams-token-*.tmp")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	keep := false
	defer func() {
		if !keep {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	keep = true
	return os.Chmod(path, 0o600)
}

func ensureTokenCacheFilePrivate(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return nil
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("teams token cache must not be a symlink")
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("teams token cache is not a regular file")
	}
	if info.Mode().Perm()&0o077 == 0 {
		return nil
	}
	return os.Chmod(path, 0o600)
}

func defaultTokenCachePath() (string, error) {
	return defaultTeamsTokenCachePath(chatWriteTokenCacheName)
}

func defaultReadTokenCachePath() (string, error) {
	return defaultTeamsTokenCachePath(readTokenCacheName)
}

func defaultFileWriteTokenCachePath() (string, error) {
	return defaultTeamsTokenCachePath(fileWriteTokenCacheName)
}

func defaultFullTokenCachePath() (string, error) {
	return defaultTeamsTokenCachePath(fullTokenCacheName)
}

func defaultTeamsTokenCachePath(fileName string) (string, error) {
	base, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	profile := defaultTeamsAuthProfile()
	scopedPath := filepath.Join(base, "codex-helper", "teams", "profiles", safeScopePathPart(profile), fileName)

	// Preserve existing default-profile logins from pre-scoped builds. New
	// profiles never fall back to the legacy shared cache path.
	if profile == "default" {
		if _, err := os.Stat(scopedPath); err == nil {
			return scopedPath, nil
		}
		legacyPath := filepath.Join(base, "codex-helper", fileName)
		if _, err := os.Stat(legacyPath); err == nil {
			if _, err := readTokenCache(legacyPath); err == nil || unsafeTeamsScopesAllowed() {
				return legacyPath, nil
			}
			if tok, err := readLegacyRefreshTokenCache(legacyPath); err == nil {
				if err := writeTokenCache(scopedPath, tok); err == nil {
					return scopedPath, nil
				}
			}
		}
	}
	return scopedPath, nil
}

func defaultTeamsAuthProfile() string {
	if v := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_AUTH_PROFILE")); v != "" {
		return v
	}
	if v := strings.TrimSpace(os.Getenv(envTeamsProfile)); v != "" {
		return v
	}
	return "default"
}

func openWindowsBrowser(target string) bool {
	if strings.TrimSpace(target) == "" {
		return false
	}
	cmdPath := "/mnt/c/Windows/System32/cmd.exe"
	if _, err := os.Stat(cmdPath); err != nil {
		return false
	}
	cmd := exec.Command(cmdPath, "/c", "start", "", target)
	return cmd.Start() == nil
}

func expandHome(path string) string {
	if path == "~" {
		if home, err := os.UserHomeDir(); err == nil {
			return home
		}
	}
	if strings.HasPrefix(path, "~/") {
		if home, err := os.UserHomeDir(); err == nil {
			return filepath.Join(home, path[2:])
		}
	}
	return path
}
