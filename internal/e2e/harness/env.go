// Package harness holds the SUT-agnostic parts of the Teams black-box E2E
// harness: the isolated environment, the network sandbox transport, the fake
// Codex, waiters, observations and verdict recording.
package harness

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

// Env is one scenario's isolated filesystem and process environment.
//
// The SUT reads its configuration from environment variables. Every variable
// is recorded in Vars; a subprocess SUT gets exactly Vars (plus PATH). Only
// when SetProcessEnv is true (in-process adapters) does Env also t.Setenv
// them, which forbids parallel scenarios within one test binary.
type Env struct {
	Root        string
	Home        string
	StateDir    string
	ConfigDir   string
	CacheDir    string
	DataDir     string
	RuntimeDir  string
	CodexHome   string
	SessionsDir string
	TokenDir    string
	WorkRoot    string // directory under which scenario work dirs are created

	TokenCaches map[string]string // kind -> path
	Vars        map[string]string // every variable this Env set
}

// ClientIDs used for the fake app registrations.
const (
	FakeClientID     = "e2e0c11e-0000-4000-8000-000000000001"
	FakeReadClientID = "e2e0c11e-0000-4000-8000-000000000002"
	MachineLabel     = "e2ehost"
)

// SetProcessEnv makes NewEnv apply the variables to the test process. The
// in-process adapter needs it; sut.Run clears it for the subprocess adapter.
var SetProcessEnv = true

// Parallel marks the test parallel when the environment allows it (i.e.
// when the SUT runs as a subprocess).
func Parallel(t *testing.T) {
	if !SetProcessEnv {
		t.Parallel()
	}
}

// NewEnv creates the directory tree, writes fake token caches issued by g and
// points every relevant environment variable at them.
func NewEnv(t testing.TB, g *fakegraph.Server) *Env {
	t.Helper()
	root := t.TempDir()
	e := &Env{
		Root:        root,
		Home:        filepath.Join(root, "home"),
		StateDir:    filepath.Join(root, "home", ".local", "state"),
		ConfigDir:   filepath.Join(root, "home", ".config"),
		CacheDir:    filepath.Join(root, "home", ".cache"),
		DataDir:     filepath.Join(root, "home", ".local", "share"),
		RuntimeDir:  filepath.Join(root, "run"),
		CodexHome:   filepath.Join(root, "home", ".codex"),
		TokenDir:    filepath.Join(root, "home", ".local", "state", "e2e-tokens"),
		WorkRoot:    filepath.Join(root, "work"),
		TokenCaches: map[string]string{},
		Vars:        map[string]string{},
	}
	e.SessionsDir = filepath.Join(e.CodexHome, "sessions")
	for _, d := range []string{e.Home, e.StateDir, e.ConfigDir, e.CacheDir, e.DataDir, e.RuntimeDir, e.CodexHome, e.SessionsDir, e.WorkRoot} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			t.Fatalf("mkdir %s: %v", d, err)
		}
	}
	if err := os.MkdirAll(e.TokenDir, 0o700); err != nil {
		t.Fatalf("mkdir token dir: %v", err)
	}
	if err := os.Chmod(e.RuntimeDir, 0o700); err != nil {
		t.Fatalf("chmod runtime dir: %v", err)
	}
	// Minimal Codex config so nothing tries to discover a real one.
	if err := os.WriteFile(filepath.Join(e.CodexHome, "config.toml"), []byte("# e2e\n"), 0o600); err != nil {
		t.Fatalf("write codex config: %v", err)
	}

	scopes := map[string]string{
		"chat":       "openid profile offline_access User.Read Chat.ReadWrite OnlineMeetings.ReadWrite",
		"read":       "openid profile offline_access User.Read Chat.Read Files.Read",
		"file-write": "openid profile offline_access User.Read Chat.ReadWrite Files.ReadWrite",
		"full":       "openid profile offline_access User.Read Chat.ReadWrite OnlineMeetings.ReadWrite Files.ReadWrite",
	}
	for _, kind := range []string{"chat", "read", "file-write", "full"} {
		access, refresh := g.IssueTokens(kind)
		path := filepath.Join(e.TokenDir, "teams-"+kind+"-token.json")
		writeTokenCache(t, path, access, refresh, scopes[kind], time.Now().Add(2*time.Hour))
		e.TokenCaches[kind] = path
	}

	set := func(k, v string) {
		if SetProcessEnv {
			t.Setenv(k, v)
		}
		e.Vars[k] = v
	}
	set("HOME", e.Home)
	set("XDG_STATE_HOME", e.StateDir)
	set("XDG_CONFIG_HOME", e.ConfigDir)
	set("XDG_CACHE_HOME", e.CacheDir)
	set("XDG_DATA_HOME", e.DataDir)
	set("XDG_RUNTIME_DIR", e.RuntimeDir)
	set("CODEX_HOME", e.CodexHome)
	set("CODEX_DIR", "")
	set("CODEX_HELPER_STATE_DIR", filepath.Join(e.StateDir, "codex-helper"))
	set("CODEX_HELPER_CONFIG", filepath.Join(e.ConfigDir, "codex-helper", "config.json"))
	set("CODEX_HELPER_TEAMS_AUTH_CONFIG", filepath.Join(e.ConfigDir, "codex-helper", "teams-auth.json"))
	set("CODEX_HELPER_TEAMS_TENANT_ID", fakegraph.TenantID)
	set("CODEX_HELPER_TEAMS_CLIENT_ID", FakeClientID)
	set("CODEX_HELPER_TEAMS_READ_CLIENT_ID", FakeReadClientID)
	set("CODEX_HELPER_TEAMS_FILE_WRITE_CLIENT_ID", FakeClientID)
	set("CODEX_HELPER_TEAMS_FULL_CLIENT_ID", FakeClientID)
	set("CODEX_HELPER_TEAMS_TOKEN_CACHE", e.TokenCaches["chat"])
	set("CODEX_HELPER_TEAMS_READ_TOKEN_CACHE", e.TokenCaches["read"])
	set("CODEX_HELPER_TEAMS_FILE_WRITE_TOKEN_CACHE", e.TokenCaches["file-write"])
	set("CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE", e.TokenCaches["full"])
	set("CODEX_HELPER_TEAMS_PROFILE", "")
	set("CODEX_HELPER_TEAMS_AUTH_PROFILE", "")
	set("CODEX_HELPER_TEAMS_MACHINE_LABEL", MachineLabel)
	set("CODEX_HELPER_TEAMS_MACHINE_ID", "")
	// Service mode: never fall back to interactive device login.
	set("CODEX_HELPER_TEAMS_SERVICE", "1")
	set("CODEX_HELPER_TEAMS_CHILD", "")
	set("CODEX_HELPER_TEAMS_ALLOWED_SHAREPOINT_HOSTS", "")
	set("CXP_RUNTIME_DISABLE", "1")
	// Make any accidental real-network attempt fail fast even outside the
	// offline Docker runner.
	set("HTTPS_PROXY", "http://127.0.0.1:9")
	set("HTTP_PROXY", "http://127.0.0.1:9")
	set("NO_PROXY", "127.0.0.1,localhost")
	return e
}

func writeTokenCache(t testing.TB, path, access, refresh, scope string, expires time.Time) {
	t.Helper()
	data, err := json.MarshalIndent(map[string]any{
		"access_token":  access,
		"refresh_token": refresh,
		"expires_at":    expires.Unix(),
		"expires_in":    int64(time.Until(expires) / time.Second),
		"scope":         scope,
		"token_type":    "Bearer",
	}, "", "  ")
	if err != nil {
		t.Fatalf("marshal token cache: %v", err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write token cache: %v", err)
	}
}

// ExpireToken rewrites a token cache so the access token is expired, forcing
// a refresh through the fake token endpoint.
func (e *Env) ExpireToken(t testing.TB, kind string) {
	t.Helper()
	path := e.TokenCaches[kind]
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read token cache: %v", err)
	}
	var tok map[string]any
	if err := json.Unmarshal(data, &tok); err != nil {
		t.Fatalf("decode token cache: %v", err)
	}
	tok["expires_at"] = time.Now().Add(-time.Hour).Unix()
	out, _ := json.MarshalIndent(tok, "", "  ")
	if err := os.WriteFile(path, out, 0o600); err != nil {
		t.Fatalf("write token cache: %v", err)
	}
}

// WorkDir creates a fresh directory for a Codex session working directory.
func (e *Env) WorkDir(t testing.TB, name string) string {
	t.Helper()
	name = strings.Map(func(r rune) rune {
		if r == '/' || r == ' ' {
			return '-'
		}
		return r
	}, name)
	dir := filepath.Join(e.WorkRoot, name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir workdir: %v", err)
	}
	return dir
}

// String summarises the environment for logs.
func (e *Env) String() string {
	return fmt.Sprintf("home=%s codex_home=%s", e.Home, e.CodexHome)
}
