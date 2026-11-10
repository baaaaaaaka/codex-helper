// Package harness is the Teams black-box E2E harness: the isolated
// environment, the fake Codex, the scenario lifecycle, waiters, the
// transcript goldens and the global invariants checked at teardown. It
// reaches cxp only from outside (see internal/e2e/doc.go).
package harness

import (
	"database/sql"

	"fmt"
	_ "modernc.org/sqlite"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
	"github.com/baaaaaaaka/codex-helper/internal/e2e/rollout"
)

// Env is one scenario's isolated filesystem and process environment.
//
// cxp reads its configuration from environment variables a user could set
// (HOME, XDG dirs, CODEX_HOME, token caches, ...). Every one is recorded in
// Vars; the cxp processes get exactly Vars (plus PATH and TMPDIR). The test
// process's own environment is never changed, so scenarios run in
// parallel.
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

	mu       sync.Mutex
	rollouts []*rollout.Writer
}

// TrackRollout registers a rollout the scenario writes as a local Codex
// user, for the order and catch-up invariants.
func (e *Env) TrackRollout(w *rollout.Writer) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, have := range e.rollouts {
		if have.Path == w.Path {
			return
		}
	}
	e.rollouts = append(e.rollouts, w)
}

// Rollouts returns the tracked rollouts.
func (e *Env) Rollouts() []*rollout.Writer {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]*rollout.Writer(nil), e.rollouts...)
}

// MovedTo is the environment of another machine whose home is home, a
// copy of this one's (the owner's machine move): every path under e.Home
// moves there and the runtime directory is the machine's own. The work
// directories, outside the home, are shared.
func (e *Env) MovedTo(t testing.TB, home string) *Env {
	t.Helper()
	move := func(p string) string {
		rel, err := filepath.Rel(e.Home, p)
		if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
			return p
		}
		return filepath.Join(home, rel)
	}
	n := &Env{Root: e.Root, Home: home, StateDir: move(e.StateDir), ConfigDir: move(e.ConfigDir), CacheDir: move(e.CacheDir),
		DataDir: move(e.DataDir), RuntimeDir: home + ".run", CodexHome: move(e.CodexHome), SessionsDir: move(e.SessionsDir),
		TokenDir: move(e.TokenDir), WorkRoot: e.WorkRoot, TokenCaches: map[string]string{}, Vars: map[string]string{}}
	for k, v := range e.TokenCaches {
		n.TokenCaches[k] = move(v)
	}
	for k, v := range e.Vars {
		n.Vars[k] = move(v)
	}
	n.Vars["XDG_RUNTIME_DIR"] = n.RuntimeDir
	if err := os.MkdirAll(n.RuntimeDir, 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", n.RuntimeDir, err)
	}
	return n
}

// ClientIDs used for the fake app registrations.
const (
	FakeClientID     = "e2e0c11e-0000-4000-8000-000000000001"
	FakeReadClientID = "e2e0c11e-0000-4000-8000-000000000002"
	MachineLabel     = "e2ehost"
)

// Parallel marks the scenario parallel (every SUT is its own process with
// its own environment).
func Parallel(t *testing.T) { t.Parallel() }

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
		path := filepath.Join(e.TokenDir, "teams-tokens.sqlite")
		writeTokenCache(t, path, kind, access, refresh, scopes[kind], time.Now().Add(2*time.Hour))
		e.TokenCaches[kind] = path
	}

	set := func(k, v string) { e.Vars[k] = v }
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

// Synthetic native fixture, independent of the production migration writer.
func writeTokenCache(t testing.TB, path, slot, access, refresh, scope string, expires time.Time) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS tokens(slot TEXT PRIMARY KEY CHECK(slot<>''),access_token TEXT NOT NULL,refresh_token TEXT NOT NULL,expires_at INTEGER NOT NULL,expires_in INTEGER NOT NULL,scope TEXT NOT NULL,token_type TEXT NOT NULL); CREATE TABLE IF NOT EXISTS token_imports(source_path TEXT PRIMARY KEY); PRAGMA user_version=1;`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`INSERT INTO tokens VALUES(?,?,?,?,?,?,?)`, slot, access, refresh, expires.Unix(), int64(time.Until(expires)/time.Second), scope, "Bearer")
	if err != nil {
		t.Fatal(err)
	}
}
func (e *Env) ExpireToken(t testing.TB, kind string) {
	t.Helper()
	db, err := sql.Open("sqlite", e.TokenCaches[kind])
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`UPDATE tokens SET expires_at=? WHERE slot=?`, time.Now().Add(-time.Hour).Unix(), kind); err != nil {
		t.Fatal(err)
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
