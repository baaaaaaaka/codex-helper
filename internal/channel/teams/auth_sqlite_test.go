package teams

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

func TestAuthSQLiteMigrationAtomicAndQuiet(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "teams-auth.json")
	raw := `{"tenant_id":"租户","read":{"client_id":"reader","scopes":"User.Read"},"profiles":{"work":{"tenant_id":"work-tenant","full":{"client_id":"worker"}}}}`
	if err := os.WriteFile(config, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	if err := UpdateAuthConfig(config, map[string]string{"client_id": "writer"}); err != nil {
		t.Fatal(err)
	}
	native := authDatabasePath(config)
	file, err := loadNativeAuthConfig(native, "work")
	if err != nil || file.TenantID != "租户" || file.Full.ClientID != "writer" || file.Read.ClientID != "reader" || file.Profiles["work"].Full.ClientID != "worker" {
		t.Fatalf("native config %#v/%v", file, err)
	}
	if b, err := os.ReadFile(config + ".migrated"); err != nil || string(b) != raw {
		t.Fatalf("config archive %q/%v", b, err)
	}
	if err = os.WriteFile(config, []byte("poisoned"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = loadNativeAuthConfig(native, "work"); err != nil {
		t.Fatal(err)
	}
	legacyFull := filepath.Join(dir, "full.json")
	legacyRead := filepath.Join(dir, "read.json")
	if err = os.WriteFile(legacyFull, []byte(`{"access_token":"old","refresh_token":"original","expires_at":9007199254740993,"expires_in":123,"scope":"User.Read","token_type":"Bearer"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(legacyRead, []byte(`{"refresh_token":"reader"}`), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "tokens.sqlite")
	sources := []legacyTokenSource{{legacyFull, "full"}, {legacyRead, "read"}}
	st, err := openTokens(path, sources)
	if err != nil {
		t.Fatal(err)
	}
	defer st.db.Close()
	full := TokenLocation{Database: path, Slot: "full"}
	read := full
	read.Slot = "read"
	tok, err := ReadTokenCache(full)
	if err != nil || tok.ExpiresAt != 9007199254740993 || tok.RefreshToken != "original" {
		t.Fatalf("token migration failed: %v", err)
	}
	observer, err := sqlitestate.Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer observer.Close()
	changes := func() int {
		var n int
		if err = st.db.QueryRow(`SELECT total_changes()`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	version := func() int {
		var n int
		if err = observer.QueryRow(`PRAGMA data_version`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	c, d := changes(), version()
	for range 20 {
		if _, err = st.db.Exec(tokenPut, tokenArgs("full", tok)...); err != nil {
			t.Fatal(err)
		}
		if _, err = ReadTokenCache(full); err != nil {
			t.Fatal(err)
		}
	}
	if c != changes() || d != version() {
		t.Fatal("unchanged credentials wrote")
	}
	// Deleting one credential cannot delete the reader or resurrect an archive.
	if err = RemoveTokenCache(context.Background(), full); err != nil {
		t.Fatal(err)
	}
	if _, err = ReadTokenCache(full); !os.IsNotExist(err) {
		t.Fatalf("logout: %v", err)
	}
	reader, err := ReadTokenCache(read)
	if err != nil || reader.RefreshToken != "reader" {
		t.Fatalf("reader affected: %v", err)
	}
	if err = os.WriteFile(legacyFull, []byte("poisoned"), 0600); err != nil {
		t.Fatal(err)
	}
	again, err := openTokens(path, sources)
	if err != nil {
		t.Fatal(err)
	}
	again.db.Close()
	if _, err = ReadTokenCache(full); !os.IsNotExist(err) {
		t.Fatal("resurrected legacy token")
	}
}

func TestAuthSQLiteMigrationRollsBack(t *testing.T) {
	for _, raw := range []string{`null`, `{`, `{} trailing`, `{"profiles":{"work":{},"work":{}}}`, `{"profiles":{"one":{},"two":null}}`} {
		t.Run("config/"+raw, func(t *testing.T) {
			legacy := filepath.Join(t.TempDir(), "auth.json")
			if err := os.WriteFile(legacy, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			if err := UpdateAuthConfig(legacy, map[string]string{"client_id": "new"}); err == nil {
				t.Fatal("invalid config imported")
			}
			assertAuthImportRolledBack(t, authDatabasePath(legacy), legacy)
		})
	}
	for _, raw := range []string{`null`, `{`, `{} trailing`, `{"schema_version":999}`, `{"expires_at":"not-integer"}`} {
		t.Run("token/"+raw, func(t *testing.T) {
			dir := t.TempDir()
			good := filepath.Join(dir, "full.json")
			bad := filepath.Join(dir, "read.json")
			path := filepath.Join(dir, "tokens.sqlite")
			if err := os.WriteFile(good, []byte(`{"refresh_token":"keep"}`), 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(bad, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			if st, err := openTokens(path, []legacyTokenSource{{good, "full"}, {bad, "read"}}); err == nil {
				st.db.Close()
				t.Fatal("invalid token imported")
			}
			assertAuthImportRolledBack(t, path, good)
			if _, err := os.Stat(bad); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func assertAuthImportRolledBack(t *testing.T, path, legacy string) {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var n, v int
	if err = db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil || n != 0 || v != 0 {
		t.Fatalf("partial import tables=%d version=%d: %v", n, v, err)
	}
	if _, err = os.Stat(legacy); err != nil {
		t.Fatal(err)
	}
}

func TestAuthSQLiteFutureCorruptAndMissingFailClosed(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tokens.sqlite")
	if _, err := ReadTokenCache(TokenLocation{Database: path, Slot: "full"}); !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("read created missing database")
	}
	if err := WriteTokenCache(TokenLocation{Database: path, Slot: "full"}, TokenCache{RefreshToken: "keep"}); err != nil {
		t.Fatal(err)
	}
	db, err := sqlitestate.OpenExisting(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`PRAGMA user_version=99`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if _, err = ReadTokenCache(TokenLocation{Database: path, Slot: "full"}); err == nil {
		t.Fatal("future database accepted")
	}
	if err = WriteTokenCache(TokenLocation{Database: path, Slot: "full"}, TokenCache{}); err == nil {
		t.Fatal("future database overwritten")
	}
	corrupt := filepath.Join(dir, "corrupt.sqlite")
	if err = os.WriteFile(corrupt, []byte("broken database"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = openExistingTokens(corrupt); err == nil {
		t.Fatal("corrupt database accepted")
	}
}

func TestAuthSQLiteCustomLegacyPathsKeepDistinctCredentials(t *testing.T) {
	dir := t.TempDir()
	config := filepath.Join(dir, "auth.private")
	raw := `{"tenant_id":"tenant-exact","full":{"client_id":"configured-client"}}`
	if err := os.WriteFile(config, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CODEX_HELPER_TEAMS_AUTH_CONFIG", config)
	file, path, err := loadAuthConfigFile("default")
	if err != nil || file.TenantID != "tenant-exact" || file.Full.ClientID != "configured-client" || path != config+".sqlite" {
		t.Fatalf("custom config %#v %s: %v", file, path, err)
	}
	if archived, err := os.ReadFile(config + ".migrated"); err != nil || string(archived) != raw {
		t.Fatal("custom config was not migrated", err)
	}
	fullPath, readPath := filepath.Join(dir, "token.full"), filepath.Join(dir, "token.read")
	if err := os.WriteFile(fullPath, []byte(`{"access_token":"full-exact","refresh_token":"full-refresh"}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(readPath, []byte(`{"access_token":"read-exact","refresh_token":"read-refresh"}`), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE", fullPath)
	t.Setenv("CODEX_HELPER_TEAMS_READ_TOKEN_CACHE", readPath)
	full, err := resolveTokenOverride(fullPath, "full")
	if err != nil {
		t.Fatal(err)
	}
	read, err := resolveTokenOverride(readPath, "read")
	if err != nil {
		t.Fatal(err)
	}
	if full.Database != read.Database || full.Slot == read.Slot {
		t.Fatalf("overrides collided: %+v %+v", full, read)
	}
	a, err := ReadTokenCache(full)
	if err != nil || a.AccessToken != "full-exact" {
		t.Fatal("full credential changed", err)
	}
	b, err := ReadTokenCache(read)
	if err != nil || b.AccessToken != "read-exact" {
		t.Fatal("reader credential changed", err)
	}
}
