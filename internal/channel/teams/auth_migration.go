package teams

// Legacy JSON is confined to this one-time import. Schema, credentials and
// source markers commit together; archived files never participate in reads.
import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
)

func authImportFile(path string) (*os.File, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("legacy Teams auth source is not a regular file: %s", path)
	}
	if info.Mode().Perm() != 0600 {
		if err = os.Chmod(path, 0600); err != nil {
			return nil, err
		}
	}
	return os.Open(path)
}
func endAuthJSON(d *json.Decoder) error {
	if _, err := d.Token(); err != io.EOF {
		if err != nil {
			return err
		}
		return errors.New("trailing Teams auth JSON")
	}
	return nil
}
func archiveAuthSources(paths []string) error {
	for _, path := range paths {
		if _, err := os.Lstat(path + ".migrated"); err == nil {
			return fmt.Errorf("legacy Teams auth archive already exists: %s", path)
		} else if !os.IsNotExist(err) {
			return err
		}
		if err := os.Rename(path, path+".migrated"); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}
func migrateAuthConfig(db *sql.DB, path string) error {
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var v int
	if err = tx.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		return err
	}
	if v == 1 {
		return sqlitestate.CommitSchema(tx, db)
	}
	if v != 0 {
		return fmt.Errorf("unsupported Teams auth config version %d", v)
	}
	if _, err = tx.Exec(`CREATE TABLE auth_profiles(profile TEXT PRIMARY KEY,tenant_id TEXT NOT NULL,read_client_id TEXT NOT NULL,read_scopes TEXT NOT NULL,full_client_id TEXT NOT NULL,full_scopes TEXT NOT NULL,chat_write_client_id TEXT NOT NULL,chat_write_scopes TEXT NOT NULL); CREATE TABLE auth_imports(source_path TEXT PRIMARY KEY)`); err != nil {
		return err
	}
	f, err := authImportFile(path)
	var imported []string
	if err == nil {
		err = importAuthProfiles(tx, json.NewDecoder(f))
		f.Close()
		if err != nil {
			return err
		}
		if _, err = tx.Exec(`INSERT INTO auth_imports VALUES(?)`, path); err != nil {
			return err
		}
		imported = append(imported, path)
	} else if !os.IsNotExist(err) {
		return err
	}
	if _, err = tx.Exec(`PRAGMA user_version=1`); err != nil {
		return err
	}
	if err = sqlitestate.CommitSchema(tx, db); err != nil {
		return err
	}
	return archiveAuthSources(imported)
}
func importAuthProfiles(tx *sql.Tx, d *json.Decoder) error {
	tok, err := d.Token()
	if err != nil || tok != json.Delim('{') {
		return fmt.Errorf("invalid legacy Teams auth object: %v", err)
	}
	// Plain INSERT makes duplicate profile names a migration error, with no
	// growing in-memory mirror of the profile collection.
	stmt, err := tx.Prepare(`INSERT INTO auth_profiles(profile,` + authProfileCols + `) VALUES(?,?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	var global authProfile
	seen := map[string]bool{}
	for d.More() {
		token, err := d.Token()
		if err != nil {
			return err
		}
		key := token.(string)
		if seen[key] {
			return fmt.Errorf("duplicate Teams auth field %q", key)
		}
		seen[key] = true
		switch key {
		case "tenant_id":
			err = d.Decode(&global.TenantID)
		case "read":
			err = d.Decode(&global.Read)
		case "full":
			err = d.Decode(&global.Full)
		case "chat_write":
			err = d.Decode(&global.ChatWrite)
		case "profiles":
			token, err = d.Token()
			if err != nil || token != json.Delim('{') {
				return errors.New("invalid Teams auth profiles")
			}
			for d.More() {
				name, err := d.Token()
				if err != nil {
					return err
				}
				if strings.TrimSpace(name.(string)) == "" {
					return errors.New("empty Teams auth profile")
				}
				var p *authProfile
				if err = d.Decode(&p); err != nil {
					return err
				}
				if p == nil {
					return errors.New("null Teams auth profile")
				}
				if _, err = stmt.Exec(authProfileArgs(name.(string), *p)...); err != nil {
					return err
				}
			}
			_, err = d.Token()
		default:
			err = skipAuthJSON(d)
		}
		if err != nil {
			return err
		}
	}
	if _, err = d.Token(); err != nil {
		return err
	}
	if err = endAuthJSON(d); err != nil {
		return err
	}
	_, err = stmt.Exec(authProfileArgs("", global)...)
	return err
}
func skipAuthJSON(d *json.Decoder) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); ok && (delim == '{' || delim == '[') {
		for d.More() {
			if delim == '{' {
				if _, err = d.Token(); err != nil {
					return err
				}
			}
			if err = skipAuthJSON(d); err != nil {
				return err
			}
		}
		_, err = d.Token()
	}
	return err
}

type legacyTokenSource struct{ Path, Slot string }

func legacyProfileTokenSources(profile string) ([]legacyTokenSource, error) {
	var out []legacyTokenSource
	for _, pair := range [][2]string{{"teams-full-token.json", "full"}, {"teams-chat-write-token.json", "chat_write"}, {"teams-read-token.json", "read"}} {
		p, err := appdirs.StatePath("teams", "profiles", safePathPart(profile), pair[0])
		if err != nil {
			return nil, err
		}
		out = append(out, legacyTokenSource{p, pair[1]})
		if p, err = appdirs.LegacyCachePath("teams", "profiles", safePathPart(profile), pair[0]); err == nil {
			out = append(out, legacyTokenSource{p, pair[1]})
		}
		if profile == "default" {
			if p, err = appdirs.LegacyCachePath(pair[0]); err == nil {
				out = append(out, legacyTokenSource{p, pair[1]})
			}
		}
	}
	return out, nil
}
func migrateTokens(db *sql.DB, sources []legacyTokenSource) error {
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var v int
	if err = tx.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		return err
	}
	if v == 1 {
		return sqlitestate.CommitSchema(tx, db)
	}
	if v != 0 {
		return fmt.Errorf("unsupported Teams token database version %d", v)
	}
	if _, err = tx.Exec(`CREATE TABLE tokens(slot TEXT PRIMARY KEY CHECK(slot<>''),access_token TEXT NOT NULL,refresh_token TEXT NOT NULL,expires_at INTEGER NOT NULL,expires_in INTEGER NOT NULL,scope TEXT NOT NULL,token_type TEXT NOT NULL); CREATE TABLE token_imports(source_path TEXT PRIMARY KEY)`); err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO tokens(slot,` + tokenCols + `) VALUES(?,?,?,?,?,?,?) ON CONFLICT(slot) DO NOTHING`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	var imported []string
	for _, source := range sources {
		f, err := authImportFile(source.Path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		// Match v0.1.20: ignore fields CXP does not use (raw token responses carry
		// ext_expires_in and id_token) but require a token-shaped object.
		d := json.NewDecoder(f)
		var raw json.RawMessage
		var token *TokenCache
		err = d.Decode(&raw)
		if err == nil {
			err = endAuthJSON(d)
		}
		if err == nil {
			err = json.Unmarshal(raw, &token)
		}
		if err == nil && token != nil && !legacyTokenFieldsPresent(raw) {
			err = errors.New("no Teams token fields")
		}
		f.Close()
		if err != nil {
			return fmt.Errorf("%s does not look like a valid Teams token cache; migration stopped: %w", source.Path, err)
		}
		if token == nil {
			return errors.New("null Teams token cache")
		}
		if token.AccessToken != "" || token.RefreshToken != "" {
			if _, err = stmt.Exec(tokenArgs(source.Slot, *token)...); err != nil {
				return err
			}
		}
		if _, err = tx.Exec(`INSERT INTO token_imports VALUES(?)`, source.Path); err != nil {
			return err
		}
		imported = append(imported, source.Path)
	}
	if _, err = tx.Exec(`PRAGMA user_version=1`); err != nil {
		return err
	}
	if err = sqlitestate.CommitSchema(tx, db); err != nil {
		return err
	}
	return archiveAuthSources(imported)
}
func legacyTokenFieldsPresent(raw json.RawMessage) bool {
	var fields map[string]json.RawMessage
	if json.Unmarshal(raw, &fields) != nil {
		return false
	}
	if len(fields) == 0 {
		return true
	}
	for _, key := range []string{"access_token", "refresh_token", "expires_at", "expires_in", "scope", "token_type"} {
		if _, ok := fields[key]; ok {
			return true
		}
	}
	return false
}
func legacyOverrideSources(path string) []legacyTokenSource {
	paths := []string{path}
	for _, name := range []string{"CODEX_HELPER_TEAMS_FULL_TOKEN_CACHE", "CODEX_HELPER_TEAMS_READ_TOKEN_CACHE"} {
		candidate := expandHome(strings.TrimSpace(os.Getenv(name)))
		if candidate != "" {
			var err error
			candidate, err = filepath.Abs(candidate)
			if err != nil {
				continue
			}
		}
		if candidate != "" && filepath.Clean(filepath.Dir(candidate)) == filepath.Clean(filepath.Dir(path)) && !strings.EqualFold(filepath.Ext(candidate), ".sqlite") {
			paths = append(paths, candidate)
		}
	}
	seen := map[string]bool{}
	var sources []legacyTokenSource
	for _, p := range paths {
		if !seen[p] {
			seen[p] = true
			sources = append(sources, legacyTokenSource{Path: p, Slot: filepath.Base(p)})
		}
	}
	return sources
}
