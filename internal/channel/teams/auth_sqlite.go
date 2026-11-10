package teams

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/appdirs"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

// TokenLocation names a row, not a file containing a serialized token. A
// profile's full/read/chat-write credentials share one database.
type TokenLocation struct{ Database, Slot string }

func (p TokenLocation) String() string { return p.Database + "#" + p.Slot }

func authDatabasePath(path string) string {
	if strings.EqualFold(filepath.Ext(path), ".json") {
		return strings.TrimSuffix(path, filepath.Ext(path)) + ".sqlite"
	}
	if strings.EqualFold(filepath.Ext(path), ".sqlite") {
		return path
	}
	return path + ".sqlite"
}
func authLegacyPath(path string) string {
	return strings.TrimSuffix(path, filepath.Ext(path)) + ".json"
}

func openAuthConfig(path string) (*sql.DB, error) {
	legacy := path
	if strings.EqualFold(filepath.Ext(path), ".sqlite") {
		legacy = authLegacyPath(path)
		if override := expandHome(strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_AUTH_CONFIG"))); override != "" && !strings.EqualFold(filepath.Ext(override), ".sqlite") && filepath.Clean(authDatabasePath(override)) == filepath.Clean(path) {
			legacy = override
		}
	}
	path = authDatabasePath(path)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	db, err := sqlitestate.Open(path, false)
	if err != nil {
		return nil, err
	}
	if err = migrateAuthConfig(db, legacy); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

const authProfileCols = `tenant_id,read_client_id,read_scopes,full_client_id,full_scopes,chat_write_client_id,chat_write_scopes`
const authProfilePut = `INSERT INTO auth_profiles(profile,tenant_id,read_client_id,read_scopes,full_client_id,full_scopes,chat_write_client_id,chat_write_scopes) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(profile) DO UPDATE SET tenant_id=excluded.tenant_id,read_client_id=excluded.read_client_id,read_scopes=excluded.read_scopes,full_client_id=excluded.full_client_id,full_scopes=excluded.full_scopes,chat_write_client_id=excluded.chat_write_client_id,chat_write_scopes=excluded.chat_write_scopes WHERE (tenant_id,read_client_id,read_scopes,full_client_id,full_scopes,chat_write_client_id,chat_write_scopes) IS NOT (excluded.tenant_id,excluded.read_client_id,excluded.read_scopes,excluded.full_client_id,excluded.full_scopes,excluded.chat_write_client_id,excluded.chat_write_scopes)`

func authProfileArgs(name string, p authProfile) []any {
	return []any{name, p.TenantID, p.Read.ClientID, p.Read.Scopes, p.Full.ClientID, p.Full.Scopes, p.ChatWrite.ClientID, p.ChatWrite.Scopes}
}
func scanAuthProfile(row interface{ Scan(...any) error }) (authProfile, error) {
	var p authProfile
	err := row.Scan(&p.TenantID, &p.Read.ClientID, &p.Read.Scopes, &p.Full.ClientID, &p.Full.Scopes, &p.ChatWrite.ClientID, &p.ChatWrite.Scopes)
	if errors.Is(err, sql.ErrNoRows) {
		return p, nil
	}
	return p, err
}
func loadNativeAuthConfig(path, profile string) (authFile, error) {
	db, err := openAuthConfig(path)
	if err != nil {
		return authFile{}, err
	}
	defer db.Close()
	tx, err := db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return authFile{}, err
	}
	defer tx.Rollback()
	global, err := scanAuthProfile(tx.QueryRow(`SELECT ` + authProfileCols + ` FROM auth_profiles WHERE profile=''`))
	if err != nil {
		return authFile{}, err
	}
	p, err := scanAuthProfile(tx.QueryRow(`SELECT `+authProfileCols+` FROM auth_profiles WHERE profile=?`, profile))
	if err != nil {
		return authFile{}, err
	}
	file := authFile{TenantID: global.TenantID, Read: global.Read, Full: global.Full, ChatWrite: global.ChatWrite, Profiles: map[string]authProfile{profile: p}}
	return file, tx.Commit()
}

// UpdateAuthConfig updates only supported defaults; profile-specific rows and
// the other credential kinds stay untouched in the same business transaction.
func UpdateAuthConfig(path string, set map[string]string) error {
	db, err := openAuthConfig(path)
	if err != nil {
		return err
	}
	defer db.Close()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	p, err := scanAuthProfile(tx.QueryRow(`SELECT ` + authProfileCols + ` FROM auth_profiles WHERE profile=''`))
	if err != nil {
		return err
	}
	for key, value := range set {
		value = strings.TrimSpace(value)
		switch key {
		case "tenant_id":
			p.TenantID = value
		case "client_id":
			p.Full.ClientID = value
		case "scopes":
			p.Full.Scopes = value
		case "read_client_id":
			p.Read.ClientID = value
		case "read_scopes":
			p.Read.Scopes = value
		default:
			return fmt.Errorf("unsupported Teams auth setting %q", key)
		}
	}
	if _, err = tx.Exec(authProfilePut, authProfileArgs("", p)...); err != nil {
		return err
	}
	return tx.Commit()
}

const tokenCols = `access_token,refresh_token,expires_at,expires_in,scope,token_type`
const tokenPut = `INSERT INTO tokens(slot,access_token,refresh_token,expires_at,expires_in,scope,token_type) VALUES(?,?,?,?,?,?,?) ON CONFLICT(slot) DO UPDATE SET access_token=excluded.access_token,refresh_token=excluded.refresh_token,expires_at=excluded.expires_at,expires_in=excluded.expires_in,scope=excluded.scope,token_type=excluded.token_type WHERE (access_token,refresh_token,expires_at,expires_in,scope,token_type) IS NOT (excluded.access_token,excluded.refresh_token,excluded.expires_at,excluded.expires_in,excluded.scope,excluded.token_type)`

func tokenArgs(slot string, t TokenCache) []any {
	return []any{slot, t.AccessToken, t.RefreshToken, t.ExpiresAt, t.ExpiresIn, t.Scope, t.TokenType}
}

type tokenStore struct{ db *sql.DB }

func openTokens(path string, legacy []legacyTokenSource) (*tokenStore, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	db, err := sqlitestate.Open(path, false)
	if err != nil {
		return nil, err
	}
	if err = migrateTokens(db, legacy); err != nil {
		db.Close()
		return nil, err
	}
	return &tokenStore{db: db}, nil
}
func checkTokenVersion(db *sql.DB) error {
	var v int
	if err := db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		return err
	}
	if v != 1 {
		return fmt.Errorf("unsupported Teams token database version %d", v)
	}
	return nil
}

func openExistingTokens(path string) (*tokenStore, error) {
	db, err := sqlitestate.OpenExisting(path)
	if err != nil {
		return nil, err
	}
	if err = checkTokenVersion(db); err != nil {
		db.Close()
		return nil, err
	}
	return &tokenStore{db: db}, nil
}
func readNativeToken(db *sql.DB, slot string) (TokenCache, error) {
	var t TokenCache
	err := db.QueryRow(`SELECT `+tokenCols+` FROM tokens WHERE slot=?`, slot).Scan(&t.AccessToken, &t.RefreshToken, &t.ExpiresAt, &t.ExpiresIn, &t.Scope, &t.TokenType)
	if errors.Is(err, sql.ErrNoRows) {
		err = os.ErrNotExist
	}
	return t, err
}
func ReadTokenCache(p TokenLocation) (TokenCache, error) {
	db, err := sqlitestate.Open(p.Database, true)
	if err != nil {
		return TokenCache{}, err
	}
	defer db.Close()
	if err = checkTokenVersion(db); err != nil {
		return TokenCache{}, err
	}
	return readNativeToken(db, p.Slot)
}
func WriteTokenCache(p TokenLocation, t TokenCache) error {
	if strings.TrimSpace(p.Slot) == "" {
		return errors.New("empty Teams token slot")
	}
	store, err := openTokens(p.Database, nil)
	if err != nil {
		return err
	}
	defer store.db.Close()
	_, err = store.db.Exec(tokenPut, tokenArgs(p.Slot, t)...)
	return err
}

// RemoveTokenCache removes one credential, never a database containing other
// identities. The caller keeps the refresh flock across this operation.
func RemoveTokenCache(ctx context.Context, p TokenLocation) error {
	unlock, err := lockFile(ctx, p.lockPath())
	if err != nil {
		return err
	}
	defer unlock()
	db, err := sqlitestate.OpenExisting(p.Database)
	if err != nil {
		return err
	}
	defer db.Close()
	if err = checkTokenVersion(db); err != nil {
		return err
	}
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	tok, err := readTokenInTx(tx, p.Slot)
	if err != nil {
		return err
	}
	if tok.AccessToken == "" && tok.RefreshToken == "" {
		return errors.New("record is not a Teams token; not removed")
	}
	if _, err = tx.Exec(`DELETE FROM tokens WHERE slot=?`, p.Slot); err != nil {
		return err
	}
	return tx.Commit()
}
func readTokenInTx(tx *sql.Tx, slot string) (TokenCache, error) {
	var t TokenCache
	err := tx.QueryRow(`SELECT `+tokenCols+` FROM tokens WHERE slot=?`, slot).Scan(&t.AccessToken, &t.RefreshToken, &t.ExpiresAt, &t.ExpiresIn, &t.Scope, &t.TokenType)
	if errors.Is(err, sql.ErrNoRows) {
		err = os.ErrNotExist
	}
	return t, err
}
func (p TokenLocation) lockPath() string { return p.Database + "." + safePathPart(p.Slot) + ".lock" }

// Override names are migration locators only. Different named override files
// in one directory become separate rows in its token database.
func resolveTokenOverride(path, kind string) (TokenLocation, error) {
	path = expandHome(path)
	var err error
	path, err = filepath.Abs(path)
	if err != nil {
		return TokenLocation{}, err
	}
	if strings.EqualFold(filepath.Ext(path), ".sqlite") {
		p := TokenLocation{Database: path, Slot: kind}
		st, err := openTokens(path, nil)
		if err != nil {
			return p, err
		}
		st.db.Close()
		return p, nil
	}
	slot := filepath.Base(path)
	p := TokenLocation{Database: filepath.Join(filepath.Dir(path), "teams-tokens.sqlite"), Slot: slot}
	st, err := openTokens(p.Database, legacyOverrideSources(path))
	if err != nil {
		return p, err
	}
	st.db.Close()
	return p, nil
}
func resolveProfileToken(profile, slot string) (TokenLocation, error) {
	dir, err := appdirs.StatePath("teams", "profiles", safePathPart(profile))
	if err != nil {
		return TokenLocation{}, err
	}
	p := TokenLocation{Database: filepath.Join(dir, "teams-tokens.sqlite"), Slot: slot}
	legacy, err := legacyProfileTokenSources(profile)
	if err != nil {
		return p, err
	}
	st, err := openTokens(p.Database, legacy)
	if err != nil {
		return p, err
	}
	st.db.Close()
	return p, nil
}
