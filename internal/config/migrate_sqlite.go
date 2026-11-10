package config

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
	"io"
	"os"
)

func migrateConfigDatabase(db *sql.DB, legacy string) error {
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var version int
	if err = tx.QueryRow(`PRAGMA user_version`).Scan(&version); err != nil {
		return err
	}
	if version == 1 {
		return sqlitestate.CommitSchema(tx, db)
	}
	if version != 0 {
		return fmt.Errorf("unsupported config database version %d", version)
	}
	for _, q := range configSchema {
		if _, err = tx.Exec(q); err != nil {
			return err
		}
	}
	cfg := Config{}
	f, err := os.Open(legacy)
	imported := err == nil
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if imported {
		// Collections are streamed to prepared statements. Retired fields are
		// skipped as tokens; their original bytes remain in the archived source.
		cfg, err = importLegacyConfig(tx, f)
		f.Close()
		if err != nil {
			return fmt.Errorf("migrate config: %w", err)
		}
	}
	cfg.Version = CurrentVersion
	cfg.MinReader = MinReaderVersion
	if _, err = tx.Exec(headerPut, headerArgs(cfg)...); err != nil {
		return err
	}
	if imported {
		if _, err = tx.Exec(`INSERT INTO config_imports VALUES(?)`, legacy); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`PRAGMA user_version=1`); err != nil {
		return err
	}
	if err = sqlitestate.CommitSchema(tx, db); err != nil {
		return err
	}
	if imported {
		if _, err = os.Lstat(legacy + ".migrated"); err == nil {
			return fmt.Errorf("config migration committed; archive already exists")
		}
		if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if err = os.Rename(legacy, legacy+".migrated"); err != nil {
			return fmt.Errorf("config migration committed; archive: %w", err)
		}
	}
	return nil
}
func importLegacyConfig(tx *sql.Tx, reader io.Reader) (Config, error) {
	var v Config
	d := json.NewDecoder(reader)
	token, err := d.Token()
	if err != nil {
		return v, err
	}
	if token != json.Delim('{') {
		return v, fmt.Errorf("legacy config must be an object")
	}
	ssh, err := tx.Prepare(`INSERT INTO ssh_args VALUES(?,?,?)`)
	if err != nil {
		return v, err
	}
	defer ssh.Close()
	// Native unique keys reject duplicates rather than last-writer-wins. Use
	// plain inserts during migration; upserts are for runtime operations only.
	profileInsert, err := tx.Prepare(configProfileInsert)
	if err != nil {
		return v, err
	}
	defer profileInsert.Close()
	instanceInsert, err := tx.Prepare(configInstanceInsert)
	if err != nil {
		return v, err
	}
	defer instanceInsert.Close()
	seen := map[string]bool{}
	for d.More() {
		key, err := d.Token()
		if err != nil {
			return v, err
		}
		name, ok := key.(string)
		if !ok || seen[name] {
			return v, fmt.Errorf("duplicate or invalid config field %v", key)
		}
		seen[name] = true
		switch name {
		case "version":
			err = d.Decode(&v.Version)
		case "minReader":
			err = d.Decode(&v.MinReader)
		case "runtimeGeneration":
			err = d.Decode(&v.RuntimeGeneration)
		case "runtimeMigrationId":
			err = d.Decode(&v.RuntimeMigrationID)
		case "runtimeMigratedAt":
			err = d.Decode(&v.RuntimeMigratedAt)
		case "runtimeCleanupPending":
			err = d.Decode(&v.RuntimeCleanupPending)
		case "proxyEnabled":
			err = d.Decode(&v.ProxyEnabled)
		case "agentAutoApproveEnabled":
			err = d.Decode(&v.AgentAutoApproveEnabled)
		case "defaults":
			err = d.Decode(&v.Defaults)
		case "teamsCodexPath":
			err = d.Decode(&v.TeamsCodexPath)
		case "profiles", "instances":
			token, err = d.Token()
			if err != nil {
				return v, err
			}
			if token == nil {
				continue
			}
			if token != json.Delim('[') {
				return v, fmt.Errorf("invalid config %s", name)
			}
			for i := 0; d.More(); i++ {
				if name == "profiles" {
					var p Profile
					if err = d.Decode(&p); err != nil {
						return v, err
					}
					if p.ID == "" {
						return v, fmt.Errorf("empty profile identity")
					}
					if _, err = profileInsert.Exec(append([]any{i}, profileArgs(p)...)...); err != nil {
						return v, err
					}
					for j, arg := range p.SSHArgs {
						if _, err = ssh.Exec(p.ID, j, arg); err != nil {
							return v, err
						}
					}
				} else {
					var inst Instance
					if err = d.Decode(&inst); err != nil {
						return v, err
					}
					if inst.ID == "" {
						return v, fmt.Errorf("empty instance identity")
					}
					if _, err = instanceInsert.Exec(append([]any{i}, instanceArgs(inst)...)...); err != nil {
						return v, err
					}
				}
			}
			token, err = d.Token()
			if err == nil && token != json.Delim(']') {
				err = fmt.Errorf("invalid config collection")
			}
		case "defaultModelProfile", "modelProfiles", "modelConfigVersion", "modelCredentials", "modelProviders", "models", "modelSources", "modelCatalogs", "modelProviderBindings", "yoloEnabled":
			err = skipRetiredConfigValue(d)
		default:
			return v, fmt.Errorf("unknown legacy config field %q", name)
		}
		if err != nil {
			return v, err
		}
	}
	if _, err = d.Token(); err != nil {
		return v, err
	}
	var extra any
	if err = d.Decode(&extra); !errors.Is(err, io.EOF) {
		return v, fmt.Errorf("trailing legacy config: %v", err)
	}
	if v.Version < 0 || v.MinReader < 0 {
		return v, fmt.Errorf("corrupt config: negative version")
	}
	if v.MinReader > 1 || v.Version > 6 {
		return v, &StaleReaderError{FileMinReader: v.MinReader, Supported: 1, WriterVersion: v.Version}
	}
	return v, nil
}
func skipRetiredConfigValue(d *json.Decoder) error {
	token, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := token.(json.Delim)
	if !ok {
		return nil
	}
	if delim != '{' && delim != '[' {
		return fmt.Errorf("invalid retired config value")
	}
	depth := 1
	for depth > 0 {
		token, err = d.Token()
		if err != nil {
			return err
		}
		if delim, ok = token.(json.Delim); ok {
			switch delim {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
		}
	}
	return nil
}

const configProfileInsert = `INSERT INTO profiles(position,id,name,host,port,user,route_target_host,route_target_port,created_at) VALUES(?,?,?,?,?,?,?,?,?)`

const configInstanceInsert = `INSERT INTO instances(position,id,profile_id,kind,broker_id,broker_epoch,owner_token,owner_acquired_at,owner_last_seen_at,owner_lease_expires_at,recovery_budget_restart_window_started_at,recovery_budget_restart_attempts,recovery_budget_request_window_started_at,recovery_budget_request_attempts,recovery_budget_blocked,recovery_budget_blocked_at,recovery_budget_last_reason,http_port,socks_port,daemon_pid,started_at,last_seen_at,model_profile_name,model_unified,model_provider,model_public_model,model_base_url,model_api_key_ref,model_ssh_proxy,model_upstream_proxy_id,model_revision,model_proxy_key,model_profile_captured) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
