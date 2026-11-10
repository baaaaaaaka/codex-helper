package config

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
	"slices"
	"time"
)

func configTime(v time.Time) string {
	if v.IsZero() {
		return ""
	}
	return v.Format(time.RFC3339Nano)
}
func parseConfigTime(v string) (time.Time, error) {
	if v == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, v)
}
func openConfigDatabase(path string, readonly bool) (*sql.DB, error) {
	return sqlitestate.Open(path, readonly)
}

var configSchema = []string{
	`CREATE TABLE config_header(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,min_reader INTEGER NOT NULL,runtime_generation INTEGER NOT NULL,runtime_migration_id TEXT NOT NULL,runtime_migrated_at TEXT NOT NULL,runtime_cleanup_pending INTEGER NOT NULL,proxy_enabled INTEGER,agent_auto_approve_enabled INTEGER,defaults_model TEXT NOT NULL,defaults_effort TEXT NOT NULL,teams_path_mode TEXT NOT NULL,teams_explicit_path TEXT NOT NULL,teams_shell_override TEXT NOT NULL)`,
	`CREATE TABLE profiles(position INTEGER NOT NULL,id TEXT NOT NULL PRIMARY KEY,name TEXT NOT NULL,host TEXT NOT NULL,port INTEGER NOT NULL,user TEXT NOT NULL,route_target_host TEXT NOT NULL,route_target_port INTEGER NOT NULL,created_at TEXT NOT NULL)`,
	`CREATE TABLE instances(position INTEGER NOT NULL,id TEXT NOT NULL PRIMARY KEY,profile_id TEXT NOT NULL,kind TEXT NOT NULL,broker_id TEXT NOT NULL,broker_epoch TEXT NOT NULL,owner_token TEXT NOT NULL,owner_acquired_at TEXT NOT NULL,owner_last_seen_at TEXT NOT NULL,owner_lease_expires_at TEXT NOT NULL,recovery_budget_restart_window_started_at TEXT NOT NULL,recovery_budget_restart_attempts INTEGER NOT NULL,recovery_budget_request_window_started_at TEXT NOT NULL,recovery_budget_request_attempts INTEGER NOT NULL,recovery_budget_blocked INTEGER NOT NULL,recovery_budget_blocked_at TEXT NOT NULL,recovery_budget_last_reason TEXT NOT NULL,http_port INTEGER NOT NULL,socks_port INTEGER NOT NULL,daemon_pid INTEGER NOT NULL,started_at TEXT NOT NULL,last_seen_at TEXT NOT NULL,model_profile_name TEXT NOT NULL,model_unified INTEGER NOT NULL,model_provider TEXT NOT NULL,model_public_model TEXT NOT NULL,model_base_url TEXT NOT NULL,model_api_key_ref TEXT NOT NULL,model_ssh_proxy TEXT NOT NULL,model_upstream_proxy_id TEXT NOT NULL,model_revision INTEGER NOT NULL,model_proxy_key TEXT NOT NULL,model_profile_captured TEXT NOT NULL)`,
	`CREATE INDEX profiles_position ON profiles(position,id)`,
	`CREATE INDEX instances_position ON instances(position,id)`,
	`CREATE INDEX instances_profile ON instances(profile_id,position,id)`,
	`CREATE TABLE ssh_args(profile_id TEXT NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,position INTEGER NOT NULL,value TEXT NOT NULL,PRIMARY KEY(profile_id,position))`,
	`CREATE TABLE config_imports(source_path TEXT PRIMARY KEY)`,
}

const headerColumns = `version,min_reader,runtime_generation,runtime_migration_id,runtime_migrated_at,runtime_cleanup_pending,proxy_enabled,agent_auto_approve_enabled,defaults_model,defaults_effort,teams_path_mode,teams_explicit_path,teams_shell_override`
const headerPut = `INSERT INTO config_header(id,version,min_reader,runtime_generation,runtime_migration_id,runtime_migrated_at,runtime_cleanup_pending,proxy_enabled,agent_auto_approve_enabled,defaults_model,defaults_effort,teams_path_mode,teams_explicit_path,teams_shell_override) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET version=excluded.version,min_reader=excluded.min_reader,runtime_generation=excluded.runtime_generation,runtime_migration_id=excluded.runtime_migration_id,runtime_migrated_at=excluded.runtime_migrated_at,runtime_cleanup_pending=excluded.runtime_cleanup_pending,proxy_enabled=excluded.proxy_enabled,agent_auto_approve_enabled=excluded.agent_auto_approve_enabled,defaults_model=excluded.defaults_model,defaults_effort=excluded.defaults_effort,teams_path_mode=excluded.teams_path_mode,teams_explicit_path=excluded.teams_explicit_path,teams_shell_override=excluded.teams_shell_override WHERE (id,version,min_reader,runtime_generation,runtime_migration_id,runtime_migrated_at,runtime_cleanup_pending,proxy_enabled,agent_auto_approve_enabled,defaults_model,defaults_effort,teams_path_mode,teams_explicit_path,teams_shell_override) IS NOT (excluded.id,excluded.version,excluded.min_reader,excluded.runtime_generation,excluded.runtime_migration_id,excluded.runtime_migrated_at,excluded.runtime_cleanup_pending,excluded.proxy_enabled,excluded.agent_auto_approve_enabled,excluded.defaults_model,excluded.defaults_effort,excluded.teams_path_mode,excluded.teams_explicit_path,excluded.teams_shell_override)`

func headerArgs(v Config) []any {
	return []any{1, v.Version, v.MinReader, v.RuntimeGeneration, v.RuntimeMigrationID, configTime(v.RuntimeMigratedAt), v.RuntimeCleanupPending, v.ProxyEnabled, v.AgentAutoApproveEnabled, v.Defaults.Model, v.Defaults.ReasoningEffort, v.TeamsCodexPath.Mode, v.TeamsCodexPath.ExplicitPath, v.TeamsCodexPath.ShellOverride}
}
func scanConfig(row interface{ Scan(...any) error }) (Config, error) {
	var v Config
	var runtime_migrated_at string
	var proxy_enabled sql.NullBool
	var agent_auto_approve_enabled sql.NullBool
	err := row.Scan(&v.Version, &v.MinReader, &v.RuntimeGeneration, &v.RuntimeMigrationID, &runtime_migrated_at, &v.RuntimeCleanupPending, &proxy_enabled, &agent_auto_approve_enabled, &v.Defaults.Model, &v.Defaults.ReasoningEffort, &v.TeamsCodexPath.Mode, &v.TeamsCodexPath.ExplicitPath, &v.TeamsCodexPath.ShellOverride)
	if err != nil {
		return v, err
	}
	v.RuntimeMigratedAt, err = parseConfigTime(runtime_migrated_at)
	if err != nil {
		return v, err
	}
	if proxy_enabled.Valid {
		v.ProxyEnabled = &proxy_enabled.Bool
	}
	if agent_auto_approve_enabled.Valid {
		v.AgentAutoApproveEnabled = &agent_auto_approve_enabled.Bool
	}
	return v, nil
}

const profileColumns = `id,name,host,port,user,route_target_host,route_target_port,created_at`
const profilePut = `INSERT INTO profiles(position,id,name,host,port,user,route_target_host,route_target_port,created_at) VALUES(?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name,host=excluded.host,port=excluded.port,user=excluded.user,route_target_host=excluded.route_target_host,route_target_port=excluded.route_target_port,created_at=excluded.created_at,position=excluded.position WHERE (position,id,name,host,port,user,route_target_host,route_target_port,created_at) IS NOT (excluded.position,excluded.id,excluded.name,excluded.host,excluded.port,excluded.user,excluded.route_target_host,excluded.route_target_port,excluded.created_at)`

func profileArgs(v Profile) []any {
	return []any{v.ID, v.Name, v.Host, v.Port, v.User, v.RouteTargetHost, v.RouteTargetPort, configTime(v.CreatedAt)}
}
func scanProfile(row interface{ Scan(...any) error }) (Profile, error) {
	var v Profile
	var created_at string
	err := row.Scan(&v.ID, &v.Name, &v.Host, &v.Port, &v.User, &v.RouteTargetHost, &v.RouteTargetPort, &created_at)
	if err != nil {
		return v, err
	}
	v.CreatedAt, err = parseConfigTime(created_at)
	if err != nil {
		return v, err
	}
	return v, nil
}

const instanceColumns = `id,profile_id,kind,broker_id,broker_epoch,owner_token,owner_acquired_at,owner_last_seen_at,owner_lease_expires_at,recovery_budget_restart_window_started_at,recovery_budget_restart_attempts,recovery_budget_request_window_started_at,recovery_budget_request_attempts,recovery_budget_blocked,recovery_budget_blocked_at,recovery_budget_last_reason,http_port,socks_port,daemon_pid,started_at,last_seen_at,model_profile_name,model_unified,model_provider,model_public_model,model_base_url,model_api_key_ref,model_ssh_proxy,model_upstream_proxy_id,model_revision,model_proxy_key,model_profile_captured`
const instancePut = `INSERT INTO instances(position,id,profile_id,kind,broker_id,broker_epoch,owner_token,owner_acquired_at,owner_last_seen_at,owner_lease_expires_at,recovery_budget_restart_window_started_at,recovery_budget_restart_attempts,recovery_budget_request_window_started_at,recovery_budget_request_attempts,recovery_budget_blocked,recovery_budget_blocked_at,recovery_budget_last_reason,http_port,socks_port,daemon_pid,started_at,last_seen_at,model_profile_name,model_unified,model_provider,model_public_model,model_base_url,model_api_key_ref,model_ssh_proxy,model_upstream_proxy_id,model_revision,model_proxy_key,model_profile_captured) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET profile_id=excluded.profile_id,kind=excluded.kind,broker_id=excluded.broker_id,broker_epoch=excluded.broker_epoch,owner_token=excluded.owner_token,owner_acquired_at=excluded.owner_acquired_at,owner_last_seen_at=excluded.owner_last_seen_at,owner_lease_expires_at=excluded.owner_lease_expires_at,recovery_budget_restart_window_started_at=excluded.recovery_budget_restart_window_started_at,recovery_budget_restart_attempts=excluded.recovery_budget_restart_attempts,recovery_budget_request_window_started_at=excluded.recovery_budget_request_window_started_at,recovery_budget_request_attempts=excluded.recovery_budget_request_attempts,recovery_budget_blocked=excluded.recovery_budget_blocked,recovery_budget_blocked_at=excluded.recovery_budget_blocked_at,recovery_budget_last_reason=excluded.recovery_budget_last_reason,http_port=excluded.http_port,socks_port=excluded.socks_port,daemon_pid=excluded.daemon_pid,started_at=excluded.started_at,last_seen_at=excluded.last_seen_at,model_profile_name=excluded.model_profile_name,model_unified=excluded.model_unified,model_provider=excluded.model_provider,model_public_model=excluded.model_public_model,model_base_url=excluded.model_base_url,model_api_key_ref=excluded.model_api_key_ref,model_ssh_proxy=excluded.model_ssh_proxy,model_upstream_proxy_id=excluded.model_upstream_proxy_id,model_revision=excluded.model_revision,model_proxy_key=excluded.model_proxy_key,model_profile_captured=excluded.model_profile_captured,position=excluded.position WHERE (position,id,profile_id,kind,broker_id,broker_epoch,owner_token,owner_acquired_at,owner_last_seen_at,owner_lease_expires_at,recovery_budget_restart_window_started_at,recovery_budget_restart_attempts,recovery_budget_request_window_started_at,recovery_budget_request_attempts,recovery_budget_blocked,recovery_budget_blocked_at,recovery_budget_last_reason,http_port,socks_port,daemon_pid,started_at,last_seen_at,model_profile_name,model_unified,model_provider,model_public_model,model_base_url,model_api_key_ref,model_ssh_proxy,model_upstream_proxy_id,model_revision,model_proxy_key,model_profile_captured) IS NOT (excluded.position,excluded.id,excluded.profile_id,excluded.kind,excluded.broker_id,excluded.broker_epoch,excluded.owner_token,excluded.owner_acquired_at,excluded.owner_last_seen_at,excluded.owner_lease_expires_at,excluded.recovery_budget_restart_window_started_at,excluded.recovery_budget_restart_attempts,excluded.recovery_budget_request_window_started_at,excluded.recovery_budget_request_attempts,excluded.recovery_budget_blocked,excluded.recovery_budget_blocked_at,excluded.recovery_budget_last_reason,excluded.http_port,excluded.socks_port,excluded.daemon_pid,excluded.started_at,excluded.last_seen_at,excluded.model_profile_name,excluded.model_unified,excluded.model_provider,excluded.model_public_model,excluded.model_base_url,excluded.model_api_key_ref,excluded.model_ssh_proxy,excluded.model_upstream_proxy_id,excluded.model_revision,excluded.model_proxy_key,excluded.model_profile_captured)`

// Existing rows keep their order. Exclude indexed profile_id unless it changes;
// assigning it unchanged still dirties listing indexes in SQLite.
const instanceUpdate = `UPDATE instances SET kind=?,broker_id=?,broker_epoch=?,owner_token=?,owner_acquired_at=?,owner_last_seen_at=?,owner_lease_expires_at=?,recovery_budget_restart_window_started_at=?,recovery_budget_restart_attempts=?,recovery_budget_request_window_started_at=?,recovery_budget_request_attempts=?,recovery_budget_blocked=?,recovery_budget_blocked_at=?,recovery_budget_last_reason=?,http_port=?,socks_port=?,daemon_pid=?,started_at=?,last_seen_at=?,model_profile_name=?,model_unified=?,model_provider=?,model_public_model=?,model_base_url=?,model_api_key_ref=?,model_ssh_proxy=?,model_upstream_proxy_id=?,model_revision=?,model_proxy_key=?,model_profile_captured=? WHERE id=?`

func instanceArgs(v Instance) []any {
	return []any{v.ID, v.ProfileID, v.Kind, v.BrokerID, v.BrokerEpoch, v.OwnerToken, configTime(v.OwnerAcquiredAt), configTime(v.OwnerLastSeenAt), configTime(v.OwnerLeaseExpiresAt), configTime(v.RecoveryBudget.RestartWindowStartedAt), v.RecoveryBudget.RestartAttempts, configTime(v.RecoveryBudget.RequestWindowStartedAt), v.RecoveryBudget.RequestAttempts, v.RecoveryBudget.Blocked, configTime(v.RecoveryBudget.BlockedAt), v.RecoveryBudget.LastReason, v.HTTPPort, v.SocksPort, v.DaemonPID, configTime(v.StartedAt), configTime(v.LastSeenAt), v.ModelProfileName, v.ModelUnified, v.ModelProvider, v.ModelPublicModel, v.ModelBaseURL, v.ModelAPIKeyRef, v.ModelSSHProxy, v.ModelUpstreamProxyID, v.ModelRevision, v.ModelProxyKey, configTime(v.ModelProfileCaptured)}
}
func scanInstance(row interface{ Scan(...any) error }) (Instance, error) {
	var v Instance
	var owner_acquired_at string
	var owner_last_seen_at string
	var owner_lease_expires_at string
	var recovery_budget_restart_window_started_at string
	var recovery_budget_request_window_started_at string
	var recovery_budget_blocked_at string
	var started_at string
	var last_seen_at string
	var model_profile_captured string
	err := row.Scan(&v.ID, &v.ProfileID, &v.Kind, &v.BrokerID, &v.BrokerEpoch, &v.OwnerToken, &owner_acquired_at, &owner_last_seen_at, &owner_lease_expires_at, &recovery_budget_restart_window_started_at, &v.RecoveryBudget.RestartAttempts, &recovery_budget_request_window_started_at, &v.RecoveryBudget.RequestAttempts, &v.RecoveryBudget.Blocked, &recovery_budget_blocked_at, &v.RecoveryBudget.LastReason, &v.HTTPPort, &v.SocksPort, &v.DaemonPID, &started_at, &last_seen_at, &v.ModelProfileName, &v.ModelUnified, &v.ModelProvider, &v.ModelPublicModel, &v.ModelBaseURL, &v.ModelAPIKeyRef, &v.ModelSSHProxy, &v.ModelUpstreamProxyID, &v.ModelRevision, &v.ModelProxyKey, &model_profile_captured)
	if err != nil {
		return v, err
	}
	v.OwnerAcquiredAt, err = parseConfigTime(owner_acquired_at)
	if err != nil {
		return v, err
	}
	v.OwnerLastSeenAt, err = parseConfigTime(owner_last_seen_at)
	if err != nil {
		return v, err
	}
	v.OwnerLeaseExpiresAt, err = parseConfigTime(owner_lease_expires_at)
	if err != nil {
		return v, err
	}
	v.RecoveryBudget.RestartWindowStartedAt, err = parseConfigTime(recovery_budget_restart_window_started_at)
	if err != nil {
		return v, err
	}
	v.RecoveryBudget.RequestWindowStartedAt, err = parseConfigTime(recovery_budget_request_window_started_at)
	if err != nil {
		return v, err
	}
	v.RecoveryBudget.BlockedAt, err = parseConfigTime(recovery_budget_blocked_at)
	if err != nil {
		return v, err
	}
	v.StartedAt, err = parseConfigTime(started_at)
	if err != nil {
		return v, err
	}
	v.LastSeenAt, err = parseConfigTime(last_seen_at)
	if err != nil {
		return v, err
	}
	v.ModelProfileCaptured, err = parseConfigTime(model_profile_captured)
	if err != nil {
		return v, err
	}
	return v, nil
}
func readConfigHeader(tx *sql.Tx) (Config, error) {
	v, err := scanConfig(tx.QueryRow(`SELECT ` + headerColumns + ` FROM config_header WHERE id=1`))
	if err != nil {
		return v, err
	}
	if v.MinReader > SupportedReaderVersion {
		return v, &StaleReaderError{FileMinReader: v.MinReader, Supported: SupportedReaderVersion, WriterVersion: v.Version}
	}
	return v, nil
}
func readProfileArgs(tx *sql.Tx, id string) ([]string, error) {
	var args []string
	rows, err := tx.Query(`SELECT value FROM ssh_args WHERE profile_id=? ORDER BY position`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var v string
		if err = rows.Scan(&v); err != nil {
			return nil, err
		}
		args = append(args, v)
	}
	return args, rows.Err()
}
func readConfigTx(tx *sql.Tx) (Config, error) {
	cfg, err := readConfigHeader(tx)
	if err != nil {
		return cfg, err
	}
	rows, err := tx.Query(`SELECT ` + profileColumns + ` FROM profiles ORDER BY position,id`)
	if err != nil {
		return cfg, err
	}
	for rows.Next() {
		v, err := scanProfile(rows)
		if err != nil {
			rows.Close()
			return cfg, err
		}
		cfg.Profiles = append(cfg.Profiles, v)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return cfg, err
	}
	for i := range cfg.Profiles {
		cfg.Profiles[i].SSHArgs, err = readProfileArgs(tx, cfg.Profiles[i].ID)
		if err != nil {
			return cfg, err
		}
	}
	rows, err = tx.Query(`SELECT ` + instanceColumns + ` FROM instances ORDER BY position,id`)
	if err != nil {
		return cfg, err
	}
	defer rows.Close()
	for rows.Next() {
		v, err := scanInstance(rows)
		if err != nil {
			return cfg, err
		}
		cfg.Instances = append(cfg.Instances, v)
	}
	return cfg, rows.Err()
}
func putProfile(tx *sql.Tx, position int, v Profile) error {
	old, err := readProfileArgs(tx, v.ID)
	if err != nil {
		return err
	}
	args := append([]any{position}, profileArgs(v)...)
	if _, err = tx.Exec(profilePut, args...); err != nil {
		return err
	}
	if slices.Equal(old, v.SSHArgs) {
		return nil
	}
	if _, err = tx.Exec(`DELETE FROM ssh_args WHERE profile_id=?`, v.ID); err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO ssh_args VALUES(?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for i, arg := range v.SSHArgs {
		if _, err = stmt.Exec(v.ID, i, arg); err != nil {
			return err
		}
	}
	return nil
}

// ChangeSettings reads and updates only the singleton settings record. Collection
// edits have their own methods; a callback cannot accidentally replace them.
func (s *Store) ChangeSettings(fn func(*Config) error) error {
	return s.write(func(tx *sql.Tx) error {
		cfg, err := readConfigHeader(tx)
		if err != nil {
			return err
		}
		if err = fn(&cfg); err != nil {
			return err
		}
		if cfg.Profiles != nil || cfg.Instances != nil {
			return fmt.Errorf("ChangeSettings cannot edit profiles or instances")
		}
		cfg.Version, cfg.MinReader = CurrentVersion, MinReaderVersion
		_, err = tx.Exec(headerPut, headerArgs(cfg)...)
		return err
	})
}

func (s *Store) PutProfile(profile Profile) error {
	if profile.ID == "" {
		return fmt.Errorf("empty profile id")
	}
	return s.write(func(tx *sql.Tx) error {
		var position int
		err := tx.QueryRow(`SELECT position FROM profiles WHERE id=?`, profile.ID).Scan(&position)
		if err == sql.ErrNoRows {
			err = tx.QueryRow(`SELECT COALESCE(MAX(position)+1,0) FROM profiles`).Scan(&position)
		}
		if err != nil {
			return err
		}
		return putProfile(tx, position, profile)
	})
}
func saveConfigTx(tx *sql.Tx, v Config) error {
	old, err := readConfigHeader(tx)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	if old.Version > CurrentVersion {
		return fmt.Errorf("refuse to overwrite newer config version %d", old.Version)
	}
	v.Version = CurrentVersion
	v.MinReader = MinReaderVersion
	if _, err = tx.Exec(headerPut, headerArgs(v)...); err != nil {
		return err
	}
	profiles := make(map[string]bool, len(v.Profiles))
	instances := make(map[string]bool, len(v.Instances))
	for i, p := range v.Profiles {
		if p.ID == "" || profiles[p.ID] {
			return fmt.Errorf("empty or duplicate profile id %q", p.ID)
		}
		profiles[p.ID] = true
		if err = putProfile(tx, i, p); err != nil {
			return err
		}
	}
	for i, inst := range v.Instances {
		if inst.ID == "" || instances[inst.ID] {
			return fmt.Errorf("empty or duplicate instance id %q", inst.ID)
		}
		instances[inst.ID] = true
		if _, err = tx.Exec(instancePut, append([]any{i}, instanceArgs(inst)...)...); err != nil {
			return err
		}
	}
	// Save/Update are explicit configuration edits. Hot instance updates below
	// never call these whole-configuration list operations.
	for _, table := range []struct {
		name string
		keep map[string]bool
	}{{"profiles", profiles}, {"instances", instances}} {
		rows, err := tx.Query(`SELECT id FROM ` + table.name)
		if err != nil {
			return err
		}
		var removed []string
		for rows.Next() {
			var id string
			if err = rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			if !table.keep[id] {
				removed = append(removed, id)
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for _, id := range removed {
			if _, err = tx.Exec(`DELETE FROM `+table.name+` WHERE id=?`, id); err != nil {
				return err
			}
		}
	}
	return nil
}

var ErrInstanceNotFound = errors.New("proxy instance not found")

func (s *Store) ChangeInstance(id string, fn func(*Instance) error) error {
	return s.write(func(tx *sql.Tx) error {
		v, err := scanInstance(tx.QueryRow(`SELECT `+instanceColumns+` FROM instances WHERE id=?`, id))
		if err == sql.ErrNoRows {
			return fmt.Errorf("%w: %q", ErrInstanceNotFound, id)
		} else if err != nil {
			return err
		}
		before := instanceArgs(v)
		if err = fn(&v); err != nil {
			return err
		}
		if v.ID != id {
			return fmt.Errorf("cannot change instance identity")
		}
		after := instanceArgs(v)
		if before[1] != after[1] {
			if _, err = tx.Exec(`UPDATE instances SET profile_id=? WHERE id=?`, v.ProfileID, id); err != nil {
				return err
			}
		}
		if !slices.Equal(before[2:], after[2:]) {
			_, err = tx.Exec(instanceUpdate, append(after[2:], id)...)
		}
		return err
	})
}
func (s *Store) PutInstance(v Instance) error {
	return s.write(func(tx *sql.Tx) error {
		var position int
		err := tx.QueryRow(`SELECT position FROM instances WHERE id=?`, v.ID).Scan(&position)
		if err == sql.ErrNoRows {
			err = tx.QueryRow(`SELECT COALESCE(MAX(position),-1)+1 FROM instances`).Scan(&position)
		}
		if err != nil {
			return err
		}
		if v.ID == "" {
			return fmt.Errorf("empty instance id")
		}
		_, err = tx.Exec(instancePut, append([]any{position}, instanceArgs(v)...)...)
		return err
	})
}
func (s *Store) DeleteInstance(id string) error {
	return s.write(func(tx *sql.Tx) error { _, err := tx.Exec(`DELETE FROM instances WHERE id=?`, id); return err })
}

// DeleteUnchangedInstances commits one prune decision after external process
// inspection. A concurrent heartbeat, owner change or recovery block preserves
// that row. Callers perform no process inspection while the writer is held.
func (s *Store) DeleteUnchangedInstances(expected []Instance) (int, error) {
	if len(expected) == 0 {
		return 0, nil
	}
	removed := 0
	err := s.write(func(tx *sql.Tx) error {
		for _, old := range expected {
			current, err := scanInstance(tx.QueryRow(`SELECT `+instanceColumns+` FROM instances WHERE id=?`, old.ID))
			if err == sql.ErrNoRows {
				continue
			}
			if err != nil {
				return err
			}
			if !slices.Equal(instanceArgs(old), instanceArgs(current)) {
				continue
			}
			if _, err = tx.Exec(`DELETE FROM instances WHERE id=?`, old.ID); err != nil {
				return err
			}
			removed++
		}
		return nil
	})
	if err != nil {
		return 0, err
	}
	return removed, nil
}
func (s *Store) DeleteInstanceWhen(id string, check func(Instance) error) error {
	return s.write(func(tx *sql.Tx) error {
		v, err := scanInstance(tx.QueryRow(`SELECT `+instanceColumns+` FROM instances WHERE id=?`, id))
		if err == sql.ErrNoRows {
			return fmt.Errorf("%w: %q", ErrInstanceNotFound, id)
		}
		if err != nil {
			return err
		}
		if err = check(v); err != nil {
			return err
		}
		_, err = tx.Exec(`DELETE FROM instances WHERE id=?`, id)
		return err
	})
}
func (s *Store) Instance(id string) (Instance, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureDB(); err != nil {
		return Instance{}, false, err
	}
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Instance{}, false, err
	}
	defer tx.Rollback()
	if _, err = readConfigHeader(tx); err != nil {
		return Instance{}, false, err
	}
	v, err := scanInstance(tx.QueryRow(`SELECT `+instanceColumns+` FROM instances WHERE id=?`, id))
	if err == sql.ErrNoRows {
		return v, false, nil
	}
	if err != nil {
		return v, false, err
	}
	return v, true, tx.Commit()
}
func (s *Store) ReadDefaults() (GlobalDefaults, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.ensureDB(); err != nil {
		return GlobalDefaults{}, err
	}
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return GlobalDefaults{}, err
	}
	defer tx.Rollback()
	v, err := readConfigHeader(tx)
	if err != nil {
		return GlobalDefaults{}, err
	}
	return v.Defaults, tx.Commit()
}
func (s *Store) WriteDefaults(v GlobalDefaults) error {
	return s.write(func(tx *sql.Tx) error {
		_, err := tx.Exec(`UPDATE config_header SET defaults_model=?,defaults_effort=? WHERE id=1 AND (defaults_model,defaults_effort) IS NOT (?,?)`, v.Model, v.ReasoningEffort, v.Model, v.ReasoningEffort)
		return err
	})
}
