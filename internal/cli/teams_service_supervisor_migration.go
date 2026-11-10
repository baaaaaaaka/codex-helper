package cli

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// All legacy filename and JSON handling for the supervisor stays here. Process
// locks and log paths retain their own lifetime, outside this database.
func migrateTeamsSupervisorConfig(db *sql.DB, path string) error {
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
		return fmt.Errorf("unsupported supervisor config database version %d", v)
	}
	for _, q := range []string{supervisorConfigDDL, supervisorConfigEnvDDL, `CREATE TABLE supervisor_config_imports(source_path TEXT PRIMARY KEY)`} {
		if _, err = tx.Exec(q); err != nil {
			return err
		}
	}
	legacy := strings.TrimSuffix(path, ".sqlite") + ".json"
	// RegistryPath was retired before SQLite cutover. Historical configs can
	// still contain it; accept it only here, without reviving runtime behavior.
	var old *struct {
		Version int  `json:"version"`
		Enabled bool `json:"enabled"`
		Spec    struct {
			teamsServiceSpec
			RegistryPath string
		} `json:"spec"`
		UpdatedAt time.Time `json:"updated_at"`
	}
	err = decodeLegacyService(legacy, &old)
	imported := err == nil
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if imported {
		if old == nil {
			return fmt.Errorf("null local supervisor config")
		}
		cfg := teamsServiceLocalSupervisorConfig{Version: old.Version, Enabled: old.Enabled, Spec: old.Spec.teamsServiceSpec, UpdatedAt: old.UpdatedAt}
		if err = putSupervisorConfig(tx, cfg, nil); err != nil {
			return err
		}
		if _, err = tx.Exec(`INSERT INTO supervisor_config_imports VALUES(?)`, legacy); err != nil {
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
		return archiveLegacyServiceFile(legacy)
	}
	return nil
}
func archiveLegacyServiceFile(path string) error {
	if _, err := os.Lstat(path + ".migrated"); err == nil {
		return fmt.Errorf("service migration committed; archive exists: %s", path)
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.Rename(path, path+".migrated")
}
func legacyLocalSupervisorRuntimeSources() ([]string, []string, error) {
	// The resolver now always uses the current runtime directory. Old directory
	// records are imported once, rather than refreshed from JSON during polling.
	dir, err := teamsServiceLocalSupervisorRuntimeDir()
	if err != nil {
		return nil, nil, err
	}
	base, err := teamsServiceLocalSupervisorConfigDir()
	if err != nil {
		return nil, nil, err
	}
	dirs := []string{dir, filepath.Join(base, "run", "local-supervisor")}
	var status, activation []string
	for _, p := range dirs {
		status = append(status, filepath.Join(p, "local-supervisor-status.json"))
		activation = append(activation, filepath.Join(p, "local-supervisor-activation.json"))
	}
	return status, activation, nil
}
func importLegacySupervisorRuntime(tx *sql.Tx, host string) ([]string, error) {
	statuses, activations, err := legacyLocalSupervisorRuntimeSources()
	if err != nil {
		return nil, err
	}
	var imported []string
	var chosen *teamsServiceLocalSupervisorStatus
	var chosenTime int64
	for _, path := range statuses {
		var status *teamsServiceLocalSupervisorStatus
		err = decodeLegacyService(path, &status)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if status == nil || status.Version < 0 || status.Version > 1 {
			return nil, fmt.Errorf("invalid legacy local supervisor status: %s", path)
		}
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if chosen == nil || info.ModTime().UnixNano() > chosenTime {
			chosen = status
			chosenTime = info.ModTime().UnixNano()
		}
		imported = append(imported, path)
	}
	if chosen != nil {
		if _, err = tx.Exec(SupervisorStatusPut, SupervisorStatusArgs(host, *chosen)...); err != nil {
			return nil, err
		}
		if err = replaceSupervisorIdentity(tx, host, "supervisor", chosen.SupervisorIdentity); err != nil {
			return nil, err
		}
		if err = replaceSupervisorIdentity(tx, host, "child", chosen.ChildIdentity); err != nil {
			return nil, err
		}
	}
	var decision *teamsServiceLocalSupervisorActivation
	chosenTime = 0
	for _, path := range activations {
		var value *teamsServiceLocalSupervisorActivation
		err = decodeLegacyService(path, &value)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if value == nil || value.Version < 0 || value.Version > 1 {
			return nil, fmt.Errorf("invalid legacy local supervisor activation: %s", path)
		}
		info, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		if decision == nil || info.ModTime().UnixNano() > chosenTime {
			decision = value
			chosenTime = info.ModTime().UnixNano()
		}
		imported = append(imported, path)
	}
	if decision != nil {
		if _, err = tx.Exec(ActivationPut, ActivationArgs(host, *decision)...); err != nil {
			return nil, err
		}
	}
	return imported, nil
}
