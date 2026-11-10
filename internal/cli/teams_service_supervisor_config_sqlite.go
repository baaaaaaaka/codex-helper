package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"

	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

type teamsSupervisorConfigDB struct{ db *sql.DB }

const supervisorConfigDDL = `CREATE TABLE supervisor_config(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,enabled INTEGER NOT NULL,executable TEXT NOT NULL,working_dir TEXT NOT NULL,windows_task_launcher_path TEXT NOT NULL,windows_watchdog_launcher_path TEXT NOT NULL,updated_at TEXT NOT NULL,has_environment INTEGER NOT NULL)`
const supervisorConfigEnvDDL = `CREATE TABLE supervisor_config_environment(name TEXT PRIMARY KEY,value TEXT NOT NULL)`
const supervisorConfigPut = `INSERT INTO supervisor_config VALUES(1,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET version=excluded.version,enabled=excluded.enabled,executable=excluded.executable,working_dir=excluded.working_dir,windows_task_launcher_path=excluded.windows_task_launcher_path,windows_watchdog_launcher_path=excluded.windows_watchdog_launcher_path,updated_at=excluded.updated_at,has_environment=excluded.has_environment WHERE (version,enabled,executable,working_dir,windows_task_launcher_path,windows_watchdog_launcher_path,updated_at,has_environment) IS NOT (excluded.version,excluded.enabled,excluded.executable,excluded.working_dir,excluded.windows_task_launcher_path,excluded.windows_watchdog_launcher_path,excluded.updated_at,excluded.has_environment)`

func openTeamsSupervisorConfig(path string) (*teamsSupervisorConfigDB, error) {
	path = serviceNativePath(path)
	if err := ensureTeamsServiceLocalSupervisorDir(filepath.Dir(path)); err != nil {
		return nil, err
	}
	db, err := sqlitestate.Open(path, false)
	if err != nil {
		return nil, err
	}
	if err = migrateTeamsSupervisorConfig(db, path); err != nil {
		db.Close()
		return nil, err
	}
	return &teamsSupervisorConfigDB{db: db}, nil
}
func readSupervisorConfig(tx *sql.Tx) (teamsServiceLocalSupervisorConfig, error) {
	var v teamsServiceLocalSupervisorConfig
	var updated string
	var env bool
	err := tx.QueryRow(`SELECT version,enabled,executable,working_dir,windows_task_launcher_path,windows_watchdog_launcher_path,updated_at,has_environment FROM supervisor_config WHERE id=1`).Scan(&v.Version, &v.Enabled, &v.Spec.Executable, &v.Spec.WorkingDir, &v.Spec.WindowsTaskLauncherPath, &v.Spec.WindowsWatchdogLauncherPath, &updated, &env)
	if errors.Is(err, sql.ErrNoRows) {
		return v, os.ErrNotExist
	}
	if err != nil {
		return v, err
	}
	if v.Version != 1 {
		return v, fmt.Errorf("unsupported local supervisor config version %d", v.Version)
	}
	v.UpdatedAt, err = parseServiceTime(updated)
	if err != nil {
		return v, err
	}
	if env {
		v.Spec.Environment = map[string]string{}
	}
	rows, err := tx.Query(`SELECT name,value FROM supervisor_config_environment`)
	if err != nil {
		return v, err
	}
	defer rows.Close()
	for rows.Next() {
		var k, x string
		if err = rows.Scan(&k, &x); err != nil {
			return v, err
		}
		if v.Spec.Environment == nil {
			v.Spec.Environment = map[string]string{}
		}
		v.Spec.Environment[k] = x
	}
	if err = rows.Err(); err != nil {
		return v, err
	}
	if v.Spec.Executable == "" {
		return v, fmt.Errorf("local supervisor config missing executable")
	}
	return v, nil
}
func (s *teamsSupervisorConfigDB) Config() (teamsServiceLocalSupervisorConfig, error) {
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return teamsServiceLocalSupervisorConfig{}, err
	}
	defer tx.Rollback()
	v, err := readSupervisorConfig(tx)
	if err != nil {
		return v, err
	}
	return v, tx.Commit()
}
func putSupervisorConfig(tx *sql.Tx, v teamsServiceLocalSupervisorConfig, old *teamsServiceLocalSupervisorConfig) error {
	if v.Version == 0 {
		v.Version = 1
	}
	if v.Version != 1 {
		return fmt.Errorf("unsupported local supervisor config version %d", v.Version)
	}
	if v.Spec.Executable == "" {
		return fmt.Errorf("local supervisor config missing executable")
	}
	if _, err := tx.Exec(supervisorConfigPut, v.Version, v.Enabled, v.Spec.Executable, v.Spec.WorkingDir, v.Spec.WindowsTaskLauncherPath, v.Spec.WindowsWatchdogLauncherPath, serviceTime(v.UpdatedAt), v.Spec.Environment != nil); err != nil {
		return err
	}
	if old != nil && maps.Equal(old.Spec.Environment, v.Spec.Environment) {
		return nil
	}
	if _, err := tx.Exec(`DELETE FROM supervisor_config_environment`); err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO supervisor_config_environment VALUES(?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for k, x := range v.Spec.Environment {
		if _, err = stmt.Exec(k, x); err != nil {
			return err
		}
	}
	return nil
}
func (s *teamsSupervisorConfigDB) Save(v teamsServiceLocalSupervisorConfig) error {
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	old, err := readSupervisorConfig(tx)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	var previous *teamsServiceLocalSupervisorConfig
	if err == nil {
		previous = &old
	}
	if err = putSupervisorConfig(tx, v, previous); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *teamsSupervisorConfigDB) Delete() error {
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = readSupervisorConfig(tx); err != nil && !os.IsNotExist(err) {
		return err
	}
	if _, err = tx.Exec(`DELETE FROM supervisor_config; DELETE FROM supervisor_config_environment`); err != nil {
		return err
	}
	return tx.Commit()
}
