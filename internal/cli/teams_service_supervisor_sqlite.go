package cli

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"maps"
	"slices"
)

const SupervisorStatusDDL = `CREATE TABLE service_supervisors(host TEXT PRIMARY KEY,version INTEGER NOT NULL,config_path TEXT NOT NULL,log_path TEXT NOT NULL,supervisor_pid INTEGER NOT NULL,supervisor_pgid INTEGER NOT NULL,child_pid INTEGER NOT NULL,child_pgid INTEGER NOT NULL,state TEXT NOT NULL,enabled INTEGER NOT NULL,started_at TEXT NOT NULL,updated_at TEXT NOT NULL,last_child_start_at TEXT NOT NULL,last_child_exit_at TEXT NOT NULL,last_exit_code INTEGER NOT NULL,last_error TEXT NOT NULL,last_health_check_at TEXT NOT NULL,last_health_reason TEXT NOT NULL,last_health_action TEXT NOT NULL,restart_count INTEGER NOT NULL)`
const SupervisorStatusPut = `INSERT INTO service_supervisors(host,version,config_path,log_path,supervisor_pid,supervisor_pgid,child_pid,child_pgid,state,enabled,started_at,updated_at,last_child_start_at,last_child_exit_at,last_exit_code,last_error,last_health_check_at,last_health_reason,last_health_action,restart_count) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(host) DO UPDATE SET version=excluded.version,config_path=excluded.config_path,log_path=excluded.log_path,supervisor_pid=excluded.supervisor_pid,supervisor_pgid=excluded.supervisor_pgid,child_pid=excluded.child_pid,child_pgid=excluded.child_pgid,state=excluded.state,enabled=excluded.enabled,started_at=excluded.started_at,updated_at=excluded.updated_at,last_child_start_at=excluded.last_child_start_at,last_child_exit_at=excluded.last_child_exit_at,last_exit_code=excluded.last_exit_code,last_error=excluded.last_error,last_health_check_at=excluded.last_health_check_at,last_health_reason=excluded.last_health_reason,last_health_action=excluded.last_health_action,restart_count=excluded.restart_count WHERE (version,config_path,log_path,supervisor_pid,supervisor_pgid,child_pid,child_pgid,state,enabled,started_at,updated_at,last_child_start_at,last_child_exit_at,last_exit_code,last_error,last_health_check_at,last_health_reason,last_health_action,restart_count) IS NOT (excluded.version,excluded.config_path,excluded.log_path,excluded.supervisor_pid,excluded.supervisor_pgid,excluded.child_pid,excluded.child_pgid,excluded.state,excluded.enabled,excluded.started_at,excluded.updated_at,excluded.last_child_start_at,excluded.last_child_exit_at,excluded.last_exit_code,excluded.last_error,excluded.last_health_check_at,excluded.last_health_reason,excluded.last_health_action,excluded.restart_count)`

func SupervisorStatusArgs(host string, v teamsServiceLocalSupervisorStatus) []any {
	return []any{host, v.Version, v.ConfigPath, v.LogPath, v.SupervisorPID, v.SupervisorPGID, v.ChildPID, v.ChildPGID, v.State, v.Enabled, serviceTime(v.StartedAt), serviceTime(v.UpdatedAt), serviceTime(v.LastChildStartAt), serviceTime(v.LastChildExitAt), v.LastExitCode, v.LastError, serviceTime(v.LastHealthCheckAt), v.LastHealthReason, v.LastHealthAction, v.RestartCount}
}
func readSupervisorStatus(tx *sql.Tx, host string) (teamsServiceLocalSupervisorStatus, bool, error) {
	var v teamsServiceLocalSupervisorStatus
	var started_at string
	var updated_at string
	var last_child_start_at string
	var last_child_exit_at string
	var last_health_check_at string
	err := tx.QueryRow(`SELECT version,config_path,log_path,supervisor_pid,supervisor_pgid,child_pid,child_pgid,state,enabled,started_at,updated_at,last_child_start_at,last_child_exit_at,last_exit_code,last_error,last_health_check_at,last_health_reason,last_health_action,restart_count FROM service_supervisors WHERE host=?`, host).Scan(&v.Version, &v.ConfigPath, &v.LogPath, &v.SupervisorPID, &v.SupervisorPGID, &v.ChildPID, &v.ChildPGID, &v.State, &v.Enabled, &started_at, &updated_at, &last_child_start_at, &last_child_exit_at, &v.LastExitCode, &v.LastError, &last_health_check_at, &v.LastHealthReason, &v.LastHealthAction, &v.RestartCount)
	if errors.Is(err, sql.ErrNoRows) {
		return v, false, nil
	}
	if err != nil {
		return v, false, err
	}
	v.StartedAt, err = parseServiceTime(started_at)
	if err != nil {
		return v, false, err
	}
	v.UpdatedAt, err = parseServiceTime(updated_at)
	if err != nil {
		return v, false, err
	}
	v.LastChildStartAt, err = parseServiceTime(last_child_start_at)
	if err != nil {
		return v, false, err
	}
	v.LastChildExitAt, err = parseServiceTime(last_child_exit_at)
	if err != nil {
		return v, false, err
	}
	v.LastHealthCheckAt, err = parseServiceTime(last_health_check_at)
	if err != nil {
		return v, false, err
	}
	if v.Version > 1 || v.Version < 0 {
		return v, false, fmt.Errorf("unsupported local supervisor record version %d", v.Version)
	}

	v.SupervisorIdentity, err = readSupervisorIdentity(tx, host, "supervisor")
	if err != nil {
		return v, false, err
	}
	v.ChildIdentity, err = readSupervisorIdentity(tx, host, "child")
	if err != nil {
		return v, false, err
	}
	return v, true, nil
}

func (s *teamsServiceStateDB) SupervisorStatus() (teamsServiceLocalSupervisorStatus, bool, error) {
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return teamsServiceLocalSupervisorStatus{}, false, err
	}
	defer tx.Rollback()
	v, ok, err := readSupervisorStatus(tx, s.host)
	if err != nil {
		return v, false, err
	}
	return v, ok, tx.Commit()
}
func (s *teamsServiceStateDB) SaveSupervisorStatus(v teamsServiceLocalSupervisorStatus) error {
	if v.Version > 1 || v.Version < 0 {
		return fmt.Errorf("unsupported local supervisor record version %d", v.Version)
	}

	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	old, _, err := readSupervisorStatus(tx, s.host)
	if err != nil {
		return err
	}
	if _, err = tx.Exec(SupervisorStatusPut, SupervisorStatusArgs(s.host, v)...); err != nil {
		return err
	}
	if !sameSupervisorIdentity(old.SupervisorIdentity, v.SupervisorIdentity) {
		if err = replaceSupervisorIdentity(tx, s.host, "supervisor", v.SupervisorIdentity); err != nil {
			return err
		}
	}
	if !sameSupervisorIdentity(old.ChildIdentity, v.ChildIdentity) {
		if err = replaceSupervisorIdentity(tx, s.host, "child", v.ChildIdentity); err != nil {
			return err
		}
	}
	return tx.Commit()
}

const ActivationDDL = `CREATE TABLE service_activations(host TEXT PRIMARY KEY,version INTEGER NOT NULL,status TEXT NOT NULL,target_version TEXT NOT NULL,observed_supervisor_env TEXT NOT NULL,old_supervisor_pid INTEGER NOT NULL,old_child_pid INTEGER NOT NULL,message TEXT NOT NULL,scheduled_at TEXT NOT NULL,deadline_at TEXT NOT NULL,updated_at TEXT NOT NULL)`
const ActivationPut = `INSERT INTO service_activations(host,version,status,target_version,observed_supervisor_env,old_supervisor_pid,old_child_pid,message,scheduled_at,deadline_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(host) DO UPDATE SET version=excluded.version,status=excluded.status,target_version=excluded.target_version,observed_supervisor_env=excluded.observed_supervisor_env,old_supervisor_pid=excluded.old_supervisor_pid,old_child_pid=excluded.old_child_pid,message=excluded.message,scheduled_at=excluded.scheduled_at,deadline_at=excluded.deadline_at,updated_at=excluded.updated_at WHERE (version,status,target_version,observed_supervisor_env,old_supervisor_pid,old_child_pid,message,scheduled_at,deadline_at,updated_at) IS NOT (excluded.version,excluded.status,excluded.target_version,excluded.observed_supervisor_env,excluded.old_supervisor_pid,excluded.old_child_pid,excluded.message,excluded.scheduled_at,excluded.deadline_at,excluded.updated_at)`

func ActivationArgs(host string, v teamsServiceLocalSupervisorActivation) []any {
	return []any{host, v.Version, v.Status, v.TargetVersion, v.ObservedSupervisorEnv, v.OldSupervisorPID, v.OldChildPID, v.Message, serviceTime(v.ScheduledAt), serviceTime(v.DeadlineAt), serviceTime(v.UpdatedAt)}
}
func readActivation(tx *sql.Tx, host string) (teamsServiceLocalSupervisorActivation, bool, error) {
	var v teamsServiceLocalSupervisorActivation
	var scheduled_at string
	var deadline_at string
	var updated_at string
	err := tx.QueryRow(`SELECT version,status,target_version,observed_supervisor_env,old_supervisor_pid,old_child_pid,message,scheduled_at,deadline_at,updated_at FROM service_activations WHERE host=?`, host).Scan(&v.Version, &v.Status, &v.TargetVersion, &v.ObservedSupervisorEnv, &v.OldSupervisorPID, &v.OldChildPID, &v.Message, &scheduled_at, &deadline_at, &updated_at)
	if errors.Is(err, sql.ErrNoRows) {
		return v, false, nil
	}
	if err != nil {
		return v, false, err
	}
	v.ScheduledAt, err = parseServiceTime(scheduled_at)
	if err != nil {
		return v, false, err
	}
	v.DeadlineAt, err = parseServiceTime(deadline_at)
	if err != nil {
		return v, false, err
	}
	v.UpdatedAt, err = parseServiceTime(updated_at)
	if err != nil {
		return v, false, err
	}
	if v.Version > 1 || v.Version < 0 {
		return v, false, fmt.Errorf("unsupported local supervisor record version %d", v.Version)
	}
	return v, true, nil
}

func (s *teamsServiceStateDB) Activation() (teamsServiceLocalSupervisorActivation, bool, error) {
	tx, err := s.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return teamsServiceLocalSupervisorActivation{}, false, err
	}
	defer tx.Rollback()
	v, ok, err := readActivation(tx, s.host)
	if err != nil {
		return v, false, err
	}
	return v, ok, tx.Commit()
}
func (s *teamsServiceStateDB) SaveActivation(v teamsServiceLocalSupervisorActivation) error {
	if v.Version > 1 || v.Version < 0 {
		return fmt.Errorf("unsupported local supervisor record version %d", v.Version)
	}
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, _, err = readActivation(tx, s.host); err != nil {
		return err
	}
	if _, err = tx.Exec(ActivationPut, ActivationArgs(s.host, v)...); err != nil {
		return err
	}
	return tx.Commit()
}

var supervisorIdentitySchema = []string{
	`CREATE TABLE service_process_identities(host TEXT NOT NULL REFERENCES service_supervisors(host) ON DELETE CASCADE,role TEXT NOT NULL CHECK(role IN ('supervisor','child')),executable TEXT NOT NULL,proc_start_time TEXT NOT NULL,has_args INTEGER NOT NULL,has_environment INTEGER NOT NULL,PRIMARY KEY(host,role))`,
	`CREATE TABLE service_process_args(host TEXT NOT NULL,role TEXT NOT NULL,position INTEGER NOT NULL,value TEXT NOT NULL,PRIMARY KEY(host,role,position),FOREIGN KEY(host,role) REFERENCES service_process_identities(host,role) ON DELETE CASCADE)`,
	`CREATE TABLE service_process_environment(host TEXT NOT NULL,role TEXT NOT NULL,name TEXT NOT NULL,value TEXT NOT NULL,PRIMARY KEY(host,role,name),FOREIGN KEY(host,role) REFERENCES service_process_identities(host,role) ON DELETE CASCADE)`,
}

func sameSupervisorIdentity(a, b *teamsServiceLocalSupervisorProcessIdentity) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return a.Executable == b.Executable && a.ProcStartTime == b.ProcStartTime && (a.Args == nil) == (b.Args == nil) && (a.Environment == nil) == (b.Environment == nil) && slices.Equal(a.Args, b.Args) && maps.Equal(a.Environment, b.Environment)
}
func readSupervisorIdentity(tx *sql.Tx, host, role string) (*teamsServiceLocalSupervisorProcessIdentity, error) {
	var v teamsServiceLocalSupervisorProcessIdentity
	var args, env bool
	err := tx.QueryRow(`SELECT executable,proc_start_time,has_args,has_environment FROM service_process_identities WHERE host=? AND role=?`, host, role).Scan(&v.Executable, &v.ProcStartTime, &args, &env)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if args {
		v.Args = []string{}
	}
	if env {
		v.Environment = map[string]string{}
	}
	rows, err := tx.Query(`SELECT value FROM service_process_args WHERE host=? AND role=? ORDER BY position`, host, role)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var arg string
		if err = rows.Scan(&arg); err != nil {
			rows.Close()
			return nil, err
		}
		v.Args = append(v.Args, arg)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	rows, err = tx.Query(`SELECT name,value FROM service_process_environment WHERE host=? AND role=?`, host, role)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var k, x string
		if err = rows.Scan(&k, &x); err != nil {
			return nil, err
		}
		if v.Environment == nil {
			v.Environment = map[string]string{}
		}
		v.Environment[k] = x
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return &v, nil
}
func replaceSupervisorIdentity(tx *sql.Tx, host, role string, v *teamsServiceLocalSupervisorProcessIdentity) error {
	if _, err := tx.Exec(`DELETE FROM service_process_identities WHERE host=? AND role=?`, host, role); err != nil {
		return err
	}
	if v == nil {
		return nil
	}
	if _, err := tx.Exec(`INSERT INTO service_process_identities VALUES(?,?,?,?,?,?)`, host, role, v.Executable, v.ProcStartTime, v.Args != nil, v.Environment != nil); err != nil {
		return err
	}
	args, err := tx.Prepare(`INSERT INTO service_process_args VALUES(?,?,?,?)`)
	if err != nil {
		return err
	}
	defer args.Close()
	for i, a := range v.Args {
		if _, err = args.Exec(host, role, i, a); err != nil {
			return err
		}
	}
	env, err := tx.Prepare(`INSERT INTO service_process_environment VALUES(?,?,?,?)`)
	if err != nil {
		return err
	}
	defer env.Close()
	for k, x := range v.Environment {
		if _, err = env.Exec(host, role, k, x); err != nil {
			return err
		}
	}
	return nil
}
func (s *teamsServiceStateDB) DeleteSupervisorStatus() error {
	tx, err := s.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, _, err = readSupervisorStatus(tx, s.host); err != nil {
		return err
	}
	if _, err = tx.Exec(`DELETE FROM service_supervisors WHERE host=?`, s.host); err != nil {
		return err
	}
	return tx.Commit()
}
