package appgateway

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

func (r *Registry) Close() error { return r.db.Close() }
func gatewayTime(v time.Time) string {
	if v.IsZero() {
		return ""
	}
	return v.Format(time.RFC3339Nano)
}
func parseGatewayTime(v string) (time.Time, error) {
	if v == "" {
		return time.Time{}, nil
	}
	return time.Parse(time.RFC3339Nano, v)
}
func openGatewayDatabase(dir string, readonly bool) (*sql.DB, error) {
	path := filepath.Join(dir, "gateway.sqlite")
	db, err := sqlitestate.Open(path, readonly)
	if err != nil {
		return nil, err
	}
	if readonly {
		var v int
		err = db.QueryRow(`PRAGMA user_version`).Scan(&v)
		if err == nil && v != 1 {
			err = fmt.Errorf("unsupported gateway database version %d", v)
		}
	} else {
		err = migrateGatewayDatabase(db, dir)
	}
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

var gatewaySchema = []string{
	`CREATE TABLE registrations(schema INTEGER NOT NULL,id TEXT NOT NULL UNIQUE,profile_id TEXT NOT NULL PRIMARY KEY,profile_fingerprint TEXT NOT NULL,http_port INTEGER NOT NULL,state TEXT NOT NULL,migration_stage TEXT NOT NULL,owner_generation TEXT NOT NULL,owner_pid INTEGER NOT NULL,owner_started_at TEXT NOT NULL,owner_executable TEXT NOT NULL,owner_command_line TEXT NOT NULL,backend_generation TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,last_ready_at TEXT NOT NULL,last_error TEXT NOT NULL,recovery_attempts INTEGER NOT NULL,recovery_window_at TEXT NOT NULL,recovery_blocked INTEGER NOT NULL,replaced_instance_id TEXT NOT NULL)`,
	`CREATE TABLE legacy_ports(profile_id TEXT NOT NULL REFERENCES registrations(profile_id) ON DELETE CASCADE,position INTEGER NOT NULL,port INTEGER NOT NULL CHECK(port BETWEEN 1 AND 65535),PRIMARY KEY(profile_id,position))`,
	`CREATE TABLE gateway_events(seq INTEGER PRIMARY KEY,profile_id TEXT NOT NULL,batch INTEGER NOT NULL,at TEXT NOT NULL,registration_id TEXT NOT NULL,event_name TEXT NOT NULL,http_port INTEGER NOT NULL,backend_generation TEXT NOT NULL,state TEXT NOT NULL,error TEXT NOT NULL,details TEXT NOT NULL)`,
	`CREATE INDEX gateway_events_batch ON gateway_events(profile_id,batch,seq)`,
	`CREATE TABLE event_windows(profile_id TEXT PRIMARY KEY,current_batch INTEGER NOT NULL,previous_batch INTEGER NOT NULL,current_bytes INTEGER NOT NULL)`,
	`CREATE TABLE gateway_imports(source_path TEXT PRIMARY KEY)`,
}

const registrationCols = `schema,id,profile_id,profile_fingerprint,http_port,state,migration_stage,owner_generation,owner_pid,owner_started_at,owner_executable,owner_command_line,backend_generation,created_at,updated_at,last_ready_at,last_error,recovery_attempts,recovery_window_at,recovery_blocked,replaced_instance_id`
const registrationPut = `INSERT INTO registrations(schema,id,profile_id,profile_fingerprint,http_port,state,migration_stage,owner_generation,owner_pid,owner_started_at,owner_executable,owner_command_line,backend_generation,created_at,updated_at,last_ready_at,last_error,recovery_attempts,recovery_window_at,recovery_blocked,replaced_instance_id) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(profile_id) DO UPDATE SET schema=excluded.schema,id=excluded.id,profile_id=excluded.profile_id,profile_fingerprint=excluded.profile_fingerprint,http_port=excluded.http_port,state=excluded.state,migration_stage=excluded.migration_stage,owner_generation=excluded.owner_generation,owner_pid=excluded.owner_pid,owner_started_at=excluded.owner_started_at,owner_executable=excluded.owner_executable,owner_command_line=excluded.owner_command_line,backend_generation=excluded.backend_generation,created_at=excluded.created_at,updated_at=excluded.updated_at,last_ready_at=excluded.last_ready_at,last_error=excluded.last_error,recovery_attempts=excluded.recovery_attempts,recovery_window_at=excluded.recovery_window_at,recovery_blocked=excluded.recovery_blocked,replaced_instance_id=excluded.replaced_instance_id WHERE (schema,id,profile_id,profile_fingerprint,http_port,state,migration_stage,owner_generation,owner_pid,owner_started_at,owner_executable,owner_command_line,backend_generation,created_at,updated_at,last_ready_at,last_error,recovery_attempts,recovery_window_at,recovery_blocked,replaced_instance_id) IS NOT (excluded.schema,excluded.id,excluded.profile_id,excluded.profile_fingerprint,excluded.http_port,excluded.state,excluded.migration_stage,excluded.owner_generation,excluded.owner_pid,excluded.owner_started_at,excluded.owner_executable,excluded.owner_command_line,excluded.backend_generation,excluded.created_at,excluded.updated_at,excluded.last_ready_at,excluded.last_error,excluded.recovery_attempts,excluded.recovery_window_at,excluded.recovery_blocked,excluded.replaced_instance_id)`

func registrationArgs(v Registration) []any {
	return []any{v.Schema, v.ID, v.ProfileID, v.ProfileFingerprint, v.HTTPPort, v.State, v.MigrationStage, v.OwnerGeneration, v.OwnerPID, gatewayTime(v.OwnerStartedAt), v.OwnerExecutable, v.OwnerCommandLine, v.BackendGeneration, gatewayTime(v.CreatedAt), gatewayTime(v.UpdatedAt), gatewayTime(v.LastReadyAt), v.LastError, v.RecoveryAttempts, gatewayTime(v.RecoveryWindowAt), v.RecoveryBlocked, v.ReplacedInstanceID}
}
func readGatewayRegistration(tx *sql.Tx, profile string) (Registration, error) {
	var v Registration
	var owner_started_at string
	var created_at string
	var updated_at string
	var last_ready_at string
	var recovery_window_at string
	err := tx.QueryRow(`SELECT `+registrationCols+` FROM registrations WHERE profile_id=?`, profile).Scan(&v.Schema, &v.ID, &v.ProfileID, &v.ProfileFingerprint, &v.HTTPPort, &v.State, &v.MigrationStage, &v.OwnerGeneration, &v.OwnerPID, &owner_started_at, &v.OwnerExecutable, &v.OwnerCommandLine, &v.BackendGeneration, &created_at, &updated_at, &last_ready_at, &v.LastError, &v.RecoveryAttempts, &recovery_window_at, &v.RecoveryBlocked, &v.ReplacedInstanceID)
	if err == sql.ErrNoRows {
		return v, ErrRegistrationNotFound
	}
	if err != nil {
		return v, err
	}
	v.OwnerStartedAt, err = parseGatewayTime(owner_started_at)
	if err != nil {
		return v, err
	}
	v.CreatedAt, err = parseGatewayTime(created_at)
	if err != nil {
		return v, err
	}
	v.UpdatedAt, err = parseGatewayTime(updated_at)
	if err != nil {
		return v, err
	}
	v.LastReadyAt, err = parseGatewayTime(last_ready_at)
	if err != nil {
		return v, err
	}
	v.RecoveryWindowAt, err = parseGatewayTime(recovery_window_at)
	if err != nil {
		return v, err
	}
	rows, err := tx.Query(`SELECT port FROM legacy_ports WHERE profile_id=? ORDER BY position`, profile)
	if err != nil {
		return v, err
	}
	defer rows.Close()
	for rows.Next() {
		var port int
		if err = rows.Scan(&port); err != nil {
			return v, err
		}
		v.LegacyPorts = append(v.LegacyPorts, port)
	}
	if err = rows.Err(); err != nil {
		return v, err
	}
	return v, Validate(v)
}
func (r *Registry) readRegistration(profile string) (Registration, error) {
	tx, err := r.db.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return Registration{}, err
	}
	defer tx.Rollback()
	v, err := readGatewayRegistration(tx, profile)
	if err != nil {
		return v, err
	}
	return v, tx.Commit()
}
func putGatewayRegistration(tx *sql.Tx, v Registration) error {
	if err := Validate(v); err != nil {
		return err
	}
	if _, err := tx.Exec(registrationPut, registrationArgs(v)...); err != nil {
		return err
	}
	stmt, err := tx.Prepare(`INSERT INTO legacy_ports(profile_id,position,port) VALUES(?,?,?)`)
	if err != nil {
		return err
	}
	defer stmt.Close()
	for i, p := range v.LegacyPorts {
		if _, err = stmt.Exec(v.ProfileID, i, p); err != nil {
			return err
		}
	}
	return nil
}
func (r *Registry) save(v Registration) error {
	if err := Validate(v); err != nil {
		return err
	}
	tx, err := r.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	old, err := readGatewayRegistration(tx, v.ProfileID)
	if err != nil && !errors.Is(err, ErrRegistrationNotFound) {
		return err
	}
	samePorts := slices.Equal(old.LegacyPorts, v.LegacyPorts)
	a, b := old, v
	a.LegacyPorts = nil
	b.LegacyPorts = nil
	a.UpdatedAt = time.Time{}
	b.UpdatedAt = time.Time{}
	// The remaining fields are scalars; no whole-object serialization.
	same := samePorts && a.Schema == b.Schema && a.ID == b.ID && a.ProfileID == b.ProfileID && a.ProfileFingerprint == b.ProfileFingerprint && a.HTTPPort == b.HTTPPort && a.State == b.State && a.MigrationStage == b.MigrationStage && a.OwnerGeneration == b.OwnerGeneration && a.OwnerPID == b.OwnerPID && a.OwnerStartedAt.Equal(b.OwnerStartedAt) && a.OwnerExecutable == b.OwnerExecutable && a.OwnerCommandLine == b.OwnerCommandLine && a.BackendGeneration == b.BackendGeneration && a.CreatedAt.Equal(b.CreatedAt) && a.UpdatedAt.Equal(b.UpdatedAt) && a.LastReadyAt.Equal(b.LastReadyAt) && a.LastError == b.LastError && a.RecoveryAttempts == b.RecoveryAttempts && a.RecoveryWindowAt.Equal(b.RecoveryWindowAt) && a.RecoveryBlocked == b.RecoveryBlocked && a.ReplacedInstanceID == b.ReplacedInstanceID
	if same {
		return tx.Commit()
	}
	v.Schema = CurrentSchema
	v.UpdatedAt = time.Now()
	if _, err = tx.Exec(registrationPut, registrationArgs(v)...); err != nil {
		return err
	}
	if !samePorts {
		if _, err = tx.Exec(`DELETE FROM legacy_ports WHERE profile_id=?`, v.ProfileID); err != nil {
			return err
		}
		stmt, err := tx.Prepare(`INSERT INTO legacy_ports VALUES(?,?,?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for i, p := range v.LegacyPorts {
			if _, err = stmt.Exec(v.ProfileID, i, p); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

// Event rotation keeps the current and previous byte-bounded batch. Switching
// batches changes one header and deletes only the expired batch; retained
// event rows are never rewritten. No periodic scan is needed.
func appendGatewayEvent(tx *sql.Tx, v Event) error {
	size := int64(128 + len(gatewayTime(v.At)) + len(v.RegistrationID) + len(v.ProfileID) + len(v.Event) + len(v.BackendGeneration) + len(v.State) + len(v.Error) + len(v.Details))
	if _, err := tx.Exec(`INSERT INTO event_windows VALUES(?,0,-1,0) ON CONFLICT(profile_id) DO NOTHING`, v.ProfileID); err != nil {
		return err
	}
	var batch, previous, bytes int64
	if err := tx.QueryRow(`SELECT current_batch,previous_batch,current_bytes FROM event_windows WHERE profile_id=?`, v.ProfileID).Scan(&batch, &previous, &bytes); err != nil {
		return err
	}
	if bytes+size > eventLogMaxBytes && bytes > 0 {
		if _, err := tx.Exec(`DELETE FROM gateway_events WHERE profile_id=? AND batch=?`, v.ProfileID, previous); err != nil {
			return err
		}
		previous = batch
		batch++
		bytes = 0
	}
	if _, err := tx.Exec(`INSERT INTO gateway_events(profile_id,batch,at,registration_id,event_name,http_port,backend_generation,state,error,details) VALUES(?,?,?,?,?,?,?,?,?,?)`, v.ProfileID, batch, gatewayTime(v.At), v.RegistrationID, v.Event, v.HTTPPort, v.BackendGeneration, v.State, v.Error, v.Details); err != nil {
		return err
	}
	_, err := tx.Exec(`UPDATE event_windows SET current_batch=?,previous_batch=?,current_bytes=? WHERE profile_id=?`, batch, previous, bytes+size, v.ProfileID)
	return err
}
func (r *Registry) AppendEvent(v Event) error {
	if r == nil {
		return errors.New("nil app gateway registry")
	}
	v.ProfileID = strings.TrimSpace(v.ProfileID)
	if v.ProfileID == "" {
		return fmt.Errorf("%w: event profile id is required", ErrInvalidRegistration)
	}
	if strings.TrimSpace(v.RegistrationID) == "" {
		v.RegistrationID = profileFilePart(v.ProfileID)
	}
	if strings.TrimSpace(v.Event) == "" {
		return errors.New("app gateway event name is required")
	}
	if v.At.IsZero() {
		v.At = time.Now().UTC()
	}
	tx, err := r.db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = appendGatewayEvent(tx, v); err != nil {
		return err
	}
	return tx.Commit()
}
