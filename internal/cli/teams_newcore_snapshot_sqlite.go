package cli

import (
	"database/sql"
	"fmt"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

func writeTeamsStatusSnapshot(path string, p teamsPublishedStatus) error {
	has := p.State.Control != nil
	var control teamsControlChat
	if has {
		control = *p.State.Control
	}
	return sqlitestate.PublishSnapshot(path, nil, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`CREATE TABLE status(id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL,generation INTEGER NOT NULL,host TEXT NOT NULL,pid INTEGER NOT NULL,at INTEGER NOT NULL,running_version TEXT NOT NULL,update_installed TEXT NOT NULL,update_scheduled TEXT NOT NULL,codex_upgrade INTEGER NOT NULL,sessions INTEGER NOT NULL,active INTEGER NOT NULL,parked INTEGER NOT NULL,queued INTEGER NOT NULL,running INTEGER NOT NULL,out_queued INTEGER NOT NULL,out_sending INTEGER NOT NULL,out_unknown INTEGER NOT NULL,workflow INTEGER NOT NULL,workflow_file TEXT NOT NULL,machine_id TEXT NOT NULL,has_control INTEGER NOT NULL,control_chat TEXT NOT NULL,control_title TEXT NOT NULL,control_url TEXT NOT NULL)`); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO status(id,version,generation,host,pid,at,running_version,update_installed,update_scheduled,codex_upgrade,sessions,active,parked,queued,running,out_queued,out_sending,out_unknown,workflow,workflow_file,machine_id,has_control,control_chat,control_title,control_url) VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`, p.V, p.Gen, p.Host, p.PID, p.At, p.State.RunningVersion, p.State.UpdateInstalled, p.State.UpdateScheduled, p.State.CodexUpgrade, p.State.Sessions, p.State.Active, p.State.Parked, p.State.Queued, p.State.Running, p.State.OutQueued, p.State.OutSending, p.State.OutUnknown, p.State.Workflow, p.State.WorkflowFile, p.State.MachineID, has, control.ChatID, control.Title, control.URL)
		return err
	})
}
func readTeamsStatusSnapshot(path string) (p teamsPublishedStatus, err error) {
	db, err := sqlitestate.OpenSnapshot(path, 64*1024)
	if err != nil {
		return p, err
	}
	defer db.Close()
	var v int
	if err = db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		return p, err
	}
	if v != 1 {
		return p, fmt.Errorf("unsupported published status schema %d", v)
	}
	var has bool
	var control teamsControlChat
	err = db.QueryRow(`SELECT version,generation,host,pid,at,running_version,update_installed,update_scheduled,codex_upgrade,sessions,active,parked,queued,running,out_queued,out_sending,out_unknown,workflow,workflow_file,machine_id,has_control,control_chat,control_title,control_url FROM status WHERE id=1`).Scan(&p.V, &p.Gen, &p.Host, &p.PID, &p.At, &p.State.RunningVersion, &p.State.UpdateInstalled, &p.State.UpdateScheduled, &p.State.CodexUpgrade, &p.State.Sessions, &p.State.Active, &p.State.Parked, &p.State.Queued, &p.State.Running, &p.State.OutQueued, &p.State.OutSending, &p.State.OutUnknown, &p.State.Workflow, &p.State.WorkflowFile, &p.State.MachineID, &has, &control.ChatID, &control.Title, &control.URL)
	if has {
		p.State.Control = &control
	}
	return p, err
}

func writeTeamsPollSnapshot(path string, s teamsPollStamp) error {
	return sqlitestate.PublishSnapshot(path, nil, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`CREATE TABLE poll(id INTEGER PRIMARY KEY CHECK(id=1),host TEXT NOT NULL,pid INTEGER NOT NULL,version TEXT NOT NULL,at INTEGER NOT NULL)`); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO poll VALUES(1,?,?,?,?)`, s.Host, s.PID, s.Version, s.At)
		return err
	})
}
func readTeamsPollSnapshot(path string) (s teamsPollStamp, err error) {
	db, err := sqlitestate.OpenSnapshot(path, 64*1024)
	if err != nil {
		return s, err
	}
	defer db.Close()
	var v int
	if err = db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		return s, err
	}
	if v != 1 {
		return s, fmt.Errorf("unsupported poll schema %d", v)
	}
	err = db.QueryRow(`SELECT host,pid,version,at FROM poll WHERE id=1`).Scan(&s.Host, &s.PID, &s.Version, &s.At)
	return s, err
}
