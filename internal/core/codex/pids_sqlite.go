package codex

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

func pidDatabasePath(path string) string {
	if strings.HasSuffix(path, ".sqlite") {
		return path
	}
	return path + ".sqlite"
}
func openPIDDatabase(path string) (*sql.DB, error) {
	native := pidDatabasePath(path)
	if err := os.MkdirAll(filepath.Dir(native), 0700); err != nil {
		return nil, err
	}
	db, err := sqlitestate.Open(native, false)
	if err != nil {
		return nil, err
	}
	if err = migratePIDDatabase(db, path); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

const pidPut = `INSERT INTO app_servers(pid,start_time,server_key,boot_id,pid_namespace) VALUES(?,?,?,?,?) ON CONFLICT(pid) DO UPDATE SET start_time=excluded.start_time,server_key=excluded.server_key,boot_id=excluded.boot_id,pid_namespace=excluded.pid_namespace WHERE (start_time,server_key,boot_id,pid_namespace) IS NOT (excluded.start_time,excluded.server_key,excluded.boot_id,excluded.pid_namespace)`

// pidMu fences the in-memory lookup through commit, so a delayed removal
// cannot erase the replacement server. Reaping with no victims does no SQL.
func (p *Pool) writePIDs(keys ...string) {
	if p.cfg.PIDFile == "" || len(keys) == 0 {
		return
	}
	p.pidMu.Lock()
	defer p.pidMu.Unlock()
	if p.pidClosed {
		return
	}
	err := p.savePIDKeys(keys)
	if err != nil {
		fmt.Fprintf(p.cfg.Log, "codex: write process records: %v\n", err)
	}
}
func (p *Pool) savePIDKeys(keys []string) error {
	if p.pidStore == nil {
		db, err := openPIDDatabase(p.cfg.PIDFile)
		if err != nil {
			return err
		}
		p.pidStore = db
	}
	// Snapshot only the affected servers, never the complete pool/history.
	records := make([]pidRecord, len(keys))
	boot, ns := bootID(), pidNamespace()
	p.mu.Lock()
	for i, key := range keys {
		if e := p.servers[key]; e != nil && e.srv != nil && !e.srv.Exited() {
			records[i] = pidRecord{PID: e.srv.PID(), Start: e.start, Key: key, Boot: boot, NS: ns}
		}
	}
	p.mu.Unlock()
	tx, err := p.pidStore.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for i, key := range keys {
		r := records[i]
		if _, err = tx.Exec(`DELETE FROM app_servers WHERE server_key=? AND pid<>?`, key, r.PID); err != nil {
			return err
		}
		if r.PID != 0 {
			if _, err = tx.Exec(pidPut, r.PID, r.Start, r.Key, r.Boot, r.NS); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}
func readPIDPage(db *sql.DB, after int) ([]pidRecord, error) {
	rows, err := db.Query(`SELECT pid,start_time,server_key,boot_id,pid_namespace FROM app_servers WHERE pid>? ORDER BY pid LIMIT 128`, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var recs []pidRecord
	for rows.Next() {
		var r pidRecord
		if err = rows.Scan(&r.PID, &r.Start, &r.Key, &r.Boot, &r.NS); err != nil {
			return nil, err
		}
		recs = append(recs, r)
	}
	return recs, rows.Err()
}

// KillStale runs under the existing core ownership fence, before the new pool
// starts. Cursors close before process I/O. The transaction that clears these
// records runs only after every identified group has stopped successfully.
func KillStale(path string, wait time.Duration) (int, error) {
	if _, err := sqlitestate.Inspect(pidDatabasePath(path)); os.IsNotExist(err) {
		if _, err = os.Lstat(legacyPIDPath(path)); os.IsNotExist(err) {
			return 0, nil
		} else if err != nil {
			return 0, err
		}
	} else if err != nil {
		return 0, err
	}
	db, err := openPIDDatabase(path)
	if err != nil {
		return 0, err
	}
	defer db.Close()
	boot, ns := bootID(), pidNamespace()
	var targets []int
	after := 0
	for {
		recs, err := readPIDPage(db, after)
		if err != nil {
			return len(targets), err
		}
		if len(recs) == 0 {
			break
		}
		for _, r := range recs {
			after = r.PID
			if r.PID <= 1 || r.PID == os.Getpid() || !sameProcess(r, boot, ns, startTime(r.PID)) || !groupAlive(r.PID) {
				continue
			}
			_ = killGroup(r.PID)
			targets = append(targets, r.PID)
		}
	}
	deadline := time.Now().Add(wait)
	for _, pid := range targets {
		for groupAlive(pid) {
			if time.Now().After(deadline) {
				return len(targets), fmt.Errorf("codex: old app-server group %d still alive", pid)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	_, err = db.Exec(`DELETE FROM app_servers`)
	return len(targets), err
}
