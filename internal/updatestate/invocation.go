package updatestate

import (
	"fmt"
	"time"
)

// Invocation is immutable input handed to a verified candidate. Progress and
// recovery decisions remain in pending_updates and the existing runtime owner.
type Invocation struct {
	Schema          int    `json:"schema"`
	CandidatePath   string `json:"candidate_path"`
	CandidateSHA256 string `json:"candidate_sha256"`
	SourceVersion   string `json:"source_version,omitempty"`
	TargetVersion   string `json:"target_version"`
	RuntimeRoot     string `json:"runtime_root"`
	EntryPath       string `json:"entry_path"`
	RecordPath      string `json:"record_path,omitempty"`
	RequestID       string `json:"request_id"`
}

const invocationDDL = `CREATE TABLE update_invocations(request_id TEXT PRIMARY KEY CHECK(request_id<>''),schema_version INTEGER NOT NULL CHECK(schema_version=1),candidate_path TEXT NOT NULL,candidate_sha256 TEXT NOT NULL,source_version TEXT NOT NULL,target_version TEXT NOT NULL,runtime_root TEXT NOT NULL,entry_path TEXT NOT NULL,record_path TEXT NOT NULL,created_at INTEGER NOT NULL);
CREATE INDEX update_invocations_expiry ON update_invocations(created_at,request_id);`

func (s *Store) SaveInvocation(r Invocation) error {
	if r.Schema != 1 || r.RequestID == "" {
		return fmt.Errorf("invalid update invocation")
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	// Inputs are read once before the candidate starts its bounded update.
	// This is not execution evidence: boot/source fencing remains authoritative.
	// Reap at most 128 week-old crash leftovers on insertion, never on reads.
	// Inserting first also keeps a repeated request ID immutable.
	now := time.Now()
	if _, err = tx.Exec(`INSERT INTO update_invocations VALUES(?,?,?,?,?,?,?,?,?,?)`, r.RequestID, r.Schema, r.CandidatePath, r.CandidateSHA256, r.SourceVersion, r.TargetVersion, r.RuntimeRoot, r.EntryPath, r.RecordPath, now.Unix()); err != nil {
		return err
	}
	if _, err = tx.Exec(`DELETE FROM update_invocations WHERE request_id IN (SELECT request_id FROM update_invocations WHERE created_at<? ORDER BY created_at,request_id LIMIT 128)`, now.Add(-7*24*time.Hour).Unix()); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) Invocation(id string) (Invocation, error) {
	var r Invocation
	err := s.db.QueryRow(`SELECT request_id,schema_version,candidate_path,candidate_sha256,source_version,target_version,runtime_root,entry_path,record_path FROM update_invocations WHERE request_id=?`, id).Scan(&r.RequestID, &r.Schema, &r.CandidatePath, &r.CandidateSHA256, &r.SourceVersion, &r.TargetVersion, &r.RuntimeRoot, &r.EntryPath, &r.RecordPath)
	if err != nil {
		return r, noRow(err)
	}
	if r.Schema != 1 {
		return r, fmt.Errorf("unsupported invocation schema %d", r.Schema)
	}
	return r, nil
}
func (s *Store) DeleteInvocation(id string) error {
	_, err := s.db.Exec(`DELETE FROM update_invocations WHERE request_id=?`, id)
	return err
}
