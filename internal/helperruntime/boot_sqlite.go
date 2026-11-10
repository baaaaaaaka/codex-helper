package helperruntime

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

// ErrBootstrapState is fatal before application setup. Falling through a
// corrupt/incompatible bootstrap could admit an old reader to migrated state.
var ErrBootstrapState = errors.New("invalid bootstrap state; use the explicit installer")

func readNativeBoot(root string) (s BootState, err error) {
	path := filepath.Join(root, BootFile)
	info, err := sqlitestate.Inspect(path)
	if os.IsNotExist(err) {
		if _, oldErr := os.Lstat(filepath.Join(root, "boot.json")); oldErr == nil {
			return s, ErrBootstrapState
		} else if !os.IsNotExist(oldErr) {
			return s, fmt.Errorf("%w: %v", ErrBootstrapState, oldErr)
		}
		return s, err
	}
	if err != nil {
		return s, fmt.Errorf("%w: %v", ErrBootstrapState, err)
	}
	if !info.Mode().IsRegular() || info.Size() > maxBootRecordSize {
		return s, ErrBootstrapState
	}
	db, err := sqlitestate.OpenSnapshot(path, maxBootRecordSize)
	if err != nil {
		return s, fmt.Errorf("%w: %v", ErrBootstrapState, err)
	}
	defer db.Close()
	var v int
	if err = db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil || v != 1 {
		return s, fmt.Errorf("%w: schema %d: %v", ErrBootstrapState, v, err)
	}
	// Decimal TEXT is one unsigned integer field, preserving the full uint64
	// generation range. SQLite's signed INTEGER would lose its upper half.
	var generation string
	var candidate, hash, attempt sql.NullString
	err = db.QueryRow(`SELECT protocol,generation,good,previous,candidate_version,candidate_sha256,candidate_attempt FROM boot WHERE id=1`).Scan(&s.Schema, &generation, &s.Good, &s.Previous, &candidate, &hash, &attempt)
	if err != nil {
		return s, fmt.Errorf("%w: %v", ErrBootstrapState, err)
	}
	if s.Generation, err = strconv.ParseUint(generation, 10, 64); err != nil {
		return s, fmt.Errorf("%w: generation", ErrBootstrapState)
	}
	if candidate.Valid {
		if !hash.Valid || !attempt.Valid {
			return s, ErrBootstrapState
		}
		s.Candidate = &BootCandidate{Version: candidate.String, SHA256: hash.String, Attempt: attempt.String}
	} else if hash.Valid || attempt.Valid {
		return s, ErrBootstrapState
	}
	if err = validateBoot(s); err != nil {
		return s, fmt.Errorf("%w: %v", ErrBootstrapState, err)
	}
	return s, nil
}

func encodeNativeBoot(root string, s BootState) ([]byte, error) {
	f, err := os.CreateTemp(root, ".boot-sqlite-*")
	if err != nil {
		return nil, err
	}
	path := f.Name()
	if err = f.Close(); err != nil {
		os.Remove(path)
		return nil, err
	}
	defer os.Remove(path)
	var candidate, hash, attempt any
	if s.Candidate != nil {
		candidate, hash, attempt = s.Candidate.Version, s.Candidate.SHA256, s.Candidate.Attempt
	}
	err = sqlitestate.PublishSnapshot(path, nil, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`CREATE TABLE boot(id INTEGER PRIMARY KEY CHECK(id=1),protocol INTEGER NOT NULL,generation TEXT NOT NULL,good TEXT NOT NULL,previous TEXT NOT NULL,candidate_version TEXT,candidate_sha256 TEXT,candidate_attempt TEXT)`); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO boot VALUES(1,?,?,?,?,?,?,?)`, s.Schema, strconv.FormatUint(s.Generation, 10), s.Good, s.Previous, candidate, hash, attempt)
		return err
	})
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.Size() > maxBootRecordSize {
		return nil, fmt.Errorf("boot SQLite record too large")
	}
	// The existing atomic-write implementation preserves its platform replacement,
	// file/dir sync and fault boundaries. Only this bounded snapshot is copied.
	return os.ReadFile(path)
}
