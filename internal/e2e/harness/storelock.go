package harness

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite" // driver "sqlite"
)

// HoldStoreWriteLock takes the write lock of the helper's SQLite store at
// path, the way a helper stopped inside a store write holds it: every other
// writer gets SQLITE_BUSY until release, readers are not blocked. It waits
// up to wait for the current writer (write transactions are short). Nothing
// is written; release rolls back. This is environment fault injection, like
// SIGSTOP: the harness does not read or change the store's contents.
func HoldStoreWriteLock(path string, wait time.Duration) (release func(), err error) {
	q := url.Values{}
	q.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", wait.Milliseconds()))
	q.Set("_txlock", "immediate")
	db, err := sql.Open("sqlite", "file:"+filepath.ToSlash(path)+"?"+q.Encode())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("harness: take the store's write lock: %w", err)
	}
	return func() {
		_ = tx.Rollback()
		_ = db.Close()
	}, nil
}

// FreezeOutsideStoreWrite freezes sut (SIGSTOP) at a point where it holds
// no store write lock: the harness holds the lock while it stops the
// process, so the process is at most waiting for it, then lets go. A
// frozen helper therefore never blocks a standby's takeover by its lock;
// the scenario that wants that holds the lock itself (HoldStoreWriteLock).
func FreezeOutsideStoreWrite(sut SUT, store string) error {
	release, err := HoldStoreWriteLock(store, 30*time.Second)
	if err != nil {
		return err
	}
	defer release()
	if !Freeze(sut) {
		return fmt.Errorf("harness: cannot freeze %s", sut.Name())
	}
	return nil
}
