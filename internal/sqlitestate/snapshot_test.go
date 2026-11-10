package sqlitestate

import (
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestSQLiteSnapshotReaderIsBoundedImmutableAndAllowsReplacement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.sqlite")
	publish := func(value int) error {
		return PublishSnapshot(path, nil, func(tx *sql.Tx) error {
			if _, err := tx.Exec(`CREATE TABLE item(value INTEGER NOT NULL)`); err != nil {
				return err
			}
			_, err := tx.Exec(`INSERT INTO item VALUES(?)`, value)
			return err
		})
	}
	if err := publish(1); err != nil {
		t.Fatal(err)
	}
	if db, err := OpenSnapshot(path, 16); err == nil {
		db.Close()
		t.Fatal("size bound ignored")
	}
	old, err := OpenSnapshot(path, 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer old.Close()
	if _, err := old.Exec(`UPDATE item SET value=2`); err == nil {
		t.Fatal("snapshot reader accepted mutation")
	}
	if err := publish(3); err != nil {
		t.Fatal("open reader blocked publication", err)
	}
	var value int
	if err := old.QueryRow(`SELECT value FROM item`).Scan(&value); err != nil || value != 1 {
		t.Fatal("old snapshot", value, err)
	}
	fresh, err := OpenSnapshot(path, 64*1024)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	if err := fresh.QueryRow(`SELECT value FROM item`).Scan(&value); err != nil || value != 3 {
		t.Fatal("fresh snapshot", value, err)
	}
	if err := os.WriteFile(path, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if db, err := OpenSnapshot(path, 64*1024); err == nil {
		db.Close()
		t.Fatal("corruption accepted")
	}
}

func TestSQLiteSnapshotAtomicFailureAndReadOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "view.sqlite")
	publish := func(n int, fail bool) error {
		return PublishSnapshot(path, nil, func(tx *sql.Tx) error {
			if _, err := tx.Exec(`CREATE TABLE view(value INTEGER NOT NULL)`); err != nil {
				return err
			}
			if _, err := tx.Exec(`INSERT INTO view VALUES(?)`, n); err != nil {
				return err
			}
			if fail {
				return errors.New("injected failure after data")
			}
			return nil
		})
	}
	if err := publish(9001, false); err != nil {
		t.Fatal(err)
	}
	before, _ := os.Stat(path)
	if err := publish(9002, true); err == nil {
		t.Fatal("accepted failed snapshot")
	}
	db, err := Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	var value int
	if err = db.QueryRow(`SELECT value FROM view`).Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != 9001 {
		t.Fatal("failed replacement changed published state", value)
	}
	if _, err = db.Exec(`UPDATE view SET value=0`); err == nil {
		t.Fatal("snapshot reader could write")
	}
	db.Close()
	after, _ := os.Stat(path)
	if !before.ModTime().Equal(after.ModTime()) || before.Size() != after.Size() {
		t.Fatal("failed publication/read changed snapshot")
	}
	entries, _ := os.ReadDir(filepath.Dir(path))
	if len(entries) != 1 {
		t.Fatalf("unpublished databases leaked: %v", entries)
	}
	if err := publish(9003, false); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err = db.QueryRow(`SELECT value FROM view`).Scan(&value); err != nil || value != 9003 {
		t.Fatalf("replacement: %d %v", value, err)
	}
}

func BenchmarkSQLiteSnapshotPublication(b *testing.B) {
	path := filepath.Join(b.TempDir(), "view.sqlite")
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := PublishSnapshot(path, nil, func(tx *sql.Tx) error {
			if _, err := tx.Exec(`CREATE TABLE view(id INTEGER PRIMARY KEY,value INTEGER NOT NULL,label TEXT NOT NULL)`); err != nil {
				return err
			}
			_, err := tx.Exec(`INSERT INTO view VALUES(1,?,'host')`, i)
			return err
		}); err != nil {
			b.Fatal(err)
		}
	}
	if info, err := os.Stat(path); err == nil {
		b.ReportMetric(float64(info.Size()), "snapshot_bytes")
	}
}

func BenchmarkSQLiteSnapshotRead(b *testing.B) {
	path := filepath.Join(b.TempDir(), "status.sqlite")
	if err := PublishSnapshot(path, nil, func(tx *sql.Tx) error {
		_, err := tx.Exec(`CREATE TABLE status(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO status VALUES(1,'ready')`)
		return err
	}); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		db, err := OpenSnapshot(path, 64*1024)
		if err != nil {
			b.Fatal(err)
		}
		var got string
		err = db.QueryRow(`SELECT value FROM status WHERE id=1`).Scan(&got)
		closeErr := db.Close()
		if err != nil || closeErr != nil || got != "ready" {
			b.Fatal(got, err, closeErr)
		}
	}
}
