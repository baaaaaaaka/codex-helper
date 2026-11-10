package sqlitestate

import (
	"bytes"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
)

func TestOpenRejectsNonemptyInvalidHeaderWithoutReset(t *testing.T) {
	for _, input := range []string{"{", "\x00", "not a SQLite database", "SQLite format 2\x00"} {
		path := filepath.Join(t.TempDir(), "state.sqlite")
		if err := os.WriteFile(path, []byte(input), 0600); err != nil {
			t.Fatal(err)
		}
		for _, readonly := range []bool{false, true} {
			if db, err := Open(path, readonly); err == nil {
				db.Close()
				t.Fatal("corruption admitted", input, readonly)
			}
			if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, []byte(input)) {
				t.Fatal("corrupt evidence changed", err)
			}
		}
	}
}

func TestOpenConcurrentAndReadOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	if db, err := Open(path, true); !os.IsNotExist(err) {
		if db != nil {
			db.Close()
		}
		t.Fatalf("missing read-only database: %v", err)
	}
	start := make(chan struct{})
	errs := make(chan error, 16)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			db, err := Open(path, false)
			if err == nil {
				_, err = db.Exec(`CREATE TABLE IF NOT EXISTS records(id INTEGER PRIMARY KEY)`)
				db.Close()
			}
			errs <- err
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	db, err := Open(path, true)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`INSERT INTO records VALUES(1)`); err == nil {
		t.Fatal("read-only connection wrote")
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err = os.Stat(path + suffix); !os.IsNotExist(err) {
			t.Fatalf("unexpected sidecar %s: %v", suffix, err)
		}
	}
	if info, err := os.Stat(path); err != nil || (runtime.GOOS != "windows" && info.Mode().Perm() != 0600) {
		t.Fatalf("permissions %v/%v", info, err)
	}
}

func TestOpenRejectsNonFilesAndWAL(t *testing.T) {
	dir := t.TempDir()
	if db, err := Open(dir, false); err == nil {
		db.Close()
		t.Fatal("opened directory")
	}
	path := filepath.Join(dir, "state.sqlite")
	db, err := Open(path, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`PRAGMA journal_mode=WAL`); err != nil {
		t.Fatal(err)
	}
	db.Close()
	if db, err := Open(path, false); err == nil {
		db.Close()
		t.Fatal("silently admitted a WAL database")
	}
	link := filepath.Join(dir, "link.sqlite")
	if err = os.Symlink(path, link); err == nil {
		if db, err := Open(link, false); err == nil {
			db.Close()
			t.Fatal("followed symlink")
		}
	}
}
