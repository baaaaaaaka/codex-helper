package codex

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

func testPIDRecords(t *testing.T, path string) []pidRecord {
	t.Helper()
	db, err := openPIDDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := readPIDPage(db, 0)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}
func TestConcurrentPIDFileWritesStayValid(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pids.sqlite")
	// The pool shares one handle between its goroutines. Independent process
	// opens are covered below; 64 FULL-sync writers on separate connections do
	// not model the pool and can exceed the bounded lock wait on Windows.
	db, err := openPIDDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if _, err := db.Exec(pidPut, 1000+i, "start", fmt.Sprint(i), "boot", "ns"); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	recs := testPIDRecords(t, path)
	if len(recs) != 64 {
		t.Fatalf("records %d", len(recs))
	}
	p := NewPool(PoolConfig{PIDFile: path})
	defer p.Close(0)
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); p.writePIDs(fmt.Sprint(i)) }(i)
	}
	wg.Wait()
	if len(testPIDRecords(t, path)) != 0 {
		t.Fatal("removed keys still recorded")
	}
}

func TestPIDSQLiteIndependentOpeners(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pids.sqlite")
	var wg sync.WaitGroup
	for i := range 4 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			db, err := openPIDDatabase(path)
			if err != nil {
				t.Error(err)
				return
			}
			defer db.Close()
			if _, err := db.Exec(pidPut, 1000+i, "start", fmt.Sprint(i), "boot", "ns"); err != nil {
				t.Error(err)
			}
		}(i)
	}
	wg.Wait()
	if n := len(testPIDRecords(t, path)); n != 4 {
		t.Fatalf("records %d", n)
	}
}
