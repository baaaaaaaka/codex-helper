package sqlitestate

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPublicationMissingDatabaseFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite")
	if _, err := Inspect(path); !os.IsNotExist(err) {
		t.Fatalf("new location %v", err)
	}
	db, err := Open(path, false)
	if err != nil {
		t.Fatal(err)
	}
	if err = Seal(db); err == nil {
		t.Fatal("version zero published")
	}
	if _, err = db.Exec(`CREATE TABLE records(id INTEGER PRIMARY KEY,value TEXT);PRAGMA user_version=1;INSERT INTO records VALUES(1,'committed')`); err != nil {
		t.Fatal(err)
	}
	if err = Seal(db); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path + ".published")
	if err != nil {
		t.Fatal(err)
	}
	for range 20 {
		if err = Seal(db); err != nil {
			t.Fatal(err)
		}
	}
	after, err := os.Stat(path + ".published")
	if err != nil || !before.ModTime().Equal(after.ModTime()) {
		t.Fatal("publication seal rewritten")
	}
	db.Close()
	if err = os.Rename(path, path+".lost"); err != nil {
		t.Fatal(err)
	}
	if _, err = Inspect(path); err == nil || os.IsNotExist(err) {
		t.Fatalf("lost published database looks new: %v", err)
	}
	if db, err := Open(path, false); err == nil {
		db.Close()
		t.Fatal("lost published database recreated")
	}
	if _, err = os.Stat(path); !os.IsNotExist(err) {
		t.Fatal("replacement database created")
	}
}

func TestPublicationRejectsInvalidMarkerAndEmptyDatabase(t *testing.T) {
	for _, raw := range []string{"wrong", "CXP-SQLITE-9\n", publicationMagic} {
		path := filepath.Join(t.TempDir(), "state.sqlite")
		if err := os.WriteFile(path+".published", []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, nil, 0600); err != nil {
			t.Fatal(err)
		}
		if db, err := Open(path, false); err == nil {
			db.Close()
			t.Fatal("invalid publication accepted")
		}
	}
}
