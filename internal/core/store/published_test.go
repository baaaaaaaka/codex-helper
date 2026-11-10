package store

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestPublishedCoreStoreNeverInitializesLostSchema(t *testing.T) {
	for _, fixture := range []string{"missing", "empty", "short", "schema-zero"} {
		t.Run(fixture, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "core.sqlite")
			switch fixture {
			case "empty", "short":
				var raw []byte
				if fixture == "short" {
					raw = []byte("broken")
				}
				mustT(t, os.WriteFile(path, raw, 0600))
			case "schema-zero":
				db, err := sql.Open("sqlite", path)
				mustT(t, err)
				_, err = db.Exec(fmt.Sprintf("PRAGMA application_id=%d", ApplicationID))
				mustT(t, err)
				mustT(t, db.Close())
			}
			before, _ := os.ReadFile(path)
			st, err := OpenWith(path, Options{RequireExisting: true})
			if err == nil {
				st.Close()
				t.Fatal("published state was initialized")
			}
			if (fixture == "empty" || fixture == "schema-zero") && !errors.Is(err, ErrUnknownVersion) {
				t.Fatalf("unexpected admission failure: %v", err)
			}
			after, readErr := os.ReadFile(path)
			if fixture == "missing" {
				if !os.IsNotExist(readErr) {
					t.Fatal("missing database was recreated", readErr)
				}
			} else if readErr != nil || !bytes.Equal(before, after) {
				t.Fatal("rejected database was modified", readErr)
			}
		})
	}
}
