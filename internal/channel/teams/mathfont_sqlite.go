package teams

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

const fontDatabaseName = "font.sqlite"
const fontDDL = `CREATE TABLE font(id INTEGER PRIMARY KEY CHECK(id=1),kind TEXT NOT NULL,font_id TEXT NOT NULL,version TEXT NOT NULL,filename TEXT NOT NULL,size INTEGER NOT NULL CHECK(size>=0),sha256 TEXT NOT NULL,installed_at TEXT NOT NULL); CREATE TABLE font_imports(source_path TEXT PRIMARY KEY)`

func readFontMarker(dir string) (fontMarker, error) {
	path := filepath.Join(dir, fontDatabaseName)
	if _, err := os.Lstat(path); os.IsNotExist(err) {
		if err = migrateFontMarker(dir); err != nil {
			return fontMarker{}, err
		}
	} else if err != nil {
		return fontMarker{}, err
	}
	db, err := sqlitestate.Open(path, true)
	if err != nil {
		return fontMarker{}, err
	}
	defer db.Close()
	var v int
	if err = db.QueryRow(`PRAGMA user_version`).Scan(&v); err != nil {
		return fontMarker{}, err
	}
	if v != 1 {
		return fontMarker{}, fmt.Errorf("unsupported math font database version %d", v)
	}
	var m fontMarker
	err = db.QueryRow(`SELECT kind,font_id,version,filename,size,sha256,installed_at FROM font WHERE id=1`).Scan(&m.Kind, &m.ID, &m.Version, &m.Filename, &m.Size, &m.SHA256, &m.Installed)
	return m, err
}
func writeFontMarker(dir string, m fontMarker) error {
	db, err := sqlitestate.Open(filepath.Join(dir, fontDatabaseName), false)
	if err != nil {
		return err
	}
	defer db.Close()
	return createFontMarker(db, m, "")
}
func createFontMarker(db *sql.DB, m fontMarker, source string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.Exec(fontDDL); err != nil {
		return err
	}
	if _, err = tx.Exec(`INSERT INTO font VALUES(1,?,?,?,?,?,?,?)`, m.Kind, m.ID, m.Version, m.Filename, m.Size, m.SHA256, m.Installed); err != nil {
		return err
	}
	if source != "" {
		if _, err = tx.Exec(`INSERT INTO font_imports VALUES(?)`, source); err != nil {
			return err
		}
	}
	if _, err = tx.Exec(`PRAGMA user_version=1`); err != nil {
		return err
	}
	return tx.Commit()
}
