package teams

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
)

func migrateFontMarker(dir string) error {
	legacy := filepath.Join(dir, "font.json")
	info, err := os.Lstat(legacy)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("legacy math font marker is not regular: %s", legacy)
	}
	f, err := os.Open(legacy)
	if err != nil {
		return err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	var m *fontMarker
	if err = d.Decode(&m); err != nil {
		return err
	}
	if m == nil {
		return fmt.Errorf("null math font marker")
	}
	if _, err = d.Token(); err != io.EOF {
		return fmt.Errorf("trailing math font marker data")
	}
	tmp, err := os.CreateTemp(dir, ".font-*.sqlite")
	if err != nil {
		return err
	}
	path := tmp.Name()
	defer os.Remove(path)
	if err = tmp.Close(); err != nil {
		return err
	}
	db, err := sqlitestate.Open(path, false)
	if err != nil {
		return err
	}
	err = createFontMarker(db, *m, legacy)
	closeErr := db.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err = os.Link(path, filepath.Join(dir, fontDatabaseName)); err != nil {
		if os.IsExist(err) {
			return nil
		}
		return err
	}
	f.Close()
	if _, err = os.Lstat(legacy + ".migrated"); err == nil {
		return fmt.Errorf("math font migration committed; archive exists: %s", legacy)
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.Rename(legacy, legacy+".migrated")
}
