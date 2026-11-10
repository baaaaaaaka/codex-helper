package skills

import (
	"database/sql"
	"encoding/json"
	"os"
	"path/filepath"
)

// Whole-store mutation exists only for old test fixtures. Production uses
// record operations and never rewrites a subscription collection.
func (s *Store) Update(fn func(*Config, *State) error) error {
	return s.transaction(true, func(tx *sql.Tx) error {
		cfg, err := readSkillConfig(tx)
		if err != nil {
			return err
		}
		st, err := readSkillStates(tx)
		if err != nil {
			return err
		}
		if err = fn(&cfg, &st); err != nil {
			return err
		}
		if _, err = tx.Exec(`DELETE FROM skill_sources`); err != nil {
			return err
		}
		if _, err = tx.Exec(`DELETE FROM skill_states`); err != nil {
			return err
		}
		for _, v := range cfg.Sources {
			if _, err = tx.Exec(skill_sourcesPut, skill_sourcesArgs(v)...); err != nil {
				return err
			}
		}
		for _, v := range st.Sources {
			if err = putSkillState(tx, v); err != nil {
				return err
			}
		}
		return nil
	})
}
func (s *Store) UpdateState(fn func(*State) error) error {
	return s.Update(func(_ *Config, st *State) error { return fn(st) })
}
func writeJSONFile(path string, v any) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0600)
}
