package skills

import (
	"database/sql"
	"slices"
)

var skillReportSchema = []string{
	`CREATE TABLE migration_report(id INTEGER PRIMARY KEY CHECK(id=1),status TEXT NOT NULL,started_at TEXT NOT NULL,ended_at TEXT NOT NULL,old_root TEXT NOT NULL,new_root TEXT NOT NULL,dry_run INTEGER NOT NULL,changed INTEGER NOT NULL,failed INTEGER NOT NULL)`,
	`CREATE TABLE migration_results(position INTEGER PRIMARY KEY,source_id TEXT NOT NULL,name TEXT NOT NULL,kind TEXT NOT NULL,status TEXT NOT NULL,message TEXT NOT NULL)`,
	`CREATE TABLE migration_skills(source_position INTEGER NOT NULL REFERENCES migration_results(position) ON DELETE CASCADE,position INTEGER NOT NULL,export_name TEXT NOT NULL,old_path TEXT NOT NULL,new_path TEXT NOT NULL,status TEXT NOT NULL,message TEXT NOT NULL,PRIMARY KEY(source_position,position))`,
}

func insertSkillReport(tx *sql.Tx, v MigrationReport) error {
	old, err := readSkillReport(tx)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	same := err == nil && len(old.Results) == len(v.Results)
	if same {
		for i, a := range old.Results {
			b := v.Results[i]
			if a.ID != b.ID || a.Name != b.Name || a.Kind != b.Kind || a.Status != b.Status || a.Message != b.Message || !slices.Equal(a.Skills, b.Skills) {
				same = false
				break
			}
		}
	}

	if _, err := tx.Exec(`INSERT INTO migration_report(id,status,started_at,ended_at,old_root,new_root,dry_run,changed,failed) VALUES(1,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET status=excluded.status,started_at=excluded.started_at,ended_at=excluded.ended_at,old_root=excluded.old_root,new_root=excluded.new_root,dry_run=excluded.dry_run,changed=excluded.changed,failed=excluded.failed
WHERE (status,started_at,ended_at,old_root,new_root,dry_run,changed,failed) IS NOT (excluded.status,excluded.started_at,excluded.ended_at,excluded.old_root,excluded.new_root,excluded.dry_run,excluded.changed,excluded.failed)`, v.Status, skillTime(v.StartedAt), skillTime(v.EndedAt), v.OldRoot, v.NewRoot, v.DryRun, v.Changed, v.Failed); err != nil {
		return err
	}
	if same {
		return nil
	}
	if _, err := tx.Exec(`DELETE FROM migration_results`); err != nil {
		return err
	}
	results, err := tx.Prepare(`INSERT INTO migration_results(position,source_id,name,kind,status,message) VALUES(?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer results.Close()
	skills, err := tx.Prepare(`INSERT INTO migration_skills(source_position,position,export_name,old_path,new_path,status,message) VALUES(?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer skills.Close()
	for i, r := range v.Results {
		if _, err = results.Exec(i, r.ID, r.Name, r.Kind, r.Status, r.Message); err != nil {
			return err
		}
		for j, sk := range r.Skills {
			if _, err = skills.Exec(i, j, sk.ExportName, sk.OldPath, sk.NewPath, sk.Status, sk.Message); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) SaveMigrationReport(v MigrationReport) error {
	return s.transaction(true, func(tx *sql.Tx) error { return insertSkillReport(tx, v) })
}

func readSkillReport(tx *sql.Tx) (MigrationReport, error) {
	var v MigrationReport
	err := func() error {
		var started, ended string
		err := tx.QueryRow(`SELECT status,started_at,ended_at,old_root,new_root,dry_run,changed,failed FROM migration_report WHERE id=1`).Scan(&v.Status, &started, &ended, &v.OldRoot, &v.NewRoot, &v.DryRun, &v.Changed, &v.Failed)
		if err != nil {
			return err
		}
		v.StartedAt, err = parseSkillTime(started)
		if err != nil {
			return err
		}
		v.EndedAt, err = parseSkillTime(ended)
		if err != nil {
			return err
		}
		rows, err := tx.Query(`SELECT source_id,name,kind,status,message FROM migration_results ORDER BY position`)
		if err != nil {
			return err
		}
		for rows.Next() {
			var r MigrationSourceResult
			if err = rows.Scan(&r.ID, &r.Name, &r.Kind, &r.Status, &r.Message); err != nil {
				rows.Close()
				return err
			}
			v.Results = append(v.Results, r)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return err
		}
		for i := range v.Results {
			rows, err := tx.Query(`SELECT export_name,old_path,new_path,status,message FROM migration_skills WHERE source_position=? ORDER BY position`, i)
			if err != nil {
				return err
			}
			for rows.Next() {
				var sk MigrationSkillResult
				if err = rows.Scan(&sk.ExportName, &sk.OldPath, &sk.NewPath, &sk.Status, &sk.Message); err != nil {
					rows.Close()
					return err
				}
				v.Results[i].Skills = append(v.Results[i].Skills, sk)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
		}
		return nil
	}()
	return v, err
}
func (s *Store) MigrationReport() (MigrationReport, bool, error) {
	var v MigrationReport
	err := s.transaction(false, func(tx *sql.Tx) error { var err error; v, err = readSkillReport(tx); return err })
	if err == sql.ErrNoRows {
		return v, false, nil
	}
	return v, err == nil, err
}
