package skills

import (
	"database/sql"
	"slices"
	"time"
)

var skillReviewSchema = []string{
	`CREATE TABLE push_reviews(chat TEXT PRIMARY KEY,name TEXT NOT NULL,direct INTEGER NOT NULL CHECK(direct IN(0,1)),created_at TEXT NOT NULL,expires_at TEXT NOT NULL)`,
	`CREATE TABLE review_sources(chat TEXT NOT NULL REFERENCES push_reviews(chat) ON DELETE CASCADE,position INTEGER NOT NULL,source_id TEXT NOT NULL,source_name TEXT NOT NULL,remote_url TEXT NOT NULL,ref TEXT NOT NULL,base_commit TEXT NOT NULL,review_refspec TEXT NOT NULL,PRIMARY KEY(chat,source_id),UNIQUE(chat,position))`,
	`CREATE TABLE review_changes(chat TEXT NOT NULL,source_id TEXT NOT NULL,position INTEGER NOT NULL,kind TEXT NOT NULL,rel_path TEXT NOT NULL,source_path TEXT NOT NULL,commit_id TEXT NOT NULL,old_sha256 TEXT NOT NULL,new_sha256 TEXT NOT NULL,old_mode INTEGER NOT NULL,new_mode INTEGER NOT NULL,size INTEGER NOT NULL,PRIMARY KEY(chat,source_id,position),FOREIGN KEY(chat,source_id) REFERENCES review_sources(chat,source_id) ON DELETE CASCADE)`,
}

func readSkillReview(tx *sql.Tx, chat string) (PushReview, error) {
	var v PushReview
	var created, expires string
	err := tx.QueryRow(`SELECT chat,name,direct,created_at,expires_at FROM push_reviews WHERE chat=?`, chat).Scan(&v.Chat, &v.Name, &v.Direct, &created, &expires)
	if err != nil {
		return v, err
	}
	v.CreatedAt, err = parseSkillTime(created)
	if err != nil {
		return v, err
	}
	v.ExpiresAt, err = parseSkillTime(expires)
	if err != nil {
		return v, err
	}
	rows, err := tx.Query(`SELECT source_id,source_name,remote_url,ref,base_commit,review_refspec FROM review_sources WHERE chat=? ORDER BY position`, chat)
	if err != nil {
		return v, err
	}
	for rows.Next() {
		var s ReviewSource
		if err = rows.Scan(&s.SourceID, &s.SourceName, &s.RemoteURL, &s.Ref, &s.BaseCommit, &s.ReviewRefSpec); err != nil {
			rows.Close()
			return v, err
		}
		v.Sources = append(v.Sources, s)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return v, err
	}
	for i := range v.Sources {
		rows, err := tx.Query(`SELECT kind,rel_path,source_path,commit_id,old_sha256,new_sha256,old_mode,new_mode,size FROM review_changes WHERE chat=? AND source_id=? ORDER BY position`, chat, v.Sources[i].SourceID)
		if err != nil {
			return v, err
		}
		for rows.Next() {
			var c ReviewChange
			if err = rows.Scan(&c.Kind, &c.RelPath, &c.SourcePath, &c.Commit, &c.OldSHA256, &c.NewSHA256, &c.OldMode, &c.NewMode, &c.Size); err != nil {
				rows.Close()
				return v, err
			}
			v.Sources[i].Changes = append(v.Sources[i].Changes, c)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return v, err
		}
	}
	return v, nil
}

func (s *Store) Review(chat string, now time.Time) (PushReview, bool, error) {
	var v PushReview
	err := s.transaction(false, func(tx *sql.Tx) error { var err error; v, err = readSkillReview(tx, chat); return err })
	if err == sql.ErrNoRows {
		return v, false, nil
	}
	if err != nil {
		return v, false, err
	}
	if !v.ExpiresAt.IsZero() && now.After(v.ExpiresAt) {
		// Delete only the revision just observed; a concurrent new review is
		// not erased by a reader expiring an old one.
		_, err = s.db.Exec(`DELETE FROM push_reviews WHERE chat=? AND created_at=? AND expires_at=?`, chat, skillTime(v.CreatedAt), skillTime(v.ExpiresAt))
		return PushReview{}, false, err
	}
	return v, true, nil
}

func (s *Store) SaveReview(v PushReview) error {
	return s.transaction(true, func(tx *sql.Tx) error {
		old, err := readSkillReview(tx, v.Chat)
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		same := old.Chat == v.Chat && old.Name == v.Name && old.Direct == v.Direct && old.CreatedAt.Equal(v.CreatedAt) && old.ExpiresAt.Equal(v.ExpiresAt) && len(old.Sources) == len(v.Sources)
		if same {
			for i, a := range old.Sources {
				b := v.Sources[i]
				if a.SourceID != b.SourceID || a.SourceName != b.SourceName || a.RemoteURL != b.RemoteURL || a.Ref != b.Ref || a.BaseCommit != b.BaseCommit || a.ReviewRefSpec != b.ReviewRefSpec || !slices.Equal(a.Changes, b.Changes) {
					same = false
					break
				}
			}
		}
		if same {
			return nil
		}
		if _, err = tx.Exec(`DELETE FROM push_reviews WHERE chat=?`, v.Chat); err != nil {
			return err
		}
		return insertSkillReview(tx, v)
	})
}

func insertSkillReview(tx *sql.Tx, v PushReview) error {
	if _, err := tx.Exec(`INSERT INTO push_reviews(chat,name,direct,created_at,expires_at) VALUES(?,?,?,?,?)`, v.Chat, v.Name, v.Direct, skillTime(v.CreatedAt), skillTime(v.ExpiresAt)); err != nil {
		return err
	}
	ss, err := tx.Prepare(`INSERT INTO review_sources(chat,position,source_id,source_name,remote_url,ref,base_commit,review_refspec) VALUES(?,?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer ss.Close()
	cs, err := tx.Prepare(`INSERT INTO review_changes(chat,source_id,position,kind,rel_path,source_path,commit_id,old_sha256,new_sha256,old_mode,new_mode,size) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		return err
	}
	defer cs.Close()
	for i, s := range v.Sources {
		if _, err = ss.Exec(v.Chat, i, s.SourceID, s.SourceName, s.RemoteURL, s.Ref, s.BaseCommit, s.ReviewRefSpec); err != nil {
			return err
		}
		for j, c := range s.Changes {
			if _, err = cs.Exec(v.Chat, s.SourceID, j, c.Kind, c.RelPath, c.SourcePath, c.Commit, c.OldSHA256, c.NewSHA256, c.OldMode, c.NewMode, c.Size); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Store) DeleteReview(chat string) error {
	_, err := s.db.Exec(`DELETE FROM push_reviews WHERE chat=?`, chat)
	return err
}
func (s *Store) DeleteReviewSource(chat, id string) error {
	return s.transaction(true, func(tx *sql.Tx) error {
		if _, err := tx.Exec(`DELETE FROM review_sources WHERE chat=? AND source_id=?`, chat, id); err != nil {
			return err
		}
		_, err := tx.Exec(`DELETE FROM push_reviews WHERE chat=? AND NOT EXISTS(SELECT 1 FROM review_sources WHERE chat=?)`, chat, chat)
		return err
	})
}
