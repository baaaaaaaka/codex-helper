package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

func migrateAttachments(t *Tx) error {
	rows, err := t.rows(`SELECT inbound_id,idx,ref,local_path,state FROM inbound_files ORDER BY inbound_id,idx`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var inbound int64
		var idx int
		var raw, state string
		var path sql.NullString
		if err := rows.Scan(&inbound, &idx, &raw, &path, &state); err != nil {
			return err
		}
		var old struct {
			FileReference
			Q *QuoteReference `json:"q"`
		}
		if strings.HasPrefix(strings.TrimSpace(raw), "{") {
			if err := legacyObject(raw, &old); err != nil {
				return fmt.Errorf("attachment %d/%d: %w", inbound, idx, err)
			}
		} else {
			old.Ref = raw
		} // pre-engine scalar references, never a live fallback
		if err := migrateDriveReference(&old.FileReference); err != nil {
			return fmt.Errorf("attachment %d/%d: %w", inbound, idx, err)
		}
		var q QuoteReference
		if old.Q != nil {
			q = *old.Q
		}
		if _, err := t.exec(`UPDATE inbound_files SET ref=?,ref_kind=?,ref_name=?,ref_size=?,ref_content_type=?,ref_attachment_id=?,ref_url=?,quote_n=?,quote_id=?,quote_from=?,quote_time=?,quote_text=?,quote_forwarded=? WHERE inbound_id=? AND idx=?`, old.Ref, old.Kind, old.Name, old.Size, old.ContentType, old.AttachmentID, old.URL, q.N, q.ID, q.From, q.Time, q.Text, q.Fwd, inbound, idx); err != nil {
			return err
		}
		if old.Q != nil {
			if q.N <= 0 {
				return fmt.Errorf("attachment %d/%d: invalid quote position", inbound, idx)
			}
			if state == "ready" && path.String != "" {
				file, err := os.Open(path.String)
				if err != nil && !errors.Is(err, os.ErrNotExist) {
					return err
				}
				if err == nil {
					var resolved QuoteText
					d := json.NewDecoder(file)
					err = d.Decode(&resolved)
					if err == nil {
						err = legacyEnd(d)
					}
					closeErr := file.Close()
					if err != nil {
						return fmt.Errorf("quote %d/%d: %w", inbound, idx, err)
					}
					if closeErr != nil {
						return closeErr
					}
					if err := t.SetResolvedQuote(inbound, idx, resolved); err != nil {
						return err
					}
				}
			}
			// Old quote cache files are migration inputs only. Never read them again
			// after commit, even if a residual file is subsequently changed.
			if _, err := t.exec(`UPDATE inbound_files SET local_path=NULL WHERE inbound_id=? AND idx=?`, inbound, idx); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()
	out, err := t.rows(`SELECT outbox_id,ref FROM outbox_files WHERE ref IS NOT NULL ORDER BY outbox_id`)
	if err != nil {
		return err
	}
	defer out.Close()
	for out.Next() {
		var id int64
		var raw string
		if err := out.Scan(&id, &raw); err != nil {
			return err
		}
		n := FileReference{Ref: raw}
		if strings.HasPrefix(strings.TrimSpace(raw), "{") {
			if err := legacyObject(raw, &n); err != nil {
				return fmt.Errorf("upload %d: %w", id, err)
			}
		}
		if err := migrateDriveReference(&n); err != nil {
			return fmt.Errorf("upload %d: %w", id, err)
		}
		if _, err := t.exec(`UPDATE outbox_files SET ref=?,ref_kind=?,ref_name=?,ref_size=?,ref_content_type=?,ref_attachment_id=?,ref_url=? WHERE outbox_id=?`, n.Ref, n.Kind, n.Name, n.Size, n.ContentType, n.AttachmentID, n.URL, id); err != nil {
			return err
		}
	}
	return out.Err()
}

// The old adapter invented a composite drive:JSON handle. It is not a Graph
// wire payload. Decode it only here, before publishing the native schema.
func migrateDriveReference(n *FileReference) error {
	raw, ok := strings.CutPrefix(n.Ref, "drive:")
	if !ok {
		return nil
	}
	var d struct {
		Item       string `json:"item"`
		Attachment string `json:"att"`
		URL        string `json:"url"`
		Name       string `json:"name"`
	}
	if err := legacyObject(raw, &d); err != nil {
		return err
	}
	if d.Item == "" || d.Attachment == "" || d.URL == "" {
		return fmt.Errorf("incomplete legacy uploaded reference")
	}
	n.Ref = "drive:" + d.Item
	n.AttachmentID = d.Attachment
	n.URL = d.URL
	if n.Name == "" {
		n.Name = d.Name
	}
	return nil
}
