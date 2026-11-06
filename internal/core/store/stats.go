package store

import "context"

// ConvThreads lists the Codex threads of a conversation, oldest first: the
// threads its turns ran on, then the threads of rollouts mirrored into it
// (`helper stats` aggregates their usage, F-STA-1). Read-only.
func (s *Store) ConvThreads(convID int64) ([]string, error) {
	var out []string
	seen := map[string]bool{}
	for _, q := range []string{
		`SELECT thread_id FROM turns WHERE conv_id = ? AND thread_id IS NOT NULL AND thread_id != '' GROUP BY thread_id ORDER BY MIN(id)`,
		`SELECT thread_id FROM transcript_sources WHERE conv_id = ? AND thread_id IS NOT NULL AND thread_id != '' GROUP BY thread_id ORDER BY MIN(id)`,
	} {
		rows, err := s.r.QueryContext(context.Background(), q, convID)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return nil, err
			}
			if !seen[id] {
				seen[id] = true
				out = append(out, id)
			}
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}
