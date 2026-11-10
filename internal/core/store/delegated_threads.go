package store

func (t *Tx) DelegatedThreads() ([]string, error) {
	rows, err := t.rows(`SELECT thread_id FROM delegated_threads ORDER BY seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// AddDelegatedThread keeps the existing newest-1000 rule. Repeated discovery
// neither writes nor refreshes a thread's position.
func (t *Tx) AddDelegatedThread(id string) error {
	res, err := t.exec(`INSERT INTO delegated_threads(thread_id) VALUES(?) ON CONFLICT(thread_id) DO NOTHING`, id)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil || n == 0 {
		return err
	}
	_, err = t.exec(`DELETE FROM delegated_threads WHERE seq < COALESCE((SELECT seq FROM delegated_threads ORDER BY seq DESC LIMIT 1 OFFSET 999),0)`)
	return err
}
