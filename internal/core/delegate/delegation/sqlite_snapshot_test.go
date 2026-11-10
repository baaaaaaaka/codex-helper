package delegation

// Full snapshots exist only as test-fixture builders and migration assertions.
import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"sort"
	"strings"
)

func LoadStore(path string) (Store, error) { return loadSQLiteStore(path) }
func SaveStore(path string, s Store) (bool, error) {
	if strings.HasSuffix(path, ".json") {
		raw, err := json.Marshal(s)
		if err != nil {
			return false, err
		}
		return true, os.WriteFile(path, raw, 0600)
	}
	return saveSQLiteStore(path, s)
}
func loadSQLiteStore(path string) (Store, error) {
	s, err := OpenWorkerDB(path)
	if err != nil {
		return Store{}, err
	}
	defer s.Close()
	out := newStore()
	err = s.transaction(context.Background(), false, func(tx *sql.Tx) error {

		{
			rows, err := tx.QueryContext(context.Background(), `SELECT `+recordsColumns+` FROM records`)
			if err != nil {
				return err
			}
			for rows.Next() {
				v, err := scanRecord(rows)
				if err != nil {
					rows.Close()
					return err
				}
				out.Records = append(out.Records, v)
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
		}

		{
			rows, err := tx.QueryContext(context.Background(), `SELECT `+routesColumns+` FROM routes`)
			if err != nil {
				return err
			}
			for rows.Next() {
				v, err := scanRoute(rows)
				if err != nil {
					rows.Close()
					return err
				}
				out.Routes[v.DelegationID] = v
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
		}

		{
			rows, err := tx.QueryContext(context.Background(), `SELECT `+remote_threadsColumns+` FROM remote_threads`)
			if err != nil {
				return err
			}
			for rows.Next() {
				v, err := scanRemoteThread(rows)
				if err != nil {
					rows.Close()
					return err
				}
				out.RemoteThreads[v.ThreadID] = v
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
		}

		{
			rows, err := tx.QueryContext(context.Background(), `SELECT `+executionsColumns+` FROM executions`)
			if err != nil {
				return err
			}
			for rows.Next() {
				v, err := scanExecutionFence(rows)
				if err != nil {
					rows.Close()
					return err
				}
				out.Executions[v.DelegationID] = v
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
		}

		{
			rows, err := tx.QueryContext(context.Background(), `SELECT `+outboxColumns+` FROM outbox`)
			if err != nil {
				return err
			}
			for rows.Next() {
				v, err := scanOutboxRecord(rows)
				if err != nil {
					rows.Close()
					return err
				}
				out.Outbox[v.RecordID] = v
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
		}

		{
			rows, err := tx.QueryContext(context.Background(), `SELECT `+inbox_cursorsColumns+` FROM inbox_cursors`)
			if err != nil {
				return err
			}
			for rows.Next() {
				v, err := scanInboxCursor(rows)
				if err != nil {
					rows.Close()
					return err
				}
				out.InboxCursors[v.ChatID] = v
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
		}

		{
			rows, err := tx.QueryContext(context.Background(), `SELECT `+inbox_backoffsColumns+` FROM inbox_backoffs`)
			if err != nil {
				return err
			}
			for rows.Next() {
				v, err := scanInboxBackoff(rows)
				if err != nil {
					rows.Close()
					return err
				}
				out.InboxBackoffs[v.ChatID] = v
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
		}

		for i := range out.Records {
			if err := readRecordChildrenTx(context.Background(), tx, &out.Records[i]); err != nil {
				return err
			}
		}
		return nil
	})
	sort.SliceStable(out.Records, func(i, j int) bool {
		a, b := parseRecordTime(out.Records[i].CreatedAt), parseRecordTime(out.Records[j].CreatedAt)
		if a.Equal(b) {
			return out.Records[i].RecordID < out.Records[j].RecordID
		}
		return a.Before(b)
	})
	return out, err
}

func saveSQLiteStore(path string, store Store) (bool, error) {
	s, err := OpenWorkerDB(path)
	if err != nil {
		return false, err
	}
	defer s.Close()
	changed := false
	err = s.transaction(context.Background(), true, func(tx *sql.Tx) error {

		{
			desired := map[string]bool{}
			for _, v := range store.Records {
				id := strings.TrimSpace(v.RecordID)
				if id == "" {
					continue
				}
				desired[id] = true
				n, err := putRecordTx(context.Background(), tx, v)
				if err != nil {
					return err
				}
				changed = changed || n > 0
			}
			rows, err := tx.QueryContext(context.Background(), `SELECT id FROM records`)
			if err != nil {
				return err
			}
			var absent []string
			for rows.Next() {
				var id string
				if err = rows.Scan(&id); err != nil {
					rows.Close()
					return err
				}
				if !desired[id] {
					absent = append(absent, id)
				}
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			for _, id := range absent {
				if _, err = tx.ExecContext(context.Background(), `DELETE FROM records WHERE id=?`, id); err != nil {
					return err
				}
				changed = true
			}
		}

		{
			desired := map[string]bool{}
			for _, v := range store.Routes {
				id := strings.TrimSpace(v.DelegationID)
				if id == "" {
					continue
				}
				desired[id] = true
				n, err := putRouteTx(context.Background(), tx, v)
				if err != nil {
					return err
				}
				changed = changed || n > 0
			}
			rows, err := tx.QueryContext(context.Background(), `SELECT id FROM routes`)
			if err != nil {
				return err
			}
			var absent []string
			for rows.Next() {
				var id string
				if err = rows.Scan(&id); err != nil {
					rows.Close()
					return err
				}
				if !desired[id] {
					absent = append(absent, id)
				}
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			for _, id := range absent {
				if _, err = tx.ExecContext(context.Background(), `DELETE FROM routes WHERE id=?`, id); err != nil {
					return err
				}
				changed = true
			}
		}

		{
			desired := map[string]bool{}
			for _, v := range store.RemoteThreads {
				id := strings.TrimSpace(v.ThreadID)
				if id == "" {
					continue
				}
				desired[id] = true
				n, err := putRemoteThreadTx(context.Background(), tx, v)
				if err != nil {
					return err
				}
				changed = changed || n > 0
			}
			rows, err := tx.QueryContext(context.Background(), `SELECT id FROM remote_threads`)
			if err != nil {
				return err
			}
			var absent []string
			for rows.Next() {
				var id string
				if err = rows.Scan(&id); err != nil {
					rows.Close()
					return err
				}
				if !desired[id] {
					absent = append(absent, id)
				}
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			for _, id := range absent {
				if _, err = tx.ExecContext(context.Background(), `DELETE FROM remote_threads WHERE id=?`, id); err != nil {
					return err
				}
				changed = true
			}
		}

		{
			desired := map[string]bool{}
			for _, v := range store.Executions {
				id := strings.TrimSpace(v.DelegationID)
				if id == "" {
					continue
				}
				desired[id] = true
				n, err := putExecutionFenceTx(context.Background(), tx, v)
				if err != nil {
					return err
				}
				changed = changed || n > 0
			}
			rows, err := tx.QueryContext(context.Background(), `SELECT id FROM executions`)
			if err != nil {
				return err
			}
			var absent []string
			for rows.Next() {
				var id string
				if err = rows.Scan(&id); err != nil {
					rows.Close()
					return err
				}
				if !desired[id] {
					absent = append(absent, id)
				}
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			for _, id := range absent {
				if _, err = tx.ExecContext(context.Background(), `DELETE FROM executions WHERE id=?`, id); err != nil {
					return err
				}
				changed = true
			}
		}

		{
			desired := map[string]bool{}
			for _, v := range store.Outbox {
				id := strings.TrimSpace(v.RecordID)
				if id == "" {
					continue
				}
				desired[id] = true
				n, err := putOutboxRecordTx(context.Background(), tx, v)
				if err != nil {
					return err
				}
				changed = changed || n > 0
			}
			rows, err := tx.QueryContext(context.Background(), `SELECT id FROM outbox`)
			if err != nil {
				return err
			}
			var absent []string
			for rows.Next() {
				var id string
				if err = rows.Scan(&id); err != nil {
					rows.Close()
					return err
				}
				if !desired[id] {
					absent = append(absent, id)
				}
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			for _, id := range absent {
				if _, err = tx.ExecContext(context.Background(), `DELETE FROM outbox WHERE id=?`, id); err != nil {
					return err
				}
				changed = true
			}
		}

		{
			desired := map[string]bool{}
			for _, v := range store.InboxCursors {
				id := strings.TrimSpace(v.ChatID)
				if id == "" {
					continue
				}
				desired[id] = true
				n, err := putInboxCursorTx(context.Background(), tx, v)
				if err != nil {
					return err
				}
				changed = changed || n > 0
			}
			rows, err := tx.QueryContext(context.Background(), `SELECT id FROM inbox_cursors`)
			if err != nil {
				return err
			}
			var absent []string
			for rows.Next() {
				var id string
				if err = rows.Scan(&id); err != nil {
					rows.Close()
					return err
				}
				if !desired[id] {
					absent = append(absent, id)
				}
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			for _, id := range absent {
				if _, err = tx.ExecContext(context.Background(), `DELETE FROM inbox_cursors WHERE id=?`, id); err != nil {
					return err
				}
				changed = true
			}
		}

		{
			desired := map[string]bool{}
			for _, v := range store.InboxBackoffs {
				id := strings.TrimSpace(v.ChatID)
				if id == "" {
					continue
				}
				desired[id] = true
				n, err := putInboxBackoffTx(context.Background(), tx, v)
				if err != nil {
					return err
				}
				changed = changed || n > 0
			}
			rows, err := tx.QueryContext(context.Background(), `SELECT id FROM inbox_backoffs`)
			if err != nil {
				return err
			}
			var absent []string
			for rows.Next() {
				var id string
				if err = rows.Scan(&id); err != nil {
					rows.Close()
					return err
				}
				if !desired[id] {
					absent = append(absent, id)
				}
			}
			err = rows.Err()
			rows.Close()
			if err != nil {
				return err
			}
			for _, id := range absent {
				if _, err = tx.ExecContext(context.Background(), `DELETE FROM inbox_backoffs WHERE id=?`, id); err != nil {
					return err
				}
				changed = true
			}
		}

		return nil
	})
	return changed, err
}
