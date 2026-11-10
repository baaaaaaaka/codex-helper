package store

// Legacy decoders belong to the schema upgrade, never the live engine. Every
// conversion and deletion shares the DDL/version transaction in Store.migrate.
import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"strings"
)

func migrateEngineState(tx *sql.Tx) error {
	// Materialize only keys. Decode one legacy value/array member at a time, not
	// all blobs plus a full Go mirror of the target database.
	rows, err := tx.Query(`SELECT key FROM meta WHERE key IN ('engine_notify','engine_imports','engine_deleg_threads','legacy_final_checks','legacy_history_checks','engine_dashboard_view','engine_update_request','engine_update_pending','engine_codex_upgrade','engine_restart_request','engine_restart_confirm') OR key GLOB 'engine_freeze:*' OR key GLOB 'engine_usage:*' OR key GLOB 'engine_cli:*' OR key GLOB 'engine_srcinfo:*' OR key GLOB 'engine_model_fork_*' ORDER BY key`)
	if err != nil {
		return err
	}
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			rows.Close()
			return err
		}
		keys = append(keys, key)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	t := &Tx{tx: tx}
	for _, key := range keys {
		var raw string
		if err := tx.QueryRow(`SELECT value FROM meta WHERE key=?`, key).Scan(&raw); err != nil {
			return err
		}
		if err := migrateEngineValue(t, key, raw); err != nil {
			return fmt.Errorf("%s: %w", key, err)
		}
		if _, err := tx.Exec(`DELETE FROM meta WHERE key=?`, key); err != nil {
			return err
		}
	}
	// Unknown old state is not silently retained as an untyped blob.
	rows, err = tx.Query(`SELECT key FROM meta`)
	if err != nil {
		return err
	}
	for rows.Next() {
		var key string
		if err = rows.Scan(&key); err != nil {
			rows.Close()
			return err
		}
		if !scalarMetaKey(key) {
			rows.Close()
			return fmt.Errorf("unmapped legacy meta key %q", key)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	return migrateAttachments(t)
}

func migrateEngineValue(t *Tx, key, raw string) error {
	// These unused counters are intentionally retired; stats reads rollouts.
	if strings.HasPrefix(key, "engine_usage:") || raw == "" {
		return nil
	}
	if strings.HasPrefix(key, "engine_freeze:") {
		conv, err := strconv.ParseInt(strings.TrimPrefix(key, "engine_freeze:"), 10, 64)
		if err != nil {
			return err
		}
		done := strings.HasPrefix(raw, "done:")
		at, err := strconv.ParseInt(strings.TrimPrefix(raw, "done:"), 10, 64)
		if err != nil {
			return err
		}
		return t.SetFreezeNotice(conv, FreezeNotice{ParkedAt: at, Done: done})
	}

	switch key {
	case "engine_restart_confirm":
		parts := strings.SplitN(raw, ":", 3)
		if len(parts) != 3 {
			return fmt.Errorf("invalid restart confirmation")
		}
		conv, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return err
		}
		inbound, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			return err
		}
		return t.SetRestartConfirmation(RestartConfirmation{Conv: conv, Inbound: inbound, Quote: parts[2]})
	case "engine_notify":
		stmt, err := t.tx.Prepare(`INSERT INTO pending_notifications(id,turn_id,conv_id,key,kind,text,request,attempted) VALUES(?,?,?,?,?,?,?,?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		return legacyArray(raw, func(d *json.Decoder) error {
			var n PendingNotification
			if err := d.Decode(&n); err != nil {
				return err
			}
			if n.ID() == "" {
				return fmt.Errorf("empty notification key")
			}
			_, err := stmt.Exec(n.ID(), n.Turn, n.Conv, n.Key, n.Kind, n.Text, n.Request, n.Attempted)
			return err
		})
	case "engine_imports":
		stmt, err := t.tx.Prepare(`INSERT INTO import_jobs(id,conv_id,kind,thread,path,rewind,keep_teams,namespace,applied,source_id,until_offset,posted,resumed,reached) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		return legacyArray(raw, func(d *json.Decoder) error {
			var n ImportJob
			if err := d.Decode(&n); err != nil {
				return err
			}
			if n.ID <= 0 {
				return fmt.Errorf("invalid import id %d", n.ID)
			}
			_, err := stmt.Exec(n.ID, n.Conv, n.Kind, n.Thread, n.Path, n.Rewind, n.KeepTeams, n.NS, n.Applied, n.Src, n.Until, n.Posted, n.Resumed, n.Reached)
			return err
		})
	case "engine_deleg_threads":
		return legacyArray(raw, func(d *json.Decoder) error {
			var id string
			if err := d.Decode(&id); err != nil {
				return err
			}
			_, err := t.exec(`INSERT INTO delegated_threads(thread_id) VALUES(?)`, id)
			return err
		})
	case "legacy_final_checks":
		stmt, err := t.tx.Prepare(`INSERT INTO answer_checks(key,conv_id,legacy_id,ext_msg_id,created,body,reply_to,found) VALUES(?,?,?,?,?,?,?,?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		return legacyArray(raw, func(d *json.Decoder) error {
			var n LegacyFinalCheck
			if err := d.Decode(&n); err != nil {
				return err
			}
			if n.Key == "" {
				return fmt.Errorf("empty answer key")
			}
			_, err := stmt.Exec(n.Key, n.ConvID, n.LegacyID, n.ExtMsgID, n.Created, n.Body, n.ReplyTo, n.Found)
			return err
		})
	case "legacy_history_checks":
		stmt, err := t.tx.Prepare(`INSERT INTO history_checks(path,conv_id,from_offset,from_line,to_offset,end_offset,done,resume,outcome) VALUES(?,?,?,?,?,?,?,?,?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		return legacyArray(raw, func(d *json.Decoder) error {
			var n HistoryCheck
			if err := d.Decode(&n); err != nil {
				return err
			}
			if n.Path == "" {
				return fmt.Errorf("empty history path")
			}
			_, err := stmt.Exec(n.Path, n.ConvID, n.From, n.FromLine, n.To, n.End, n.Done, n.Resume, n.Outcome)
			return err
		})
	case "engine_dashboard_view":
		var n DashboardView
		if err := legacyObject(raw, &n); err != nil {
			return err
		}
		return t.SetDashboardView(&n)
	case "engine_update_request":
		var n UpdateRequest
		if err := legacyObject(raw, &n); err != nil {
			return err
		}
		return t.SetUpdateRequest(n)
	case "engine_update_pending":
		var n UpdateRequest
		if err := legacyObject(raw, &n); err != nil {
			return err
		}
		return t.SetUpdatePending(n)
	case "engine_restart_request":
		var n RestartRequest
		if err := legacyObject(raw, &n); err != nil {
			return err
		}
		return t.SetRestartRequest(n)
	case "engine_codex_upgrade":
		var n CodexRequest
		if err := legacyObject(raw, &n); err != nil {
			return err
		}
		return t.SetCodexRequest(n)
	}
	switch {
	case strings.HasPrefix(key, "engine_cli:"):
		id := strings.TrimPrefix(key, "engine_cli:")
		if id == "" {
			return fmt.Errorf("empty request id")
		}
		var n CLIProgress
		if err := legacyObject(raw, &n); err != nil {
			return err
		}
		return t.SetCLIProgress(id, n)
	case strings.HasPrefix(key, "engine_model_fork_"):
		id, err := strconv.ParseInt(strings.TrimPrefix(key, "engine_model_fork_"), 10, 64)
		if err != nil {
			return err
		}
		var n ModelForkNote
		if err := legacyObject(raw, &n); err != nil {
			return err
		}
		return t.SetModelForkNote(id, n)
	case strings.HasPrefix(key, "engine_srcinfo:"):
		id, err := strconv.ParseInt(strings.TrimPrefix(key, "engine_srcinfo:"), 10, 64)
		if err != nil {
			return err
		}
		var n struct {
			Model  string
			Effort string
			Tier   string
			Goal   *struct {
				Objective string
				Status    string
			}
		}
		if err := legacyObject(raw, &n); err != nil {
			return err
		}
		info := SourceInfo{Model: n.Model, Effort: n.Effort, Tier: n.Tier, HasGoal: n.Goal != nil}
		if n.Goal != nil {
			info.GoalObjective = n.Goal.Objective
			info.GoalStatus = n.Goal.Status
		}
		return t.SetSourceInfo(id, info)
	}
	return fmt.Errorf("unhandled legacy key")
}

// legacyArray streams members and rejects malformed delimiters/trailing input.
// It is limited to this one schema conversion, not a runtime persistence API.
func legacyArray(raw string, insert func(*json.Decoder) error) error {
	d := json.NewDecoder(strings.NewReader(raw))
	token, err := d.Token()
	if err != nil {
		return err
	}
	if token != nil {
		if token != json.Delim('[') {
			return fmt.Errorf("expected array")
		}
		for d.More() {
			if err := insert(d); err != nil {
				return err
			}
		}
		if _, err := d.Token(); err != nil {
			return err
		}
	}
	return legacyEnd(d)
}
func legacyObject(raw string, value any) error {
	d := json.NewDecoder(strings.NewReader(raw))
	if err := d.Decode(value); err != nil {
		return err
	}
	return legacyEnd(d)
}
func legacyEnd(d *json.Decoder) error {
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return fmt.Errorf("trailing data: %v", err)
	}
	return nil
}
