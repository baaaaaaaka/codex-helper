package store

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// This frozen schema and literal data deliberately do not use migrations or
// current serializers to build the input or expected output.
func legacyEngineDB(t *testing.T) (string, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "v5.sqlite")
	db, err := sql.Open("sqlite", path)
	mustT(t, err)
	t.Cleanup(func() { db.Close() })
	schema, err := os.ReadFile("testdata/engine-v5.sql")
	mustT(t, err)
	_, err = db.Exec(string(schema))
	mustT(t, err)
	_, err = db.Exec(`INSERT INTO conversations(id,label,kind,channel,state,created_at,updated_at) VALUES(1,'work','work','teams','active',1,1);
 INSERT INTO transcript_sources(id,path,thread_id,conv_id,state,offset,line,size,mtime,updated_at) VALUES(1,'/synthetic/rollout.jsonl','thread',1,'tracking',17,2,100,8,1);
 INSERT INTO inbound(id,conv_id,ext_msg_id,ext_time,received_at,sender,kind,state,body) VALUES(1,1,'old-inbound',1,1,'owner','prompt','done','original');
 INSERT INTO outbox(id,conv_id,delivery_key,seq,kind,body,state,attempt_token,created_at) VALUES(1,1,'turn:71:final:1',1,'final','original answer','unknown','original-token',1)`)
	mustT(t, err)
	return path, db
}

func legacyMeta(t *testing.T, db *sql.DB, key, value string) {
	t.Helper()
	_, err := db.Exec(`INSERT INTO meta(key,value) VALUES(?,?)`, key, value)
	mustT(t, err)
}

func TestEngineWALRecoversAfterRetainedReader(t *testing.T) {
	s, _ := openTest(t)
	mustT(t, s.Update(func(tx *Tx) error { return tx.SetCLIProgress("one", CLIProgress{Stage: "initial"}) }))
	_, err := s.w.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
	mustT(t, err)
	observer, err := sql.Open("sqlite", dsn(s.Path(), true))
	mustT(t, err)
	defer observer.Close()
	read, err := observer.Begin()
	mustT(t, err)
	defer read.Rollback()
	var stage string
	mustT(t, read.QueryRow(`SELECT stage FROM cli_requests WHERE id='one'`).Scan(&stage))
	for i := 0; i < 50; i++ {
		mustT(t, s.Update(func(tx *Tx) error { return tx.SetCLIProgress("one", CLIProgress{Stage: fmt.Sprint(i)}) }))
	}
	var busy, frames, checkpointed int
	mustT(t, s.w.QueryRow(`PRAGMA wal_checkpoint(PASSIVE)`).Scan(&busy, &frames, &checkpointed))
	if frames <= checkpointed {
		t.Fatalf("reader did not retain WAL: busy=%d frames=%d checkpointed=%d", busy, frames, checkpointed)
	}
	info, err := os.Stat(s.Path() + "-wal")
	mustT(t, err)
	t.Logf("retained-reader WAL bytes=%d frames=%d checkpointed=%d", info.Size(), frames, checkpointed)
	mustT(t, read.Rollback())
	mustT(t, s.w.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&busy, &frames, &checkpointed))
	if busy != 0 || frames != 0 || checkpointed != 0 {
		t.Fatalf("WAL not reclaimed after reader closed: %d/%d/%d", busy, frames, checkpointed)
	}
	info, err = os.Stat(s.Path() + "-wal")
	mustT(t, err)
	if info.Size() != 0 {
		t.Fatalf("WAL retained %d bytes after checkpoint", info.Size())
	}
	mustT(t, s.Read(func(tx *Tx) error {
		p, ok, err := tx.CLIProgress("one")
		if !ok || p.Stage != "49" {
			t.Fatal(p, ok)
		}
		return err
	}))
}

func TestEngineStateMigrationFrozenV5(t *testing.T) {
	path, db := legacyEngineDB(t)
	for key, value := range map[string]string{
		"engine_notify":         `[{"turn":71,"conv":1,"request":"你好\nrequest","attempted":true},{"conv":1,"key":"mirror:thread:9007199254740993:","kind":"local","text":"notice"}]`,
		"engine_imports":        `[{"id":9,"conv":1,"kind":"full","thread":"thread","path":"/synthetic/rollout.jsonl","rewind":true,"keep_teams":true,"ns":"独立","applied":true,"src":1,"until":9007199254740993,"posted":3,"resumed":true,"reached":true}]`,
		"engine_deleg_threads":  `["thread-二","thread-一"]`,
		"legacy_final_checks":   `[{"key":"old-answer","conv":1,"legacy_id":"legacy-id","ext_msg_id":"external","created":9007199254740993,"body":"same\nanswer","reply_to":"parent","found":"found-id"}]`,
		"legacy_history_checks": `[{"path":"/synthetic/rollout.jsonl","conv":1,"from":17,"from_line":2,"to":70,"end":100,"done":true,"resume":60,"outcome":"found"}]`,
		"engine_usage:1":        `{"turns":10,"total":12345}`,
		"engine_srcinfo:1":      `{"goal":{"objective":"目标","status":"active"},"model":"m","effort":"high","tier":"standard"}`,
		"engine_cli:once":       `{"stage":"importing","conv":1,"old":"old-chat","chat":"new-chat","url":"url","title":"title","name":"name","import":9,"result":{"lines":["second","first","second"],"error":"saved"}}`,
		"engine_dashboard_view": `{"kind":"sessions","workspace":"ws","items":[{"n":2,"kind":"session","workspace":"ws","thread":"t2"},{"n":1,"kind":"session","workspace":"ws","thread":"t1"}],"expires":1800000000000}`,
		"engine_model_fork_1":   `{"src":1,"model":"profile"}`,
	} {
		legacyMeta(t, db, key, value)
	}
	cached := filepath.Join(t.TempDir(), "old-quote.json")
	mustT(t, os.WriteFile(cached, []byte(`{"from":"full sender","time":"full time","text":"full quote\nbody","fetched":true,"full":"/synthetic/export.txt"}`), 0600))
	_, err := db.Exec(`INSERT INTO inbound_files(inbound_id,idx,name,ext_ref,local_path,kind,state) VALUES(1,0,'doc','{"k":0,"n":"doc","s":9007199254740993,"t":"text/plain","r":"reference"}','/synthetic/doc','file','ready'),(1,1,'quote','{"q":{"n":1,"id":"q1","from":"preview sender","text":"preview","fwd":true}}',?,'file','ready')`, cached)
	mustT(t, err)
	_, err = db.Exec(`INSERT INTO outbox_files(outbox_id,local_path,size,uploaded_bytes,ext_item_id) VALUES(1,'/synthetic/doc',99,99,'{"k":0,"n":"uploaded","s":99,"t":"text/plain","r":"external-reference"}')`)
	mustT(t, err)
	// Keep a committed WAL uncheckpointed while the new store opens.
	_, err = db.Exec(`PRAGMA journal_mode=WAL`)
	mustT(t, err)
	legacyMeta(t, db, "machine_label", "original")
	s, err := Open(path)
	mustT(t, err)
	defer s.Close()
	mustT(t, s.Read(func(tx *Tx) error {
		n, ok, err := tx.Notification("turn:71")
		if err != nil {
			return err
		}
		if !ok || !n.Attempted || n.Request != "你好\nrequest" {
			t.Fatalf("notification: %+v", n)
		}
		j, ok, err := tx.ImportJob(9)
		if err != nil {
			return err
		}
		if !ok || j.Until != 9007199254740993 || !j.KeepTeams || !j.Reached || j.NS != "独立" {
			t.Fatalf("import: %+v", j)
		}
		ids, err := tx.DelegatedThreads()
		if err != nil {
			return err
		}
		if fmt.Sprint(ids) != "[thread-二 thread-一]" {
			t.Fatal(ids)
		}
		a, ok, err := tx.AnswerCheck("old-answer")
		if err != nil {
			return err
		}
		if !ok || a.Found != "found-id" || a.Created != 9007199254740993 || a.Body != "same\nanswer" {
			t.Fatal(a)
		}
		h, ok, err := tx.HistoryCheck("/synthetic/rollout.jsonl")
		if err != nil {
			return err
		}
		if !ok || !h.Done || h.Resume != 60 || h.FromLine != 2 {
			t.Fatal(h)
		}
		info, ok, err := tx.SourceInfo(1)
		if err != nil {
			return err
		}
		if !ok || info != (SourceInfo{Model: "m", Effort: "high", Tier: "standard", HasGoal: true, GoalObjective: "目标", GoalStatus: "active"}) {
			t.Fatal(info)
		}
		cli, ok, err := tx.CLIProgress("once")
		if err != nil {
			return err
		}
		if !ok || cli.Import != 9 || cli.Result == nil || fmt.Sprint(cli.Result.Lines) != "[second first second]" || cli.Result.Error != "saved" {
			t.Fatal(cli)
		}
		view, err := tx.DashboardView()
		if err != nil {
			return err
		}
		if view == nil || len(view.Items) != 2 || view.Items[0].N != 2 || view.Items[1].Thread != "t1" {
			t.Fatal(view)
		}
		note, ok, err := tx.ModelForkNote(1)
		if err != nil {
			return err
		}
		if !ok || note.Model != "profile" {
			t.Fatal(note)
		}
		files, err := tx.InboundFiles(1)
		if err != nil {
			return err
		}
		if len(files) != 2 || files[0].Ref.Size != 9007199254740993 || files[0].Ref.Ref != "reference" || files[1].Quote == nil || !files[1].Quote.Fwd || files[1].ResolvedQuote == nil || files[1].ResolvedQuote.Text != "full quote\nbody" || files[1].LocalPath != "" {
			t.Fatalf("files: %+v", files)
		}
		uploaded, ok, err := tx.OutboxFile(1)
		if err != nil {
			return err
		}
		if !ok || uploaded.Ref.Name != "uploaded" || uploaded.Ref.Ref != "external-reference" {
			t.Fatal(uploaded)
		}
		o, ok, err := tx.Outbox(1)
		if err != nil {
			return err
		}
		if !ok || o.State != OutUnknown || o.Body != "original answer" || o.AttemptToken != "original-token" {
			t.Fatal(o)
		}
		return nil
	}))
	if n := countWhere(t, s, `SELECT count(*) FROM meta WHERE key<>'machine_label'`); n != 0 {
		t.Fatalf("legacy keys remain: %d", n)
	}
	mustT(t, s.Close())
	mustT(t, db.Close())
	// Residual migration input is neither authoritative nor consulted on reopen.
	mustT(t, os.WriteFile(cached, []byte("poisoned legacy cache"), 0600))
	reopened, err := Open(path)
	mustT(t, err)
	defer reopened.Close()
	mustT(t, reopened.Read(func(tx *Tx) error {
		files, err := tx.InboundFiles(1)
		if err == nil && files[1].ResolvedQuote.Text != "full quote\nbody" {
			t.Fatal("legacy file overrode SQL")
		}
		return err
	}))
}

func TestEngineManagementMigration(t *testing.T) {
	for _, tc := range []struct{ key, value, table, field, want string }{
		{"engine_update_request", `{"tag":"v2","prerelease":true,"conv":1,"inbound":7,"quote":"q","from":"v1"}`, "update_requests", "tag", "v2"},
		{"engine_update_pending", `{"tag":"v3","conv":1}`, "update_pending", "tag", "v3"},
		{"engine_restart_request", `{"conv":1,"inbound":7,"quote":"q","force":true}`, "restart_requests", "quote", "q"},
		{"engine_codex_upgrade", `{"id":"upgrade","conv":1,"inbound":7,"quote":"q","targets":[{"conv":2,"inbound":9},{"conv":1,"inbound":7}]}`, "codex_upgrade", "request_id", "upgrade"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			path, db := legacyEngineDB(t)
			legacyMeta(t, db, tc.key, tc.value)
			s, err := Open(path)
			mustT(t, err)
			defer s.Close()
			var got string
			mustT(t, s.r.QueryRow(`SELECT `+tc.field+` FROM `+tc.table).Scan(&got))
			if got != tc.want {
				t.Fatal(got)
			}
			if tc.key == "engine_codex_upgrade" {
				mustT(t, s.Read(func(tx *Tx) error {
					n, ok, err := tx.CodexRequest()
					if !ok || len(n.Targets) != 2 || n.Targets[0].Conv != 2 || n.Targets[1].Inbound != 7 {
						t.Fatal(n)
					}
					return err
				}))
			}
		})
	}
}

func TestEngineStateMigrationRollsBack(t *testing.T) {
	for _, tc := range []struct{ name, key, value, trigger string }{
		{name: "malformed", key: "engine_notify", value: `[{"conv":1,"key":"first"},{`},
		{name: "duplicate", key: "engine_notify", value: `[{"conv":1,"key":"same"},{"conv":1,"key":"same"}]`},
		{name: "trailing", key: "engine_notify", value: `[] {}`},
		{name: "duplicate-import-conversation", key: "engine_imports", value: `[{"id":1,"conv":1},{"id":2,"conv":1}]`},
		{name: "delete-failure", key: "engine_notify", value: `[{"conv":1,"key":"first"}]`, trigger: `CREATE TRIGGER stop_delete BEFORE DELETE ON meta WHEN OLD.key='engine_notify' BEGIN SELECT RAISE(ABORT,'injected delete failure'); END`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, db := legacyEngineDB(t)
			legacyMeta(t, db, "engine_cli:before", `{"stage":"created","conv":1}`)
			legacyMeta(t, db, tc.key, tc.value)
			if tc.trigger != "" {
				_, err := db.Exec(tc.trigger)
				mustT(t, err)
			}
			if s, err := Open(path); err == nil {
				s.Close()
				t.Fatal("accepted invalid migration")
			}
			var version, tables, keys int
			mustT(t, db.QueryRow(`PRAGMA user_version`).Scan(&version))
			mustT(t, db.QueryRow(`SELECT count(*) FROM sqlite_schema WHERE name IN ('pending_notifications','cli_requests')`).Scan(&tables))
			mustT(t, db.QueryRow(`SELECT count(*) FROM meta WHERE key IN ('engine_cli:before',?)`, tc.key).Scan(&keys))
			if version != 5 || tables != 0 || keys != 2 {
				t.Fatalf("partial migration: version=%d tables=%d keys=%d", version, tables, keys)
			}
			var value string
			mustT(t, db.QueryRow(`SELECT value FROM meta WHERE key=?`, tc.key).Scan(&value))
			if value != tc.value {
				t.Fatal("source changed")
			}
		})
	}
}

func TestEngineTypedStateUnchangedWritesNothing(t *testing.T) {
	s, _ := openTest(t)
	save := func(tx *Tx) error {
		if err := tx.SetCLIProgress("once", CLIProgress{Stage: "done", Result: &CLIResult{Lines: []string{"one", "two"}}}); err != nil {
			return err
		}
		if err := tx.SetCodexRequest(CodexRequest{ID: "once", Targets: []CodexTarget{{Conv: 1, Inbound: 2}}}); err != nil {
			return err
		}
		if err := tx.SetDashboardView(&DashboardView{Kind: "sessions", Items: []DashboardItem{{N: 1, Thread: "one"}}, Expires: 99}); err != nil {
			return err
		}
		return tx.SetImportJob(ImportJob{ID: 1, Conv: 1, Kind: "full", Posted: 2})
	}
	mustT(t, s.Update(save))
	_, err := s.w.Exec(`PRAGMA wal_checkpoint(PASSIVE)`)
	mustT(t, err)
	observer, err := sql.Open("sqlite", dsn(s.Path(), true))
	mustT(t, err)
	defer observer.Close()
	conn, err := observer.Conn(context.Background())
	mustT(t, err)
	defer conn.Close()
	version := func() int64 {
		var v int64
		mustT(t, conn.QueryRowContext(context.Background(), `PRAGMA data_version`).Scan(&v))
		return v
	}
	walSize := func() int64 {
		st, err := os.Stat(s.Path() + "-wal")
		mustT(t, err)
		return st.Size()
	}
	v0, wal0 := version(), walSize()
	var before, after int64
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&before))
	for range 50 {
		mustT(t, s.Update(save))
		mustT(t, s.Read(func(tx *Tx) error { _, _, err := tx.CLIProgress("once"); return err }))
	}
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&after))
	if after != before || version() != v0 || walSize() != wal0 {
		t.Fatalf("unchanged state wrote: rows %d->%d, data_version %d->%d, WAL %d->%d", before, after, v0, version(), wal0, walSize())
	}
	// Parent-only progress must not rewrite result lines or target/item lists.
	mustT(t, s.Update(func(tx *Tx) error {
		return tx.SetCLIProgress("once", CLIProgress{Stage: "completed", Result: &CLIResult{Lines: []string{"one", "two"}}})
	}))
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&after))
	if after-before != 1 {
		t.Fatalf("parent change touched %d rows", after-before)
	}
	for {
		select {
		case <-s.Changes():
			continue
		default:
			goto drained
		}
	}
drained:
	mustT(t, s.Read(func(tx *Tx) error { _, _, err := tx.CLIProgress("once"); return err }))
	select {
	case <-s.Changes():
		t.Fatal("read woke state publisher")
	default:
	}
}

func TestEngineImportMigrationPreservesArrayOrder(t *testing.T) {
	path, db := legacyEngineDB(t)
	legacyMeta(t, db, "engine_imports", `[{"id":9,"conv":1},{"id":2,"conv":2}]`)
	s, err := Open(path)
	mustT(t, err)
	defer s.Close()
	jobs, err := s.ImportJobs()
	mustT(t, err)
	if len(jobs) != 2 || jobs[0].ID != 9 || jobs[1].ID != 2 {
		t.Fatalf("import order changed: %+v", jobs)
	}
}

func TestNotificationUnknownBacklogDoesNotHideReady(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "backlog")
	mustT(t, s.Update(func(tx *Tx) error {
		// Large bodies must never be materialized by notification evidence queries.
		_, err := tx.exec(`WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<1000)
INSERT INTO outbox(conv_id,delivery_key,seq,kind,body,state,created_at) SELECT ?, 'old:'||i||':final',i,'final',?,'unknown',1 FROM n`, conv, strings.Repeat("body", 1024))
		if err != nil {
			return err
		}
		for i := 1; i <= 1000; i++ {
			if err := tx.AddNotification(PendingNotification{Conv: conv, Key: fmt.Sprintf("old:%d:", i), Text: strings.Repeat("notice", 100)}); err != nil {
				return err
			}
		}
		return tx.AddNotification(PendingNotification{Conv: conv, Key: "new:", Request: "ready"})
	}))
	mustT(t, s.Read(func(tx *Tx) error {
		list, _, err := tx.NotificationPage(0, 128, false)
		if err != nil {
			return err
		}
		if len(list) != 1 || list[0].Key != "new:" {
			t.Fatalf("ready notice hidden: %d", len(list))
		}
		return nil
	}))
	if got := countWhere(t, s, `SELECT count(*) FROM pending_notifications`); got != 1001 {
		t.Fatal("discarded unknown evidence")
	}
}

func TestEngineMetaRejectsFormerObjects(t *testing.T) {
	s, _ := openTest(t)
	for _, key := range []string{"engine_notify", "engine_imports", "engine_deleg_threads", "legacy_final_checks", "legacy_history_checks", "engine_dashboard_view", "engine_update_request", "engine_update_pending", "engine_codex_upgrade", "engine_restart_request", "engine_usage:1", "engine_srcinfo:1", "engine_cli:once", "engine_model_fork_1"} {
		if err := s.Update(func(tx *Tx) error { return tx.SetMeta(key, `{"unexpected":"old writer"}`) }); err == nil {
			t.Fatalf("allowed old object key %s", key)
		}
	}
	if n := countWhere(t, s, `SELECT count(*) FROM meta WHERE value LIKE '%unexpected%'`); n != 0 {
		t.Fatal("persisted rejected JSON")
	}
}

func TestNotificationCardAttemptIsDurable(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "attempt")
	mustT(t, s.Update(func(tx *Tx) error { return tx.AddNotification(PendingNotification{Conv: conv, Key: "once:"}) }))
	mustT(t, s.Update(func(tx *Tx) error {
		ok, err := tx.AttemptNotification("once:")
		if !ok {
			t.Fatal("first claim rejected")
		}
		return err
	}))
	mustT(t, s.Update(func(tx *Tx) error {
		ok, err := tx.AttemptNotification("once:")
		if ok {
			t.Fatal("second card attempt allowed")
		}
		return err
	}))
	reopened, err := Open(s.Path())
	mustT(t, err)
	defer reopened.Close()
	mustT(t, reopened.Read(func(tx *Tx) error {
		n, ok, err := tx.Notification("once:")
		if !ok || !n.Attempted {
			t.Fatal("lost card fence")
		}
		return err
	}))
}

func TestEngineMetaScalarBoundaryAndServiceEvidence(t *testing.T) {
	path, db := legacyEngineDB(t)
	legacyMeta(t, db, "engine_restart_confirm", "1:9007199254740993:quote:with:colons")
	legacyMeta(t, db, "engine_freeze:1", "done:9007199254740995")
	legacyMeta(t, db, "engine_freeze:2", "9007199254740997")
	s, err := Open(path)
	mustT(t, err)
	defer s.Close()
	if r, ok, err := s.RestartConfirmation(); err != nil || !ok || r.Conv != 1 || r.Inbound != 9007199254740993 || r.Quote != "quote:with:colons" {
		t.Fatalf("restart evidence: %+v %v", r, err)
	}
	if r, ok, err := s.FreezeNotice(1); err != nil || !ok || !r.Done || r.ParkedAt != 9007199254740995 {
		t.Fatalf("completed freeze: %+v %v", r, err)
	}
	if r, ok, err := s.FreezeNotice(2); err != nil || !ok || r.Done || r.ParkedAt != 9007199254740997 {
		t.Fatalf("pending freeze: %+v %v", r, err)
	}
	_, err = s.Acquire(testIdent(1))
	mustT(t, err)
	// A delayed acknowledgement of the previous park cannot settle a new park.
	mustT(t, s.Update(func(tx *Tx) error { return tx.CompleteFreezeNotice(2, 9007199254740996) }))
	if r, _, _ := s.FreezeNotice(2); r.Done {
		t.Fatal("stale completion settled later park")
	}
	for _, key := range []string{"engine_future_object", "unregistered", "engine_notify", "engine_freeze:2", "engine_restart_confirm"} {
		if err = s.Update(func(tx *Tx) error { return tx.SetMeta(key, `{"future":true}`) }); err == nil {
			t.Fatal("untyped state admitted", key)
		}
	}
	if err = s.Update(func(tx *Tx) error { return tx.SetMeta("engine_user_title:1", "a title") }); err != nil {
		t.Fatal(err)
	}
}

func TestEngineMetaUnmappedLegacyStateRollsBack(t *testing.T) {
	path, db := legacyEngineDB(t)
	legacyMeta(t, db, "engine_future_object", `{"queue":[1,2,3]}`)
	if s, err := Open(path); err == nil {
		s.Close()
		t.Fatal("unknown legacy business object retained silently")
	}
	var version int
	mustT(t, db.QueryRow(`PRAGMA user_version`).Scan(&version))
	if version != 5 {
		t.Fatal("failed migration changed version", version)
	}
	var raw string
	mustT(t, db.QueryRow(`SELECT value FROM meta WHERE key='engine_future_object'`).Scan(&raw))
	if raw != `{"queue":[1,2,3]}` {
		t.Fatal("failed migration lost source", raw)
	}
}

func TestEngineUploadedReferenceMigration(t *testing.T) {
	const raw = `{"k":0,"n":"报告.pdf","s":9007199254740993,"r":"drive:{\"item\":\"item-中\",\"att\":\"guid-exact\",\"url\":\"https://tenant.sharepoint.com/file?q=one&k=two\",\"name\":\"报告.pdf\"}"}`
	path, db := legacyEngineDB(t)
	_, err := db.Exec(`INSERT INTO inbound_files(inbound_id,idx,name,ext_ref,kind,state) VALUES(1,0,'报告.pdf',?,'file','ready')`, raw)
	mustT(t, err)
	_, err = db.Exec(`INSERT INTO outbox_files(outbox_id,local_path,size,uploaded_bytes,ext_item_id) VALUES(1,'/synthetic/report',17,17,?)`, raw)
	mustT(t, err)
	s, err := Open(path)
	mustT(t, err)
	defer s.Close()
	mustT(t, s.Read(func(tx *Tx) error {
		in, err := tx.InboundFiles(1)
		if err != nil {
			return err
		}
		out, ok, err := tx.OutboxFile(1)
		if err != nil {
			return err
		}
		if len(in) != 1 || !ok {
			t.Fatal("missing uploaded references")
		}
		for _, ref := range []FileReference{in[0].Ref, out.Ref} {
			if ref.Ref != "drive:item-中" || ref.AttachmentID != "guid-exact" || ref.URL != "https://tenant.sharepoint.com/file?q=one&k=two" || ref.Size != 9007199254740993 {
				t.Fatalf("reference %+v", ref)
			}
		}
		original, _, err := tx.Outbox(1)
		if original.State != OutUnknown || original.AttemptToken != "original-token" {
			t.Fatal("lost send evidence", original)
		}
		return err
	}))
	_, err = s.Acquire(testIdent(100))
	mustT(t, err)
	next := FileReference{Ref: "drive:next", AttachmentID: "next-guid", URL: "https://tenant.sharepoint.com/next", Name: "next.pdf", Size: 23}
	mustT(t, s.Update(func(tx *Tx) error { return tx.SetUploadedFile(1, 23, next) }))
	mustT(t, s.Read(func(tx *Tx) error {
		got, ok, err := tx.OutboxFile(1)
		if !ok || got.Ref != next {
			t.Fatalf("native round trip %+v", got)
		}
		return err
	}))
}

func TestEngineBadUploadedReferenceRollsBack(t *testing.T) {
	path, db := legacyEngineDB(t)
	const raw = `{"k":0,"r":"drive:{\"item\":\"retained\",\"att\":\"guid\"}"}`
	_, err := db.Exec(`INSERT INTO outbox_files(outbox_id,local_path,size,uploaded_bytes,ext_item_id) VALUES(1,'/synthetic/report',17,17,?)`, raw)
	mustT(t, err)
	if s, err := Open(path); err == nil {
		s.Close()
		t.Fatal("accepted incomplete legacy drive reference")
	}
	var version int
	mustT(t, db.QueryRow(`PRAGMA user_version`).Scan(&version))
	if version != 5 {
		t.Fatal("partial migration published", version)
	}
	var retained string
	mustT(t, db.QueryRow(`SELECT ext_item_id FROM outbox_files WHERE outbox_id=1`).Scan(&retained))
	if retained != raw {
		t.Fatal("changed failed source")
	}
}
