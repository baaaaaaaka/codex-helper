package store

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
)

func TestModelSwitchAtomicReservationAndInputOwnership(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "switch")
	var original Turn
	mustT(t, s.Update(func(tx *Tx) error {
		rows, err := tx.AdmitWindow("switch", conv, []InboundMsg{prompt("original", 1)}, "1")
		if err != nil {
			return err
		}
		if err = tx.MarkStarting(rows[0].TurnID, "th-switch"); err != nil {
			return err
		}
		if err = tx.MarkRunning(rows[0].TurnID, "native-old"); err != nil {
			return err
		}
		original, _, err = tx.Turn(rows[0].TurnID)
		return err
	}))
	rollback := errors.New("injected failure")
	if err := s.Update(func(tx *Tx) error {
		if err := tx.BeginModelSwitch(original); err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if countWhere(t, s, `SELECT COUNT(*) FROM model_switches`) != 0 {
		t.Fatal("partial handoff persisted")
	}
	var continued Turn
	var input int64
	mustT(t, s.Update(func(tx *Tx) error {
		for range 10 {
			if err := tx.BeginModelSwitch(original); err != nil {
				return err
			}
		}
		if err := tx.FinishTurn(original.ID, TurnInterrupted, "model switch", nil); err != nil {
			return err
		}
		rows, err := tx.AdmitWindow("switch", conv, []InboundMsg{prompt("correction", 2)}, "2")
		if err != nil {
			return err
		}
		input = rows[0].TurnID
		sw, _, err := tx.ModelSwitch(conv)
		if err != nil {
			return err
		}
		continued, err = tx.NewContinuation(sw, original, nil)
		if err != nil {
			return err
		}
		sw, _, err = tx.ModelSwitch(conv)
		if err != nil {
			return err
		}
		same, err := tx.NewContinuation(sw, original, nil)
		if err != nil {
			return err
		}
		if same.ID != continued.ID {
			t.Fatal("duplicate continuation")
		}
		if err = tx.AddContinuationInput(continued.ID, input); err != nil {
			return err
		}
		if err = tx.AddContinuationInput(continued.ID, input); err != nil {
			return err
		}
		next, ok, err := tx.NextRunnable(conv)
		if err != nil {
			return err
		}
		if !ok || next.ID != continued.ID {
			t.Fatalf("claimed input became execution: %+v %v", next, ok)
		}
		if err = tx.MarkStarting(continued.ID, "th-switch"); err != nil {
			return err
		}
		if err = tx.EndModelSwitch(conv, original.ID, original.Attempt); err != nil {
			return err
		}
		if err = tx.MarkRunning(continued.ID, "native-new"); err != nil {
			return err
		}
		return tx.AcceptContinuationInputs(continued.ID)
	}))
	if continued.ResumeFrom != original.ID || continued.OutputKey != original.TurnKey {
		t.Fatalf("lost task identity: %+v", continued)
	}
	if countWhere(t, s, `SELECT COUNT(*) FROM continuation_inputs`) != 1 || countWhere(t, s, `SELECT COUNT(*) FROM model_switches`) != 0 {
		t.Fatal("unbounded or unfinished handoff")
	}
	mustT(t, s.Read(func(tx *Tx) error {
		in, _, err := tx.Turn(input)
		if in.State != TurnCompleted {
			t.Errorf("input not settled with acceptance: %+v", in)
		}
		return err
	}))
}

func TestModelSwitchReadsDoNotWriteOrScanHistory(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "history")
	mustT(t, s.Update(func(tx *Tx) error {
		for n := range 5000 {
			if _, err := tx.exec(`INSERT INTO inbound(conv_id,ext_msg_id,ext_time,received_at,sender,kind,state,body) VALUES(?,?,?,1,'owner','command','done','model status')`, conv, fmt.Sprint(n), n); err != nil {
				return err
			}
		}
		return nil
	}))
	var before, after int64
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&before))
	for range 20 {
		mustT(t, s.Update(func(tx *Tx) error {
			if _, _, err := tx.ModelSwitch(conv); err != nil {
				return err
			}
			rows, err := tx.PendingCommands(conv)
			if len(rows) != 0 {
				t.Error("read completed history")
			}
			return err
		}))
	}
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&after))
	if before != after {
		t.Fatalf("unchanged scheduler wrote rows: %d -> %d", before, after)
	}
	checkHotPlans(t, s)
}

func TestModelSwitchMigrationPreservesExistingWork(t *testing.T) {
	path := filepath.Join(t.TempDir(), "v8.sqlite")
	db, err := sql.Open("sqlite", path)
	mustT(t, err)
	for _, migration := range migrations[:8] {
		for _, q := range migration {
			_, err = db.Exec(q)
			mustT(t, err)
		}
	}
	_, err = db.Exec(fmt.Sprintf("PRAGMA application_id=%d", ApplicationID))
	mustT(t, err)
	_, err = db.Exec("PRAGMA user_version=8")
	mustT(t, err)
	_, err = db.Exec(`INSERT INTO conversations(id,label,kind,channel,state,created_at,updated_at) VALUES(1,'old','work','teams','active',1,2)`)
	mustT(t, err)
	_, err = db.Exec(`INSERT INTO threads(conv_id,thread_id,cwd,model,effort,updated_at) VALUES(1,'original-thread','/work','original-model','high',2)`)
	mustT(t, err)
	_, err = db.Exec(`INSERT INTO turns(conv_id,turn_key,state,thread_id,codex_turn_id,model,effort,attempt,queued_at) VALUES(1,'original-client','running','original-thread','original-native','original-model','high',2,1)`)
	mustT(t, err)
	mustT(t, db.Close())
	s := openAt(t, path, newClock())
	mustT(t, s.Read(func(tx *Tx) error {
		th, _, err := tx.Thread(1)
		if err != nil {
			return err
		}
		tr, _, err := tx.Turn(1)
		if err != nil {
			return err
		}
		if th.SettingsVersion != 0 || th.ThreadID != "original-thread" || th.Model != "original-model" || th.Effort != "high" || tr.ResumeFrom != 0 || tr.OutputKey != "" || tr.Attempt != 2 || tr.State != TurnRunning || tr.CodexTurnID != "original-native" {
			t.Fatalf("migration changed active work: %+v %+v", th, tr)
		}
		_, yes, err := tx.ModelSwitch(1)
		if yes {
			t.Fatal("migration invented a switch")
		}
		return err
	}))
}

func BenchmarkModelSwitchIdleReads(b *testing.B) {
	for _, history := range []int{0, 5000} {
		b.Run(fmt.Sprint(history), func(b *testing.B) {
			s, err := Open(filepath.Join(b.TempDir(), "bench.db"))
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			if _, err = s.Acquire(Ident{Host: "test", PID: 1, BootID: "test"}); err != nil {
				b.Fatal(err)
			}
			err = s.Update(func(tx *Tx) error {
				id, err := tx.CreateConversation(NewConversation{Label: "bench", Kind: KindWork, Channel: "teams", ExtChatID: "bench"})
				if err != nil {
					return err
				}
				for i := range history {
					if _, err = tx.exec(`INSERT INTO inbound(conv_id,ext_msg_id,ext_time,received_at,sender,kind,state,body) VALUES(?,?,1,1,'owner','command','done','model status')`, id, fmt.Sprint(i)); err != nil {
						return err
					}
				}
				return nil
			})
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err = s.Read(func(tx *Tx) error {
					if _, _, err := tx.ModelSwitch(1); err != nil {
						return err
					}
					_, err := tx.PendingCommands(1)
					return err
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// A continuation can claim fresh input and be rejected before native start
// while an earlier accepted attempt still gives the execution a native id.
// Switching during that retry must move only its unaccepted input ownership.
func TestModelSwitchTransfersUnacceptedContinuationInputs(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "transfer")
	mustT(t, s.Update(func(tx *Tx) error {
		rows, err := tx.AdmitWindow("transfer", conv, []InboundMsg{prompt("source", 1), prompt("accepted", 2), prompt("pending", 3)}, "3")
		if err != nil {
			return err
		}
		source, accepted, pending := rows[0].TurnID, rows[1].TurnID, rows[2].TurnID
		if err = tx.MarkStarting(source, "thread"); err != nil {
			return err
		}
		if err = tx.MarkRunning(source, "earlier-native"); err != nil {
			return err
		}
		if err = tx.AddContinuationInput(source, accepted); err != nil {
			return err
		}
		if err = tx.AcceptContinuationInputs(source); err != nil {
			return err
		}
		if err = tx.AddContinuationInput(source, pending); err != nil {
			return err
		}
		tr, _, err := tx.Turn(source)
		if err != nil {
			return err
		}
		if err = tx.BeginModelSwitch(tr); err != nil {
			return err
		}
		if err = tx.FinishTurn(source, TurnInterrupted, "switch during retry", nil); err != nil {
			return err
		}
		sw, _, err := tx.ModelSwitch(conv)
		if err != nil {
			return err
		}
		next, err := tx.NewContinuation(sw, tr, nil)
		if err != nil {
			return err
		}
		oldInputs, err := tx.ContinuationInputs(source)
		if err != nil {
			return err
		}
		newInputs, err := tx.ContinuationInputs(next.ID)
		if err != nil {
			return err
		}
		if len(oldInputs) != 1 || oldInputs[0].ID != accepted || len(newInputs) != 1 || newInputs[0].ID != pending {
			t.Fatalf("input ownership lost or replayed: old=%+v new=%+v", oldInputs, newInputs)
		}
		return nil
	}))
}
