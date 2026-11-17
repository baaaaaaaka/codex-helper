package store

import "testing"

func TestGoalControlsWatermarkAndRecovery(t *testing.T) {
	s, _ := openTest(t)
	var conv int64
	mustT(t, s.Update(func(tx *Tx) error {
		c, err := tx.CreateConversation(NewConversation{Label: "goal", Kind: KindWork, Channel: "teams", ExtChatID: "chat"})
		conv = c
		return err
	}))
	first := GoalControl{Conv: conv, Inbound: 20, Thread: "native", Cwd: "/work", Turn: 7, Pending: true}
	mustT(t, s.Update(func(tx *Tx) error { return tx.SetGoalControl(first) }))
	mustT(t, s.Update(func(tx *Tx) error { return tx.SetGoalControl(GoalControl{Conv: conv, Inbound: 19}) }))
	mustT(t, s.Read(func(tx *Tx) error {
		g, ok, err := tx.GoalControl(conv)
		if !ok || g != first {
			t.Fatalf("older control replaced stop: %+v", g)
		}
		return err
	}))
	mustT(t, s.Update(func(tx *Tx) error { return tx.FinishGoalControl(conv, 19) }))
	mustT(t, s.Read(func(tx *Tx) error {
		rows, err := tx.PendingGoalControls()
		if len(rows) != 1 {
			t.Fatal(rows)
		}
		return err
	}))
	mustT(t, s.Update(func(tx *Tx) error { return tx.FinishGoalControl(conv, 20) }))
	mustT(t, s.Read(func(tx *Tx) error {
		rows, err := tx.PendingGoalControls()
		if len(rows) != 0 {
			t.Fatal(rows)
		}
		return err
	}))
}

// One retained watermark per conversation; repeated reads and no-op controls
// must not amplify writes as ordinary chat history grows.
func TestGoalControlsNoOpWritesAndBoundedRows(t *testing.T) {
	s, _ := openTest(t)
	conv := newWork(t, s, "goal-controls")
	g := GoalControl{Conv: conv, Inbound: 1, Thread: "native", NativeTurn: "selected", Pending: true}
	mustT(t, s.Update(func(tx *Tx) error { return tx.SetGoalControl(g) }))
	var before, after int64
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&before))
	for i := 0; i < 100; i++ {
		mustT(t, s.Read(func(tx *Tx) error { _, _, err := tx.GoalControl(conv); return err }))
		mustT(t, s.Update(func(tx *Tx) error { return tx.SetGoalControl(g) }))
		mustT(t, s.Update(func(tx *Tx) error { return tx.FinishGoalControl(conv, 0) }))
	}
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&after))
	if before != after {
		t.Fatalf("unchanged controls wrote %d rows", after-before)
	}
	for i := int64(2); i <= 1000; i++ {
		g.Inbound = i
		mustT(t, s.Update(func(tx *Tx) error { return tx.SetGoalControl(g) }))
	}
	var n int
	mustT(t, s.w.QueryRow(`SELECT count(*) FROM goal_controls`).Scan(&n))
	if n != 1 {
		t.Fatalf("controls accumulated %d rows", n)
	}
	mustT(t, s.Update(func(tx *Tx) error { return tx.FinishGoalControl(conv, g.Inbound) }))
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&before))
	for i := 0; i < 20; i++ {
		mustT(t, s.Update(func(tx *Tx) error { return tx.FinishGoalControl(conv, g.Inbound) }))
	}
	mustT(t, s.w.QueryRow(`SELECT total_changes()`).Scan(&after))
	if before != after {
		t.Fatal("replayed completion wrote again")
	}
}
