package store

// GoalControl is the latest explicit control intent, not a cached native goal.
// Keeping the inbound watermark prevents an older queued activation from
// overtaking an urgent cancellation. Only unfinished stops are replayed.
type GoalControl struct {
	Conv, Inbound           int64
	Thread, Cwd, NativeTurn string
	Turn                    int64
	Pending                 bool
}

func (t *Tx) GoalControl(conv int64) (GoalControl, bool, error) {
	var g GoalControl
	g.Conv = conv
	_, ok, err := one(t.row(`SELECT inbound_id,thread_id,cwd,turn_id,native_turn,pending FROM goal_controls WHERE conv_id=?`, conv), func(s scanner) (bool, error) {
		return true, s.Scan(&g.Inbound, &g.Thread, &g.Cwd, &g.Turn, &g.NativeTurn, &g.Pending)
	})
	return g, ok, err
}
func (t *Tx) SetGoalControl(g GoalControl) error {
	_, err := t.exec(`INSERT INTO goal_controls(conv_id,inbound_id,thread_id,cwd,turn_id,native_turn,pending) VALUES(?,?,?,?,?,?,?) ON CONFLICT(conv_id) DO UPDATE SET inbound_id=excluded.inbound_id,thread_id=excluded.thread_id,cwd=excluded.cwd,turn_id=excluded.turn_id,native_turn=excluded.native_turn,pending=excluded.pending WHERE excluded.inbound_id>=goal_controls.inbound_id AND (inbound_id,thread_id,cwd,turn_id,native_turn,pending) IS NOT (excluded.inbound_id,excluded.thread_id,excluded.cwd,excluded.turn_id,excluded.native_turn,excluded.pending)`, g.Conv, g.Inbound, g.Thread, g.Cwd, g.Turn, g.NativeTurn, g.Pending)
	return err
}
func (t *Tx) FinishGoalControl(conv, inbound int64) error {
	_, err := t.exec(`UPDATE goal_controls SET pending=0 WHERE conv_id=? AND inbound_id=? AND pending=1`, conv, inbound)
	return err
}
func (t *Tx) PendingGoalControls() ([]GoalControl, error) {
	rows, err := t.rows(`SELECT conv_id,inbound_id,thread_id,cwd,turn_id,native_turn,pending FROM goal_controls WHERE pending=1 ORDER BY conv_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GoalControl
	for rows.Next() {
		var g GoalControl
		if err := rows.Scan(&g.Conv, &g.Inbound, &g.Thread, &g.Cwd, &g.Turn, &g.NativeTurn, &g.Pending); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}
