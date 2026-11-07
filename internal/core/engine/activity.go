package engine

import "time"

// Chat lists (`st`, `list`, `stats` in the control chat, the new-chat
// summary) put the most recently active chat last, so the newest is
// what the owner sees first at the bottom of the message (owner request
// 2026-11-07). Activity is Codex's, not Teams': the newest change of the
// chat's Codex rollouts (each user or agent record is appended there, by
// the helper's turns and the TUI alike); a chat without one (a new chat)
// counts from its creation.

// activityTimes returns each conversation's activity time. It reads the
// store: the caller does not hold e.mu.
func (e *Engine) activityTimes() map[int64]time.Time {
	at := map[int64]time.Time{}
	srcs, err := e.st.Sources()
	if err != nil {
		e.logf("chat activity: %v", err)
	}
	for _, s := range srcs {
		if t := time.UnixMilli(s.MTime); s.ConvID != 0 && s.MTime > 0 && t.After(at[s.ConvID]) {
			at[s.ConvID] = t
		}
	}
	e.mu.Lock()
	for id, cs := range e.convs {
		if _, ok := at[id]; !ok {
			at[id] = cs.created
		}
	}
	e.mu.Unlock()
	return at
}

// byActivity orders conversation ids by activity, the most recent last
// (ties by id).
func byActivity(at map[int64]time.Time, a, b int64) bool {
	if ta, tb := at[a], at[b]; !ta.Equal(tb) {
		return ta.Before(tb)
	}
	return a < b
}
