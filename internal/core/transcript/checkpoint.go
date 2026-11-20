package transcript

import (
	"crypto/sha256"
	"encoding/json"
)

// TerminalAnchor identifies a turn boundary without depending on its JSON
// whitespace or its byte and line positions in a paginated rollout.
type TerminalAnchor struct {
	End, Line int64
	Hash      [sha256.Size]byte
}

func terminalHash(start Event, end Event) [sha256.Size]byte {
	// Match two ordered records of the same identified turn. File positions
	// and such representation details as trailing whitespace are irrelevant.
	if start.TurnID == "" || start.TurnID != end.TurnID || end.Kind != KindTurnComplete || end.Time.IsZero() {
		return [sha256.Size]byte{}
	}
	raw, _ := json.Marshal([]any{start.TurnID, start.Time, end.Time, end.HasLast, end.Text})
	return sha256.Sum256(raw)
}

// BoundaryTracker remembers an independently verifiable pair of records.
type BoundaryTracker struct {
	start Event
	Last  TerminalAnchor
}

func (a *BoundaryTracker) Observe(ev Event, end, line int64) {
	switch ev.Kind {
	case KindTurnStarted:
		if a.start.TurnID != ev.TurnID {
			a.start = ev
		}
	case KindTurnComplete:
		if sum := terminalHash(a.start, ev); sum != ([sha256.Size]byte{}) {
			a.Last = TerminalAnchor{End: end, Line: line, Hash: sum}
		}
		a.start = Event{}
	case KindTurnAborted:
		a.start = Event{}
	}
}

func (t *Tracker) TerminalAnchor() TerminalAnchor { return t.boundary.Last }
