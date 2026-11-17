// Package goal defines native goal values shared by transport and presentation.
// Codex owns execution and lifecycle. An observation is never a scheduler.
package goal

// State is the native goal payload. Empty Status means confirmed absence only
// when returned by a successful query, or inside a known Snapshot.
type State struct {
	Objective       string `json:"objective"`
	Status          string `json:"status"`
	TokenBudget     *int64 `json:"tokenBudget,omitempty"`
	TokensUsed      int64  `json:"tokensUsed"`
	TimeUsedSeconds int64  `json:"timeUsedSeconds"`
	CreatedAt       int64  `json:"createdAt"`
	UpdatedAt       int64  `json:"updatedAt"`
}

func (s State) Clone() State {
	if s.TokenBudget != nil {
		n := *s.TokenBudget
		s.TokenBudget = &n
	}
	return s
}

// Snapshot distinguishes an unobserved/disconnected goal from known absence.
// Historical observations retain their value with Known=false.
type Snapshot struct {
	Known    bool // a successful native observation confirms the state (including absence)
	Observed bool // a native source exists; historical state is never substituted as current
	State
}

func (s Snapshot) Active() bool { return s.Known && s.Status == "active" }
