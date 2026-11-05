package store

// Key formats shared by the converter (internal/core/migrate) and the engine.
// Both must use these functions: a key written by one and read by the other
// under a different spelling silently loses the state (a receive cursor
// that goes back to its migration-time value, a Retry-After not honoured).

// ChatStreamKey is the streams.key of the receive stream of one chat.
func ChatStreamKey(channel, chatID string) string { return channel + ":chat:" + chatID }

// AccountReadGate and AccountWriteGate are the gates.key of the
// account-wide read and write throttling gates of a channel.
func AccountReadGate(channel string) string  { return channel + ":account-read" }
func AccountWriteGate(channel string) string { return channel + ":account-write" }

// ChatGate is the gates.key of a per-chat throttling gate.
func ChatGate(channel, chatID string) string { return channel + ":chat-gate:" + chatID }

// MetaMigratedFrom is the meta key the converter sets (to the legacy store
// version) in a store it produced. The engine branches on it: a migrated
// store is not a fresh install.
const MetaMigratedFrom = "migrated_from"

// MetaMigrationSummary is the meta key the converter sets to a short,
// user-facing summary of the conversion. The engine posts it to the
// control chat once and clears it in the same transaction.
const MetaMigrationSummary = "migration_summary"

// MetaLegacyFinalChecks is the meta key holding the converter's list of
// LegacyFinalCheck: the previous helper's answers whose send outcome was
// unknown. The engine checks each once against the chat's history at first
// start and resends it only when it is absent (owner ruling 2026-11-05,
// Q1). An entry decided as sent stays, with Found set, so that post is
// recognised as the helper's own whenever it is read; an entry resent or
// given up is removed.
const MetaLegacyFinalChecks = "legacy_final_checks"

// MetaHistoryChecks is the meta key holding the converter's list of
// HistoryCheck: converted rollouts whose catch-up starts before the
// previous helper's furthest position with no delivery proof for what lies
// between (STORE-DESIGN §9.4: old positions are no proof). At first start
// the engine reads each one's chat history once, before that rollout's
// catch-up posts anything, and resumes after the newest record it finds
// posted there (canary rerun brief 2026-11-06: s001 posted at least 267
// records the previous helper had already posted). The decision stays in
// the list (Done), so a restart never checks again.
const MetaHistoryChecks = "legacy_history_checks"

// HistoryCheck is one converted rollout whose resume point is checked
// against its chat's history.
type HistoryCheck struct {
	Path     string `json:"path"`
	ConvID   int64  `json:"conv"`
	From     int64  `json:"from"`      // the converted source offset (where catch-up starts without the check)
	FromLine int64  `json:"from_line"` // its line
	To       int64  `json:"to"`        // the previous helper's furthest position (checkpoint, anchor cutoff, watcher)
	End      int64  `json:"end"`       // the rollout's size at the conversion: its records up to here are compared
	Done     bool   `json:"done,omitempty"`
	// Resume is the end of the newest record found in the chat (0: none
	// found, catch-up starts at From).
	Resume  int64  `json:"resume,omitempty"`
	Outcome string `json:"outcome,omitempty"`
}

// LegacyFinalCheck is one answer part of the previous helper whose send
// outcome was unknown. Its outbox row is imported dropped (it holds no
// conversation's sends back); the engine resends Body only when the chat's
// history does not have it.
type LegacyFinalCheck struct {
	Key      string `json:"key"`       // delivery key of the imported (dropped) row
	ConvID   int64  `json:"conv"`      // its conversation
	LegacyID string `json:"legacy_id"` // the old outbox id: the hidden marker the old helper posted it with
	ExtMsgID string `json:"ext_msg_id,omitempty"`
	Created  int64  `json:"created"` // unix ms
	Body     string `json:"body"`    // the old row's text (Markdown)
	ReplyTo  string `json:"reply_to,omitempty"`
	// Found is the id of the chat message found to be this answer: the
	// check is decided and the message is the helper's own post.
	Found string `json:"found,omitempty"`
}
