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
