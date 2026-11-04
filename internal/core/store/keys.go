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
