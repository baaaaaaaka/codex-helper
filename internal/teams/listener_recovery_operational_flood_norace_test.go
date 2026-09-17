//go:build !race

package teams

// Keep the normal vertical listener fixture above the fixed SQLite admission
// page. The store-level regression covers that page boundary under -race;
// this normal-build scale fixture keeps the end-to-end listener assertion at
// the production-sized 65-row boundary without making race instrumentation
// spend a minute decoding rows that do not add another invariant.
func listenerRecoveryOperationalFloodCount() int { return 65 }
