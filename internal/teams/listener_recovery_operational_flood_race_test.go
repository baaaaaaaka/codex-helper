//go:build race

package teams

// The store package keeps the 64-row compatibility/page-boundary check under
// -race. The vertical listener check only needs to prove that the ordinary
// tail is not hidden behind more work than one listener quantum; using one
// extra continuation row keeps that invariant while avoiding a synthetic
// race-instrumentation timeout from 65 durable SQLite/JSON rows.
func listenerRecoveryOperationalFloodCount() int { return maxWorkChatPollsPerCycle + 1 }
