//go:build race

package store

import "time"

// Race instrumentation and a contended hosted runner can make a single
// setup-free owner validation cross one second. Keep the assertion bounded at
// two seconds; this still rejects the multi-attempt/backoff path without
// treating race overhead as a retry.
func sqliteProjectionAuditPermanentPathMaxDurationForTest() time.Duration {
	return 2 * time.Second
}
