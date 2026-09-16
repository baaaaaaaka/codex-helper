//go:build race

package store

import "time"

// A race-instrumented SQLite page commit can spend substantially longer in
// the driver and filesystem than the production page itself. Keep the test's
// first-page boundary finite, but do not turn that scheduler/I/O margin into a
// false failure before the heartbeat assertion can run.
func storeBackfillPageWaitTimeoutForTest() time.Duration { return 90 * time.Second }
