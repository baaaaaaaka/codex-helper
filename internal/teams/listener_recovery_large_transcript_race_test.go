//go:build race

package teams

import "time"

// The fixture intentionally reads an approximately 8 MiB JSONL record. Race
// instrumentation multiplies both JSON parsing and the durable checkpoint
// writes, so use a larger finite bound for the race proof rather than treating
// instrumentation cost as a parser liveness failure.
func listenerRecoveryLargeTranscriptTimeout() time.Duration { return 60 * time.Second }
