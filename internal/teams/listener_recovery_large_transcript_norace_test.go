//go:build !race

package teams

import "time"

// The normal build can inspect the large JSONL record inside the original
// finite observation window. Keep that window short so a genuinely stalled
// transcript scanner is reported promptly.
func listenerRecoveryLargeTranscriptTimeout() time.Duration { return 15 * time.Second }
