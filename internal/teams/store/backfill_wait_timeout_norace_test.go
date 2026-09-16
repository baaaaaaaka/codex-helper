//go:build !race

package store

import "time"

func storeBackfillPageWaitTimeoutForTest() time.Duration { return 5 * time.Second }
