//go:build !race

package store

import "time"

func sqliteProjectionAuditPermanentPathMaxDurationForTest() time.Duration {
	return time.Second
}
