//go:build !linux

package lease

import "time"

func pidNamespace() string { return "" }

func startTime(int) string { return "" }

func bootTime() time.Time { return time.Time{} }
