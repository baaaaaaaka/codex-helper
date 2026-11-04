//go:build !unix

package store

// pidAlive is conservative where no cheap check exists: a holder is treated
// as alive until its heartbeat is stale.
func pidAlive(pid int) bool { return pid > 0 }
