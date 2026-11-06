//go:build !unix

package lease

import "os"

// pidAlive is conservative where no cheap check exists: the owner counts
// as alive until its slot is stale.
func pidAlive(pid int) bool { return pid > 0 }

func linkCount(os.FileInfo) (uint64, bool) { return 0, false }
