//go:build windows

package fakecodex

import "github.com/baaaaaaaka/codex-helper/internal/proc"

// Opening a process can succeed after it exits while a handle is retained.
// Use the exit-code check shared with the production process helpers.
func pidAlive(pid int) bool {
	return proc.IsAlive(pid)
}
