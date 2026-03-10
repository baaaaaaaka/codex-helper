package cli

import (
	"sync"
	"testing"
)

var cliTestHookGuard sync.Mutex

// lockCLITestHooks serializes tests that mutate package- or process-global
// state such as seam function variables, stack starters, or os.Stdin.
func lockCLITestHooks(t *testing.T) {
	t.Helper()
	cliTestHookGuard.Lock()
	t.Cleanup(cliTestHookGuard.Unlock)
}
