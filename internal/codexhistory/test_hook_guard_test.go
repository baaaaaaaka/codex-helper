package codexhistory

import (
	"sync"
	"testing"
)

var codexHistoryTestHookGuard sync.Mutex

func lockCodexHistoryTestHooks(t *testing.T) {
	t.Helper()
	codexHistoryTestHookGuard.Lock()
	t.Cleanup(codexHistoryTestHookGuard.Unlock)
}
