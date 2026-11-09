package cli

import (
	"context"
	"crypto/rand"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"github.com/baaaaaaaka/codex-helper/internal/update"
)

// Started by the first-frame callback, but owned by runHistoryTui's context:
// leaving the selector for a long-running Codex session does not stop updates.
func startTUIAutoUpdate(ctx context.Context) {
	current, managed := helperruntime.Current()
	if !managed || !helperruntime.AutoUpdateAllowed() {
		return
	}
	var jitter [1]byte
	_, _ = rand.Read(jitter[:])
	go runTUIAutoUpdate(ctx, current, time.Duration(30+int(jitter[0])%90)*time.Second, update.DefaultAutoUpdateCheckInterval, update.CheckAndStageTUI)
}

func runTUIAutoUpdate(ctx context.Context, current helperruntime.Context, initial, interval time.Duration, check func(context.Context, helperruntime.Context) error) {
	timer := time.NewTimer(initial)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		checkCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
		_ = check(checkCtx, current)
		cancel()
		timer.Reset(interval)
	}
}
