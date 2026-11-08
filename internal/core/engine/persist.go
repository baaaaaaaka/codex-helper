package engine

import (
	"context"
	"fmt"

	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// persist retries only the local state transition, never the transport write.
// A full disk or busy store must not strand an accepted send as 'sending',
// nor let a native execution start without durable publication ownership.
func (e *Engine) persist(ctx context.Context, operation string, fn func(*store.Tx) error) error {
	for attempt := 0; ; attempt++ {
		err := e.update(fn)
		if err == nil {
			return nil
		}
		if finalStoreError(err) || ctx.Err() != nil {
			return err
		}
		if attempt < 3 || attempt%20 == 0 {
			e.logf("%s: %v; retaining state for retry", operation, err)
		}
		if !e.waitProgress(ctx, attempt) {
			return fmt.Errorf("%s: %w", operation, ctx.Err())
		}
	}
}
