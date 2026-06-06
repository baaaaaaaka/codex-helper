package skills

import (
	"context"
	"fmt"
	"time"

	"github.com/gofrs/flock"
)

func (m *Manager) withOperationLock(ctx context.Context, fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	lock := flock.New(m.Store.ConfigPath() + ".operation.lock")
	locked, err := lock.TryLockContext(ctx, 50*time.Millisecond)
	if err != nil {
		return fmt.Errorf("lock skill operations: %w", err)
	}
	if !locked {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fmt.Errorf("lock skill operations: lock not acquired")
	}
	defer func() { _ = lock.Unlock() }()
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn()
}
