package skills

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gofrs/flock"
)

func TestOperationLockHonorsContextWhileWaiting(t *testing.T) {
	mgr := newTestManager(t)
	lock := flock.New(mgr.Store.ConfigPath() + ".operation.lock")
	if err := lock.Lock(); err != nil {
		t.Fatalf("lock: %v", err)
	}
	defer func() { _ = lock.Unlock() }()

	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err := mgr.MigrateLegacySkills(ctx, MigrationOptions{IncludeBuiltins: false})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("migrate error = %v, want context deadline", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("operation lock ignored context; elapsed=%s", elapsed)
	}
}
