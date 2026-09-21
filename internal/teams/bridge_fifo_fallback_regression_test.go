package teams

import (
	"errors"
	"fmt"
	"testing"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestIndeterminateOutboxFIFOIsDurableDeferred(t *testing.T) {
	cause := fmt.Errorf("bounded compatibility scan: %w", teamstore.ErrOutboxPredecessorIndeterminate)
	now := time.Now()
	deferredErr := deferIndeterminateOutboxFIFO("chat:fallback", cause)
	if !isOutboxDeliveryDeferred(deferredErr) {
		t.Fatalf("indeterminate FIFO error = %v, want delivery deferral", deferredErr)
	}
	if !errors.Is(deferredErr, teamstore.ErrOutboxPredecessorIndeterminate) {
		t.Fatalf("deferred FIFO error = %v, want original indeterminate cause", deferredErr)
	}
	var deferred outboxDeliveryDeferredError
	if !errors.As(deferredErr, &deferred) {
		t.Fatalf("deferred FIFO error = %T %v, want outboxDeliveryDeferredError", deferredErr, deferredErr)
	}
	if deferred.ChatID != "chat:fallback" || !deferred.Until.After(now) {
		t.Fatalf("deferred FIFO metadata = %#v, want chat and future retry gate", deferred)
	}
}

func TestStaleOutboxFIFOSnapshotUsesShortRetryGate(t *testing.T) {
	cause := fmt.Errorf("%w: %w: concurrent claim", teamstore.ErrOutboxPredecessorIndeterminate, teamstore.ErrOutboxFIFOSnapshotStale)
	started := time.Now()
	deferredErr := deferIndeterminateOutboxFIFO("chat:racing", cause)
	var deferred outboxDeliveryDeferredError
	if !errors.As(deferredErr, &deferred) {
		t.Fatalf("stale FIFO error = %T %v, want durable deferral", deferredErr, deferredErr)
	}
	if !errors.Is(deferredErr, teamstore.ErrOutboxFIFOSnapshotStale) {
		t.Fatalf("stale FIFO deferral = %v, want stale snapshot cause", deferredErr)
	}
	if !deferred.Until.After(started) || !deferred.Until.Before(started.Add(2*outboxFIFOSnapshotRetryBackoff)) {
		t.Fatalf("stale FIFO retry gate = %s, want approximately %s", deferred.Until, outboxFIFOSnapshotRetryBackoff)
	}

	now := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	if got := outboxRetryGateUntil(cause, now); !got.Equal(now.Add(outboxFIFOSnapshotRetryBackoff)) {
		t.Fatalf("stale FIFO retry gate from raw error = %s, want %s", got, now.Add(outboxFIFOSnapshotRetryBackoff))
	}
}
