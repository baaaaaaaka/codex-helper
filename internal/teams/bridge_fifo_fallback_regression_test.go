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
