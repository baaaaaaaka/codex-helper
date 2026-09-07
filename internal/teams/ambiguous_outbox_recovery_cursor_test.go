package teams

import (
	"testing"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

func TestAmbiguousOutboxRecoveryCursorPreservesIDOnlyLegacyRows(t *testing.T) {
	cursor := teamstore.PendingOutboxCursor{ID: "outbox:legacy-zero-created"}
	encoded := encodeAmbiguousOutboxRecoveryCursor(cursor)
	if encoded != "0"+ambiguousOutboxRecoveryCursorSeparator+cursor.ID {
		t.Fatalf("encoded ID-only cursor = %q, want explicit zero-time sentinel", encoded)
	}
	decoded := decodeAmbiguousOutboxRecoveryCursor(encoded)
	if decoded.ID != cursor.ID || !decoded.CreatedAt.IsZero() {
		t.Fatalf("decoded ID-only cursor = %#v, want ID %q with zero time", decoded, cursor.ID)
	}

	// The decoded cursor must remain non-zero even though its timestamp is zero;
	// otherwise a restart would treat it as the beginning of the scan and
	// repeatedly revisit the same legacy rows.
	if decoded.IsZero() {
		t.Fatalf("decoded ID-only cursor was treated as the beginning of the scan")
	}
}

func TestAmbiguousOutboxRecoveryCursorRejectsMalformedValues(t *testing.T) {
	for _, value := range []string{"", "0", "not-a-time\x00outbox:id", "2026-09-07T00:00:00Z\x00"} {
		if got := decodeAmbiguousOutboxRecoveryCursor(value); !got.IsZero() {
			t.Fatalf("decode malformed cursor %q = %#v, want zero cursor", value, got)
		}
	}
}
