package store

import (
	"context"
	"testing"
)

func TestMessageLookupBatchMatchesSingleLookupAcrossBackends(t *testing.T) {
	ctx := context.Background()
	for _, useSQLite := range []bool{false, true} {
		t.Run(map[bool]string{false: "legacy-json", true: "sqlite"}[useSQLite], func(t *testing.T) {
			store := newTestStore(t)
			seedLargeMessageLookupState(t, store, 64, 64, 32)
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}

			cases := map[string][]string{
				"inbound-chat-000": {
					largeInboundMessageID(0),
					largeInboundMessageID(97),
					"message-not-present",
					largeInboundMessageID(0),
				},
				"provenance-user-chat": {
					largeUserProvenanceMessageID(0),
					largeUserProvenanceMessageID(2),
					"message-not-present",
				},
				"provenance-helper-chat": {
					largeHelperProvenanceMessageID(1),
					largeHelperProvenanceMessageID(3),
				},
			}
			for chatID, messageIDs := range cases {
				batch, err := store.MessageLookupBatch(ctx, chatID, messageIDs)
				if err != nil {
					t.Fatalf("MessageLookupBatch(%q): %v", chatID, err)
				}
				if len(batch) != len(uniqueMessageIDsForTest(messageIDs)) {
					t.Fatalf("batch %q length = %d, want %d unique IDs", chatID, len(batch), len(uniqueMessageIDsForTest(messageIDs)))
				}
				for _, messageID := range messageIDs {
					single, err := store.MessageLookup(ctx, chatID, messageID)
					if err != nil {
						t.Fatalf("MessageLookup(%q/%q): %v", chatID, messageID, err)
					}
					if !messageLookupEqual(batch[messageID], single) {
						t.Fatalf("batch lookup %q/%q = %#v, single = %#v", chatID, messageID, batch[messageID], single)
					}
				}
			}
		})
	}
}

func uniqueMessageIDsForTest(messageIDs []string) map[string]struct{} {
	unique := make(map[string]struct{}, len(messageIDs))
	for _, messageID := range messageIDs {
		if messageID != "" {
			unique[messageID] = struct{}{}
		}
	}
	return unique
}

func TestMessageLookupBatchReturnsMissingIDsAsZeroLookup(t *testing.T) {
	store := newTestStore(t)
	got, err := store.MessageLookupBatch(context.Background(), "chat-1", []string{"", "missing", "missing"})
	if err != nil {
		t.Fatalf("MessageLookupBatch: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("batch length = %d, want one normalized ID", len(got))
	}
	if got["missing"] != (MessageLookup{}) {
		t.Fatalf("missing lookup = %#v, want zero value", got["missing"])
	}
}

func BenchmarkMessageLookupPollWindowBatch(b *testing.B) {
	store := newBenchmarkStore(b)
	ctx := context.Background()
	seedLargeMessageLookupState(b, store, 7000, 0, 0)
	if _, err := store.MigrateLargeStateToSQLite(ctx, 0); err != nil {
		b.Fatalf("MigrateLargeStateToSQLite: %v", err)
	}
	messageIDs := make([]string, 0, 64)
	for i := 0; i < 64; i++ {
		messageIDs = append(messageIDs, largeInboundMessageID(i*97))
	}

	b.Run("scalar", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for _, messageID := range messageIDs {
				if _, err := store.MessageLookup(ctx, "inbound-chat-000", messageID); err != nil {
					b.Fatalf("MessageLookup: %v", err)
				}
			}
		}
		b.ReportMetric(float64(len(messageIDs)), "lookups/op")
	})
	b.Run("batch", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := store.MessageLookupBatch(ctx, "inbound-chat-000", messageIDs); err != nil {
				b.Fatalf("MessageLookupBatch: %v", err)
			}
		}
		b.ReportMetric(float64(len(messageIDs)), "lookups/op")
	})
}
