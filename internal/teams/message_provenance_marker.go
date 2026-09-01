package teams

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

// The marker is deliberately a strict transport-boundary suffix. It must not
// be accepted from arbitrary prose or from an HTML comment in the middle of a
// user message, otherwise a user can accidentally (or deliberately) make the
// helper suppress a real inbound message.
// Outbox IDs may embed an opaque Teams chat ID such as
// "19:...@thread.v2". Keep the marker grammar narrow, but include the
// characters used by Graph's stable chat IDs so a real outbox never silently
// loses its exact recovery proof.
var helperOutboxProvenanceMarkerPattern = regexp.MustCompile(`(?i)<!--\s*codex-helper-outbox:([A-Za-z0-9._:@%+=~-]{1,200})\s*-->\s*$`)

func helperOutboxProvenanceMarker(outboxID string) string {
	outboxID = strings.TrimSpace(outboxID)
	if outboxID == "" || !helperOutboxProvenanceMarkerPattern.MatchString("<!-- codex-helper-outbox:"+outboxID+" -->") {
		return ""
	}
	return "<!-- codex-helper-outbox:" + outboxID + " -->"
}

func helperOutboxProvenanceMarkerID(content string) string {
	match := helperOutboxProvenanceMarkerPattern.FindStringSubmatch(strings.TrimSpace(content))
	if len(match) != 2 {
		return ""
	}
	return strings.TrimSpace(match[1])
}

func stripHelperOutboxProvenanceMarker(content string) string {
	return strings.TrimSpace(helperOutboxProvenanceMarkerPattern.ReplaceAllString(strings.TrimSpace(content), ""))
}

func (b *Bridge) recordHelperOutboxMarkerProvenance(ctx context.Context, chatID string, messageID string, outboxID string) error {
	if b == nil || b.store == nil || strings.TrimSpace(chatID) == "" || strings.TrimSpace(messageID) == "" || strings.TrimSpace(outboxID) == "" {
		return nil
	}
	now := time.Now()
	_, err := b.store.RecordMessageProvenance(ctx, teamstore.MessageProvenanceRecord{
		TeamsChatID:    strings.TrimSpace(chatID),
		TeamsMessageID: strings.TrimSpace(messageID),
		Origin:         teamstore.MessageOriginHelperOutbox,
		OutboxID:       strings.TrimSpace(outboxID),
		Kind:           "provenance-marker",
		Diagnostic:     "explicit helper outbox marker",
		CreatedAt:      now,
		UpdatedAt:      now,
	})
	if err != nil && b.out != nil {
		_, _ = fmt.Fprintf(b.out, "Teams helper provenance marker record error: %v\n", err)
	}
	return err
}
