package teams

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
)

var helperOutboxProvenanceMarkerPattern = regexp.MustCompile(`(?i)<!--\s*codex-helper-outbox:([A-Za-z0-9._:-]{1,200})\s*-->`)

func helperOutboxProvenanceMarker(outboxID string) string {
	outboxID = strings.TrimSpace(outboxID)
	if outboxID == "" || !helperOutboxProvenanceMarkerPattern.MatchString("<!-- codex-helper-outbox:"+outboxID+" -->") {
		return ""
	}
	return "<!-- codex-helper-outbox:" + outboxID + " -->"
}

func helperOutboxProvenanceMarkerID(content string) string {
	match := helperOutboxProvenanceMarkerPattern.FindStringSubmatch(content)
	if len(match) != 2 {
		return ""
	}
	return strings.TrimSpace(match[1])
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
