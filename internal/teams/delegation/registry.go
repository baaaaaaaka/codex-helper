package delegation

import (
	"encoding/base64"
	"encoding/json"
	"html"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/teamshtml"
)

const RecordMarker = "CXP_DELEGATION_RECORD_V1"

type ChatMessage struct {
	ID   string
	Body ChatMessageBody
}

type ChatMessageBody struct {
	Content string
}

func RenderRecordHTML(record Record) string {
	raw, _ := json.Marshal(record)
	encoded := base64.RawURLEncoding.EncodeToString(raw)
	return "<p>" + RecordMarker + " " + html.EscapeString(encoded) + "</p>"
}

func ParseRecordMessage(content string) (Record, bool) {
	text := teamshtml.PlainTextFromTeamsHTML(content)
	fields := strings.Fields(text)
	for i, field := range fields {
		if field != RecordMarker || i+1 >= len(fields) {
			continue
		}
		raw, err := base64.RawURLEncoding.DecodeString(fields[i+1])
		if err != nil {
			continue
		}
		var record Record
		if err := json.Unmarshal(raw, &record); err != nil {
			continue
		}
		if strings.TrimSpace(record.Kind) == "" || strings.TrimSpace(record.DelegationID) == "" || strings.TrimSpace(record.RecordID) == "" {
			continue
		}
		return record, true
	}
	return Record{}, false
}

func ObserveRecords(messages []ChatMessage) []Record {
	records := make([]Record, 0, len(messages))
	seen := map[string]struct{}{}
	for _, msg := range messages {
		record, ok := ParseRecordMessage(msg.Body.Content)
		if !ok {
			continue
		}
		if _, exists := seen[record.RecordID]; exists {
			continue
		}
		seen[record.RecordID] = struct{}{}
		records = append(records, record)
	}
	return records
}

func RecordsForID(records []Record, delegationID string) []Record {
	delegationID = strings.TrimSpace(delegationID)
	out := make([]Record, 0, len(records))
	for _, record := range records {
		if strings.TrimSpace(record.DelegationID) == delegationID {
			out = append(out, record)
		}
	}
	return out
}
