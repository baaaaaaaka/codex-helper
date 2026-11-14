package transcript

import (
	"bytes"
	"encoding/json"
	"strings"
	"time"
)

func scheduledTurn(raw json.RawMessage, turnID string) *Schedule {
	if turnID == "" || !bytes.Contains(raw, []byte(`"turn_attribution"`)) {
		return nil
	}
	var p struct {
		Attribution struct {
			TurnID  string `json:"turn_id"`
			Trigger string `json:"turn_trigger"`
		} `json:"turn_attribution"`
	}
	if json.Unmarshal(raw, &p) != nil || p.Attribution.TurnID != turnID {
		return nil
	}
	switch p.Attribution.Trigger {
	case "automation_cron_scheduled", "automation_cron_run_now", "automation_heartbeat_scheduled", "automation_heartbeat_run_now":
		return &Schedule{}
	}
	return nil
}

// Schedule describes evidence attached to this turn, never thread settings.
// Missing evidence leaves the ordinary projection unchanged.
type Schedule struct {
	ID           string
	Instructions string
	Mixed        bool
	Input        bool // this output is the scheduled input itself
}

// heartbeat requires the host's provenance marker as well as the canonical
// bounded envelope. Quoted instructions alone are ordinary user input.
func heartbeat(raw json.RawMessage, text string) *Schedule {
	if len(text) > 3600 || !bytes.Contains(raw, []byte(`"internal_chat_message_metadata_passthrough"`)) {
		return nil
	}
	var p struct {
		Metadata struct {
			Kinds []string `json:"content_item_kinds"`
		} `json:"internal_chat_message_metadata_passthrough"`
	}
	if json.Unmarshal(raw, &p) != nil || len(p.Metadata.Kinds) != 1 || p.Metadata.Kinds[0] != "user.heartbeat" {
		return nil
	}
	s := strings.TrimSuffix(text, "\n")
	s, ok := strings.CutPrefix(s, "<heartbeat>\n  <automation_id>")
	if !ok {
		return nil
	}
	id, s, ok := strings.Cut(s, "</automation_id>\n  <current_time_iso>")
	if !ok || id == "" || len(id) > 128 {
		return nil
	}
	for _, c := range id {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_') {
			return nil
		}
	}
	timestamp, s, ok := strings.Cut(s, "</current_time_iso>\n  <instructions>\n")
	if !ok {
		return nil
	}
	if _, err := time.Parse(time.RFC3339Nano, timestamp); err != nil {
		return nil
	}
	s, ok = strings.CutSuffix(s, "\n  </instructions>\n</heartbeat>")
	if !ok {
		return nil
	}
	return &Schedule{ID: id, Instructions: s}
}
