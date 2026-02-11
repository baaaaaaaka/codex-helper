package codexhistory

import (
	"bufio"
	"bytes"
	"encoding/json"
	"io"
	"os"
	"strings"
	"time"
)

// sessionFileMeta holds lightweight metadata extracted by scanning a session file.
type sessionFileMeta struct {
	SessionID    string
	ProjectPath  string
	FirstPrompt  string
	MessageCount int
	CreatedAt    time.Time
	ModifiedAt   time.Time
}

// codexEnvelope is the outer JSON structure of every line in a Codex JSONL file.
// Payload is kept as RawMessage to avoid parsing heavy fields (reasoning, turn_context).
type codexEnvelope struct {
	Timestamp string          `json:"timestamp"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
}

type codexSessionMetaPayload struct {
	ID  string `json:"id"`
	Cwd string `json:"cwd"`
}

// codexResponsePayload is a unified struct for response_item payloads.
// Different fields are populated depending on the Type.
type codexResponsePayload struct {
	Type    string          `json:"type"`
	Role    string          `json:"role"`
	Phase   string          `json:"phase"`
	Name    string          `json:"name"`
	Content json.RawMessage `json:"content"`
}

func readSessionFileMeta(filePath string) (sessionFileMeta, error) {
	var meta sessionFileMeta
	f, err := os.Open(filePath)
	if err != nil {
		return meta, err
	}
	defer f.Close()

	reader := bufio.NewReaderSize(f, 64*1024)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil && err != io.EOF {
			return meta, err
		}
		line = bytes.TrimSpace(line)
		if len(line) > 0 {
			processMetaLine(line, &meta)
		}
		if err == io.EOF {
			break
		}
	}

	if meta.CreatedAt.IsZero() || meta.ModifiedAt.IsZero() {
		if st, err := os.Stat(filePath); err == nil {
			if meta.CreatedAt.IsZero() {
				meta.CreatedAt = st.ModTime()
			}
			if meta.ModifiedAt.IsZero() {
				meta.ModifiedAt = st.ModTime()
			}
		}
	}
	return meta, nil
}

func processMetaLine(line []byte, meta *sessionFileMeta) {
	var env codexEnvelope
	if json.Unmarshal(line, &env) != nil {
		return
	}

	ts := parseTimestamp(env.Timestamp)
	if !ts.IsZero() {
		if meta.CreatedAt.IsZero() || ts.Before(meta.CreatedAt) {
			meta.CreatedAt = ts
		}
		if meta.ModifiedAt.IsZero() || ts.After(meta.ModifiedAt) {
			meta.ModifiedAt = ts
		}
	}

	switch env.Type {
	case "session_meta":
		var payload codexSessionMetaPayload
		if json.Unmarshal(env.Payload, &payload) == nil {
			if meta.SessionID == "" && payload.ID != "" {
				meta.SessionID = payload.ID
			}
			if meta.ProjectPath == "" && payload.Cwd != "" {
				meta.ProjectPath = payload.Cwd
			}
		}

	case "response_item":
		var payload codexResponsePayload
		if json.Unmarshal(env.Payload, &payload) != nil {
			return
		}
		if payload.Type != "message" {
			return
		}
		role := strings.ToLower(payload.Role)
		if role != "user" && role != "assistant" {
			return
		}
		if role == "user" {
			text := extractContentText(payload.Content)
			if shouldSkipFirstPrompt(text) {
				return // system-injected user message, skip entirely
			}
			meta.MessageCount++
			if meta.FirstPrompt == "" {
				meta.FirstPrompt = text
			}
		} else {
			meta.MessageCount++
		}

	case "event_msg":
		// Could extract user_message for first prompt fallback,
		// but response_item/user is the canonical source.
	}
}

// parseSessionIDFromFilename extracts the session UUID from a Codex session filename.
// Format: rollout-2026-02-11T15-52-56-019c4bb0-5fdb-7352-9b9c-9efe77d2d60d.jsonl
// The UUID is the last 36 characters before .jsonl.
func parseSessionIDFromFilename(name string) string {
	name = strings.TrimSuffix(name, ".jsonl")
	// UUID is 36 chars: 8-4-4-4-12
	if len(name) < 36 {
		return ""
	}
	candidate := name[len(name)-36:]
	// Quick validation: check hyphens at expected positions
	if len(candidate) == 36 &&
		candidate[8] == '-' && candidate[13] == '-' &&
		candidate[18] == '-' && candidate[23] == '-' {
		return candidate
	}
	return ""
}

// parseTimestampFromFilename extracts the timestamp from a Codex session filename.
// Format: rollout-2026-02-11T15-52-56-{uuid}.jsonl
func parseTimestampFromFilename(name string) time.Time {
	name = strings.TrimSuffix(name, ".jsonl")
	// After "rollout-", there's a timestamp like 2026-02-11T15-52-56
	const prefix = "rollout-"
	if !strings.HasPrefix(name, prefix) {
		return time.Time{}
	}
	rest := name[len(prefix):]
	// Timestamp is 19 chars: 2026-02-11T15-52-56
	if len(rest) < 19 {
		return time.Time{}
	}
	tsStr := rest[:19]
	// Convert dashes in time portion back to colons: 15-52-56 -> 15:52:56
	// tsStr = "2026-02-11T15-52-56"
	if len(tsStr) >= 19 && tsStr[13] == '-' && tsStr[16] == '-' {
		b := []byte(tsStr)
		b[13] = ':'
		b[16] = ':'
		tsStr = string(b)
	}
	t, err := time.Parse("2006-01-02T15:04:05", tsStr)
	if err != nil {
		return time.Time{}
	}
	return t
}

func parseTimestamp(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t
	}
	return time.Time{}
}

// extractContentText extracts text from a Codex content array.
// Content is always [{type: "input_text"/"output_text", text: "..."}].
func extractContentText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}

	var items []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &items) == nil {
		var parts []string
		for _, item := range items {
			if item.Text != "" {
				parts = append(parts, item.Text)
			}
		}
		return strings.Join(parts, "\n")
	}

	// Fallback: try as plain string
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	return ""
}

func selectProjectPath(sessions []Session) string {
	counts := map[string]int{}
	for _, sess := range sessions {
		path := strings.TrimSpace(sess.ProjectPath)
		if path == "" {
			continue
		}
		counts[path]++
	}
	if len(counts) == 0 {
		return ""
	}
	best := ""
	bestCount := -1
	for path, count := range counts {
		if count > bestCount || (count == bestCount && strings.ToLower(path) < strings.ToLower(best)) {
			best = path
			bestCount = count
		}
	}
	return best
}
