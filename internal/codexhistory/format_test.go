package codexhistory

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// roleLabel
// ---------------------------------------------------------------------------

func TestRoleLabel(t *testing.T) {
	tests := []struct {
		role string
		want string
	}{
		{"user", "User"},
		{"assistant", "Assistant"},
		{"assistant_commentary", "Assistant (update)"},
		{"tool", "Tool"},
		{"tool_result", "Tool Result"},
		{"thinking", "Thinking"},
		{"unknown_role", "Message"},
		{"", "Message"},
	}
	for _, tt := range tests {
		t.Run(tt.role, func(t *testing.T) {
			if got := roleLabel(tt.role); got != tt.want {
				t.Errorf("roleLabel(%q) = %q, want %q", tt.role, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// truncateRunes
// ---------------------------------------------------------------------------

func TestTruncateRunes(t *testing.T) {
	tests := []struct {
		name     string
		s        string
		maxRunes int
		want     string
	}{
		{"zero limit", "hello", 0, ""},
		{"negative limit", "hello", -1, ""},
		{"under limit", "hi", 10, "hi"},
		{"exact limit", "hello", 5, "hello"},
		{"over limit", "hello world", 5, "hello…"},
		{"multibyte under", "你好世界", 10, "你好世界"},
		{"multibyte over", "你好世界测试", 4, "你好世界…"},
		{"empty string", "", 5, ""},
		{"single char over", "ab", 1, "a…"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := truncateRunes(tt.s, tt.maxRunes); got != tt.want {
				t.Errorf("truncateRunes(%q, %d) = %q, want %q", tt.s, tt.maxRunes, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// FormatMessages
// ---------------------------------------------------------------------------

func TestFormatMessages_Single(t *testing.T) {
	msgs := []Message{
		{Role: "user", Content: "hello"},
	}
	got := FormatMessages(msgs, 0)
	if !strings.Contains(got, "User:") {
		t.Errorf("missing role label: %q", got)
	}
	if !strings.Contains(got, "hello") {
		t.Errorf("missing content: %q", got)
	}
}

func TestFormatMessages_Multiple(t *testing.T) {
	msgs := []Message{
		{Role: "user", Content: "question"},
		{Role: "assistant", Content: "answer"},
		{Role: "tool", Content: "Tool: bash\nls"},
	}
	got := FormatMessages(msgs, 0)
	if !strings.Contains(got, "User:") {
		t.Errorf("missing User role")
	}
	if !strings.Contains(got, "Assistant:") {
		t.Errorf("missing Assistant role")
	}
	if !strings.Contains(got, "Tool:") {
		t.Errorf("missing Tool role")
	}
}

func TestFormatMessages_WithMaxLen(t *testing.T) {
	msgs := []Message{
		{Role: "user", Content: "a very long message that should be truncated"},
	}
	got := FormatMessages(msgs, 10)
	if !strings.Contains(got, "…") {
		t.Errorf("expected truncation ellipsis in: %q", got)
	}
}

func TestFormatMessages_ZeroMaxLen(t *testing.T) {
	longContent := strings.Repeat("x", 1000)
	msgs := []Message{
		{Role: "user", Content: longContent},
	}
	got := FormatMessages(msgs, 0)
	if strings.Contains(got, "…") {
		t.Errorf("maxLen=0 should not truncate")
	}
	if !strings.Contains(got, longContent) {
		t.Errorf("full content should be preserved")
	}
}

func TestFormatMessages_Empty(t *testing.T) {
	got := FormatMessages(nil, 0)
	if got != "" {
		t.Errorf("expected empty string for nil msgs, got %q", got)
	}
}

func TestFormatMessages_AllRoles(t *testing.T) {
	msgs := []Message{
		{Role: "user", Content: "q"},
		{Role: "assistant", Content: "a"},
		{Role: "assistant_commentary", Content: "c"},
		{Role: "tool", Content: "t"},
		{Role: "tool_result", Content: "r"},
		{Role: "thinking", Content: "th"},
	}
	got := FormatMessages(msgs, 0)
	for _, want := range []string{"User:", "Assistant:", "Assistant (update):", "Tool:", "Tool Result:", "Thinking:"} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in output: %q", want, got)
		}
	}
}

// ---------------------------------------------------------------------------
// FormatSession
// ---------------------------------------------------------------------------

func TestFormatSession_AllFields(t *testing.T) {
	now := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	s := Session{
		SessionID:   "test-session-id",
		ProjectPath: "/home/user/project",
		Summary:     "test summary",
		CreatedAt:   now,
		ModifiedAt:  now.Add(time.Hour),
	}
	got := FormatSession(s)
	if !strings.Contains(got, "Session: test-session-id") {
		t.Errorf("missing session id: %q", got)
	}
	if !strings.Contains(got, "Project: /home/user/project") {
		t.Errorf("missing project path: %q", got)
	}
	if !strings.Contains(got, "Summary: test summary") {
		t.Errorf("missing summary: %q", got)
	}
	if !strings.Contains(got, "Created:") {
		t.Errorf("missing created: %q", got)
	}
	if !strings.Contains(got, "Modified:") {
		t.Errorf("missing modified: %q", got)
	}
}

func TestFormatSession_MinimalFields(t *testing.T) {
	s := Session{
		SessionID: "min-id",
	}
	got := FormatSession(s)
	if !strings.Contains(got, "Session: min-id") {
		t.Errorf("missing session id: %q", got)
	}
	// Should not contain Project/Summary/Created/Modified lines
	if strings.Contains(got, "Project:") {
		t.Errorf("should not have Project line: %q", got)
	}
	if strings.Contains(got, "Summary:") {
		t.Errorf("should not have Summary line: %q", got)
	}
}

func TestFormatSession_WithFilePath(t *testing.T) {
	// Create a real session file so FormatSession can read it.
	dir := t.TempDir()
	f := filepath.Join(dir, "session.jsonl")
	content := `{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"test question"}]}}
{"timestamp":"2026-01-01T00:01:00Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"test answer"}]}}
`
	if err := os.WriteFile(f, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	s := Session{
		SessionID: "with-file",
		FilePath:  f,
	}
	got := FormatSession(s)
	if !strings.Contains(got, "User:") {
		t.Errorf("should contain formatted messages: %q", got)
	}
	if !strings.Contains(got, "test question") {
		t.Errorf("should contain user message: %q", got)
	}
	if !strings.Contains(got, "Assistant:") {
		t.Errorf("should contain assistant label: %q", got)
	}
}

func TestFormatSession_NonexistentFilePath(t *testing.T) {
	s := Session{
		SessionID: "bad-file",
		FilePath:  "/nonexistent/session.jsonl",
	}
	got := FormatSession(s)
	// Should not panic, just not include messages section
	if !strings.Contains(got, "Session: bad-file") {
		t.Errorf("should still format header: %q", got)
	}
}
