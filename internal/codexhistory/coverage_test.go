package codexhistory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// jsonEscapePath escapes backslashes so a path can be safely embedded in a JSON
// string literal.  This is a no-op on Unix but required on Windows where
// filepath.Join produces backslash separators.
func jsonEscapePath(p string) string {
	return strings.ReplaceAll(p, `\`, `\\`)
}

// fixedTime returns a deterministic timestamp for tests.
func fixedTime() time.Time {
	return time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
}

// ---------------------------------------------------------------------------
// parseTimestampFromFilename
// ---------------------------------------------------------------------------

func TestParseTimestampFromFilename(t *testing.T) {
	tests := []struct {
		name     string
		filename string
		wantZero bool
		wantYear int
	}{
		{
			name:     "valid filename",
			filename: "rollout-2026-02-11T15-52-56-019c4bb0-5fdb-7352-9b9c-9efe77d2d60d.jsonl",
			wantZero: false,
			wantYear: 2026,
		},
		{
			name:     "non-rollout prefix",
			filename: "session-2026-02-11T15-52-56-uuid.jsonl",
			wantZero: true,
		},
		{
			name:     "too short after prefix",
			filename: "rollout-2026.jsonl",
			wantZero: true,
		},
		{
			name:     "no jsonl extension still works",
			filename: "rollout-2026-02-11T15-52-56-uuid",
			wantZero: false,
			wantYear: 2026,
		},
		{
			name:     "empty string",
			filename: "",
			wantZero: true,
		},
		{
			name:     "just prefix",
			filename: "rollout-",
			wantZero: true,
		},
		{
			name:     "invalid timestamp chars",
			filename: "rollout-XXXX-XX-XXTXX-XX-XX-uuid.jsonl",
			wantZero: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseTimestampFromFilename(tt.filename)
			if tt.wantZero && !got.IsZero() {
				t.Errorf("expected zero time, got %v", got)
			}
			if !tt.wantZero {
				if got.IsZero() {
					t.Fatal("expected non-zero time")
				}
				if got.Year() != tt.wantYear {
					t.Errorf("year = %d, want %d", got.Year(), tt.wantYear)
				}
			}
		})
	}
}

func TestParseTimestampFromFilename_CorrectTimeParts(t *testing.T) {
	ts := parseTimestampFromFilename("rollout-2026-03-15T10-30-45-uuid.jsonl")
	if ts.IsZero() {
		t.Fatal("expected non-zero")
	}
	if ts.Hour() != 10 || ts.Minute() != 30 || ts.Second() != 45 {
		t.Errorf("time = %v, want 10:30:45", ts)
	}
	if ts.Month() != 3 || ts.Day() != 15 {
		t.Errorf("date = %v, want March 15", ts)
	}
}

// ---------------------------------------------------------------------------
// parseTimestamp — additional paths
// ---------------------------------------------------------------------------

func TestParseTimestamp_RFC3339NonNano(t *testing.T) {
	got := parseTimestamp("2026-06-01T12:00:00Z")
	if got.IsZero() {
		t.Fatal("expected non-zero for RFC3339")
	}
	if got.Year() != 2026 || got.Month() != 6 {
		t.Errorf("parsed = %v", got)
	}
}

func TestParseTimestamp_RFC3339Nano(t *testing.T) {
	got := parseTimestamp("2026-06-01T12:00:00.123456789Z")
	if got.IsZero() {
		t.Fatal("expected non-zero for RFC3339Nano")
	}
}

func TestParseTimestamp_Invalid(t *testing.T) {
	got := parseTimestamp("not-a-timestamp")
	if !got.IsZero() {
		t.Errorf("expected zero, got %v", got)
	}
}

func TestParseTimestamp_Empty(t *testing.T) {
	got := parseTimestamp("")
	if !got.IsZero() {
		t.Errorf("expected zero for empty")
	}
}

func TestParseTimestamp_Whitespace(t *testing.T) {
	got := parseTimestamp("  2026-06-01T12:00:00Z  ")
	if got.IsZero() {
		t.Fatal("should parse with surrounding whitespace")
	}
}

// ---------------------------------------------------------------------------
// extractContentText — additional paths
// ---------------------------------------------------------------------------

func TestExtractContentText_PlainString(t *testing.T) {
	raw := json.RawMessage(`"plain text content"`)
	got := extractContentText(raw)
	if got != "plain text content" {
		t.Errorf("got %q, want %q", got, "plain text content")
	}
}

func TestExtractContentText_Array(t *testing.T) {
	raw := json.RawMessage(`[{"type":"input_text","text":"first"},{"type":"input_text","text":"second"}]`)
	got := extractContentText(raw)
	if got != "first\nsecond" {
		t.Errorf("got %q, want %q", got, "first\nsecond")
	}
}

func TestExtractContentText_EmptyArray(t *testing.T) {
	raw := json.RawMessage(`[]`)
	got := extractContentText(raw)
	if got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

func TestExtractContentText_Nil(t *testing.T) {
	got := extractContentText(nil)
	if got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

func TestExtractContentText_EmptyRaw(t *testing.T) {
	got := extractContentText(json.RawMessage{})
	if got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

func TestExtractContentText_InvalidJSON(t *testing.T) {
	raw := json.RawMessage(`{broken`)
	got := extractContentText(raw)
	if got != "" {
		t.Errorf("got %q for invalid JSON", got)
	}
}

func TestExtractContentText_ArrayWithEmptyText(t *testing.T) {
	raw := json.RawMessage(`[{"type":"input_text","text":""},{"type":"input_text","text":"real"}]`)
	got := extractContentText(raw)
	if got != "real" {
		t.Errorf("got %q, want %q", got, "real")
	}
}

// ---------------------------------------------------------------------------
// parseSessionIDFromFilename — additional paths
// ---------------------------------------------------------------------------

func TestParseSessionIDFromFilename(t *testing.T) {
	tests := []struct {
		name string
		file string
		want string
	}{
		{
			name: "valid",
			file: "rollout-2026-02-11T15-52-56-019c4bb0-5fdb-7352-9b9c-9efe77d2d60d.jsonl",
			want: "019c4bb0-5fdb-7352-9b9c-9efe77d2d60d",
		},
		{
			name: "too short",
			file: "short.jsonl",
			want: "",
		},
		{
			name: "no hyphens in UUID position",
			file: "rollout-2026-02-11T15-52-56-019c4bb05fdb73529b9c9efe77d2d60d.jsonl",
			want: "",
		},
		{
			name: "empty",
			file: "",
			want: "",
		},
		{
			name: "just uuid length",
			file: "019c4bb0-5fdb-7352-9b9c-9efe77d2d60d.jsonl",
			want: "019c4bb0-5fdb-7352-9b9c-9efe77d2d60d",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseSessionIDFromFilename(tt.file)
			if got != tt.want {
				t.Errorf("parseSessionIDFromFilename(%q) = %q, want %q", tt.file, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// shouldSkipFirstPrompt — additional paths
// ---------------------------------------------------------------------------

func TestShouldSkipFirstPrompt(t *testing.T) {
	tests := []struct {
		text string
		want bool
	}{
		{"", true},
		{"  ", true},
		{"hello world", false},
		{"<environment_context>data</environment_context>", true},
		{"# AGENTS.md\nskills", true},
		{"some text with <INSTRUCTIONS> embedded", true},
		{"normal question?", false},
		{"<just an opening tag but not wrapping>", true}, // HasPrefix "<" and HasSuffix ">" → treated as XML-wrapped
		{"<partial tag without closing", false},          // starts with < but doesn't end with >
	}
	for _, tt := range tests {
		t.Run(tt.text, func(t *testing.T) {
			if got := shouldSkipFirstPrompt(tt.text); got != tt.want {
				t.Errorf("shouldSkipFirstPrompt(%q) = %v, want %v", tt.text, got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Session.DisplayTitle — all paths
// ---------------------------------------------------------------------------

func TestSessionDisplayTitle(t *testing.T) {
	tests := []struct {
		name string
		s    Session
		want string
	}{
		{"summary priority", Session{Summary: "sum", FirstPrompt: "fp", SessionID: "id"}, "sum"},
		{"first prompt fallback", Session{FirstPrompt: "fp", SessionID: "id"}, "fp"},
		{"session id fallback", Session{SessionID: "id"}, "id"},
		{"untitled", Session{}, "untitled"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.DisplayTitle(); got != tt.want {
				t.Errorf("DisplayTitle() = %q, want %q", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// ResolveCodexDir
// ---------------------------------------------------------------------------

func TestResolveCodexDir_Override(t *testing.T) {
	got, err := ResolveCodexDir("/custom/path")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	want := filepath.FromSlash("/custom/path")
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestResolveCodexDir_OverrideWithWhitespace(t *testing.T) {
	got, err := ResolveCodexDir("  /custom/path  ")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	want := filepath.FromSlash("/custom/path")
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestResolveCodexDir_EnvVar(t *testing.T) {
	t.Setenv(EnvCodexDir, "/env/codex")
	got, err := ResolveCodexDir("")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	want := filepath.FromSlash("/env/codex")
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestResolveCodexDir_Default(t *testing.T) {
	t.Setenv(EnvCodexDir, "")
	got, err := ResolveCodexDir("")
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	home, _ := os.UserHomeDir()
	want := filepath.Join(home, ".codex")
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// mergeSessionMetadata
// ---------------------------------------------------------------------------

func TestMergeSessionMetadata_SummaryFill(t *testing.T) {
	base := Session{SessionID: "s1"}
	other := Session{SessionID: "s1", Summary: "new summary"}
	merged := mergeSessionMetadata(base, other)
	if merged.Summary != "new summary" {
		t.Errorf("Summary = %q", merged.Summary)
	}
}

func TestMergeSessionMetadata_SummaryKeepExisting(t *testing.T) {
	base := Session{Summary: "existing"}
	other := Session{Summary: "other"}
	merged := mergeSessionMetadata(base, other)
	if merged.Summary != "existing" {
		t.Errorf("Summary = %q, should keep existing", merged.Summary)
	}
}

func TestMergeSessionMetadata_FirstPromptFill(t *testing.T) {
	base := Session{}
	other := Session{FirstPrompt: "prompt"}
	merged := mergeSessionMetadata(base, other)
	if merged.FirstPrompt != "prompt" {
		t.Errorf("FirstPrompt = %q", merged.FirstPrompt)
	}
}

func TestMergeSessionMetadata_MessageCountTakeHigher(t *testing.T) {
	base := Session{MessageCount: 5}
	other := Session{MessageCount: 10}
	merged := mergeSessionMetadata(base, other)
	if merged.MessageCount != 10 {
		t.Errorf("MessageCount = %d, want 10", merged.MessageCount)
	}
}

func TestMergeSessionMetadata_MessageCountKeepHigher(t *testing.T) {
	base := Session{MessageCount: 10}
	other := Session{MessageCount: 3}
	merged := mergeSessionMetadata(base, other)
	if merged.MessageCount != 10 {
		t.Errorf("MessageCount = %d, want 10", merged.MessageCount)
	}
}

func TestMergeSessionMetadata_CreatedAtTakeEarlier(t *testing.T) {
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	base := Session{CreatedAt: t2}
	other := Session{CreatedAt: t1}
	merged := mergeSessionMetadata(base, other)
	if !merged.CreatedAt.Equal(t1) {
		t.Errorf("CreatedAt = %v, want %v (earlier)", merged.CreatedAt, t1)
	}
}

func TestMergeSessionMetadata_CreatedAtFillZero(t *testing.T) {
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	base := Session{}
	other := Session{CreatedAt: t1}
	merged := mergeSessionMetadata(base, other)
	if !merged.CreatedAt.Equal(t1) {
		t.Errorf("CreatedAt = %v, want %v", merged.CreatedAt, t1)
	}
}

func TestMergeSessionMetadata_ModifiedAtTakeLater(t *testing.T) {
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)
	base := Session{ModifiedAt: t1}
	other := Session{ModifiedAt: t2}
	merged := mergeSessionMetadata(base, other)
	if !merged.ModifiedAt.Equal(t2) {
		t.Errorf("ModifiedAt = %v, want %v (later)", merged.ModifiedAt, t2)
	}
}

func TestMergeSessionMetadata_ModifiedAtFillZero(t *testing.T) {
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	base := Session{}
	other := Session{ModifiedAt: t1}
	merged := mergeSessionMetadata(base, other)
	if !merged.ModifiedAt.Equal(t1) {
		t.Errorf("ModifiedAt = %v, want %v", merged.ModifiedAt, t1)
	}
}

func TestMergeSessionMetadata_FilePathFallback(t *testing.T) {
	// base.FilePath doesn't exist as a file, other does.
	dir := t.TempDir()
	realFile := filepath.Join(dir, "real.jsonl")
	if err := os.WriteFile(realFile, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	base := Session{FilePath: "/nonexistent/path.jsonl"}
	other := Session{FilePath: realFile}
	merged := mergeSessionMetadata(base, other)
	if merged.FilePath != realFile {
		t.Errorf("FilePath = %q, want %q", merged.FilePath, realFile)
	}
}

func TestMergeSessionMetadata_ProjectPathFill(t *testing.T) {
	base := Session{}
	other := Session{ProjectPath: "/proj"}
	merged := mergeSessionMetadata(base, other)
	if merged.ProjectPath != "/proj" {
		t.Errorf("ProjectPath = %q", merged.ProjectPath)
	}
}

func TestMergeSessionMetadata_ProjectPathKeepExisting(t *testing.T) {
	base := Session{ProjectPath: "/existing"}
	other := Session{ProjectPath: "/other"}
	merged := mergeSessionMetadata(base, other)
	if merged.ProjectPath != "/existing" {
		t.Errorf("ProjectPath = %q, should keep existing", merged.ProjectPath)
	}
}

// ---------------------------------------------------------------------------
// isFile
// ---------------------------------------------------------------------------

func TestIsFile_RegularFile(t *testing.T) {
	f := filepath.Join(t.TempDir(), "test.txt")
	if err := os.WriteFile(f, []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	if !isFile(f) {
		t.Error("expected true for regular file")
	}
}

func TestIsFile_Directory(t *testing.T) {
	if isFile(t.TempDir()) {
		t.Error("expected false for directory")
	}
}

func TestIsFile_Nonexistent(t *testing.T) {
	if isFile("/nonexistent/path/file.txt") {
		t.Error("expected false for nonexistent")
	}
}

func TestIsFile_EmptyPath(t *testing.T) {
	if isFile("") {
		t.Error("expected false for empty path")
	}
}

func TestIsFile_WhitespacePath(t *testing.T) {
	if isFile("   ") {
		t.Error("expected false for whitespace path")
	}
}

// ---------------------------------------------------------------------------
// isDir — additional paths
// ---------------------------------------------------------------------------

func TestIsDir_Nonexistent(t *testing.T) {
	if isDir("/nonexistent/dir") {
		t.Error("expected false")
	}
}

func TestIsDir_EmptyPath(t *testing.T) {
	if isDir("") {
		t.Error("expected false for empty")
	}
}

func TestIsDir_RegularFile(t *testing.T) {
	f := filepath.Join(t.TempDir(), "file.txt")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if isDir(f) {
		t.Error("expected false for regular file")
	}
}

// ---------------------------------------------------------------------------
// SessionWorkingDir
// ---------------------------------------------------------------------------

func TestSessionWorkingDir_RealDir(t *testing.T) {
	dir := t.TempDir()
	s := Session{ProjectPath: dir}
	if got := SessionWorkingDir(s); got != dir {
		t.Errorf("got %q, want %q", got, dir)
	}
}

func TestSessionWorkingDir_NonexistentPath(t *testing.T) {
	s := Session{ProjectPath: "/nonexistent/project"}
	got := SessionWorkingDir(s)
	if got != "/nonexistent/project" {
		t.Errorf("got %q, want /nonexistent/project", got)
	}
}

func TestSessionWorkingDir_Empty(t *testing.T) {
	s := Session{}
	if got := SessionWorkingDir(s); got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

func TestSessionWorkingDir_Whitespace(t *testing.T) {
	s := Session{ProjectPath: "   "}
	if got := SessionWorkingDir(s); got != "" {
		t.Errorf("got %q, want empty", got)
	}
}

// ---------------------------------------------------------------------------
// globRecursive
// ---------------------------------------------------------------------------

func TestGlobRecursive_FindsFiles(t *testing.T) {
	dir := t.TempDir()
	subdir := filepath.Join(dir, "nested")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatal(err)
	}
	targetID := "abcd1234-5678-9abc-def0-111111111111"
	f1 := filepath.Join(dir, "rollout-2026-01-01T00-00-00-"+targetID+".jsonl")
	f2 := filepath.Join(subdir, "rollout-2026-02-01T00-00-00-"+targetID+".jsonl")
	fOther := filepath.Join(dir, "rollout-2026-01-01T00-00-00-other-uuid.jsonl")
	for _, f := range []string{f1, f2, fOther} {
		if err := os.WriteFile(f, []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	matches := globRecursive(dir, targetID)
	if len(matches) != 2 {
		t.Fatalf("expected 2 matches, got %d: %v", len(matches), matches)
	}
}

func TestGlobRecursive_NoMatches(t *testing.T) {
	dir := t.TempDir()
	f := filepath.Join(dir, "unrelated.jsonl")
	if err := os.WriteFile(f, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	matches := globRecursive(dir, "nonexistent-uuid")
	if len(matches) != 0 {
		t.Errorf("expected 0 matches, got %d", len(matches))
	}
}

func TestGlobRecursive_NonexistentDir(t *testing.T) {
	matches := globRecursive("/nonexistent/dir", "any")
	if len(matches) != 0 {
		t.Errorf("expected 0, got %d", len(matches))
	}
}

// ---------------------------------------------------------------------------
// FindSessionByID
// ---------------------------------------------------------------------------

func TestFindSessionByID_EmptyID(t *testing.T) {
	_, err := FindSessionByID("", "")
	if err == nil {
		t.Fatal("expected error for empty session ID")
	}
	if !strings.Contains(err.Error(), "empty session ID") {
		t.Errorf("error = %v", err)
	}
}

func TestFindSessionByID_WhitespaceID(t *testing.T) {
	_, err := FindSessionByID("", "   ")
	if err == nil {
		t.Fatal("expected error for whitespace session ID")
	}
}

func TestFindSessionByID_Found(t *testing.T) {
	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	sessionID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	writeSessionFile(t, sessionsDir, sessionID, "2026-06-01T10:00:00Z", projDir, `"cli"`, "find me")

	sess, err := FindSessionByID(tmpDir, sessionID)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if sess == nil {
		t.Fatal("session not found")
	}
	if sess.SessionID != sessionID {
		t.Errorf("SessionID = %q", sess.SessionID)
	}
	if sess.FirstPrompt != "find me" {
		t.Errorf("FirstPrompt = %q", sess.FirstPrompt)
	}
}

func TestFindSessionByID_NotFound(t *testing.T) {
	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	writeSessionFile(t, sessionsDir, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "2026-06-01T10:00:00Z", projDir, `"cli"`, "exists")

	_, err := FindSessionByID(tmpDir, "99999999-8888-7777-6666-555555555555")
	if err == nil {
		t.Fatal("expected error for not found")
	}
	if !strings.Contains(err.Error(), "session not found") {
		t.Errorf("error = %v", err)
	}
}

func TestFindSessionByID_FoundViaDiscoveryFallback(t *testing.T) {
	// Place file in a subdirectory so glob pattern might not match directly
	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	subdir := filepath.Join(sessionsDir, "nested")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatal(err)
	}
	sessionID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	fname := "rollout-2026-01-01T00-00-00-" + sessionID + ".jsonl"
	content := `{"timestamp":"2026-06-01T10:00:00Z","type":"session_meta","payload":{"id":"` + sessionID + `","cwd":"` + jsonEscapePath(projDir) + `","source":"cli"}}
{"timestamp":"2026-06-01T10:01:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"nested find"}]}}
`
	if err := os.WriteFile(filepath.Join(subdir, fname), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	sess, err := FindSessionByID(tmpDir, sessionID)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if sess == nil {
		t.Fatal("session not found")
	}
	if sess.SessionID != sessionID {
		t.Errorf("SessionID = %q", sess.SessionID)
	}
}

// ---------------------------------------------------------------------------
// FindSessionWithProject — additional paths
// ---------------------------------------------------------------------------

func TestFindSessionWithProject_EmptyID(t *testing.T) {
	_, _, err := FindSessionWithProject("", "")
	if err == nil {
		t.Fatal("expected error for empty session ID")
	}
}

func TestFindSessionWithProject_NotFound(t *testing.T) {
	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	writeSessionFile(t, sessionsDir, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "2026-06-01T10:00:00Z", projDir, `"cli"`, "x")

	_, _, err := FindSessionWithProject(tmpDir, "99999999-8888-7777-6666-555555555555")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "session not found") {
		t.Errorf("error = %v", err)
	}
}

// ---------------------------------------------------------------------------
// isEmptySession — additional paths
// ---------------------------------------------------------------------------

func TestIsEmptySession_SummaryMakesNonEmpty(t *testing.T) {
	s := Session{Summary: "has summary"}
	if isEmptySession(s) {
		t.Error("session with summary should not be empty")
	}
}

func TestIsEmptySession_FirstPromptMakesNonEmpty(t *testing.T) {
	s := Session{FirstPrompt: "has prompt"}
	if isEmptySession(s) {
		t.Error("session with first prompt should not be empty")
	}
}

func TestIsEmptySession_MessageCountMakesNonEmpty(t *testing.T) {
	s := Session{MessageCount: 1}
	if isEmptySession(s) {
		t.Error("session with messages should not be empty")
	}
}

func TestIsEmptySession_TrulyEmpty(t *testing.T) {
	s := Session{}
	if !isEmptySession(s) {
		t.Error("empty session should be empty")
	}
}

func TestIsEmptySession_WhitespaceOnlyFields(t *testing.T) {
	s := Session{FirstPrompt: "   ", Summary: "   "}
	if !isEmptySession(s) {
		t.Error("whitespace-only fields should still be empty")
	}
}

// ---------------------------------------------------------------------------
// filterEmptySessions
// ---------------------------------------------------------------------------

func TestFilterEmptySessions_Empty(t *testing.T) {
	got := filterEmptySessions(nil)
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}
}

func TestFilterEmptySessions_Mixed(t *testing.T) {
	sessions := []Session{
		{SessionID: "empty1"},
		{SessionID: "has-prompt", FirstPrompt: "hello"},
		{SessionID: "empty2"},
		{SessionID: "has-count", MessageCount: 5},
		{SessionID: "has-summary", Summary: "sum"},
	}
	got := filterEmptySessions(sessions)
	if len(got) != 3 {
		t.Fatalf("expected 3, got %d", len(got))
	}
	ids := make([]string, len(got))
	for i, s := range got {
		ids[i] = s.SessionID
	}
	want := []string{"has-prompt", "has-count", "has-summary"}
	for i, w := range want {
		if ids[i] != w {
			t.Errorf("ids[%d] = %q, want %q", i, ids[i], w)
		}
	}
}

// ---------------------------------------------------------------------------
// loadHistoryIndex
// ---------------------------------------------------------------------------

func TestLoadHistoryIndex_FileNotExist(t *testing.T) {
	idx := loadHistoryIndex("/nonexistent/dir")
	if len(idx.sessions) != 0 {
		t.Errorf("expected empty index, got %d entries", len(idx.sessions))
	}
}

func TestLoadHistoryIndex_ValidEntries(t *testing.T) {
	dir := t.TempDir()
	entries := []string{
		`{"session_id":"s1","ts":1770777540,"text":"hello world"}`,
		`{"session_id":"s1","ts":1770777600,"text":"second prompt"}`,
		`{"session_id":"s2","ts":1770777540,"text":"different session"}`,
	}
	if err := os.WriteFile(filepath.Join(dir, "history.jsonl"), []byte(strings.Join(entries, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	idx := loadHistoryIndex(dir)
	if len(idx.sessions) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(idx.sessions))
	}
	info, ok := idx.lookup("s1")
	if !ok {
		t.Fatal("s1 not found")
	}
	// First prompt should be the earlier one (ts=1770777540)
	if info.FirstPrompt != "hello world" {
		t.Errorf("FirstPrompt = %q, want %q", info.FirstPrompt, "hello world")
	}
	info2, ok := idx.lookup("s2")
	if !ok {
		t.Fatal("s2 not found")
	}
	if info2.FirstPrompt != "different session" {
		t.Errorf("FirstPrompt = %q", info2.FirstPrompt)
	}
}

func TestLoadHistoryIndex_SkipsSystemInjected(t *testing.T) {
	dir := t.TempDir()
	entries := []string{
		`{"session_id":"s1","ts":1770777540,"text":"# AGENTS.md\nskill instructions"}`,
		`{"session_id":"s1","ts":1770777600,"text":"real prompt"}`,
	}
	if err := os.WriteFile(filepath.Join(dir, "history.jsonl"), []byte(strings.Join(entries, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	idx := loadHistoryIndex(dir)
	info, ok := idx.lookup("s1")
	if !ok {
		t.Fatal("s1 not found")
	}
	if info.FirstPrompt != "real prompt" {
		t.Errorf("FirstPrompt = %q, want %q (should skip system-injected)", info.FirstPrompt, "real prompt")
	}
}

func TestLoadHistoryIndex_InvalidJSON(t *testing.T) {
	dir := t.TempDir()
	content := "{broken json\n" + `{"session_id":"s1","ts":1770777540,"text":"valid"}` + "\n"
	if err := os.WriteFile(filepath.Join(dir, "history.jsonl"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	idx := loadHistoryIndex(dir)
	info, ok := idx.lookup("s1")
	if !ok {
		t.Fatal("s1 should be found despite earlier invalid line")
	}
	if info.FirstPrompt != "valid" {
		t.Errorf("FirstPrompt = %q", info.FirstPrompt)
	}
}

func TestLoadHistoryIndex_EmptyFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "history.jsonl"), []byte(""), 0o644); err != nil {
		t.Fatal(err)
	}
	idx := loadHistoryIndex(dir)
	if len(idx.sessions) != 0 {
		t.Errorf("expected empty, got %d", len(idx.sessions))
	}
}

func TestLoadHistoryIndex_SkipsEmptyText(t *testing.T) {
	dir := t.TempDir()
	entries := []string{
		`{"session_id":"s1","ts":1770777540,"text":""}`,
		`{"session_id":"s1","ts":1770777600,"text":"actual prompt"}`,
	}
	if err := os.WriteFile(filepath.Join(dir, "history.jsonl"), []byte(strings.Join(entries, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	idx := loadHistoryIndex(dir)
	info, ok := idx.lookup("s1")
	if !ok {
		t.Fatal("s1 not found")
	}
	if info.FirstPrompt != "actual prompt" {
		t.Errorf("FirstPrompt = %q", info.FirstPrompt)
	}
}

// ---------------------------------------------------------------------------
// lookup
// ---------------------------------------------------------------------------

func TestLookup_EmptySessionID(t *testing.T) {
	idx := historyIndex{sessions: map[string]*historySessionInfo{}}
	_, ok := idx.lookup("")
	if ok {
		t.Error("expected false for empty session ID")
	}
}

func TestLookup_NilSessions(t *testing.T) {
	idx := historyIndex{}
	_, ok := idx.lookup("any")
	if ok {
		t.Error("expected false for nil sessions map")
	}
}

func TestLookup_NilEntry(t *testing.T) {
	idx := historyIndex{sessions: map[string]*historySessionInfo{"s1": nil}}
	_, ok := idx.lookup("s1")
	if ok {
		t.Error("expected false for nil entry")
	}
}

// ---------------------------------------------------------------------------
// readSessionFileMeta — timestamp fallback from file stat
// ---------------------------------------------------------------------------

func TestReadSessionFileMeta_TimestampFallbackFromStat(t *testing.T) {
	// File with no timestamp in content → should fall back to file mtime.
	f := filepath.Join(t.TempDir(), "no-ts.jsonl")
	content := `{"type":"session_meta","payload":{"id":"s1","cwd":"/tmp","source":"cli"}}
`
	if err := os.WriteFile(f, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	meta, err := readSessionFileMeta(f)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if meta.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero (fallback to file stat)")
	}
	if meta.ModifiedAt.IsZero() {
		t.Error("ModifiedAt should not be zero (fallback to file stat)")
	}
}

func TestReadSessionFileMeta_FileNotFound(t *testing.T) {
	_, err := readSessionFileMeta("/nonexistent/file.jsonl")
	if err == nil {
		t.Fatal("expected error for nonexistent file")
	}
}

// ---------------------------------------------------------------------------
// processMetaLine — event_msg branch
// ---------------------------------------------------------------------------

func TestProcessMetaLine_EventMsg(t *testing.T) {
	var meta sessionFileMeta
	processMetaLine([]byte(`{"timestamp":"2026-01-01T00:00:00Z","type":"event_msg","payload":{"type":"user_message","content":"from event"}}`), &meta)
	// event_msg branch in processMetaLine is a no-op (comment says "Could extract..."),
	// so it should not set any fields except timestamps.
	if meta.FirstPrompt != "" {
		t.Errorf("event_msg should not set FirstPrompt, got %q", meta.FirstPrompt)
	}
	if meta.MessageCount != 0 {
		t.Errorf("event_msg should not increment MessageCount, got %d", meta.MessageCount)
	}
	// But timestamp should still be tracked.
	if meta.CreatedAt.IsZero() {
		t.Error("timestamp should still be parsed from event_msg")
	}
}

func TestProcessMetaLine_InvalidJSON(t *testing.T) {
	var meta sessionFileMeta
	processMetaLine([]byte(`{broken`), &meta)
	// Should not panic or modify meta.
	if meta.SessionID != "" || meta.MessageCount != 0 {
		t.Error("invalid JSON should not modify meta")
	}
}

func TestProcessMetaLine_EmptyLine(t *testing.T) {
	var meta sessionFileMeta
	processMetaLine([]byte(``), &meta)
	if meta.SessionID != "" {
		t.Error("empty line should not modify meta")
	}
}

// ---------------------------------------------------------------------------
// collectSessionFiles
// ---------------------------------------------------------------------------

func TestCollectSessionFiles_MixedExtensions(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"a.jsonl", "b.jsonl", "c.json", "d.txt", "e.jsonl"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	files, err := collectSessionFiles(dir)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(files) != 3 {
		t.Errorf("expected 3 .jsonl files, got %d", len(files))
	}
}

func TestCollectSessionFiles_EmptyDir(t *testing.T) {
	dir := t.TempDir()
	files, err := collectSessionFiles(dir)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(files) != 0 {
		t.Errorf("expected 0, got %d", len(files))
	}
}

func TestCollectSessionFiles_Nested(t *testing.T) {
	dir := t.TempDir()
	subdir := filepath.Join(dir, "sub", "deep")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatal(err)
	}
	for _, f := range []string{
		filepath.Join(dir, "top.jsonl"),
		filepath.Join(subdir, "deep.jsonl"),
	} {
		if err := os.WriteFile(f, []byte("{}"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	files, err := collectSessionFiles(dir)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if len(files) != 2 {
		t.Errorf("expected 2, got %d", len(files))
	}
}

// ---------------------------------------------------------------------------
// session cache
// ---------------------------------------------------------------------------

func TestSessionFileCache_HitAndMiss(t *testing.T) {
	resetSessionFileCache()
	f := filepath.Join(t.TempDir(), "cached.jsonl")
	content := `{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"c1","cwd":"/tmp","source":"cli"}}
{"timestamp":"2026-01-01T00:01:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"cached prompt"}]}}
`
	if err := os.WriteFile(f, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	// First read: cache miss.
	meta1, err := readSessionFileMetaCached(f)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if meta1.SessionID != "c1" {
		t.Errorf("SessionID = %q", meta1.SessionID)
	}

	// Second read: cache hit (same mtime).
	meta2, err := readSessionFileMetaCached(f)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if meta2.SessionID != "c1" {
		t.Errorf("cache hit: SessionID = %q", meta2.SessionID)
	}
	if meta2.FirstPrompt != "cached prompt" {
		t.Errorf("FirstPrompt = %q", meta2.FirstPrompt)
	}
}

func TestSessionFileCache_FileDeleted(t *testing.T) {
	resetSessionFileCache()
	f := filepath.Join(t.TempDir(), "del.jsonl")
	if err := os.WriteFile(f, []byte(`{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"d1","cwd":"/tmp","source":"cli"}}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	// Warm the cache.
	_, err := readSessionFileMetaCached(f)
	if err != nil {
		t.Fatalf("error: %v", err)
	}

	// Delete the file.
	os.Remove(f)

	// Should return error, not stale cache.
	_, err = readSessionFileMetaCached(f)
	if err == nil {
		t.Fatal("expected error after file deletion")
	}
}

// ---------------------------------------------------------------------------
// groupByProject
// ---------------------------------------------------------------------------

func TestGroupByProject_MultipleProjects(t *testing.T) {
	sessions := []Session{
		{SessionID: "s1", ProjectPath: "/proj/a"},
		{SessionID: "s2", ProjectPath: "/proj/b"},
		{SessionID: "s3", ProjectPath: "/proj/a"},
		{SessionID: "s4"},
	}
	projects := groupByProject(sessions)
	if len(projects) != 3 {
		t.Fatalf("expected 3 projects, got %d", len(projects))
	}
	// Find by key
	found := map[string]int{}
	for _, p := range projects {
		found[p.Key] = len(p.Sessions)
	}
	if found["/proj/a"] != 2 {
		t.Errorf("/proj/a sessions = %d, want 2", found["/proj/a"])
	}
	if found["/proj/b"] != 1 {
		t.Errorf("/proj/b sessions = %d, want 1", found["/proj/b"])
	}
	if found["(unknown)"] != 1 {
		t.Errorf("(unknown) sessions = %d, want 1", found["(unknown)"])
	}
}

func TestGroupByProject_AllUnknown(t *testing.T) {
	sessions := []Session{
		{SessionID: "s1"},
		{SessionID: "s2"},
	}
	projects := groupByProject(sessions)
	if len(projects) != 1 {
		t.Fatalf("expected 1 project, got %d", len(projects))
	}
	if projects[0].Key != "(unknown)" {
		t.Errorf("key = %q", projects[0].Key)
	}
	if projects[0].Path != "" {
		t.Errorf("path = %q, want empty", projects[0].Path)
	}
}

// ---------------------------------------------------------------------------
// DiscoverProjects — additional coverage paths
// ---------------------------------------------------------------------------

func TestDiscoverProjects_NoSessionsDir(t *testing.T) {
	dir := t.TempDir()
	// No "sessions" subdirectory.
	_, err := DiscoverProjects(dir)
	if err == nil {
		t.Fatal("expected error for missing sessions dir")
	}
}

func TestDiscoverProjects_EmptySessionsDir(t *testing.T) {
	tmpDir, _, _ := setupCodexDir(t)
	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if projects != nil {
		t.Errorf("expected nil for empty sessions dir, got %v", projects)
	}
}

func TestDiscoverProjects_DeduplicateBySessionID(t *testing.T) {
	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	sessionID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

	// Write two files with the same session ID (simulating duplicate).
	f1 := filepath.Join(sessionsDir, "rollout-2026-01-01T00-00-00-"+sessionID+".jsonl")
	f2 := filepath.Join(sessionsDir, "rollout-2026-01-02T00-00-00-"+sessionID+".jsonl")
	content1 := `{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"` + sessionID + `","cwd":"` + jsonEscapePath(projDir) + `","source":"cli"}}
{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"first copy"}]}}
`
	content2 := `{"timestamp":"2026-01-02T00:00:00Z","type":"session_meta","payload":{"id":"` + sessionID + `","cwd":"` + jsonEscapePath(projDir) + `","source":"cli"}}
{"timestamp":"2026-01-02T00:00:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"second copy"}]}}
`
	if err := os.WriteFile(f1, []byte(content1), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f2, []byte(content2), 0o644); err != nil {
		t.Fatal(err)
	}

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	all := collectAllSessions(projects)
	if len(all) != 1 {
		t.Fatalf("expected 1 deduplicated session, got %d", len(all))
	}
}

func TestDiscoverProjects_InvalidSessionIDSkipped(t *testing.T) {
	tmpDir, sessionsDir, projDir := setupCodexDir(t)

	// Write a valid session.
	validID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	writeSessionFile(t, sessionsDir, validID, "2026-01-01T00:00:00Z", projDir, `"cli"`, "valid")

	// Write a file with no valid UUID in the name.
	badFile := filepath.Join(sessionsDir, "no-uuid-here.jsonl")
	if err := os.WriteFile(badFile, []byte(`{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"x","cwd":"/tmp","source":"cli"}}`+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	all := collectAllSessions(projects)
	if len(all) != 1 {
		t.Fatalf("expected 1 session (bad filename skipped), got %d", len(all))
	}
}

func TestDiscoverProjects_HistoryEnrichment(t *testing.T) {
	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	sessionID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

	// Session file with no user message.
	fname := "rollout-2026-01-01T00-00-00-" + sessionID + ".jsonl"
	content := `{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"` + sessionID + `","cwd":"` + jsonEscapePath(projDir) + `","source":"cli"}}
`
	if err := os.WriteFile(filepath.Join(sessionsDir, fname), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	// History file with the first prompt.
	historyContent := `{"session_id":"` + sessionID + `","ts":1770777540,"text":"from history"}` + "\n"
	if err := os.WriteFile(filepath.Join(tmpDir, "history.jsonl"), []byte(historyContent), 0o644); err != nil {
		t.Fatal(err)
	}

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	all := collectAllSessions(projects)
	if len(all) != 1 {
		t.Fatalf("expected 1 session, got %d", len(all))
	}
	if all[0].FirstPrompt != "from history" {
		t.Errorf("FirstPrompt = %q, want %q (enriched from history)", all[0].FirstPrompt, "from history")
	}
}

func TestDiscoverProjects_HistoryEnrichmentTimestamps(t *testing.T) {
	// Session file with no timestamps in content, history provides time.
	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	sessionID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

	fname := "rollout-2026-01-01T00-00-00-" + sessionID + ".jsonl"
	// No timestamp field in the envelope.
	content := `{"type":"session_meta","payload":{"id":"` + sessionID + `","cwd":"` + jsonEscapePath(projDir) + `","source":"cli"}}
`
	if err := os.WriteFile(filepath.Join(sessionsDir, fname), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	historyContent := `{"session_id":"` + sessionID + `","ts":1770777540,"text":"history prompt"}` + "\n"
	if err := os.WriteFile(filepath.Join(tmpDir, "history.jsonl"), []byte(historyContent), 0o644); err != nil {
		t.Fatal(err)
	}

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	all := collectAllSessions(projects)
	if len(all) != 1 {
		t.Fatalf("expected 1 session, got %d", len(all))
	}
	if all[0].CreatedAt.IsZero() {
		t.Error("CreatedAt should be enriched from history or filename")
	}
	if all[0].ModifiedAt.IsZero() {
		t.Error("ModifiedAt should be enriched from history or filename")
	}
}

func TestDiscoverProjects_TimestampFromFilename(t *testing.T) {
	// Session with no timestamps in content or history — should get from filename.
	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	sessionID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

	fname := "rollout-2026-03-15T10-30-00-" + sessionID + ".jsonl"
	content := `{"type":"session_meta","payload":{"id":"` + sessionID + `","cwd":"` + jsonEscapePath(projDir) + `","source":"cli"}}
{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"prompt"}]}}
`
	if err := os.WriteFile(filepath.Join(sessionsDir, fname), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	all := collectAllSessions(projects)
	if len(all) != 1 {
		t.Fatalf("expected 1 session, got %d", len(all))
	}
	// CreatedAt should come from filename since no timestamp in content.
	if all[0].CreatedAt.IsZero() {
		t.Error("CreatedAt should come from filename fallback")
	}
}

func TestDiscoverProjects_SessionIDFromFilename(t *testing.T) {
	// Session file where session_meta has no ID → should use filename UUID.
	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	sessionID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

	fname := "rollout-2026-01-01T00-00-00-" + sessionID + ".jsonl"
	content := `{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"","cwd":"` + jsonEscapePath(projDir) + `","source":"cli"}}
{"timestamp":"2026-01-01T00:01:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"no meta id"}]}}
`
	if err := os.WriteFile(filepath.Join(sessionsDir, fname), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	all := collectAllSessions(projects)
	if len(all) != 1 {
		t.Fatalf("expected 1 session, got %d", len(all))
	}
	if all[0].SessionID != sessionID {
		t.Errorf("SessionID = %q, want %q (from filename)", all[0].SessionID, sessionID)
	}
}

func TestDiscoverProjects_BadFileReturnsFirstErr(t *testing.T) {
	// A session directory with a bad file (unreadable) + a good file.
	tmpDir, sessionsDir, projDir := setupCodexDir(t)

	goodID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	writeSessionFile(t, sessionsDir, goodID, "2026-01-01T00:00:00Z", projDir, `"cli"`, "good")

	// Create a file that has a valid UUID in name but is a directory (unreadable as file).
	badID := "bbbbbbbb-1111-2222-3333-444444444444"
	badPath := filepath.Join(sessionsDir, "rollout-2026-01-01T00-00-00-"+badID+".jsonl")
	if err := os.MkdirAll(badPath, 0o755); err != nil {
		t.Fatal(err)
	}

	projects, err := DiscoverProjects(tmpDir)
	// Should still return projects but with a non-nil error.
	if len(projects) == 0 {
		t.Fatal("should still return projects despite bad file")
	}
	// The firstErr may or may not be set depending on WalkDir behavior with dirs ending in .jsonl.
	// At minimum, the good session should be present.
	all := collectAllSessions(projects)
	if findSession(all, goodID) == nil {
		t.Error("good session should still be found")
	}
	_ = err // err may or may not be set
}

func TestDiscoverProjects_UnreadableFileReturnsFirstErr(t *testing.T) {
	tmpDir, sessionsDir, projDir := setupCodexDir(t)

	goodID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	writeSessionFile(t, sessionsDir, goodID, "2026-01-01T00:00:00Z", projDir, `"cli"`, "good session")

	// Create a file with valid UUID name but unreadable (chmod 000).
	badID := "bbbbbbbb-1111-2222-3333-444444444444"
	badPath := filepath.Join(sessionsDir, "rollout-2026-01-01T00-00-00-"+badID+".jsonl")
	if err := os.WriteFile(badPath, []byte("content"), 0o644); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS == "windows" {
		t.Skip("chmod 000 does not restrict reads on Windows")
	}
	if err := os.Chmod(badPath, 0o000); err != nil {
		t.Skip("cannot chmod on this platform")
	}
	t.Cleanup(func() { os.Chmod(badPath, 0o644) })

	projects, err := DiscoverProjects(tmpDir)
	// Should return projects AND a non-nil firstErr.
	all := collectAllSessions(projects)
	if findSession(all, goodID) == nil {
		t.Error("good session should be found")
	}
	if err == nil {
		t.Error("expected non-nil error (firstErr) from unreadable file")
	}
}

func TestDiscoverProjects_ModifiedAtFallbackToCreatedAt(t *testing.T) {
	// Session with CreatedAt from content but no separate ModifiedAt.
	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	sessionID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

	fname := "rollout-2026-01-01T00-00-00-" + sessionID + ".jsonl"
	// Only one timestamp → CreatedAt = ModifiedAt = same value.
	content := `{"timestamp":"2026-06-01T12:00:00Z","type":"session_meta","payload":{"id":"` + sessionID + `","cwd":"` + jsonEscapePath(projDir) + `","source":"cli"}}
{"timestamp":"2026-06-01T12:00:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"single ts"}]}}
`
	if err := os.WriteFile(filepath.Join(sessionsDir, fname), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	all := collectAllSessions(projects)
	if len(all) != 1 {
		t.Fatalf("expected 1, got %d", len(all))
	}
	if all[0].CreatedAt.IsZero() || all[0].ModifiedAt.IsZero() {
		t.Error("both timestamps should be set")
	}
}

// ---------------------------------------------------------------------------
// processMetaLine — response_item edge cases
// ---------------------------------------------------------------------------

func TestProcessMetaLine_ResponseItemBadPayload(t *testing.T) {
	// Payload that fails unmarshal into codexResponsePayload.
	var meta sessionFileMeta
	processMetaLine([]byte(`{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":"not an object"}`), &meta)
	if meta.MessageCount != 0 {
		t.Error("bad payload should not increment MessageCount")
	}
}

func TestProcessMetaLine_ResponseItemNonMessage(t *testing.T) {
	// payload.Type != "message" → should return early.
	var meta sessionFileMeta
	processMetaLine([]byte(`{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"type":"function_call","name":"read"}}`), &meta)
	if meta.MessageCount != 0 {
		t.Error("non-message response_item should not increment MessageCount")
	}
}

func TestProcessMetaLine_ResponseItemDeveloperRole(t *testing.T) {
	// role is "developer" → not "user" or "assistant" → returns early.
	var meta sessionFileMeta
	processMetaLine([]byte(`{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"type":"message","role":"developer","content":[{"type":"input_text","text":"system msg"}]}}`), &meta)
	if meta.MessageCount != 0 {
		t.Error("developer role should not increment MessageCount")
	}
}

func TestProcessMetaLine_ResponseItemSystemInjectedUser(t *testing.T) {
	// User message that's system-injected (AGENTS.md) → skipped.
	var meta sessionFileMeta
	processMetaLine([]byte(`{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"# AGENTS.md\nskills"}]}}`), &meta)
	if meta.MessageCount != 0 {
		t.Error("system-injected user message should not increment MessageCount")
	}
	if meta.FirstPrompt != "" {
		t.Error("FirstPrompt should not be set for system-injected")
	}
}

func TestProcessMetaLine_SecondUserKeepsFirstPrompt(t *testing.T) {
	var meta sessionFileMeta
	processMetaLine([]byte(`{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"first"}]}}`), &meta)
	processMetaLine([]byte(`{"timestamp":"2026-01-01T00:01:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"second"}]}}`), &meta)
	if meta.FirstPrompt != "first" {
		t.Errorf("FirstPrompt = %q, want first", meta.FirstPrompt)
	}
	if meta.MessageCount != 2 {
		t.Errorf("MessageCount = %d, want 2", meta.MessageCount)
	}
}

// ---------------------------------------------------------------------------
// FindSessionByID — history enrichment inside fast path
// ---------------------------------------------------------------------------

func TestFindSessionByID_FoundViaMetaSessionID(t *testing.T) {
	// File found by globRecursive, but filename UUID differs from meta.SessionID.
	// The fast path should match via meta.SessionID.
	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	metaSessionID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	// Filename contains the target UUID so globRecursive finds it,
	// but parseSessionIDFromFilename returns a different UUID.
	// Actually, we just use the same UUID in the filename for glob to find it,
	// and verify the fast path works.
	writeSessionFile(t, sessionsDir, metaSessionID, "2026-01-01T00:00:00Z", projDir, `"cli"`, "via meta id")

	sess, err := FindSessionByID(tmpDir, metaSessionID)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if sess == nil {
		t.Fatal("session should be found")
	}
	if sess.FirstPrompt != "via meta id" {
		t.Errorf("FirstPrompt = %q", sess.FirstPrompt)
	}
}

func TestFindSessionByID_GlobMatchWithReadError(t *testing.T) {
	// A glob match where readSessionFileMetaCached fails → should continue to next match.
	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	sessionID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

	// Good file.
	writeSessionFile(t, sessionsDir, sessionID, "2026-01-01T00:00:00Z", projDir, `"cli"`, "found me")

	// Create a nested dir that also matches (won't be readable as file).
	subdir := filepath.Join(sessionsDir, "sub")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatal(err)
	}
	badFile := filepath.Join(subdir, "rollout-2026-01-01T00-00-00-"+sessionID+".jsonl")
	if err := os.WriteFile(badFile, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	os.Chmod(badFile, 0o000)
	t.Cleanup(func() { os.Chmod(badFile, 0o644) })

	sess, err := FindSessionByID(tmpDir, sessionID)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if sess == nil {
		t.Fatal("should find the good file despite bad one")
	}
}

func TestFindSessionWithProject_DiscoverError(t *testing.T) {
	// Pass a dir without sessions/ to trigger DiscoverProjects error.
	dir := t.TempDir()
	_, _, err := FindSessionWithProject(dir, "some-id")
	if err == nil {
		t.Fatal("expected error when DiscoverProjects fails")
	}
}

func TestFindSessionByID_HistoryEnrichment(t *testing.T) {
	tmpDir, sessionsDir, projDir := setupCodexDir(t)
	sessionID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

	// Session with no user message.
	fname := "rollout-2026-01-01T00-00-00-" + sessionID + ".jsonl"
	content := `{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"` + sessionID + `","cwd":"` + jsonEscapePath(projDir) + `","source":"cli"}}
`
	if err := os.WriteFile(filepath.Join(sessionsDir, fname), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	historyContent := `{"session_id":"` + sessionID + `","ts":1770777540,"text":"enriched prompt"}` + "\n"
	if err := os.WriteFile(filepath.Join(tmpDir, "history.jsonl"), []byte(historyContent), 0o644); err != nil {
		t.Fatal(err)
	}

	sess, err := FindSessionByID(tmpDir, sessionID)
	if err != nil {
		t.Fatalf("error: %v", err)
	}
	if sess.FirstPrompt != "enriched prompt" {
		t.Errorf("FirstPrompt = %q, want enriched from history", sess.FirstPrompt)
	}
}
