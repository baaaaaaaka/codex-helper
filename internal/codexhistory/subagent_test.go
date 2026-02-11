package codexhistory

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// parseSessionSource — table-driven
// ---------------------------------------------------------------------------

func TestParseSessionSource(t *testing.T) {
	tests := []struct {
		name         string
		raw          json.RawMessage
		wantIsSub    bool
		wantType     string
		wantParentID string
	}{
		// ---- non-subagent strings ----
		{
			name:      "cli string",
			raw:       json.RawMessage(`"cli"`),
			wantIsSub: false,
		},
		{
			name:      "vscode string",
			raw:       json.RawMessage(`"vscode"`),
			wantIsSub: false,
		},
		{
			name:      "exec string",
			raw:       json.RawMessage(`"exec"`),
			wantIsSub: false,
		},
		{
			name:      "mcp string",
			raw:       json.RawMessage(`"mcp"`),
			wantIsSub: false,
		},
		{
			name:      "empty string",
			raw:       json.RawMessage(`""`),
			wantIsSub: false,
		},
		{
			name:      "unknown source string",
			raw:       json.RawMessage(`"some_future_source"`),
			wantIsSub: false,
		},

		// ---- nil / empty ----
		{
			name:      "nil raw",
			raw:       nil,
			wantIsSub: false,
		},
		{
			name:      "zero-length raw",
			raw:       json.RawMessage{},
			wantIsSub: false,
		},
		{
			name:      "json null",
			raw:       json.RawMessage(`null`),
			wantIsSub: false, // json.Unmarshal into string succeeds with ""
		},

		// ---- string subagent types ----
		{
			name:      "review subagent",
			raw:       json.RawMessage(`{"subagent": "review"}`),
			wantIsSub: true,
			wantType:  "review",
		},
		{
			name:      "compact subagent",
			raw:       json.RawMessage(`{"subagent": "compact"}`),
			wantIsSub: true,
			wantType:  "compact",
		},

		// ---- thread_spawn ----
		{
			name:         "thread_spawn with parent and depth",
			raw:          json.RawMessage(`{"subagent":{"thread_spawn":{"parent_thread_id":"abc-123","depth":1}}}`),
			wantIsSub:    true,
			wantType:     "thread_spawn",
			wantParentID: "abc-123",
		},
		{
			name:         "thread_spawn with depth zero",
			raw:          json.RawMessage(`{"subagent":{"thread_spawn":{"parent_thread_id":"root-id","depth":0}}}`),
			wantIsSub:    true,
			wantType:     "thread_spawn",
			wantParentID: "root-id",
		},
		{
			name:         "thread_spawn with large depth",
			raw:          json.RawMessage(`{"subagent":{"thread_spawn":{"parent_thread_id":"deep-id","depth":99}}}`),
			wantIsSub:    true,
			wantType:     "thread_spawn",
			wantParentID: "deep-id",
		},
		{
			name:      "thread_spawn missing parent_thread_id",
			raw:       json.RawMessage(`{"subagent":{"thread_spawn":{"depth":1}}}`),
			wantIsSub: true,
			wantType:  "unknown", // no parent_thread_id → falls through to unknown
		},

		// ---- unknown / malformed subagent objects ----
		{
			name:      "unknown subagent object",
			raw:       json.RawMessage(`{"subagent": {"something_new": {}}}`),
			wantIsSub: true,
			wantType:  "unknown",
		},
		{
			name:      "subagent value is number",
			raw:       json.RawMessage(`{"subagent": 42}`),
			wantIsSub: true,
			wantType:  "unknown",
		},
		{
			name:      "subagent value is boolean",
			raw:       json.RawMessage(`{"subagent": true}`),
			wantIsSub: true,
			wantType:  "unknown",
		},
		{
			name:      "subagent value is null",
			raw:       json.RawMessage(`{"subagent": null}`),
			wantIsSub: true, // null → RawMessage is "null" (4 bytes), unmarshal to string succeeds with ""
			wantType:  "",
		},
		{
			name:      "subagent value is array",
			raw:       json.RawMessage(`{"subagent": ["a","b"]}`),
			wantIsSub: true,
			wantType:  "unknown",
		},
		{
			name:      "subagent value is empty object",
			raw:       json.RawMessage(`{"subagent": {}}`),
			wantIsSub: true,
			wantType:  "unknown",
		},

		// ---- objects without subagent key ----
		{
			name:      "object without subagent key",
			raw:       json.RawMessage(`{"foo": "bar"}`),
			wantIsSub: false,
		},
		{
			name:      "empty object",
			raw:       json.RawMessage(`{}`),
			wantIsSub: false,
		},

		// ---- invalid JSON ----
		{
			name:      "invalid JSON",
			raw:       json.RawMessage(`{broken`),
			wantIsSub: false,
		},
		{
			name:      "bare number",
			raw:       json.RawMessage(`123`),
			wantIsSub: false, // unmarshal to string fails, unmarshal to struct fails
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isSub, subType, parentID := parseSessionSource(tt.raw)
			if isSub != tt.wantIsSub {
				t.Errorf("isSubagent = %v, want %v", isSub, tt.wantIsSub)
			}
			if subType != tt.wantType {
				t.Errorf("subagentType = %q, want %q", subType, tt.wantType)
			}
			if parentID != tt.wantParentID {
				t.Errorf("parentThreadID = %q, want %q", parentID, tt.wantParentID)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// processMetaLine — subagent field extraction
// ---------------------------------------------------------------------------

func TestProcessMetaLine_SubagentFields(t *testing.T) {
	tests := []struct {
		name             string
		line             string
		wantIsSub        bool
		wantSubType      string
		wantParentThread string
		wantSessionID    string
		wantProjectPath  string
	}{
		{
			name:          "review subagent",
			line:          `{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"sess-1","cwd":"/tmp","source":{"subagent":"review"}}}`,
			wantIsSub:     true,
			wantSubType:   "review",
			wantSessionID: "sess-1",
		},
		{
			name:             "thread_spawn subagent",
			line:             `{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"sess-2","cwd":"/work","source":{"subagent":{"thread_spawn":{"parent_thread_id":"parent-uuid","depth":2}}}}}`,
			wantIsSub:        true,
			wantSubType:      "thread_spawn",
			wantParentThread: "parent-uuid",
			wantSessionID:    "sess-2",
			wantProjectPath:  "/work",
		},
		{
			name:            "compact subagent",
			line:            `{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"sess-c","cwd":"/proj","source":{"subagent":"compact"}}}`,
			wantIsSub:       true,
			wantSubType:     "compact",
			wantSessionID:   "sess-c",
			wantProjectPath: "/proj",
		},
		{
			name:            "regular cli source",
			line:            `{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"sess-3","cwd":"/home","source":"cli"}}`,
			wantIsSub:       false,
			wantSessionID:   "sess-3",
			wantProjectPath: "/home",
		},
		{
			name:            "source field missing from payload",
			line:            `{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"sess-4","cwd":"/x"}}`,
			wantIsSub:       false,
			wantSessionID:   "sess-4",
			wantProjectPath: "/x",
		},
		{
			name:          "source is null",
			line:          `{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"sess-5","cwd":"/y","source":null}}`,
			wantIsSub:     false,
			wantSessionID: "sess-5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var meta sessionFileMeta
			processMetaLine([]byte(tt.line), &meta)
			if meta.IsSubagent != tt.wantIsSub {
				t.Errorf("IsSubagent = %v, want %v", meta.IsSubagent, tt.wantIsSub)
			}
			if meta.SubagentType != tt.wantSubType {
				t.Errorf("SubagentType = %q, want %q", meta.SubagentType, tt.wantSubType)
			}
			if meta.ParentThreadID != tt.wantParentThread {
				t.Errorf("ParentThreadID = %q, want %q", meta.ParentThreadID, tt.wantParentThread)
			}
			if tt.wantSessionID != "" && meta.SessionID != tt.wantSessionID {
				t.Errorf("SessionID = %q, want %q", meta.SessionID, tt.wantSessionID)
			}
			if tt.wantProjectPath != "" && meta.ProjectPath != tt.wantProjectPath {
				t.Errorf("ProjectPath = %q, want %q", meta.ProjectPath, tt.wantProjectPath)
			}
		})
	}
}

func TestProcessMetaLine_SubagentDoesNotAffectOtherFields(t *testing.T) {
	// Ensure subagent sessions still track timestamps and messages correctly.
	var meta sessionFileMeta
	processMetaLine([]byte(`{"timestamp":"2026-01-15T10:00:00Z","type":"session_meta","payload":{"id":"sub-1","cwd":"/proj","source":{"subagent":"review"}}}`), &meta)
	processMetaLine([]byte(`{"timestamp":"2026-01-15T10:05:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"do review"}]}}`), &meta)
	processMetaLine([]byte(`{"timestamp":"2026-01-15T10:10:00Z","type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"done"}]}}`), &meta)

	if !meta.IsSubagent {
		t.Fatal("expected IsSubagent=true")
	}
	if meta.MessageCount != 2 {
		t.Errorf("MessageCount = %d, want 2", meta.MessageCount)
	}
	if meta.FirstPrompt != "do review" {
		t.Errorf("FirstPrompt = %q, want %q", meta.FirstPrompt, "do review")
	}
	if meta.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero")
	}
	if meta.ModifiedAt.IsZero() {
		t.Error("ModifiedAt should not be zero")
	}
	if !meta.ModifiedAt.After(meta.CreatedAt) {
		t.Error("ModifiedAt should be after CreatedAt")
	}
}

func TestProcessMetaLine_MultipleSessionMetaLines(t *testing.T) {
	// First session_meta sets subagent fields; second line should not overwrite
	// already-set SessionID / ProjectPath, but subagent fields are set on first
	// call and not cleared.
	var meta sessionFileMeta
	processMetaLine([]byte(`{"timestamp":"2026-01-01T00:00:00Z","type":"session_meta","payload":{"id":"first-id","cwd":"/first","source":{"subagent":"review"}}}`), &meta)
	processMetaLine([]byte(`{"timestamp":"2026-01-01T00:01:00Z","type":"session_meta","payload":{"id":"second-id","cwd":"/second","source":"cli"}}`), &meta)

	// SessionID and ProjectPath are only set if empty, so they keep first values.
	if meta.SessionID != "first-id" {
		t.Errorf("SessionID = %q, want first-id", meta.SessionID)
	}
	if meta.ProjectPath != "/first" {
		t.Errorf("ProjectPath = %q, want /first", meta.ProjectPath)
	}
	// IsSubagent was set to true and never cleared.
	if !meta.IsSubagent {
		t.Error("IsSubagent should remain true once set")
	}
}

func TestProcessMetaLine_NonSessionMetaDoesNotSetSubagent(t *testing.T) {
	var meta sessionFileMeta
	processMetaLine([]byte(`{"timestamp":"2026-01-01T00:00:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}}`), &meta)
	if meta.IsSubagent {
		t.Fatal("response_item should not set IsSubagent")
	}
}

// ---------------------------------------------------------------------------
// readSessionFileMeta — file-level subagent parsing
// ---------------------------------------------------------------------------

func TestReadSessionFileMeta_SubagentFile(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "sub.jsonl")
	content := `{"timestamp":"2026-03-01T12:00:00Z","type":"session_meta","payload":{"id":"file-sub-1","cwd":"/proj","source":{"subagent":{"thread_spawn":{"parent_thread_id":"file-parent","depth":1}}}}}
{"timestamp":"2026-03-01T12:01:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"sub task"}]}}
`
	if err := os.WriteFile(tmpFile, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	meta, err := readSessionFileMeta(tmpFile)
	if err != nil {
		t.Fatalf("readSessionFileMeta error: %v", err)
	}
	if !meta.IsSubagent {
		t.Fatal("expected IsSubagent=true")
	}
	if meta.SubagentType != "thread_spawn" {
		t.Errorf("SubagentType = %q, want thread_spawn", meta.SubagentType)
	}
	if meta.ParentThreadID != "file-parent" {
		t.Errorf("ParentThreadID = %q, want file-parent", meta.ParentThreadID)
	}
	if meta.SessionID != "file-sub-1" {
		t.Errorf("SessionID = %q, want file-sub-1", meta.SessionID)
	}
	if meta.FirstPrompt != "sub task" {
		t.Errorf("FirstPrompt = %q, want %q", meta.FirstPrompt, "sub task")
	}
	if meta.MessageCount != 1 {
		t.Errorf("MessageCount = %d, want 1", meta.MessageCount)
	}
}

func TestReadSessionFileMeta_RegularFile(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "regular.jsonl")
	content := `{"timestamp":"2026-03-01T12:00:00Z","type":"session_meta","payload":{"id":"reg-1","cwd":"/proj","source":"cli"}}
{"timestamp":"2026-03-01T12:01:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]}}
`
	if err := os.WriteFile(tmpFile, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	meta, err := readSessionFileMeta(tmpFile)
	if err != nil {
		t.Fatalf("readSessionFileMeta error: %v", err)
	}
	if meta.IsSubagent {
		t.Fatal("expected IsSubagent=false for regular file")
	}
	if meta.SubagentType != "" {
		t.Errorf("SubagentType = %q, want empty", meta.SubagentType)
	}
	if meta.ParentThreadID != "" {
		t.Errorf("ParentThreadID = %q, want empty", meta.ParentThreadID)
	}
}

// ---------------------------------------------------------------------------
// attachSubagents — unit tests
// ---------------------------------------------------------------------------

func makeSession(id string, modTime time.Time) Session {
	return Session{
		SessionID:  id,
		ModifiedAt: modTime,
	}
}

func makeSub(id, parentID, agentID string, modTime time.Time) SubagentSession {
	return SubagentSession{
		SessionID:       id,
		ParentSessionID: parentID,
		AgentID:         agentID,
		ModifiedAt:      modTime,
		MessageCount:    1,
		FirstPrompt:     "task " + id,
	}
}

func TestAttachSubagents_NoPending(t *testing.T) {
	sessions := []Session{makeSession("p1", time.Now())}
	idx := map[string]int{"p1": 0}
	result := attachSubagents(sessions, idx, nil)
	if len(result) != 1 {
		t.Fatalf("expected 1 session, got %d", len(result))
	}
	if len(result[0].Subagents) != 0 {
		t.Fatalf("expected 0 subagents, got %d", len(result[0].Subagents))
	}
}

func TestAttachSubagents_AllMatchParent(t *testing.T) {
	now := time.Now()
	sessions := []Session{makeSession("p1", now)}
	idx := map[string]int{"p1": 0}
	pending := []SubagentSession{
		makeSub("c1", "p1", "thread_spawn", now.Add(1*time.Minute)),
		makeSub("c2", "p1", "review", now.Add(2*time.Minute)),
	}
	result := attachSubagents(sessions, idx, pending)
	if len(result) != 1 {
		t.Fatalf("expected 1 session, got %d", len(result))
	}
	if len(result[0].Subagents) != 2 {
		t.Fatalf("expected 2 subagents, got %d", len(result[0].Subagents))
	}
	// Sorted descending by ModifiedAt: c2 first.
	if result[0].Subagents[0].SessionID != "c2" {
		t.Errorf("first subagent = %q, want c2", result[0].Subagents[0].SessionID)
	}
	if result[0].Subagents[1].SessionID != "c1" {
		t.Errorf("second subagent = %q, want c1", result[0].Subagents[1].SessionID)
	}
}

func TestAttachSubagents_AllOrphans(t *testing.T) {
	now := time.Now()
	sessions := []Session{makeSession("p1", now)}
	idx := map[string]int{"p1": 0}
	pending := []SubagentSession{
		makeSub("o1", "nonexistent", "review", now),
		makeSub("o2", "", "compact", now),
	}
	result := attachSubagents(sessions, idx, pending)
	// 1 original + 2 orphans promoted.
	if len(result) != 3 {
		t.Fatalf("expected 3 sessions, got %d", len(result))
	}
	if len(result[0].Subagents) != 0 {
		t.Fatalf("parent should have 0 subagents, got %d", len(result[0].Subagents))
	}
	// Orphans should have summary labels.
	if result[1].Summary != "[review subagent]" {
		t.Errorf("orphan 1 Summary = %q, want [review subagent]", result[1].Summary)
	}
	if result[2].Summary != "[compact subagent]" {
		t.Errorf("orphan 2 Summary = %q, want [compact subagent]", result[2].Summary)
	}
}

func TestAttachSubagents_OrphanEmptyAgentID(t *testing.T) {
	now := time.Now()
	sessions := []Session{makeSession("p1", now)}
	idx := map[string]int{"p1": 0}
	pending := []SubagentSession{
		makeSub("o1", "", "", now),
	}
	pending[0].AgentID = "" // explicitly empty
	result := attachSubagents(sessions, idx, pending)
	if len(result) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(result))
	}
	if result[1].Summary != "[subagent]" {
		t.Errorf("orphan Summary = %q, want [subagent]", result[1].Summary)
	}
}

func TestAttachSubagents_MixedMatchAndOrphan(t *testing.T) {
	now := time.Now()
	sessions := []Session{
		makeSession("p1", now),
		makeSession("p2", now),
	}
	idx := map[string]int{"p1": 0, "p2": 1}
	pending := []SubagentSession{
		makeSub("c1", "p1", "thread_spawn", now),
		makeSub("o1", "missing", "review", now),
		makeSub("c2", "p2", "compact", now),
	}
	result := attachSubagents(sessions, idx, pending)
	// 2 originals + 1 orphan.
	if len(result) != 3 {
		t.Fatalf("expected 3 sessions, got %d", len(result))
	}
	if len(result[0].Subagents) != 1 || result[0].Subagents[0].SessionID != "c1" {
		t.Errorf("p1 should have subagent c1")
	}
	if len(result[1].Subagents) != 1 || result[1].Subagents[0].SessionID != "c2" {
		t.Errorf("p2 should have subagent c2")
	}
	if result[2].SessionID != "o1" {
		t.Errorf("orphan session should be o1, got %s", result[2].SessionID)
	}
}

func TestAttachSubagents_OrphanPreservesFields(t *testing.T) {
	now := time.Now()
	sessions := []Session{}
	idx := map[string]int{}
	pending := []SubagentSession{
		{
			SessionID:       "orph-1",
			ParentSessionID: "",
			AgentID:         "review",
			FirstPrompt:     "review this",
			MessageCount:    5,
			CreatedAt:       now.Add(-time.Hour),
			ModifiedAt:      now,
			FilePath:        "/tmp/orph.jsonl",
		},
	}
	result := attachSubagents(sessions, idx, pending)
	if len(result) != 1 {
		t.Fatalf("expected 1 session, got %d", len(result))
	}
	s := result[0]
	if s.SessionID != "orph-1" {
		t.Errorf("SessionID = %q", s.SessionID)
	}
	if s.FirstPrompt != "review this" {
		t.Errorf("FirstPrompt = %q", s.FirstPrompt)
	}
	if s.MessageCount != 5 {
		t.Errorf("MessageCount = %d", s.MessageCount)
	}
	if s.FilePath != "/tmp/orph.jsonl" {
		t.Errorf("FilePath = %q", s.FilePath)
	}
	if !s.CreatedAt.Equal(now.Add(-time.Hour)) {
		t.Errorf("CreatedAt not preserved")
	}
	if !s.ModifiedAt.Equal(now) {
		t.Errorf("ModifiedAt not preserved")
	}
}

func TestAttachSubagents_SingleSubagentNoSort(t *testing.T) {
	now := time.Now()
	sessions := []Session{makeSession("p1", now)}
	idx := map[string]int{"p1": 0}
	pending := []SubagentSession{
		makeSub("c1", "p1", "review", now),
	}
	result := attachSubagents(sessions, idx, pending)
	if len(result[0].Subagents) != 1 {
		t.Fatalf("expected 1 subagent, got %d", len(result[0].Subagents))
	}
	if result[0].Subagents[0].SessionID != "c1" {
		t.Errorf("subagent SessionID = %q, want c1", result[0].Subagents[0].SessionID)
	}
}

// ---------------------------------------------------------------------------
// DiscoverProjects — integration tests
// ---------------------------------------------------------------------------

// helper: create a Codex session file in sessionsDir.
func writeSessionFile(t *testing.T, sessionsDir, sessionID, ts, projDir, sourceJSON string, userMsg string) string {
	t.Helper()
	// Build a valid-looking filename.
	fname := "rollout-2026-01-01T00-00-00-" + sessionID + ".jsonl"
	fpath := filepath.Join(sessionsDir, fname)

	var lines []string
	meta := `{"timestamp":"` + ts + `","type":"session_meta","payload":{"id":"` + sessionID + `","cwd":"` + projDir + `","source":` + sourceJSON + `}}`
	lines = append(lines, meta)
	if userMsg != "" {
		msg := `{"timestamp":"` + ts + `","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"` + userMsg + `"}]}}`
		lines = append(lines, msg)
	}
	content := strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(fpath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return fpath
}

func setupCodexDir(t *testing.T) (tmpDir, sessionsDir, projDir string) {
	t.Helper()
	ResetCache()
	tmpDir = t.TempDir()
	sessionsDir = filepath.Join(tmpDir, "sessions")
	if err := os.MkdirAll(sessionsDir, 0o755); err != nil {
		t.Fatal(err)
	}
	projDir = t.TempDir()
	return
}

func findSession(sessions []Session, id string) *Session {
	for i := range sessions {
		if sessions[i].SessionID == id {
			return &sessions[i]
		}
	}
	return nil
}

func collectAllSessions(projects []Project) []Session {
	var all []Session
	for _, p := range projects {
		all = append(all, p.Sessions...)
	}
	return all
}

func TestDiscoverProjects_SubagentAssociation(t *testing.T) {
	tmpDir, sessionsDir, projDir := setupCodexDir(t)

	parentID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	childID := "11111111-2222-3333-4444-555555555555"
	orphanID := "99999999-8888-7777-6666-555555555555"
	ts := "2026-06-01T10:00:00Z"

	writeSessionFile(t, sessionsDir, parentID, ts, projDir, `"cli"`, "hello parent")
	writeSessionFile(t, sessionsDir, childID, ts, projDir,
		`{"subagent":{"thread_spawn":{"parent_thread_id":"`+parentID+`","depth":1}}}`, "child task")
	writeSessionFile(t, sessionsDir, orphanID, ts, projDir,
		`{"subagent":"review"}`, "review task")

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects: %v", err)
	}
	all := collectAllSessions(projects)

	parent := findSession(all, parentID)
	if parent == nil {
		t.Fatal("parent session not found")
	}
	if len(parent.Subagents) != 1 {
		t.Fatalf("expected 1 subagent on parent, got %d", len(parent.Subagents))
	}
	if parent.Subagents[0].SessionID != childID {
		t.Errorf("child SessionID = %q, want %s", parent.Subagents[0].SessionID, childID)
	}
	if parent.Subagents[0].AgentID != "thread_spawn" {
		t.Errorf("child AgentID = %q, want thread_spawn", parent.Subagents[0].AgentID)
	}
	if parent.Subagents[0].ParentSessionID != parentID {
		t.Errorf("child ParentSessionID = %q, want %s", parent.Subagents[0].ParentSessionID, parentID)
	}
	if parent.Subagents[0].FirstPrompt != "child task" {
		t.Errorf("child FirstPrompt = %q, want %q", parent.Subagents[0].FirstPrompt, "child task")
	}

	orphan := findSession(all, orphanID)
	if orphan == nil {
		t.Fatal("orphan subagent not found as top-level session")
	}
	if orphan.Summary != "[review subagent]" {
		t.Errorf("orphan Summary = %q, want [review subagent]", orphan.Summary)
	}

	// Child should NOT appear as a top-level session.
	if findSession(all, childID) != nil {
		t.Fatal("child subagent should not appear as top-level session")
	}
}

func TestDiscoverProjects_SubagentSortOrder(t *testing.T) {
	tmpDir, sessionsDir, projDir := setupCodexDir(t)

	parentID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	child1ID := "11111111-2222-3333-4444-555555555555"
	child2ID := "22222222-3333-4444-5555-666666666666"
	child3ID := "33333333-4444-5555-6666-777777777777"

	writeSessionFile(t, sessionsDir, parentID, "2026-01-01T00:00:00Z", projDir, `"cli"`, "parent")
	writeSessionFile(t, sessionsDir, child1ID, "2026-01-01T01:00:00Z", projDir,
		`{"subagent":{"thread_spawn":{"parent_thread_id":"`+parentID+`","depth":1}}}`, "child1")
	writeSessionFile(t, sessionsDir, child2ID, "2026-01-01T03:00:00Z", projDir,
		`{"subagent":{"thread_spawn":{"parent_thread_id":"`+parentID+`","depth":1}}}`, "child2")
	writeSessionFile(t, sessionsDir, child3ID, "2026-01-01T02:00:00Z", projDir,
		`{"subagent":{"thread_spawn":{"parent_thread_id":"`+parentID+`","depth":1}}}`, "child3")

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects: %v", err)
	}

	parent := findSession(collectAllSessions(projects), parentID)
	if parent == nil {
		t.Fatal("parent not found")
	}
	if len(parent.Subagents) != 3 {
		t.Fatalf("expected 3 subagents, got %d", len(parent.Subagents))
	}
	// Descending by ModifiedAt: child2 (03:00) > child3 (02:00) > child1 (01:00).
	wantOrder := []string{child2ID, child3ID, child1ID}
	for i, wantID := range wantOrder {
		if parent.Subagents[i].SessionID != wantID {
			t.Errorf("subagent[%d].SessionID = %q, want %q", i, parent.Subagents[i].SessionID, wantID)
		}
	}
}

func TestDiscoverProjects_OnlySubagents_AllOrphans(t *testing.T) {
	tmpDir, sessionsDir, projDir := setupCodexDir(t)

	id1 := "aaaaaaaa-1111-2222-3333-444444444444"
	id2 := "bbbbbbbb-1111-2222-3333-444444444444"

	writeSessionFile(t, sessionsDir, id1, "2026-01-01T00:00:00Z", projDir,
		`{"subagent":"review"}`, "review1")
	writeSessionFile(t, sessionsDir, id2, "2026-01-01T01:00:00Z", projDir,
		`{"subagent":"compact"}`, "compact1")

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects: %v", err)
	}
	all := collectAllSessions(projects)
	if len(all) != 2 {
		t.Fatalf("expected 2 top-level sessions (promoted orphans), got %d", len(all))
	}
	s1 := findSession(all, id1)
	s2 := findSession(all, id2)
	if s1 == nil || s2 == nil {
		t.Fatal("both orphan sessions should be present")
	}
	if s1.Summary != "[review subagent]" {
		t.Errorf("s1 Summary = %q", s1.Summary)
	}
	if s2.Summary != "[compact subagent]" {
		t.Errorf("s2 Summary = %q", s2.Summary)
	}
}

func TestDiscoverProjects_NoSubagents_Unchanged(t *testing.T) {
	tmpDir, sessionsDir, projDir := setupCodexDir(t)

	id1 := "aaaaaaaa-1111-2222-3333-444444444444"
	id2 := "bbbbbbbb-1111-2222-3333-444444444444"

	writeSessionFile(t, sessionsDir, id1, "2026-01-01T00:00:00Z", projDir, `"cli"`, "task1")
	writeSessionFile(t, sessionsDir, id2, "2026-01-01T01:00:00Z", projDir, `"vscode"`, "task2")

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects: %v", err)
	}
	all := collectAllSessions(projects)
	if len(all) != 2 {
		t.Fatalf("expected 2 sessions, got %d", len(all))
	}
	for _, s := range all {
		if len(s.Subagents) != 0 {
			t.Errorf("session %s should have no subagents", s.SessionID)
		}
	}
}

func TestDiscoverProjects_MultipleParentsEachWithChildren(t *testing.T) {
	tmpDir, sessionsDir, projDir := setupCodexDir(t)

	p1 := "aaaaaaaa-1111-2222-3333-444444444444"
	p2 := "bbbbbbbb-1111-2222-3333-444444444444"
	c1a := "cccccccc-1111-2222-3333-444444444444"
	c1b := "dddddddd-1111-2222-3333-444444444444"
	c2a := "eeeeeeee-1111-2222-3333-444444444444"

	ts := "2026-06-01T10:00:00Z"
	writeSessionFile(t, sessionsDir, p1, ts, projDir, `"cli"`, "parent1")
	writeSessionFile(t, sessionsDir, p2, ts, projDir, `"cli"`, "parent2")
	writeSessionFile(t, sessionsDir, c1a, ts, projDir,
		`{"subagent":{"thread_spawn":{"parent_thread_id":"`+p1+`","depth":1}}}`, "c1a")
	writeSessionFile(t, sessionsDir, c1b, ts, projDir,
		`{"subagent":{"thread_spawn":{"parent_thread_id":"`+p1+`","depth":1}}}`, "c1b")
	writeSessionFile(t, sessionsDir, c2a, ts, projDir,
		`{"subagent":{"thread_spawn":{"parent_thread_id":"`+p2+`","depth":1}}}`, "c2a")

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects: %v", err)
	}
	all := collectAllSessions(projects)

	parent1 := findSession(all, p1)
	parent2 := findSession(all, p2)
	if parent1 == nil || parent2 == nil {
		t.Fatal("both parents should exist")
	}
	if len(parent1.Subagents) != 2 {
		t.Errorf("parent1 subagents = %d, want 2", len(parent1.Subagents))
	}
	if len(parent2.Subagents) != 1 {
		t.Errorf("parent2 subagents = %d, want 1", len(parent2.Subagents))
	}
	if parent2.Subagents[0].SessionID != c2a {
		t.Errorf("parent2 child = %q, want %s", parent2.Subagents[0].SessionID, c2a)
	}

	// No children should appear as top-level sessions.
	for _, cid := range []string{c1a, c1b, c2a} {
		if findSession(all, cid) != nil {
			t.Errorf("child %s should not be a top-level session", cid)
		}
	}
}

func TestDiscoverProjects_SubagentInSubdirectory(t *testing.T) {
	tmpDir, sessionsDir, projDir := setupCodexDir(t)

	parentID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	childID := "11111111-2222-3333-4444-555555555555"
	ts := "2026-06-01T10:00:00Z"

	writeSessionFile(t, sessionsDir, parentID, ts, projDir, `"cli"`, "parent")

	// Create child in a nested subdirectory.
	subDir := filepath.Join(sessionsDir, "nested", "deep")
	if err := os.MkdirAll(subDir, 0o755); err != nil {
		t.Fatal(err)
	}
	childFile := filepath.Join(subDir, "rollout-2026-01-01T00-00-00-"+childID+".jsonl")
	childContent := `{"timestamp":"` + ts + `","type":"session_meta","payload":{"id":"` + childID + `","cwd":"` + projDir + `","source":{"subagent":{"thread_spawn":{"parent_thread_id":"` + parentID + `","depth":1}}}}}
{"timestamp":"` + ts + `","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"nested child"}]}}
`
	if err := os.WriteFile(childFile, []byte(childContent), 0o644); err != nil {
		t.Fatal(err)
	}

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects: %v", err)
	}
	all := collectAllSessions(projects)
	parent := findSession(all, parentID)
	if parent == nil {
		t.Fatal("parent not found")
	}
	if len(parent.Subagents) != 1 {
		t.Fatalf("expected 1 subagent, got %d", len(parent.Subagents))
	}
	if parent.Subagents[0].SessionID != childID {
		t.Errorf("child SessionID = %q, want %s", parent.Subagents[0].SessionID, childID)
	}
}

func TestDiscoverProjects_EmptySubagentFiltered(t *testing.T) {
	// A subagent with no messages/prompt should still be filtered by
	// filterEmptySessions when promoted as orphan.
	tmpDir, sessionsDir, projDir := setupCodexDir(t)

	parentID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	emptyOrphanID := "11111111-2222-3333-4444-555555555555"
	ts := "2026-06-01T10:00:00Z"

	writeSessionFile(t, sessionsDir, parentID, ts, projDir, `"cli"`, "parent")
	// Orphan with no user message → empty session after promotion.
	writeSessionFile(t, sessionsDir, emptyOrphanID, ts, projDir,
		`{"subagent":"review"}`, "")

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects: %v", err)
	}
	all := collectAllSessions(projects)

	// The empty orphan has Summary set by attachSubagents so it won't be filtered.
	orphan := findSession(all, emptyOrphanID)
	if orphan == nil {
		t.Fatal("orphan with summary should survive filterEmptySessions")
	}
	if orphan.Summary != "[review subagent]" {
		t.Errorf("orphan Summary = %q", orphan.Summary)
	}
}

func TestDiscoverProjects_SubagentMetadataPopulated(t *testing.T) {
	// Verify that SubagentSession fields are fully populated from the file.
	tmpDir, sessionsDir, projDir := setupCodexDir(t)

	parentID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	childID := "11111111-2222-3333-4444-555555555555"

	writeSessionFile(t, sessionsDir, parentID, "2026-07-01T10:00:00Z", projDir, `"cli"`, "parent")
	childPath := writeSessionFile(t, sessionsDir, childID, "2026-07-01T11:00:00Z", projDir,
		`{"subagent":{"thread_spawn":{"parent_thread_id":"`+parentID+`","depth":1}}}`, "my sub task")

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects: %v", err)
	}
	parent := findSession(collectAllSessions(projects), parentID)
	if parent == nil {
		t.Fatal("parent not found")
	}
	if len(parent.Subagents) != 1 {
		t.Fatalf("expected 1 subagent, got %d", len(parent.Subagents))
	}
	sub := parent.Subagents[0]
	if sub.SessionID != childID {
		t.Errorf("SessionID = %q", sub.SessionID)
	}
	if sub.AgentID != "thread_spawn" {
		t.Errorf("AgentID = %q", sub.AgentID)
	}
	if sub.ParentSessionID != parentID {
		t.Errorf("ParentSessionID = %q", sub.ParentSessionID)
	}
	if sub.FirstPrompt != "my sub task" {
		t.Errorf("FirstPrompt = %q", sub.FirstPrompt)
	}
	if sub.MessageCount != 1 {
		t.Errorf("MessageCount = %d", sub.MessageCount)
	}
	if sub.FilePath != childPath {
		t.Errorf("FilePath = %q, want %q", sub.FilePath, childPath)
	}
	if sub.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero")
	}
	if sub.ModifiedAt.IsZero() {
		t.Error("ModifiedAt should not be zero")
	}
}

func TestDiscoverProjects_OrphanGroupedUnderUnknownProject(t *testing.T) {
	// Orphan subagent has no ProjectPath → should be grouped under "(unknown)".
	tmpDir, sessionsDir, _ := setupCodexDir(t)

	orphanID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	ts := "2026-06-01T10:00:00Z"

	// Write orphan with empty cwd to ensure it ends up in (unknown) project.
	fname := "rollout-2026-01-01T00-00-00-" + orphanID + ".jsonl"
	fpath := filepath.Join(sessionsDir, fname)
	content := `{"timestamp":"` + ts + `","type":"session_meta","payload":{"id":"` + orphanID + `","cwd":"","source":{"subagent":"compact"}}}
{"timestamp":"` + ts + `","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"compact work"}]}}
`
	if err := os.WriteFile(fpath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects: %v", err)
	}
	if len(projects) != 1 {
		t.Fatalf("expected 1 project, got %d", len(projects))
	}
	if projects[0].Key != "(unknown)" {
		t.Errorf("project Key = %q, want (unknown)", projects[0].Key)
	}
	if len(projects[0].Sessions) != 1 {
		t.Fatalf("expected 1 session in unknown project, got %d", len(projects[0].Sessions))
	}
	s := projects[0].Sessions[0]
	if s.SessionID != orphanID {
		t.Errorf("SessionID = %q", s.SessionID)
	}
	if s.Summary != "[compact subagent]" {
		t.Errorf("Summary = %q", s.Summary)
	}
}

func TestDiscoverProjects_MixedSubagentTypes(t *testing.T) {
	// Parent with review, compact, and thread_spawn children.
	tmpDir, sessionsDir, projDir := setupCodexDir(t)

	parentID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	reviewID := "11111111-2222-3333-4444-555555555555"
	compactID := "22222222-3333-4444-5555-666666666666"
	spawnID := "33333333-4444-5555-6666-777777777777"

	writeSessionFile(t, sessionsDir, parentID, "2026-01-01T00:00:00Z", projDir, `"cli"`, "parent")
	writeSessionFile(t, sessionsDir, reviewID, "2026-01-01T01:00:00Z", projDir,
		`{"subagent":"review"}`, "review it") // orphan — review has no parent_thread_id
	writeSessionFile(t, sessionsDir, compactID, "2026-01-01T02:00:00Z", projDir,
		`{"subagent":"compact"}`, "compact it") // orphan
	writeSessionFile(t, sessionsDir, spawnID, "2026-01-01T03:00:00Z", projDir,
		`{"subagent":{"thread_spawn":{"parent_thread_id":"`+parentID+`","depth":1}}}`, "spawned")

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects: %v", err)
	}
	all := collectAllSessions(projects)

	parent := findSession(all, parentID)
	if parent == nil {
		t.Fatal("parent not found")
	}
	// Only thread_spawn has parent_thread_id pointing to parent.
	if len(parent.Subagents) != 1 {
		t.Fatalf("expected 1 subagent (thread_spawn), got %d", len(parent.Subagents))
	}
	if parent.Subagents[0].AgentID != "thread_spawn" {
		t.Errorf("subagent AgentID = %q", parent.Subagents[0].AgentID)
	}

	// Review and compact are orphans (no ParentSessionID).
	review := findSession(all, reviewID)
	compact := findSession(all, compactID)
	if review == nil {
		t.Fatal("review orphan not found")
	}
	if compact == nil {
		t.Fatal("compact orphan not found")
	}
}

// ---------------------------------------------------------------------------
// FindSessionByID / FindSessionWithProject — subagent-aware
// ---------------------------------------------------------------------------

func TestFindSessionWithProject_ReturnsSubagents(t *testing.T) {
	tmpDir, sessionsDir, projDir := setupCodexDir(t)

	parentID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	childID := "11111111-2222-3333-4444-555555555555"
	ts := "2026-06-01T10:00:00Z"

	writeSessionFile(t, sessionsDir, parentID, ts, projDir, `"cli"`, "parent task")
	writeSessionFile(t, sessionsDir, childID, ts, projDir,
		`{"subagent":{"thread_spawn":{"parent_thread_id":"`+parentID+`","depth":1}}}`, "child task")

	sess, proj, err := FindSessionWithProject(tmpDir, parentID)
	if err != nil {
		t.Fatalf("FindSessionWithProject: %v", err)
	}
	if sess == nil {
		t.Fatal("session nil")
	}
	if proj == nil {
		t.Fatal("project nil")
	}
	if len(sess.Subagents) != 1 {
		t.Fatalf("expected 1 subagent, got %d", len(sess.Subagents))
	}
	if sess.Subagents[0].SessionID != childID {
		t.Errorf("subagent SessionID = %q, want %s", sess.Subagents[0].SessionID, childID)
	}
}

func TestFindSessionWithProject_OrphanFoundAsSession(t *testing.T) {
	tmpDir, sessionsDir, projDir := setupCodexDir(t)

	orphanID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	ts := "2026-06-01T10:00:00Z"

	writeSessionFile(t, sessionsDir, orphanID, ts, projDir,
		`{"subagent":"review"}`, "orphan task")

	sess, _, err := FindSessionWithProject(tmpDir, orphanID)
	if err != nil {
		t.Fatalf("FindSessionWithProject: %v", err)
	}
	if sess == nil {
		t.Fatal("session nil — orphan should be findable")
	}
	if sess.Summary != "[review subagent]" {
		t.Errorf("Summary = %q, want [review subagent]", sess.Summary)
	}
}

// ---------------------------------------------------------------------------
// SubagentSession.DisplayTitle
// ---------------------------------------------------------------------------

func TestSubagentSession_DisplayTitle(t *testing.T) {
	tests := []struct {
		name string
		sub  SubagentSession
		want string
	}{
		{
			name: "summary takes priority",
			sub:  SubagentSession{Summary: "sum", FirstPrompt: "fp", AgentID: "review"},
			want: "sum",
		},
		{
			name: "first prompt fallback",
			sub:  SubagentSession{FirstPrompt: "fp", AgentID: "review"},
			want: "fp",
		},
		{
			name: "agent id fallback",
			sub:  SubagentSession{AgentID: "compact"},
			want: "compact",
		},
		{
			name: "untitled fallback",
			sub:  SubagentSession{},
			want: "untitled",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.sub.DisplayTitle(); got != tt.want {
				t.Errorf("DisplayTitle() = %q, want %q", got, tt.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Orphan Session.DisplayTitle uses Summary
// ---------------------------------------------------------------------------

func TestOrphanSession_DisplayTitle(t *testing.T) {
	// When an orphan is promoted, it gets a Summary like "[review subagent]".
	s := Session{Summary: "[review subagent]", FirstPrompt: "do review"}
	if got := s.DisplayTitle(); got != "[review subagent]" {
		t.Errorf("DisplayTitle() = %q, want [review subagent]", got)
	}
}
