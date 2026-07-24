package cli

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
)

func TestHistoryTuiPathUnderExcludedRoot(t *testing.T) {
	tests := []struct {
		path string
		want bool
	}{
		{path: "/tmp", want: true},
		{path: "/tmp/work", want: true},
		{path: "/tmp/work/nested", want: true},
		{path: "/tmp/../tmp/work", want: true},
		{path: "/private/tmp", want: true},
		{path: "/private/tmp/work", want: true},
		{path: "/private/tmp2", want: false},
		{path: "/tmp2", want: false},
		{path: "/tmp-work", want: false},
		{path: "/var/tmp/work", want: false},
		{path: "/home/baka/project", want: false},
		{path: "", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			if got := historyTuiPathUnderExcludedRoot(tt.path); got != tt.want {
				t.Fatalf("historyTuiPathUnderExcludedRoot(%q) = %t, want %t", tt.path, got, tt.want)
			}
		})
	}
}

func TestFilterHistoryTuiProjects(t *testing.T) {
	projects := []codexhistory.Project{
		{Key: "tmp-root", Path: "/tmp", Sessions: []codexhistory.Session{{SessionID: "tmp-root"}}},
		{Key: "tmp-child", Path: "/tmp/work", Sessions: []codexhistory.Session{{SessionID: "tmp-child"}}},
		{Key: "private-tmp", Path: "/private/tmp/work", Sessions: []codexhistory.Session{{SessionID: "private-tmp"}}},
		{Key: "tmp-prefix", Path: "/tmp2", Sessions: []codexhistory.Session{{SessionID: "tmp-prefix"}}},
		{
			Key:      "unknown-temp",
			Sessions: []codexhistory.Session{{ProjectPath: "/tmp/unknown"}},
		},
		{Key: "outside", Path: "/workspace/project"},
	}

	filtered := filterHistoryTuiProjects(projects)
	keys := make([]string, 0, len(filtered))
	for _, project := range filtered {
		keys = append(keys, project.Key)
	}
	if got, want := strings.Join(keys, ","), "tmp-prefix,outside"; got != want {
		t.Fatalf("filtered project keys = %q, want %q", got, want)
	}
}

func TestLoadHistoryTuiProjectsFiltersDiscoveredTempProjects(t *testing.T) {
	codexDir := t.TempDir()
	sessionsDir := filepath.Join(codexDir, "sessions")
	if err := os.MkdirAll(sessionsDir, 0o755); err != nil {
		t.Fatalf("mkdir sessions: %v", err)
	}
	t.Cleanup(func() { _ = codexhistory.CloseCaches() })

	writeHistoryTuiSessionFixture(t, sessionsDir, "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "/tmp/cxp-tui-hidden")
	writeHistoryTuiSessionFixture(t, sessionsDir, "11111111-2222-3333-4444-555555555555", "/workspace/cxp-tui-visible")

	projects, err := loadHistoryTuiProjects(context.Background(), codexDir)
	if err != nil {
		t.Fatalf("loadHistoryTuiProjects: %v", err)
	}
	if len(projects) != 1 {
		t.Fatalf("projects = %#v, want one visible project", projects)
	}
	if projects[0].Path != "/workspace/cxp-tui-visible" {
		t.Fatalf("visible project path = %q, want /workspace/cxp-tui-visible", projects[0].Path)
	}
	if len(projects[0].Sessions) != 1 || projects[0].Sessions[0].SessionID != "11111111-2222-3333-4444-555555555555" {
		t.Fatalf("visible sessions = %#v, want only the non-temp session", projects[0].Sessions)
	}
}

func writeHistoryTuiSessionFixture(t *testing.T, sessionsDir, sessionID, cwd string) {
	t.Helper()
	idJSON, err := json.Marshal(sessionID)
	if err != nil {
		t.Fatalf("marshal session id: %v", err)
	}
	cwdJSON, err := json.Marshal(cwd)
	if err != nil {
		t.Fatalf("marshal cwd: %v", err)
	}
	content := fmt.Sprintf(
		`{"timestamp":"2026-07-24T00:00:00Z","type":"session_meta","payload":{"id":%s,"cwd":%s,"source":"cli"}}
{"timestamp":"2026-07-24T00:01:00Z","type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"history filter fixture"}]}}
`, idJSON, cwdJSON)
	path := filepath.Join(sessionsDir, "rollout-2026-07-24T00-00-00-"+sessionID+".jsonl")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write session fixture: %v", err)
	}
}
