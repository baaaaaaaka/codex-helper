package codexhistory

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiscoverProjects_SortsProjectsAlphabetically(t *testing.T) {
	tmpDir, sessionsDir, _ := setupCodexDir(t)
	projectAlpha := filepath.Join(tmpDir, "alpha")
	projectZulu := filepath.Join(tmpDir, "zulu")
	if err := os.MkdirAll(projectAlpha, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(projectZulu, 0o755); err != nil {
		t.Fatal(err)
	}

	writeSessionFile(t, sessionsDir, "aaaaaaaa-1111-2222-3333-444444444444", "2026-01-01T05:00:00Z", projectZulu, `"cli"`, "zulu")
	writeSessionFile(t, sessionsDir, "bbbbbbbb-1111-2222-3333-444444444444", "2026-01-01T01:00:00Z", projectAlpha, `"cli"`, "alpha")

	projects, err := DiscoverProjects(tmpDir)
	if err != nil {
		t.Fatalf("DiscoverProjects: %v", err)
	}
	if len(projects) != 2 {
		t.Fatalf("expected 2 projects, got %d", len(projects))
	}
	if projects[0].Path != projectAlpha {
		t.Fatalf("expected alphabetical ordering, got %q first", projects[0].Path)
	}
	if projects[1].Path != projectZulu {
		t.Fatalf("expected zulu second, got %q", projects[1].Path)
	}
}
