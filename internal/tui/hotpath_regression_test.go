package tui

import (
	"context"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
)

func TestListViewsFilterReusesBaseItems(t *testing.T) {
	project := codexhistory.Project{Key: "p", Path: "/project", Sessions: []codexhistory.Session{{SessionID: "s", FirstPrompt: "task"}}}
	s := newTestState([]codexhistory.Project{project})
	s.projectItems(Options{})
	base := &s.views.projectBase[0]
	s.projectFilter = "project"
	if len(s.projectItems(Options{})) != 1 || base != &s.views.projectBase[0] {
		t.Fatal("filter rebuilt project base")
	}
	s.sessionItems(project)
	sessionBase := &s.views.sessionBase[0]
	s.sessionFilter = "task"
	s.sessionItems(project)
	if sessionBase != &s.views.sessionBase[0] {
		t.Fatal("filter rebuilt session base")
	}
	s.projectRevision++
	s.projectItems(Options{})
	if base == &s.views.projectBase[0] {
		t.Fatal("data revision failed to rebuild base")
	}
}

func TestPrepareLayoutReusesBodyAcrossStatusChanges(t *testing.T) {
	body := strings.Repeat("你好 answer\n", 100)
	first := prepareLayout(context.Background(), layoutJob{key: "old", lines: []string{"header", body}, body: body, width: 40})
	next := prepareLayout(context.Background(), layoutJob{key: "refresh", lines: []string{"changed header", body, "Refreshing preview..."}, body: body, width: 40, reuse: first.entry})
	if len(next.entry.bodyLines) == 0 || &first.entry.bodyLines[0] != &next.entry.bodyLines[0] {
		t.Fatal("status change rewrapped unchanged body")
	}
	if got := strings.Join(next.entry.lines, "\n"); !strings.HasPrefix(got, "changed header") || !strings.HasSuffix(got, "Refreshing preview...") {
		t.Fatal("stale header/footer")
	}
	resized := prepareLayout(context.Background(), layoutJob{lines: []string{body}, body: body, width: 20, reuse: first.entry})
	if &resized.entry.bodyLines[0] == &first.entry.bodyLines[0] {
		t.Fatal("resize retained stale wrapping")
	}
	changed := prepareLayout(context.Background(), layoutJob{lines: []string{body + "new"}, body: body + "new", width: 40, reuse: first.entry})
	if &changed.entry.bodyLines[0] == &first.entry.bodyLines[0] {
		t.Fatal("changed text retained stale wrapping")
	}
}
