package tui

import (
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
)

// List snapshots contain immutable project/session data. Only the UI goroutine
// accesses them. Selection and scroll are not cached with list contents.
type listViews struct {
	projectsValid, sessionsValid                        bool
	projectFilterValid, sessionFilterValid              bool
	projectBase                                         []projectItem
	sessionBase                                         []sessionItem
	projectRevision, sessionRevision, expansionRevision uint64
	cwd, projectFilter, sessionFilter                   string
	project                                             projectSelectionIdentity
	projects                                            []projectItem
	sessions                                            []sessionItem
}

func (s *uiState) projectItems(opts Options) []projectItem {
	v := &s.views
	if !v.projectsValid || v.projectRevision != s.projectRevision || v.cwd != opts.DefaultCwd {
		v.projectBase = buildProjectItems(s.projects, opts.DefaultCwd)
		for i := range v.projectBase {
			v.projectBase[i].searchLabel = strings.ToLower(v.projectBase[i].label)
		}
		v.projectFilterValid = false
		v.projectsValid = true
		v.projectRevision = s.projectRevision
		v.cwd = opts.DefaultCwd
		v.sessionsValid = false
	}
	if !v.projectFilterValid || v.projectFilter != s.projectFilter {
		v.projects = filterProjects(v.projectBase, s.projectFilter)
		v.projectFilter = s.projectFilter
		v.projectFilterValid = true
	}
	return v.projects
}
func (s *uiState) sessionItems(project codexhistory.Project) []sessionItem {
	v := &s.views
	identity := projectIdentity(project)
	if !v.sessionsValid || v.sessionRevision != s.projectRevision || v.expansionRevision != s.expansionRevision || v.project != identity {
		v.sessionBase = buildSessionItems(project, s.expandedSessions)
		for i := range v.sessionBase {
			v.sessionBase[i].searchLabel = strings.ToLower(v.sessionBase[i].label)
		}
		v.sessionFilterValid = false
		v.sessionsValid = true
		v.sessionRevision = s.projectRevision
		v.expansionRevision = s.expansionRevision
		v.project = identity
	}
	if !v.sessionFilterValid || v.sessionFilter != s.sessionFilter {
		v.sessions = filterSessions(v.sessionBase, s.sessionFilter)
		v.sessionFilter = s.sessionFilter
		v.sessionFilterValid = true
	}
	return v.sessions
}
