package tui

import "github.com/baaaaaaaka/codex-helper/internal/codexhistory"

// List snapshots contain immutable project/session data. Only the UI goroutine
// accesses them. Selection and scroll are not cached with list contents.
type listViews struct {
	projectsValid, sessionsValid                        bool
	projectRevision, sessionRevision, expansionRevision uint64
	cwd, projectFilter, sessionFilter                   string
	project                                             projectSelectionIdentity
	projects                                            []projectItem
	sessions                                            []sessionItem
}

func (s *uiState) projectItems(opts Options) []projectItem {
	v := &s.views
	if !v.projectsValid || v.projectRevision != s.projectRevision || v.cwd != opts.DefaultCwd || v.projectFilter != s.projectFilter {
		v.projects = filterProjects(buildProjectItems(s.projects, opts.DefaultCwd), s.projectFilter)
		v.projectsValid = true
		v.projectRevision = s.projectRevision
		v.cwd = opts.DefaultCwd
		v.projectFilter = s.projectFilter
		v.sessionsValid = false
	}
	return v.projects
}
func (s *uiState) sessionItems(project codexhistory.Project) []sessionItem {
	v := &s.views
	identity := projectIdentity(project)
	if !v.sessionsValid || v.sessionRevision != s.projectRevision || v.expansionRevision != s.expansionRevision || v.project != identity || v.sessionFilter != s.sessionFilter {
		v.sessions = filterSessions(buildSessionItems(project, s.expandedSessions), s.sessionFilter)
		v.sessionsValid = true
		v.sessionRevision = s.projectRevision
		v.expansionRevision = s.expansionRevision
		v.project = identity
		v.sessionFilter = s.sessionFilter
	}
	return v.sessions
}
