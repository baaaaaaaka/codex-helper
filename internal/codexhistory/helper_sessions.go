package codexhistory

import "strings"

const (
	HelperSessionTitleKeyword        = "[codex-helper]"
	HelperControlSessionTitleKeyword = "[codex-helper-control]"
	HelperDebugSessionTitleKeyword   = "[codex-helper-debug]"
)

const (
	helperSessionKindControl = "control"
	helperSessionKindDebug   = "debug"
)

// HelperSessionKind returns a non-empty kind for helper-owned sessions that
// should not appear in normal history browsers.
func HelperSessionKind(s Session) string {
	if kind := helperSessionKindFromTitleFields(s.Summary, s.FirstPrompt); kind != "" {
		return kind
	}
	if isKnownHelperTempDebugSession(s) {
		return helperSessionKindDebug
	}
	return ""
}

func HelperSubagentSessionKind(s SubagentSession) string {
	return helperSessionKindFromTitleFields(s.Summary, s.FirstPrompt)
}

func IsHelperSession(s Session) bool {
	return HelperSessionKind(s) != ""
}

func IsHelperSubagentSession(s SubagentSession) bool {
	return HelperSubagentSessionKind(s) != ""
}

func MarkHelperSessionTitle(title string, kind string) string {
	title = strings.TrimSpace(title)
	if title == "" {
		title = "untitled"
	}
	if hasHelperTitleKeyword(title) {
		return title
	}
	switch kind {
	case helperSessionKindControl:
		return HelperControlSessionTitleKeyword + " " + title
	case helperSessionKindDebug:
		return HelperDebugSessionTitleKeyword + " " + title
	default:
		return HelperSessionTitleKeyword + " " + title
	}
}

func FilterUserVisibleProjects(projects []Project) []Project {
	if len(projects) == 0 {
		return nil
	}

	type projectAccumulator struct {
		project     Project
		seenSession map[string]bool
	}

	accumulators := map[string]*projectAccumulator{}
	order := make([]string, 0, len(projects))
	workspaceForProject := func(project Project, path string) *projectAccumulator {
		path = strings.TrimSpace(path)
		key := userVisibleProjectWorkspaceKey(project, path)
		acc := accumulators[key]
		if acc != nil {
			if acc.project.Path == "" && path != "" {
				acc.project.Path = path
			}
			if acc.project.Key == "" {
				acc.project.Key = strings.TrimSpace(project.Key)
			}
			return acc
		}
		acc = &projectAccumulator{
			project: Project{
				Key:  strings.TrimSpace(project.Key),
				Path: path,
			},
			seenSession: map[string]bool{},
		}
		accumulators[key] = acc
		order = append(order, key)
		return acc
	}

	for _, project := range projects {
		if len(project.Sessions) == 0 {
			_ = workspaceForProject(project, project.Path)
			continue
		}
		sessions := filterUserVisibleSessions(project.Path, project.Sessions)
		if len(sessions) == 0 {
			continue
		}
		for _, session := range sessions {
			workspacePath := strings.TrimSpace(session.ProjectPath)
			if workspacePath == "" {
				workspacePath = strings.TrimSpace(project.Path)
				session.ProjectPath = workspacePath
			}
			acc := workspaceForProject(project, workspacePath)
			if key := userVisibleSessionKey(session); key != "" {
				if acc.seenSession[key] {
					continue
				}
				acc.seenSession[key] = true
			}
			acc.project.Sessions = append(acc.project.Sessions, session)
		}
	}
	out := make([]Project, 0, len(order))
	for _, key := range order {
		out = append(out, accumulators[key].project)
	}
	return out
}

func userVisibleProjectWorkspaceKey(project Project, path string) string {
	path = strings.TrimSpace(path)
	if path != "" {
		return "path:" + normalizeHelperSessionPath(path)
	}
	if key := strings.TrimSpace(project.Key); key != "" {
		return "key:" + key
	}
	return "unknown"
}

func userVisibleSessionKey(session Session) string {
	if id := strings.TrimSpace(session.SessionID); id != "" {
		return "id:" + id
	}
	if filePath := strings.TrimSpace(session.FilePath); filePath != "" {
		return "file:" + normalizeHelperSessionPath(filePath)
	}
	return ""
}

func FilterUserVisibleSessions(sessions []Session) []Session {
	return filterUserVisibleSessions("", sessions)
}

func filterUserVisibleSessions(projectPath string, sessions []Session) []Session {
	if len(sessions) == 0 {
		return nil
	}
	out := make([]Session, 0, len(sessions))
	for _, session := range sessions {
		if strings.TrimSpace(session.ProjectPath) == "" {
			session.ProjectPath = strings.TrimSpace(projectPath)
		}
		if IsHelperSession(session) {
			continue
		}
		if len(session.Subagents) > 0 {
			session.Subagents = FilterUserVisibleSubagentSessions(session.Subagents)
		}
		out = append(out, session)
	}
	return out
}

func FilterUserVisibleSubagentSessions(sessions []SubagentSession) []SubagentSession {
	if len(sessions) == 0 {
		return nil
	}
	out := make([]SubagentSession, 0, len(sessions))
	for _, session := range sessions {
		if IsHelperSubagentSession(session) {
			continue
		}
		out = append(out, session)
	}
	return out
}

func displayTitleWithHelperMarker(title string, kind string) string {
	if kind == "" {
		return title
	}
	return MarkHelperSessionTitle(title, kind)
}

func helperSessionKindFromTitleFields(fields ...string) string {
	for _, field := range fields {
		if kind, ok := helperMarkerKindFromTitle(field); ok {
			return kind
		}
	}
	for _, field := range fields {
		if isLegacyControlFallbackPrompt(field) {
			return helperSessionKindControl
		}
	}
	return ""
}

func isLegacyControlFallbackPrompt(field string) bool {
	lower := strings.ToLower(strings.TrimSpace(field))
	if lower == "" {
		return false
	}
	return strings.HasPrefix(lower, "you are handling an unrecognized message from the user's microsoft teams control chat for codex-helper.") &&
		strings.Contains(lower, "\nuser message:")
}

func isKnownHelperTempDebugSession(s Session) bool {
	projectPath := normalizeHelperSessionPath(s.ProjectPath)
	if !isLikelyTempProjectPath(projectPath) {
		return false
	}
	if hasKnownHelperTempProjectPath(projectPath) {
		return true
	}

	joined := strings.ToLower(s.Summary + "\n" + s.FirstPrompt)
	for _, marker := range knownHelperTempPromptMarkers {
		if strings.Contains(joined, marker) {
			return true
		}
	}
	return false
}

func normalizeHelperSessionPath(path string) string {
	path = strings.TrimSpace(path)
	path = strings.ReplaceAll(path, "\\", "/")
	for strings.Contains(path, "//") {
		path = strings.ReplaceAll(path, "//", "/")
	}
	return path
}

func isLikelyTempProjectPath(path string) bool {
	lower := strings.ToLower(path)
	return strings.HasPrefix(lower, "/tmp/") ||
		strings.HasPrefix(lower, "/private/tmp/") ||
		strings.HasPrefix(lower, "/var/tmp/") ||
		strings.Contains(lower, "/var/folders/") ||
		strings.Contains(lower, "/appdata/local/temp/") ||
		strings.Contains(lower, "/appdata/local/temporary internet files/") ||
		strings.Contains(lower, "/temp/")
}

func hasKnownHelperTempProjectPath(path string) bool {
	lower := strings.ToLower(path)
	for _, marker := range knownHelperTempPathMarkers {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

var knownHelperTempPathMarkers = []string{
	"/codex-helper-real-probe",
	"/codex-json-",
	"/testlivebridgeconcurrentworkchatsoptin",
	"/testlivebridgerealcodexuserjourneyoptin",
	"/testlivebridgerealdevelopertasksoptin",
}

var knownHelperTempPromptMarkers = []string{
	"default-probe-ok",
	"model-probe-ok",
	"edit_sample_done",
	"codex_json_sample_ok",
	"tool_sample_done",
	"scn_art_",
	"scn_data_",
	"scn_docs_",
	"teams real codex real-codex-",
	"real-dev-fixed-",
	"direct-codex-smoke",
	"test-mini-done",
	"default-resume-one",
	"mini-resume-one",
	"mini-resume-two",
}

func hasHelperTitleKeyword(title string) bool {
	_, ok := helperMarkerKindFromTitle(title)
	return ok
}

func helperMarkerKindFromTitle(title string) (string, bool) {
	lower := strings.ToLower(strings.TrimSpace(title))
	switch {
	case strings.HasPrefix(lower, strings.ToLower(HelperControlSessionTitleKeyword)):
		return helperSessionKindControl, true
	case strings.HasPrefix(lower, strings.ToLower(HelperDebugSessionTitleKeyword)):
		return helperSessionKindDebug, true
	case strings.HasPrefix(lower, strings.ToLower(HelperSessionTitleKeyword)):
		return helperSessionKindDebug, true
	default:
		return "", false
	}
}
