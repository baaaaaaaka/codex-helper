package codexhistory

// TODO(phase2): Implement Codex session discovery from ~/.codex/sessions/YYYY/MM/DD/

func DiscoverProjects(codexDir string) ([]Project, error) {
	// TODO(phase2): walk ~/.codex/sessions/ and group by working directory
	return []Project{}, nil
}

func FindSessionByID(codexDir, sessionID string) (*Session, error) {
	// TODO(phase2): locate session file by ID
	return nil, nil
}

func FindSessionWithProject(codexDir, sessionID string) (*Session, *Project, error) {
	// TODO(phase2): locate session and its parent project
	return nil, nil, nil
}

func SessionWorkingDir(s Session) string {
	// TODO(phase2): extract working directory from session metadata
	return s.ProjectPath
}
