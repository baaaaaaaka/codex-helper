package codexhistory

import (
	"time"
)

const EnvCodexDir = "CODEX_DIR"

type Project struct {
	Key      string
	Path     string
	Sessions []Session
}

type Session struct {
	SessionID    string
	Summary      string
	FirstPrompt  string
	MessageCount int
	CreatedAt    time.Time
	ModifiedAt   time.Time
	ProjectPath  string
	FilePath     string
	Subagents    []SubagentSession
}

type SubagentSession struct {
	AgentID         string
	ParentSessionID string
	SessionID       string
	Summary         string
	FirstPrompt     string
	MessageCount    int
	CreatedAt       time.Time
	ModifiedAt      time.Time
	FilePath        string
}

func (s Session) DisplayTitle() string {
	kind := HelperSessionKind(s)
	if s.Summary != "" {
		return displayTitleWithHelperMarker(s.Summary, kind)
	}
	if s.FirstPrompt != "" {
		return displayTitleWithHelperMarker(s.FirstPrompt, kind)
	}
	if s.SessionID != "" {
		return displayTitleWithHelperMarker(s.SessionID, kind)
	}
	return displayTitleWithHelperMarker("untitled", kind)
}

func (s SubagentSession) DisplayTitle() string {
	kind := HelperSubagentSessionKind(s)
	if s.Summary != "" {
		return displayTitleWithHelperMarker(s.Summary, kind)
	}
	if s.FirstPrompt != "" {
		return displayTitleWithHelperMarker(s.FirstPrompt, kind)
	}
	if s.AgentID != "" {
		return displayTitleWithHelperMarker(s.AgentID, kind)
	}
	return displayTitleWithHelperMarker("untitled", kind)
}

func ResolveCodexDir(override string) (string, error) {
	resolution, err := ResolveCodexDirSelection(override)
	if err != nil {
		return "", err
	}
	return resolution.Dir, nil
}
