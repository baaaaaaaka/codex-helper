package codexhistory

import (
	"os"
	"path/filepath"
	"strings"
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
	if s.Summary != "" {
		return s.Summary
	}
	if s.FirstPrompt != "" {
		return s.FirstPrompt
	}
	if s.SessionID != "" {
		return s.SessionID
	}
	return "untitled"
}

func (s SubagentSession) DisplayTitle() string {
	if s.Summary != "" {
		return s.Summary
	}
	if s.FirstPrompt != "" {
		return s.FirstPrompt
	}
	if s.AgentID != "" {
		return s.AgentID
	}
	return "untitled"
}

func DefaultCodexDir() string {
	if v := os.Getenv(EnvCodexDir); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".codex")
}

func ResolveCodexDir(override string) (string, error) {
	if v := strings.TrimSpace(override); v != "" {
		return filepath.Clean(os.ExpandEnv(v)), nil
	}
	if v := strings.TrimSpace(os.Getenv(EnvCodexDir)); v != "" {
		return filepath.Clean(os.ExpandEnv(v)), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".codex"), nil
}
