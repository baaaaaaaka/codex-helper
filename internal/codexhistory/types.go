package codexhistory

import (
	"sort"
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
	SessionID       string
	ParentSessionID string `json:"-"`
	AgentID         string `json:"-"`
	ThreadName      string `json:",omitempty"`
	Summary         string
	FirstPrompt     string
	MessageCount    int
	CreatedAt       time.Time
	ModifiedAt      time.Time
	ProjectPath     string
	FilePath        string
	Subagents       []SubagentSession
}

// SessionGraphNode is the flat, relationship-preserving view of discovered
// Codex transcripts. DiscoverProjects keeps its historical tree-shaped
// compatibility view; stats and other graph consumers can use this view
// without losing nested thread_spawn parent IDs.
type SessionGraphNode struct {
	SessionID       string
	ParentSessionID string
	AgentID         string
	ThreadName      string
	Summary         string
	FirstPrompt     string
	MessageCount    int
	CreatedAt       time.Time
	ModifiedAt      time.Time
	ProjectPath     string
	FilePath        string
	IsSubagent      bool
}

func (n SessionGraphNode) AsSubagentSession() SubagentSession {
	return SubagentSession{
		AgentID:         n.AgentID,
		ParentSessionID: n.ParentSessionID,
		SessionID:       n.SessionID,
		ThreadName:      n.ThreadName,
		Summary:         n.Summary,
		FirstPrompt:     n.FirstPrompt,
		MessageCount:    n.MessageCount,
		CreatedAt:       n.CreatedAt,
		ModifiedAt:      n.ModifiedAt,
		FilePath:        n.FilePath,
	}
}

// FlattenSessionGraph preserves every discovered session ID and its immediate
// parent relationship. The legacy discovery shape attaches direct children to
// known parents and promotes nested/orphan records; this function normalizes
// both forms into one deterministic flat view.
func FlattenSessionGraph(projects []Project) []SessionGraphNode {
	byID := make(map[string]SessionGraphNode)
	order := make([]string, 0)
	add := func(node SessionGraphNode) {
		node.SessionID = strings.TrimSpace(node.SessionID)
		if node.SessionID == "" {
			return
		}
		if existing, ok := byID[node.SessionID]; ok {
			if existing.ParentSessionID == "" {
				existing.ParentSessionID = strings.TrimSpace(node.ParentSessionID)
			}
			if existing.AgentID == "" {
				existing.AgentID = strings.TrimSpace(node.AgentID)
			}
			if existing.ThreadName == "" {
				existing.ThreadName = node.ThreadName
			}
			if existing.Summary == "" {
				existing.Summary = node.Summary
			}
			if existing.FirstPrompt == "" {
				existing.FirstPrompt = node.FirstPrompt
			}
			if existing.FilePath == "" {
				existing.FilePath = node.FilePath
			}
			if existing.ProjectPath == "" {
				existing.ProjectPath = node.ProjectPath
			}
			if existing.CreatedAt.IsZero() || (!node.CreatedAt.IsZero() && node.CreatedAt.Before(existing.CreatedAt)) {
				existing.CreatedAt = node.CreatedAt
			}
			if node.ModifiedAt.After(existing.ModifiedAt) {
				existing.ModifiedAt = node.ModifiedAt
			}
			existing.IsSubagent = existing.IsSubagent || node.IsSubagent
			byID[node.SessionID] = existing
			return
		}
		node.ParentSessionID = strings.TrimSpace(node.ParentSessionID)
		node.AgentID = strings.TrimSpace(node.AgentID)
		node.IsSubagent = node.IsSubagent || node.ParentSessionID != ""
		byID[node.SessionID] = node
		order = append(order, node.SessionID)
	}

	for _, project := range projects {
		for _, session := range project.Sessions {
			add(SessionGraphNode{
				SessionID:       session.SessionID,
				ParentSessionID: session.ParentSessionID,
				AgentID:         session.AgentID,
				ThreadName:      session.ThreadName,
				Summary:         session.Summary,
				FirstPrompt:     session.FirstPrompt,
				MessageCount:    session.MessageCount,
				CreatedAt:       session.CreatedAt,
				ModifiedAt:      session.ModifiedAt,
				ProjectPath:     firstNonEmptyString(session.ProjectPath, project.Path),
				FilePath:        session.FilePath,
				IsSubagent:      session.ParentSessionID != "",
			})
			for _, subagent := range session.Subagents {
				add(SessionGraphNode{
					SessionID:       subagent.SessionID,
					ParentSessionID: subagent.ParentSessionID,
					AgentID:         subagent.AgentID,
					ThreadName:      subagent.ThreadName,
					Summary:         subagent.Summary,
					FirstPrompt:     subagent.FirstPrompt,
					MessageCount:    subagent.MessageCount,
					CreatedAt:       subagent.CreatedAt,
					ModifiedAt:      subagent.ModifiedAt,
					ProjectPath:     project.Path,
					FilePath:        subagent.FilePath,
					IsSubagent:      true,
				})
			}
		}
	}

	result := make([]SessionGraphNode, 0, len(order))
	for _, id := range order {
		result = append(result, byID[id])
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].SessionID != result[j].SessionID {
			return result[i].SessionID < result[j].SessionID
		}
		return result[i].FilePath < result[j].FilePath
	})
	return result
}

type SubagentSession struct {
	AgentID         string
	ParentSessionID string
	SessionID       string
	ThreadName      string `json:",omitempty"`
	Summary         string
	FirstPrompt     string
	MessageCount    int
	CreatedAt       time.Time
	ModifiedAt      time.Time
	FilePath        string
}

func (s Session) DisplayTitle() string {
	kind := HelperSessionKind(s)
	if s.ThreadName != "" {
		return displayTitleWithHelperMarker(s.ThreadName, kind)
	}
	if s.Summary != "" {
		return displayTitleWithHelperMarker(s.Summary, kind)
	}
	if title := firstPromptTitleText(s.FirstPrompt); title != "" {
		return displayTitleWithHelperMarker(title, kind)
	}
	if s.SessionID != "" {
		return displayTitleWithHelperMarker(s.SessionID, kind)
	}
	return displayTitleWithHelperMarker("untitled", kind)
}

func (s SubagentSession) DisplayTitle() string {
	kind := HelperSubagentSessionKind(s)
	if s.ThreadName != "" {
		return displayTitleWithHelperMarker(s.ThreadName, kind)
	}
	if s.Summary != "" {
		return displayTitleWithHelperMarker(s.Summary, kind)
	}
	if title := firstPromptTitleText(s.FirstPrompt); title != "" {
		return displayTitleWithHelperMarker(title, kind)
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
