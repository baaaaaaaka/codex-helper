package codexhistory

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

func DiscoverProjects(codexDir string) ([]Project, error) {
	root, err := ResolveCodexDir(codexDir)
	if err != nil {
		return nil, err
	}
	sessionsDir := filepath.Join(root, "sessions")
	if !isDir(sessionsDir) {
		return nil, fmt.Errorf("Codex sessions dir not found: %s", sessionsDir)
	}

	historyIdx := loadHistoryIndex(root)

	files, err := collectSessionFiles(sessionsDir)
	if err != nil {
		return nil, fmt.Errorf("walk sessions dir: %w", err)
	}
	if len(files) == 0 {
		return nil, nil
	}

	var firstErr error
	sessionIndex := map[string]int{}
	sessions := make([]Session, 0, len(files))

	for _, filePath := range files {
		name := filepath.Base(filePath)
		sessionID := parseSessionIDFromFilename(name)
		if sessionID == "" {
			continue
		}

		meta, err := readSessionFileMetaCached(filePath)
		if err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("read session %s: %w", filePath, err)
			}
			continue
		}

		if meta.SessionID == "" {
			meta.SessionID = sessionID
		}

		// Enrich from history.jsonl
		if info, ok := historyIdx.lookup(sessionID); ok {
			if meta.FirstPrompt == "" && info.FirstPrompt != "" {
				meta.FirstPrompt = info.FirstPrompt
			}
			if meta.CreatedAt.IsZero() && !info.FirstPromptTime.IsZero() {
				meta.CreatedAt = info.FirstPromptTime
			}
			if meta.ModifiedAt.IsZero() && !info.FirstPromptTime.IsZero() {
				meta.ModifiedAt = info.FirstPromptTime
			}
		}

		// Fallback timestamps from filename
		if meta.CreatedAt.IsZero() {
			if ts := parseTimestampFromFilename(name); !ts.IsZero() {
				meta.CreatedAt = ts
			}
		}
		if meta.ModifiedAt.IsZero() {
			meta.ModifiedAt = meta.CreatedAt
		}

		sess := Session{
			SessionID:    sessionID,
			FirstPrompt:  meta.FirstPrompt,
			MessageCount: meta.MessageCount,
			CreatedAt:    meta.CreatedAt,
			ModifiedAt:   meta.ModifiedAt,
			ProjectPath:  strings.TrimSpace(meta.ProjectPath),
			FilePath:     filePath,
		}

		// Deduplicate by session ID, keep the more recent
		if existingIdx, ok := sessionIndex[sessionID]; ok {
			if sess.ModifiedAt.After(sessions[existingIdx].ModifiedAt) {
				sessions[existingIdx] = mergeSessionMetadata(sessions[existingIdx], sess)
			}
			continue
		}
		sessionIndex[sessionID] = len(sessions)
		sessions = append(sessions, sess)
	}

	sessions = filterEmptySessions(sessions)

	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].ModifiedAt.After(sessions[j].ModifiedAt)
	})

	projects := groupByProject(sessions)

	sort.Slice(projects, func(i, j int) bool {
		return strings.ToLower(projects[i].Path) < strings.ToLower(projects[j].Path)
	})

	if firstErr != nil {
		return projects, firstErr
	}
	return projects, nil
}

func groupByProject(sessions []Session) []Project {
	groups := map[string][]Session{}
	var keys []string
	for _, sess := range sessions {
		key := strings.TrimSpace(sess.ProjectPath)
		if key == "" {
			key = "(unknown)"
		}
		if _, ok := groups[key]; !ok {
			keys = append(keys, key)
		}
		groups[key] = append(groups[key], sess)
	}
	projects := make([]Project, 0, len(groups))
	for _, key := range keys {
		sessionsForKey := groups[key]
		path := key
		if path == "(unknown)" {
			path = ""
		}
		projectKey := path
		if projectKey == "" {
			projectKey = "(unknown)"
		}
		projects = append(projects, Project{
			Key:      projectKey,
			Path:     path,
			Sessions: sessionsForKey,
		})
	}
	return projects
}

func mergeSessionMetadata(base Session, other Session) Session {
	if base.Summary == "" && other.Summary != "" {
		base.Summary = other.Summary
	}
	if base.FirstPrompt == "" && other.FirstPrompt != "" {
		base.FirstPrompt = other.FirstPrompt
	}
	if other.MessageCount > base.MessageCount {
		base.MessageCount = other.MessageCount
	}

	if base.CreatedAt.IsZero() {
		base.CreatedAt = other.CreatedAt
	} else if !other.CreatedAt.IsZero() && other.CreatedAt.Before(base.CreatedAt) {
		base.CreatedAt = other.CreatedAt
	}

	if base.ModifiedAt.IsZero() {
		base.ModifiedAt = other.ModifiedAt
	} else if !other.ModifiedAt.IsZero() && other.ModifiedAt.After(base.ModifiedAt) {
		base.ModifiedAt = other.ModifiedAt
	}

	basePath := strings.TrimSpace(base.FilePath)
	otherPath := strings.TrimSpace(other.FilePath)
	if !isFile(basePath) && isFile(otherPath) {
		base.FilePath = otherPath
	}

	baseProjectPath := strings.TrimSpace(base.ProjectPath)
	otherProjectPath := strings.TrimSpace(other.ProjectPath)
	if baseProjectPath == "" && otherProjectPath != "" {
		base.ProjectPath = other.ProjectPath
	}

	return base
}

func FindSessionByID(codexDir, sessionID string) (*Session, error) {
	if strings.TrimSpace(sessionID) == "" {
		return nil, fmt.Errorf("empty session ID")
	}

	root, err := ResolveCodexDir(codexDir)
	if err != nil {
		return nil, err
	}
	sessionsDir := filepath.Join(root, "sessions")

	// Fast path: glob for files containing the session ID
	pattern := filepath.Join(sessionsDir, "**", "*"+sessionID+".jsonl")
	matches, _ := filepath.Glob(pattern)
	if len(matches) == 0 {
		// Try without nested glob (filepath.Glob doesn't support **)
		matches = globRecursive(sessionsDir, sessionID)
	}
	for _, filePath := range matches {
		meta, err := readSessionFileMetaCached(filePath)
		if err != nil {
			continue
		}
		name := filepath.Base(filePath)
		fileSessionID := parseSessionIDFromFilename(name)
		if fileSessionID == sessionID || meta.SessionID == sessionID {
			historyIdx := loadHistoryIndex(root)
			if info, ok := historyIdx.lookup(sessionID); ok {
				if meta.FirstPrompt == "" && info.FirstPrompt != "" {
					meta.FirstPrompt = info.FirstPrompt
				}
			}
			sess := &Session{
				SessionID:    sessionID,
				FirstPrompt:  meta.FirstPrompt,
				MessageCount: meta.MessageCount,
				CreatedAt:    meta.CreatedAt,
				ModifiedAt:   meta.ModifiedAt,
				ProjectPath:  strings.TrimSpace(meta.ProjectPath),
				FilePath:     filePath,
			}
			return sess, nil
		}
	}

	// Fallback: full discovery
	projects, err := DiscoverProjects(codexDir)
	if err != nil && len(projects) == 0 {
		return nil, err
	}
	for _, project := range projects {
		for i := range project.Sessions {
			if project.Sessions[i].SessionID == sessionID {
				sess := project.Sessions[i]
				return &sess, nil
			}
		}
	}
	return nil, fmt.Errorf("session not found: %s", sessionID)
}

func FindSessionWithProject(codexDir, sessionID string) (*Session, *Project, error) {
	if strings.TrimSpace(sessionID) == "" {
		return nil, nil, fmt.Errorf("empty session ID")
	}

	projects, err := DiscoverProjects(codexDir)
	if err != nil && len(projects) == 0 {
		return nil, nil, err
	}
	for i := range projects {
		for j := range projects[i].Sessions {
			if projects[i].Sessions[j].SessionID == sessionID {
				sess := projects[i].Sessions[j]
				proj := projects[i]
				return &sess, &proj, nil
			}
		}
	}
	return nil, nil, fmt.Errorf("session not found: %s", sessionID)
}

func SessionWorkingDir(s Session) string {
	path := strings.TrimSpace(s.ProjectPath)
	if isDir(path) {
		return path
	}
	if path != "" {
		return path
	}
	return ""
}

// globRecursive walks sessionsDir and returns files whose name contains sessionID.
func globRecursive(sessionsDir, sessionID string) []string {
	var matches []string
	_ = filepath.WalkDir(sessionsDir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if strings.Contains(d.Name(), sessionID) && strings.HasSuffix(d.Name(), ".jsonl") {
			matches = append(matches, path)
		}
		return nil
	})
	return matches
}

// ResetCache clears the session file cache. Useful for testing.
func ResetCache() {
	resetSessionFileCache()
}
