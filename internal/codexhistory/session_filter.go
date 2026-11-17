package codexhistory

import "strings"

func filterEmptySessions(sessions []Session) []Session {
	if len(sessions) == 0 {
		return sessions
	}
	// Most cached directories contain no empty sessions. Reuse their slice;
	// never mutate a caller-owned slice when filtering is necessary.
	firstEmpty := -1
	for i, sess := range sessions {
		if isEmptySession(sess) {
			firstEmpty = i
			break
		}
	}
	if firstEmpty < 0 {
		return sessions
	}
	out := make([]Session, 0, len(sessions)-1)
	out = append(out, sessions[:firstEmpty]...)
	for _, sess := range sessions[firstEmpty+1:] {
		if isEmptySession(sess) {
			continue
		}
		out = append(out, sess)
	}
	return out
}

func isEmptySession(session Session) bool {
	if session.MessageCount != 0 {
		return false
	}
	if strings.TrimSpace(session.FirstPrompt) != "" {
		return false
	}
	if strings.TrimSpace(session.Summary) != "" {
		return false
	}
	return true
}
