package teams

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const maxTrackedMessageIDs = 500

type Registry struct {
	Version          int                  `json:"version"`
	UserID           string               `json:"user_id,omitempty"`
	UserPrincipal    string               `json:"user_principal,omitempty"`
	ControlChatID    string               `json:"control_chat_id,omitempty"`
	ControlChatURL   string               `json:"control_chat_url,omitempty"`
	ControlChatTopic string               `json:"control_chat_topic,omitempty"`
	Sessions         []Session            `json:"sessions,omitempty"`
	Chats            map[string]ChatState `json:"chats,omitempty"`
}

type Session struct {
	ID            string    `json:"id"`
	ChatID        string    `json:"chat_id"`
	ChatURL       string    `json:"chat_url,omitempty"`
	Topic         string    `json:"topic"`
	Status        string    `json:"status"`
	CodexThreadID string    `json:"codex_thread_id,omitempty"`
	Cwd           string    `json:"cwd,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type ChatState struct {
	SeenMessageIDs []string `json:"seen_message_ids,omitempty"`
	SentMessageIDs []string `json:"sent_message_ids,omitempty"`
}

func DefaultRegistryPath() (string, error) {
	base, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "codex-helper", "teams-registry.json"), nil
}

func LoadRegistry(path string) (Registry, error) {
	if strings.TrimSpace(path) == "" {
		var err error
		path, err = DefaultRegistryPath()
		if err != nil {
			return Registry{}, err
		}
	}
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		reg := Registry{Version: 1}
		reg.ensureMaps()
		return reg, nil
	}
	if err != nil {
		return Registry{}, err
	}
	var reg Registry
	if err := json.Unmarshal(data, &reg); err != nil {
		return Registry{}, err
	}
	if reg.Version == 0 {
		reg.Version = 1
	}
	reg.ensureMaps()
	return reg, nil
}

func SaveRegistry(path string, reg Registry) error {
	if strings.TrimSpace(path) == "" {
		var err error
		path, err = DefaultRegistryPath()
		if err != nil {
			return err
		}
	}
	reg.ensureMaps()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(reg, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (r *Registry) ensureMaps() {
	if r.Version == 0 {
		r.Version = 1
	}
	if r.Chats == nil {
		r.Chats = make(map[string]ChatState)
	}
}

func (r *Registry) SessionByChatID(chatID string) *Session {
	for i := range r.Sessions {
		if r.Sessions[i].ChatID == chatID {
			return &r.Sessions[i]
		}
	}
	return nil
}

func (r *Registry) SessionByID(sessionID string) *Session {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" {
		return nil
	}
	for i := range r.Sessions {
		if r.Sessions[i].ID == sessionID {
			return &r.Sessions[i]
		}
	}
	return nil
}

func (r *Registry) SessionByCodexThreadID(threadID string) *Session {
	threadID = strings.TrimSpace(threadID)
	if threadID == "" {
		return nil
	}
	var fallback *Session
	for i := range r.Sessions {
		if r.Sessions[i].CodexThreadID == threadID {
			if r.Sessions[i].Status == "active" {
				return &r.Sessions[i]
			}
			if fallback == nil {
				fallback = &r.Sessions[i]
			}
		}
	}
	return fallback
}

func (r *Registry) NextSessionID() string {
	max := 0
	for _, s := range r.Sessions {
		if strings.HasPrefix(s.ID, "s") {
			var n int
			for _, ch := range strings.TrimPrefix(s.ID, "s") {
				if ch < '0' || ch > '9' {
					n = 0
					break
				}
				n = n*10 + int(ch-'0')
			}
			if n > max {
				max = n
			}
		}
	}
	return "s" + leftPadInt(max+1, 3)
}

func (r *Registry) ActiveSessions() []Session {
	var sessions []Session
	for _, s := range r.Sessions {
		if s.Status == "active" {
			sessions = append(sessions, s)
		}
	}
	sort.Slice(sessions, func(i, j int) bool {
		return sessions[i].UpdatedAt.After(sessions[j].UpdatedAt)
	})
	return sessions
}

func (r *Registry) HasSeen(chatID string, messageID string) bool {
	state := r.Chats[chatID]
	return containsString(state.SeenMessageIDs, messageID)
}

func (r *Registry) HasSent(chatID string, messageID string) bool {
	state := r.Chats[chatID]
	return containsString(state.SentMessageIDs, messageID)
}

func (r *Registry) MarkSeen(chatID string, messageID string) {
	if messageID == "" {
		return
	}
	r.ensureMaps()
	state := r.Chats[chatID]
	state.SeenMessageIDs = appendUniqueBounded(state.SeenMessageIDs, messageID, maxTrackedMessageIDs)
	r.Chats[chatID] = state
}

func (r *Registry) MarkSent(chatID string, messageID string) {
	if messageID == "" {
		return
	}
	r.ensureMaps()
	state := r.Chats[chatID]
	state.SentMessageIDs = appendUniqueBounded(state.SentMessageIDs, messageID, maxTrackedMessageIDs)
	state.SeenMessageIDs = appendUniqueBounded(state.SeenMessageIDs, messageID, maxTrackedMessageIDs)
	r.Chats[chatID] = state
}

func appendUniqueBounded(values []string, value string, limit int) []string {
	if containsString(values, value) {
		return values
	}
	values = append(values, value)
	if limit > 0 && len(values) > limit {
		values = values[len(values)-limit:]
	}
	return values
}

func containsString(values []string, value string) bool {
	for _, v := range values {
		if v == value {
			return true
		}
	}
	return false
}

func leftPadInt(n int, width int) string {
	s := strconvItoa(n)
	for len(s) < width {
		s = "0" + s
	}
	return s
}

func strconvItoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
