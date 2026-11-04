package steps

import (
	"regexp"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/harness"
)

var (
	teamsChatIDRE = regexp.MustCompile(`19:[A-Za-z0-9_\-]+@(?:thread\.v2|thread\.tacv2|unq\.gbl\.spaces)`)
	uuidRE        = regexp.MustCompile(`(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`)
	turnKeyRE     = regexp.MustCompile(`\bturn[:_](?:inbound|outbound|[0-9a-f]{6,})`)
)

// InternalLeaks lists the internal identifiers found in a user-facing text:
// Teams chat and message ids, Codex thread and turn ids (every one the fake
// Codex handed out in this scenario, plus anything shaped like a UUID),
// helper turn keys, and helper state / Codex home paths. The user's own
// work directories and the helper's session ids (`s001`, which the user
// types in `r <id>`) are not internal.
func InternalLeaks(s *harness.Scenario, text string) []string {
	var out []string
	add := func(kind, v string) { out = append(out, kind+" "+v) }
	for _, m := range teamsChatIDRE.FindAllString(text, -1) {
		add("teams-chat-id", m)
	}
	for _, c := range s.Graph.Chats() {
		for _, m := range s.Graph.Messages(c.ID) {
			if len(m.ID) >= 8 && strings.Contains(text, m.ID) {
				add("teams-message-id", m.ID)
			}
		}
	}
	seen := map[string]bool{}
	for _, c := range s.Codex.Calls() {
		for kind, id := range map[string]string{"codex-thread-id": c.ThreadID, "codex-turn-id": c.TurnID} {
			if id != "" && !seen[id] && strings.Contains(text, id) {
				seen[id] = true
				add(kind, id)
			}
		}
	}
	for _, m := range uuidRE.FindAllString(text, -1) {
		if !seen[m] {
			add("uuid", m)
		}
	}
	for _, m := range turnKeyRE.FindAllString(text, -1) {
		add("turn-key", m)
	}
	e := s.Env
	for _, p := range []string{e.StateDir, e.ConfigDir, e.CacheDir, e.DataDir, e.RuntimeDir, e.CodexHome, e.SessionsDir, e.TokenDir} {
		if p != "" && strings.Contains(text, p) {
			add("path", p)
		}
	}
	for _, k := range []string{".jsonl", "rollout-", "/.codex", ".sqlite", ".db"} {
		if strings.Contains(text, k) {
			add("path-fragment", k)
		}
	}
	return out
}
