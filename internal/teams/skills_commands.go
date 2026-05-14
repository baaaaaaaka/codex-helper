package teams

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/skills"
)

var newTeamsSkillsManagerForCommand = newTeamsSkillsManager

func (b *Bridge) handleSkillsCommand(ctx context.Context, chatID string, arg string) error {
	mgr, err := newTeamsSkillsManagerForCommand()
	if err != nil {
		return b.sendToChat(ctx, chatID, "skills setup failed: "+err.Error())
	}
	fields := strings.Fields(strings.TrimSpace(arg))
	name := ""
	action := "list"
	if len(fields) > 0 {
		action = strings.ToLower(fields[0])
	}
	if len(fields) > 1 {
		name = fields[1]
	}
	switch action {
	case "", "list", "status", "st":
		entries, err := mgr.List(ctx)
		if err != nil {
			return b.sendToChat(ctx, chatID, "skills list failed: "+err.Error())
		}
		return b.sendToChat(ctx, chatID, formatTeamsSkillEntries(entries))
	case "sync":
		results, err := mgr.Sync(ctx, skills.SyncOptions{Name: name, All: name == ""})
		body := formatTeamsSkillSyncResults(results)
		if err != nil {
			body += "\n\nSync failed: " + err.Error()
		}
		return b.sendToChat(ctx, chatID, body)
	case "push":
		changes, err := mgr.LocalChanges(ctx, name)
		if err != nil {
			return b.sendToChat(ctx, chatID, "skills push review failed: "+err.Error())
		}
		if len(changes) == 0 {
			return b.sendToChat(ctx, chatID, "No local skill changes to push.")
		}
		return b.sendToChat(ctx, chatID, formatTeamsSkillPushReview(changes))
	case "add", "remove", "rm":
		return b.sendToChat(ctx, chatID, "Use local `cxp skills "+action+"` for this operation so auth prompts and destructive confirmations happen in your terminal.")
	default:
		return b.sendToChat(ctx, chatID, "usage: `helper skills list`, `helper skills sync [name]`, or `helper skills push [name]`")
	}
}

func newTeamsSkillsManager() (*skills.Manager, error) {
	configBase, err := os.UserConfigDir()
	if err != nil {
		return nil, err
	}
	cacheBase, err := os.UserCacheDir()
	if err != nil {
		return nil, err
	}
	home, _ := os.UserHomeDir()
	codexDir := strings.TrimSpace(os.Getenv("CODEX_DIR"))
	if codexDir == "" {
		codexDir = strings.TrimSpace(os.Getenv("CODEX_HOME"))
	}
	if codexDir == "" && home != "" {
		codexDir = filepath.Join(home, ".codex")
	}
	return skills.NewManager(skills.ManagerOptions{
		ConfigDir: filepath.Join(configBase, "codex-proxy"),
		CacheDir:  filepath.Join(cacheBase, "codex-proxy", "skill-subscriptions"),
		CodexDir:  codexDir,
		HomeDir:   home,
	})
}

func formatTeamsSkillEntries(entries []skills.StatusEntry) string {
	if len(entries) == 0 {
		return "No skill subscriptions."
	}
	var b strings.Builder
	b.WriteString("## Skills\n")
	for _, entry := range entries {
		status := entry.State.Status
		if status == "" {
			status = skills.StatusReady
		}
		auto := "auto-sync off"
		if entry.Source.AutoSync {
			auto = "auto-sync on"
		}
		_, _ = fmt.Fprintf(&b, "\n- **%s**: %s, %s", entry.Source.Name, status, auto)
		if entry.Source.Ref != "" {
			_, _ = fmt.Fprintf(&b, ", ref `%s`", entry.Source.Ref)
		}
		if entry.Source.Path != "" {
			_, _ = fmt.Fprintf(&b, ", path `%s`", entry.Source.Path)
		}
		for _, skill := range entry.State.InstalledSkills {
			_, _ = fmt.Fprintf(&b, "\n  - `%s` -> `%s`", skill.Name, skill.ExportName)
		}
		if entry.State.LastError != "" {
			_, _ = fmt.Fprintf(&b, "\n  - error: %s", entry.State.LastError)
		}
	}
	return b.String()
}

func formatTeamsSkillSyncResults(results []skills.SyncResult) string {
	if len(results) == 0 {
		return "No skill subscriptions matched."
	}
	var b strings.Builder
	b.WriteString("## Skills Sync\n")
	for _, result := range results {
		if result.Error != nil {
			_, _ = fmt.Fprintf(&b, "\n- **%s**: failed: %v", result.Source.Name, result.Error)
			continue
		}
		_, _ = fmt.Fprintf(&b, "\n- **%s**: synced %d skill(s)", result.Source.Name, len(result.Installed))
		if result.Commit != "" {
			_, _ = fmt.Fprintf(&b, " at `%s`", shortTeamsSHA(result.Commit))
		}
	}
	return b.String()
}

func formatTeamsSkillPushReview(changes []skills.LocalChange) string {
	var b strings.Builder
	b.WriteString("## Skills Push Review\n\nLocal changes were found. Push is intentionally completed in a local terminal so each change can be confirmed before any git push.\n\nRun `cxp skills push` locally.\n")
	for _, change := range changes {
		_, _ = fmt.Fprintf(&b, "\n- **%s** `%s` in `%s`", strings.ToUpper(string(change.Kind)), change.SourcePath, change.Source.Name)
	}
	return b.String()
}

func shortTeamsSHA(v string) string {
	v = strings.TrimSpace(v)
	if len(v) <= 12 {
		return v
	}
	return v[:12]
}
