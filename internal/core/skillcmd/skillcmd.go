// Package skillcmd is the chat `skills` command of the new core: a thin
// engine.Skills over internal/skills (list, add, sync). The parsers and
// formatters are copied from the legacy Teams bridge. `push` reviews and
// pushes from a chat (push.go, gap batch B); remove stays in the local
// CLI.
package skillcmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/skills"
)

// Options locate the skill subscription state (defaults: the user's config
// and cache dirs, $CODEX_HOME or ~/.codex).
type Options struct {
	ConfigDir, CacheDir, CodexDir, HomeDir string
}

// Command implements engine.Skills.
type Command struct {
	opts Options
	mu   sync.Mutex // the push reviews file (push.go)
}

// New returns the skills command backend.
func New(opts Options) *Command { return &Command{opts: opts} }

const usage = "Usage: `helper skills list`, `helper skills add <github/gitlab/git-url>`, `helper skills sync [name]`, " +
	"`helper skills push [--direct] [name]` or `helper skills push [--direct] confirm`. " +
	"Use the local `cxp skills` command to remove skills."

// Command runs one `skills …` command outside any chat and returns the
// reply; `push` needs a chat (ChatCommand).
func (c *Command) Command(ctx context.Context, args string) string {
	return c.ChatCommand(ctx, "", args)
}

// ChatCommand runs one `skills …` command of chat (engine.ChatSkills): a
// `push` review is kept for the chat it was made in.
func (c *Command) ChatCommand(ctx context.Context, chat, args string) string {
	action, rest := split(args)
	switch action {
	case "", "list", "ls", "status", "st":
	case "add", "sync":
	case "push":
		if strings.TrimSpace(chat) == "" {
			return "Use the local `cxp skills push` command for this, so sign-in prompts and confirmations happen in your terminal."
		}
	case "remove", "rm":
		return "Use the local `cxp skills " + action + "` command for this, so sign-in prompts and confirmations happen in your terminal."
	case "help", "?":
		return usage
	default:
		return usage
	}
	mgr, warning, err := c.manager(ctx)
	if err != nil {
		return "Skills are not available: " + err.Error()
	}
	var body string
	switch action {
	case "push":
		body = c.push(ctx, chat, mgr, rest)
	case "add":
		raw, perr := parseAdd(rest)
		if perr != nil {
			return perr.Error()
		}
		if raw == "" {
			return "Usage: `helper skills add <github/gitlab/git-url>`"
		}
		source, result, err := mgr.Add(ctx, raw, skills.AddOptions{})
		body = formatAdd(source, result, err)
	case "sync":
		name := first(rest)
		results, err := mgr.Sync(ctx, skills.SyncOptions{Name: name, All: name == ""})
		body = formatSync(results)
		if err != nil {
			body += "\n\nSync failed: " + err.Error()
		}
	default:
		entries, err := mgr.List(ctx)
		if err != nil {
			return "Listing skills failed: " + err.Error()
		}
		body = formatEntries(entries)
	}
	if warning != "" {
		body += "\n\n" + warning
	}
	return body
}

// resolved fills the default directories in.
func (c *Command) resolved() (Options, error) {
	o := c.opts
	if o.ConfigDir == "" {
		base, err := os.UserConfigDir()
		if err != nil {
			return o, err
		}
		o.ConfigDir = filepath.Join(base, "codex-proxy")
	}
	if o.CacheDir == "" {
		base, err := os.UserCacheDir()
		if err != nil {
			return o, err
		}
		o.CacheDir = filepath.Join(base, "codex-proxy", "skill-subscriptions")
	}
	if o.HomeDir == "" {
		o.HomeDir, _ = os.UserHomeDir()
	}
	if o.CodexDir == "" {
		o.CodexDir = firstNonEmpty(strings.TrimSpace(os.Getenv("CODEX_DIR")), strings.TrimSpace(os.Getenv("CODEX_HOME")))
		if o.CodexDir == "" && o.HomeDir != "" {
			o.CodexDir = filepath.Join(o.HomeDir, ".codex")
		}
	}
	return o, nil
}

func (c *Command) manager(ctx context.Context) (*skills.Manager, string, error) {
	o, err := c.resolved()
	if err != nil {
		return nil, "", err
	}
	mgr, err := skills.NewManager(skills.ManagerOptions{ConfigDir: o.ConfigDir, CacheDir: o.CacheDir, CodexDir: o.CodexDir, HomeDir: o.HomeDir})
	if err != nil {
		return nil, "", err
	}
	mctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	report, err := mgr.MigrateLegacySkills(mctx, skills.MigrationOptions{IncludeBuiltins: true})
	switch {
	case err != nil:
		return mgr, "Skills migration warning: " + err.Error(), nil
	case report.Failed:
		return mgr, "Skills migration warning: some managed skills could not be migrated; run the local `cxp skills doctor` for details.", nil
	}
	return mgr, "", nil
}

func split(arg string) (string, string) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return "", ""
	}
	action, rest, _ := strings.Cut(arg, " ")
	return strings.ToLower(strings.TrimSpace(action)), strings.TrimSpace(rest)
}

func first(rest string) string {
	f := strings.Fields(rest)
	if len(f) == 0 {
		return ""
	}
	return f[0]
}

func parseAdd(rest string) (string, error) {
	var raw []string
	for _, f := range strings.Fields(rest) {
		if strings.HasPrefix(f, "-") {
			return "", fmt.Errorf("Usage: `helper skills add <github/gitlab/git-url>`")
		}
		raw = append(raw, strings.Trim(f, "<>`"))
	}
	return strings.Join(raw, " "), nil
}

func formatEntries(entries []skills.StatusEntry) string {
	if len(entries) == 0 {
		return "No skill subscriptions. Add one with `helper skills add <github/gitlab/git-url>`."
	}
	var b strings.Builder
	b.WriteString("Skills:\n")
	for _, entry := range entries {
		status := entry.State.Status
		if status == "" {
			status = skills.StatusReady
		}
		auto := "auto-sync off"
		if entry.Source.AutoSync {
			auto = "auto-sync on"
		}
		fmt.Fprintf(&b, "\n- **%s**: %s, %s, target `%s`", entry.Source.Name, status, auto, firstNonEmpty(entry.Source.TargetKind, skills.TargetAgents))
		if entry.Source.Ref != "" {
			fmt.Fprintf(&b, ", ref `%s`", entry.Source.Ref)
		}
		for _, s := range entry.State.InstalledSkills {
			fmt.Fprintf(&b, "\n  - `%s` -> `%s`", s.Name, s.ExportName)
		}
		if entry.State.LastError != "" {
			fmt.Fprintf(&b, "\n  - error: %s", entry.State.LastError)
		}
	}
	return b.String()
}

func formatAdd(source skills.Source, result skills.SyncResult, err error) string {
	if source.Name == "" {
		if err != nil {
			return "Adding the skill failed: " + err.Error()
		}
		return "Adding the skill failed."
	}
	var b strings.Builder
	fmt.Fprintf(&b, "Added **%s**", source.Name)
	if source.RemoteURL != "" {
		fmt.Fprintf(&b, " from `%s`", skills.RedactURLSecrets(source.RemoteURL))
	}
	if source.Ref != "" {
		fmt.Fprintf(&b, " (ref `%s`)", source.Ref)
	}
	b.WriteString(".")
	if err != nil {
		fmt.Fprintf(&b, "\n\nThe first sync failed: %s", err.Error())
		return b.String()
	}
	fmt.Fprintf(&b, "\n\nInstalled %d skill(s).", len(result.Installed))
	for _, s := range result.Installed {
		fmt.Fprintf(&b, "\n- `%s` -> `%s`", s.Name, s.ExportName)
	}
	return b.String()
}

func formatSync(results []skills.SyncResult) string {
	if len(results) == 0 {
		return "No skill subscriptions matched."
	}
	var b strings.Builder
	b.WriteString("Skills sync:\n")
	for _, r := range results {
		if r.Error != nil {
			fmt.Fprintf(&b, "\n- **%s**: failed: %v", r.Source.Name, r.Error)
			continue
		}
		fmt.Fprintf(&b, "\n- **%s**: synced %d skill(s)", r.Source.Name, len(r.Installed))
		if c := strings.TrimSpace(r.Commit); c != "" {
			if len(c) > 12 {
				c = c[:12]
			}
			fmt.Fprintf(&b, " at `%s`", c)
		}
	}
	return b.String()
}

func firstNonEmpty(v ...string) string {
	for _, s := range v {
		if strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}
