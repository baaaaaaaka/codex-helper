package cli

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/skills"
)

func newSkillsCmd(root *rootOptions) *cobra.Command {
	var codexDir string
	cmd := &cobra.Command{
		Use:   "skills",
		Short: "Manage Codex skill subscriptions",
	}
	cmd.PersistentFlags().StringVar(&codexDir, "codex-dir", "", "Override Codex data dir (default: ~/.codex)")
	cmd.AddCommand(
		newSkillsInstallBuiltinCmd(root, &codexDir),
		newSkillsAddCmd(root, &codexDir),
		newSkillsListCmd(root, &codexDir),
		newSkillsSyncCmd(root, &codexDir),
		newSkillsRemoveCmd(root, &codexDir),
		newSkillsDoctorCmd(root, &codexDir),
		newSkillsPushCmd(root, &codexDir),
	)
	return cmd
}

func newSkillsInstallBuiltinCmd(root *rootOptions, codexDir *string) *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "install-builtin [name]",
		Short: "Install bundled skills into the Codex skills directory",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, stop := withSignalContext(cmd.Context())
			defer stop()
			mgr, err := newSkillsManager(root, *codexDir, cmd.OutOrStdout())
			if err != nil {
				return err
			}
			name := ""
			if len(args) > 0 {
				name = args[0]
			}
			if !yes {
				target, err := mgr.TargetRoot(skills.TargetCodexHome)
				if err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Target: %s\n", target)
				if strings.TrimSpace(name) == "" {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Built-in skills: %s\n", strings.Join(skills.BuiltinSkillNames(), ", "))
				} else {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Built-in skill: %s\n", name)
				}
				ok, err := promptSkillYesNo(cmd.InOrStdin(), cmd.OutOrStdout(), "Install bundled skill(s)?", true)
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
			}
			results, err := mgr.InstallBuiltins(ctx, skills.BuiltinInstallOptions{Name: name})
			for _, result := range results {
				for _, skill := range result.Installed {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Installed bundled skill %s -> %s\n", skill.Name, skill.TargetPath)
				}
			}
			return err
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "Skip the install confirmation prompt")
	return cmd
}

func newSkillsAddCmd(root *rootOptions, codexDir *string) *cobra.Command {
	var name string
	var ref string
	var subPath string
	var target string
	var noAutoSync bool
	var yes bool
	cmd := &cobra.Command{
		Use:   "add <github/gitlab/git-url>",
		Short: "Install skills from a git source and keep them updated",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, stop := withSignalContext(cmd.Context())
			defer stop()
			mgr, err := newSkillsManager(root, *codexDir, cmd.OutOrStdout())
			if err != nil {
				return err
			}
			auto := !noAutoSync
			parsed, err := skills.ParseURL(args[0], skills.URLParseOptions{Name: name, Ref: ref, Path: subPath})
			if err != nil {
				return err
			}
			if !yes {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Source: %s\n", parsed.RemoteURL)
				if parsed.Ref != "" {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Ref: %s\n", parsed.Ref)
				}
				if parsed.Path != "" {
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Path: %s\n", parsed.Path)
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Target: %s\n", firstNonEmptyString(target, skills.TargetCodexHome))
				if auto {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Auto-sync: on")
				} else {
					_, _ = fmt.Fprintln(cmd.OutOrStdout(), "Auto-sync: off")
				}
				ok, err := promptSkillYesNo(cmd.InOrStdin(), cmd.OutOrStdout(), "Install and keep updated?", true)
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
			}
			source, result, err := mgr.Add(ctx, args[0], skills.AddOptions{
				Name:       name,
				Ref:        ref,
				Path:       subPath,
				TargetKind: target,
				AutoSync:   &auto,
			})
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Installed %d skill(s) from %s\n", len(result.Installed), source.Name)
			for _, skill := range result.Installed {
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "- %s -> %s\n", skill.Name, skill.TargetPath)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "Local source name (default: inferred)")
	cmd.Flags().StringVar(&ref, "ref", "", "Branch, tag, or commit to sync (default: remote default branch or URL branch)")
	cmd.Flags().StringVar(&subPath, "path", "", "Subdirectory containing skills (default: inferred from URL or repository root)")
	cmd.Flags().StringVar(&target, "target", skills.TargetCodexHome, "Install target: codex-home or agents")
	cmd.Flags().BoolVar(&noAutoSync, "no-auto-sync", false, "Disable daily auto-sync for this source")
	cmd.Flags().BoolVar(&yes, "yes", false, "Skip the install confirmation prompt")
	return cmd
}

func newSkillsListCmd(root *rootOptions, codexDir *string) *cobra.Command {
	var asJSON bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List skill subscriptions",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			mgr, err := newSkillsManager(root, *codexDir, cmd.OutOrStdout())
			if err != nil {
				return err
			}
			entries, err := mgr.List(cmd.Context())
			if err != nil {
				return err
			}
			if asJSON {
				data, err := json.MarshalIndent(entries, "", "  ")
				if err != nil {
					return err
				}
				_, _ = fmt.Fprintln(cmd.OutOrStdout(), string(data))
				return nil
			}
			printSkillEntries(cmd.OutOrStdout(), entries)
			return nil
		},
	}
	cmd.Flags().BoolVar(&asJSON, "json", false, "Print JSON")
	return cmd
}

func newSkillsSyncCmd(root *rootOptions, codexDir *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sync [name]",
		Short: "Sync one skill source, or all sources when no name is given",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, stop := withSignalContext(cmd.Context())
			defer stop()
			mgr, err := newSkillsManager(root, *codexDir, cmd.OutOrStdout())
			if err != nil {
				return err
			}
			name := ""
			if len(args) > 0 {
				name = args[0]
			}
			results, err := mgr.Sync(ctx, skills.SyncOptions{Name: name, All: name == ""})
			for _, result := range results {
				if result.Error != nil {
					_, _ = fmt.Fprintf(cmd.ErrOrStderr(), "%s: %v\n", result.Source.Name, result.Error)
					continue
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "%s: synced %d skill(s) at %s\n", result.Source.Name, len(result.Installed), shortSHA(result.Commit))
			}
			return err
		},
	}
	return cmd
}

func newSkillsRemoveCmd(root *rootOptions, codexDir *string) *cobra.Command {
	var yes bool
	cmd := &cobra.Command{
		Use:   "remove <name>",
		Short: "Remove a skill subscription and its managed installed skills",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, stop := withSignalContext(cmd.Context())
			defer stop()
			if !yes {
				ok, err := promptSkillYesNo(cmd.InOrStdin(), cmd.OutOrStdout(), "Remove this subscription and managed installed skills?", false)
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
			}
			mgr, err := newSkillsManager(root, *codexDir, cmd.OutOrStdout())
			if err != nil {
				return err
			}
			source, err := mgr.Remove(ctx, args[0])
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Removed %s\n", source.Name)
			return nil
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "Skip the remove confirmation prompt")
	return cmd
}

func newSkillsDoctorCmd(root *rootOptions, codexDir *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "doctor [name]",
		Short: "Check local skill subscription state",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			mgr, err := newSkillsManager(root, *codexDir, cmd.OutOrStdout())
			if err != nil {
				return err
			}
			entries, err := mgr.List(cmd.Context())
			if err != nil {
				return err
			}
			name := ""
			if len(args) > 0 {
				name = args[0]
			}
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Config: %s\n", mgr.Store.ConfigPath())
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "State: %s\n", mgr.Store.StatePath())
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Cache: %s\n", mgr.CacheDir)
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Codex skill root: %s\n", filepath.Join(mgr.CodexDir, "skills"))
			_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Agents skill root: %s\n", filepath.Join(mgr.HomeDir, ".agents", "skills"))
			found := false
			for _, entry := range entries {
				if name != "" && !skillSourceMatches(entry.Source, name) {
					continue
				}
				found = true
				printSkillEntryDoctor(cmd.OutOrStdout(), entry)
			}
			if name != "" && !found {
				return fmt.Errorf("skill source %q not found", name)
			}
			return nil
		},
	}
	return cmd
}

func newSkillsPushCmd(root *rootOptions, codexDir *string) *cobra.Command {
	var direct bool
	cmd := &cobra.Command{
		Use:   "push [name]",
		Short: "Push local edits to subscribed skill sources with per-change confirmation",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, stop := withSignalContext(cmd.Context())
			defer stop()
			mgr, err := newSkillsManager(root, *codexDir, cmd.OutOrStdout())
			if err != nil {
				return err
			}
			name := ""
			if len(args) > 0 {
				name = args[0]
			}
			return runSkillsPush(ctx, mgr, name, direct, cmd.InOrStdin(), cmd.OutOrStdout())
		},
	}
	cmd.Flags().BoolVar(&direct, "direct", false, "Push directly to the subscribed ref instead of a review branch")
	return cmd
}

func newSkillsManager(root *rootOptions, codexDir string, out io.Writer) (*skills.Manager, error) {
	_, paths, err := newRootStore(root, codexDir)
	if err != nil {
		return nil, err
	}
	return newSkillsManagerForPaths(paths, out)
}

func newSkillsManagerForPaths(paths effectivePaths, out io.Writer) (*skills.Manager, error) {
	cacheDir, err := defaultSkillsCacheDir()
	if err != nil {
		return nil, err
	}
	return skills.NewManager(skills.ManagerOptions{
		ConfigDir: filepath.Dir(paths.ConfigPath),
		CacheDir:  cacheDir,
		CodexDir:  paths.CodexDir,
		HomeDir:   paths.Home,
		Out:       out,
	})
}

func startSkillsDailyAutoSync(ctx context.Context, paths effectivePaths) {
	mgr, err := newSkillsManagerForPaths(paths, io.Discard)
	if err != nil {
		return
	}
	mgr.StartDailyAutoSync(ctx)
}

func defaultSkillsCacheDir() (string, error) {
	base, err := os.UserCacheDir()
	if err != nil {
		return "", fmt.Errorf("get user cache dir: %w", err)
	}
	return filepath.Join(base, "codex-proxy", "skill-subscriptions"), nil
}

func printSkillEntries(out io.Writer, entries []skills.StatusEntry) {
	if len(entries) == 0 {
		_, _ = fmt.Fprintln(out, "No skill subscriptions.")
		return
	}
	for _, entry := range entries {
		state := entry.State.Status
		if state == "" {
			state = skills.StatusReady
		}
		auto := "auto-sync off"
		if entry.Source.AutoSync {
			auto = "auto-sync on"
		}
		_, _ = fmt.Fprintf(out, "%s  %s  %s\n", entry.Source.Name, state, auto)
		_, _ = fmt.Fprintf(out, "  %s", entry.Source.RemoteURL)
		if entry.Source.Ref != "" {
			_, _ = fmt.Fprintf(out, " @ %s", entry.Source.Ref)
		}
		if entry.Source.Path != "" {
			_, _ = fmt.Fprintf(out, " path %s", entry.Source.Path)
		}
		_, _ = fmt.Fprintln(out)
		for _, skill := range entry.State.InstalledSkills {
			_, _ = fmt.Fprintf(out, "  - %s -> %s\n", skill.Name, skill.TargetPath)
		}
		if entry.State.LastError != "" {
			_, _ = fmt.Fprintf(out, "  error: %s\n", entry.State.LastError)
		}
	}
}

func printSkillEntryDoctor(out io.Writer, entry skills.StatusEntry) {
	_, _ = fmt.Fprintf(out, "\nSource: %s\n", entry.Source.Name)
	_, _ = fmt.Fprintf(out, "  id: %s\n", entry.Source.ID)
	_, _ = fmt.Fprintf(out, "  status: %s\n", firstNonEmptyString(entry.State.Status, skills.StatusReady))
	_, _ = fmt.Fprintf(out, "  target: %s (%s)\n", entry.Source.TargetRoot, entry.Source.TargetKind)
	if entry.State.LastSyncAt.IsZero() {
		_, _ = fmt.Fprintln(out, "  last sync: never")
	} else {
		_, _ = fmt.Fprintf(out, "  last sync: %s\n", entry.State.LastSyncAt.Format(time.RFC3339))
	}
	if entry.State.LastCommit != "" {
		_, _ = fmt.Fprintf(out, "  commit: %s\n", entry.State.LastCommit)
	}
	if entry.State.LastError != "" {
		_, _ = fmt.Fprintf(out, "  error: %s\n", entry.State.LastError)
	}
	for _, skill := range entry.State.InstalledSkills {
		_, _ = fmt.Fprintf(out, "  skill: %s -> %s (%d files)\n", skill.Name, skill.TargetPath, len(skill.Files))
	}
}

func promptSkillYesNo(in io.Reader, out io.Writer, question string, def bool) (bool, error) {
	suffix := " [y/N] "
	if def {
		suffix = " [Y/n] "
	}
	_, _ = fmt.Fprint(out, question+suffix)
	line, err := readPromptLine(in)
	if err != nil && err != io.EOF {
		return false, err
	}
	answer := strings.ToLower(strings.TrimSpace(line))
	if answer == "" {
		return def, nil
	}
	switch answer {
	case "y", "yes":
		return true, nil
	case "n", "no":
		return false, nil
	default:
		return false, fmt.Errorf("expected yes or no")
	}
}

type promptLineReader interface {
	ReadString(byte) (string, error)
}

func readPromptLine(in io.Reader) (string, error) {
	if reader, ok := in.(promptLineReader); ok {
		return reader.ReadString('\n')
	}
	return bufio.NewReader(in).ReadString('\n')
}

func runSkillsPush(ctx context.Context, mgr *skills.Manager, name string, direct bool, in io.Reader, out io.Writer) error {
	changes, err := mgr.LocalChanges(ctx, name)
	if err != nil {
		return err
	}
	if len(changes) == 0 {
		_, _ = fmt.Fprintln(out, "No local skill changes to push.")
		return nil
	}
	grouped := groupSkillChangesBySource(changes)
	reader := bufio.NewReader(in)
	for _, sourceID := range sortedChangeSourceIDs(grouped) {
		sourceChanges := grouped[sourceID]
		source := sourceChanges[0].Source
		_, _ = fmt.Fprintf(out, "\nSource %s (%s)\n", source.Name, source.RemoteURL)
		var confirmed []skills.LocalChange
		for _, change := range sourceChanges {
			printSkillChange(out, change)
			ok, err := promptSkillYesNo(reader, out, "Include this change?", false)
			if err != nil {
				return err
			}
			if ok {
				confirmed = append(confirmed, change)
			}
		}
		if len(confirmed) == 0 {
			_, _ = fmt.Fprintf(out, "Skipped %s\n", source.Name)
			continue
		}
		if err := pushConfirmedSkillChanges(ctx, mgr, source, confirmed, direct, reader, out); err != nil {
			return err
		}
	}
	return nil
}

func runSkillsTextMenu(ctx context.Context, root *rootOptions, codexDir string, in io.Reader, out io.Writer) error {
	mgr, err := newSkillsManager(root, codexDir, out)
	if err != nil {
		return err
	}
	reader := bufio.NewReader(in)
	for {
		_, _ = fmt.Fprintln(out, "\nSkills")
		_, _ = fmt.Fprintln(out, "1. List")
		_, _ = fmt.Fprintln(out, "2. Add source")
		_, _ = fmt.Fprintln(out, "3. Sync all")
		_, _ = fmt.Fprintln(out, "4. Remove source")
		_, _ = fmt.Fprintln(out, "5. Push local edits")
		_, _ = fmt.Fprintln(out, "6. Back to TUI")
		_, _ = fmt.Fprint(out, "Choose: ")
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			return err
		}
		switch strings.TrimSpace(line) {
		case "1", "list", "l":
			entries, err := mgr.List(ctx)
			if err != nil {
				return err
			}
			printSkillEntries(out, entries)
		case "2", "add", "a":
			_, _ = fmt.Fprint(out, "GitHub/GitLab/git URL: ")
			urlLine, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				return err
			}
			urlLine = strings.TrimSpace(urlLine)
			if urlLine == "" {
				continue
			}
			source, result, err := mgr.Add(ctx, urlLine, skills.AddOptions{})
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(out, "Installed %d skill(s) from %s\n", len(result.Installed), source.Name)
		case "3", "sync", "s":
			results, err := mgr.Sync(ctx, skills.SyncOptions{All: true})
			for _, result := range results {
				if result.Error != nil {
					_, _ = fmt.Fprintf(out, "%s: %v\n", result.Source.Name, result.Error)
				} else {
					_, _ = fmt.Fprintf(out, "%s: synced %d skill(s)\n", result.Source.Name, len(result.Installed))
				}
			}
			if err != nil {
				return err
			}
		case "4", "remove", "rm", "r":
			_, _ = fmt.Fprint(out, "Source name: ")
			nameLine, err := reader.ReadString('\n')
			if err != nil && err != io.EOF {
				return err
			}
			nameLine = strings.TrimSpace(nameLine)
			if nameLine == "" {
				continue
			}
			ok, err := promptSkillYesNo(reader, out, "Remove this subscription and managed installed skills?", false)
			if err != nil {
				return err
			}
			if !ok {
				continue
			}
			source, err := mgr.Remove(ctx, nameLine)
			if err != nil {
				return err
			}
			_, _ = fmt.Fprintf(out, "Removed %s\n", source.Name)
		case "5", "push", "p":
			if err := runSkillsPush(ctx, mgr, "", false, reader, out); err != nil {
				return err
			}
		case "6", "back", "b", "q", "":
			return nil
		default:
			_, _ = fmt.Fprintln(out, "Unknown choice.")
		}
	}
}

func groupSkillChangesBySource(changes []skills.LocalChange) map[string][]skills.LocalChange {
	grouped := map[string][]skills.LocalChange{}
	for _, change := range changes {
		grouped[change.Source.ID] = append(grouped[change.Source.ID], change)
	}
	for id := range grouped {
		sort.Slice(grouped[id], func(i, j int) bool {
			if grouped[id][i].Skill.ExportName != grouped[id][j].Skill.ExportName {
				return grouped[id][i].Skill.ExportName < grouped[id][j].Skill.ExportName
			}
			return grouped[id][i].RelPath < grouped[id][j].RelPath
		})
	}
	return grouped
}

func sortedChangeSourceIDs(grouped map[string][]skills.LocalChange) []string {
	ids := make([]string, 0, len(grouped))
	for id := range grouped {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return grouped[ids[i]][0].Source.Name < grouped[ids[j]][0].Source.Name
	})
	return ids
}

func printSkillChange(out io.Writer, change skills.LocalChange) {
	_, _ = fmt.Fprintf(out, "\n%s %s\n", strings.ToUpper(string(change.Kind)), change.SourcePath)
	if change.OldSHA256 != "" && (change.NewSHA256 == "" || change.NewSHA256 != change.OldSHA256) {
		_, _ = fmt.Fprintf(out, "  old: %s\n", shortSHA(change.OldSHA256))
	}
	if change.NewSHA256 != "" && change.NewSHA256 != change.OldSHA256 {
		_, _ = fmt.Fprintf(out, "  new: %s", shortSHA(change.NewSHA256))
		if change.Size > 0 {
			_, _ = fmt.Fprintf(out, " (%d bytes)", change.Size)
		}
		_, _ = fmt.Fprintln(out)
	}
	if change.OldMode != change.NewMode {
		if change.OldMode == 0 {
			_, _ = fmt.Fprintf(out, "  mode: %s\n", skillModeString(change.NewMode))
		} else if change.NewMode == 0 {
			_, _ = fmt.Fprintf(out, "  mode: %s -> deleted\n", skillModeString(change.OldMode))
		} else {
			_, _ = fmt.Fprintf(out, "  mode: %s -> %s\n", skillModeString(change.OldMode), skillModeString(change.NewMode))
		}
	}
}

func pushConfirmedSkillChanges(ctx context.Context, mgr *skills.Manager, source skills.Source, changes []skills.LocalChange, direct bool, in io.Reader, out io.Writer) error {
	baseCommit := changes[0].Commit
	if baseCommit == "" {
		return fmt.Errorf("source %s has no base commit in its skill manifest", source.Name)
	}
	for _, change := range changes[1:] {
		if change.Commit != baseCommit {
			return fmt.Errorf("source %s has changes from multiple base commits; sync or push one skill at a time", source.Name)
		}
	}
	tempDir, err := os.MkdirTemp("", "cxp-skills-push-*")
	if err != nil {
		return fmt.Errorf("create push temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()
	repoDir := filepath.Join(tempDir, "repo")
	git := mgr.Git
	if _, err := git.Run(ctx, "", nil, "clone", "--no-checkout", source.RemoteURL, repoDir); err != nil {
		return err
	}
	_, _ = git.Run(ctx, repoDir, nil, "config", "core.hooksPath", "NUL")
	if _, err := git.Run(ctx, repoDir, nil, "checkout", "--detach", baseCommit); err != nil {
		return err
	}
	var stagePaths []string
	for _, change := range changes {
		if err := applySkillChangeToRepo(change, repoDir); err != nil {
			return err
		}
		stagePaths = append(stagePaths, change.SourcePath)
	}
	args := append([]string{"add", "-A", "--"}, stagePaths...)
	if _, err := git.Run(ctx, repoDir, nil, args...); err != nil {
		return err
	}
	if _, err := git.Run(ctx, repoDir, nil, "diff", "--cached", "--quiet"); err == nil {
		_, _ = fmt.Fprintf(out, "No staged changes for %s\n", source.Name)
		return nil
	}
	summary, _ := git.Run(ctx, repoDir, nil, "diff", "--cached", "--stat")
	_, _ = fmt.Fprintf(out, "\nFinal staged diff for %s:\n%s\n", source.Name, string(summary))
	ok, err := promptSkillYesNo(in, out, "Create commit and push these confirmed changes?", false)
	if err != nil {
		return err
	}
	if !ok {
		_, _ = fmt.Fprintln(out, "Push cancelled.")
		return nil
	}
	commitMessage := "Update Codex skills from cxp"
	if _, err := git.Run(ctx, repoDir, nil,
		"-c", "user.name=codex-helper",
		"-c", "user.email=codex-helper@example.invalid",
		"-c", "commit.gpgsign=false",
		"commit", "-m", commitMessage,
	); err != nil {
		return err
	}
	branch := reviewBranchName(source, baseCommit)
	refspec := "HEAD:refs/heads/" + branch
	if direct {
		refspec, err = directPushRefSpec(ctx, git, source)
		if err != nil {
			return err
		}
	}
	_, _ = fmt.Fprintf(out, "Remote: %s\nRef: %s\n", source.RemoteURL, refspec)
	ok, err = promptSkillYesNo(in, out, "Push now?", false)
	if err != nil {
		return err
	}
	if !ok {
		_, _ = fmt.Fprintln(out, "Push cancelled.")
		return nil
	}
	if _, err := git.Run(ctx, repoDir, nil, "push", "origin", refspec); err != nil {
		return err
	}
	_, _ = fmt.Fprintf(out, "Pushed %s\n", refspec)
	return nil
}

func directPushRefSpec(ctx context.Context, git skills.GitRunner, source skills.Source) (string, error) {
	ref := strings.TrimSpace(source.Ref)
	if ref == "" || strings.EqualFold(ref, "HEAD") {
		return "", fmt.Errorf("--direct requires the subscription to use an explicit branch ref")
	}
	if strings.HasPrefix(ref, "refs/tags/") || looksLikeFullSHA(ref) {
		return "", fmt.Errorf("--direct can only push to an existing branch, got %q", ref)
	}
	branch := strings.TrimPrefix(ref, "refs/heads/")
	if strings.HasPrefix(branch, "-") {
		return "", fmt.Errorf("--direct branch ref %q is not safe", ref)
	}
	if _, err := git.Run(ctx, "", nil, "check-ref-format", "--branch", branch); err != nil {
		return "", fmt.Errorf("--direct branch ref %q is not valid: %w", ref, err)
	}
	out, err := git.Run(ctx, "", nil, "ls-remote", "--heads", source.RemoteURL, branch)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(string(out)) == "" {
		return "", fmt.Errorf("--direct branch %q was not found on the remote", branch)
	}
	return "HEAD:refs/heads/" + branch, nil
}

func applySkillChangeToRepo(change skills.LocalChange, repoDir string) error {
	target := filepath.Join(repoDir, filepath.FromSlash(change.SourcePath))
	cleanRepo, err := filepath.Abs(repoDir)
	if err != nil {
		return err
	}
	cleanTarget, err := filepath.Abs(target)
	if err != nil {
		return err
	}
	if cleanTarget != cleanRepo && !strings.HasPrefix(cleanTarget, cleanRepo+string(filepath.Separator)) {
		return fmt.Errorf("change path escapes repository: %s", change.SourcePath)
	}
	switch change.Kind {
	case skills.ChangeDeleted:
		if err := os.Remove(target); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("delete %s: %w", change.SourcePath, err)
		}
	default:
		data, err := os.ReadFile(filepath.Join(change.Skill.TargetPath, filepath.FromSlash(change.RelPath)))
		if err != nil {
			return fmt.Errorf("read local skill file %s: %w", change.RelPath, err)
		}
		sum := sha256.Sum256(data)
		got := hex.EncodeToString(sum[:])
		if change.NewSHA256 != "" && got != change.NewSHA256 {
			return fmt.Errorf("%s changed during review; restart push", change.RelPath)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return fmt.Errorf("create repo dir: %w", err)
		}
		mode := skillChangeFileMode(change)
		if err := os.WriteFile(target, data, mode); err != nil {
			return fmt.Errorf("write repo file %s: %w", change.SourcePath, err)
		}
		_ = os.Chmod(target, mode)
	}
	return nil
}

func skillChangeFileMode(change skills.LocalChange) fs.FileMode {
	if change.NewMode != 0 {
		return fs.FileMode(change.NewMode)
	}
	for _, file := range change.Skill.Files {
		if file.RelPath == change.RelPath && file.Mode != 0 {
			return fs.FileMode(file.Mode)
		}
	}
	return 0o644
}

func skillModeString(mode uint32) string {
	if mode == 0 {
		return "unknown"
	}
	return fmt.Sprintf("%06o", mode)
}

func reviewBranchName(source skills.Source, baseCommit string) string {
	short := shortSHA(baseCommit)
	name := strings.Trim(source.Name, ".-_")
	if name == "" {
		name = "skills"
	}
	return "skill/" + name + "-" + time.Now().Format("20060102-150405") + "-" + short
}

func looksLikeFullSHA(v string) bool {
	if len(v) != 40 {
		return false
	}
	for _, r := range v {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		case r >= 'A' && r <= 'F':
		default:
			return false
		}
	}
	return true
}

func shortSHA(v string) string {
	v = strings.TrimSpace(v)
	if len(v) <= 12 {
		return v
	}
	return v[:12]
}

func skillSourceMatches(source skills.Source, idOrName string) bool {
	q := strings.TrimSpace(idOrName)
	return q != "" && (source.ID == q || strings.EqualFold(source.Name, q))
}

func firstNonEmptyString(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}
