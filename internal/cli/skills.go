package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
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
	cmd.PersistentFlags().StringVar(&codexDir, "codex-dir", "", "Override legacy Codex data dir used for migration (default: ~/.codex)")
	cmd.AddCommand(
		newSkillsInstallBuiltinCmd(root, &codexDir),
		newSkillsAddCmd(root, &codexDir),
		newSkillsMigrateCmd(root, &codexDir),
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
		Short: "Install bundled skills into the user agents skills directory",
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
			runSkillsSelfHeal(ctx, mgr, cmd.ErrOrStderr())
			if !yes {
				target, err := mgr.TargetRoot(skills.TargetAgents)
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
			runSkillsSelfHeal(ctx, mgr, cmd.ErrOrStderr())
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
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Target: %s\n", firstNonEmptyString(target, skills.TargetAgents))
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
	cmd.Flags().StringVar(&target, "target", skills.TargetAgents, "Install target: agents or codex-home")
	_ = cmd.Flags().MarkHidden("target")
	cmd.Flags().BoolVar(&noAutoSync, "no-auto-sync", false, "Disable daily auto-sync for this source")
	cmd.Flags().BoolVar(&yes, "yes", false, "Skip the install confirmation prompt")
	return cmd
}

func newSkillsMigrateCmd(root *rootOptions, codexDir *string) *cobra.Command {
	var yes bool
	var dryRun bool
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "Migrate managed skills from the legacy Codex skills directory to ~/.agents/skills",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, stop := withSignalContext(cmd.Context())
			defer stop()
			mgr, err := newSkillsManager(root, *codexDir, cmd.OutOrStdout())
			if err != nil {
				return err
			}
			if !yes && !dryRun {
				oldRoot, err := mgr.TargetRoot(skills.TargetCodexHome)
				if err != nil {
					return err
				}
				newRoot, err := mgr.TargetRoot(skills.TargetAgents)
				if err != nil {
					return err
				}
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Legacy root: %s\n", oldRoot)
				_, _ = fmt.Fprintf(cmd.OutOrStdout(), "Agents root: %s\n", newRoot)
				ok, err := promptSkillYesNo(cmd.InOrStdin(), cmd.OutOrStdout(), "Migrate managed skills?", true)
				if err != nil {
					return err
				}
				if !ok {
					return nil
				}
			}
			report, err := mgr.MigrateLegacySkills(ctx, skills.MigrationOptions{DryRun: dryRun, IncludeBuiltins: true})
			printSkillMigrationReport(cmd.OutOrStdout(), report)
			return err
		},
	}
	cmd.Flags().BoolVar(&yes, "yes", false, "Skip the migration confirmation prompt")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "Show what would be migrated without changing files")
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
			runSkillsSelfHeal(cmd.Context(), mgr, cmd.ErrOrStderr())
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
			runSkillsSelfHeal(ctx, mgr, cmd.ErrOrStderr())
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
			runSkillsSelfHeal(ctx, mgr, cmd.ErrOrStderr())
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
			runSkillsSelfHeal(cmd.Context(), mgr, cmd.ErrOrStderr())
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
			printSkillMigrationStatus(cmd.OutOrStdout(), mgr)
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
			runSkillsSelfHeal(ctx, mgr, cmd.ErrOrStderr())
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
	runSkillsSelfHeal(ctx, mgr, io.Discard)
	mgr.StartDailyAutoSync(ctx)
}

func runSkillsSelfHeal(ctx context.Context, mgr *skills.Manager, out io.Writer) {
	healCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	report, err := mgr.MigrateLegacySkills(healCtx, skills.MigrationOptions{IncludeBuiltins: true})
	if err != nil {
		_, _ = fmt.Fprintf(out, "Warning: failed to migrate managed skills to ~/.agents/skills: %v\n", err)
		return
	}
	if report.Failed {
		_, _ = fmt.Fprintf(out, "Warning: some managed skills could not be migrated to ~/.agents/skills; run `cxp skills doctor` for details.\n")
		return
	}
	if migrationReportHasUnresolved(report) {
		_, _ = fmt.Fprintf(out, "Warning: some legacy managed skills were not migrated because of local edits or target conflicts; run `cxp skills doctor` for details.\n")
	}
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
		_, _ = fmt.Fprintf(out, "%s  %s  %s  target %s\n", entry.Source.Name, state, auto, firstNonEmptyString(entry.Source.TargetKind, skills.TargetAgents))
		_, _ = fmt.Fprintf(out, "  %s", skills.RedactURLSecrets(entry.Source.RemoteURL))
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

func printSkillMigrationReport(out io.Writer, report skills.MigrationReport) {
	_, _ = fmt.Fprintf(out, "Migration: %s\n", report.Status)
	_, _ = fmt.Fprintf(out, "Legacy root: %s\n", report.OldRoot)
	_, _ = fmt.Fprintf(out, "Agents root: %s\n", report.NewRoot)
	for _, result := range report.Results {
		if result.Status == skills.MigrationStatusSkipped {
			continue
		}
		_, _ = fmt.Fprintf(out, "%s %s: %s", result.Kind, result.Name, result.Status)
		if result.Message != "" {
			_, _ = fmt.Fprintf(out, " (%s)", result.Message)
		}
		_, _ = fmt.Fprintln(out)
		for _, skill := range result.Skills {
			_, _ = fmt.Fprintf(out, "  - %s: %s", skill.ExportName, skill.Status)
			if skill.NewPath != "" {
				_, _ = fmt.Fprintf(out, " -> %s", skill.NewPath)
			}
			if skill.Message != "" {
				_, _ = fmt.Fprintf(out, " (%s)", skill.Message)
			}
			_, _ = fmt.Fprintln(out)
		}
	}
}

func printSkillMigrationStatus(out io.Writer, mgr *skills.Manager) {
	path := mgr.MigrationStatusPath()
	_, _ = fmt.Fprintf(out, "Migration status file: %s\n", path)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			_, _ = fmt.Fprintln(out, "Migration: not run")
		}
		return
	}
	var report skills.MigrationReport
	if err := json.Unmarshal(data, &report); err != nil {
		_, _ = fmt.Fprintf(out, "Migration: unreadable status (%v)\n", err)
		return
	}
	ended := "unknown time"
	if !report.EndedAt.IsZero() {
		ended = report.EndedAt.Format(time.RFC3339)
	}
	_, _ = fmt.Fprintf(out, "Migration: %s at %s (changed=%t failed=%t)\n", firstNonEmptyString(report.Status, skills.MigrationStatusSkipped), ended, report.Changed, report.Failed)
	for _, result := range report.Results {
		if !migrationStatusNeedsAttention(result.Status) {
			continue
		}
		_, _ = fmt.Fprintf(out, "  migration issue: %s %s %s", result.Kind, result.Name, result.Status)
		if result.Message != "" {
			_, _ = fmt.Fprintf(out, " (%s)", result.Message)
		}
		_, _ = fmt.Fprintln(out)
		for _, skill := range result.Skills {
			if !migrationStatusNeedsAttention(skill.Status) {
				continue
			}
			_, _ = fmt.Fprintf(out, "    - %s: %s", skill.ExportName, skill.Status)
			if skill.OldPath != "" {
				_, _ = fmt.Fprintf(out, " old=%s", skill.OldPath)
			}
			if skill.NewPath != "" {
				_, _ = fmt.Fprintf(out, " new=%s", skill.NewPath)
			}
			if skill.Message != "" {
				_, _ = fmt.Fprintf(out, " (%s)", skill.Message)
			}
			_, _ = fmt.Fprintln(out)
		}
	}
}

func migrationReportHasUnresolved(report skills.MigrationReport) bool {
	for _, result := range report.Results {
		if migrationStatusNeedsAttention(result.Status) {
			return true
		}
		for _, skill := range result.Skills {
			if migrationStatusNeedsAttention(skill.Status) {
				return true
			}
		}
	}
	return false
}

func migrationStatusNeedsAttention(status string) bool {
	switch status {
	case skills.MigrationStatusFailed, skills.MigrationStatusLocalModified, skills.MigrationStatusConflict:
		return true
	default:
		return false
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
		_, _ = fmt.Fprintf(out, "\nSource %s (%s)\n", source.Name, skills.RedactURLSecrets(source.RemoteURL))
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
	_, err := skills.PushConfirmedLocalChanges(ctx, mgr, source, changes, skills.PushLocalChangesOptions{
		Direct: direct,
		Out:    out,
		ConfirmCommit: func(skills.Source, string) (bool, error) {
			return promptSkillYesNo(in, out, "Create commit and push these confirmed changes?", false)
		},
		ConfirmPush: func(skills.Source, string) (bool, error) {
			return promptSkillYesNo(in, out, "Push now?", false)
		},
	})
	return err
}

func skillModeString(mode uint32) string {
	if mode == 0 {
		return "unknown"
	}
	return fmt.Sprintf("%06o", mode)
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
