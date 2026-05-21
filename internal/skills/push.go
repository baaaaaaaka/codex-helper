package skills

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type PushLocalChangesOptions struct {
	Direct        bool
	RefSpec       string
	Now           time.Time
	Out           io.Writer
	ConfirmCommit func(source Source, summary string) (bool, error)
	ConfirmPush   func(source Source, refspec string) (bool, error)
}

type PushLocalChangesResult struct {
	Source          Source
	RefSpec         string
	Summary         string
	Pushed          bool
	NoStagedChanges bool
}

func GroupLocalChangesBySource(changes []LocalChange) map[string][]LocalChange {
	grouped := map[string][]LocalChange{}
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

func SortedChangeSourceIDs(grouped map[string][]LocalChange) []string {
	ids := make([]string, 0, len(grouped))
	for id := range grouped {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return grouped[ids[i]][0].Source.Name < grouped[ids[j]][0].Source.Name
	})
	return ids
}

func ValidateLocalChangesForPush(source Source, changes []LocalChange) (string, error) {
	if len(changes) == 0 {
		return "", fmt.Errorf("source %s has no local changes to push", source.Name)
	}
	baseCommit := changes[0].Commit
	if baseCommit == "" {
		return "", fmt.Errorf("source %s has no base commit in its skill manifest", source.Name)
	}
	for _, change := range changes[1:] {
		if change.Commit != baseCommit {
			return "", fmt.Errorf("source %s has changes from multiple base commits; sync or push one skill at a time", source.Name)
		}
	}
	return baseCommit, nil
}

func PushConfirmedLocalChanges(ctx context.Context, mgr *Manager, source Source, changes []LocalChange, opts PushLocalChangesOptions) (PushLocalChangesResult, error) {
	result := PushLocalChangesResult{Source: source}
	if mgr == nil {
		return result, fmt.Errorf("skills manager is required")
	}
	git := mgr.Git
	if git == nil {
		git = ExecGitRunner{Timeout: 2 * time.Minute}
	}
	out := opts.Out
	if out == nil {
		out = io.Discard
	}
	baseCommit, err := ValidateLocalChangesForPush(source, changes)
	if err != nil {
		return result, err
	}
	tempDir, err := os.MkdirTemp("", "cxp-skills-push-*")
	if err != nil {
		return result, fmt.Errorf("create push temp dir: %w", err)
	}
	defer func() { _ = os.RemoveAll(tempDir) }()
	repoDir := filepath.Join(tempDir, "repo")
	if _, err := git.Run(ctx, "", nil, "clone", "--no-checkout", source.RemoteURL, repoDir); err != nil {
		return result, AnnotateGitAuthError(source, err)
	}
	_, _ = git.Run(ctx, repoDir, nil, "config", "core.hooksPath", "NUL")
	if _, err := git.Run(ctx, repoDir, nil, "checkout", "--detach", baseCommit); err != nil {
		return result, err
	}
	var stagePaths []string
	for _, change := range changes {
		if err := applyLocalChangeToRepo(change, repoDir); err != nil {
			return result, err
		}
		stagePaths = append(stagePaths, change.SourcePath)
	}
	args := append([]string{"add", "-A", "--"}, stagePaths...)
	if _, err := git.Run(ctx, repoDir, nil, args...); err != nil {
		return result, err
	}
	if _, err := git.Run(ctx, repoDir, nil, "diff", "--cached", "--quiet"); err == nil {
		_, _ = fmt.Fprintf(out, "No staged changes for %s\n", source.Name)
		result.NoStagedChanges = true
		return result, nil
	}
	summary, _ := git.Run(ctx, repoDir, nil, "diff", "--cached", "--stat")
	result.Summary = string(summary)
	_, _ = fmt.Fprintf(out, "\nFinal staged diff for %s:\n%s\n", source.Name, string(summary))
	if opts.ConfirmCommit != nil {
		ok, err := opts.ConfirmCommit(source, string(summary))
		if err != nil {
			return result, err
		}
		if !ok {
			_, _ = fmt.Fprintln(out, "Push cancelled.")
			return result, nil
		}
	}
	commitMessage := "Update Codex skills from cxp"
	if _, err := git.Run(ctx, repoDir, nil,
		"-c", "user.name=codex-helper",
		"-c", "user.email=codex-helper@example.invalid",
		"-c", "commit.gpgsign=false",
		"commit", "-m", commitMessage,
	); err != nil {
		return result, err
	}
	refspec := strings.TrimSpace(opts.RefSpec)
	if refspec == "" {
		if opts.Direct {
			refspec, err = DirectPushRefSpec(ctx, git, source)
			if err != nil {
				return result, err
			}
		} else {
			branch := ReviewBranchNameAt(source, baseCommit, opts.Now)
			refspec = "HEAD:refs/heads/" + branch
		}
	}
	result.RefSpec = refspec
	_, _ = fmt.Fprintf(out, "Remote: %s\nRef: %s\n", RedactURLSecrets(source.RemoteURL), refspec)
	if opts.ConfirmPush != nil {
		ok, err := opts.ConfirmPush(source, refspec)
		if err != nil {
			return result, err
		}
		if !ok {
			_, _ = fmt.Fprintln(out, "Push cancelled.")
			return result, nil
		}
	}
	if _, err := git.Run(ctx, repoDir, nil, "push", "origin", refspec); err != nil {
		return result, AnnotateGitAuthError(source, err)
	}
	result.Pushed = true
	_, _ = fmt.Fprintf(out, "Pushed %s\n", refspec)
	return result, nil
}

func DirectPushRefSpec(ctx context.Context, git GitRunner, source Source) (string, error) {
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
	if git == nil {
		git = ExecGitRunner{Timeout: 2 * time.Minute}
	}
	if _, err := git.Run(ctx, "", nil, "check-ref-format", "--branch", branch); err != nil {
		return "", fmt.Errorf("--direct branch ref %q is not valid: %w", ref, err)
	}
	out, err := git.Run(ctx, "", nil, "ls-remote", "--heads", source.RemoteURL, branch)
	if err != nil {
		return "", AnnotateGitAuthError(source, err)
	}
	if strings.TrimSpace(string(out)) == "" {
		return "", fmt.Errorf("--direct branch %q was not found on the remote", branch)
	}
	return "HEAD:refs/heads/" + branch, nil
}

func ReviewBranchName(source Source, baseCommit string) string {
	return ReviewBranchNameAt(source, baseCommit, time.Now())
}

func ReviewBranchNameAt(source Source, baseCommit string, now time.Time) string {
	short := ShortSHA(baseCommit)
	name := strings.Trim(source.Name, ".-_")
	if name == "" {
		name = "skills"
	}
	if now.IsZero() {
		now = time.Now()
	}
	return "skill/" + name + "-" + now.Format("20060102-150405") + "-" + short
}

func ShortSHA(v string) string {
	v = strings.TrimSpace(v)
	if len(v) <= 12 {
		return v
	}
	return v[:12]
}

func SkillModeString(mode uint32) string {
	if mode == 0 {
		return "unknown"
	}
	return fmt.Sprintf("%06o", mode)
}

func applyLocalChangeToRepo(change LocalChange, repoDir string) error {
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
	case ChangeDeleted:
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
		mode := localChangeFileMode(change)
		if err := os.WriteFile(target, data, mode); err != nil {
			return fmt.Errorf("write repo file %s: %w", change.SourcePath, err)
		}
		_ = os.Chmod(target, mode)
	}
	return nil
}

func localChangeFileMode(change LocalChange) fs.FileMode {
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
