package skillcmd

// `skills push` from a chat (gap batch B, F-CTL-17): v0.1.20's two-step
// review (`helper skills push [--direct] [name]`) and `helper skills push
// [--direct] confirm` (teams/skills_commands.go), with its texts. The
// pending review is kept per chat for 30 minutes in the skill database; confirm pushes only the reviewed
// changes and refuses a review the local edits no longer match.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/skills"
)

const (
	pushReviewTTL = 30 * time.Minute
	pushUsage     = "usage: `helper skills push [--direct] [name]` or `helper skills push [--direct] confirm`"
)

type pushArgs struct {
	Name            string
	Confirm, Direct bool
}

func parsePushArgs(rest string) (pushArgs, error) {
	var out pushArgs
	for _, f := range strings.Fields(strings.TrimSpace(rest)) {
		switch strings.ToLower(f) {
		case "confirm", "--confirm":
			out.Confirm = true
		case "--direct":
			out.Direct = true
		default:
			if strings.HasPrefix(f, "-") || out.Name != "" {
				return pushArgs{}, errors.New(pushUsage)
			}
			out.Name = f
		}
	}
	if out.Confirm && out.Name != "" {
		return pushArgs{}, errors.New(pushUsage)
	}
	return out, nil
}

// pushReview is v0.1.20's teamstore.SkillPushReview.
type pushReview = skills.PushReview
type reviewSource = skills.ReviewSource
type reviewChange = skills.ReviewChange

// groupBySource is v0.1.20's skills.GroupLocalChangesBySource.
func groupBySource(changes []skills.LocalChange) map[string][]skills.LocalChange {
	grouped := map[string][]skills.LocalChange{}
	for _, ch := range changes {
		grouped[ch.Source.ID] = append(grouped[ch.Source.ID], ch)
	}
	for id := range grouped {
		g := grouped[id]
		sort.Slice(g, func(i, j int) bool {
			if g[i].Skill.ExportName != g[j].Skill.ExportName {
				return g[i].Skill.ExportName < g[j].Skill.ExportName
			}
			return g[i].RelPath < g[j].RelPath
		})
	}
	return grouped
}

// sortedSourceIDs is v0.1.20's skills.SortedChangeSourceIDs.
func sortedSourceIDs(grouped map[string][]skills.LocalChange) []string {
	ids := make([]string, 0, len(grouped))
	for id := range grouped {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return grouped[ids[i]][0].Source.Name < grouped[ids[j]][0].Source.Name })
	return ids
}

func buildReview(chat, name string, direct bool, changes []skills.LocalChange, now time.Time) (pushReview, error) {
	r := pushReview{Chat: chat, Name: strings.TrimSpace(name), Direct: direct, CreatedAt: now, ExpiresAt: now.Add(pushReviewTTL)}
	grouped := groupBySource(changes)
	for _, id := range sortedSourceIDs(grouped) {
		sc := grouped[id]
		src := sc[0].Source
		base, err := skills.ValidateLocalChangesForPush(src, sc)
		if err != nil {
			return pushReview{}, err
		}
		rs := reviewSource{SourceID: src.ID, SourceName: src.Name, RemoteURL: src.RemoteURL, Ref: src.Ref, BaseCommit: base,
			ReviewRefSpec: "HEAD:refs/heads/" + skills.ReviewBranchNameAt(src, base, now)}
		for _, ch := range sc {
			rs.Changes = append(rs.Changes, reviewChange{Kind: string(ch.Kind), RelPath: ch.RelPath, SourcePath: ch.SourcePath,
				Commit: ch.Commit, OldSHA256: ch.OldSHA256, NewSHA256: ch.NewSHA256, OldMode: ch.OldMode, NewMode: ch.NewMode, Size: ch.Size})
		}
		r.Sources = append(r.Sources, rs)
	}
	return r, nil
}

// Reviews share the subscription module database. The command's manager
// owns its connection; no review operation loads another chat's changes.
func (c *Command) loadReview(db *skills.Store, chat string) (pushReview, bool, error) {
	return db.Review(chat, time.Now())
}
func (c *Command) clearReview(db *skills.Store, chat string) { _ = db.DeleteReview(chat) }
func (c *Command) removeReviewSource(db *skills.Store, chat, sourceID string) error {
	return db.DeleteReviewSource(chat, sourceID)
}

// push runs `skills push …` for chat.
func (c *Command) push(ctx context.Context, chat string, mgr *skills.Manager, rest string) string {
	args, err := parsePushArgs(rest)
	if err != nil {
		return err.Error()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.Confirm {
		return c.confirmPush(ctx, chat, mgr, args)
	}
	changes, err := mgr.LocalChanges(ctx, args.Name)
	if err != nil {
		return "skills push review failed: " + err.Error()
	}
	if len(changes) == 0 {
		return "No local skill changes to push."
	}
	r, err := buildReview(chat, args.Name, args.Direct, changes, time.Now())
	if err != nil {
		return "skills push review failed: " + err.Error()
	}
	if args.Direct && !reviewHasDirect(r) {
		return "skills push review failed: `--direct` requires every reviewed subscription to use an explicit branch ref."
	}
	if err := mgr.Store.SaveReview(r); err != nil {
		return "skills push review failed: " + err.Error()
	}
	return formatReview(r)
}

func (c *Command) confirmPush(ctx context.Context, chat string, mgr *skills.Manager, args pushArgs) string {
	r, ok, err := c.loadReview(mgr.Store, chat)
	if err != nil {
		return "skills push confirm failed: " + err.Error()
	}
	if !ok {
		return "No pending skills push review. Run `helper skills push [name]` first."
	}
	changes, err := mgr.LocalChanges(ctx, r.Name)
	if err != nil {
		return "skills push confirm failed: " + err.Error()
	}
	reviewed := map[string]bool{}
	for _, s := range r.Sources {
		reviewed[s.SourceID] = true
	}
	var current []skills.LocalChange
	for _, ch := range changes {
		if reviewed[ch.Source.ID] {
			current = append(current, ch)
		}
	}
	if err := reviewStillMatches(r, current); err != nil {
		c.clearReview(mgr.Store, chat)
		return "Pending skills push review is stale: " + err.Error() + "\n\nRun `helper skills push [name]` again."
	}
	grouped := groupBySource(current)
	direct := args.Direct || r.Direct
	if direct && !reviewHasDirect(r) {
		return "skills push failed: `--direct` requires every reviewed subscription to use an explicit branch ref."
	}
	var log strings.Builder
	var pushed []skills.PushLocalChangesResult
	for _, s := range r.Sources {
		sc := grouped[s.SourceID]
		if len(sc) == 0 {
			c.clearReview(mgr.Store, chat)
			return "Pending skills push review is stale: source " + s.SourceName + " no longer has the reviewed changes.\n\nRun `helper skills push [name]` again."
		}
		opts := skills.PushLocalChangesOptions{Direct: direct, Out: &log}
		if !direct {
			opts.RefSpec = s.ReviewRefSpec
		}
		res, err := skills.PushConfirmedLocalChanges(ctx, mgr, sc[0].Source, sc, opts)
		if err != nil {
			body := "skills push failed: " + err.Error()
			if t := strings.TrimSpace(log.String()); t != "" {
				body += "\n\n" + fenced(t)
			}
			return body
		}
		if res.Pushed {
			pushed = append(pushed, res)
		}
		if err := c.removeReviewSource(mgr.Store, chat, s.SourceID); err != nil {
			return "skills push state update failed after push: " + err.Error()
		}
	}
	return formatPushResult(pushed, log.String(), direct)
}

func reviewStillMatches(r pushReview, changes []skills.LocalChange) error {
	cur, err := buildReview(r.Chat, r.Name, r.Direct, changes, r.CreatedAt)
	if err != nil {
		return err
	}
	if len(cur.Sources) != len(r.Sources) {
		return fmt.Errorf("reviewed %d source(s), found %d current source(s)", len(r.Sources), len(cur.Sources))
	}
	for i := range r.Sources {
		want, got := r.Sources[i], cur.Sources[i]
		if want.SourceID != got.SourceID || want.SourceName != got.SourceName || want.RemoteURL != got.RemoteURL ||
			want.Ref != got.Ref || want.BaseCommit != got.BaseCommit || want.ReviewRefSpec != got.ReviewRefSpec {
			return fmt.Errorf("source %s changed since review", want.SourceName)
		}
		if len(want.Changes) != len(got.Changes) {
			return fmt.Errorf("source %s reviewed %d change(s), found %d current change(s)", want.SourceName, len(want.Changes), len(got.Changes))
		}
		for j := range want.Changes {
			if want.Changes[j] != got.Changes[j] {
				return fmt.Errorf("change %s changed since review", want.Changes[j].SourcePath)
			}
		}
	}
	return nil
}

func reviewHasDirect(r pushReview) bool {
	for _, s := range r.Sources {
		if !sourceHasDirect(s) {
			return false
		}
	}
	return len(r.Sources) > 0
}

func sourceHasDirect(s reviewSource) bool {
	ref := strings.TrimSpace(s.Ref)
	return ref != "" && !strings.EqualFold(ref, "HEAD") && !strings.HasPrefix(ref, "refs/tags/") && !fullSHA(ref)
}

func fullSHA(v string) bool {
	if len(v) != 40 {
		return false
	}
	for _, r := range v {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f' || r >= 'A' && r <= 'F') {
			return false
		}
	}
	return true
}

func directTarget(ref string) string {
	b := strings.TrimPrefix(strings.TrimSpace(ref), "refs/heads/")
	if b == "" {
		b = strings.TrimSpace(ref)
	}
	return "HEAD:refs/heads/" + b
}

// formatReview is v0.1.20's formatTeamsSkillPushReview.
func formatReview(r pushReview) string {
	var b strings.Builder
	b.WriteString("## Skills Push Review\n\n")
	if r.Direct {
		b.WriteString("Direct push was requested for this review. ")
	} else {
		b.WriteString("A review-branch push is pending. ")
	}
	b.WriteString("Review the changes below, then send `helper skills push confirm` to commit and push from Teams.")
	if !r.Direct && reviewHasDirect(r) {
		b.WriteString(" To push directly to the subscribed branch instead, send `helper skills push --direct confirm`.")
	}
	if !r.ExpiresAt.IsZero() {
		fmt.Fprintf(&b, "\n\nExpires: `%s`", r.ExpiresAt.Format(time.RFC3339))
	}
	for _, s := range r.Sources {
		fmt.Fprintf(&b, "\n\n- **%s**", s.SourceName)
		if s.RemoteURL != "" {
			fmt.Fprintf(&b, "\n  - remote: `%s`", skills.RedactURLSecrets(s.RemoteURL))
		}
		if s.Ref != "" {
			fmt.Fprintf(&b, "\n  - subscribed ref: `%s`", s.Ref)
		}
		fmt.Fprintf(&b, "\n  - review target: `%s`", s.ReviewRefSpec)
		if sourceHasDirect(s) {
			fmt.Fprintf(&b, "\n  - direct target: `%s`", directTarget(s.Ref))
		}
		for _, ch := range s.Changes {
			fmt.Fprintf(&b, "\n  - **%s** `%s`", strings.ToUpper(ch.Kind), ch.SourcePath)
		}
	}
	return b.String()
}

// formatPushResult is v0.1.20's formatTeamsSkillPushResult.
func formatPushResult(results []skills.PushLocalChangesResult, log string, direct bool) string {
	var b strings.Builder
	b.WriteString("## Skills Push\n\n")
	if direct {
		b.WriteString("Mode: direct\n")
	} else {
		b.WriteString("Mode: review branch\n")
	}
	if len(results) == 0 {
		b.WriteString("\nNo staged changes were pushed.")
	} else {
		for _, r := range results {
			fmt.Fprintf(&b, "\n- **%s**: pushed `%s`", r.Source.Name, r.RefSpec)
		}
	}
	if t := strings.TrimSpace(log); t != "" {
		b.WriteString("\n\n" + fenced(t))
	}
	return b.String()
}

func fenced(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	if len(text) > 6000 {
		text = text[:6000] + "\n... truncated ..."
	}
	return "```text\n" + text + "\n```"
}
