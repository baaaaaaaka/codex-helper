package teams

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/skills"
	teamstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
	xhtml "golang.org/x/net/html"
)

var newTeamsSkillsManagerForCommand = newTeamsSkillsManager

const teamsSkillPushReviewTTL = 30 * time.Minute

func (b *Bridge) handleSkillsCommand(ctx context.Context, chatID string, arg string) error {
	return b.handleSkillsCommandFromMessage(ctx, chatID, ChatMessage{}, arg)
}

func (b *Bridge) handleSkillsCommandFromMessage(ctx context.Context, chatID string, msg ChatMessage, arg string) error {
	mgr, err := newTeamsSkillsManagerForCommand()
	if err != nil {
		return b.sendToChat(ctx, chatID, "skills setup failed: "+err.Error())
	}
	action, rest := splitTeamsSkillsCommand(arg)
	if action == "" {
		action = "list"
	}
	switch action {
	case "", "list", "status", "st":
		entries, err := mgr.List(ctx)
		if err != nil {
			return b.sendToChat(ctx, chatID, "skills list failed: "+err.Error())
		}
		return b.sendToChat(ctx, chatID, formatTeamsSkillEntries(entries))
	case "add":
		rawURL := teamsSkillAddURLFromTeamsMessage(msg, rest)
		if rawURL == "" {
			return b.sendToChat(ctx, chatID, "usage: `helper skills add <github/gitlab/git-url>`")
		}
		source, result, err := mgr.Add(ctx, rawURL, skills.AddOptions{})
		return b.sendToChat(ctx, chatID, formatTeamsSkillAddResult(source, result, err))
	case "sync":
		name := firstTeamsSkillArg(rest)
		results, err := mgr.Sync(ctx, skills.SyncOptions{Name: name, All: name == ""})
		body := formatTeamsSkillSyncResults(results)
		if err != nil {
			body += "\n\nSync failed: " + err.Error()
		}
		return b.sendToChat(ctx, chatID, body)
	case "push":
		args, err := parseTeamsSkillPushArgs(rest)
		if err != nil {
			return b.sendToChat(ctx, chatID, err.Error())
		}
		if args.Confirm {
			return b.confirmTeamsSkillPush(ctx, chatID, mgr, args)
		}
		return b.reviewTeamsSkillPush(ctx, chatID, mgr, args)
	case "remove", "rm":
		return b.sendToChat(ctx, chatID, "Use local `cxp skills "+action+"` for this operation so auth prompts and destructive confirmations happen in your terminal.")
	default:
		return b.sendToChat(ctx, chatID, "usage: `helper skills list`, `helper skills add <github/gitlab/git-url>`, `helper skills sync [name]`, `helper skills push [--direct] [name]`, or `helper skills push [--direct] confirm`")
	}
}

func splitTeamsSkillsCommand(arg string) (string, string) {
	arg = strings.TrimSpace(arg)
	if arg == "" {
		return "", ""
	}
	action, rest := splitDashboardCommandBody(arg)
	return strings.ToLower(strings.TrimSpace(action)), strings.TrimSpace(rest)
}

func firstTeamsSkillArg(rest string) string {
	fields := strings.Fields(strings.TrimSpace(rest))
	if len(fields) == 0 {
		return ""
	}
	return fields[0]
}

type teamsSkillPushArgs struct {
	Name    string
	Confirm bool
	Direct  bool
}

func parseTeamsSkillPushArgs(rest string) (teamsSkillPushArgs, error) {
	var out teamsSkillPushArgs
	for _, field := range strings.Fields(strings.TrimSpace(rest)) {
		switch strings.ToLower(strings.TrimSpace(field)) {
		case "confirm", "--confirm":
			out.Confirm = true
		case "--direct":
			out.Direct = true
		default:
			if strings.HasPrefix(field, "-") {
				return teamsSkillPushArgs{}, fmt.Errorf("usage: `helper skills push [--direct] [name]` or `helper skills push [--direct] confirm`")
			}
			if out.Name != "" {
				return teamsSkillPushArgs{}, fmt.Errorf("usage: `helper skills push [--direct] [name]` or `helper skills push [--direct] confirm`")
			}
			out.Name = field
		}
	}
	if out.Confirm && out.Name != "" {
		return teamsSkillPushArgs{}, fmt.Errorf("usage: `helper skills push [--direct] [name]` or `helper skills push [--direct] confirm`")
	}
	return out, nil
}

func (b *Bridge) reviewTeamsSkillPush(ctx context.Context, chatID string, mgr *skills.Manager, args teamsSkillPushArgs) error {
	changes, err := mgr.LocalChanges(ctx, args.Name)
	if err != nil {
		return b.sendToChat(ctx, chatID, "skills push review failed: "+err.Error())
	}
	if len(changes) == 0 {
		return b.sendToChat(ctx, chatID, "No local skill changes to push.")
	}
	review, err := buildTeamsSkillPushReview(chatID, args.Name, args.Direct, changes, time.Now())
	if err != nil {
		return b.sendToChat(ctx, chatID, "skills push review failed: "+err.Error())
	}
	if args.Direct && !teamsSkillPushReviewHasDirectCandidate(review) {
		return b.sendToChat(ctx, chatID, "skills push review failed: `--direct` requires every reviewed subscription to use an explicit branch ref.")
	}
	if err := b.saveTeamsSkillPushReview(ctx, review); err != nil {
		return b.sendToChat(ctx, chatID, "skills push review failed: "+err.Error())
	}
	return b.sendToChat(ctx, chatID, formatTeamsSkillPushReview(review))
}

func (b *Bridge) confirmTeamsSkillPush(ctx context.Context, chatID string, mgr *skills.Manager, args teamsSkillPushArgs) error {
	review, ok, err := b.loadTeamsSkillPushReview(ctx, chatID)
	if err != nil {
		return b.sendToChat(ctx, chatID, "skills push confirm failed: "+err.Error())
	}
	if !ok {
		return b.sendToChat(ctx, chatID, "No pending skills push review. Run `helper skills push [name]` first.")
	}
	if args.Name != "" && review.Name != "" && !strings.EqualFold(args.Name, review.Name) {
		return b.sendToChat(ctx, chatID, "Pending skills push review is for `"+review.Name+"`; run `helper skills push "+args.Name+"` to create a new review.")
	}
	changes, err := mgr.LocalChanges(ctx, review.Name)
	if err != nil {
		return b.sendToChat(ctx, chatID, "skills push confirm failed: "+err.Error())
	}
	reviewChanges := filterTeamsSkillPushReviewChanges(review, changes)
	if err := ensureTeamsSkillPushReviewStillMatches(review, reviewChanges); err != nil {
		_ = b.clearTeamsSkillPushReview(context.Background(), chatID)
		return b.sendToChat(ctx, chatID, "Pending skills push review is stale: "+err.Error()+"\n\nRun `helper skills push [name]` again.")
	}
	grouped := skills.GroupLocalChangesBySource(reviewChanges)
	direct := args.Direct || review.Direct
	if direct && !teamsSkillPushReviewHasDirectCandidate(review) {
		return b.sendToChat(ctx, chatID, "skills push failed: `--direct` requires every reviewed subscription to use an explicit branch ref.")
	}
	var log strings.Builder
	var pushed []skills.PushLocalChangesResult
	for _, sourceReview := range review.Sources {
		sourceChanges := grouped[sourceReview.SourceID]
		if len(sourceChanges) == 0 {
			_ = b.clearTeamsSkillPushReview(context.Background(), chatID)
			return b.sendToChat(ctx, chatID, "Pending skills push review is stale: source "+sourceReview.SourceName+" no longer has the reviewed changes.\n\nRun `helper skills push [name]` again.")
		}
		opts := skills.PushLocalChangesOptions{Direct: direct, Out: &log}
		if !direct {
			opts.RefSpec = sourceReview.ReviewRefSpec
		}
		result, err := skills.PushConfirmedLocalChanges(ctx, mgr, sourceChanges[0].Source, sourceChanges, opts)
		if err != nil {
			body := "skills push failed: " + err.Error()
			if text := strings.TrimSpace(log.String()); text != "" {
				body += "\n\n" + fencedTextBlock(text)
			}
			return b.sendToChat(ctx, chatID, body)
		}
		if result.Pushed {
			pushed = append(pushed, result)
		}
		if err := b.removeTeamsSkillPushReviewSource(ctx, chatID, sourceReview.SourceID); err != nil {
			return b.sendToChat(ctx, chatID, "skills push state update failed after push: "+err.Error())
		}
	}
	return b.sendToChat(ctx, chatID, formatTeamsSkillPushResult(pushed, log.String(), direct))
}

func cleanTeamsSkillURL(raw string) string {
	raw = strings.TrimSpace(raw)
	raw = strings.Trim(raw, "<>")
	return strings.TrimSpace(raw)
}

type teamsHTMLAnchor struct {
	Href string
	Text string
}

func teamsSkillAddURLFromTeamsMessage(msg ChatMessage, raw string) string {
	cleaned := cleanTeamsSkillURL(raw)
	if cleaned == "" {
		return ""
	}
	anchors := safeTeamsSkillAnchorsFromTeamsHTML(msg.Body.Content)
	if len(anchors) == 0 {
		return cleaned
	}
	normalizedRaw := normalizeTeamsSkillAnchorText(cleaned)
	explicitRaw := looksLikeExplicitTeamsSkillURL(cleaned)
	for _, anchor := range anchors {
		text := normalizeTeamsSkillAnchorText(cleanTeamsSkillURL(anchor.Text))
		if text == "" {
			continue
		}
		if normalizedRaw == text || (!explicitRaw && strings.Contains(normalizedRaw, text)) {
			return anchor.Href
		}
	}
	if len(anchors) == 1 && !explicitRaw {
		return anchors[0].Href
	}
	return cleaned
}

func safeTeamsSkillAnchorsFromTeamsHTML(content string) []teamsHTMLAnchor {
	content = commandRouteHTMLForTeamsSkillAnchors(content)
	if content == "" {
		return nil
	}
	root, err := xhtml.Parse(strings.NewReader("<html><body>" + content + "</body></html>"))
	if err != nil {
		return nil
	}
	var out []teamsHTMLAnchor
	var walk func(*xhtml.Node)
	walk = func(n *xhtml.Node) {
		if n == nil {
			return
		}
		if n.Type == xhtml.ElementNode && strings.EqualFold(n.Data, "a") {
			href, ok := normalizeSafeTeamsSkillHref(teamsHTMLNodeAttr(n, "href"))
			if ok {
				out = append(out, teamsHTMLAnchor{Href: href, Text: teamsHTMLNodeText(n)})
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	return out
}

func commandRouteHTMLForTeamsSkillAnchors(content string) string {
	content = strings.ReplaceAll(content, "\r\n", "\n")
	for {
		next := commandRouteQuoteHTML.ReplaceAllString(content, "")
		if next == content {
			break
		}
		content = next
	}
	content = commandRouteAttachmentHTML.ReplaceAllString(content, "")
	return strings.TrimSpace(content)
}

func teamsHTMLNodeAttr(n *xhtml.Node, key string) string {
	for _, attr := range n.Attr {
		if strings.EqualFold(attr.Key, key) {
			return strings.TrimSpace(attr.Val)
		}
	}
	return ""
}

func teamsHTMLNodeText(n *xhtml.Node) string {
	var b strings.Builder
	var walk func(*xhtml.Node)
	walk = func(node *xhtml.Node) {
		if node == nil {
			return
		}
		if node.Type == xhtml.TextNode {
			b.WriteString(node.Data)
			return
		}
		if node.Type == xhtml.ElementNode && strings.EqualFold(node.Data, "br") {
			b.WriteByte(' ')
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(n)
	return strings.TrimSpace(strings.Join(strings.Fields(b.String()), " "))
}

func normalizeSafeTeamsSkillHref(raw string) (string, bool) {
	raw = strings.TrimSpace(raw)
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" {
		return "", false
	}
	if isTeamsSafeLinkHost(parsed.Hostname()) {
		raw = strings.TrimSpace(parsed.Query().Get("url"))
		if raw == "" {
			return "", false
		}
		parsed, err = url.Parse(raw)
		if err != nil || parsed.Scheme == "" {
			return "", false
		}
	}
	switch strings.ToLower(parsed.Scheme) {
	case "http", "https", "ssh":
		if strings.TrimSpace(parsed.Hostname()) == "" {
			return "", false
		}
		return raw, true
	default:
		return "", false
	}
}

func isTeamsSafeLinkHost(host string) bool {
	host = strings.TrimSuffix(strings.ToLower(strings.TrimSpace(host)), ".")
	return host == "safelinks.protection.outlook.com" || strings.HasSuffix(host, ".safelinks.protection.outlook.com")
}

func normalizeTeamsSkillAnchorText(text string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(text)), " ")
}

func looksLikeExplicitTeamsSkillURL(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	if strings.Contains(raw, "://") {
		return true
	}
	at := strings.Index(raw, "@")
	colon := strings.Index(raw, ":")
	return at > 0 && colon > at+1
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

func formatTeamsSkillAddResult(source skills.Source, result skills.SyncResult, err error) string {
	if source.Name == "" {
		if err != nil {
			return "skills add failed: " + err.Error()
		}
		return "skills add failed."
	}
	var b strings.Builder
	b.WriteString("## Skills Add\n")
	_, _ = fmt.Fprintf(&b, "\n- **%s**", source.Name)
	if source.RemoteURL != "" {
		_, _ = fmt.Fprintf(&b, "\n  - remote: `%s`", redactTeamsSkillURL(source.RemoteURL))
	}
	if source.Ref != "" {
		_, _ = fmt.Fprintf(&b, "\n  - ref: `%s`", source.Ref)
	}
	if source.Path != "" {
		_, _ = fmt.Fprintf(&b, "\n  - path: `%s`", source.Path)
	}
	if source.AutoSync {
		b.WriteString("\n  - auto-sync: on")
	} else {
		b.WriteString("\n  - auto-sync: off")
	}
	if err != nil {
		status := result.State.Status
		if status == "" {
			status = skills.StatusSyncFailed
		}
		_, _ = fmt.Fprintf(&b, "\n  - status: `%s`", status)
		_, _ = fmt.Fprintf(&b, "\n\nInitial sync failed: %s", err.Error())
		return b.String()
	}
	_, _ = fmt.Fprintf(&b, "\n\nInstalled %d skill(s).", len(result.Installed))
	for _, skill := range result.Installed {
		_, _ = fmt.Fprintf(&b, "\n- `%s` -> `%s`", skill.Name, skill.ExportName)
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

func buildTeamsSkillPushReview(chatID string, name string, direct bool, changes []skills.LocalChange, now time.Time) (teamstore.SkillPushReview, error) {
	if now.IsZero() {
		now = time.Now()
	}
	review := teamstore.SkillPushReview{
		ID:          teamsSkillPushReviewKey(chatID),
		TeamsChatID: strings.TrimSpace(chatID),
		Name:        strings.TrimSpace(name),
		Direct:      direct,
		CreatedAt:   now,
		ExpiresAt:   now.Add(teamsSkillPushReviewTTL),
	}
	grouped := skills.GroupLocalChangesBySource(changes)
	for _, sourceID := range skills.SortedChangeSourceIDs(grouped) {
		sourceChanges := grouped[sourceID]
		source := sourceChanges[0].Source
		baseCommit, err := skills.ValidateLocalChangesForPush(source, sourceChanges)
		if err != nil {
			return teamstore.SkillPushReview{}, err
		}
		sourceReview := teamstore.SkillPushReviewSource{
			SourceID:      source.ID,
			SourceName:    source.Name,
			RemoteURL:     source.RemoteURL,
			Ref:           source.Ref,
			BaseCommit:    baseCommit,
			ReviewRefSpec: "HEAD:refs/heads/" + skills.ReviewBranchNameAt(source, baseCommit, now),
		}
		for _, change := range sourceChanges {
			sourceReview.Changes = append(sourceReview.Changes, teamstore.SkillPushReviewChange{
				Kind:       string(change.Kind),
				RelPath:    change.RelPath,
				SourcePath: change.SourcePath,
				Commit:     change.Commit,
				OldSHA256:  change.OldSHA256,
				NewSHA256:  change.NewSHA256,
				OldMode:    change.OldMode,
				NewMode:    change.NewMode,
				Size:       change.Size,
			})
		}
		review.Sources = append(review.Sources, sourceReview)
	}
	return review, nil
}

func (b *Bridge) saveTeamsSkillPushReview(ctx context.Context, review teamstore.SkillPushReview) error {
	if b.store == nil {
		return fmt.Errorf("teams state store is not configured")
	}
	return b.store.Update(ctx, func(state *teamstore.State) error {
		if state.SkillPushReviews == nil {
			state.SkillPushReviews = make(map[string]teamstore.SkillPushReview)
		}
		state.SkillPushReviews[teamsSkillPushReviewKey(review.TeamsChatID)] = review
		return nil
	})
}

func (b *Bridge) loadTeamsSkillPushReview(ctx context.Context, chatID string) (teamstore.SkillPushReview, bool, error) {
	if b.store == nil {
		return teamstore.SkillPushReview{}, false, fmt.Errorf("teams state store is not configured")
	}
	state, err := b.store.Load(ctx)
	if err != nil {
		return teamstore.SkillPushReview{}, false, err
	}
	review, ok := state.SkillPushReviews[teamsSkillPushReviewKey(chatID)]
	if !ok {
		return teamstore.SkillPushReview{}, false, nil
	}
	if !review.ExpiresAt.IsZero() && time.Now().After(review.ExpiresAt) {
		_ = b.clearTeamsSkillPushReview(context.Background(), chatID)
		return teamstore.SkillPushReview{}, false, nil
	}
	return review, true, nil
}

func (b *Bridge) clearTeamsSkillPushReview(ctx context.Context, chatID string) error {
	if b.store == nil {
		return fmt.Errorf("teams state store is not configured")
	}
	return b.store.Update(ctx, func(state *teamstore.State) error {
		delete(state.SkillPushReviews, teamsSkillPushReviewKey(chatID))
		return nil
	})
}

func (b *Bridge) removeTeamsSkillPushReviewSource(ctx context.Context, chatID string, sourceID string) error {
	if b.store == nil {
		return fmt.Errorf("teams state store is not configured")
	}
	return b.store.Update(ctx, func(state *teamstore.State) error {
		key := teamsSkillPushReviewKey(chatID)
		review, ok := state.SkillPushReviews[key]
		if !ok {
			return nil
		}
		next := review.Sources[:0]
		for _, source := range review.Sources {
			if source.SourceID != sourceID {
				next = append(next, source)
			}
		}
		if len(next) == 0 {
			delete(state.SkillPushReviews, key)
			return nil
		}
		review.Sources = append([]teamstore.SkillPushReviewSource(nil), next...)
		state.SkillPushReviews[key] = review
		return nil
	})
}

func ensureTeamsSkillPushReviewStillMatches(review teamstore.SkillPushReview, changes []skills.LocalChange) error {
	current, err := buildTeamsSkillPushReview(review.TeamsChatID, review.Name, review.Direct, changes, review.CreatedAt)
	if err != nil {
		return err
	}
	if len(current.Sources) != len(review.Sources) {
		return fmt.Errorf("reviewed %d source(s), found %d current source(s)", len(review.Sources), len(current.Sources))
	}
	for i := range review.Sources {
		if err := compareTeamsSkillPushReviewSource(review.Sources[i], current.Sources[i]); err != nil {
			return err
		}
	}
	return nil
}

func compareTeamsSkillPushReviewSource(want, got teamstore.SkillPushReviewSource) error {
	if want.SourceID != got.SourceID || want.SourceName != got.SourceName || want.RemoteURL != got.RemoteURL || want.Ref != got.Ref || want.BaseCommit != got.BaseCommit || want.ReviewRefSpec != got.ReviewRefSpec {
		return fmt.Errorf("source %s changed since review", want.SourceName)
	}
	if len(want.Changes) != len(got.Changes) {
		return fmt.Errorf("source %s reviewed %d change(s), found %d current change(s)", want.SourceName, len(want.Changes), len(got.Changes))
	}
	for i := range want.Changes {
		if want.Changes[i] != got.Changes[i] {
			return fmt.Errorf("change %s changed since review", want.Changes[i].SourcePath)
		}
	}
	return nil
}

func teamsSkillPushReviewKey(chatID string) string {
	return strings.TrimSpace(chatID)
}

func filterTeamsSkillPushReviewChanges(review teamstore.SkillPushReview, changes []skills.LocalChange) []skills.LocalChange {
	if len(review.Sources) == 0 || len(changes) == 0 {
		return nil
	}
	reviewed := make(map[string]struct{}, len(review.Sources))
	for _, source := range review.Sources {
		reviewed[source.SourceID] = struct{}{}
	}
	filtered := make([]skills.LocalChange, 0, len(changes))
	for _, change := range changes {
		if _, ok := reviewed[change.Source.ID]; ok {
			filtered = append(filtered, change)
		}
	}
	return filtered
}

func formatTeamsSkillPushReview(review teamstore.SkillPushReview) string {
	var b strings.Builder
	b.WriteString("## Skills Push Review\n\n")
	if review.Direct {
		b.WriteString("Direct push was requested for this review. ")
	} else {
		b.WriteString("A review-branch push is pending. ")
	}
	b.WriteString("Review the changes below, then send `helper skills push confirm` to commit and push from Teams.")
	if !review.Direct && teamsSkillPushReviewHasDirectCandidate(review) {
		b.WriteString(" To push directly to the subscribed branch instead, send `helper skills push --direct confirm`.")
	}
	if !review.ExpiresAt.IsZero() {
		_, _ = fmt.Fprintf(&b, "\n\nExpires: `%s`", review.ExpiresAt.Format(time.RFC3339))
	}
	for _, source := range review.Sources {
		_, _ = fmt.Fprintf(&b, "\n\n- **%s**", source.SourceName)
		if source.RemoteURL != "" {
			_, _ = fmt.Fprintf(&b, "\n  - remote: `%s`", redactTeamsSkillURL(source.RemoteURL))
		}
		if source.Ref != "" {
			_, _ = fmt.Fprintf(&b, "\n  - subscribed ref: `%s`", source.Ref)
		}
		_, _ = fmt.Fprintf(&b, "\n  - review target: `%s`", source.ReviewRefSpec)
		if teamsSkillPushSourceHasDirectCandidate(source) {
			_, _ = fmt.Fprintf(&b, "\n  - direct target: `%s`", teamsSkillPushDirectTargetLabel(source.Ref))
		}
		for _, change := range source.Changes {
			_, _ = fmt.Fprintf(&b, "\n  - **%s** `%s`", strings.ToUpper(change.Kind), change.SourcePath)
		}
	}
	return b.String()
}

func teamsSkillPushReviewHasDirectCandidate(review teamstore.SkillPushReview) bool {
	for _, source := range review.Sources {
		if !teamsSkillPushSourceHasDirectCandidate(source) {
			return false
		}
	}
	return len(review.Sources) > 0
}

func teamsSkillPushSourceHasDirectCandidate(source teamstore.SkillPushReviewSource) bool {
	ref := strings.TrimSpace(source.Ref)
	return ref != "" && !strings.EqualFold(ref, "HEAD") && !strings.HasPrefix(ref, "refs/tags/") && !teamsSkillPushLooksLikeFullSHA(ref)
}

func teamsSkillPushDirectTargetLabel(ref string) string {
	branch := strings.TrimPrefix(strings.TrimSpace(ref), "refs/heads/")
	if branch == "" {
		branch = strings.TrimSpace(ref)
	}
	return "HEAD:refs/heads/" + branch
}

func teamsSkillPushLooksLikeFullSHA(v string) bool {
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

func formatTeamsSkillPushResult(results []skills.PushLocalChangesResult, log string, direct bool) string {
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
		for _, result := range results {
			_, _ = fmt.Fprintf(&b, "\n- **%s**: pushed `%s`", result.Source.Name, result.RefSpec)
		}
	}
	if text := strings.TrimSpace(log); text != "" {
		b.WriteString("\n\n")
		b.WriteString(fencedTextBlock(text))
	}
	return b.String()
}

func fencedTextBlock(text string) string {
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}
	if len(text) > 6000 {
		text = text[:6000] + "\n... truncated ..."
	}
	return "```text\n" + text + "\n```"
}

func shortTeamsSHA(v string) string {
	v = strings.TrimSpace(v)
	if len(v) <= 12 {
		return v
	}
	return v[:12]
}

func redactTeamsSkillURL(raw string) string {
	return skills.RedactURLSecrets(raw)
}
