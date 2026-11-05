package harness

// Transcript goldens.
//
// At teardown every scenario renders everything the SUT made externally
// visible — every chat (title, members, each message in order with its
// sender, exact HTML, mentions, hosted-content hashes, attachments, edits),
// every Workflow card, every file it uploaded, every turn it started in
// Codex, and every cxp command the scenario ran (args, stdout, stderr,
// exit code) — normalizes the volatile values (normalize.go) and compares
// the text with testdata/<scenario>.golden of the scenario package.
//
//   - E2E_UPDATE_GOLDEN=1 rewrites the golden in the source tree.
//   - E2E_GOLDEN_OUT=<dir> writes every actual transcript to
//     <dir>/<package path>/testdata/<scenario>.golden instead (the Docker
//     runner mounts the source read-only); with E2E_UPDATE_GOLDEN=1 there
//     the run passes and the files are copied back by hand.
//
// A mismatch fails the scenario with a unified diff.

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/e2e/fakegraph"
)

// Golden environment switches.
const (
	EnvUpdateGolden = "E2E_UPDATE_GOLDEN"
	EnvGoldenOut    = "E2E_GOLDEN_OUT"
)

// GoldenOptions declares the parts of a scenario's transcript that are
// genuinely nondeterministic. Every entry needs a reason.
type GoldenOptions struct {
	// Skip leaves the scenario without a golden (reason required).
	Skip string
	// UnorderedChats renders the messages of the chats whose title contains
	// the key in a canonical order (by sender, then text) instead of the
	// posting order, because the scenario does not fix that order. Value:
	// the reason.
	UnorderedChats map[string]string
	// Masks replace narrow volatile text with a fixed placeholder.
	Masks []Mask
	// OmitCodex leaves the Codex section out (reason).
	OmitCodex string
}

// Mask replaces every match of Pattern in the transcript with Replace.
type Mask struct {
	Pattern *regexp.Regexp
	Replace string
	Reason  string
}

func (o GoldenOptions) validate() error {
	for k, r := range o.UnorderedChats {
		if strings.TrimSpace(r) == "" {
			return fmt.Errorf("golden: UnorderedChats[%q] needs a reason", k)
		}
	}
	for _, m := range o.Masks {
		if m.Pattern == nil || strings.TrimSpace(m.Reason) == "" {
			return fmt.Errorf("golden: a mask needs a pattern and a reason")
		}
	}
	return nil
}

// GoldenTranscript renders the normalized external transcript of the scenario.
func (s *Scenario) GoldenTranscript() string {
	n := s.newNormalizer()
	var b strings.Builder
	fmt.Fprintf(&b, "# %s: %s\n", s.ID, s.Opts.Title)
	b.WriteString("# cxp black-box transcript; volatile values are numbered placeholders\n")

	owner, coworker := s.Graph.Owner(), s.Graph.Coworker()
	who := func(id string) string {
		switch id {
		case owner.ID:
			return "owner"
		case coworker.ID:
			return "coworker"
		}
		return n.norm(id)
	}

	chats := s.Graph.Chats()
	// Sort chats by their normalized title (a scratch normalizer, so the
	// placeholders of the real one follow output order), then creation.
	keys := make([]string, len(chats))
	for i, c := range chats {
		keys[i] = s.newNormalizer().norm(c.Topic)
	}
	idx := make([]int, len(chats))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool { return keys[idx[a]] < keys[idx[b]] })
	for _, i := range idx {
		c := chats[i]
		n.chatID(c.ID)
	}
	for _, i := range idx {
		c := chats[i]
		var members []string
		for _, m := range c.Members {
			members = append(members, who(m))
		}
		fmt.Fprintf(&b, "\n== chat %s %q\n", n.chatID(c.ID), n.norm(c.Topic))
		fmt.Fprintf(&b, "   type: %s; members: %s\n", c.ChatType, strings.Join(members, ", "))
		if len(c.TopicLog) > 1 {
			for _, t := range c.TopicLog {
				fmt.Fprintf(&b, "   title was: %q\n", n.norm(t))
			}
		}
		msgs := s.Graph.Messages(c.ID)
		for _, m := range msgs {
			n.msgID(m.ID)
		}
		if reason, ok := s.unordered(c.Topic); ok {
			fmt.Fprintf(&b, "   (messages in canonical order, not posting order: %s)\n", reason)
			sort.SliceStable(msgs, func(a, b int) bool {
				ka := string(msgs[a].Origin) + "\x00" + n.scratch(msgs[a].Content)
				kb := string(msgs[b].Origin) + "\x00" + n.scratch(msgs[b].Content)
				return ka < kb
			})
		}
		for _, m := range msgs {
			s.renderMessage(&b, n, m, who)
		}
	}

	if cards := s.Graph.WorkflowCards(); len(cards) > 0 {
		b.WriteString("\n== workflow cards\n")
		for i, c := range cards {
			fmt.Fprintf(&b, "-- card %d: status %d%s\n", i+1, c.Status, faultSuffix(c.Fault))
			for _, t := range c.Texts {
				writeBlock(&b, "   text: ", n.norm(t))
			}
			for _, a := range c.Actions {
				fmt.Fprintf(&b, "   action: %s\n", n.norm(canonicalJSON(a)))
			}
		}
	}

	if files := s.Graph.DriveItems(); len(files) > 0 {
		b.WriteString("\n== uploaded files\n")
		sort.SliceStable(files, func(a, b int) bool { return n.scratch(files[a].Path) < n.scratch(files[b].Path) })
		for _, f := range files {
			fmt.Fprintf(&b, "-- %s\n   %s, %d bytes, writes: %s\n", n.norm(f.Path), fakegraph.HashBytes(f.Data), len(f.Data), strings.Join(f.Writes, ","))
		}
	}

	if s.Opts.Golden.OmitCodex == "" {
		s.renderCodex(&b, n)
	} else {
		fmt.Fprintf(&b, "\n== codex: omitted (%s)\n", s.Opts.Golden.OmitCodex)
	}

	s.mu.Lock()
	runs := append([]CLIRun(nil), s.cli...)
	s.mu.Unlock()
	if len(runs) > 0 {
		b.WriteString("\n== cli\n")
		for _, r := range runs {
			fmt.Fprintf(&b, "$ cxp %s\n", n.norm(strings.Join(r.Args, " ")))
			fmt.Fprintf(&b, "   exit %d\n", r.Code)
			writeBlock(&b, "   stdout| ", n.norm(r.Stdout))
			writeBlock(&b, "   stderr| ", n.norm(r.Stderr))
		}
	}
	out := b.String()
	for _, m := range s.Opts.Golden.Masks {
		out = m.Pattern.ReplaceAllString(out, m.Replace)
	}
	return out
}

func (s *Scenario) unordered(topic string) (string, bool) {
	for k, r := range s.Opts.Golden.UnorderedChats {
		if strings.Contains(topic, k) {
			return r, true
		}
	}
	return "", false
}

func faultSuffix(f string) string {
	if f == "" {
		return ""
	}
	return " (injected fault " + f + ")"
}

var senders = map[fakegraph.Origin]string{
	fakegraph.OriginSUT:         "helper",
	fakegraph.OriginUser:        "owner",
	fakegraph.OriginOtherHelper: "peer-helper",
	fakegraph.OriginCoworker:    "coworker",
	fakegraph.OriginSystem:      "system",
	fakegraph.OriginSeeded:      "seeded",
}

func (s *Scenario) renderMessage(b *strings.Builder, n *normalizer, m fakegraph.Message, who func(string) string) {
	sender := senders[m.Origin]
	if sender == "" {
		sender = string(m.Origin)
	}
	if m.Instance == "cxp-cli" {
		sender = "cxp-cli" // posted by a cxp command the scenario ran
	}
	extra := ""
	if m.MessageType != "message" {
		extra = " " + m.MessageType
	}
	if len(m.QuotedIDs) > 0 {
		var q []string
		for _, id := range m.QuotedIDs {
			q = append(q, n.msgID(id))
		}
		extra += " quoting " + strings.Join(q, ",")
	}
	fmt.Fprintf(b, "-- %s %s%s (%s)\n", n.msgID(m.ID), sender, extra, m.ContentType)
	writeHTML(b, n.norm(m.Content))
	for _, raw := range m.Mentions {
		var mention struct {
			ID          json.Number `json:"id"`
			MentionText string      `json:"mentionText"`
			Mentioned   struct {
				User *struct {
					ID string `json:"id"`
				} `json:"user"`
			} `json:"mentioned"`
		}
		_ = json.Unmarshal(raw, &mention)
		target := "?"
		if mention.Mentioned.User != nil {
			target = who(mention.Mentioned.User.ID)
		}
		fmt.Fprintf(b, "   mention %s: %s %q\n", mention.ID, target, mention.MentionText)
	}
	for _, h := range m.HostedHashes {
		fmt.Fprintf(b, "   hosted: %s\n", h)
	}
	for _, a := range m.Attachments {
		var v any
		_ = json.Unmarshal(a, &v)
		fmt.Fprintf(b, "   attachment: %s\n", n.norm(canonicalJSON(v)))
	}
	if m.PatchCount > 0 {
		fmt.Fprintf(b, "   edited %d time(s); earlier versions:\n", m.PatchCount)
		for i, old := range m.PatchHistory {
			fmt.Fprintf(b, "   [version %d]\n", i+1)
			writeHTML(b, n.norm(old))
		}
	}
}

func (s *Scenario) renderCodex(b *strings.Builder, n *normalizer) {
	calls := s.Codex.Calls()
	if len(calls) == 0 {
		return
	}
	b.WriteString("\n== codex turns (per thread, in start order)\n")
	var threads []string
	byThread := map[string][]CodexCall{}
	for _, c := range calls {
		key := c.ThreadID
		if key == "" {
			key = "(no thread)"
		}
		if _, ok := byThread[key]; !ok {
			threads = append(threads, key)
		}
		byThread[key] = append(byThread[key], c)
	}
	for _, t := range threads {
		cs := byThread[t]
		fmt.Fprintf(b, "-- thread %s (%s, cwd %s)\n", n.norm(t), cs[0].Kind, n.norm(cs[0].Cwd))
		for _, c := range cs {
			opts := ""
			if c.Model != "" {
				opts += " model=" + c.Model
			}
			if c.Effort != "" {
				opts += " effort=" + c.Effort
			}
			if len(c.Images) > 0 {
				opts += fmt.Sprintf(" images=%d", len(c.Images))
			}
			fmt.Fprintf(b, "   turn%s: %s\n", opts, c.Outcome)
			writeBlock(b, "   prompt| ", n.norm(c.Prompt))
		}
	}
}

// writeHTML writes HTML split for reading at block boundaries; joining the
// lines (and turning a trailing ⏎ back into "\n") gives the exact HTML.
func writeHTML(b *strings.Builder, html string) {
	if html == "" {
		b.WriteString("   |\n")
		return
	}
	for _, line := range splitHTML(html) {
		b.WriteString("   | ")
		b.WriteString(line)
		b.WriteByte('\n')
	}
}

var htmlBreakAfter = regexp.MustCompile(`(?i)(</p>|<br\s*/?>|</li>|</tr>|</h[1-6]>|</pre>|</div>|</blockquote>|</ul>|</ol>|</table>|<hr\s*/?>)`)

func splitHTML(html string) []string {
	var lines []string
	cur := strings.Builder{}
	flush := func() {
		lines = append(lines, cur.String())
		cur.Reset()
	}
	rest := html
	for rest != "" {
		nl := strings.IndexByte(rest, '\n')
		loc := htmlBreakAfter.FindStringIndex(rest)
		switch {
		case nl >= 0 && (loc == nil || nl < loc[1]):
			cur.WriteString(rest[:nl])
			cur.WriteString("⏎")
			flush()
			rest = rest[nl+1:]
		case loc != nil:
			cur.WriteString(rest[:loc[1]])
			flush()
			rest = rest[loc[1]:]
		default:
			cur.WriteString(rest)
			rest = ""
		}
	}
	if cur.Len() > 0 {
		flush()
	}
	return lines
}

func writeBlock(b *strings.Builder, prefix, text string) {
	if text == "" {
		return
	}
	text = strings.TrimSuffix(text, "\n")
	for _, line := range strings.Split(text, "\n") {
		b.WriteString(strings.TrimRight(prefix+line, " "))
		b.WriteByte('\n')
	}
}

func canonicalJSON(v any) string {
	data, err := json.Marshal(v) // map keys are sorted
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(data)
}

// goldenPath is testdata/<scenario>.golden of the running test package.
func (s *Scenario) goldenPath() string {
	return filepath.Join("testdata", sanitize(s.ID)+".golden")
}

// checkGolden compares the transcript with the golden and reports the
// outcome ("match", "mismatch", "updated", "written", "skipped: ...").
func (s *Scenario) checkGolden(actual string) string {
	if s.Opts.Golden.Skip != "" {
		return "skipped: " + s.Opts.Golden.Skip
	}
	path := s.goldenPath()
	update := os.Getenv(EnvUpdateGolden) == "1"
	result := ""
	if out := os.Getenv(EnvGoldenOut); out != "" {
		rel := packageRel()
		dst := filepath.Join(out, rel, path)
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err == nil {
			_ = os.WriteFile(dst, []byte(actual), 0o644)
		}
		result = "written"
	}
	if update {
		if os.Getenv(EnvGoldenOut) == "" {
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				s.T.Errorf("golden: %v", err)
				return "error"
			}
			if err := os.WriteFile(path, []byte(actual), 0o644); err != nil {
				s.T.Errorf("golden: %v", err)
				return "error"
			}
		}
		return "updated"
	}
	want, err := os.ReadFile(path)
	if err != nil {
		s.T.Errorf("GOLDEN %s: no golden file %s (run with %s=1 to create it)\n--- actual transcript ---\n%s", s.ID, path, EnvUpdateGolden, actual)
		return "missing"
	}
	if string(want) == actual {
		if result != "" {
			return result
		}
		return "match"
	}
	s.T.Errorf("GOLDEN %s: the transcript differs from %s (%s=1 accepts it)\n%s", s.ID, path, EnvUpdateGolden, UnifiedDiff(string(want), actual, path, "actual"))
	return "mismatch"
}

// packageRel is the test package directory relative to the module root.
func packageRel() string {
	wd, err := os.Getwd()
	if err != nil {
		return "unknown"
	}
	for dir := wd; ; dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			rel, _ := filepath.Rel(dir, wd)
			return rel
		}
		if filepath.Dir(dir) == dir {
			return filepath.Base(wd)
		}
	}
}
