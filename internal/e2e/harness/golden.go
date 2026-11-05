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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html"
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
	// Races declares pairs of helper messages whose order in a chat is a
	// known race in cxp: when the two are adjacent in the reverse order,
	// the golden shows them in the declared order and says so.
	Races []Race
	// FinishedNotices renders the owner's "✅ Codex finished"
	// notifications of the control chat (one per final) as a sorted list,
	// one line per notification with the session and request it names and
	// its @mention, not as messages, because finals that complete together
	// are posted in timing order. Value: the reason. A session notified
	// twice still shows twice.
	FinishedNotices string
}

// Race is a declared ordering race between two helper messages of one
// chat: First and Second are substrings of their plain text.
type Race struct {
	First, Second, Reason string
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
	for _, r := range o.Races {
		if r.First == "" || r.Second == "" || strings.TrimSpace(r.Reason) == "" {
			return fmt.Errorf("golden: a race needs both messages and a reason")
		}
	}
	if o.FinishedNotices != "" && strings.TrimSpace(o.FinishedNotices) == "" {
		return fmt.Errorf("golden: FinishedNotices needs a reason")
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
		var notices []string
		if s.Opts.Golden.FinishedNotices != "" && strings.HasPrefix(c.Topic, ControlChatTopicMarker) {
			msgs, notices = splitFinishedNotices(msgs, who)
		}
		var raced []string
		msgs, raced = s.Opts.Golden.applyRaces(msgs)
		for _, r := range raced {
			fmt.Fprintf(&b, "   (declared race, shown in the declared order: %s)\n", r)
		}
		// Message placeholders are numbered per chat, so a difference in
		// one chat does not renumber the others.
		n.counters["msg"] = 0
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
			s.renderMessage(&b, n, m, who, c.Topic == RegistryChatTopic)
		}
		if len(notices) > 0 {
			fmt.Fprintf(&b, "-- \"Codex finished\" notices, one line each, sorted (posting order is timing: %s)\n", s.Opts.Golden.FinishedNotices)
			for _, x := range notices {
				fmt.Fprintf(&b, "   finished: %s\n", n.norm(x))
			}
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

func (s *Scenario) renderMessage(b *strings.Builder, n *normalizer, m fakegraph.Message, who func(string) string, heartbeat bool) {
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
	writeWireRecords(b, n, m.Content)
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
	if m.PatchCount > 0 && heartbeat {
		// The registry card is re-written on every heartbeat: how often
		// depends on the scenario's duration, so only the final version
		// is behaviour.
		b.WriteString("   edited by heartbeats (count and earlier versions not recorded: timing)\n")
	} else if m.PatchCount > 0 {
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
	b.WriteString("\n== codex turns (per thread; threads by kind, directory and first prompt; turns in start order)\n")
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
	// Threads of different chats start concurrently: order them by what
	// they are, not by which start won the race.
	key := func(t string) string {
		c := byThread[t][0]
		return c.Kind + "\x00" + n.scratch(c.Cwd) + "\x00" + n.scratch(c.Prompt)
	}
	sort.SliceStable(threads, func(a, b int) bool { return key(threads[a]) < key(threads[b]) })
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

// wireRecordRE finds cxp's machine-to-machine records (delegation records,
// registry cards): a marker and a base64url JSON payload.
var wireRecordRE = regexp.MustCompile(`(CXP_[A-Z_]+_V\d+)\s+([A-Za-z0-9_\-]{8,})`)

// writeWireRecords decodes the wire records of a message so the golden
// shows their content (they are what other machines read).
func writeWireRecords(b *strings.Builder, n *normalizer, content string) {
	for _, sub := range wireRecordRE.FindAllStringSubmatch(content, -1) {
		raw, err := base64.RawURLEncoding.DecodeString(sub[2])
		if err != nil {
			fmt.Fprintf(b, "   record %s: not base64url (%v)\n", sub[1], err)
			continue
		}
		var v any
		if err := json.Unmarshal(raw, &v); err != nil {
			fmt.Fprintf(b, "   record %s: not JSON (%v)\n", sub[1], err)
			continue
		}
		pretty, _ := json.MarshalIndent(v, "", "  ")
		fmt.Fprintf(b, "   record %s:\n", sub[1])
		writeBlock(b, "   record| ", n.norm(string(pretty)))
	}
}

var (
	finishedChatRE    = regexp.MustCompile(`<strong>Chat:</strong> ([^<]*)`)
	finishedRequestRE = regexp.MustCompile(`<strong>Request:</strong> ([^<]*)`)
)

// finishedHeading marks the owner's notification of a final.
const finishedHeading = "<strong>✅ Codex finished</strong>"

// splitFinishedNotices takes the helper's "✅ Codex finished" notices out
// of msgs and returns, sorted, one line per notice: the session and the
// request it names, and its mentions.
func splitFinishedNotices(msgs []fakegraph.Message, who func(string) string) ([]fakegraph.Message, []string) {
	var rest []fakegraph.Message
	var out []string
	for _, m := range msgs {
		if m.Origin != fakegraph.OriginSUT || !strings.Contains(m.Content, finishedHeading) {
			rest = append(rest, m)
			continue
		}
		var at []string
		for _, raw := range m.Mentions {
			var mention struct {
				Mentioned struct {
					User *struct {
						ID string `json:"id"`
					} `json:"user"`
				} `json:"mentioned"`
			}
			_ = json.Unmarshal(raw, &mention)
			if mention.Mentioned.User != nil {
				at = append(at, "@"+who(mention.Mentioned.User.ID))
			}
		}
		mentions := " (no mention)"
		if len(at) > 0 {
			mentions = " (" + strings.Join(at, " ") + ")"
		}
		line := "unparsed: " + strings.TrimSpace(m.PlainText())
		if sub := finishedChatRE.FindAllStringSubmatch(m.Content, -1); len(sub) == 1 {
			line = html.UnescapeString(sub[0][1])
			if req := finishedRequestRE.FindStringSubmatch(m.Content); req != nil {
				line += " | request: " + html.UnescapeString(req[1])
			}
		}
		out = append(out, line+mentions)
	}
	sort.Strings(out)
	return rest, out
}

// applyRaces puts the declared racing pairs of msgs into their declared
// order and returns the reasons of the races present in the chat (in
// either order, so the golden does not depend on who won).
func (o GoldenOptions) applyRaces(msgs []fakegraph.Message) ([]fakegraph.Message, []string) {
	var reasons []string
	for _, r := range o.Races {
		found := false
		for i := 0; i+1 < len(msgs); i++ {
			a, b := msgs[i], msgs[i+1]
			if a.Origin != fakegraph.OriginSUT || b.Origin != fakegraph.OriginSUT {
				continue
			}
			at, bt := a.PlainText(), b.PlainText()
			switch {
			case strings.Contains(at, r.First) && strings.Contains(bt, r.Second):
				found = true
			case strings.Contains(at, r.Second) && strings.Contains(bt, r.First):
				msgs = append([]fakegraph.Message(nil), msgs...)
				msgs[i], msgs[i+1] = b, a
				found = true
			}
		}
		if found {
			reasons = append(reasons, r.Reason)
		}
	}
	return msgs, reasons
}
