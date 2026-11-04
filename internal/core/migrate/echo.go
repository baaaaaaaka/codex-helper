package migrate

import (
	"regexp"
	"strings"
	"unicode"
)

// Self-echo proof (fidelity run, s001). Early helper versions kept no
// delivery ledger and pruned their outbox, so a rollout can have an old
// checkpoint and no proof at all: it resumes at the start and its records
// are mirrored again. Some of those records are nevertheless provably in the
// chat already: the old helper read its own posts back while its self-filter
// failed and stored them as inbound events ("🤖 ✅ Codex answer: …",
// "🧑‍💻 User: …", or an imported prompt without a label, status "ignored").
// Such a record gets a sent mirror row under its key, so the engine's
// per-record dedupe skips it; every record without that evidence is still
// mirrored (no loss).

// echoKeyRunes is how much of the normalized text identifies a record.
const echoKeyRunes = 80

// echoMinRunes: shorter texts ("ok", "continue") identify nothing.
const echoMinRunes = 12

// echoMsg is one helper message the old core read back from a chat.
type echoMsg struct {
	final bool   // a Codex answer; else a prompt
	key   string // echoKey of its body
	at    int64  // when the old core received it (unix ms)
	ext   string // its Teams message id
	used  bool
}

var echoLabel = regexp.MustCompile(`^(🧑‍💻 User|🤖 ✅ Codex answer)( \[part \d+/\d+\])?:\s*`)

// noteEcho keeps an inbound event of conversation conv when it is a helper
// post read back: a labelled answer or prompt (any status), or an unlabelled
// text the old core ignored (an imported prompt). Unlabelled texts the old
// core queued or ran were typed by the owner and prove nothing.
func (c *converter) noteEcho(conv int64, in legacyInbound) {
	text := strings.TrimSpace(in.Text)
	m := echoLabel.FindStringSubmatch(text)
	final := false
	switch {
	case m != nil:
		final = m[1] != "🧑‍💻 User"
		text = text[len(m[0]):]
	case in.Status != "ignored" || strings.HasPrefix(text, "🔧") || strings.HasPrefix(text, "🤖"):
		return
	}
	key := echoKey(text)
	if key == "" || in.TeamsMessageID == "" {
		return
	}
	c.echoes[conv] = append(c.echoes[conv], &echoMsg{final: final, key: key, at: ms(firstTime(in.ReceivedAt, in.CreatedAt)), ext: in.TeamsMessageID})
}

func firstTime(ts ...ltime) ltime {
	for _, t := range ts {
		if !t.IsZero() {
			return t
		}
	}
	return ltime{}
}

// echoKey is the lower-cased letters and digits of the first echoKeyRunes
// of them in text ("" when there are fewer than echoMinRunes). Markup and
// punctuation differ between a rollout record and the text Teams returns.
func echoKey(text string) string {
	var b strings.Builder
	n := 0
	for _, r := range text {
		if !unicode.IsLetter(r) && !unicode.IsNumber(r) && r != '_' {
			continue
		}
		b.WriteRune(unicode.ToLower(r))
		if n++; n == echoKeyRunes {
			break
		}
	}
	if n < echoMinRunes {
		return ""
	}
	return b.String()
}

// echoProof marks the records from rs.off on that a self-echo in the
// conversation proves posted: each echo proves one record of its kind with
// the same key that it was received after, in file order (scanned records
// before rs.off consume their echoes too, so a repeated text is proven only
// as often as it was echoed). Returns the number marked.
func (c *converter) echoProof(p *pathInfo, rs resume, thread string) (int, error) {
	echoes := c.echoes[p.conv]
	if len(echoes) == 0 || thread == "" || p.scan == nil {
		return 0, nil
	}
	n := 0
	for _, r := range p.scan.Recs {
		if r.Key == "" {
			continue
		}
		for _, e := range echoes {
			if e.used || e.final != (r.Kind == "final") || e.key != r.Key || (r.At > 0 && e.at > 0 && e.at < r.At) {
				continue
			}
			e.used = true
			key := MirrorKey(thread, r.ID, 1)
			if r.Start < rs.off || c.mirrorUsed[key] {
				break // proven already (before the resume point, or a ledger row)
			}
			c.mirrorUsed[key] = true
			if err := c.sentMirror(p.conv, key, e.ext, e.at); err != nil {
				return n, err
			}
			n++
			break
		}
	}
	return n, nil
}
