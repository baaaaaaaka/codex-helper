package harness

// Transcript normalization: volatile values become placeholders numbered by
// first appearance in the transcript, so equal values stay equal and the
// golden is the same in every run. Everything else is kept verbatim: it is
// behaviour.
//
//	<root>            the scenario's temporary root (HOME is <root>/home)
//	<bin>             the directory of the built cxp and fake Codex
//	<hostname>        the container's host name
//	<chat:N>          Teams chat ids (19:…@thread.v2), in transcript order
//	<msg:N>           Teams message ids (millisecond stamps of the fake)
//	<uuid:N>          UUIDs (Codex thread/turn ids, session ids, GUIDs)
//	<time:N>          ISO-8601 / "YYYY-MM-DD HH:MM[:SS]" timestamps
//	<ms:N>, <unix:N>  other millisecond / second Unix stamps
//	<pid:N>           "pid <n>"
//	<port:N>          loopback ports
//	<tmp:N>           other paths under the temp directory
//	<hex:N>           hex strings of 24+ digits (hashes, request ids),
//	                  except "sha256:" content hashes, which are behaviour
//	<b64:N>           40+ character base64url runs with letters and digits
//	                  (tokens, encoded records)
//
// Scenario tokens (E2E-…) are deterministic and kept.

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

type normalizer struct {
	exact    []exactRule
	ids      map[string]string
	counters map[string]int
	re       *regexp.Regexp
	kinds    []string
}

type exactRule struct{ from, to string }

// genericRules are tried left to right on every text (first match wins
// at a position); each names the placeholder kind.
var genericRules = []struct{ kind, pattern string }{
	{"chat", `19:[A-Za-z0-9_\-]+@(?:thread\.v2|thread\.tacv2|unq\.gbl\.spaces)`},
	{"uuid", `(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`},
	{"time", `\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?`},
	{"time", `\b\d{4}-\d{2}-\d{2}[ _]\d{2}[:\-]\d{2}(?:[:\-]\d{2})?(?: ?(?:UTC|Z|[+-]\d{2}:?\d{2}))?`},
	{"keep", `sha256:[0-9a-f]{64}`},
	{"keep", `E2E[A-Z0-9]*-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*`},
	{"hex", `\b[0-9a-f]{24,}\b`},
	{"ms", `\b1[78]\d{11}\b`},
	{"unix", `\b1[78]\d{8}\b`},
	{"pid", `\bpid \d+\b`},
	{"port", `\b(?:127\.0\.0\.1|localhost|\[::1\]):\d+\b`},
	{"tmp", `/tmp/[^\s"'<>&|;,)\]]+`},
	{"b64", `\b[A-Za-z0-9_\-]*[0-9][A-Za-z0-9_\-]*[A-Za-z][A-Za-z0-9_\-]{38,}={0,2}|\b[A-Za-z0-9_\-]*[A-Za-z][A-Za-z0-9_\-]*[0-9][A-Za-z0-9_\-]{38,}={0,2}`},
}

func (s *Scenario) newNormalizer() *normalizer {
	n := &normalizer{ids: map[string]string{}, counters: map[string]int{}}
	add := func(from, to string) {
		if from != "" && len(from) > 3 {
			n.exact = append(n.exact, exactRule{from, to})
		}
	}
	if s != nil && s.Env != nil {
		add(s.Env.Root, "<root>")
		if real, err := filepath.EvalSymlinks(s.Env.Root); err == nil && real != s.Env.Root {
			add(real, "<root>")
		}
	}
	if CLIBinary != "" {
		add(filepath.Dir(CLIBinary), "<bin>")
	}
	if h, err := os.Hostname(); err == nil {
		add(h, "<hostname>")
	}
	sort.SliceStable(n.exact, func(a, b int) bool { return len(n.exact[a].from) > len(n.exact[b].from) })
	var parts []string
	for _, e := range n.exact {
		parts = append(parts, "("+regexp.QuoteMeta(e.from)+")")
		n.kinds = append(n.kinds, "exact:"+e.to)
	}
	for _, g := range genericRules {
		p := g.pattern
		flags := ""
		if strings.HasPrefix(p, "(?i)") {
			flags, p = "(?i:", strings.TrimPrefix(p, "(?i)")
		}
		if flags != "" {
			parts = append(parts, "("+flags+p+"))")
		} else {
			parts = append(parts, "("+p+")")
		}
		n.kinds = append(n.kinds, g.kind)
	}
	n.re = regexp.MustCompile(strings.Join(parts, "|"))
	return n
}

func (n *normalizer) placeholder(kind, raw string) string {
	if p, ok := n.ids[raw]; ok {
		return p
	}
	n.counters[kind]++
	p := fmt.Sprintf("<%s:%d>", kind, n.counters[kind])
	n.ids[raw] = p
	return p
}

// chatID and msgID assign (or return) the placeholder of a known id.
func (n *normalizer) chatID(id string) string { return n.placeholder("chat", id) }
func (n *normalizer) msgID(id string) string  { return n.placeholder("msg", id) }

// norm normalizes text.
func (n *normalizer) norm(text string) string {
	if text == "" {
		return ""
	}
	return n.re.ReplaceAllStringFunc(text, func(m string) string {
		if p, ok := n.ids[m]; ok {
			return p
		}
		sub := n.re.FindStringSubmatchIndex(m)
		kind := ""
		for i := range n.kinds {
			if sub[2*(i+1)] >= 0 {
				kind = n.kinds[i]
				break
			}
		}
		switch {
		case strings.HasPrefix(kind, "exact:"):
			return strings.TrimPrefix(kind, "exact:")
		case kind == "keep":
			return m
		case kind == "pid":
			return "pid " + n.placeholder("pid", m)
		case kind == "port":
			i := strings.LastIndexByte(m, ':')
			return m[:i+1] + n.placeholder("port", m)
		}
		return n.placeholder(kind, m)
	})
}

// scratch normalizes text without numbering (sort keys).
func (n *normalizer) scratch(text string) string {
	return n.re.ReplaceAllString(text, "<x>")
}
