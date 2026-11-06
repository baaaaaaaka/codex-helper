package harness

// Transcript normalization: volatile values become placeholders numbered by
// first appearance in the transcript, so equal values stay equal and the
// golden is the same in every run. Everything else is kept verbatim: it is
// behaviour.
//
//	<root>            the scenario's temporary root (HOME is <root>/home)
//	<bin>             the directory of the built cxp and fake Codex
//	<hostname>        the container's host name
//	<chat:N>          Teams chat ids (19:…@thread.v2, also URL-encoded in
//	                  deep links), in transcript order
//	<msg:N>           Teams message ids (millisecond stamps of the fake),
//	                  numbered per chat
//	codex-helper-outbox:<marker>
//	                  the idempotency marker of a helper post (unnumbered)
//	E2EANSWER-<n>     the fake Codex's global answer counter
//	<uuid:N>          UUIDs (Codex thread/turn ids, session ids, GUIDs)
//	<time:N>          ISO-8601 / "YYYY-MM-DD HH:MM[:SS]" timestamps
//	<time>            the time of a control-chat history entry in a prompt
//	<ms:N>, <unix:N>  other millisecond / second Unix stamps
//	<pid:N>           "pid <n>"
//	<port:N>          loopback ports
//	<tmp:N>           other paths under the temp directory
//	<key:N>           cxp-<key>: the store's random turn-key prefix
//	<turn:N>          cxp-<key>-<turn>: a turn's sequence number (store-wide,
//	                  so it depends on the start order of concurrent chats)
//	<file:N>          core-files/<n>/: the store row of a received file
//	<id:N>            hex ids of 16+ digits after a prefix (del_, rec_,
//	                  machine:, w-…); the prefix is kept; worker instance
//	                  ids keep their shape: worker_machine:<id:N>_<pid:M>
//	<upload:N>        codex-<date>-<time>-<rand> upload folder names
//	<hex:N>           hex strings of 24+ digits (hashes, request ids),
//	                  except "sha256:" content hashes, which are behaviour
//	<b64:N>           40+ character base64url runs with letters and digits
//	                  (tokens, encoded records)
//
// Scenario tokens (E2E-…) are deterministic and kept.

import (
	"fmt"
	"net/url"
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
	{"chaturl", `19%3[Aa][A-Za-z0-9_\-]+%40(?:thread\.v2|thread\.tacv2|unq\.gbl\.spaces)`},
	{"uuid", `(?i)\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b`},
	// A Codex rollout path: sessions/YYYY/MM/DD/rollout-<start time>-<thread>.jsonl.
	{"rollout", `\b\d{4}/\d{2}/\d{2}/rollout-\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}-`},
	// An entry of the control-chat history in a control question's
	// prompt: whether two entries share a second is timing.
	{"histtime", `- \d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z (?:user|helper)/`},
	{"time", `\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?`},
	{"time", `\b\d{4}-\d{2}-\d{2}[ _]\d{2}[:\-]\d{2}(?:[:\-]\d{2})?(?: ?(?:UTC|Z|[+-]\d{2}:?\d{2}))?`},
	{"outbox", `codex-helper-outbox:[A-Za-z0-9._\-]+`},
	{"answer", `\bE2EANSWER-\d+\b`},
	{"keep", `sha256:[0-9a-f]{64}`},
	{"keep", `E2E[A-Z0-9]*-[A-Za-z0-9]+(?:-[A-Za-z0-9]+)*`},
	{"worker", `\bworker_[a-z]+:[0-9a-f]{16,}_\d+\b`},
	{"upload", `\bcodex-\d{8}-\d{6}-[0-9a-f]{8}\b`},
	{"turnid", `\bcxp-[0-9a-f]{8}-\d+\b`},
	{"turnkey", `\bcxp-[0-9a-f]{8}\b`},
	{"corefile", `core-files/\d+/`},
	{"hexid", `\b[a-z][a-z_]*[_:\-][0-9a-f]{16,}\b`},
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

// placeholder numbers raw within kind. A rule that keeps part of the match
// (a "pid " or "cxp-" prefix) keys its placeholder with a leading NUL, so
// the whole-match lookup in norm never returns the bare placeholder.
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
			return "pid " + n.placeholder("pid", "\x00"+strings.TrimPrefix(m, "pid "))
		case kind == "rollout":
			// The day and start time are the run's; the thread id after
			// them is numbered on its own.
			return "<day>/rollout-<start>-"
		case kind == "outbox":
			// The idempotency marker of every helper post: its presence is
			// behaviour, its value (store prefix + delivery hash) is not.
			return "codex-helper-outbox:<marker>"
		case kind == "histtime":
			return "- <time> " + m[strings.LastIndexByte(m, ' ')+1:]
		case kind == "answer":
			// The fake Codex numbers its answers across all threads; with
			// concurrent chats the number depends on start order.
			return "E2EANSWER-<n>"
		case kind == "chaturl":
			// A URL-encoded chat id (deep links) is the same chat.
			if id, err := url.PathUnescape(m); err == nil {
				return n.chatID(id)
			}
		case kind == "worker":
			// worker_<machine id>_<pid>
			i, j := strings.IndexByte(m, ':'), strings.LastIndexByte(m, '_')
			return m[:i+1] + n.placeholder("id", m[i+1:j]) + "_" + n.placeholder("pid", "\x00"+m[j+1:])
		case kind == "upload":
			return "codex-" + n.placeholder("upload", "\x00"+m)
		case kind == "turnid":
			// cxp-<key>-<turn sequence>: concurrent chats number their
			// turns in start order.
			i := strings.LastIndexByte(m, '-')
			return "cxp-" + n.placeholder("key", "\x00"+m[:i]) + "-" + n.placeholder("turn", "\x00"+m)
		case kind == "corefile":
			// core-files/<inbound message row>/
			return "core-files/" + n.placeholder("file", "\x00"+m) + "/"
		case kind == "turnkey":
			return "cxp-" + n.placeholder("key", "\x00"+m)
		case kind == "hexid":
			i := strings.LastIndexAny(m, "_:-")
			return m[:i+1] + n.placeholder("id", m[i+1:])
		case kind == "port":
			i := strings.LastIndexByte(m, ':')
			return m[:i+1] + n.placeholder("port", "\x00"+m[i+1:])
		}
		return n.placeholder(kind, m)
	})
}

// scratch normalizes text without numbering (sort keys).
func (n *normalizer) scratch(text string) string {
	return n.re.ReplaceAllString(text, "<x>")
}
