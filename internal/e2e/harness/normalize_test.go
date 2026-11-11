package harness

import (
	"strings"
	"testing"
)

func TestNormalizerKeepsShapeOfRepeatedValues(t *testing.T) {
	n := (*Scenario)(nil).newNormalizer()
	in := strings.Join([]string{
		"pid 3005 pid 3005 worker_machine:a7e42e60a5923d9f_3005",
		"cxp-8084bfb7 cxp-8084bfb7-1 cxp-4e13c96a core-files/10/x",
		"codex-20261105-021916-605028a1 codex-20261105-021916-605028a1",
		"127.0.0.1:35715 127.0.0.1:35715",
		"del_45e87b1f796c7735f88bff8d589a9918 machine:a7e42e60a5923d9f w-1c7ea8aef006b1b5",
		"19:abc_DEF-1@thread.v2 https://teams.microsoft.com/l/chat/19%3Aabc_DEF-1%40thread.v2/0",
		"<!-- codex-helper-outbox:cxp-8084bfb7.0123456789abcdef0123 --> E2EANSWER-12 E2E-DUP-plain-2",
		"sha256:" + strings.Repeat("ab", 32),
	}, "\n")
	want := strings.Join([]string{
		"pid <pid:1> pid <pid:1> worker_machine:<id:1>_<pid:1>",
		"cxp-<key:1> cxp-<key:1>-<turn:1> cxp-<key:2> core-files/<file:1>/x",
		"codex-<upload:1> codex-<upload:1>",
		"127.0.0.1:<port:1> 127.0.0.1:<port:1>",
		"del_<id:2> machine:<id:1> w-<id:3>",
		"<chat:1> https://teams.microsoft.com/l/chat/<chat:1>/0",
		"<!-- codex-helper-outbox:<marker> --> E2EANSWER-<n> E2E-DUP-plain-2",
		"sha256:" + strings.Repeat("ab", 32),
	}, "\n")
	if got := n.norm(in); got != want {
		t.Fatalf("normalized:\n%s", UnifiedDiff(want, got, "want", "got"))
	}
}

func TestUnifiedDiffShowsChangedLines(t *testing.T) {
	d := UnifiedDiff("a\nb\nc\n", "a\nB\nc\n", "want", "got")
	for _, s := range []string{"--- want", "+++ got", "-b", "+B"} {
		if !strings.Contains(d, s) {
			t.Fatalf("diff lacks %q:\n%s", s, d)
		}
	}
}

func TestNormalizerPreservesReceiptLinksAndPauseIdentity(t *testing.T) {
	n := (*Scenario)(nil).newNormalizer()
	input := `<p>Received at <a href="https://teams/x">2026-11-11 15:06:35 UTC+08:00</a></p> ` +
		`<p>Paused at 2026-11-11 15:06:36 UTC+08:00</p> #cxp-freeze%3A111 #cxp-freeze%3A222 #cxp-freeze%3A111`
	got := n.norm(input)
	for _, want := range []string{`Received at <a href="https://teams/x"><event-time></a>`, `Paused at <event-time>`, `#cxp-freeze%3A<pause:1> #cxp-freeze%3A<pause:2> #cxp-freeze%3A<pause:1>`} {
		if !strings.Contains(got, want) {
			t.Fatalf("normalizer hid annotation structure: %s", got)
		}
	}
}
