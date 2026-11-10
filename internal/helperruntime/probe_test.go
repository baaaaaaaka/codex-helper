package helperruntime

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestSafeBootProtocolProbeIsInertAndExact(t *testing.T) {
	t.Setenv(EnvProbeProtocol, "2")
	var out bytes.Buffer
	if code, handled := HandleProtocolProbe([]string{"cxp", ProtocolProbeFlag}, &out); !handled || code != 0 || out.String() != "2\n" {
		t.Fatalf("%d %v %q", code, handled, out.String())
	}
	for _, args := range [][]string{{"cxp"}, {"cxp", ProtocolProbeFlag, "tui"}, {"cxp", "tui", ProtocolProbeFlag}} {
		out.Reset()
		if _, handled := HandleProtocolProbe(args, &out); handled || out.Len() != 0 {
			t.Fatal(args)
		}
	}
}

func TestHistoricalBootProtocolProbe(t *testing.T) {
	t.Setenv(EnvDisable, "1")
	t.Setenv(EnvProbeProtocol, "")
	var out bytes.Buffer
	if code, handled := HandleProtocolProbe([]string{"candidate", ProtocolProbeFlag}, &out); !handled || code != 0 || out.String() != "1\n" {
		t.Fatalf("%d %v %q", code, handled, out.String())
	}
}

func TestSafeBootProbeOutputHasFixedMemoryBound(t *testing.T) {
	var b probeOutput
	for i := 0; i < 100; i++ {
		if n, err := b.Write([]byte(strings.Repeat("x", 1000))); n != 1000 || err != nil {
			t.Fatalf("%d %v", n, err)
		}
	}
	if !b.truncated || b.Len() != 16<<10 {
		t.Fatalf("%d %v", b.Len(), b.truncated)
	}
	// os/exec uses io.Copy. Embedding bytes.Buffer would expose ReadFrom and
	// silently bypass Write's size bound even though direct-write tests pass.
	var copied probeOutput
	if _, err := io.Copy(&copied, io.LimitReader(strings.NewReader(strings.Repeat("x", 100<<10)), 100<<10)); err != nil {
		t.Fatal(err)
	}
	if !copied.truncated || copied.Len() != 16<<10 {
		t.Fatalf("io.Copy bypassed limit: %d %v", copied.Len(), copied.truncated)
	}
}
