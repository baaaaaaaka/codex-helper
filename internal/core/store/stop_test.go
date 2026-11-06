package store

import (
	"os"
	"strings"
	"testing"
	"time"
)

// The kill-switch file written by `cxp teams stop` and `shutdown` records
// who stopped the helper, where and when; stopping again keeps the first
// record, starting removes it, and a file made by hand still stops the
// helper and is described by its path.
func TestStopRecordRoundTripAndHandMadeFile(t *testing.T) {
	dir := t.TempDir()
	at := time.Date(2026, 11, 5, 9, 30, 0, 0, time.UTC)
	created, err := WriteStop(dir, StopRecord{Source: StopByCLI, Host: "host-a\nx", At: at})
	if err != nil || !created {
		t.Fatalf("WriteStop: created=%v err=%v", created, err)
	}
	created, err = WriteStop(dir, StopRecord{Source: StopByTeams, Host: "host-b", At: at.Add(time.Hour)})
	if err != nil || created {
		t.Fatalf("second WriteStop: created=%v err=%v, want the first record kept", created, err)
	}
	r, ok, err := ReadStop(dir)
	if err != nil || !ok || r.Source != StopByCLI || r.Host != "host-a x" || !r.At.Equal(at) {
		t.Fatalf("ReadStop = %+v ok=%v err=%v", r, ok, err)
	}
	st, err := Open(dir + "/core.db")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if !st.Disabled() {
		t.Fatal("the written record does not disable the store")
	}
	when := at.Local().Format("2006-01-02 15:04:05 -07:00")
	if got, want := r.Describe(DisabledPath(dir)), "Stopped on all machines by `cxp teams stop` (host-a x, "+when+"). Start again with `cxp teams start`."; got != want {
		t.Fatalf("Describe = %q, want %q", got, want)
	}
	if got := r.Reason(); !strings.HasPrefix(got, "stopped on all machines by `cxp teams stop` (host-a x, ") || !strings.HasSuffix(got, " until `cxp teams start`") {
		t.Fatalf("Reason = %q", got)
	}
	teams := StopRecord{Source: StopByTeams, Host: "m", At: at}
	if got, want := teams.Describe(""), "Stopped on all machines by `shutdown` in the Teams control chat (m, "+when+"). Start again with `cxp teams start`."; got != want {
		t.Fatalf("Describe = %q, want %q", got, want)
	}

	removed, err := RemoveStop(dir)
	if err != nil || !removed || st.Disabled() {
		t.Fatalf("RemoveStop: removed=%v err=%v disabled=%v", removed, err, st.Disabled())
	}
	if removed, err := RemoveStop(dir); err != nil || removed {
		t.Fatalf("second RemoveStop: removed=%v err=%v", removed, err)
	}
	if _, ok, _ := ReadStop(dir); ok {
		t.Fatal("ReadStop found a removed file")
	}

	// Made by hand (empty): stopped, no record, described by its path.
	if err := os.WriteFile(DisabledPath(dir), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	r, ok, err = ReadStop(dir)
	if err != nil || !ok || r != (StopRecord{}) {
		t.Fatalf("hand-made ReadStop = %+v ok=%v err=%v", r, ok, err)
	}
	if got, want := r.Describe(DisabledPath(dir)), "Stopped on all machines: the kill-switch file "+DisabledPath(dir)+" exists. Start again with `cxp teams start`."; got != want {
		t.Fatalf("hand-made Describe = %q, want %q", got, want)
	}
	if got := r.Reason(); got != "the kill-switch file DISABLED exists" {
		t.Fatalf("hand-made Reason = %q", got)
	}
	if created, err := WriteStop(dir, StopRecord{Source: StopByCLI, Host: "h", At: at}); err != nil || created {
		t.Fatalf("WriteStop over a hand-made file: created=%v err=%v, want it kept", created, err)
	}
	if b, _ := os.ReadFile(DisabledPath(dir)); len(b) != 0 {
		t.Fatalf("the hand-made file was rewritten: %q", b)
	}
}
