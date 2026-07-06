package userpath

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"strings"
	"testing"
)

func TestProbeProtocolRoundTrip(t *testing.T) {
	nonce := strings.Repeat("a", probeNonceBytes*2)
	want := probePayload{Nonce: nonce, Path: "/home/u/.local/bin:/usr/bin", Home: "/home/u", Cwd: "/home/u", UID: 1000, EUID: 1000, GID: 100, Groups: []uint32{20, 30}}
	var frame bytes.Buffer
	if err := writeProbePayload(&frame, want); err != nil {
		t.Fatal(err)
	}
	got, err := readProbePayload(&frame, nonce)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("payload = %#v, want %#v", got, want)
	}
}

func TestProbeProtocolRejectsWrongNonceAndTruncation(t *testing.T) {
	nonce := strings.Repeat("b", probeNonceBytes*2)
	var frame bytes.Buffer
	if err := writeProbePayload(&frame, probePayload{Nonce: nonce, Path: "/bin"}); err != nil {
		t.Fatal(err)
	}
	if _, err := readProbePayload(bytes.NewReader(frame.Bytes()), strings.Repeat("c", probeNonceBytes*2)); err == nil || !strings.Contains(err.Error(), "nonce mismatch") {
		t.Fatalf("wrong nonce error = %v", err)
	}
	data := frame.Bytes()
	if _, err := readProbePayload(bytes.NewReader(data[:len(data)-1]), nonce); err == nil {
		t.Fatal("truncated frame succeeded")
	}
}

func TestProbeProtocolRejectsOversizedFrameBeforeAllocation(t *testing.T) {
	var frame bytes.Buffer
	frame.WriteString(probeMagic)
	for _, value := range []any{probeVersion, uint16(1), uint32(maxProbeField + 1), uint32(0), uint32(0), uint32(0), uint32(0), uint32(0), uint32(0)} {
		if err := binary.Write(&frame, binary.BigEndian, value); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := readProbePayload(&frame, "x"); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("oversized frame error = %v", err)
	}
}
