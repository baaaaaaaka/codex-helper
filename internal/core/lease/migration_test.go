package lease

import (
	"encoding/binary"
	"hash/crc32"
	"strings"
	"testing"
)

func TestLeaseNativeSlotAndLegacyTakeoverEvidence(t *testing.T) {
	// Literal historical data, not the current encoder's output.
	body := []byte(`{"v":1,"gen":9007199254740993,"state":"released","host":"旧-host","pid":42,"nonce":"n","beat":9007199254740995,"wall":-1,"db":"core.g9.sqlite"}`)
	old := make([]byte, slotSize)
	copy(old, "CXL1")
	binary.BigEndian.PutUint16(old[4:], uint16(len(body)))
	binary.BigEndian.PutUint32(old[6:], crc32.ChecksumIEEE(body))
	copy(old[slotHeader:], body)
	r, ok, err := decode(old)
	if err != nil || !ok || r.Gen != 9007199254740993 || r.Beat != 9007199254740995 || r.Wall != -1 || r.Host != "旧-host" {
		t.Fatalf("lost old ownership evidence: %+v %v", r, err)
	}
	native, err := encode(r)
	if err != nil {
		t.Fatal(err)
	}
	if string(native[:4]) != "CXL2" || strings.Contains(string(native), `"gen"`) {
		t.Fatal("native writer emitted legacy JSON")
	}
	got, ok, err := decode(native)
	if err != nil || !ok || got.Gen != r.Gen || got.Beat != r.Beat || got.Wall != r.Wall || got.Host != r.Host || got.State != Released {
		t.Fatalf("native evidence: %+v %v", got, err)
	}
	r.Host = strings.Repeat("a", slotSize)
	if _, err := encode(r); err == nil {
		t.Fatal("accepted oversized slot")
	}
	// Future versions remain nonempty and unknown: fencing must not treat them
	// as an empty/released slot or silently create a new database.
	native[3] = '9'
	if _, ok, err := decode(native); !ok || err == nil {
		t.Fatal("future slot did not fence reader")
	}
}
