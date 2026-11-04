package store

import (
	"crypto/sha256"
	"path/filepath"
	"testing"
)

// TestRehashLegacy: imported legacy hashes never match an echo; RehashLegacy
// replaces them with the adapter's hash of the kept body, once.
func TestRehashLegacy(t *testing.T) {
	c := newClock()
	s := openAt(t, filepath.Join(t.TempDir(), "core.sqlite"), c)
	legacy := sha256.Sum256([]byte("<p>answer</p>"))
	var conv int64
	mustT(t, s.Import(func(im *Importer) error {
		var err error
		conv, err = im.Conversation(Conversation{Label: "s1", Kind: KindWork, Channel: "teams", ExtChatID: "chat-1", State: ConvActive})
		if err != nil {
			return err
		}
		if err := im.Park(conv); err != nil {
			return err
		}
		at := c.now().UnixMilli()
		for i, o := range []Outbox{
			{DeliveryKey: "legacy-outbox:1", Kind: OutFinal, Body: "answer", BodyHash: legacy[:], HashAlgo: HashLegacy, ExtMsgID: "x1"},
			{DeliveryKey: "legacy-delivery:2", Kind: OutMirror, HashAlgo: HashLegacy, ExtMsgID: "x2"},
		} {
			o.ConvID, o.Seq, o.State, o.CreatedAt, o.SentAt = conv, int64(i+1), OutSent, at, at
			if _, _, err := im.Outbox(o, nil); err != nil {
				return err
			}
		}
		return nil
	}))
	if n := countWhere(t, s, `SELECT count(*) FROM conversations WHERE id = ? AND state = 'parked' AND parked_at IS NOT NULL`, conv); n != 1 {
		t.Fatal("Importer.Park did not park the conversation")
	}
	_, err := s.Acquire(testIdent(100))
	mustT(t, err)
	adapter := func(body string) []byte { h := sha256.Sum256([]byte("adapter:" + body)); return h[:] }
	isOwn := func(h []byte) bool {
		var own bool
		mustT(t, s.Update(func(tx *Tx) error {
			var err error
			own, err = tx.IsOwn(conv, "", h, 0)
			return err
		}))
		return own
	}
	if isOwn(legacy[:]) {
		t.Fatal("a legacy hash matched an echo before re-hashing")
	}
	var n int
	mustT(t, s.Update(func(tx *Tx) error { var err error; n, err = tx.RehashLegacy(adapter); return err }))
	if n != 2 {
		t.Fatalf("rehashed %d rows, want 2", n)
	}
	if !isOwn(adapter("answer")) {
		t.Fatal("re-hashed body does not match")
	}
	if got := countWhere(t, s, `SELECT count(*) FROM outbox WHERE body_hash IS NULL AND hash_algo IS NULL AND body IS NULL`); got != 1 {
		t.Fatalf("key-only row: %d with a NULL hash, want 1", got)
	}
	mustT(t, s.Update(func(tx *Tx) error { var err error; n, err = tx.RehashLegacy(adapter); return err }))
	if n != 0 {
		t.Fatalf("second RehashLegacy touched %d rows", n)
	}
}
