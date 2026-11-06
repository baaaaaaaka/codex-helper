package store

import (
	"crypto/sha256"
	"errors"
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

// TestLegacyUnsentOutboxRendered: the unsettled imported rows are listed in
// conversation order; SetOutboxRendered replaces the legacy text with the
// rendered payload once, so RehashLegacy leaves those rows alone.
func TestLegacyUnsentOutboxRendered(t *testing.T) {
	c := newClock()
	s := openAt(t, filepath.Join(t.TempDir(), "core.sqlite"), c)
	mustT(t, s.Import(func(im *Importer) error {
		conv, err := im.Conversation(Conversation{Label: "s1", Kind: KindWork, Channel: "teams", ExtChatID: "chat-1", State: ConvActive})
		if err != nil {
			return err
		}
		at := c.now().UnixMilli()
		for i, o := range []Outbox{
			{DeliveryKey: "legacy-outbox:sent", Kind: OutFinal, Body: "old", HashAlgo: HashLegacy, State: OutSent, ExtMsgID: "x1", SentAt: at},
			{DeliveryKey: "legacy-outbox:q", Kind: OutFinal, Body: "**queued**", HashAlgo: HashLegacy, State: OutQueued},
			{DeliveryKey: "legacy-outbox:u", Kind: OutNotice, Body: "unknown", HashAlgo: HashLegacy, State: OutUnknown},
			{DeliveryKey: "legacy-outbox:d", Kind: OutNotice, Body: "dropped", HashAlgo: HashLegacy, State: OutDropped},
		} {
			o.ConvID, o.Seq, o.CreatedAt = conv, int64(i+1), at
			if _, _, err := im.Outbox(o, nil); err != nil {
				return err
			}
		}
		return nil
	}))
	_, err := s.Acquire(testIdent(100))
	mustT(t, err)
	var rows []Outbox
	mustT(t, s.Update(func(tx *Tx) error { var err error; rows, err = tx.LegacyUnsentOutbox(); return err }))
	if len(rows) != 2 || rows[0].DeliveryKey != "legacy-outbox:q" || rows[1].DeliveryKey != "legacy-outbox:u" {
		t.Fatalf("unsent legacy rows: %+v", rows)
	}
	mustT(t, s.Update(func(tx *Tx) error {
		for _, r := range rows {
			if err := tx.SetOutboxRendered(r.ID, `{"body":{"content":"`+r.Body+`"}}`, []byte{1, 2}); err != nil {
				return err
			}
		}
		return nil
	}))
	if got := countWhere(t, s, `SELECT count(*) FROM outbox WHERE hash_algo IS NULL AND body LIKE '{"body"%' AND body_hash = x'0102'`); got != 2 {
		t.Fatalf("rendered rows: %d, want 2", got)
	}
	mustT(t, s.Update(func(tx *Tx) error {
		if err := tx.SetOutboxRendered(rows[0].ID, "{}", nil); !errors.Is(err, ErrNotFound) {
			t.Errorf("rendering a row twice: %v", err)
		}
		rows, err = tx.LegacyUnsentOutbox()
		return err
	}))
	if len(rows) != 0 {
		t.Fatalf("still listed after rendering: %d", len(rows))
	}
}
