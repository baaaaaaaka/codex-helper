package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// hashingChannel is the fake channel with the adapter's body hash.
type hashingChannel struct{ *channeltest.Fake }

func (hashingChannel) BodyHash(body string) string {
	h := sha256.Sum256([]byte("adapter:" + body))
	return hex.EncodeToString(h[:])
}

// The first start on a converted store replaces the imported legacy body
// hashes with the adapter's, so the helper's own migrated messages are
// recognized as echoes; later starts do not repeat it.
func TestLegacyHashesRehashedOnceAtFirstStart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "core.sqlite")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	legacy := sha256.Sum256([]byte("<p>answer</p>"))
	var conv int64
	if err := st.Import(func(im *store.Importer) error {
		var err error
		conv, err = im.Conversation(store.Conversation{Label: "s1", Kind: store.KindWork, Channel: "teams", ExtChatID: "chat-legacy", State: store.ConvActive})
		if err != nil {
			return err
		}
		if err := im.Park(conv); err != nil {
			return err
		}
		at := im.Now()
		_, _, err = im.Outbox(store.Outbox{ConvID: conv, Seq: 1, DeliveryKey: "legacy-outbox:1", Kind: store.OutFinal, Body: "answer",
			BodyHash: legacy[:], HashAlgo: store.HashLegacy, ExtMsgID: "x1", State: store.OutSent, CreatedAt: at, SentAt: at}, nil)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	st.Close()

	ch := hashingChannel{channeltest.New()}
	var logMu sync.Mutex
	var log strings.Builder
	isOwn := func(st *store.Store, h []byte) bool {
		var own bool
		if err := st.Update(func(tx *store.Tx) error {
			var err error
			own, err = tx.IsOwn(conv, "", h, 0)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		return own
	}
	run := func() {
		st, err := store.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		defer st.Close()
		if _, err := st.Acquire(store.Ident{Host: "h", PID: 1, BootID: "b"}); err != nil {
			t.Fatal(err)
		}
		defer st.Release()
		e, err := New(Config{Store: st, Channel: ch, Agent: newAgent(), Timing: FastTiming(20 * time.Millisecond), Machine: "m",
			Log: lockedWriter{&logMu, &log}})
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- e.Run(ctx) }()
		deadline := time.Now().Add(10 * time.Second)
		for {
			if v, _, _ := st.Meta(metaRehashed); v == "1" {
				break
			}
			if time.Now().After(deadline) {
				cancel()
				<-done
				t.Fatalf("legacy hashes not re-hashed at start; log:\n%s", log.String())
			}
			time.Sleep(10 * time.Millisecond)
		}
		cancel()
		<-done
		if isOwn(st, legacy[:]) {
			t.Error("the legacy hash still matches")
		}
		if !isOwn(st, hashBytes(ch.BodyHash("answer"))) {
			t.Error("the adapter's hash of the migrated final does not match")
		}
	}
	run()
	run()
	logMu.Lock()
	defer logMu.Unlock()
	if n := strings.Count(log.String(), "re-hashed 1 imported messages"); n != 1 {
		t.Fatalf("re-hash logged %d times over two starts, want 1; log:\n%s", n, log.String())
	}
}
