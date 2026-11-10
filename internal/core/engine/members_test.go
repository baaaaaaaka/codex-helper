package engine

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

type memberTestChannel struct {
	*channeltest.Fake
	other       atomic.Bool
	fail        atomic.Bool
	checks      atomic.Int32
	mu          sync.Mutex
	annotations map[string]channel.Annotation
}

func (c *memberTestChannel) OtherParticipants(context.Context, string) (bool, error) {
	c.checks.Add(1)
	if c.fail.Load() {
		return false, fmt.Errorf("membership unavailable")
	}
	return c.other.Load(), nil
}
func (c *memberTestChannel) Annotate(_ context.Context, chat, id string, a channel.Annotation) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.annotations[chat+"/"+id] = a
	return nil
}

func TestMembershipRestoresSingleUserVoiceAnnotations(t *testing.T) {
	asr := &fakeASR{release: make(chan struct{})}
	close(asr.release)
	var ch *memberTestChannel
	r := newRig(t, func(c *Config) {
		ch = &memberTestChannel{Fake: c.Channel.(*channeltest.Fake), annotations: map[string]channel.Annotation{}}
		ch.other.Store(true)
		c.Channel = ch
		c.Transcriber = asr
		c.FilesDir = t.TempDir()
	})
	work := r.newWork(t.TempDir())
	conv := convIDForTest(r, work)
	// Let a real membership window persist the old group state. Mutating the
	// database and live cache separately can race an in-flight owner-only
	// window and leave the fixture inconsistent before the repair is tested.
	r.wait("initial group membership", func() bool {
		v, _, err := r.st.Meta(fmt.Sprintf("%s%d", metaGroupPrefix, conv))
		return err == nil && v == "1"
	})
	ch.other.Store(false)
	r.e.mu.Lock()
	r.e.convs[conv].membersKnown = false
	r.e.mu.Unlock()
	ref := r.ch.AddFile("voice.ogg", []byte("OggS..."))
	ref.Kind = channel.FileVoice
	msg := r.ch.PostUser(work, "", ref)
	r.wait("voice annotated after membership repair", func() bool {
		ch.mu.Lock()
		defer ch.mu.Unlock()
		a := ch.annotations[work+"/"+msg.ID]
		return len(a.Clips) == 1 && a.Clips[0].Text == "hello from voice"
	})
	if v, _, err := r.st.Meta(fmt.Sprintf("%s%d", metaGroupPrefix, conv)); err != nil || v != "0" {
		t.Fatalf("stale group flag: %q %v", v, err)
	}
	r.ch.PostUser(work, "ordinary owner request")
	r.wait("ordinary request runs without mention", func() bool { return r.ag.calls("ordinary owner request") == 1 })
	// Current membership overrides an old human sender too. Seeing history
	// cannot permanently put a now owner-only chat back into group mode.
	r.ch.PostCoworker(work, "historical non-owner")
	r.ch.PostUser(work, "next ordinary owner request")
	r.wait("still single-user", func() bool { return r.ag.calls("next ordinary owner request") == 1 })
	r.e.mu.Lock()
	group := r.e.convs[conv].group
	r.e.mu.Unlock()
	if group {
		t.Fatal("historical sender overrode current members")
	}
}

func TestApplicationAndMissingSenderAreNotMembershipEvidence(t *testing.T) {
	r := newRig(t)
	work := r.newWork(t.TempDir())
	conv := convIDForTest(r, work)
	for i, from := range []string{"app:workflow", ""} {
		var evidence bool
		err := r.e.update(func(tx *store.Tx) error {
			c, _, err := r.e.classify(tx, conv, store.KindWork, false, channel.InboundMsg{ID: fmt.Sprint(i), Kind: channel.KindMessage, From: from, Text: "@codex notification", MentionsOwner: true})
			evidence = c.other
			if c.in.Kind != store.InSystem {
				t.Error("application/unknown sender can invoke Codex")
			}
			return err
		})
		if err != nil || evidence {
			t.Fatalf("sender %q counted as another participant: %v", from, err)
		}
	}
	var evidence bool
	if err := r.e.update(func(tx *store.Tx) error {
		c, _, err := r.e.classify(tx, conv, store.KindWork, false, channel.InboundMsg{ID: "human", From: "coworker", Text: "hello"})
		evidence = c.other
		return err
	}); err != nil || !evidence {
		t.Fatal("lost real human fallback")
	}
}

func TestMembershipRefreshIsBoundedAndReversible(t *testing.T) {
	var ch *memberTestChannel
	r := newRig(t, func(c *Config) {
		ch = &memberTestChannel{Fake: c.Channel.(*channeltest.Fake), annotations: map[string]channel.Annotation{}}
		c.Channel = ch
	})
	work := r.newWork(t.TempDir())
	conv := convIDForTest(r, work)
	r.wait("initial membership", func() bool { r.e.mu.Lock(); defer r.e.mu.Unlock(); return r.e.convs[conv].membersKnown })
	before := ch.checks.Load()
	for i := 0; i < 10; i++ {
		w := channel.Window{}
		if err := r.e.currentParticipants(context.Background(), conv, work, &w); err != nil {
			t.Fatal(err)
		}
	}
	if ch.checks.Load() != before {
		t.Fatal("membership requested on every poll")
	}
	// A fresh human sender invalidates the cached owner-only snapshot.
	ch.other.Store(true)
	w := channel.Window{Msgs: []channel.InboundMsg{{ID: "new-human", From: "coworker", Kind: channel.KindMessage}}}
	if err := r.e.currentParticipants(context.Background(), conv, work, &w); err != nil || w.OtherParticipants == nil || !*w.OtherParticipants {
		t.Fatalf("cached single-user snapshot hid a new participant: %v", err)
	}
	if err := r.e.admit(context.Background(), conv, w); err != nil {
		t.Fatal(err)
	}
	before = ch.checks.Load()
	w = channel.Window{Msgs: []channel.InboundMsg{{ID: "another-human-message", From: "coworker", Kind: channel.KindMessage}}}
	if err := r.e.currentParticipants(context.Background(), conv, work, &w); err != nil {
		t.Fatal(err)
	}
	if ch.checks.Load() != before || w.OtherParticipants == nil || !*w.OtherParticipants {
		t.Fatal("known group rechecked for each human message")
	}
	ch.fail.Store(true)
	r.e.mu.Lock()
	r.e.convs[conv].membersChecked = time.Time{}
	r.e.mu.Unlock()
	failedWindow := channel.Window{}
	if err := r.e.currentParticipants(context.Background(), conv, work, &failedWindow); err == nil || failedWindow.OtherParticipants != nil {
		t.Fatal("failed lookup became an owner-only snapshot")
	}
	r.e.mu.Lock()
	stillGroup := r.e.convs[conv].group
	r.e.mu.Unlock()
	if !stillGroup {
		t.Fatal("failed check cleared group flag")
	}
	ch.fail.Store(false)
	for _, other := range []bool{true, false} {
		ch.other.Store(other)
		r.e.mu.Lock()
		r.e.convs[conv].membersChecked = time.Time{}
		r.e.mu.Unlock()
		w := channel.Window{}
		if err := r.e.currentParticipants(context.Background(), conv, work, &w); err != nil {
			t.Fatal(err)
		}
		if w.OtherParticipants == nil || *w.OtherParticipants != other {
			t.Fatal("membership not reversible")
		}
		if err := r.e.admit(context.Background(), conv, w); err != nil {
			t.Fatal(err)
		}
		r.e.mu.Lock()
		got := r.e.convs[conv].group
		r.e.mu.Unlock()
		if got != other {
			t.Fatal("membership not applied")
		}
	}
}

func TestMembershipVoiceEditWaitsForFirstReadAndChecksCurrentGroup(t *testing.T) {
	ch := &memberTestChannel{Fake: channeltest.New(), annotations: map[string]channel.Annotation{}}
	e := &Engine{
		cfg: Config{Channel: ch}, ch: timedChannel{ch}, now: time.Now,
		convs:      map[int64]*convState{1: {group: true}},
		readTokens: newPacer(100, time.Minute, time.Now),
	}
	path := filepath.Join(t.TempDir(), "transcript.txt")
	if err := os.WriteFile(path, []byte("recovered voice"), 0600); err != nil {
		t.Fatal(err)
	}
	// ASR resumes before the chat's first read repairs a stale group flag.
	e.labelVoice(1, store.Inbound{Sender: store.SenderOwner, ExtMsgID: "voice"}, []voiceClip{{name: "clip", path: path}})
	if len(e.edits[1]) != 1 {
		t.Fatal("voice edit lost before membership was read")
	}
	ed := e.edits[1][0]
	if err := ed.run(context.Background(), "chat"); !errors.Is(err, errEditLater) || len(ch.annotations) != 0 {
		t.Fatalf("edit did not wait for first read: %v", err)
	}
	e.convs[1].readOnce, e.convs[1].group = true, false
	if err := ed.run(context.Background(), "chat"); err != nil {
		t.Fatal(err)
	}
	if a := ch.annotations["chat/voice"]; len(a.Clips) != 1 || a.Clips[0].Text != "recovered voice" {
		t.Fatal("voice edit not restored after first read")
	}
	// A person may join after the edit was queued. Check at execution too.
	delete(ch.annotations, "chat/voice")
	e.convs[1].group = true
	if err := ed.run(context.Background(), "chat"); err != nil || len(ch.annotations) != 0 {
		t.Fatalf("voice edit ignored current group state: %v", err)
	}
}
