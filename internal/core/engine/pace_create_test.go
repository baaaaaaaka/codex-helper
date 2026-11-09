package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/channel/channeltest"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// writeLog records the time of every channel write (post, chat creation,
// upload): what the account's Graph write budget counts.
type writeLog struct {
	*channeltest.Fake
	mu      sync.Mutex
	at      []time.Time
	creates []string // titles, in order
	posts   []string
	// received is configured before the rig starts, when a test needs to
	// distinguish delivery to the helper from posting to the fake server.
	received func(channel.Window)
}

func (w *writeLog) Receive(ctx context.Context, chat, cursor string) (channel.Window, error) {
	window, err := w.Fake.Receive(ctx, chat, cursor)
	if err == nil && w.received != nil {
		w.received(window)
	}
	return window, err
}

func (w *writeLog) note(create bool, body string) {
	w.mu.Lock()
	w.at = append(w.at, time.Now())
	if create {
		w.creates = append(w.creates, body)
	} else if body != "" {
		w.posts = append(w.posts, body)
	}
	w.mu.Unlock()
}

func (w *writeLog) Send(ctx context.Context, chat string, p channel.Part, token string) (channel.Sent, error) {
	w.note(false, p.Body)
	return w.Fake.Send(ctx, chat, p, token)
}

func (w *writeLog) CreateChat(ctx context.Context, title, key string) (channel.ChatInfo, error) {
	w.note(true, title)
	return w.Fake.CreateChat(ctx, title, key)
}

func (w *writeLog) UploadFile(ctx context.Context, src, name string, sess *channel.UploadSession, save func(channel.UploadSession) error) (channel.FileRef, error) {
	w.note(false, "")
	return w.Fake.UploadFile(ctx, src, name, sess, save)
}

func (w *writeLog) count(sub string) (posts, creates int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, b := range w.posts {
		if strings.Contains(b, sub) {
			posts++
		}
	}
	return posts, len(w.creates)
}

// createdBefore is the number of chats created before the first one whose
// title contains sub (-1 when there is none).
func (w *writeLog) createdBefore(sub string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i, title := range w.creates {
		if strings.Contains(title, sub) {
			return i
		}
	}
	return -1
}

// maxIn is the most writes in any window of length d.
func (w *writeLog) maxIn(d time.Duration) int {
	w.mu.Lock()
	at := append([]time.Time(nil), w.at...)
	w.mu.Unlock()
	sort.Slice(at, func(i, j int) bool { return at[i].Before(at[j]) })
	best, j := 0, 0
	for i := range at {
		for at[i].Sub(at[j]) >= d {
			j++
		}
		if n := i - j + 1; n > best {
			best = n
		}
	}
	return best
}

// creatingRig is backlogRig plus creating conversations: local sessions
// found on disk whose catch-up chats do not exist yet, each with mirror
// rows queued for its chat-to-be.
func creatingRig(t *testing.T, tweak func(*Config), active, creating, rows int) (*rig, *writeLog) {
	t.Helper()
	r := &rig{t: t, ch: channeltest.New(), ag: newAgent(), path: filepath.Join(t.TempDir(), "core.db"), log: &strings.Builder{}, logMu: &sync.Mutex{}}
	w := &writeLog{Fake: r.ch}
	r.tweak = func(c *Config) {
		c.Channel = w
		c.Timing.ChatRate = time.Millisecond
		if tweak != nil {
			tweak(c)
		}
	}
	t.Cleanup(r.stop)
	ctx := context.Background()
	var chats []string
	for i := 0; i < active; i++ {
		ci, _ := r.ch.CreateChat(ctx, "w", fmt.Sprintf("k%02d", i))
		chats = append(chats, ci.ID)
	}
	st, err := store.Open(r.path)
	if err != nil {
		t.Fatal(err)
	}
	err = st.Import(func(im *store.Importer) error {
		for i := 0; i < active+creating; i++ {
			c := store.Conversation{Label: fmt.Sprintf("s%03d", i+1), Title: fmt.Sprintf("catch-up %d", i), Kind: store.KindWork, Channel: "teams", State: store.ConvCreating}
			if i < active {
				c.ExtChatID, c.State = chats[i], store.ConvActive
			}
			id, err := im.Conversation(c)
			if err != nil {
				return err
			}
			if i >= active {
				// A discovered session whose records are queued (mirror.go).
				if err := im.SetMeta(discoveredKey(id), discoveredReady); err != nil {
					return err
				}
			}
			if i < active {
				if err := im.Stream(store.Stream{Key: store.ChatStreamKey("teams", chats[i]), ConvID: id, Cursor: "0", Seeded: true}); err != nil {
					return err
				}
			}
			for k := 0; k < rows; k++ {
				key := fmt.Sprintf("bl:%d:%03d", id, k)
				if _, _, err := im.Outbox(store.Outbox{ConvID: id, DeliveryKey: key, Seq: int64(k + 1), Kind: store.OutMirror, Body: "catch-up backlog " + key, State: store.OutQueued, CreatedAt: im.Now()}, nil); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	st.Close()
	r.start()
	return r, w
}

// Real-store acceptance (2026-11-05): a first start on a migrated store
// made 303 Graph writes in 60 s, 243 posts plus 60 chat creations; the
// creations went around the account's write pacer. Every write, a chat
// creation included, now takes a token from the one pacer, so the writes
// in any window d stay within the bucket plus rate×d.
func TestEveryWriteIsPacedIncludingChatCreation(t *testing.T) {
	const rate, active, creating, rows = 20.0, 10, 40, 2
	r, w := creatingRig(t, func(c *Config) { c.WriteRate = rate }, active, creating, rows)
	want := (active + creating) * rows
	r.waitFor("catch-up delivered", 30*time.Second, func() bool {
		n, c := w.count("catch-up backlog")
		return n == want && c >= creating
	})
	for _, d := range []time.Duration{100 * time.Millisecond, 250 * time.Millisecond, time.Second, 2 * time.Second} {
		limit := int(rate) + int(rate*d.Seconds()) + 2 // bucket + refill, +2 for edges
		n := w.maxIn(d)
		t.Logf("max %d writes in %v (limit %d)", n, d, limit)
		if n > limit {
			t.Errorf("%d writes in %v, want at most %d at %.0f/s (bucket %.0f)", n, d, limit, rate, rate)
		}
	}
}

// Catch-up chat creations wait behind what the owner waits for: the chat
// of `new` is created with the next write token although 40 catch-up chats
// (at 4 writes/s) are still to be created, and a turn in it is answered
// while most of the catch-up waits. (Reads are not the bottleneck here.)
func TestRepliesBeforeCatchUpChatCreation(t *testing.T) {
	skipWallClockPacingUnderRace(t)
	const creating = 40
	dir := filepath.Join(t.TempDir(), "livework")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	var before atomic.Int64
	before.Store(-1)
	r, w := creatingRig(t, func(c *Config) {
		c.WriteRate, c.ReadRate = 4, 1000
		log := c.Channel.(*writeLog)
		log.received = func(window channel.Window) {
			for _, m := range window.Msgs {
				if m.ByOwner && m.Text == "new "+dir {
					_, n := log.count("")
					before.CompareAndSwap(-1, int64(n))
				}
			}
		}
	}, 0, creating, 1)
	ctl := r.control()
	r.ch.PostUser(ctl, "new "+dir)
	r.waitFor("work chat", 20*time.Second, func() bool { return w.createdBefore("livework") >= 0 })
	// Count from delivery to the helper: it cannot prioritize a command
	// still waiting on the fake server. Allow the writes already in flight.
	if n := int64(w.createdBefore("livework")) - before.Load(); before.Load() < 0 || n > 3 {
		t.Fatalf("%d catch-up chats were created after receiving `new` and before its chat (read baseline %d)", n, before.Load())
	}
	r.wait("work chat created", func() bool { return len(r.own(ctl, "Work chat created")) == 1 })
	var work string
	convs, err := r.st.Conversations()
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range convs {
		if strings.HasSuffix(c.Title, "livework") {
			work = c.ExtChatID
		}
	}
	r.ch.PostUser(work, "live turn")
	r.waitFor("answer", 20*time.Second, func() bool { return len(r.own(work, "ANSWER to live turn")) == 1 })
	if n, _ := w.count("catch-up backlog"); n > creating/2 {
		t.Fatalf("%d of %d catch-up messages went before the live answer", n, creating)
	}
}

// The default write ceiling keeps any 60 s under 240 Graph writes with a
// margin, even when the demand takes every token: the bucket holds one
// second of rate, so a minute admits 61 × the rate (4/s admitted 244; the
// real-store run measured 243 posts in 60 s).
func TestDefaultWriteRateKeepsAMinuteUnder240(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "core.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	clk := &testClock{t: time.Unix(1000, 0)}
	e, err := New(Config{Store: st, Channel: channeltest.New(), Agent: newAgent(), Now: clk.now})
	if err != nil {
		t.Fatal(err)
	}
	var at []time.Time
	for end := clk.now().Add(5 * time.Minute); clk.now().Before(end); clk.add(10 * time.Millisecond) {
		for {
			if ok, _ := e.writeTokens.take(); !ok {
				break
			}
			at = append(at, clk.now())
		}
	}
	best, j := 0, 0
	for i := range at {
		for at[i].Sub(at[j]) >= time.Minute {
			j++
		}
		best = max(best, i-j+1)
	}
	if limit := 240 * 92 / 100; best > limit {
		t.Fatalf("%d writes in a minute at the default write rate %.1f/s, want at most %d (8%% under 240)", best, e.cfg.WriteRate, limit)
	}
	t.Logf("at most %d writes in any 60 s at %.1f/s", best, e.cfg.WriteRate)
}

// `new` commands sent at once are answered in their order, as by the old
// helper: each creates its chat in the command (e2e IDLE: chats created by
// the sender in parallel announced s002 and s003 before s001).
func TestNewChatsAnnouncedInCommandOrder(t *testing.T) {
	const n = 6
	// The first work chat takes longer to create (Graph latency varies).
	slow := &slowCreate{delay: []time.Duration{0, 2 * time.Second}}
	r := newRig(t, func(c *Config) {
		c.WriteRate = 3
		slow.Fake = c.Channel.(*channeltest.Fake)
		c.Channel = slow
	})
	ctl := r.control()
	for i := 0; i < n; i++ {
		r.ch.PostUser(ctl, "new "+t.TempDir())
	}
	r.waitFor("work chats", 30*time.Second, func() bool { return len(r.own(ctl, "Work chat created")) == n })
	for i, m := range r.own(ctl, "Work chat created") {
		if want := fmt.Sprintf("Work chat created: s%03d", i+1); !strings.Contains(m.Text, want) {
			t.Fatalf("notice %d is %q, want %q", i+1, m.Text, want)
		}
	}
}

// slowCreate delays the i-th chat creation by delay[i].
type slowCreate struct {
	*channeltest.Fake
	mu    sync.Mutex
	n     int
	delay []time.Duration
}

func (c *slowCreate) CreateChat(ctx context.Context, title, key string) (channel.ChatInfo, error) {
	c.mu.Lock()
	var d time.Duration
	if c.n < len(c.delay) {
		d = c.delay[c.n]
	}
	c.n++
	c.mu.Unlock()
	time.Sleep(d)
	return c.Fake.CreateChat(ctx, title, key)
}
