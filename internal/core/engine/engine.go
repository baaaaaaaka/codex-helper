// Package engine is the runtime of the rebuilt helper core (CORE-DESIGN):
// a fixed set of loops (receiver, runner, sender, resolver, mirror,
// commands, maintenance) driven by the store's state. Loops are woken by the
// transactions that create their work; there is no goroutine per
// conversation and no store access across network or Codex I/O.
//
// The engine depends only on internal/core/store and on interfaces: the
// messaging channel (internal/channel) and the Codex Agent.
package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
)

// Timing holds every cadence of the engine.
type Timing struct {
	Tick time.Duration // scheduler resolution

	// Receive tiers (§2): interval by time since the last activity.
	Hot, Warm, Cool, Cold    time.Duration
	HotFor, WarmFor, CoolFor time.Duration
	ParkAfter                time.Duration // cold this long -> park (work chats)
	Control                  time.Duration // control chat read interval (<= 5 s)
	ScanInterval             time.Duration // activity scan cadence (scan mode)
	SafetyRead               time.Duration // direct read of an idle chat in scan mode
	MaxBackoff               time.Duration // failing stream backoff cap

	MirrorStat time.Duration // stat tick for tracked rollouts
	Discovery  time.Duration // discovery walk for new rollouts
	FullWalk   time.Duration // full (non date-pruned) discovery walk

	TurnPoll        time.Duration // Codex turn/completed safety poll
	CancelWait      time.Duration // wait after turn/interrupt before killing
	ResolveDelay    time.Duration // wait before reading back an unknown send
	ResolveTries    int
	ResolveAbsent   time.Duration // read-backs span this long after an ambiguous send before "absent" is authoritative (Graph listing lag)
	CapacityRetry   time.Duration // window from the first capacity error
	CapacityBackoff time.Duration // first capacity retry delay (doubles)
	Heartbeat       time.Duration
	Maintenance     time.Duration // agent reap / vacuum cadence
	ChatRate        time.Duration // min gap between two sends to one chat
	DuplicateFor    time.Duration // duplicate prompt guard
	BreakerWindow   time.Duration
	BreakerCount    int
	// R-19: hints after this much Codex silence in a running turn, and
	// the bound after which the helper stops the turn (0 disables each).
	SilenceHint, SilenceStuck, StallLimit time.Duration
	// R-35: reminder cadence for a turn that stays queued (0 disables).
	QueueReminder time.Duration
}

// ProductionTiming are the CORE-DESIGN values.
func ProductionTiming() Timing {
	return Timing{
		Tick: 250 * time.Millisecond,
		Hot:  time.Second, Warm: 5 * time.Second, Cool: 10 * time.Second, Cold: 30 * time.Second,
		HotFor: 2 * time.Minute, WarmFor: 15 * time.Minute, CoolFor: 4 * time.Hour, ParkAfter: 48 * time.Hour,
		Control: 5 * time.Second, ScanInterval: 5 * time.Second, SafetyRead: 10 * time.Minute, MaxBackoff: 5 * time.Minute,
		MirrorStat: 2 * time.Second, Discovery: 10 * time.Second, FullWalk: 10 * time.Minute,
		TurnPoll: 30 * time.Second, CancelWait: 10 * time.Second,
		ResolveDelay: 5 * time.Second, ResolveTries: 3, ResolveAbsent: 2 * time.Minute, CapacityRetry: 5 * time.Minute, CapacityBackoff: 10 * time.Second,
		Heartbeat: 30 * time.Second, Maintenance: time.Minute, ChatRate: time.Second,
		DuplicateFor: 3 * time.Minute, BreakerWindow: time.Minute, BreakerCount: 3,
		SilenceHint: 7 * time.Minute, SilenceStuck: 15 * time.Minute, StallLimit: time.Hour,
		QueueReminder: 10 * time.Minute,
	}
}

// FastTiming shortens the receive and mirror cadences to base (tests).
func FastTiming(base time.Duration) Timing {
	t := ProductionTiming()
	t.Tick = base / 5
	if t.Tick < 10*time.Millisecond {
		t.Tick = 10 * time.Millisecond
	}
	t.Hot, t.Warm, t.Cool, t.Cold = base, 2*base, 4*base, 8*base
	t.Control, t.ScanInterval = base, base
	t.SafetyRead = 60 * base
	t.MaxBackoff = 30 * base
	t.MirrorStat, t.Discovery, t.FullWalk = base, 2*base, 20*base
	t.TurnPoll = 2 * time.Second
	t.ResolveDelay = 500 * time.Millisecond
	t.ResolveAbsent = 3 * time.Second
	t.Maintenance = 10 * time.Second
	t.CapacityBackoff = time.Second
	t.CancelWait = 5 * time.Second
	return t
}

// Config configures an Engine.
type Config struct {
	Store       *store.Store
	Channel     channel.Channel
	ChannelName string // conversations.channel ("teams")
	Agent       Agent
	Transcriber Transcriber
	// Notifier delivers the owner notification of a final (e.g. a Teams
	// Workflow card). nil (or Available()==false) means @mention instead.
	Notifier Notifier

	Log     io.Writer
	Now     func() time.Time
	Timing  Timing
	Machine string // shown in the control chat title
	Version string

	// SessionsDir is $CODEX_HOME/sessions ("" disables mirroring).
	SessionsDir string
	// FilesDir receives inbound attachments.
	FilesDir string
	// ActivityScan uses the channel's ActivityScanner (one account-wide
	// read per interval) instead of per-chat tier polling for idle chats.
	ActivityScan bool
	// Registry enables the cross-machine delegation inbox.
	Registry bool
	// Skills runs the `skills` command (nil: not available).
	Skills Skills

	Readers, Senders, Turns int
	// ReadRate caps chat reads per second account-wide (token bucket).
	ReadRate float64
	// WriteRate caps sends per second account-wide.
	WriteRate float64
}

// Notifier delivers out-of-band owner notifications.
type Notifier interface {
	Available() bool
	NotifyFinal(ctx context.Context, conv, title, text, url string) error
}

// Engine is a running core.
type Engine struct {
	cfg Config
	st  *store.Store
	ch  channel.Channel
	// gRead and gWrite are the keys of the account-wide gates.
	gRead, gWrite string
	// scanner is set when the channel supports an activity scan (scan
	// mode): idle chats are then read only when the scan shows a change.
	scanner channel.ActivityScanner
	ag      Agent
	t       Timing
	log     io.Writer
	now     func() time.Time

	mu        sync.Mutex
	convs     map[int64]*convState
	byChat    map[string]int64
	byThread  map[string]int64
	prefix    string // turn key prefix
	preserved bool   // markers survive the channel (self-test)
	controlID int64
	// inboxID is the delegation inbox (Config.Registry); set once at start.
	inboxID   int64
	ownerName string

	runnerWake   chan struct{}
	runnerDirty  map[int64]bool
	runnerAll    bool
	senderWake   chan struct{}
	resolverWake chan struct{}
	mirrorWake   chan struct{}
	cmdQueue     chan cmdJob
	cmdMu        sync.Mutex
	cmdPending   map[int64][]cmdJob // per-conversation command queues; present while a worker runs
	asrWake      chan struct{}
	fileTries    map[string]int      // transient attachment failures by "<inbound>/<idx>"
	uploadAt     map[int64]time.Time // outbound file rows waiting before an upload retry

	readTokens  *bucket
	writeTokens *bucket
	// sendLast is the last work conversation the sender served; the next
	// pass starts after it (round robin). Sender goroutine only.
	sendLast int64

	fatalOnce sync.Once
	fatal     error
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	mirror       *mirrorState
	sendFailures map[int64]int
	unknown      map[int64]*unknownState
	gates        map[string]time.Time
	forkThreads  map[string]bool  // forked threads: mirror from the end of the copied history
	reminded     map[int64]int    // queued turn id -> queue reminders posted (R-35)
	intro        map[int64]string // first notice of a chat being created (mirror)
	restart      restartConfirm   // confirmation owed for a `helper restart`
	stats        engineStats
}

type engineStats struct {
	mu        sync.Mutex
	turns     int
	completed int
	reads     int
	sends     int
	started   time.Time
}

// New validates cfg and returns an Engine (not started).
func New(cfg Config) (*Engine, error) {
	if cfg.Store == nil || cfg.Channel == nil || cfg.Agent == nil {
		return nil, errors.New("engine: Store, Channel and Agent are required")
	}
	if cfg.ChannelName == "" {
		cfg.ChannelName = "teams"
	}
	if cfg.Log == nil {
		cfg.Log = io.Discard
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	if cfg.Timing.Tick == 0 {
		cfg.Timing = ProductionTiming()
	}
	if cfg.Readers <= 0 {
		cfg.Readers = 8
	}
	if cfg.Senders <= 0 {
		cfg.Senders = 8
	}
	if cfg.Turns <= 0 {
		cfg.Turns = 8
	}
	if cfg.ReadRate <= 0 {
		cfg.ReadRate = 4
	}
	if cfg.WriteRate <= 0 {
		cfg.WriteRate = 4
	}
	e := &Engine{
		cfg: cfg, st: cfg.Store, ch: timedChannel{cfg.Channel}, ag: cfg.Agent, t: cfg.Timing, log: cfg.Log, now: cfg.Now,
		gRead: store.AccountReadGate(cfg.ChannelName), gWrite: store.AccountWriteGate(cfg.ChannelName),
		convs: map[int64]*convState{}, intro: map[int64]string{}, forkThreads: map[string]bool{}, byChat: map[string]int64{}, byThread: map[string]int64{},
		runnerWake: make(chan struct{}, 1), runnerDirty: map[int64]bool{},
		senderWake: make(chan struct{}, 1), resolverWake: make(chan struct{}, 1),
		mirrorWake: make(chan struct{}, 1), cmdQueue: make(chan cmdJob, 256), cmdPending: map[int64][]cmdJob{}, reminded: map[int64]int{}, asrWake: make(chan struct{}, 1), fileTries: map[string]int{}, uploadAt: map[int64]time.Time{},
	}
	if sc, ok := cfg.Channel.(channel.ActivityScanner); ok && cfg.ActivityScan && e.t.ScanInterval > 0 {
		e.scanner = timedScanner{sc}
	}
	e.readTokens = newBucket(cfg.ReadRate, cfg.ReadRate*2, e.now)
	e.writeTokens = newBucket(cfg.WriteRate, cfg.WriteRate*2, e.now)
	return e, nil
}

func (e *Engine) logf(format string, args ...any) {
	fmt.Fprintf(e.log, "%s engine: %s\n", e.now().Format("15:04:05.000"), fmt.Sprintf(format, args...))
}

// fail records a fatal error and stops the engine.
func (e *Engine) fail(err error) {
	e.fatalOnce.Do(func() {
		e.fatal = err
		e.logf("fatal: %v", err)
		if e.cancel != nil {
			e.cancel()
		}
	})
}

// update runs fn in a store write transaction; ErrFenced stops the engine.
func (e *Engine) update(fn func(*store.Tx) error) error {
	err := e.st.Update(fn)
	if errors.Is(err, store.ErrFenced) || errors.Is(err, store.ErrNotAcquired) {
		e.fail(err)
	}
	return err
}

func wake(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (e *Engine) wakeSender()   { wake(e.senderWake) }
func (e *Engine) wakeResolver() { wake(e.resolverWake) }

// wakeRunner marks conversations for the runner (none = all candidates).
func (e *Engine) wakeRunner(ids ...int64) {
	e.mu.Lock()
	for _, id := range ids {
		e.runnerDirty[id] = true
	}
	e.mu.Unlock()
	wake(e.runnerWake)
}

// Run starts the engine and blocks until ctx ends or a fatal error. The
// caller must have acquired the store.
func (e *Engine) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	e.cancel = cancel
	defer cancel()
	e.stats.started = e.now()
	if err := e.start(ctx); err != nil {
		e.ag.Close()
		return err
	}
	loops := []func(context.Context){
		e.receiverLoop, e.runnerLoop, e.senderLoop, e.resolverLoop,
		e.commandLoop, e.maintenanceLoop, e.asrLoop,
	}
	if e.cfg.SessionsDir != "" {
		loops = append(loops, e.mirrorLoop)
	}
	if e.scanner != nil {
		loops = append(loops, e.scanLoop)
	}
	for _, l := range loops {
		e.wg.Add(1)
		go func(l func(context.Context)) {
			defer e.wg.Done()
			l(ctx)
		}(l)
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.reconcile(ctx)
	}()
	<-ctx.Done()
	e.wg.Wait()
	e.ag.Close()
	if e.fatal != nil {
		return e.fatal
	}
	return ctx.Err()
}

// start loads state and makes sure the control chat exists.
func (e *Engine) start(ctx context.Context) error {
	if err := e.ag.Prepare(ctx); err != nil {
		// A leftover app-server that would not die is not fatal: Codex
		// refuses a second writer on the same thread, which surfaces as the
		// active-writer notice on that turn.
		e.logf("engine: prepare agent: %v", err)
	}
	if err := e.update(func(tx *store.Tx) error {
		p, ok, err := tx.Meta(store.MetaTurnKeyPrefix)
		if err != nil {
			return err
		}
		if !ok {
			p = "cxp-" + randHex(4)
			if err := tx.SetMeta(store.MetaTurnKeyPrefix, p); err != nil {
				return err
			}
		}
		e.prefix = p
		v, _, err := tx.Meta(metaMarkerSelfTest)
		e.preserved = v == "preserved"
		return err
	}); err != nil {
		return err
	}
	if err := e.load(); err != nil {
		return err
	}
	if err := e.loadGates(); err != nil {
		return err
	}
	if err := e.rehashLegacy(); err != nil {
		return fmt.Errorf("engine: re-hash legacy outbox: %w", err)
	}
	// Rows that were being sent when the previous process died have an
	// unknown outcome.
	var unknown int
	if err := e.update(func(tx *store.Tx) error {
		for id := range e.convs {
			rows, err := tx.PendingOutbox(id)
			if err != nil {
				return err
			}
			for _, r := range rows {
				if r.State == store.OutSending {
					if err := tx.MarkUnknown(r.ID, "helper stopped during send"); err != nil {
						return err
					}
					unknown++
				}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	if unknown > 0 {
		e.logf("%d sends in flight at the last stop are unknown; resolving by read-back", unknown)
	}
	if err := e.ensureInbox(); err != nil {
		return err
	}
	if err := e.ensureControl(ctx); err != nil {
		return err
	}
	e.resumeCreating(ctx)
	e.requeueCommands()
	e.loadRestartConfirm()
	return nil
}

// load reads conversations, threads and streams into memory.
func (e *Engine) load() error {
	convs, err := e.st.Conversations()
	if err != nil {
		return err
	}
	streams, err := e.st.Streams()
	if err != nil {
		return err
	}
	threads, err := e.st.Threads()
	if err != nil {
		return err
	}
	// One stream per conversation, under store.ChatStreamKey. A row under
	// another key (an older engine wrote "chat:<id>") is used only when the
	// canonical row is missing; the cursor then moves to the canonical key
	// with the next admission.
	byConv := map[int64][]store.Stream{}
	for _, s := range streams {
		byConv[s.ConvID] = append(byConv[s.ConvID], s)
	}
	pick := func(c store.Conversation) (store.Stream, bool) {
		rows := byConv[c.ID]
		want := store.ChatStreamKey(e.cfg.ChannelName, c.ExtChatID)
		var best store.Stream
		found := false
		for _, s := range rows {
			if s.Key == want {
				return s, true
			}
			if !found || s.UpdatedAt > best.UpdatedAt {
				best, found = s, true
			}
		}
		if len(rows) > 0 {
			e.logf("conversation %s has no stream %q; using %q", c.Label, want, best.Key)
		}
		return best, found
	}
	now := e.now()
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, c := range convs {
		cs := e.trackLocked(c)
		if s, ok := pick(c); ok {
			cs.cursor, cs.seeded = s.Cursor, s.Seeded
			if s.UpdatedAt > 0 {
				cs.lastActivity = time.UnixMilli(s.UpdatedAt)
			}
		}
		if cs.lastActivity.IsZero() {
			cs.lastActivity = time.UnixMilli(c.UpdatedAt)
		}
		// Read every live chat once soon after start (catch-up).
		cs.nextRead = now
		if c.Kind == store.KindControl && c.State != store.ConvClosed {
			e.controlID = c.ID
		}
	}
	for _, th := range threads {
		if cs := e.convs[th.ConvID]; cs != nil {
			cs.cwd = th.Cwd
			cs.threadID = th.ThreadID
			cs.forkParent = th.ForkParent
			if th.ThreadID != "" {
				e.byThread[th.ThreadID] = th.ConvID
			}
		}
	}
	for id, cs := range e.convs {
		if v, ok, _ := e.st.Meta(metaGroupPrefix + fmt.Sprint(id)); ok && v == "1" {
			cs.group = true
		}
	}
	return nil
}

// trackLocked creates or refreshes the in-memory state of a conversation.
func (e *Engine) trackLocked(c store.Conversation) *convState {
	cs := e.convs[c.ID]
	if cs == nil {
		cs = &convState{id: c.ID, recent: map[string]recentPrompt{}, seen: map[string]time.Time{}, created: time.UnixMilli(c.CreatedAt)}
		e.convs[c.ID] = cs
	}
	if cs.chat != "" && cs.chat != c.ExtChatID {
		delete(e.byChat, cs.chat)
	}
	cs.label, cs.kind, cs.state, cs.chat, cs.url, cs.title = c.Label, c.Kind, c.State, c.ExtChatID, c.ExtURL, c.Title
	cs.paused = c.PausedAt != 0
	if c.ExtChatID != "" {
		e.byChat[c.ExtChatID] = c.ID
	}
	return cs
}

// refresh re-reads one conversation into memory after a transaction
// changed it.
func (e *Engine) refresh(tx *store.Tx, id int64) (store.Conversation, error) {
	c, ok, err := tx.Conversation(id)
	if err != nil || !ok {
		return c, err
	}
	e.mu.Lock()
	e.trackLocked(c)
	e.mu.Unlock()
	return c, nil
}

func (e *Engine) conv(id int64) *convState {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.convs[id]
}

// Stats is a snapshot for `helper status` and tests. Conversations counts
// work sessions only (not the control chat or the delegation inbox).
type Stats struct {
	Conversations, Active, Parked, Running int
	Turns, Completed, Reads, Sends         int
	Uptime                                 time.Duration
}

// Snapshot returns current counters.
func (e *Engine) Snapshot() Stats {
	e.mu.Lock()
	var s Stats
	for _, c := range e.convs {
		if c.id == e.controlID || c.id == e.inboxID {
			continue // not sessions
		}
		s.Conversations++
		switch c.state {
		case store.ConvActive:
			s.Active++
		case store.ConvParked:
			s.Parked++
		}
		if c.running != nil {
			s.Running++
		}
	}
	e.mu.Unlock()
	e.stats.mu.Lock()
	s.Turns, s.Completed, s.Reads, s.Sends = e.stats.turns, e.stats.completed, e.stats.reads, e.stats.sends
	s.Uptime = e.now().Sub(e.stats.started)
	e.stats.mu.Unlock()
	return s
}

func (e *Engine) count(f func(*engineStats)) {
	e.stats.mu.Lock()
	f(&e.stats)
	e.stats.mu.Unlock()
}

func normalize(s string) string { return strings.ToLower(strings.Join(strings.Fields(s), " ")) }
