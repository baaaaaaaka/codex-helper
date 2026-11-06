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
	"sync/atomic"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
	"github.com/baaaaaaaka/codex-helper/internal/core/store"
	"github.com/baaaaaaaka/codex-helper/internal/core/transcript"
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
	FullWalk   time.Duration // a cycle over the older day directories starts at most this often
	// Tiered rollout stat (B1): a rollout that changed within MirrorHotFor,
	// or whose conversation runs a turn, is stat'ed every MirrorStat;
	// other tracked rollouts every MirrorIdleStat, and those of parked
	// conversations every MirrorParkedStat (the bound on a local session
	// waking its parked chat).
	MirrorHotFor, MirrorIdleStat, MirrorParkedStat time.Duration

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
	// R-19: hints after this much Codex silence in a running turn (0
	// disables each). A silent turn is never stopped (F-TRN-18).
	SilenceHint, SilenceStuck time.Duration
	// R-35: reminder cadence for a turn that stays queued (0 disables).
	QueueReminder time.Duration
	// ThrottleFloor is the wait after a throttle response that names no
	// Retry-After (review L9), so the service is not retried every tick.
	ThrottleFloor time.Duration
	// PaceIncrease is the quiet period (no account write throttle) after
	// which the paced write rate grows by one step (pace.go).
	PaceIncrease time.Duration
	// Registry is the machine card heartbeat (registry.go).
	Registry time.Duration
	// UpdateCheck is the automatic release check cadence; UpdateDrain
	// and CodexDrain bound the wait for running turns before a helper
	// update and a Codex CLI upgrade (upgrade.go).
	UpdateCheck, UpdateDrain, CodexDrain time.Duration
}

// ProductionTiming are the CORE-DESIGN values.
func ProductionTiming() Timing {
	return Timing{
		Tick: 250 * time.Millisecond,
		Hot:  time.Second, Warm: 5 * time.Second, Cool: 10 * time.Second, Cold: 30 * time.Second,
		HotFor: 2 * time.Minute, WarmFor: 15 * time.Minute, CoolFor: 4 * time.Hour, ParkAfter: 48 * time.Hour,
		Control: 5 * time.Second, ScanInterval: 5 * time.Second, SafetyRead: 10 * time.Minute, MaxBackoff: 5 * time.Minute,
		MirrorStat: 2 * time.Second, Discovery: 10 * time.Second, FullWalk: 10 * time.Minute,
		MirrorHotFor: 5 * time.Minute, MirrorIdleStat: 30 * time.Second, MirrorParkedStat: time.Minute,
		TurnPoll: 30 * time.Second, CancelWait: 10 * time.Second,
		ResolveDelay: 5 * time.Second, ResolveTries: 3, ResolveAbsent: 2 * time.Minute, CapacityRetry: 5 * time.Minute, CapacityBackoff: 10 * time.Second,
		Heartbeat: 30 * time.Second, Maintenance: time.Minute, ChatRate: time.Second,
		DuplicateFor: 3 * time.Minute, BreakerWindow: time.Minute, BreakerCount: 3,
		SilenceHint: 7 * time.Minute, SilenceStuck: 15 * time.Minute,
		QueueReminder: 10 * time.Minute,
		ThrottleFloor: 10 * time.Second,
		PaceIncrease:  30 * time.Second,
		Registry:      5 * time.Minute,
		UpdateCheck:   30 * time.Minute,
		UpdateDrain:   10 * time.Minute,
		CodexDrain:    30 * time.Minute,
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
	t.MirrorHotFor, t.MirrorIdleStat, t.MirrorParkedStat = 300*base, 15*base, 30*base
	t.TurnPoll = 2 * time.Second
	t.ResolveDelay = 500 * time.Millisecond
	t.ResolveAbsent = 3 * time.Second
	t.Maintenance = 10 * time.Second
	t.CapacityBackoff = time.Second
	t.CancelWait = 5 * time.Second
	t.ThrottleFloor = 10 * base
	t.PaceIncrease = 30 * base
	t.Registry = 20 * base
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
	// Host is the host this helper runs on. When the previous run was on
	// another host (a takeover on a shared home), the control chat gets one
	// notice naming both; TakeoverWhy says how the previous owner ended:
	// "stale" (it stopped answering) or anything else (it stopped).
	Host        string
	TakeoverWhy string

	// SessionsDir is $CODEX_HOME/sessions ("" disables mirroring).
	SessionsDir string
	// FilesDir receives inbound attachments.
	FilesDir string
	// ActivityScan uses the channel's ActivityScanner (one account-wide
	// read per interval) instead of per-chat tier polling for idle chats.
	ActivityScan bool
	// Registry enables the cross-machine delegation inbox.
	Registry bool
	// Delegations decodes and builds inbox records; with Registry set the
	// engine runs the requests addressed to this machine (nil: requests are
	// recorded and never run).
	Delegations Delegations
	// DelegationDir is the working directory of delegated turns ("" = the
	// home directory).
	DelegationDir string
	// InboxKey is the channel key of the inbox chat ("" = derived from the
	// store); a registry publishes it so requesters find the same chat.
	InboxKey string
	// Registrar publishes the machine card (with Registry; nil = none).
	Registrar Registrar
	// Skills runs the `skills` command (nil: not available).
	Skills Skills
	// Updater updates the helper itself: automatic checks every
	// Timing.UpdateCheck and `helper update` (nil: off). UpdatePrerelease
	// includes pre-releases in automatic checks.
	Updater          Updater
	UpdatePrerelease bool
	// CodexUpgrader upgrades the Codex CLI when Codex rejects a request as
	// too old, and on `codex update now` (nil: not available).
	CodexUpgrader CodexUpgrader

	Readers, Senders, Turns int
	// ReadRate caps chat reads per second account-wide (paced like writes, pace.go).
	ReadRate float64
	// WriteRate caps Graph writes per second account-wide (posts, file
	// uploads, chat creations, the registry card): the ceiling of the
	// adaptive write pacing (pace.go). Default DefaultWriteRate.
	WriteRate float64

	finishFault func() error // tests: fails the turn-finish transaction
}

// Notifier delivers out-of-band owner notifications.
type Notifier interface {
	Available() bool
	NotifyFinal(ctx context.Context, conv, title, text, url string) error
}

// EventNotifier is a Notifier that also posts other owner events (a live
// local session found on disk); without it they go to the control chat.
type EventNotifier interface {
	NotifyEvent(ctx context.Context, heading, text, url string) error
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
	// statFile stats a rollout (transcript.Stat; tests count calls).
	statFile func(string) (transcript.FileInfo, error)
	// readMeta reads a rollout's first record (transcript.ReadMeta;
	// tests slow it down).
	readMeta func(string) (transcript.SessionMeta, error)

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

	runnerWake  chan struct{}
	runnerDirty map[int64]bool
	runnerAll   bool
	senderWake  chan struct{}
	// readWake: a conversation's next read moved earlier (receive.go).
	readWake     chan struct{}
	resolverWake chan struct{}
	mirrorWake   chan struct{}
	cmdQueue     chan cmdJob
	cmdMu        sync.Mutex
	cmdPending   map[int64][]cmdJob // per-conversation command queues; present while a worker runs
	asrWake      chan struct{}
	notifyWake   chan struct{}
	scanWake     chan struct{} // a read throttle: the scan waits for the gate (receive.go)
	historyWake  chan struct{} // a resume history check was decided (history_check.go)
	notifyN      atomic.Int64  // pending owner notifications (notify.go)
	// statsCache is the rollout usage cache of `helper stats` (stats.go).
	statsOnce  sync.Once
	statsCache *transcript.UsageCache
	// dash and imp are the control dashboard and the history imports
	// (dashboard.go, import.go; batch A).
	dash dashState
	imp  importState
	// scanFirst: after an account read throttle the activity scan gets
	// the first read when the gate opens (receive.go).
	scanFirst atomic.Bool
	// scanBlind is the read interval of idle active work chats while the
	// activity scan has failed for longer than scanBlindAfter (0: the scan
	// works). Guarded by mu (receive.go scanFallbackLocked).
	scanBlind time.Duration
	// drainFlag holds new turn starts while an update waits for running
	// turns to finish (upgrade.go).
	drainFlag atomic.Bool
	// restartDrain / restartRun: a `helper restart now` drains (its
	// prompts get the restart notice) / its waiter runs (restart.go).
	restartDrain, restartRun atomic.Bool
	// stopping: `shutdown` stopped the helper on all machines; until this
	// process stops it starts no turn, reads no chat and sends only the
	// control chat up to the shutdown reply, whose delivery keys start with
	// stopReply (guarded by mu; shutdown.go).
	stopping  atomic.Bool
	stopReply string
	// writeWaiters counts writers in waitWrite: the sender leaves the
	// account's write tokens to them (sender.go).
	writeWaiters atomic.Int32
	// starting counts turn starts between the drain check and running
	// being set (guarded by mu).
	starting     int
	updateWake   chan struct{}
	registryWake chan struct{}
	fileTries    map[string]int      // transient attachment failures by "<inbound>/<idx>"
	uploadAt     map[int64]time.Time // outbound file rows waiting before an upload retry

	readTokens  *pacer
	writeTokens *pacer
	// liveRun counts the live reads the receiver dispatched in a row while
	// background reads waited (receive.go orderReads). Receiver goroutine
	// only.
	liveRun int
	// sendLast is, per send class (sendClass), the last work conversation
	// the sender served; the next pass starts after it (round robin).
	// Sender goroutine only.
	sendLast [sendClasses]int64

	fatalOnce sync.Once
	fatal     error
	cancel    context.CancelFunc
	wg        sync.WaitGroup

	mirror       *mirrorState
	sendFailures map[int64]int
	requeued     []cmdJob // commands to re-run when the command loop starts
	unknown      map[int64]*unknownState
	gates        map[string]time.Time
	forkThreads  map[string]bool  // forked threads: mirror from the end of the copied history
	delegThreads map[string]bool  // Codex threads of delegated turns: never mirrored
	reminded     map[int64]int    // queued turn id -> queue reminders posted (R-35)
	intro        map[int64]string // first notice of a chat being created (mirror)
	restart      restartConfirm   // confirmation owed for a `helper restart`
	// chatWant are the creating conversations whose chat the sender is
	// to create, with the send class the creation is ranked in (control.go
	// requestChat; under mu).
	chatWant map[int64]int
	// legacy are the previous helper's unconfirmed answers (legacy_check.go),
	// guarded by legacyMu.
	legacyMu sync.Mutex
	legacy   []legacyAnswer
	// orphans are the turns the previous process left starting or
	// running, until reconcile settles them (under mu). None of them runs:
	// Prepare stopped the previous app-servers.
	orphans map[int64]bool
	stats   engineStats
	// Teams edits (edits.go, annotations.go, rename.go): queued chat
	// edits per conversation, the machine label and the unread marks.
	edits  map[int64][]*chatEdit
	titles titleState
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
		cfg.WriteRate = DefaultWriteRate
	}
	e := &Engine{
		cfg: cfg, st: cfg.Store, ch: timedChannel{cfg.Channel}, ag: cfg.Agent, t: cfg.Timing, log: cfg.Log, now: cfg.Now, statFile: transcript.Stat, readMeta: transcript.ReadMeta,
		gRead: store.AccountReadGate(cfg.ChannelName), gWrite: store.AccountWriteGate(cfg.ChannelName),
		convs: map[int64]*convState{}, intro: map[int64]string{}, forkThreads: map[string]bool{}, delegThreads: map[string]bool{}, byChat: map[string]int64{}, byThread: map[string]int64{},
		runnerWake: make(chan struct{}, 1), runnerDirty: map[int64]bool{},
		senderWake: make(chan struct{}, 1), readWake: make(chan struct{}, 1), resolverWake: make(chan struct{}, 1),
		mirrorWake: make(chan struct{}, 1), cmdQueue: make(chan cmdJob, 256), cmdPending: map[int64][]cmdJob{}, reminded: map[int64]int{}, asrWake: make(chan struct{}, 1), notifyWake: make(chan struct{}, 1), scanWake: make(chan struct{}, 1), historyWake: make(chan struct{}, 1), updateWake: make(chan struct{}, 1), registryWake: make(chan struct{}, 1), fileTries: map[string]int{}, uploadAt: map[int64]time.Time{},
	}
	if sc, ok := cfg.Channel.(channel.ActivityScanner); ok && cfg.ActivityScan && e.t.ScanInterval > 0 {
		e.scanner = timedScanner{sc}
	}
	e.readTokens = newPacer(cfg.ReadRate, e.t.PaceIncrease, e.now)
	e.writeTokens = newPacer(cfg.WriteRate, e.t.PaceIncrease, e.now)
	if p, ok := cfg.Registrar.(WritePaced); ok {
		p.PaceWrites(e.waitWrite)
	}
	return e, nil
}

// DefaultWriteRate is the ceiling of the account's write pacing, for every
// Graph write together. The pacer's bucket holds one second of rate, so
// the most writes any 60 s can see is 61 × the rate: 219 at 3.6/s. That
// keeps a full minute of catch-up 8% under the 240 writes per minute
// (4/s) the helper must never exceed, where 4/s with its one-second burst
// would allow 244 (the real-store run measured 243 posts alone, and 303
// writes with the unpaced chat creations).
const DefaultWriteRate = 3.6

// WritePaced is implemented by a Registrar (or any other writer the
// engine hands its writes to) whose Graph writes must share the account's
// write pacing: the engine gives it wait, which blocks until one write may
// go (or ctx ends).
type WritePaced interface {
	PaceWrites(wait func(ctx context.Context) error)
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

func (e *Engine) wakeSender() { wake(e.senderWake) }

// wakeReader: a conversation's next read may be due sooner than the
// receiver loop's sleep.
func (e *Engine) wakeReader() { wake(e.readWake) }

// wakeNotify: an outbox row left the queue; a pending owner notification
// may be due.
func (e *Engine) wakeNotify() {
	if e.notifyN.Load() > 0 {
		wake(e.notifyWake)
	}
}
func (e *Engine) wakeResolver() { wake(e.resolverWake) }

// wakeRunner marks conversations for the runner (none = all candidates).
func (e *Engine) wakeRunner(ids ...int64) {
	e.mu.Lock()
	for _, id := range ids {
		e.runnerDirty[id] = true
	}
	e.mu.Unlock()
	wake(e.runnerWake)
	e.wakeNotify() // the same events (a row left the outbox) can make a notification due
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
	// The turns the previous process left open, read before the runner
	// starts: a held turn this process starts must not be taken for one
	// of them ("did not run" posted while it runs).
	open, err := e.st.OpenTurns()
	if err != nil {
		e.logf("reconcile: %v", err)
	}
	e.mu.Lock()
	e.orphans = map[int64]bool{}
	for _, tr := range open {
		if tr.State != store.TurnQueued {
			e.orphans[tr.ID] = true
		}
	}
	e.mu.Unlock()
	// Before the receiver's first read: a post of one of them must not be
	// taken for the owner's.
	e.loadLegacyAnswers(ctx)
	loops := []func(context.Context){
		e.receiverLoop, e.runnerLoop, e.senderLoop, e.resolverLoop,
		e.commandLoop, e.maintenanceLoop, e.asrLoop, e.notifyLoop,
		e.legacyCheckLoop,
	}
	if e.cfg.SessionsDir != "" {
		loops = append(loops, e.mirrorLoop, e.historyCheckLoop)
	}
	if e.scanner != nil {
		loops = append(loops, e.scanLoop)
	}
	if e.cfg.Registry && e.cfg.Registrar != nil {
		loops = append(loops, e.registryLoop)
	}
	if e.cfg.Updater != nil || e.cfg.CodexUpgrader != nil {
		loops = append(loops, e.updateLoop)
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
		e.reconcile(ctx, open)
	}()
	<-ctx.Done()
	e.wg.Wait()
	if e.fatal == nil || errors.Is(e.fatal, ErrRestart) {
		e.flushEdits(flushEditsMax) // a graceful stop keeps the last labels and titles (edits.go)
	}
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
		if err := tx.SetMeta(metaRunningVersion, e.cfg.Version); err != nil {
			return err
		}
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
	if err := e.renderLegacy(ctx); err != nil {
		return fmt.Errorf("engine: render imported messages: %w", err)
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
	if err := e.loadDelegThreads(); err != nil {
		return err
	}
	if err := e.ensureControl(ctx); err != nil {
		return err
	}
	e.resumeCreating()
	e.resumeFreezes()
	e.requeueCommands()
	e.loadRestartConfirm()
	e.resumeRestart(ctx) // restart.go: a `helper restart now` still waiting
	if err := e.announceUpdate(); err != nil {
		return fmt.Errorf("engine: announce update: %w", err)
	}
	if err := e.announceMigration(); err != nil {
		return fmt.Errorf("engine: announce migration: %w", err)
	}
	if err := e.announceTakeover(); err != nil {
		return fmt.Errorf("engine: announce takeover: %w", err)
	}
	return nil
}

// announceMigration posts the converter's summary to the control chat
// once: it is enqueued and cleared in one transaction.
func (e *Engine) announceMigration() error {
	return e.update(func(tx *store.Tx) error {
		text, _, err := tx.Meta(store.MetaMigrationSummary)
		if err != nil || strings.TrimSpace(text) == "" {
			return err
		}
		if err := e.enqueueNotice(tx, e.controlID, "migration:summary", text); err != nil {
			return err
		}
		return tx.SetMeta(store.MetaMigrationSummary, "")
	})
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
	e.wakeReader() // new, re-activated or re-bound: may be due now
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
