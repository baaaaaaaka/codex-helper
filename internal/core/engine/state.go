package engine

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"
)

// Meta keys owned by the engine.
const (
	metaMarkerSelfTest = "engine_marker_selftest" // "preserved" once our marker came back
	metaGroupPrefix    = "engine_group:"          // + conv id: "1" for a chat with other people
	metaNextLabel      = "engine_next_label"      // next sNNN number
	metaMirrorInit     = "engine_mirror_initialized"
	metaRealignPrefix  = "engine_realign:" // + source id: unix ms of the realign point while a rewritten rollout is re-read
	metaRehashed       = "engine_legacy_rehashed"
	metaRestart        = "engine_restart_confirm" // "<conv>:<inbound>:<quote>" of a pending `helper restart`
	metaInbox          = "engine_inbox_conv"      // conversation id of the delegation inbox
	// metaDiscoveredPrefix + conversation id: a work conversation the
	// mirror created for a local session discovered on disk, until its
	// chat is created (no `new` notices, no control-chat notice):
	// discoveredPending while the session has nothing to mirror (no chat
	// yet, as v0.1.20 skipped empty sessions), discoveredReady once its
	// notice and first records are queued.
	metaDiscoveredPrefix = "engine_discovered:"
	discoveredPending    = "pending"
	discoveredReady      = "ready"
)

// Visible labels, as v0.1.20 posted them (fidelity run): helper notices,
// local prompts, Codex answers, and Codex status (progress, commentary,
// compaction, and turn outcomes other than a cancel).
const (
	labelHelper = "🔧 Helper"
	labelUser   = "🧑‍💻 User"
	labelAnswer = "🤖 ✅ Codex answer"
	labelStatus = "🤖 ⏳ Codex status"
)

// convState is the in-memory state of one conversation. It is guarded by
// Engine.mu and is a cache of the store plus scheduling data that is never
// persisted.
type convState struct {
	id                                   int64
	label, kind, state, chat, url, title string
	paused, group                        bool
	cwd, threadID                        string
	forkParent                           int64
	created                              time.Time

	// receive
	cursor       string
	seeded       bool
	lastActivity time.Time // any activity (parking)
	lastUser     time.Time // owner activity: prompt, command, turn end, wake (scan mode hot tier)
	scanLast     string    // newest message id the activity scan reported
	// scanDue: the activity scan showed a change not read yet; such a
	// read goes before the timed polls. scanGen counts the scan's marks,
	// so a read that was already running when the scan marked the chat
	// does not clear it.
	scanDue bool
	scanGen uint64
	// scanSince is when the scan first marked the change not read yet:
	// flagged chats are read oldest flag first (S7b), so one whose read
	// drew a throttle is not overtaken by newer flags.
	scanSince time.Time
	// scanLive is the scan mark (scanGen) of the newest change whose
	// newest message is not one this helper sent (0: none unread): a
	// message of the owner or someone else, read at live priority. A
	// change that shows only the helper's own post is read back in the
	// background.
	scanLive uint64
	// ownPosts are the ids of the messages this helper sent last (S7): a
	// scan whose newest message is one of them shows no new message.
	ownPosts []string
	nextRead time.Time
	// safeAt is the safety read nextReadLocked scheduled (scan mode, idle
	// chat): a read that starts with nextRead still at it, and no scan
	// mark, is that safety read.
	safeAt   time.Time
	reading  bool
	failures int
	seen     map[string]time.Time // ext ids admitted or classified recently

	// runner
	running *runState
	// lastTurnEnd is when a started turn of this conversation last ended:
	// a queued turn admitted before it waited behind it (R-35).
	lastTurnEnd time.Time
	// batchThrough is the last turn named by a "starting your N queued
	// requests" notice: the turns up to it get no notice of their own.
	batchThrough int64
	recent       map[string]recentPrompt // normalized prompt -> last prompt
	lastDup      int64                   // inbound id of the last duplicate-ignored prompt
	breaker      []time.Time
	capWake      time.Time

	// sender
	sending  bool
	lastSend time.Time
}

type recentPrompt struct {
	at      time.Time
	inbound int64
}

// runState is a turn being executed.
type runState struct {
	turnID    int64
	turnKey   string
	cancelled bool
	// stalled: stopped by the stall bound (R-19); under Engine.mu.
	stalled bool
	// lastEvent is the unix ms of the last Codex event (silence tracking).
	lastEvent atomic.Int64
	done      chan struct{}
	// request is the delegation request text of a delegated turn ("" for
	// an ordinary turn).
	request string
	// queuedPrompt is the quote of a prompt that waited behind an earlier
	// turn: it heads the turn's first message (R-35, folded).
	queuedPrompt string
}

func randHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func ms(t time.Time) int64 { return t.UnixMilli() }

func fromMS(v int64) time.Time {
	if v == 0 {
		return time.Time{}
	}
	return time.UnixMilli(v)
}

func plural(n int, one, many string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, one)
	}
	return fmt.Sprintf("%d %s", n, many)
}
