package engine

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"math"
	"sync"
	"time"
)

// Meta keys owned by the engine.
const (
	metaMarkerSelfTest = "engine_marker_selftest" // "preserved" once our marker came back
	metaGroupPrefix    = "engine_group:"          // + conv id: "1" for a chat with other people
	metaNextLabel      = "engine_next_label"      // next sNNN number
	metaMirrorInit     = "engine_mirror_initialized"
	metaRehashed       = "engine_legacy_rehashed"
	metaRestart        = "engine_restart_confirm" // "<conv>:<inbound>:<quote>" of a pending `helper restart`
	metaInbox          = "engine_inbox_conv"      // conversation id of the delegation inbox
	gateAccountRead    = "teams:account-read"
	gateAccountWrite   = "teams:account-write"
)

// Visible labels.
const (
	labelHelper = "🔧 Helper"
	labelCodex  = "Codex"
	labelUser   = "User"
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
	nextRead     time.Time
	reading      bool
	failures     int
	seen         map[string]time.Time // ext ids admitted or classified recently

	// runner
	running *runState
	recent  map[string]recentPrompt // normalized prompt -> last prompt
	lastDup int64                   // inbound id of the last duplicate-ignored prompt
	breaker []time.Time
	capWake time.Time

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
	done      chan struct{}
}

// bucket is a token bucket.
type bucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
	now    func() time.Time
}

func newBucket(rate, burst float64, now func() time.Time) *bucket {
	return &bucket{rate: rate, burst: burst, tokens: burst, last: now(), now: now}
}

// borrow takes one token even when the bucket is empty (down to -burst):
// for the read that serves every chat at once (the activity scan), which
// must not lose to per-chat reads; the debt delays those instead.
func (b *bucket) borrow() {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	b.tokens = math.Min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
	b.last = now
	b.tokens = math.Max(-b.burst, b.tokens-1)
}

// take takes one token; it returns false (and how long to wait) when empty.
func (b *bucket) take() (bool, time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.now()
	b.tokens = math.Min(b.burst, b.tokens+now.Sub(b.last).Seconds()*b.rate)
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	return false, time.Duration((1 - b.tokens) / b.rate * float64(time.Second))
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
