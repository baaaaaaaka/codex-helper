package codex

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"
)

// DefaultIdle is how long an idle app-server is kept warm.
const DefaultIdle = 10 * time.Minute

// PoolConfig configures a Pool.
type PoolConfig struct {
	// Idle is how long a server without leases stays warm (DefaultIdle).
	Idle time.Duration
	// PIDFile records the process groups of live servers ("" = none).
	PIDFile string
	Log     io.Writer
	Now     func() time.Time
}

// Pool keeps one app-server per key (conversation), warm while in use and
// for Idle afterwards.
type Pool struct {
	cfg PoolConfig

	mu      sync.Mutex
	servers map[string]*entry
	closed  bool

	// pidMu serializes PID file writes from snapshot to rename, so an
	// older snapshot can never replace a newer one (review M3).
	pidMu sync.Mutex
}

type entry struct {
	key    string
	srv    *Server
	start  string // process start time (pid reuse guard)
	loaded map[string]bool
	leases int
	last   time.Time
	ready  chan struct{} // closed when srv/err is set
	err    error
	stamp  string // ConfigFile content before the server started
}

// NewPool returns an empty pool.
func NewPool(cfg PoolConfig) *Pool {
	if cfg.Idle <= 0 {
		cfg.Idle = DefaultIdle
	}
	if cfg.Log == nil {
		cfg.Log = io.Discard
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	return &Pool{cfg: cfg, servers: map[string]*entry{}}
}

// Lease is a hold on a pool server; it stays warm until Release (+Idle).
type Lease struct {
	p    *Pool
	e    *entry
	once sync.Once
}

// Server returns the leased server.
func (l *Lease) Server() *Server { return l.e.srv }

// Release ends the lease.
func (l *Lease) Release() {
	l.once.Do(func() {
		l.p.mu.Lock()
		l.e.leases--
		l.e.last = l.p.cfg.Now()
		l.p.mu.Unlock()
	})
}

// Load makes threadID loaded in the leased server (thread/resume once per
// server process).
func (l *Lease) Load(ctx context.Context, threadID string) error {
	l.p.mu.Lock()
	ok := l.e.loaded[threadID]
	l.p.mu.Unlock()
	if ok {
		return nil
	}
	if _, err := l.e.srv.ThreadResume(ctx, threadID); err != nil {
		return err
	}
	l.Loaded(threadID)
	return nil
}

// Loaded records that threadID is loaded (after thread/start or fork).
func (l *Lease) Loaded(threadID string) {
	l.p.mu.Lock()
	l.e.loaded[threadID] = true
	l.p.mu.Unlock()
}

// ErrPoolClosed is returned by Acquire after Close.
var ErrPoolClosed = errors.New("codex: pool closed")

// Acquire returns a lease on the server of key, starting it with cfg when
// there is none (or it exited).
func (p *Pool) Acquire(ctx context.Context, key string, cfg Config) (*Lease, error) {
	for {
		p.mu.Lock()
		if p.closed {
			p.mu.Unlock()
			return nil, ErrPoolClosed
		}
		e := p.servers[key]
		if e != nil {
			select {
			case <-e.ready:
				if e.err != nil || e.srv.Exited() {
					delete(p.servers, key)
					p.mu.Unlock()
					p.writePIDs()
					continue
				}
				e.leases++
				e.last = p.cfg.Now()
				p.mu.Unlock()
				return &Lease{p: p, e: e}, nil
			default:
				p.mu.Unlock()
				select {
				case <-e.ready:
					continue
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
		}
		e = &entry{key: key, loaded: map[string]bool{}, ready: make(chan struct{}), leases: 1, stamp: configStamp(cfg.ConfigFile)}
		p.servers[key] = e
		p.mu.Unlock()

		srv, err := Start(ctx, cfg)
		p.mu.Lock()
		if err != nil {
			e.err = err
			delete(p.servers, key)
		} else {
			e.srv = srv
			e.start = startTime(srv.PID())
			e.last = p.cfg.Now()
		}
		closed := p.closed
		close(e.ready)
		p.mu.Unlock()
		if err != nil {
			return nil, err
		}
		p.writePIDs()
		if closed {
			srv.Kill()
			return nil, ErrPoolClosed
		}
		return &Lease{p: p, e: e}, nil
	}
}

// AcquireCurrent is Acquire at the start of a turn: Codex reads its
// config file (MCP servers, providers, features) only when the app-server
// starts, so a warm server of key started before cfg.ConfigFile changed
// is closed first (stdin EOF, waiting for it to exit, so no two processes
// hold the thread) and the turn runs on a fresh one, with no helper
// restart (SPEC R-24, F-TRN-12; rc.15 58c6bc0). It never interrupts work:
// a server with a lease (a running turn, fork or lookup) is kept and the
// next turn start replaces it. Nothing polls: the file is read here only.
func (p *Pool) AcquireCurrent(ctx context.Context, key string, cfg Config) (*Lease, error) {
	if cfg.ConfigFile != "" {
		stamp := configStamp(cfg.ConfigFile)
		var stale *entry
		p.mu.Lock()
		if e := p.servers[key]; e != nil && e.leases == 0 && e.stamp != stamp {
			select {
			case <-e.ready:
				if e.srv != nil && !e.srv.Running() { // never under a running turn
					delete(p.servers, key)
					stale = e
				}
			default:
			}
		}
		p.mu.Unlock()
		if stale != nil {
			fmt.Fprintf(p.cfg.Log, "codex: %s changed; restarting app-server %s (pid %d) for the next turn\n", cfg.ConfigFile, key, stale.srv.PID())
			stale.srv.Close(5 * time.Second)
			p.writePIDs()
		}
	}
	return p.Acquire(ctx, key, cfg)
}

// configStamp identifies the content of path ("" for no file to watch).
func configStamp(path string) string {
	if path == "" {
		return ""
	}
	data, err := os.ReadFile(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return "absent"
	case err != nil:
		return "unreadable: " + err.Error()
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// Busy reports whether key has a live server.
func (p *Pool) Busy(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.servers[key]
	return e != nil && e.srv != nil && !e.srv.Exited()
}

// Peek returns the live server of key without leasing it (nil: none). It
// is for reading what the server knows and for calls that do not need the
// server kept (an interrupt).
func (p *Pool) Peek(key string) *Server {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.servers[key]
	if e == nil || e.srv == nil || e.srv.Exited() {
		return nil
	}
	return e.srv
}

// Drop kills the server of key now (e.g. after it misbehaved).
func (p *Pool) Drop(key string) {
	p.mu.Lock()
	e := p.servers[key]
	if e != nil && e.srv != nil {
		delete(p.servers, key)
	} else {
		e = nil
	}
	p.mu.Unlock()
	if e != nil {
		e.srv.Kill()
		p.writePIDs()
	}
}

// Reap closes servers that exited or idled past Idle; it returns how many
// were removed.
func (p *Pool) Reap() int {
	now := p.cfg.Now()
	var victims []*entry
	p.mu.Lock()
	for k, e := range p.servers {
		select {
		case <-e.ready:
		default:
			continue
		}
		if e.srv == nil {
			continue
		}
		if e.leases == 0 && !e.srv.Exited() && e.srv.Running() {
			// A turn Codex runs by itself (an active goal continues the
			// thread when it goes idle): kept warm while it runs.
			e.last = now
			continue
		}
		if e.srv.Exited() || (e.leases == 0 && now.Sub(e.last) >= p.cfg.Idle) {
			delete(p.servers, k)
			victims = append(victims, e)
		}
	}
	p.mu.Unlock()
	if len(victims) == 0 {
		return 0
	}
	var wg sync.WaitGroup
	for _, e := range victims {
		wg.Add(1)
		go func(e *entry) {
			defer wg.Done()
			e.srv.Close(5 * time.Second)
		}(e)
	}
	wg.Wait()
	p.writePIDs()
	return len(victims)
}

// Size returns the number of live servers.
func (p *Pool) Size() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.servers)
}

// Close ends every server (stdin EOF, then group kill after grace) and
// refuses new leases.
func (p *Pool) Close(grace time.Duration) {
	p.mu.Lock()
	p.closed = true
	var all []*entry
	for k, e := range p.servers {
		delete(p.servers, k)
		all = append(all, e)
	}
	p.mu.Unlock()
	var wg sync.WaitGroup
	for _, e := range all {
		wg.Add(1)
		go func(e *entry) {
			defer wg.Done()
			<-e.ready
			if e.srv != nil {
				e.srv.Close(grace)
			}
		}(e)
	}
	wg.Wait()
	p.writePIDs()
}

// pidRecord is one PID file entry. Boot and NS say which kernel instance
// and pid namespace the pid belongs to: a home copied to another machine
// (or a machine that rebooted) carries the file along, and its pids mean
// nothing there.
type pidRecord struct {
	PID   int    `json:"pid"`
	Start string `json:"start,omitempty"`
	Key   string `json:"key,omitempty"`
	Boot  string `json:"boot,omitempty"`
	NS    string `json:"pidns,omitempty"`
}

func (p *Pool) writePIDs() {
	if p.cfg.PIDFile == "" {
		return
	}
	p.pidMu.Lock()
	defer p.pidMu.Unlock()
	p.mu.Lock()
	recs := []pidRecord{}
	boot, ns := bootID(), pidNamespace()
	for _, e := range p.servers {
		if e.srv != nil && !e.srv.Exited() {
			recs = append(recs, pidRecord{PID: e.srv.PID(), Start: e.start, Key: e.key, Boot: boot, NS: ns})
		}
	}
	p.mu.Unlock()
	sort.Slice(recs, func(i, j int) bool { return recs[i].PID < recs[j].PID })
	data, _ := json.Marshal(recs)
	if err := writeFileAtomic(p.cfg.PIDFile, data); err != nil {
		fmt.Fprintf(p.cfg.Log, "codex: write pid file: %v\n", err)
	}
}

// sameProcess reports whether record r still names the live process pid
// r.PID, given this machine's boot id and pid namespace and the pid's
// current start time (each "" when unknown).
func sameProcess(r pidRecord, boot, ns, start string) bool {
	if r.Boot != "" && boot != "" && r.Boot != boot {
		return false // another machine, or before a reboot
	}
	if r.NS != "" && ns != "" && r.NS != ns {
		return false // another pid namespace (container)
	}
	if start != "" && r.Start != "" {
		return start == r.Start // else a recycled pid
	}
	// The start time cannot be compared: only a matching boot vouches
	// that the pid was not recycled by another machine.
	return r.Boot != "" && r.Boot == boot
}

func writeFileAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	// A unique temp file per write: two writers never share (and so never
	// interleave into) one file.
	f, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	_, werr := f.Write(data)
	if serr := f.Sync(); werr == nil {
		werr = serr
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = os.Chmod(tmp, 0o600)
	}
	if werr == nil {
		werr = os.Rename(tmp, path)
		// Windows refuses (access denied) a rename onto a file another
		// writer is replacing at that moment; that writer is done shortly.
		for i := 0; werr != nil && runtime.GOOS == "windows" && i < 50; i++ {
			time.Sleep(10 * time.Millisecond)
			werr = os.Rename(tmp, path)
		}
	}
	if werr != nil {
		_ = os.Remove(tmp)
	}
	return werr
}

// KillStale kills the app-server process groups recorded in pidFile by a
// previous helper process and waits until they are gone, so that turns
// are reconciled only once no old server can still change them. A record
// is left alone unless it provably names the same process: it must come
// from this boot and pid namespace (a copied home or a reboot leaves
// records whose pids mean nothing here), and a pid whose process now has
// a different start time was recycled. A record that cannot be checked
// either way (no start time and no boot id) is left alone too. The file
// is emptied on success.
func KillStale(pidFile string, wait time.Duration) (killed int, err error) {
	data, rerr := os.ReadFile(pidFile)
	if errors.Is(rerr, os.ErrNotExist) {
		return 0, nil
	}
	if rerr != nil {
		return 0, rerr
	}
	var recs []pidRecord
	if len(data) > 0 {
		if jerr := json.Unmarshal(data, &recs); jerr != nil {
			return 0, fmt.Errorf("codex: pid file %s: %w", pidFile, jerr)
		}
	}
	var targets []int
	boot, ns := bootID(), pidNamespace()
	for _, r := range recs {
		if r.PID <= 1 || r.PID == os.Getpid() {
			continue
		}
		if !sameProcess(r, boot, ns, startTime(r.PID)) {
			continue
		}
		if !groupAlive(r.PID) {
			continue
		}
		_ = killGroup(r.PID)
		targets = append(targets, r.PID)
	}
	deadline := time.Now().Add(wait)
	for _, pid := range targets {
		for groupAlive(pid) {
			if time.Now().After(deadline) {
				return len(targets), fmt.Errorf("codex: old app-server group %d still alive", pid)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	if err := writeFileAtomic(pidFile, []byte("[]")); err != nil {
		return len(targets), err
	}
	return len(targets), nil
}

// Running reports native work, including TUI turns and goal continuations.
func (p *Pool) Running() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.servers {
		if e.srv != nil && !e.srv.Exited() && e.srv.Running() {
			return true
		}
	}
	return false
}

// Retain keeps this exact server alive for a bounded auxiliary request. It
// never starts or replaces a process, unlike Acquire.
func (l *Lease) Retain() (*Lease, error) {
	l.p.mu.Lock()
	defer l.p.mu.Unlock()
	if l.p.closed {
		return nil, ErrPoolClosed
	}
	if l.e.srv.Exited() {
		return nil, ErrExited
	}
	l.e.leases++
	return &Lease{p: l.p, e: l.e}, nil
}
