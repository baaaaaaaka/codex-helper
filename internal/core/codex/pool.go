package codex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
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
		e = &entry{key: key, loaded: map[string]bool{}, ready: make(chan struct{}), leases: 1}
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

// Busy reports whether key has a live server.
func (p *Pool) Busy(key string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	e := p.servers[key]
	return e != nil && e.srv != nil && !e.srv.Exited()
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

// pidRecord is one PID file entry.
type pidRecord struct {
	PID   int    `json:"pid"`
	Start string `json:"start,omitempty"`
	Key   string `json:"key,omitempty"`
}

func (p *Pool) writePIDs() {
	if p.cfg.PIDFile == "" {
		return
	}
	p.mu.Lock()
	recs := []pidRecord{}
	for _, e := range p.servers {
		if e.srv != nil && !e.srv.Exited() {
			recs = append(recs, pidRecord{PID: e.srv.PID(), Start: e.start, Key: e.key})
		}
	}
	p.mu.Unlock()
	sort.Slice(recs, func(i, j int) bool { return recs[i].PID < recs[j].PID })
	data, _ := json.Marshal(recs)
	if err := writeFileAtomic(p.cfg.PIDFile, data); err != nil {
		fmt.Fprintf(p.cfg.Log, "codex: write pid file: %v\n", err)
	}
}

func writeFileAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// KillStale kills the app-server process groups recorded in pidFile by a
// previous helper process and waits until they are gone, so that turns
// are reconciled only once no old server can still change them. A pid
// whose process now has a different start time was recycled and is left
// alone. The file is emptied on success.
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
	for _, r := range recs {
		if r.PID <= 1 || r.PID == os.Getpid() {
			continue
		}
		if cur := startTime(r.PID); cur != "" && r.Start != "" && cur != r.Start {
			continue // recycled pid: the leader is a different process
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
