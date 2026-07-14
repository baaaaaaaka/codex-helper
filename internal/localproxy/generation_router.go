package localproxy

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
)

var errGenerationForceClosed = errors.New("generation was force-closed during dial")

// GenerationRouter keeps the public HTTP proxy listener stable while its
// backend dialer is replaced. Existing connections retain a lease on the old
// generation; new dials use the current generation after Swap returns.
type GenerationRouter struct {
	mu      sync.RWMutex
	current *routerGeneration
	all     map[uint64]*routerGeneration
	closed  bool
	nextID  atomic.Uint64
}

type routerGeneration struct {
	id     uint64
	dialer Dialer

	mu       sync.Mutex
	draining bool
	// forcedClosed is set before a bounded drain closes the current connection
	// set. An in-flight dial still owns a reference, so without this fence it
	// could add a connection after the close snapshot and hand a stale socket
	// back to the caller.
	forcedClosed bool
	refs         int
	connSet      map[net.Conn]struct{}
	drained      chan struct{}
}

func NewGenerationRouter(d Dialer) (*GenerationRouter, error) {
	if d == nil {
		return nil, errors.New("generation router dialer is required")
	}
	r := &GenerationRouter{}
	r.all = make(map[uint64]*routerGeneration)
	r.current = r.newGeneration(d)
	return r, nil
}

func (r *GenerationRouter) newGeneration(d Dialer) *routerGeneration {
	g := &routerGeneration{
		id:      r.nextID.Add(1),
		dialer:  d,
		connSet: make(map[net.Conn]struct{}),
		drained: make(chan struct{}),
	}
	r.all[g.id] = g
	return g
}

func (r *GenerationRouter) Dial(network, addr string) (net.Conn, error) {
	return r.DialContext(context.Background(), network, addr)
}

func (r *GenerationRouter) DialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	g, err := r.acquire()
	if err != nil {
		return nil, err
	}

	conn, err := dialWithContext(ctx, g.dialer, network, addr)
	if err != nil {
		g.release(nil)
		return nil, err
	}
	if conn == nil {
		g.release(nil)
		return nil, errors.New("generation router dialer returned a nil connection")
	}
	if !g.add(conn) {
		_ = conn.Close()
		g.release(nil)
		return nil, errGenerationForceClosed
	}
	return &leasedConn{Conn: conn, release: func() { g.release(conn) }}, nil
}

func (r *GenerationRouter) acquire() (*routerGeneration, error) {
	r.mu.RLock()
	if r.closed || r.current == nil {
		r.mu.RUnlock()
		return nil, errors.New("generation router is closed")
	}
	g := r.current
	g.mu.Lock()
	if g.draining {
		g.mu.Unlock()
		r.mu.RUnlock()
		return nil, errors.New("current generation is draining")
	}
	g.refs++
	g.mu.Unlock()
	r.mu.RUnlock()
	return g, nil
}

// Swap installs a new generation and marks the previous generation draining.
// The returned ID is zero when there was no previous generation.
func (r *GenerationRouter) Swap(d Dialer) (uint64, error) {
	if d == nil {
		return 0, errors.New("generation router dialer is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return 0, errors.New("generation router is closed")
	}
	next := r.newGeneration(d)
	var oldID uint64
	if r.current != nil {
		old := r.current
		old.mu.Lock()
		old.draining = true
		oldID = old.id
		old.maybeCloseDrainedLocked()
		old.mu.Unlock()
	}
	r.current = next
	return oldID, nil
}

func (r *GenerationRouter) CurrentGeneration() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.current == nil {
		return 0
	}
	return r.current.id
}

// Drain waits for all connections belonging to the generation that was
// current before the most recent Swap. On timeout it closes the remaining
// connections, allowing the caller to bound cleanup without blocking forever.
func (r *GenerationRouter) Drain(ctx context.Context, generationID uint64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	g := r.findGeneration(generationID)
	if g == nil {
		return nil
	}
	g.mu.Lock()
	g.draining = true
	g.maybeCloseDrainedLocked()
	drained := g.drained
	g.mu.Unlock()

	select {
	case <-drained:
		r.forgetGeneration(generationID)
		return nil
	case <-ctx.Done():
		g.closeConnections()
		select {
		case <-drained:
			r.forgetGeneration(generationID)
			return ctx.Err()
		default:
			return ctx.Err()
		}
	}
}

func (r *GenerationRouter) forgetGeneration(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.current == nil || r.current.id != id {
		delete(r.all, id)
	}
}

func (r *GenerationRouter) findGeneration(id uint64) *routerGeneration {
	if id == 0 {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.all[id]
}

func (r *GenerationRouter) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	g := r.current
	r.current = nil
	all := make([]*routerGeneration, 0, len(r.all))
	for _, generation := range r.all {
		all = append(all, generation)
	}
	r.mu.Unlock()
	if g == nil && len(all) == 0 {
		return nil
	}
	for _, generation := range all {
		generation.mu.Lock()
		generation.draining = true
		generation.maybeCloseDrainedLocked()
		generation.mu.Unlock()
	}
	for _, generation := range all {
		select {
		case <-generation.drained:
		case <-ctx.Done():
			for _, remaining := range all {
				remaining.closeConnections()
			}
			return ctx.Err()
		}
	}
	return nil
}

func (g *routerGeneration) add(conn net.Conn) bool {
	g.mu.Lock()
	if g.forcedClosed {
		g.mu.Unlock()
		return false
	}
	g.connSet[conn] = struct{}{}
	g.mu.Unlock()
	return true
}

func (g *routerGeneration) release(conn net.Conn) {
	g.mu.Lock()
	if conn != nil {
		if _, ok := g.connSet[conn]; !ok {
			g.mu.Unlock()
			return
		}
		delete(g.connSet, conn)
	}
	if g.refs > 0 {
		g.refs--
	}
	g.maybeCloseDrainedLocked()
	g.mu.Unlock()
}

func (g *routerGeneration) maybeCloseDrainedLocked() {
	if g.draining && g.refs == 0 {
		select {
		case <-g.drained:
		default:
			close(g.drained)
		}
	}
}

func (g *routerGeneration) closeConnections() {
	g.mu.Lock()
	g.forcedClosed = true
	connections := make([]net.Conn, 0, len(g.connSet))
	for conn := range g.connSet {
		connections = append(connections, conn)
	}
	g.mu.Unlock()
	for _, conn := range connections {
		_ = conn.Close()
		g.release(conn)
	}
}

type leasedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *leasedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

func dialWithContext(ctx context.Context, d Dialer, network, addr string) (net.Conn, error) {
	if cd, ok := d.(ContextDialer); ok {
		return cd.DialContext(ctx, network, addr)
	}
	return dialContextFallback(ctx, d, network, addr)
}
