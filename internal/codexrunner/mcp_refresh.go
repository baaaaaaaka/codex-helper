package codexrunner

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	appServerMethodMCPServerReload = "config/mcpServer/reload"
	// A fixed poll interval bounds refresh triggers without adding an event
	// watcher dependency or any work to the prompt path.
	mcpRefreshPollInterval = time.Second
)

type mcpConfigFileStateKind uint8

const (
	mcpConfigFileUnavailable mcpConfigFileStateKind = iota
	mcpConfigFileMissing
	mcpConfigFilePresent
)

type mcpConfigFileState struct {
	kind    mcpConfigFileStateKind
	size    int64
	mode    fs.FileMode
	modTime time.Time
	info    fs.FileInfo
}

func readMCPConfigFileState(path string) mcpConfigFileState {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return mcpConfigFileState{kind: mcpConfigFileMissing}
		}
		return mcpConfigFileState{kind: mcpConfigFileUnavailable}
	}
	return mcpConfigFileState{
		kind:    mcpConfigFilePresent,
		size:    info.Size(),
		mode:    info.Mode(),
		modTime: info.ModTime(),
		info:    info,
	}
}

func (s mcpConfigFileState) equal(other mcpConfigFileState) bool {
	if s.kind != other.kind {
		return false
	}
	if s.kind != mcpConfigFilePresent {
		return true
	}
	if s.size != other.size || s.mode != other.mode || !s.modTime.Equal(other.modTime) {
		return false
	}
	if s.info == nil || other.info == nil {
		return s.info == nil && other.info == nil
	}
	return os.SameFile(s.info, other.info)
}

type mcpRefreshCoordinator struct {
	path         string
	pollInterval time.Duration
	refresh      func(context.Context) error

	trigger chan struct{}
	ctx     context.Context
	cancel  context.CancelFunc
	done    chan struct{}
	stop    sync.Once
}

func (r *AppServerRunner) startMCPRefreshLocked(initialState mcpConfigFileState) {
	path := mcpConfigFilePath(r.CodexHome)
	if path == "" || r.mcpRefresh != nil {
		return
	}
	coordinator := newMCPRefreshCoordinator(
		path,
		mcpRefreshPollInterval,
		func(ctx context.Context) error {
			if err := ctx.Err(); err != nil {
				return err
			}
			_, err := r.request(ctx, appServerMethodMCPServerReload, nil)
			return err
		},
	)
	r.mcpRefresh = coordinator
	coordinator.startWithInitialState(context.Background(), initialState)
}

func mcpConfigFilePath(codexHome string) string {
	if strings.TrimSpace(codexHome) == "" {
		return ""
	}
	return filepath.Join(filepath.Clean(codexHome), "config.toml")
}

func (r *AppServerRunner) detachMCPRefreshAndWait() {
	r.mu.Lock()
	coordinator := r.mcpRefresh
	r.mcpRefresh = nil
	r.mu.Unlock()
	if coordinator != nil {
		coordinator.stopAndWait()
	}
}

func newMCPRefreshCoordinator(
	path string,
	pollInterval time.Duration,
	refresh func(context.Context) error,
) *mcpRefreshCoordinator {
	if pollInterval <= 0 {
		pollInterval = mcpRefreshPollInterval
	}
	return &mcpRefreshCoordinator{
		path:         filepath.Clean(path),
		pollInterval: pollInterval,
		refresh:      refresh,
		trigger:      make(chan struct{}, 1),
		done:         make(chan struct{}),
	}
}

func (c *mcpRefreshCoordinator) start(parent context.Context) {
	c.startWithInitialState(parent, readMCPConfigFileState(c.path))
}

func (c *mcpRefreshCoordinator) startWithInitialState(parent context.Context, initialState mcpConfigFileState) {
	c.ctx, c.cancel = context.WithCancel(parent)
	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		c.watch(initialState)
	}()
	go func() {
		defer workers.Done()
		c.work()
	}()
	go func() {
		workers.Wait()
		close(c.done)
	}()
}

func (c *mcpRefreshCoordinator) stopAndWait() {
	c.stop.Do(func() {
		if c.cancel != nil {
			c.cancel()
		}
	})
	<-c.done
}

func (c *mcpRefreshCoordinator) watch(lastState mcpConfigFileState) {
	ticker := time.NewTicker(c.pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			state := readMCPConfigFileState(c.path)
			if state.equal(lastState) {
				continue
			}
			lastState = state
			select {
			case c.trigger <- struct{}{}:
			default:
			}
		}
	}
}

func (c *mcpRefreshCoordinator) work() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-c.trigger:
			if c.ctx.Err() != nil {
				return
			}
			// Refresh failures are intentionally not retried for the same observed
			// file state. The next distinct state is the retry signal.
			// The coordinator context is also the runner lifecycle context. Using
			// it for the whole RPC keeps server-side reloads single-flight: a
			// client-side deadline could otherwise abandon an in-progress server
			// reload and queue an overlapping one.
			_ = c.refresh(c.ctx)
		}
	}
}
