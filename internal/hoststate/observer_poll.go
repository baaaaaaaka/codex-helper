package hoststate

import (
	"sync"
	"time"
)

type pollingObserver struct {
	interval time.Duration

	lifecycleMu sync.Mutex
	mu          sync.Mutex
	closed      bool
	started     bool
	events      chan Event
	stop        chan struct{}
	closeCh     chan struct{}
	closeFn     sync.Once
}

func newPollingObserver(opts Options) Observer {
	interval := opts.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &pollingObserver{
		interval: interval,
		events:   make(chan Event, 8),
		stop:     make(chan struct{}),
		closeCh:  make(chan struct{}),
	}
}

func (o *pollingObserver) Events() <-chan Event { return o.events }

func (o *pollingObserver) Start() error {
	o.lifecycleMu.Lock()
	defer o.lifecycleMu.Unlock()
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		return ErrObserverClosed
	}
	if o.started {
		o.mu.Unlock()
		return nil
	}
	o.started = true
	o.mu.Unlock()
	go o.run()
	return nil
}

func (o *pollingObserver) run() {
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()
	previous := interfaceFingerprint()
	lastTick := time.Now().Round(0)
	for {
		select {
		case <-o.stop:
			close(o.closeCh)
			return
		case now := <-ticker.C:
			now = now.Round(0)
			if wallElapsed(now, lastTick) > 2*o.interval {
				o.emit(Event{Kind: EventPowerDidWake, At: now, Source: "poll-gap"})
			}
			lastTick = now
			current := interfaceFingerprint()
			if current != previous {
				previous = current
				o.emit(Event{Kind: EventNetworkChanged, At: now, Source: "interface-poll"})
			}
		}
	}
}

func (o *pollingObserver) emit(event Event) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return
	}
	select {
	case o.events <- event:
	default:
	}
}

func (o *pollingObserver) Close() error {
	o.lifecycleMu.Lock()
	defer o.lifecycleMu.Unlock()
	o.closeFn.Do(func() {
		o.mu.Lock()
		wasStarted := o.started
		o.closed = true
		o.mu.Unlock()
		close(o.stop)
		if wasStarted {
			<-o.closeCh
		} else {
			close(o.closeCh)
		}
		close(o.events)
	})
	return nil
}
