//go:build windows

package hoststate

import (
	"sync"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

const (
	deviceNotifyCallback  = 0x00000002
	pbtApmsuspend         = 0x0004
	pbtApmresumeSuspend   = 0x0007
	pbtApmresumeAutomatic = 0x0012
	pbtApmresumeCritical  = 0x0006
)

type deviceNotifySubscribeParameters struct {
	callback uintptr
	context  uintptr
}

type windowsObserver struct {
	interval time.Duration

	mu          sync.Mutex
	closed      bool
	started     bool
	events      chan Event
	stop        chan struct{}
	closeCh     chan struct{}
	closeFn     sync.Once
	callbacks   []uintptr
	powerHandle windows.Handle
	ipHandle    windows.Handle
}

var (
	modPowrProf         = windows.NewLazySystemDLL("PowrProf.dll")
	procPowerRegister   = modPowrProf.NewProc("PowerRegisterSuspendResumeNotification")
	procPowerUnregister = modPowrProf.NewProc("PowerUnregisterSuspendResumeNotification")
)

func newPlatformObserver(opts Options) Observer {
	interval := opts.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &windowsObserver{
		interval: interval,
		events:   make(chan Event, 16),
		stop:     make(chan struct{}),
		closeCh:  make(chan struct{}),
	}
}

func (o *windowsObserver) Events() <-chan Event { return o.events }

func (o *windowsObserver) Start() error {
	o.mu.Lock()
	if o.started {
		o.mu.Unlock()
		return nil
	}
	o.started = true
	o.mu.Unlock()
	// Both registrations are user-mode APIs. If a particular Windows runtime
	// lacks one of them, the polling fallback remains active.
	o.registerPower()
	o.registerNetwork()
	go o.run()
	return nil
}

func (o *windowsObserver) registerPower() {
	callback := windows.NewCallback(func(_ uintptr, eventType uint32, _ uintptr) uintptr {
		switch eventType {
		case pbtApmsuspend:
			o.emit(Event{Kind: EventPowerWillSleep, Source: "windows-power"})
		case pbtApmresumeAutomatic, pbtApmresumeSuspend, pbtApmresumeCritical:
			o.emit(Event{Kind: EventPowerDidWake, Source: "windows-power"})
		}
		return 0
	})
	o.mu.Lock()
	o.callbacks = append(o.callbacks, callback)
	o.mu.Unlock()
	params := deviceNotifySubscribeParameters{callback: callback}
	result, _, _ := procPowerRegister.Call(deviceNotifyCallback, uintptr(unsafe.Pointer(&params)), uintptr(unsafe.Pointer(&o.powerHandle)))
	if result != 0 {
		o.powerHandle = 0
	}
}

func (o *windowsObserver) registerNetwork() {
	callback := windows.NewCallback(func(_ uintptr, _ uintptr, _ uint32) {
		o.emit(Event{Kind: EventNetworkChanged, Source: "windows-ip-interface"})
	})
	o.mu.Lock()
	o.callbacks = append(o.callbacks, callback)
	o.mu.Unlock()
	var handle windows.Handle
	if err := windows.NotifyIpInterfaceChange(windows.AF_UNSPEC, callback, nil, false, &handle); err == nil {
		o.ipHandle = handle
	}
}

func (o *windowsObserver) run() {
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()
	previous := interfaceFingerprint()
	lastTick := time.Now()
	for {
		select {
		case <-o.stop:
			close(o.closeCh)
			return
		case now := <-ticker.C:
			if now.Sub(lastTick) > 2*o.interval {
				o.emit(Event{Kind: EventPowerDidWake, At: now, Source: "windows-poll-gap"})
			}
			lastTick = now
			current := interfaceFingerprint()
			if current != previous {
				previous = current
				o.emit(Event{Kind: EventNetworkChanged, At: now, Source: "windows-interface-poll"})
			}
		}
	}
}

func (o *windowsObserver) emit(event Event) {
	if event.At.IsZero() {
		event.At = time.Now()
	}
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

func (o *windowsObserver) Close() error {
	o.closeFn.Do(func() {
		o.mu.Lock()
		wasStarted := o.started
		o.closed = true
		ipHandle := o.ipHandle
		powerHandle := o.powerHandle
		o.mu.Unlock()
		if ipHandle != 0 {
			_ = windows.CancelMibChangeNotify2(ipHandle)
		}
		if powerHandle != 0 {
			_, _, _ = procPowerUnregister.Call(uintptr(powerHandle))
		}
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
