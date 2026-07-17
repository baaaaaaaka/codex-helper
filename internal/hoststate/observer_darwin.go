//go:build darwin && cgo

package hoststate

/*
#cgo LDFLAGS: -framework IOKit -framework CoreFoundation -framework SystemConfiguration
#include <CoreFoundation/CoreFoundation.h>
#include <IOKit/IOKitLib.h>
#include <IOKit/IOMessage.h>
#include <IOKit/pwr_mgt/IOPMLib.h>
#include <SystemConfiguration/SystemConfiguration.h>
#include <stdint.h>
#include <stdlib.h>

enum {
	CXP_POWER_WILL_SLEEP = 1,
	CXP_POWER_DID_WAKE = 2,
	CXP_NETWORK_CHANGED = 3,
};

extern void goHostStateDarwinEvent(uintptr_t context, uint32_t event);

typedef struct cxp_power_observer {
	io_connect_t root_power;
	io_object_t notifier;
	IONotificationPortRef notification_port;
	CFRunLoopRef run_loop;
	CFRunLoopSourceRef run_loop_source;
	SCDynamicStoreRef config_store;
	CFRunLoopSourceRef config_source;
	uintptr_t context;
	volatile int stop_requested;
} cxp_power_observer;

static void cxp_network_callback(SCDynamicStoreRef store, CFArrayRef changed_keys, void *info) {
	(void)store;
	(void)changed_keys;
	cxp_power_observer *observer = (cxp_power_observer *)info;
	if (observer != NULL) {
		goHostStateDarwinEvent(observer->context, CXP_NETWORK_CHANGED);
	}
}

static void cxp_power_callback(void *refcon, io_service_t service, natural_t message_type, void *message_argument) {
	(void)service;
	cxp_power_observer *observer = (cxp_power_observer *)refcon;
	if (observer == NULL) {
		return;
	}
	switch (message_type) {
	case kIOMessageCanSystemSleep:
		// Never hold up system sleep while notifying Go code.
		IOAllowPowerChange(observer->root_power, (long)message_argument);
		break;
	case kIOMessageSystemWillSleep:
		goHostStateDarwinEvent(observer->context, CXP_POWER_WILL_SLEEP);
		IOAllowPowerChange(observer->root_power, (long)message_argument);
		break;
	case kIOMessageSystemWillPowerOn:
	case kIOMessageSystemHasPoweredOn:
		goHostStateDarwinEvent(observer->context, CXP_POWER_DID_WAKE);
		break;
	default:
		break;
	}
}

static cxp_power_observer *cxp_power_start(void) {
	cxp_power_observer *observer = calloc(1, sizeof(*observer));
	if (observer == NULL) {
		return NULL;
	}
	observer->root_power = IORegisterForSystemPower(
		observer,
		&observer->notification_port,
		cxp_power_callback,
		&observer->notifier);
	if (observer->root_power == IO_OBJECT_NULL || observer->notification_port == NULL) {
		free(observer);
		return NULL;
	}
	observer->run_loop_source = IONotificationPortGetRunLoopSource(observer->notification_port);
	observer->run_loop = CFRunLoopGetCurrent();
	if (observer->run_loop_source == NULL || observer->run_loop == NULL) {
		IOObjectRelease(observer->notifier);
		IONotificationPortDestroy(observer->notification_port);
		IOServiceClose(observer->root_power);
		free(observer);
		return NULL;
	}
	CFRunLoopAddSource(observer->run_loop, observer->run_loop_source, kCFRunLoopDefaultMode);
	SCDynamicStoreContext store_context = {0, observer, NULL, NULL, NULL};
	observer->config_store = SCDynamicStoreCreate(NULL, CFSTR("codex-helper.hoststate"), cxp_network_callback, &store_context);
	if (observer->config_store != NULL) {
		CFStringRef interface_key = SCDynamicStoreKeyCreateNetworkInterfaceEntity(
			NULL, kSCDynamicStoreDomainState, CFSTR(".*"), kSCEntNetIPv4);
		CFStringRef global_key = SCDynamicStoreKeyCreateNetworkGlobalEntity(
			NULL, kSCDynamicStoreDomainState, kSCEntNetIPv4);
		const void *keys[] = {global_key};
		CFArrayRef key_array = global_key != NULL
			? CFArrayCreate(NULL, keys, 1, &kCFTypeArrayCallBacks)
			: NULL;
		const void *patterns[] = {interface_key};
		CFArrayRef pattern_array = interface_key != NULL
			? CFArrayCreate(NULL, patterns, 1, &kCFTypeArrayCallBacks)
			: NULL;
		if ((key_array != NULL || pattern_array != NULL) &&
			SCDynamicStoreSetNotificationKeys(observer->config_store, key_array, pattern_array)) {
			observer->config_source = SCDynamicStoreCreateRunLoopSource(NULL, observer->config_store, 0);
			if (observer->config_source != NULL) {
				CFRunLoopAddSource(observer->run_loop, observer->config_source, kCFRunLoopDefaultMode);
			}
		}
		if (key_array != NULL) {
			CFRelease(key_array);
		}
		if (pattern_array != NULL) {
			CFRelease(pattern_array);
		}
		if (interface_key != NULL) {
			CFRelease(interface_key);
		}
		if (global_key != NULL) {
			CFRelease(global_key);
		}
	}
	return observer;
}

static void cxp_power_set_context(cxp_power_observer *observer, uintptr_t context) {
	if (observer != NULL) {
		observer->context = context;
	}
}

static void cxp_power_run(cxp_power_observer *observer) {
	if (observer != NULL) {
		while (!observer->stop_requested) {
			// The bounded mode makes Close safe even if it races with the
			// goroutine immediately before CFRunLoopRunInMode starts.
			CFRunLoopRunInMode(kCFRunLoopDefaultMode, 0.1, true);
		}
	}
}

static void cxp_power_request_stop(cxp_power_observer *observer) {
	if (observer != NULL) {
		observer->stop_requested = 1;
		if (observer->run_loop != NULL) {
			CFRunLoopStop(observer->run_loop);
		}
	}
}

static void cxp_power_destroy(cxp_power_observer *observer) {
	if (observer == NULL) {
		return;
	}
	if (observer->run_loop != NULL && observer->run_loop_source != NULL) {
		CFRunLoopRemoveSource(observer->run_loop, observer->run_loop_source, kCFRunLoopDefaultMode);
	}
	if (observer->run_loop != NULL && observer->config_source != NULL) {
		CFRunLoopRemoveSource(observer->run_loop, observer->config_source, kCFRunLoopDefaultMode);
	}
	if (observer->config_source != NULL) {
		CFRelease(observer->config_source);
	}
	if (observer->config_store != NULL) {
		CFRelease(observer->config_store);
	}
	if (observer->notifier != IO_OBJECT_NULL) {
		IOObjectRelease(observer->notifier);
	}
	if (observer->notification_port != NULL) {
		IONotificationPortDestroy(observer->notification_port);
	}
	if (observer->root_power != IO_OBJECT_NULL) {
		IOServiceClose(observer->root_power);
	}
	free(observer);
}
*/
import "C"

import (
	"runtime"
	"sync"
	"time"
	"unsafe"
)

var darwinObserverRegistry sync.Map

type darwinObserver struct {
	interval time.Duration

	mu      sync.Mutex
	closed  bool
	started bool
	events  chan Event
	stop    chan struct{}
	closeFn sync.Once

	powerHandle *C.cxp_power_observer
	powerDone   chan struct{}
	networkDone chan struct{}
	registryKey uintptr
}

func newPlatformObserver(opts Options) Observer {
	interval := opts.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	return &darwinObserver{
		interval:    interval,
		events:      make(chan Event, 16),
		stop:        make(chan struct{}),
		powerDone:   make(chan struct{}),
		networkDone: make(chan struct{}),
	}
}

func (o *darwinObserver) Events() <-chan Event { return o.events }

func (o *darwinObserver) Start() error {
	o.mu.Lock()
	if o.started {
		o.mu.Unlock()
		return nil
	}
	o.started = true
	o.mu.Unlock()

	go o.runPower()
	go o.runNetwork()
	return nil
}

func (o *darwinObserver) runPower() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer close(o.powerDone)
	handle := C.cxp_power_start()
	if handle == nil {
		return
	}
	key := uintptr(unsafe.Pointer(handle))
	darwinObserverRegistry.Store(key, o)
	C.cxp_power_set_context(handle, C.uintptr_t(key))
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		darwinObserverRegistry.Delete(key)
		C.cxp_power_destroy(handle)
		return
	}
	o.powerHandle = handle
	o.registryKey = key
	o.mu.Unlock()
	C.cxp_power_run(handle)
	darwinObserverRegistry.Delete(key)
	C.cxp_power_destroy(handle)
}

func (o *darwinObserver) runNetwork() {
	defer close(o.networkDone)
	ticker := time.NewTicker(o.interval)
	defer ticker.Stop()
	previous := interfaceFingerprint()
	lastTick := time.Now()
	for {
		select {
		case <-o.stop:
			return
		case now := <-ticker.C:
			if now.Sub(lastTick) > 2*o.interval {
				o.emit(Event{Kind: EventPowerDidWake, At: now, Source: "darwin-poll-gap"})
			}
			lastTick = now
			current := interfaceFingerprint()
			if current != previous {
				previous = current
				o.emit(Event{Kind: EventNetworkChanged, At: now, Source: "darwin-interface-poll"})
			}
		}
	}
}

//export goHostStateDarwinEvent
func goHostStateDarwinEvent(context C.uintptr_t, event C.uint32_t) {
	value, ok := darwinObserverRegistry.Load(uintptr(context))
	if !ok {
		return
	}
	o, ok := value.(*darwinObserver)
	if !ok {
		return
	}
	switch uint32(event) {
	case C.CXP_POWER_WILL_SLEEP:
		o.emit(Event{Kind: EventPowerWillSleep, Source: "darwin-iokit"})
	case C.CXP_POWER_DID_WAKE:
		o.emit(Event{Kind: EventPowerDidWake, Source: "darwin-iokit"})
	case C.CXP_NETWORK_CHANGED:
		o.emit(Event{Kind: EventNetworkChanged, Source: "darwin-system-configuration"})
	}
}

func (o *darwinObserver) emit(event Event) {
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

func (o *darwinObserver) Close() error {
	o.closeFn.Do(func() {
		o.mu.Lock()
		wasStarted := o.started
		o.closed = true
		handle := o.powerHandle
		o.mu.Unlock()
		if handle != nil {
			C.cxp_power_request_stop(handle)
		}
		close(o.stop)
		if wasStarted {
			<-o.powerDone
			<-o.networkDone
		} else {
			close(o.powerDone)
			close(o.networkDone)
		}
		darwinObserverRegistry.Range(func(key, value any) bool {
			if value == o {
				darwinObserverRegistry.Delete(key)
			}
			return true
		})
		close(o.events)
	})
	return nil
}
