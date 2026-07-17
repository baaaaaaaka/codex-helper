//go:build darwin && !cgo

package hoststate

func newPlatformObserver(opts Options) Observer {
	return newPollingObserver(opts)
}
