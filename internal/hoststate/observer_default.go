//go:build !windows && !darwin

package hoststate

func newPlatformObserver(opts Options) Observer {
	return newPollingObserver(opts)
}
