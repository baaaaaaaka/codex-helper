//go:build windows

package cli

func exitDueToFatalSignal(_ error) bool { return false }
