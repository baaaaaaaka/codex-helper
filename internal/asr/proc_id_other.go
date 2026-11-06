//go:build !linux && !darwin && !windows

package asr

// processID is not implemented here: without it no server is ever recorded
// or killed as an orphan.
func processID(int) (string, bool) { return "", false }
