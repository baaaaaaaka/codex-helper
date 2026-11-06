//go:build !windows && !linux && !darwin

package codex

func startTime(int) string { return "" }

func bootID() string { return "" }

func pidNamespace() string { return "" }
