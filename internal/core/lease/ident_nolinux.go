//go:build !linux

package lease

func pidNamespace() string { return "" }

func startTime(int) string { return "" }
