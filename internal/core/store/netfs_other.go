//go:build !linux

package store

func networkFS(string) bool { return false }
