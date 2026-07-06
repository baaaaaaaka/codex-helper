//go:build windows

package userpath

func currentProcessIDs() (uint32, uint32, uint32) { return 0, 0, 0 }

func currentProcessGroups() []uint32 { return nil }
