//go:build !darwin && !windows

package cli

// teamsOSBootID: no boot id is known here (Linux reads its own).
func teamsOSBootID() string { return "" }
