//go:build !linux

package cli

func legacyUpdaterVersionPreflight() error {
	return nil
}

// LegacyUpdaterVersionPreflight is a no-op on platforms that never shipped
// the affected Unix symlink updater behavior.
func LegacyUpdaterVersionPreflight() error {
	return legacyUpdaterVersionPreflight()
}
