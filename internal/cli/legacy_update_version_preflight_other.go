//go:build !linux

package cli

func legacyUpdaterVersionPreflight() error {
	return nil
}
