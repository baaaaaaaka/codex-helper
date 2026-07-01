//go:build !linux && !darwin && !windows

package candidateupdate

import "fmt"

func directParentExecutable() (string, error) {
	return "", fmt.Errorf("legacy runtime bridge is unsupported on this platform")
}

func replaceLegacyStableEntry(candidate string, entry string, runningRuntime string, root string) error {
	return fmt.Errorf("legacy runtime bridge is unsupported on this platform")
}
