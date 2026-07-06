//go:build !windows

package cli

import "context"

func teamsUserPathExpectedSID(context.Context, string, []string) (string, error) {
	return "", nil
}
