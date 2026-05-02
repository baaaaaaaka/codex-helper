//go:build windows

package cli

import "os"

func defaultTeamsServiceUserID() string {
	return os.Getenv("USERNAME")
}
