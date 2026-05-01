//go:build !windows

package cli

import (
	"os"
	"strconv"
)

func defaultTeamsServiceUserID() string {
	return strconv.Itoa(os.Getuid())
}
