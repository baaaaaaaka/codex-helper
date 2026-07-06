package cli

import (
	"fmt"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/userpath"
)

func runEarlyUserPathProbe(args []string) (bool, error) {
	if len(args) == 0 || args[0] != "__user-path-probe" {
		return false, nil
	}
	fd := -1
	nonce := ""
	socketPath := ""
	for index := 1; index < len(args); index++ {
		if index+1 >= len(args) {
			return true, fmt.Errorf("missing value for %s", args[index])
		}
		value := args[index+1]
		switch args[index] {
		case "--fd":
			parsed, err := strconv.Atoi(value)
			if err != nil {
				return true, fmt.Errorf("invalid user PATH probe descriptor: %w", err)
			}
			fd = parsed
		case "--nonce":
			nonce = value
		case "--socket":
			socketPath = value
		default:
			return true, fmt.Errorf("unknown user PATH probe argument %q", args[index])
		}
		index++
	}
	if socketPath != "" {
		if fd >= 3 {
			return true, fmt.Errorf("user PATH probe accepts either a socket or descriptor, not both")
		}
		return true, userpath.WriteProbeSocket(socketPath, nonce)
	}
	if fd < 3 {
		return true, fmt.Errorf("user PATH probe requires a socket or descriptor of at least 3")
	}
	return true, userpath.WriteProbe(uintptr(fd), nonce)
}

func newInternalUserPathProbeCmd() *cobra.Command {
	var fd int
	var nonce string
	var socketPath string
	cmd := &cobra.Command{
		Use:    "__user-path-probe",
		Hidden: true,
		Args:   cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			if socketPath != "" {
				if fd >= 3 {
					return fmt.Errorf("user PATH probe accepts either a socket or descriptor, not both")
				}
				return userpath.WriteProbeSocket(socketPath, nonce)
			}
			if fd < 3 {
				return fmt.Errorf("user PATH probe requires a socket or descriptor of at least 3")
			}
			return userpath.WriteProbe(uintptr(fd), nonce)
		},
	}
	cmd.Flags().IntVar(&fd, "fd", -1, "internal protocol descriptor")
	cmd.Flags().StringVar(&socketPath, "socket", "", "internal protocol socket")
	cmd.Flags().StringVar(&nonce, "nonce", "", "internal protocol nonce")
	return cmd
}
