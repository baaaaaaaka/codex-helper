//go:build windows

package userpath

import "fmt"

func WriteProbeSocket(string, string) error {
	return fmt.Errorf("Unix user PATH probe sockets are unavailable on Windows")
}
