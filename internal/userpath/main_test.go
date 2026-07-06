package userpath

import (
	"fmt"
	"os"
	"strconv"
	"testing"
)

func TestMain(m *testing.M) {
	for index, arg := range os.Args {
		if arg != "__user-path-probe" {
			continue
		}
		fd := -1
		nonce := ""
		socketPath := ""
		for pos := index + 1; pos+1 < len(os.Args); pos += 2 {
			switch os.Args[pos] {
			case "--fd":
				fd, _ = strconv.Atoi(os.Args[pos+1])
			case "--nonce":
				nonce = os.Args[pos+1]
			case "--socket":
				socketPath = os.Args[pos+1]
			}
		}
		if socketPath != "" {
			if err := WriteProbeSocket(socketPath, nonce); err != nil {
				_, _ = fmt.Fprintln(os.Stderr, err)
				os.Exit(3)
			}
			os.Exit(0)
		}
		if fd < 3 {
			_, _ = fmt.Fprintln(os.Stderr, "invalid helper fd")
			os.Exit(2)
		}
		if err := WriteProbe(uintptr(fd), nonce); err != nil {
			_, _ = fmt.Fprintln(os.Stderr, err)
			os.Exit(3)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}
