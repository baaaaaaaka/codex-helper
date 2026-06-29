package main

import (
	"fmt"
	"os"

	"github.com/baaaaaaaka/codex-helper/internal/cli"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
)

func main() {
	if exitCode, handled, err := helperruntime.Launch(cli.RuntimeVersion(), os.Args); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Warning: cxp runtime activation failed; continuing with the current executable: %v\n", err)
	} else if handled {
		os.Exit(exitCode)
	}
	os.Exit(cli.Execute())
}
