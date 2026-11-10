package updatestate

import (
	"flag"
	"fmt"
	"io"
	"path/filepath"
	"time"
)

// HandleActivationCommand lets the existing native activation worker persist
// its result even when the installed entry is still locked by its parent.
func HandleActivationCommand(args []string, stderr io.Writer) (int, bool) {
	if len(args) < 2 || args[1] != "--cxp-record-activation" {
		return 0, false
	}
	fs := flag.NewFlagSet("--cxp-record-activation", flag.ContinueOnError)
	fs.SetOutput(stderr)
	r := ActivationResult{Version: 1, UpdatedAt: time.Now()}
	fs.StringVar(&r.Source, "source", "", "Pending executable")
	fs.StringVar(&r.Dest, "dest", "", "Installed entry")
	fs.StringVar(&r.Want, "want", "", "Target version")
	fs.StringVar(&r.Status, "status", "", "Activation status")
	fs.StringVar(&r.Message, "message", "", "Activation detail")
	if err := fs.Parse(args[2:]); err != nil {
		return 1, true
	}
	if fs.NArg() != 0 || r.Source == "" || r.Dest == "" || r.Want == "" || (r.Status != "running" && r.Status != "success" && r.Status != "failed") {
		fmt.Fprintln(stderr, "invalid activation result")
		return 1, true
	}
	store, err := Open(filepath.Dir(r.Source))
	if err != nil {
		fmt.Fprintln(stderr, err)
		return 1, true
	}
	defer store.Close()
	if err = store.SaveResult(r.Source, r); err != nil {
		fmt.Fprintln(stderr, err)
		return 1, true
	}
	return 0, true
}
