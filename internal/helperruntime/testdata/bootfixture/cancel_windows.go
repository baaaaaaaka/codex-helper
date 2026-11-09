//go:build windows

package main

import (
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"os"
)

func cancelSelf()    { os.Exit(-1073741510) }
func handledCancel() { helperruntime.CancelStartup(); os.Exit(0) }
