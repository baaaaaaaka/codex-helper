//go:build !windows

package main

import (
	"context"
	"github.com/baaaaaaaka/codex-helper/internal/helperruntime"
	"os"
	"os/signal"
	"time"
)

func cancelSelf() {
	p, _ := os.FindProcess(os.Getpid())
	_ = p.Signal(os.Interrupt)
	time.Sleep(time.Second)
	os.Exit(130)
}
func handledCancel() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	p, _ := os.FindProcess(os.Getpid())
	_ = p.Signal(os.Interrupt)
	<-ctx.Done()
	helperruntime.CancelStartup()
	os.Exit(0)
}
