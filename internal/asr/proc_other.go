//go:build !linux && !windows

package asr

import "syscall"

func setDeathSignal(*syscall.SysProcAttr) {}
