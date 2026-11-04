package asr

import "syscall"

// setDeathSignal makes the kernel kill the child when the helper dies, so a
// resident llama-server never outlives the helper.
func setDeathSignal(attr *syscall.SysProcAttr) { attr.Pdeathsig = syscall.SIGKILL }
