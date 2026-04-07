package cli

import "fmt"

type execIdentityRequired struct {
	home string
}

func (e *execIdentityRequired) Error() string {
	return fmt.Sprintf("refuse to use foreign codex home %q as root without a resolvable target uid/gid", e.home)
}
