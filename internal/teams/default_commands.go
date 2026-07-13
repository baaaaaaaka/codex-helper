package teams

import (
	"context"
	"fmt"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/modelprofile"
)

type DefaultCommandAction string

const (
	DefaultCommandStatus DefaultCommandAction = "status"
	DefaultCommandList   DefaultCommandAction = "list"
	DefaultCommandSet    DefaultCommandAction = "set"
	DefaultCommandReset  DefaultCommandAction = "reset"
	DefaultCommandHelp   DefaultCommandAction = "help"
)

// DefaultCommand is transport-neutral. The Teams bridge owns the stable
// grammar while the configured manager owns the extensible setting registry,
// typed persistence, validation, and formatting.
type DefaultCommand struct {
	Setting string
	Action  DefaultCommandAction
	Value   string
}

type GlobalDefaultManager interface {
	HandleDefaultCommand(context.Context, DefaultCommand) (string, error)
	ResolveDefaultReasoningEffort(context.Context, modelprofile.Snapshot) (string, string, error)
}

func (b *Bridge) handleDefaultControlCommand(ctx context.Context, arg string) (string, error) {
	if b == nil || b.defaultManager == nil {
		return "Global default management is not configured for this Teams service.", nil
	}
	command, err := parseDefaultCommand(arg)
	if err != nil {
		return "", err
	}
	return b.defaultManager.HandleDefaultCommand(ctx, command)
}

func parseDefaultCommand(arg string) (DefaultCommand, error) {
	setting, rest := modelCommandParts(arg)
	if setting == "" || setting == "status" || setting == "current" {
		if strings.TrimSpace(rest) != "" {
			return DefaultCommand{}, fmt.Errorf("usage: `default status`")
		}
		return DefaultCommand{Action: DefaultCommandStatus}, nil
	}
	if setting == "help" || setting == "?" {
		return DefaultCommand{Action: DefaultCommandHelp}, nil
	}
	action, value := modelCommandParts(rest)
	if action == "" || action == "status" || action == "current" {
		return DefaultCommand{Setting: setting, Action: DefaultCommandStatus}, nil
	}
	switch action {
	case "list", "ls", "options", "choices":
		if value != "" {
			return DefaultCommand{}, fmt.Errorf("usage: `default %s list`", setting)
		}
		return DefaultCommand{Setting: setting, Action: DefaultCommandList}, nil
	case "set", "use", "switch":
		if value == "" {
			return DefaultCommand{}, fmt.Errorf("usage: `default %s set <value>`", setting)
		}
		return DefaultCommand{Setting: setting, Action: DefaultCommandSet, Value: value}, nil
	case "reset", "clear", "unset":
		if value != "" {
			return DefaultCommand{}, fmt.Errorf("usage: `default %s reset`", setting)
		}
		return DefaultCommand{Setting: setting, Action: DefaultCommandReset}, nil
	case "help", "?":
		return DefaultCommand{Setting: setting, Action: DefaultCommandHelp}, nil
	default:
		return DefaultCommand{}, fmt.Errorf("unknown default action %q; use `default %s status|list|set|reset`", action, setting)
	}
}

func defaultCommandWorkChatMessage() string {
	return "`default` changes global settings and can only be used in the Control chat. Use `model` or `effort` to change only this Work chat."
}

func (b *Bridge) reasoningEffortFromGlobalDefault(ctx context.Context, snapshot modelprofile.Snapshot) (string, string, error) {
	if b == nil || b.defaultManager == nil {
		return "", "", nil
	}
	return b.defaultManager.ResolveDefaultReasoningEffort(ctx, snapshot)
}
