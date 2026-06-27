package migration

import "strings"

// These names are accepted only as hidden, local compatibility shims. CXP
// consumes them before launching Codex, so they never enter the child argv or
// Codex telemetry. Keep their spellings isolated in the migration package.
const (
	LegacyRunModeFlagName       = "yolo"
	LegacyBeaconSandboxFlagName = "no-yolo"
)

// RemoveLegacyCodexExecutionOverrides is the one compatibility boundary for
// persisted arguments written by older helper releases. The returned slice is
// safe to translate into the standard approval runtime; removed values are
// never forwarded to Codex.
func RemoveLegacyCodexExecutionOverrides(args []string) []string {
	out := make([]string, 0, len(args))
	for index := 0; index < len(args); index++ {
		arg := strings.TrimSpace(args[index])
		switch arg {
		case "--yolo", "--dangerously-bypass-approvals-and-sandbox":
			continue
		case "--sandbox", "-s", "--ask-for-approval", "-a":
			if index+1 < len(args) {
				index++
			}
			continue
		case "-c", "--config":
			if index+1 < len(args) && isLegacyExecutionConfig(args[index+1]) {
				index++
				continue
			}
		}
		if strings.HasPrefix(arg, "--sandbox=") || strings.HasPrefix(arg, "--ask-for-approval=") {
			continue
		}
		out = append(out, args[index])
	}
	return out
}

func isLegacyExecutionConfig(value string) bool {
	key, _, ok := strings.Cut(strings.TrimSpace(value), "=")
	if !ok {
		return false
	}
	key = strings.TrimSpace(key)
	return key == "sandbox_mode" || key == "approval_policy" || key == "approvals_reviewer"
}
