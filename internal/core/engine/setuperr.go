package engine

import "strings"

// Setup errors (F-TRN-16): a turn Codex never started because of the
// helper's own setup gets v0.1.20's "setup issue on my side" notice with
// its class (bridge.go infraLaunchFailureNotice,
// missingNodeRuntimeFailureNotice, codexConfigurationFailureNotice). Every
// notice says the request did not run and how to run it again; the helper
// never retries it by itself. The diagnostic is the error without internal
// ids or paths (userError).

// errLaunchPrefix is the agent's error for an app-server that could not be
// started (codexagent.Run).
const errLaunchPrefix = "Codex could not be started"

// setupClass names the class of a not-started turn's error ("" when it is
// not a setup error).
func setupClass(errText string) string {
	lower := strings.ToLower(errText)
	switch {
	case strings.Contains(lower, "‘node’: no such file or directory"),
		strings.Contains(lower, "'node': no such file or directory"),
		strings.Contains(lower, `"node": executable file not found`),
		strings.Contains(lower, "node: not found"),
		strings.Contains(lower, "node.exe is not recognized"):
		return "node"
	case strings.Contains(lower, "cloudconfigbundle"):
		return "config-route"
	case strings.HasPrefix(errText, errLaunchPrefix):
		return "launch"
	}
	return ""
}

// diagnostic is the admin diagnostic line of a setup notice.
func diagnostic(errText string) string {
	return "Diagnostic for the admin:\n" + userError(errText)
}

// textNotStartedTurn is the notice of a turn Codex did not start.
func textNotStartedTurn(errText string) string {
	switch setupClass(errText) {
	case "node":
		return "⚠️ Codex did not start because the helper process could not find its Node.js runtime.\n\n" +
			"No user work ran or was duplicated. An administrator should repair or update CXP, then send `helper retry last` here.\n\n" +
			diagnostic(errText)
	case "config-route":
		return "⚠️ Codex could not load the workspace-managed configuration through the selected network route.\n\n" +
			"No Codex thread or user work was started, so retrying later will not duplicate work.\n" +
			"Once the network/proxy path is corrected, send `helper retry last` here.\n\n" +
			diagnostic(errText)
	case "launch":
		return textSetupIssue(errText)
	}
	return "Codex did not start this request (" + userError(errText) + "). It was not run; send `helper retry last` to run it."
}

// textSetupIssue is v0.1.20's infraLaunchFailureNotice.
func textSetupIssue(errText string) string {
	return "⚠️ I couldn't start Codex due to a setup issue on my side — this is not a problem with your request.\n\n" +
		"This usually means the Codex CLI or my own configuration needs attention (for example a version mismatch after an update). The helper will not automatically retry the user request after this failure, to avoid duplicating work.\n" +
		"Once it's sorted out, send `helper retry last` here.\n\n" +
		diagnostic(errText)
}

// textFailedTurn is the notice of a turn that failed in Codex. A setup
// class (an app-server that exited before the turn began) reads as one.
func textFailedTurn(errText string) string {
	if c := setupClass(errText); c == "node" || c == "config-route" {
		return textNotStartedTurn(errText)
	}
	return "Codex reported an error and the request did not finish: " + userError(errText) + "\n\nIt was not re-run. Send `helper retry last` to run it again."
}
