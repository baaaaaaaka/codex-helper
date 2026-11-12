package harness

// ExpectedContinuationPrompt is the external instruction contract observed by
// the fake Codex server. Keep it independent from product implementation.
const ExpectedContinuationPrompt = "Continue the active unfinished task from its current state. Keep the previous response language unless the user requests otherwise. Verify interrupted actions before retrying; do not repeat completed work."
