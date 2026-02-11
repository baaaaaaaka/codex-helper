package codexhistory

// TODO(phase2): Implement Codex JSONL session message parsing

type Message struct {
	Role    string
	Content string
}

func ReadSessionMessages(path string, maxMessages int) ([]Message, error) {
	// TODO(phase2): parse JSONL session file
	return []Message{}, nil
}
