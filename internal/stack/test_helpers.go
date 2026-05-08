package stack

// NewStackForTest returns a minimal Stack for tests without SSH.
func NewStackForTest(httpPort, socksPort int) *Stack {
	return NewStackForTestWithCloseHook(httpPort, socksPort, nil)
}

// NewStackForTestWithCloseHook returns a minimal Stack for tests and invokes
// closeHook exactly once when the stack is first closed.
func NewStackForTestWithCloseHook(httpPort, socksPort int, closeHook func()) *Stack {
	return &Stack{
		HTTPPort:  httpPort,
		SocksPort: socksPort,
		fatalCh:   make(chan error),
		stopCh:    make(chan struct{}),
		closeHook: closeHook,
	}
}
