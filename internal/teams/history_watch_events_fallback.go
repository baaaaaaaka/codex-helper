//go:build !linux

package teams

func newHistoryWatchEventSource() historyWatchEventSource {
	return fallbackHistoryWatchEventSource{}
}
