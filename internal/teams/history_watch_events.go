package teams

// historyWatchEventSource is only a hint for selecting files to inspect. It
// never replaces the stat-before/read/stat-after and identity checks applied
// to a selected transcript. An unavailable or uncertain source must report
// uncertain=true so the caller performs the complete native scan.
//
// When prune is false, paths is an incremental set whose parent directories
// should be watched; existing watches are retained. A complete path set is
// passed with prune=true at startup and reconciliation boundaries. Keeping
// this distinction out of the normal cycle avoids rebuilding the full
// historical directory set on every poll while reconciliation remains the
// completeness fence for directories that disappeared or appeared outside
// the recent window.
type historyWatchEventSource interface {
	Update(paths []string, prune bool) (dirty []string, uncertain bool, err error)
	Close() error
}

type fallbackHistoryWatchEventSource struct{}

func (fallbackHistoryWatchEventSource) Update([]string, bool) ([]string, bool, error) {
	return nil, true, nil
}

func (fallbackHistoryWatchEventSource) Close() error { return nil }
