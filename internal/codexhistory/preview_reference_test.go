package codexhistory

import (
	"sort"
	"time"
)

func cloneMessageSeenState(in *messageSeenState) *messageSeenState {
	out := newMessageSeenState()
	if in == nil {
		return out
	}
	for key, value := range in.sourceKeys {
		out.sourceKeys[key] = value
	}
	for key, value := range in.fallbackTimes {
		out.fallbackTimes[key] = value
	}
	for key, value := range in.fallbackSourceKind {
		out.fallbackSourceKind[key] = value
	}
	return out
}

func diffMessageSeenState(before *messageSeenState, after *messageSeenState) sessionPreviewSeenDelta {
	delta := sessionPreviewSeenDelta{}
	if after == nil {
		return delta
	}
	for key := range after.sourceKeys {
		if before == nil || !before.sourceKeys[key] {
			delta.sourceKeys = append(delta.sourceKeys, key)
		}
	}
	sort.Strings(delta.sourceKeys)
	for key, timestamp := range after.fallbackTimes {
		previous, ok := time.Time{}, false
		if before != nil {
			previous, ok = before.fallbackTimes[key]
		}
		sourceKind := after.fallbackSourceKind[key]
		if ok && previous.Equal(timestamp) && before.fallbackSourceKind[key] == sourceKind {
			continue
		}
		delta.fallback = append(delta.fallback, persistentSessionPreviewFallbackMessage{
			Key:        key,
			Timestamp:  timestamp,
			SourceKind: sourceKind,
		})
	}
	sort.Slice(delta.fallback, func(i, j int) bool { return delta.fallback[i].Key < delta.fallback[j].Key })
	return delta
}

func historyPromptForTest(idx historyIndex, id string) string {
	value, _ := idx.lookup(id)
	return value.FirstPrompt
}
