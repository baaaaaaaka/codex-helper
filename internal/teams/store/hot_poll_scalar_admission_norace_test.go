//go:build !race

package store

func hotPollInvalidPageBadCount() int { return sqliteHotPollReadyLimit }
