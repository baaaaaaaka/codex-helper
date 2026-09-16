//go:build !race

package store

func hotPollInvalidPageBadCount() int { return sqliteHotPollReadyLimit }

func hotPollSemanticMalformedPollCount() int { return 520 }

func hotPollJSONFrontierOperationalCount() int { return 60 }
