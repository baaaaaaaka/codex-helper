//go:build !race

package store

func hotPollInvalidPageBadCount() int { return sqliteHotPollReadyLimit }

func hotPollInvalidPageHealthyCount() int { return 20 }

func hotPollReadyAdmissionHealthyCount() int { return 20 }

func hotPollSemanticMalformedPollCount() int { return 520 }

// The normal build keeps the prefix beyond one SQL page so the structural
// recovery test exercises keyset traversal as well as the healthy-tail
// invariant. The race build uses one malformed row for the same semantic
// assertion; the larger scale traversal is covered by the non-race test.
func hotPollStructurallyEmptyPendingPageCount() int { return sqliteHotPollReadyLimit + 8 }

func hotPollOperationalHintPrefixCount() int { return sqliteHotPollReadyLimit }

// The normal build keeps the page-sized semantic-malformed prefix so this
// schedule assertion also covers traversal beyond the bounded ready page. The
// race build uses one equivalent recovery row: race instrumentation makes the
// JSON compatibility oracle materially slower, so the scale check belongs to
// the non-race fixture while the invariant remains covered in both builds.
func hotPollReadyScheduleSemanticMalformedCount() int { return sqliteHotPollReadyLimit + 8 }

func hotPollJSONFrontierOperationalCount() int { return 60 }
