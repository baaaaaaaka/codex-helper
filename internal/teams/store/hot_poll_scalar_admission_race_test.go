//go:build race

package store

// The production compatibility lane deliberately has a two-second wall-clock
// budget. The invalid-page regression keeps a full 64-row malformed prefix in
// normal tests, but race instrumentation makes each SQLite/JSON operation
// materially slower and turns the test into a budget test rather than a
// correctness test. One row still exercises the malformed-row recovery lane
// and the healthy-tail admission invariant under -race; the normal build
// retains the full 64-row page-boundary fixture below.
func hotPollInvalidPageBadCount() int { return 1 }

// The normal build keeps the large semantic-malformed prefix to exercise
// keyset traversal beyond eight 64-row pages. Race instrumentation makes the
// canonical JSON compatibility scan materially slower and would turn that
// scale check into a production two-second-budget test; the race build keeps
// the healthy-tail and malformed-row admission invariant covered with one row.
func hotPollSemanticMalformedPollCount() int { return 1 }
