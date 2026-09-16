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

// One healthy tail row is enough to prove that the bounded canonical fallback
// crosses a malformed prefix. Keeping the race fixture narrow avoids spending
// the production two-second compatibility budget decoding healthy rows that do
// not add another invariant to this test; the non-race build retains the larger
// quota-sized fixture.
func hotPollInvalidPageHealthyCount() int { return 1 }

// The race build only needs one healthy row to prove that a corrupt future row
// does not consume the ready admission slot. The normal build retains the
// larger fixture that also exercises filling the ordinary quota.
func hotPollReadyAdmissionHealthyCount() int { return 1 }

// The normal build keeps the large semantic-malformed prefix to exercise
// keyset traversal beyond eight 64-row pages. Race instrumentation makes the
// canonical JSON compatibility scan materially slower and would turn that
// scale check into a production two-second-budget test; the race build keeps
// the healthy-tail and malformed-row admission invariant covered with one row.
func hotPollSemanticMalformedPollCount() int { return 1 }

// Keep the ready-schedule regression focused on the healthy-tail and bounded
// malformed-row invariant under -race. The non-race build retains the larger
// prefix to exercise keyset traversal; race JSON evaluation must stay inside
// the production compatibility budget on hosted runners.
func hotPollReadyScheduleSemanticMalformedCount() int { return 1 }

// This admission fixture needs enough rows to exercise the control-chat
// reservation and stale-frontier fallback, but it is not the keyset-scale
// stress test. Keep the race build below the production two-second legacy
// compatibility budget; the normal build retains the full page-sized fixture.
func hotPollJSONFrontierOperationalCount() int { return 8 }
