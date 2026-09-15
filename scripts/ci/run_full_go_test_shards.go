// Command run_full_go_test_shards runs the full Go test suite while isolating
// large or host-sensitive packages into independently compiled test processes. The
// package test names are discovered from `go test -list`, partitioned by
// disjoint name prefixes, and checked so every runnable Test/Example/Fuzz
// entry is selected exactly once.
//
// This is intended for the non-Linux full-suite CI jobs, where the Linux job
// also owns the single combined coverage profile.  Splitting only the large
// packages keeps the platform jobs fast without dropping tests or changing
// their assertions.
//go:build ignore

package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	defaultShardCount = 16
	maxConcurrentJobs = 4
)

var runnableNamePattern = regexp.MustCompile(`^(Test|Example|Fuzz)[A-Za-z0-9_]*$`)

// Host-sensitive test families intentionally exercise long-lived listener
// state, timing-sensitive error isolation, or process-wide performance
// fixtures. These tests are independently correct but share process-global
// test plumbing with older package fixtures. Keep them in their own test
// process rather than allowing unrelated tests to make their timing assertions
// nondeterministic.
var isolatedRunnableNames = map[string]map[string]bool{
	"./internal/cli": {
		// This fixture starts a real Codex-shaped process tree and asserts
		// bounded process-group cancellation. Keep it out of the ordinary
		// package pool, where unrelated test processes can make the PID and
		// signal observation nondeterministic.
		"TestMigrateCodexRolloutBeforeTUIHonorsCancellationAndProcessGroup": true,
		// App Gateway daemon tests observe short registration, cooldown, and
		// restart windows. Running the package beside Teams/store shards can
		// delay those observations on hosted Windows runners even though the
		// daemon fixture itself is isolated in a temporary directory.
		"TestRunAppGatewayDaemonKeepsStableFrontendWhileBackendRuns":            true,
		"TestRunAppGatewayDaemonDoesNotConsumeLegacyBlockedBudget":              true,
		"TestRunAppGatewayDaemonModernStandbyDNSGapThenRecoveryKeepsClientPort": true,
		"TestRunAppGatewayDaemonBoundsBackendRecoveryBeforeCooldown":            true,
		"TestRunAppGatewayDaemonBackendSwapKeepsFrontendPort":                   true,
		"TestRunAppGatewayDaemonRestartReusesStablePort":                        true,
		// These tests launch a detached child and observe a short /proc reaping
		// window. Keep their process-tree observation away from concurrent race
		// shard pressure so an exited PID cannot be sampled through a stale read.
		"TestProxyStartBackgroundReapsExitedDetachedChild":     true,
		"TestStartCodexAppProxyDaemonReapsExitedDetachedChild": true,
	},
	"./internal/tui": {
		// This test drives a real refresh ticker and has a short semantic
		// context. Keep its scheduler observation independent from the large
		// race-test shard pool; changing the deadline would hide the CI
		// scheduling problem rather than make the test more reliable.
		"TestSelectSessionAutoRefreshUpdatesThreadNameTitle": true,
	},
	"./internal/teams": {
		"TestBridgeLinkedTranscriptConcurrentSQLiteSyncPublishesExactlyOnce": true,
		"TestCXPPerfModelExternalScenariosCoverCommonPaths":                  true,
		"TestTeamsListenFalseGraphWorkerSaturationPreservesHealthyPoll":      true,
		// This liveness fixture must observe an actual listener scheduling
		// window. Keep it out of the broad shard pool, where unrelated race and
		// SQLite processes can consume the hosted runner before its first poll.
		"TestTeamsListenFalseGraphHeadFailureDoesNotStarveHealthyTail":             true,
		"TestTeamsListenFalseGraphStatefulHeadContinuationDrainsTerminalPage":      true,
		"TestTeamsListenFalseHistoryWatchFullPoolDoesNotStarveHealthyTail":         true,
		"TestTeamsListenFalseUsesConfiguredRunnerStreaming":                        true,
		"TestTeamsListenFalseLinkedTranscriptSessionErrorDoesNotStarveHealthyTail": true,
		"TestTeamsListenFalseLinkedTranscriptSlowHeadDoesNotStarveHealthyTail":     true,
		"TestTeamsListenFalseLinkedTranscriptFullPoolDoesNotStarveHealthyTail":     true,
		"TestTeamsListenFalseHistoryWatchSlowHeadDoesNotStarveHealthyTail":         true,
		"TestTeamsListenFalseOwnerLossCancelsHistoryWatchBeforeStaleCommit":        true,
		"TestTeamsListenFalseOwnerLossFencesCooperativeTurn":                       true,
		"TestTeamsListenFalseTaskStartedPromptRaceRecoversAfterNextCycle":          true,
		"TestTeamsListenFalsePollPhaseTimeoutDoesNotPoisonNextCycle":               true,
		"TestTeamsListenFalseSlowInboundMutationDoesNotConsumeDurableCleanupGrace": true,
		"TestTeamsListenFalseStartupHeartbeatProtectsSlowInitialization":           true,
		"TestTeamsListenFalseCurrentStateReplayMatrix":                             true,
		"TestTeamsListenFalseSQLiteTranscriptBacklogProgresses":                    true,
		"TestTeamsListenFalseMalformedActiveSQLitePollDoesNotBaseline":             true,
		"TestTeamsListenFalsePollFrontierSurvivesStoreReopenAndOwnerTakeover":      true,
		"TestTeamsListenFalsePollContinuationSurvivesReopenBeforeDrain":            true,
		"TestTeamsListenFalseShutdownDoesNotRunAsyncTurnFollowupAfterGrace":        true,
		"TestTeamsListenFalseMalformedPollDoesNotBlockHealthyChat":                 true,
		"TestBridgeSyncLinkedTranscriptReleasesPendingRootAcrossSQLiteStoreReopen": true,
		"TestTeamsOwnershipStressGraphStallDoesNotStopOtherChatPollCI":             true,
		"TestTeamsOwnershipStressGraphStallThenTranscriptCatchupCI":                true,
		"TestTeamsOwnershipStressTranscriptCatchupWhileTUIContinuesCI":             true,
		"TestTeamsOwnershipStressContinuationFailureIsIsolatedByPollOnceCI":        true,
		"TestTeamsOwnershipStressFifthChatReachesNextWorkerWaveCI":                 true,
		"TestTeamsOwnershipStressPagedBacklogAfterServiceOutageCI":                 true,
		"TestTeamsOwnershipStressSQLiteHeartbeatSurvivesSaturatedGraphWorkersCI":   true,
		"TestCXPPerfModelSQLiteExternalScenariosCoverCommonPaths":                  true,
		"TestCXPPerfModelProfilesCanSeedStoreAndPoll":                              true,
		"TestTeamsUnresolvedTranscriptOutboxDoesNotLivelockHealthyTail":            true,
		"TestTeamsOutboxAcceptedResponseFinishesAfterPhaseDeadline":                true,
		// This listener test starts a real continuous loop over a file-backed
		// store.  Keep startup/recovery timing independent from unrelated
		// package tests; the test's own Graph fixture already covers the
		// concurrency boundary it needs.
		"TestTeamsListenFalseRecoversExpiredAmbiguousOutboxWithoutPost": true,
	},
	"./internal/teams/store": {
		"TestSQLiteHotPollAdmissionBoundsSemanticallyMalformedPollLaneAndPreservesHealthyChat": true,
		"TestSQLiteSemanticallyMalformedOutboxRowsDoNotHideHealthyWork":                        true,
		"TestSQLiteHotPollWorkCandidatesRotateOperationalRowsBeyondLimit":                      true,
		// This cross-backend owner-fencing test migrates a file-backed store to
		// SQLite. On Windows, modernc SQLite may block in FlushFileBuffers when
		// unrelated store shards share the hosted runner. Keep the migration
		// observation isolated instead of weakening its finite assertions.
		"TestStoreHistoryWatchOwnerCapabilityFencesTakeoverAcrossBackends": true,
		// This cross-backend legacy-owner test performs the same durable lease
		// migration and can spend its whole short budget in a Windows SQLite
		// commit. Keep its two backend assertions in a clean process as well.
		"TestStoreOwnerBindsLegacyQueuedTurnAndRejectsPreviousOwnerCallbacks": true,
	},
}

// A small subset of the isolated fixtures also needs host-level resource
// isolation. A separate test process prevents package globals from leaking,
// but it does not prevent other shard processes from consuming the hosted
// runner's scheduler and filesystem while a finite liveness observation is in
// progress.
var exclusiveRunnableNames = map[string]map[string]bool{
	"./internal/cli": {
		"TestMigrateCodexRolloutBeforeTUIHonorsCancellationAndProcessGroup":     true,
		"TestRunAppGatewayDaemonKeepsStableFrontendWhileBackendRuns":            true,
		"TestRunAppGatewayDaemonDoesNotConsumeLegacyBlockedBudget":              true,
		"TestRunAppGatewayDaemonModernStandbyDNSGapThenRecoveryKeepsClientPort": true,
		"TestRunAppGatewayDaemonBoundsBackendRecoveryBeforeCooldown":            true,
		"TestRunAppGatewayDaemonBackendSwapKeepsFrontendPort":                   true,
		"TestRunAppGatewayDaemonRestartReusesStablePort":                        true,
		"TestProxyStartBackgroundReapsExitedDetachedChild":                      true,
		"TestStartCodexAppProxyDaemonReapsExitedDetachedChild":                  true,
	},
	"./internal/tui": {
		"TestSelectSessionAutoRefreshUpdatesThreadNameTitle": true,
	},
	"./internal/teams": {
		// These tests already run in their own process, but their first listener
		// cycle is itself the assertion.  Do not start them beside other shard
		// processes that can consume the hosted runner before Graph admission.
		"TestTeamsListenFalsePollPhaseTimeoutDoesNotPoisonNextCycle":               true,
		"TestTeamsListenFalseSlowInboundMutationDoesNotConsumeDurableCleanupGrace": true,
		"TestTeamsListenFalseGraphWorkerSaturationPreservesHealthyPoll":            true,
		"TestTeamsListenFalseGraphHeadFailureDoesNotStarveHealthyTail":             true,
		"TestTeamsListenFalseGraphStatefulHeadContinuationDrainsTerminalPage":      true,
		"TestTeamsListenFalseHistoryWatchFullPoolDoesNotStarveHealthyTail":         true,
		"TestTeamsListenFalseLinkedTranscriptFullPoolDoesNotStarveHealthyTail":     true,
		"TestTeamsListenFalseUsesConfiguredRunnerStreaming":                        true,
		"TestTeamsListenFalseSQLiteTranscriptBacklogProgresses":                    true,
		"TestTeamsListenFalsePollFrontierSurvivesStoreReopenAndOwnerTakeover":      true,
		"TestTeamsListenFalsePollContinuationSurvivesReopenBeforeDrain":            true,
		"TestTeamsListenFalseShutdownDoesNotRunAsyncTurnFollowupAfterGrace":        true,
		"TestTeamsListenFalseOwnerLossCancelsHistoryWatchBeforeStaleCommit":        true,
		"TestTeamsListenFalseOwnerLossFencesCooperativeTurn":                       true,
		"TestTeamsListenFalseRecoversExpiredAmbiguousOutboxWithoutPost":            true,
		"TestTeamsListenFalseStartupHeartbeatProtectsSlowInitialization":           true,
		"TestTeamsListenFalseMalformedPollDoesNotBlockHealthyChat":                 true,
		"TestBridgeSyncLinkedTranscriptReleasesPendingRootAcrossSQLiteStoreReopen": true,
		"TestTeamsOwnershipStressTranscriptCatchupWhileTUIContinuesCI":             true,
		"TestTeamsOwnershipStressFifthChatReachesNextWorkerWaveCI":                 true,
		"TestTeamsOutboxAcceptedResponseFinishesAfterPhaseDeadline":                true,
		// These outbox regressions exercise durable SQLite/file boundaries in
		// addition to fairness. Keep the process isolated and serialize it on the
		// hosted runner so unrelated shard I/O cannot turn the durable assertion
		// into another readiness tail.
		"TestTeamsMainLoopOutboxFairnessBypassesPersistentGraphFailurePrefix":  true,
		"TestTeamsMainLoopOutboxFairnessCursorWalksPastDistinctChatScanPrefix": true,
		"TestTeamsMainLoopOutboxFairnessWalksPastDistinctChatScanPrefix":       true,
		"TestTeamsMainLoopOutboxLedgerFailureDoesNotStarveHealthyTail":         true,
		"TestTeamsUnresolvedTranscriptOutboxDoesNotLivelockHealthyTail":        true,
	},
	"./internal/teams/store": {
		"TestSQLiteHotPollAdmissionBoundsSemanticallyMalformedPollLaneAndPreservesHealthyChat": true,
		"TestSQLiteSemanticallyMalformedOutboxRowsDoNotHideHealthyWork":                        true,
		"TestSQLiteHotPollWorkCandidatesRotateOperationalRowsBeyondLimit":                      true,
		"TestStoreHistoryWatchOwnerCapabilityFencesTakeoverAcrossBackends":                     true,
		"TestStoreOwnerBindsLegacyQueuedTurnAndRejectsPreviousOwnerCallbacks":                  true,
	},
}

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }

func (s *stringList) Set(value string) error {
	value = strings.TrimSpace(value)
	if value == "" {
		return errors.New("package must not be empty")
	}
	*s = append(*s, value)
	return nil
}

type prefixBucket struct {
	prefix string
	names  []string
	exact  bool
}

type shardPlan struct {
	packageName string
	prefixes    []string
	count       int
	weight      int
	exclusive   bool
}

type testJob struct {
	label     string
	args      []string
	weight    int
	exclusive bool
}

type jobResult struct {
	label    string
	err      error
	duration time.Duration
}

func main() {
	shards := flag.Int("shards", defaultShardCount, "maximum number of shards per large package")
	parallel := flag.Int("parallel", 16, "go test -parallel value")
	testTimeout := flag.Duration("timeout", 20*time.Minute, "per-shard go test timeout")
	race := flag.Bool("race", false, "pass -race to go test")
	listOnly := flag.Bool("list-only", false, "print the plan without executing tests")
	partitionCount := flag.Int("partition-count", 1, "number of independent hosted-runner partitions")
	partitionIndex := flag.Int("partition-index", 0, "zero-based hosted-runner partition index")
	var requestedPackages stringList
	flag.Var(&requestedPackages, "package", "package to include; may be repeated (default: go list ./...)")
	flag.Parse()

	if *shards <= 0 {
		fatal(errors.New("shards must be positive"))
	}
	if *parallel <= 0 {
		fatal(errors.New("parallel must be positive"))
	}
	if *testTimeout <= 0 {
		fatal(errors.New("timeout must be positive"))
	}
	if *partitionCount <= 0 {
		fatal(errors.New("partition-count must be positive"))
	}
	if *partitionIndex < 0 || *partitionIndex >= *partitionCount {
		fatal(fmt.Errorf("partition-index %d is outside partition-count %d", *partitionIndex, *partitionCount))
	}

	packages := []string(requestedPackages)
	if len(packages) == 0 {
		var err error
		packages, err = listPackages()
		if err != nil {
			fatal(err)
		}
	}

	jobs, err := makeJobs(packages, *shards, *parallel, *testTimeout, *race)
	if err != nil {
		fatal(err)
	}
	jobs = partitionTestJobs(jobs, *partitionCount, *partitionIndex)
	if len(jobs) == 0 {
		fatal(fmt.Errorf("partition %d/%d selected no test jobs", *partitionIndex+1, *partitionCount))
	}
	if *listOnly {
		for _, job := range jobs {
			fmt.Printf("%s: go", job.label)
			for _, arg := range job.args {
				fmt.Printf(" %q", arg)
			}
			fmt.Printf(" # estimated-weight=%d estimated-exclusive=%t\n", job.weight, job.exclusive)
		}
		return
	}

	results := runJobs(jobs, *testTimeout)
	var failed []jobResult
	for _, result := range results {
		if result.err != nil {
			failed = append(failed, result)
		}
	}
	if len(failed) != 0 {
		for _, result := range failed {
			fmt.Fprintf(flag.CommandLine.Output(), "full test job failed: %s: %v\n", result.label, result.err)
		}
		os.Exit(1)
	}
	fmt.Printf("full test shards passed: %d job(s)\n", len(jobs))
}

// partitionTestJobs assigns complete test processes to independent hosted
// runners. This is deliberately done after makeJobs has established the
// exact-once name coverage and after host-sensitive tests have been marked
// exclusive. A partition therefore never runs half of a test, and the
// existing exclusive-before-parallel ordering is retained within each runner.
// The runners have independent filesystems, so this increases wall-clock
// parallelism without introducing the same-runner SQLite contention that
// prevents increasing maxConcurrentJobs safely.
//
// Jobs are placed by descending estimated work rather than by index modulo.
// Large package shards use their discovered test-name count as the estimate;
// isolated semantic families use a small conservative multiplier. Exclusive
// and parallel jobs are assigned independently because the former is a serial
// phase while the latter is drained by a four-process worker pool. The estimate
// is only for cross-runner bin packing: every selected command still runs with
// its original timeout, race mode, selector, and assertions.
func partitionTestJobs(jobs []testJob, partitionCount, partitionIndex int) []testJob {
	if partitionCount <= 1 {
		return jobs
	}
	assignments := make([]int, len(jobs))
	for _, exclusive := range []bool{true, false} {
		indices := make([]int, 0, len(jobs))
		for index := range jobs {
			if jobs[index].exclusive == exclusive {
				indices = append(indices, index)
			}
			if jobs[index].weight <= 0 {
				jobs[index].weight = 1
			}
		}
		assignBalancedJobIndices(jobs, indices, partitionCount, assignments)
	}
	selected := make([]testJob, 0, (len(jobs)+partitionCount-1)/partitionCount)
	for index, job := range jobs {
		if assignments[index] == partitionIndex {
			selected = append(selected, job)
		}
	}
	return selected
}

// assignBalancedJobIndices uses deterministic largest-first packing for one
// execution phase. Keeping phases separate prevents a large parallel shard from
// hiding a serial exclusive wave (or vice versa) in a single aggregate score.
func assignBalancedJobIndices(jobs []testJob, indices []int, partitionCount int, assignments []int) {
	if len(indices) == 0 {
		return
	}
	type partitionBin struct {
		weight int
		jobs   int
	}
	bins := make([]partitionBin, partitionCount)
	sort.SliceStable(indices, func(i, j int) bool {
		left, right := jobs[indices[i]], jobs[indices[j]]
		if left.weight != right.weight {
			return left.weight > right.weight
		}
		return left.label < right.label
	})
	for _, jobIndex := range indices {
		best := 0
		for index := 1; index < len(bins); index++ {
			if bins[index].weight < bins[best].weight ||
				(bins[index].weight == bins[best].weight && bins[index].jobs < bins[best].jobs) {
				best = index
			}
		}
		assignments[jobIndex] = best
		bins[best].weight += jobs[jobIndex].weight
		bins[best].jobs++
	}
}

func estimatedIsolatedJobWeight(packageName, name string) int {
	// A discovered test name is one logical job even when it contains many
	// subtests. These families are known to perform bounded listener, process,
	// or SQLite observations, so give them a conservative scheduling weight to
	// avoid placing all of the long single-test jobs in one hosted partition.
	if isTeamsRecoveryPackage(packageName) {
		switch {
		case strings.HasPrefix(name, "TestTeamsListenFalse"), strings.HasPrefix(name, "TestTeamsOwnershipStress"):
			return 8
		case strings.HasPrefix(name, "TestTeamsMainLoopOutbox"), strings.Contains(name, "SQLite"):
			return 4
		case strings.Contains(name, "Stress"), strings.Contains(name, "Outbox"):
			return 4
		}
	}
	if isCodexRunnerPackage(packageName) || strings.Contains(name, "AppGateway") {
		return 4
	}
	return 1
}

func estimatedRegularJobWeight(testNameCount int) int {
	if testNameCount <= 0 {
		return 1
	}
	// Ordinary packages are deliberately kept as one process, so their raw
	// name count is not comparable to a large-package shard. A conservative
	// eight-name bucket keeps a 1,400-name CLI package near the cost of one
	// 250-name Teams shard while still leaving the full package invocation
	// intact.
	return (testNameCount + 7) / 8
}

func fatal(err error) {
	fmt.Fprintln(flag.CommandLine.Output(), err)
	os.Exit(1)
}

func listPackages() ([]string, error) {
	output, err := commandOutput("go", "list", "./...")
	if err != nil {
		return nil, fmt.Errorf("list Go packages: %w", err)
	}
	var packages []string
	for _, line := range strings.Split(string(output), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			packages = append(packages, line)
		}
	}
	if len(packages) == 0 {
		return nil, errors.New("go list returned no packages")
	}
	return packages, nil
}

func makeJobs(packages []string, shardCount, parallel int, testTimeout time.Duration, race bool) ([]testJob, error) {
	var ordinary []string
	var ordinaryIsolated []testJob
	var plans []shardPlan
	for _, packageName := range packages {
		if !isLargePackage(packageName) {
			if len(isolatedRunnableNamesForPackage(packageName)) == 0 && !isCodexRunnerPackage(packageName) {
				ordinary = append(ordinary, packageName)
				continue
			}
			// Ordinary packages may still contain a small, reviewed family of
			// host-sensitive tests. Discover the names before constructing the
			// ordinary job so semantic families receive the same exact-once
			// process/resource boundary as large packages.
			names, err := listRunnableNames(packageName, race)
			if err != nil {
				return nil, err
			}
			isolated := runnableIsolationMap(packageName, names)
			if len(isolated) == 0 {
				ordinary = append(ordinary, packageName)
				continue
			}
			var isolatedNames []string
			var regularNames []string
			for _, name := range names {
				if isolated[name] {
					isolatedNames = append(isolatedNames, name)
				} else {
					regularNames = append(regularNames, name)
				}
			}
			if len(isolatedNames) == 0 {
				// A platform-specific isolated test may not be compiled on this
				// runner. Keep the package in the ordinary invocation in that case.
				ordinary = append(ordinary, packageName)
				continue
			}
			sort.Strings(isolatedNames)
			if len(regularNames) != 0 {
				args := []string{"test"}
				if race {
					args = append(args, "-race")
				}
				args = append(args,
					fmt.Sprintf("-timeout=%s", testTimeout),
					fmt.Sprintf("-parallel=%d", parallel),
					"-count=1",
					packageName,
					"-skip",
					exactRunnablePattern(isolatedNames),
				)
				ordinaryIsolated = append(ordinaryIsolated, testJob{
					label:  fmt.Sprintf("%s ordinary tests (isolated names skipped)", packageName),
					args:   args,
					weight: estimatedRegularJobWeight(len(regularNames)),
				})
			}
			exclusive := runnableExclusivityMap(packageName, names)
			for _, name := range isolatedNames {
				args := []string{"test"}
				if race {
					args = append(args, "-race")
				}
				args = append(args,
					fmt.Sprintf("-timeout=%s", testTimeout),
					fmt.Sprintf("-parallel=%d", parallel),
					"-count=1",
					packageName,
					"-run",
					exactRunnablePattern([]string{name}),
				)
				ordinaryIsolated = append(ordinaryIsolated, testJob{
					label:     fmt.Sprintf("%s isolated test %s", packageName, name),
					args:      args,
					weight:    estimatedIsolatedJobWeight(packageName, name),
					exclusive: exclusive[name],
				})
			}
			continue
		}
		names, err := listRunnableNames(packageName, race)
		if err != nil {
			return nil, err
		}
		if len(names) == 0 {
			// Keep an empty test package in the ordinary invocation so it is
			// still compiled and checked like it is by `go test ./...`.
			ordinary = append(ordinary, packageName)
			continue
		}
		var isolatedNames []string
		isolated := runnableIsolationMap(packageName, names)
		exclusive := runnableExclusivityMap(packageName, names)
		for _, name := range names {
			if isolated[name] {
				isolatedNames = append(isolatedNames, name)
			}
		}
		packagePlans, planErr := planPackageShards(packageName, names, shardCount, isolated)
		if planErr != nil {
			return nil, planErr
		}
		plans = append(plans, packagePlans...)
		for _, name := range isolatedNames {
			plans = append(plans, shardPlan{
				packageName: packageName,
				prefixes:    []string{regexp.QuoteMeta(name) + "$"},
				count:       1,
				weight:      estimatedIsolatedJobWeight(packageName, name),
				exclusive:   exclusive[name],
			})
		}
		if err := validatePackagePlans(names, plansForPackage(plans, packageName)); err != nil {
			return nil, err
		}
	}
	sort.Strings(ordinary)
	sort.Slice(plans, func(i, j int) bool {
		if plans[i].packageName != plans[j].packageName {
			return plans[i].packageName < plans[j].packageName
		}
		return plans[i].prefixes[0] < plans[j].prefixes[0]
	})

	var jobs []testJob
	if len(ordinary) != 0 {
		args := []string{"test"}
		if race {
			args = append(args, "-race")
		}
		args = append(args, fmt.Sprintf("-timeout=%s", testTimeout), fmt.Sprintf("-parallel=%d", parallel), "-count=1")
		args = append(args, ordinary...)
		jobs = append(jobs, testJob{label: "ordinary packages", args: args, weight: len(ordinary)})
	}
	sort.Slice(ordinaryIsolated, func(i, j int) bool { return ordinaryIsolated[i].label < ordinaryIsolated[j].label })
	jobs = append(jobs, ordinaryIsolated...)
	planTotals := make(map[string]int)
	for _, plan := range plans {
		planTotals[plan.packageName]++
	}
	planIndexes := make(map[string]int)
	for _, plan := range plans {
		planIndexes[plan.packageName]++
		planIndex := planIndexes[plan.packageName]
		pattern := planPattern(plan)
		args := []string{"test"}
		if race {
			args = append(args, "-race")
		}
		args = append(args,
			fmt.Sprintf("-timeout=%s", testTimeout),
			fmt.Sprintf("-parallel=%d", parallel),
			"-count=1",
			plan.packageName,
			"-run",
			pattern,
		)
		jobs = append(jobs, testJob{
			label:     fmt.Sprintf("%s shard %d/%d (%d test names)", plan.packageName, planIndex, planTotals[plan.packageName], plan.count),
			args:      args,
			weight:    plan.weight,
			exclusive: plan.exclusive,
		})
	}
	return jobs, nil
}

func isLargePackage(packageName string) bool {
	packageName = strings.TrimSuffix(strings.TrimSpace(packageName), "/")
	if packageName == "./internal/tui" || strings.HasSuffix(packageName, "/internal/tui") {
		return true
	}
	return packageName == "./internal/teams" ||
		strings.HasSuffix(packageName, "/internal/teams") ||
		packageName == "./internal/teams/store" ||
		strings.HasSuffix(packageName, "/internal/teams/store")
}

func isolatedRunnableNamesForPackage(packageName string) map[string]bool {
	if names, ok := isolatedRunnableNames[packageName]; ok {
		return names
	}
	if strings.HasSuffix(packageName, "/internal/tui") {
		return isolatedRunnableNames["./internal/tui"]
	}
	if strings.HasSuffix(packageName, "/internal/cli") {
		return isolatedRunnableNames["./internal/cli"]
	}
	if strings.HasSuffix(packageName, "/internal/teams/store") {
		return isolatedRunnableNames["./internal/teams/store"]
	}
	if strings.HasSuffix(packageName, "/internal/teams") {
		return isolatedRunnableNames["./internal/teams"]
	}
	return nil
}

// runnableIsolationMap combines the reviewed exact-name list with bounded
// semantic families whose members are expected to grow as regressions are
// added. Keeping the family rule here means a newly-added listener liveness
// case cannot silently fall back into a broad shard until someone remembers
// to edit a second static map.
func runnableIsolationMap(packageName string, names []string) map[string]bool {
	base := isolatedRunnableNamesForPackage(packageName)
	isolated := make(map[string]bool, len(base)+len(names))
	for name := range base {
		isolated[name] = true
	}
	for _, name := range names {
		if autoIsolatedRunnableName(packageName, name) {
			isolated[name] = true
		}
	}
	return isolated
}

func autoIsolatedRunnableName(packageName, name string) bool {
	if isTeamsRecoveryPackage(packageName) {
		// Every TestTeamsListenFalse case drives the continuous listener through
		// a finite readiness/recovery window. Keeping the family rule broad
		// prevents a newly-added listener regression (for example a SQLite
		// admission flood) from silently joining a shard with unrelated test
		// processes. The outbox family is process-isolated because it owns temporary
		// stores and scheduler state, but it does not observe host-level scheduling;
		// host exclusivity is assigned separately below.
		return strings.HasPrefix(name, "TestTeamsListenFalse") ||
			strings.HasPrefix(name, "TestTeamsMainLoopOutbox") ||
			strings.HasPrefix(name, "TestTeamsThirdPartyCacheStress") ||
			strings.HasPrefix(name, "TestTeamsOwnershipStress") ||
			strings.HasPrefix(name, "TestTeamsGraph429Stress") ||
			name == "TestTeamsWorkChatAudienceLookupUsesPollBudget"
	}
	if isCodexRunnerPackage(packageName) {
		// These fixtures start an OS wrapper and a long-lived descendant, then
		// assert that Close tears down the whole tree. Their short PID/readiness
		// and cleanup windows are real host observations; unrelated full-suite
		// processes can delay PowerShell/tasklist without changing the product
		// behavior under test.
		return strings.HasPrefix(name, "TestAppServerProcessCloseTerminates")
	}
	return false
}

// autoExclusiveRunnableName is deliberately narrower than
// autoIsolatedRunnableName. Process isolation protects package globals and
// temporary stores; host exclusivity is reserved for fixtures whose semantic
// deadline observes runner-wide scheduling, process trees, or host I/O. Most
// outbox fairness tests can therefore remain in their own process while
// sharing the bounded worker pool with ordinary shards; explicitly durable
// outbox regressions stay in the reviewed exclusive map above.
func autoExclusiveRunnableName(packageName, name string) bool {
	if isTeamsRecoveryPackage(packageName) {
		// Cache-stress and audience-admission fixtures also make short async or
		// Graph-budget observations; keep them away from unrelated shard pressure.
		return strings.HasPrefix(name, "TestTeamsListenFalse") ||
			strings.HasPrefix(name, "TestTeamsThirdPartyCacheStress") ||
			strings.HasPrefix(name, "TestTeamsOwnershipStress") ||
			strings.HasPrefix(name, "TestTeamsGraph429Stress") ||
			name == "TestTeamsWorkChatAudienceLookupUsesPollBudget"
	}
	if isCodexRunnerPackage(packageName) {
		return strings.HasPrefix(name, "TestAppServerProcessCloseTerminates")
	}
	return false
}

func isCodexRunnerPackage(packageName string) bool {
	packageName = strings.TrimSuffix(strings.TrimSpace(packageName), "/")
	return packageName == "./internal/codexrunner" || strings.HasSuffix(packageName, "/internal/codexrunner")
}

func isTeamsRecoveryPackage(packageName string) bool {
	packageName = strings.TrimSuffix(strings.TrimSpace(packageName), "/")
	return packageName == "./internal/teams" || strings.HasSuffix(packageName, "/internal/teams")
}

func exclusiveRunnableNamesForPackage(packageName string) map[string]bool {
	if names, ok := exclusiveRunnableNames[packageName]; ok {
		return names
	}
	if strings.HasSuffix(packageName, "/internal/tui") {
		return exclusiveRunnableNames["./internal/tui"]
	}
	if strings.HasSuffix(packageName, "/internal/cli") {
		return exclusiveRunnableNames["./internal/cli"]
	}
	if strings.HasSuffix(packageName, "/internal/teams/store") {
		return exclusiveRunnableNames["./internal/teams/store"]
	}
	if strings.HasSuffix(packageName, "/internal/teams") {
		return exclusiveRunnableNames["./internal/teams"]
	}
	return nil
}

func runnableExclusivityMap(packageName string, names []string) map[string]bool {
	base := exclusiveRunnableNamesForPackage(packageName)
	exclusive := make(map[string]bool, len(base)+len(names))
	for name := range base {
		exclusive[name] = true
	}
	for _, name := range names {
		if autoExclusiveRunnableName(packageName, name) {
			exclusive[name] = true
		}
	}
	return exclusive
}

func plansForPackage(plans []shardPlan, packageName string) []shardPlan {
	selected := make([]shardPlan, 0)
	for _, plan := range plans {
		if plan.packageName == packageName {
			selected = append(selected, plan)
		}
	}
	return selected
}

func validatePackagePlans(names []string, plans []shardPlan) error {
	if len(plans) == 0 {
		return errors.New("package has no shard plans")
	}
	compiled := make([]*regexp.Regexp, 0, len(plans))
	for _, plan := range plans {
		pattern := planPattern(plan)
		compiled = append(compiled, regexp.MustCompile(pattern))
	}
	for _, name := range names {
		matches := 0
		for _, pattern := range compiled {
			if pattern.MatchString(name) {
				matches++
			}
		}
		if matches != 1 {
			return fmt.Errorf("runnable name %q belongs to %d package plans", name, matches)
		}
	}
	return nil
}

func listRunnableNames(packageName string, race bool) ([]string, error) {
	args := []string{"test"}
	if race {
		args = append(args, "-race")
	}
	args = append(args, packageName, "-run", "^$", "-list", "^(Test|Example|Fuzz)")
	output, err := commandOutput("go", args...)
	if err != nil {
		return nil, fmt.Errorf("list runnable names in %s: %w", packageName, err)
	}
	seen := make(map[string]bool)
	var names []string
	for _, raw := range strings.Split(string(output), "\n") {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		if strings.HasPrefix(name, "Test") || strings.HasPrefix(name, "Example") || strings.HasPrefix(name, "Fuzz") {
			if !runnableNamePattern.MatchString(name) {
				return nil, fmt.Errorf("%s returned unsupported runnable name %q", packageName, name)
			}
			if seen[name] {
				return nil, fmt.Errorf("%s returned duplicate runnable name %q", packageName, name)
			}
			seen[name] = true
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, nil
}

func planPackageShards(packageName string, names []string, shardCount int, isolated map[string]bool) ([]shardPlan, error) {
	var regularNames []string
	for _, name := range names {
		if !isolated[name] {
			regularNames = append(regularNames, name)
		}
	}
	if len(regularNames) == 0 {
		return nil, nil
	}
	perShard := (len(regularNames) + shardCount - 1) / shardCount
	buckets := splitPrefixBuckets(names, "", perShard, isolated)
	regularBuckets := make([]prefixBucket, 0, len(buckets))
	for _, bucket := range buckets {
		for _, name := range bucket.names {
			if isolated[name] {
				if len(bucket.names) != 1 {
					return nil, fmt.Errorf("%s isolated test %q shares a shard prefix with another test", packageName, name)
				}
				goto nextBucket
			}
		}
		regularBuckets = append(regularBuckets, bucket)
	nextBucket:
	}
	if len(regularBuckets) == 0 {
		return nil, fmt.Errorf("%s produced no regular shard buckets", packageName)
	}
	if err := validateShardCoverage(regularNames, regularBuckets); err != nil {
		return nil, fmt.Errorf("validate %s shard coverage: %w", packageName, err)
	}

	// Greedy bin packing keeps the number of selected test names balanced even
	// when one prefix (for example TestBridge...) dominates a package.
	sort.Slice(regularBuckets, func(i, j int) bool { return len(regularBuckets[i].names) > len(regularBuckets[j].names) })
	binCount := shardCount
	if len(regularBuckets) < binCount {
		binCount = len(regularBuckets)
	}
	bins := make([]shardPlan, binCount)
	for i := range bins {
		bins[i].packageName = packageName
	}
	for _, bucket := range regularBuckets {
		best := 0
		for i := 1; i < len(bins); i++ {
			if bins[i].count < bins[best].count {
				best = i
			}
		}
		bins[best].prefixes = append(bins[best].prefixes, bucket.regexPiece())
		bins[best].count += len(bucket.names)
	}
	for i := range bins {
		sort.Strings(bins[i].prefixes)
		bins[i].weight = bins[i].count
	}
	sort.Slice(bins, func(i, j int) bool {
		if bins[i].count != bins[j].count {
			return bins[i].count > bins[j].count
		}
		return bins[i].prefixes[0] < bins[j].prefixes[0]
	})
	return bins, nil
}

func splitPrefixBuckets(names []string, prefix string, maxNames int, isolated map[string]bool) []prefixBucket {
	hasIsolated := false
	for _, name := range names {
		if isolated[name] {
			hasIsolated = true
			break
		}
	}
	if len(names) <= maxNames && !hasIsolated {
		return []prefixBucket{{prefix: prefix, names: names}}
	}
	if len(names) == 1 {
		return []prefixBucket{{prefix: names[0], names: names, exact: true}}
	}
	groups := make(map[byte][]string)
	var exact []string
	for _, name := range names {
		if len(name) == len(prefix) {
			exact = append(exact, name)
			continue
		}
		if len(name) < len(prefix) {
			// A shorter name cannot be a child of this prefix. The list
			// validator should catch a malformed prefix tree, but retain the
			// name as an exact bucket so it is never silently omitted.
			exact = append(exact, name)
			continue
		}
		next := name[len(prefix)]
		groups[next] = append(groups[next], name)
	}
	var buckets []prefixBucket
	for _, name := range exact {
		buckets = append(buckets, prefixBucket{prefix: name, names: []string{name}, exact: true})
	}
	keys := make([]int, 0, len(groups))
	for key := range groups {
		keys = append(keys, int(key))
	}
	sort.Ints(keys)
	for _, key := range keys {
		childPrefix := prefix + string(rune(key))
		buckets = append(buckets, splitPrefixBuckets(groups[byte(key)], childPrefix, maxNames, isolated)...)
	}
	return buckets
}

func validateShardCoverage(names []string, buckets []prefixBucket) error {
	for _, name := range names {
		matches := 0
		for _, bucket := range buckets {
			if (bucket.exact && name == bucket.prefix) || (!bucket.exact && strings.HasPrefix(name, bucket.prefix)) {
				matches++
			}
		}
		if matches != 1 {
			return fmt.Errorf("runnable name %q belongs to %d buckets", name, matches)
		}
	}
	var covered int
	for _, bucket := range buckets {
		covered += len(bucket.names)
	}
	if covered != len(names) {
		return fmt.Errorf("covered %d names, listed %d", covered, len(names))
	}
	return nil
}

func joinPatterns(prefixes []string) string {
	return strings.Join(prefixes, "|")
}

func exactRunnablePattern(names []string) string {
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, regexp.QuoteMeta(name))
	}
	return "^(?:" + joinPatterns(quoted) + ")$"
}

func planPattern(plan shardPlan) string {
	return "^(?:" + joinPatterns(plan.prefixes) + ")"
}

func (bucket prefixBucket) regexPiece() string {
	piece := regexp.QuoteMeta(bucket.prefix)
	if bucket.exact {
		return piece + "$"
	}
	return piece
}

func commandOutput(name string, args ...string) ([]byte, error) {
	var stderr bytes.Buffer
	command := exec.Command(name, args...)
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		message := strings.TrimSpace(stderr.String())
		if message != "" {
			return nil, fmt.Errorf("%w: %s", err, message)
		}
		return nil, err
	}
	return output, nil
}

func runJobs(jobs []testJob, testTimeout time.Duration) []jobResult {
	parallel, exclusive := splitTestJobs(jobs)
	// Run host-sensitive fixtures before the broad shard pool.  Exclusive means
	// that no sibling shard runs at the same time, but running those fixtures
	// only after the pool would still make their finite liveness observations
	// inherit the hosted runner's accumulated CPU, filesystem, and process
	// pressure.  Keeping the exclusive phase first preserves the same total
	// work and makes its isolation boundary start from a clean runner state.
	collected := runExclusiveJobs(exclusive, testTimeout)
	collected = append(collected, runParallelJobs(parallel, testTimeout)...)
	return collected
}

func runExclusiveJobs(jobs []testJob, testTimeout time.Duration) []jobResult {
	collected := make([]jobResult, 0, len(jobs))
	for _, job := range jobs {
		fmt.Printf("starting %s\n", job.label)
		result := executeJob(job, testTimeout)
		if result.err == nil {
			fmt.Printf("passed %s duration=%s\n", result.label, result.duration)
		} else {
			fmt.Printf("failed %s duration=%s\n%s\n", result.label, result.duration, result.err)
		}
		collected = append(collected, result)
	}
	return collected
}

func splitTestJobs(jobs []testJob) (parallel, exclusive []testJob) {
	parallel = make([]testJob, 0, len(jobs))
	exclusive = make([]testJob, 0)
	for _, job := range jobs {
		if job.exclusive {
			exclusive = append(exclusive, job)
			continue
		}
		parallel = append(parallel, job)
	}
	return parallel, exclusive
}

func runParallelJobs(jobs []testJob, testTimeout time.Duration) []jobResult {
	if len(jobs) == 0 {
		return nil
	}
	// Start the largest estimated jobs first so the actual four-worker queue
	// follows the same longest-processing-time order used by the partition
	// guard. This reduces the chance that a late heavy shard becomes the final
	// worker tail when earlier jobs finish at different times.
	ordered := append([]testJob(nil), jobs...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].weight != ordered[j].weight {
			return ordered[i].weight > ordered[j].weight
		}
		return ordered[i].label < ordered[j].label
	})
	workerCount := maxConcurrentJobs
	if len(ordered) < workerCount {
		workerCount = len(ordered)
	}
	queue := make(chan testJob)
	results := make(chan jobResult, len(ordered))
	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for job := range queue {
				results <- executeJob(job, testTimeout)
			}
		}()
	}
	go func() {
		for _, job := range ordered {
			fmt.Printf("starting %s\n", job.label)
			queue <- job
		}
		close(queue)
		wg.Wait()
		close(results)
	}()

	collected := make([]jobResult, 0, len(ordered))
	for result := range results {
		if result.err == nil {
			fmt.Printf("passed %s duration=%s\n", result.label, result.duration)
		} else {
			fmt.Printf("failed %s duration=%s\n%s\n", result.label, result.duration, result.err)
		}
		collected = append(collected, result)
	}
	return collected
}

func executeJob(job testJob, testTimeout time.Duration) jobResult {
	startedAt := time.Now()
	// The Go test watchdog is the semantic deadline. A small outer grace keeps
	// the runner from killing the wrapper at the exact instant it is collecting
	// the test process's timeout report.
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout+3*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", job.args...)
	var output bytes.Buffer
	command.Stdout = &output
	command.Stderr = &output
	err := command.Run()
	if ctx.Err() != nil {
		err = fmt.Errorf("outer timeout after %s: %w", testTimeout+3*time.Minute, ctx.Err())
	}
	if err != nil {
		return jobResult{
			label:    job.label,
			err:      fmt.Errorf("%w\n%s", err, strings.TrimSpace(output.String())),
			duration: time.Since(startedAt),
		}
	}
	return jobResult{label: job.label, duration: time.Since(startedAt)}
}
