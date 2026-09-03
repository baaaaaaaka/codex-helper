// Command run_full_go_test_shards runs the full Go test suite while isolating
// the two largest packages into independently compiled test processes.  The
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
	defaultShardCount = 8
	maxConcurrentJobs = 4
)

var runnableNamePattern = regexp.MustCompile(`^(Test|Example|Fuzz)[A-Za-z0-9_]*$`)

// A small number of tests intentionally exercise long-lived listener state.
// The stateful Graph page regression is independently correct but shares
// process-global test plumbing with older package fixtures. Keep it in its
// own test process rather than allowing unrelated tests to make its timing
// assertion nondeterministic.
var isolatedRunnableNames = map[string]map[string]bool{
	"./internal/teams": {
		"TestTeamsListenFalseGraphStatefulHeadContinuationDrainsTerminalPage": true,
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
}

type testJob struct {
	label string
	args  []string
}

type jobResult struct {
	label string
	err   error
}

func main() {
	shards := flag.Int("shards", defaultShardCount, "maximum number of shards per large package")
	parallel := flag.Int("parallel", 16, "go test -parallel value")
	testTimeout := flag.Duration("timeout", 20*time.Minute, "per-shard go test timeout")
	listOnly := flag.Bool("list-only", false, "print the plan without executing tests")
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

	packages := []string(requestedPackages)
	if len(packages) == 0 {
		var err error
		packages, err = listPackages()
		if err != nil {
			fatal(err)
		}
	}

	jobs, err := makeJobs(packages, *shards, *parallel, *testTimeout)
	if err != nil {
		fatal(err)
	}
	if len(jobs) == 0 {
		fatal(errors.New("no test jobs were planned"))
	}
	if *listOnly {
		for _, job := range jobs {
			fmt.Printf("%s: go", job.label)
			for _, arg := range job.args {
				fmt.Printf(" %q", arg)
			}
			fmt.Println()
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

func makeJobs(packages []string, shardCount, parallel int, testTimeout time.Duration) ([]testJob, error) {
	var ordinary []string
	var plans []shardPlan
	for _, packageName := range packages {
		if !isLargePackage(packageName) {
			ordinary = append(ordinary, packageName)
			continue
		}
		names, err := listRunnableNames(packageName)
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
		isolated := isolatedRunnableNamesForPackage(packageName)
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
		args := []string{"test", fmt.Sprintf("-timeout=%s", testTimeout), fmt.Sprintf("-parallel=%d", parallel), "-count=1"}
		args = append(args, ordinary...)
		jobs = append(jobs, testJob{label: "ordinary packages", args: args})
	}
	planTotals := make(map[string]int)
	for _, plan := range plans {
		planTotals[plan.packageName]++
	}
	planIndexes := make(map[string]int)
	for _, plan := range plans {
		planIndexes[plan.packageName]++
		planIndex := planIndexes[plan.packageName]
		pattern := planPattern(plan)
		args := []string{
			"test",
			fmt.Sprintf("-timeout=%s", testTimeout),
			fmt.Sprintf("-parallel=%d", parallel),
			"-count=1",
			plan.packageName,
			"-run",
			pattern,
		}
		jobs = append(jobs, testJob{
			label: fmt.Sprintf("%s shard %d/%d (%d test names)", plan.packageName, planIndex, planTotals[plan.packageName], plan.count),
			args:  args,
		})
	}
	return jobs, nil
}

func isLargePackage(packageName string) bool {
	packageName = strings.TrimSuffix(strings.TrimSpace(packageName), "/")
	return packageName == "./internal/teams" ||
		strings.HasSuffix(packageName, "/internal/teams") ||
		packageName == "./internal/teams/store" ||
		strings.HasSuffix(packageName, "/internal/teams/store")
}

func isolatedRunnableNamesForPackage(packageName string) map[string]bool {
	if names, ok := isolatedRunnableNames[packageName]; ok {
		return names
	}
	if strings.HasSuffix(packageName, "/internal/teams/store") {
		return isolatedRunnableNames["./internal/teams/store"]
	}
	if strings.HasSuffix(packageName, "/internal/teams") {
		return isolatedRunnableNames["./internal/teams"]
	}
	return nil
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

func listRunnableNames(packageName string) ([]string, error) {
	output, err := commandOutput("go", "test", packageName, "-run", "^$", "-list", "^(Test|Example|Fuzz)")
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
	workerCount := maxConcurrentJobs
	if len(jobs) < workerCount {
		workerCount = len(jobs)
	}
	queue := make(chan testJob)
	results := make(chan jobResult, len(jobs))
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
		for _, job := range jobs {
			fmt.Printf("starting %s\n", job.label)
			queue <- job
		}
		close(queue)
		wg.Wait()
		close(results)
	}()

	collected := make([]jobResult, 0, len(jobs))
	for result := range results {
		if result.err == nil {
			fmt.Printf("passed %s\n", result.label)
		} else {
			fmt.Printf("failed %s\n%s\n", result.label, result.err)
		}
		collected = append(collected, result)
	}
	return collected
}

func executeJob(job testJob, testTimeout time.Duration) jobResult {
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
			label: job.label,
			err:   fmt.Errorf("%w\n%s", err, strings.TrimSpace(output.String())),
		}
	}
	return jobResult{label: job.label}
}
