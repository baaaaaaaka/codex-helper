// Command check_teams_recovery_manifest validates and runs the required Teams
// recovery tests.  Keeping the selector in a machine-readable manifest avoids
// a CI job silently becoming a no-op when a test is renamed or moved.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/baaaaaaaka/codex-helper/scripts/ci/manifestprocess"
)

const (
	manifestSelectorTimeout       = 10 * time.Minute
	manifestBuildTimeout          = 10 * time.Minute
	manifestRuntimeGrace          = 30 * time.Second
	teamsOwnershipStressStrictEnv = "CODEX_HELPER_TEAMS_OWNERSHIP_STRESS_STRICT"
)

type manifest struct {
	Version int            `json:"version"`
	Tests   []manifestTest `json:"tests"`
}

type manifestTest struct {
	Name         string   `json:"name"`
	Package      string   `json:"package"`
	Job          string   `json:"job"`
	Tier         string   `json:"tier"`
	Backends     []string `json:"backends"`
	Vertical     string   `json:"vertical"`
	RealListener bool     `json:"real_listener"`
	ListenerMode string   `json:"listener_mode"`
	Once         *bool    `json:"once"`
	FakeGraph    bool     `json:"fake_graph"`
	Exclusive    bool     `json:"exclusive"`
	MaxSeconds   int      `json:"max_seconds"`
	Oracle       string   `json:"oracle"`
	Baseline     string   `json:"baseline"`
}

type goTestJSONEvent struct {
	Action  string `json:"Action"`
	Package string `json:"Package"`
	Test    string `json:"Test"`
}

var testNamePattern = regexp.MustCompile(`^Test[A-Za-z0-9_]+$`)

func main() {
	manifestPath := flag.String("manifest", "scripts/ci/teams_recovery_tests.json", "required-test manifest")
	job := flag.String("job", "", "run only entries assigned to this job")
	race := flag.Bool("race", false, "pass -race to go test")
	listOnly := flag.Bool("list-only", false, "validate selectors without running tests")
	flag.Parse()

	m, err := readManifest(*manifestPath)
	if err != nil {
		fatal(err)
	}
	selected, err := selectTests(m, *job)
	if err != nil {
		fatal(err)
	}
	if err := validateSelectors(selected); err != nil {
		fatal(err)
	}
	if *listOnly {
		return
	}
	if err := runManifestTests(selected, *race); err != nil {
		fatal(err)
	}
}

func readManifest(path string) (manifest, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return manifest{}, fmt.Errorf("read recovery manifest %q: %w", path, err)
	}
	var m manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return manifest{}, fmt.Errorf("decode recovery manifest %q: %w", path, err)
	}
	if m.Version != 1 {
		return manifest{}, fmt.Errorf("unsupported recovery manifest version %d", m.Version)
	}
	if len(m.Tests) == 0 {
		return manifest{}, errors.New("recovery manifest has no tests")
	}
	return m, nil
}

func selectTests(m manifest, job string) ([]manifestTest, error) {
	selected := make([]manifestTest, 0, len(m.Tests))
	seen := make(map[string]bool, len(m.Tests))
	for _, item := range m.Tests {
		if job != "" && item.Job != job {
			continue
		}
		if seen[item.Name] {
			return nil, fmt.Errorf("duplicate recovery test %q", item.Name)
		}
		seen[item.Name] = true
		selected = append(selected, item)
	}
	if len(selected) == 0 {
		if job == "" {
			return nil, errors.New("recovery manifest selected no tests")
		}
		return nil, fmt.Errorf("recovery manifest selected no tests for job %q", job)
	}
	return selected, nil
}

func validateSelectors(tests []manifestTest) error {
	byPackage := make(map[string][]manifestTest)
	realListenerSources := make(map[string]realListenerSource)
	for _, item := range tests {
		if !testNamePattern.MatchString(item.Name) {
			return fmt.Errorf("invalid exact test name %q", item.Name)
		}
		if strings.TrimSpace(item.Package) == "" || strings.TrimSpace(item.Job) == "" ||
			strings.TrimSpace(item.Tier) == "" || strings.TrimSpace(item.Vertical) == "" {
			return fmt.Errorf("recovery test %q is missing package/job/tier/vertical metadata", item.Name)
		}
		if item.MaxSeconds <= 0 {
			return fmt.Errorf("recovery test %q has invalid max_seconds=%d", item.Name, item.MaxSeconds)
		}
		if len(item.Backends) == 0 {
			return fmt.Errorf("recovery test %q has no backend metadata", item.Name)
		}
		if strings.TrimSpace(item.Oracle) == "" || strings.TrimSpace(item.Baseline) == "" {
			return fmt.Errorf("recovery test %q must document oracle and baseline", item.Name)
		}
		if item.RealListener && !item.FakeGraph {
			return fmt.Errorf("real listener test %q must use the isolated fake Graph", item.Name)
		}
		if item.RealListener && (item.ListenerMode != "continuous" || item.Once == nil || *item.Once) {
			return fmt.Errorf("real listener test %q must declare listener_mode=continuous and once=false", item.Name)
		}
		if item.RealListener && item.MaxSeconds < 1 {
			return fmt.Errorf("real listener test %q must have a positive watchdog budget", item.Name)
		}
		if item.RealListener {
			if err := validateRealListenerSourceCached(item, realListenerSources); err != nil {
				return err
			}
		}
		byPackage[item.Package] = append(byPackage[item.Package], item)
	}

	packages := make([]string, 0, len(byPackage))
	for pkg := range byPackage {
		packages = append(packages, pkg)
	}
	sort.Strings(packages)
	for _, pkg := range packages {
		items := byPackage[pkg]
		names := make([]string, 0, len(items))
		for _, item := range items {
			names = append(names, item.Name)
		}
		sort.Strings(names)
		alternatives := make([]string, 0, len(names))
		for _, name := range names {
			alternatives = append(alternatives, regexp.QuoteMeta(name))
		}
		selector := "^(?:" + strings.Join(alternatives, "|") + ")$"
		listed, err := runCommandOutputWithTimeout(manifestSelectorTimeout, "go", "test", pkg, "-run", "^$", "-list", selector)
		if err != nil {
			return fmt.Errorf("list recovery tests in %s: %w", pkg, err)
		}
		for _, item := range items {
			if !hasExactListedTest(listed, item.Name) {
				return fmt.Errorf("recovery test %q is not present in %s (go test -list output: %s)", item.Name, pkg, strings.TrimSpace(listed))
			}
		}
	}
	return nil
}

type realListenerSource struct {
	functions        map[string]*ast.FuncDecl
	harnessHasListen bool
}

func realListenerPackagePath(item manifestTest) (string, error) {
	packagePath := filepath.Clean(filepath.FromSlash(strings.TrimSpace(item.Package)))
	if packagePath == "." || packagePath == ".." || strings.Contains(packagePath, "...") {
		return "", fmt.Errorf("real listener test %q must use a local concrete package for source validation", item.Name)
	}
	if strings.HasPrefix(packagePath, "."+string(filepath.Separator)) {
		packagePath = strings.TrimPrefix(packagePath, "."+string(filepath.Separator))
	}
	return packagePath, nil
}

func validateRealListenerSourceCached(item manifestTest, cache map[string]realListenerSource) error {
	packagePath, err := realListenerPackagePath(item)
	if err != nil {
		return err
	}
	source, ok := cache[packagePath]
	if !ok {
		source, err = loadRealListenerSource(item, packagePath)
		if err != nil {
			return err
		}
		cache[packagePath] = source
	}
	return source.validate(item)
}

// validateRealListenerSource prevents the manifest's real_listener bit from
// becoming a self-declared label.  Required listener entries must call the
// shared harness, and that harness must itself invoke production Bridge.Listen
// rather than a phase helper or a test-only loop.  A lifecycle test that must
// stop after exactly one owner generation may instead call the production
// listenOwnerGeneration entry directly; the runtime harness also checks
// Once=false.  This source check makes a renamed/empty selector fail before CI
// can report a misleading green recovery job.
func validateRealListenerSource(item manifestTest) error {
	packagePath, err := realListenerPackagePath(item)
	if err != nil {
		return err
	}
	source, err := loadRealListenerSource(item, packagePath)
	if err != nil {
		return err
	}
	return source.validate(item)
}

func loadRealListenerSource(item manifestTest, packagePath string) (realListenerSource, error) {
	harness := "startListenerRecovery"
	entries, err := os.ReadDir(packagePath)
	if err != nil {
		return realListenerSource{}, fmt.Errorf("read source package for real listener test %q: %w", item.Name, err)
	}
	functions := make(map[string]*ast.FuncDecl)
	harnessHasListen := false
	fset := token.NewFileSet()
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(packagePath, entry.Name())
		file, parseErr := parser.ParseFile(fset, path, nil, 0)
		if parseErr != nil {
			return realListenerSource{}, fmt.Errorf("parse source package for real listener test %q: %w", item.Name, parseErr)
		}
		for _, decl := range file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || function.Name == nil || function.Body == nil {
				continue
			}
			functions[function.Name.Name] = function
			if function.Name.Name == harness {
				ast.Inspect(function.Body, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel != nil && selector.Sel.Name == "Listen" {
						harnessHasListen = true
					}
					return true
				})
			}
		}
	}
	return realListenerSource{functions: functions, harnessHasListen: harnessHasListen}, nil
}

func (source realListenerSource) validate(item manifestTest) error {
	functions := source.functions
	harness := "startListenerRecovery"
	if functions[item.Name] == nil {
		return fmt.Errorf("real listener test %q is not present in source package %s", item.Name, item.Package)
	}
	if !source.harnessHasListen {
		return fmt.Errorf("real listener harness %q in %s does not call Bridge.Listen", harness, item.Package)
	}
	calledHarness := false
	visited := make(map[string]bool)
	queue := []string{item.Name}
	for len(queue) > 0 && !calledHarness {
		name := queue[0]
		queue = queue[1:]
		if visited[name] {
			continue
		}
		visited[name] = true
		function := functions[name]
		if function == nil {
			continue
		}
		ast.Inspect(function.Body, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			if ident, ok := call.Fun.(*ast.Ident); ok {
				if ident.Name == harness {
					calledHarness = true
					return false
				}
				if functions[ident.Name] != nil && !visited[ident.Name] {
					queue = append(queue, ident.Name)
				}
			}
			if selector, ok := call.Fun.(*ast.SelectorExpr); ok && selector.Sel != nil && (selector.Sel.Name == "Listen" || selector.Sel.Name == "listenOwnerGeneration") {
				// A lifecycle test may need to control its own context/done channel
				// (for example, to release the lease between two generations) and
				// therefore call the production listener generation directly instead
				// of using the shared goroutine helper.
				calledHarness = true
				return false
			}
			return true
		})
	}
	if !calledHarness {
		return fmt.Errorf("real listener test %q does not reach the production listener harness %q", item.Name, harness)
	}
	return nil
}

func runManifestTests(tests []manifestTest, race bool) (runErr error) {
	ordered := append([]manifestTest(nil), tests...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Package != ordered[j].Package {
			return ordered[i].Package < ordered[j].Package
		}
		return ordered[i].Name < ordered[j].Name
	})
	testDir, err := os.MkdirTemp("", "codex-helper-teams-recovery-")
	if err != nil {
		return fmt.Errorf("create recovery test binary directory: %w", err)
	}
	defer func() {
		if cleanupErr := os.RemoveAll(testDir); cleanupErr != nil {
			cleanupErr = fmt.Errorf("remove recovery test binary directory %q: %w", testDir, cleanupErr)
			if runErr == nil {
				runErr = cleanupErr
			} else {
				runErr = errors.Join(runErr, cleanupErr)
			}
		}
	}()

	packages := make([]string, 0)
	seenPackages := make(map[string]bool)
	for _, item := range ordered {
		if seenPackages[item.Package] {
			continue
		}
		seenPackages[item.Package] = true
		packages = append(packages, item.Package)
	}
	sort.Strings(packages)
	binaries := make(map[string]string, len(packages))
	packageDirs := make(map[string]string, len(packages))
	for index, packageName := range packages {
		binaryName := fmt.Sprintf("recovery-%d.test", index)
		if runtime.GOOS == "windows" {
			binaryName += ".exe"
		}
		binaryPath := filepath.Join(testDir, binaryName)
		if err := buildManifestTestBinary(packageName, race, binaryPath); err != nil {
			return err
		}
		packageDir, err := resolveManifestPackageDir(packageName)
		if err != nil {
			return err
		}
		binaries[packageName] = binaryPath
		packageDirs[packageName] = packageDir
	}

	parallel, exclusive := splitManifestTests(ordered)
	var outputMu sync.Mutex
	if err := runManifestTestPool(parallel, binaries, packageDirs, race, &outputMu); err != nil {
		return err
	}
	// Some real-listener fixtures have a finite progress observation whose
	// result depends on the host scheduler and filesystem.  They still run in
	// their own test process, but must also run without sibling recovery
	// processes competing for hosted-runner resources.
	for _, item := range exclusive {
		if err := runManifestTest(item, binaries[item.Package], packageDirs[item.Package], &outputMu); err != nil {
			return err
		}
	}
	return nil
}

func splitManifestTests(tests []manifestTest) (parallel, exclusive []manifestTest) {
	parallel = make([]manifestTest, 0, len(tests))
	exclusive = make([]manifestTest, 0)
	for _, item := range tests {
		if item.Exclusive {
			exclusive = append(exclusive, item)
			continue
		}
		parallel = append(parallel, item)
	}
	return parallel, exclusive
}

func runManifestTestPool(ordered []manifestTest, binaries map[string]string, packageDirs map[string]string, race bool, outputMu *sync.Mutex) error {
	if len(ordered) == 0 {
		return nil
	}
	// Each manifest entry remains in its own test process, so running several
	// entries concurrently does not share test globals or weaken per-entry
	// watchdog isolation.  Bound the pool by the runner's available CPUs: the
	// recovery corpus is deliberately expensive, but an unbounded process burst
	// would trade wall time for memory pressure and scheduler contention.
	workerCount := manifestTestWorkerCount(race)
	jobs := make(chan manifestTestJob)
	results := make(chan manifestTestResult, len(ordered))
	var workers sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range jobs {
				err := runManifestTest(job.item, binaries[job.item.Package], packageDirs[job.item.Package], outputMu)
				results <- manifestTestResult{index: job.index, err: err}
			}
		}()
	}
	go func() {
		for index, item := range ordered {
			jobs <- manifestTestJob{index: index, item: item}
		}
		close(jobs)
		workers.Wait()
		close(results)
	}()
	errs := make([]error, len(ordered))
	for result := range results {
		errs[result.index] = result.err
	}
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

const maxManifestTestWorkers = 4

type manifestTestJob struct {
	index int
	item  manifestTest
}

type manifestTestResult struct {
	index int
	err   error
}

func manifestTestWorkerCount(race bool) int {
	return manifestTestWorkerCountFor(race, runtime.GOOS, runtime.GOMAXPROCS(0))
}

func manifestTestWorkerCountFor(race bool, goos string, gomaxprocs int) int {
	// The Windows race runner uses file-backed modernc SQLite for many
	// independent manifest entries.  Concurrent processes contend on the
	// hosted runner's filesystem flush path and can make a healthy migration
	// exceed its deliberately small per-test watchdog.  Serialize only this
	// test configuration; normal and non-Windows race runs retain the bounded
	// parallel pool.
	if race && strings.EqualFold(strings.TrimSpace(goos), "windows") {
		return 1
	}
	workerCount := gomaxprocs
	if workerCount < 1 {
		workerCount = 1
	}
	if workerCount > maxManifestTestWorkers {
		workerCount = maxManifestTestWorkers
	}
	return workerCount
}

type synchronizedManifestWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (w synchronizedManifestWriter) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.w.Write(data)
}

func runManifestTest(item manifestTest, binaryPath string, packageDir string, outputMu *sync.Mutex) error {
	selector := "^" + regexp.QuoteMeta(item.Name) + "$"
	args := []string{
		"tool",
		"test2json",
		"-p",
		item.Package,
		"-t",
		binaryPath,
		"-test.v",
		"-test.timeout",
		strconv.Itoa(item.MaxSeconds) + "s",
		"-test.run",
		selector,
	}
	// Compile each package once, then run every manifest entry in its own
	// process. This preserves process isolation and each test's watchdog
	// without paying the large package compile/link cost 109 times. The small
	// runtime grace covers process startup; the test binary itself has the
	// exact manifest timeout and therefore still reports a useful stack if it
	// fails to return.
	watchdog := time.Duration(item.MaxSeconds)*time.Second + manifestRuntimeGrace
	ctx, cancel := context.WithTimeout(context.Background(), watchdog)
	defer cancel()
	var output bytes.Buffer
	cmd := exec.Command("go", args...)
	// `go test` runs the test binary with the package directory as its working
	// directory. Preserve that contract because recovery fixtures and a few
	// tests intentionally use package-relative testdata paths.
	cmd.Dir = packageDir
	// Required recovery tests must fail on a semantic finding in every runner
	// environment. Do not inherit a developer's exploratory diagnostic setting
	// into the CI manifest.
	cmd.Env = manifestChildEnvironment()
	stdout := synchronizedManifestWriter{mu: outputMu, w: os.Stdout}
	stderr := synchronizedManifestWriter{mu: outputMu, w: os.Stderr}
	cmd.Stdout = io.MultiWriter(stdout, &output)
	cmd.Stderr = stderr
	err := manifestprocess.Run(ctx, cmd)
	if err != nil && ctx.Err() == context.DeadlineExceeded {
		return fmt.Errorf("recovery test %s (%s) exceeded external watchdog %s; output:\n%s", item.Name, item.Package, watchdog, strings.TrimSpace(output.String()))
	}
	if err != nil {
		return fmt.Errorf("run recovery test %s (%s): %w; output:\n%s", item.Name, item.Package, err, strings.TrimSpace(output.String()))
	}
	if err := validateTestJSONOutput(output.Bytes(), item.Name, item.Backends); err != nil {
		return fmt.Errorf("validate recovery test %s (%s): %w; output:\n%s", item.Name, item.Package, err, strings.TrimSpace(output.String()))
	}
	return nil
}

func resolveManifestPackageDir(packageName string) (string, error) {
	packageName = strings.TrimSpace(packageName)
	if packageName == "" {
		return "", errors.New("recovery test package is empty")
	}
	if packageName == "." || strings.HasPrefix(packageName, "."+string(filepath.Separator)) {
		dir, err := filepath.Abs(filepath.FromSlash(packageName))
		if err != nil {
			return "", fmt.Errorf("resolve recovery test package %s: %w", packageName, err)
		}
		if info, err := os.Stat(dir); err != nil {
			return "", fmt.Errorf("stat recovery test package %s: %w", packageName, err)
		} else if !info.IsDir() {
			return "", fmt.Errorf("recovery test package %s is not a directory", packageName)
		}
		return dir, nil
	}
	listed, err := runCommandOutputWithTimeout(manifestSelectorTimeout, "go", "list", "-f", "{{.Dir}}", packageName)
	if err != nil {
		return "", fmt.Errorf("resolve recovery test package %s: %w", packageName, err)
	}
	dir := strings.TrimSpace(listed)
	if dir == "" || strings.ContainsRune(dir, '\n') {
		return "", fmt.Errorf("resolve recovery test package %s returned invalid directory %q", packageName, dir)
	}
	if info, err := os.Stat(dir); err != nil {
		return "", fmt.Errorf("stat recovery test package %s directory %s: %w", packageName, dir, err)
	} else if !info.IsDir() {
		return "", fmt.Errorf("recovery test package %s directory %s is not a directory", packageName, dir)
	}
	return dir, nil
}

func buildManifestTestBinary(packageName string, race bool, binaryPath string) error {
	args := []string{"test"}
	if race {
		args = append(args, "-race")
	}
	args = append(args, "-c", "-o", binaryPath, packageName)
	ctx, cancel := context.WithTimeout(context.Background(), manifestBuildTimeout)
	defer cancel()
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("go", args...)
	cmd.Env = manifestChildEnvironment()
	cmd.Stdout = io.MultiWriter(os.Stdout, &stdout)
	cmd.Stderr = io.MultiWriter(os.Stderr, &stderr)
	if err := manifestprocess.Run(ctx, cmd); err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("build recovery test binary for %s exceeded %s; output:\n%s", packageName, manifestBuildTimeout, strings.TrimSpace(string(combineManifestCommandOutput(stdout.Bytes(), stderr.Bytes()))))
		}
		return fmt.Errorf("build recovery test binary for %s: %w; output:\n%s", packageName, err, strings.TrimSpace(string(combineManifestCommandOutput(stdout.Bytes(), stderr.Bytes()))))
	}
	return nil
}

func manifestChildEnvironment() []string {
	const required = teamsOwnershipStressStrictEnv + "=1"
	env := os.Environ()
	for index, entry := range env {
		if strings.HasPrefix(entry, teamsOwnershipStressStrictEnv+"=") {
			env[index] = required
			return env
		}
	}
	return append(env, required)
}

func validateTestJSONOutput(data []byte, name string, expectedBackends ...[]string) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return errors.New("required test name is empty")
	}
	scanner := bufio.NewScanner(bytes.NewReader(data))
	// A test log may contain a large diagnostic or a serialized fixture.  Keep
	// the parser bounded, but do not let Scanner's 64 KiB default turn a valid
	// failure report into an opaque runner error.
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	seenRun := false
	seenPass := false
	events := make([]goTestJSONEvent, 0, 16)
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var event goTestJSONEvent
		if err := json.Unmarshal(line, &event); err != nil {
			return fmt.Errorf("decode go test JSON event: %w", err)
		}
		if event.Test != name && !strings.HasPrefix(event.Test, name+"/") {
			continue
		}
		events = append(events, event)
		switch event.Action {
		case "run":
			if event.Test == name {
				seenRun = true
			}
		case "pass":
			if event.Test == name {
				seenPass = true
			}
		case "fail":
			return fmt.Errorf("go test reported fail for %s", event.Test)
		case "skip":
			return fmt.Errorf("required test was skipped: %s", event.Test)
		}
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan go test JSON output: %w", err)
	}
	if !seenRun {
		return fmt.Errorf("required test %q did not emit a run event", name)
	}
	if !seenPass {
		return fmt.Errorf("required test %q did not emit a pass event", name)
	}
	if len(expectedBackends) > 0 {
		if err := validateExpectedBackendSubtests(events, name, expectedBackends[0]); err != nil {
			return err
		}
	}
	return nil
}

// validateExpectedBackendSubtests gives the manifest's backend metadata real
// meaning.  A top-level Go test can pass while a backend subtest is silently
// omitted (or skipped), which made a JSON/SQLite entry look covered when only
// one store actually ran.  Existing tests use either explicit names
// ("json"/"sqlite") or the boolean form ("sqlite=false"/"sqlite=true");
// both are accepted, but every declared backend must emit its own RUN and
// PASS event.
func validateExpectedBackendSubtests(events []goTestJSONEvent, name string, backends []string) error {
	if len(backends) <= 1 {
		return nil
	}
	seen := make(map[string]map[string]bool, len(backends))
	for _, backend := range backends {
		backend = strings.TrimSpace(strings.ToLower(backend))
		if backend == "" {
			return fmt.Errorf("required test %q declares an empty backend", name)
		}
		seen[backend] = map[string]bool{"run": false, "pass": false}
	}
	for _, event := range events {
		if !strings.HasPrefix(event.Test, name+"/") {
			continue
		}
		suffix := strings.ToLower(strings.TrimPrefix(event.Test, name+"/"))
		for backend := range seen {
			if !backendSubtestSuffixMatches(backend, suffix) {
				continue
			}
			switch event.Action {
			case "run":
				seen[backend]["run"] = true
			case "pass":
				seen[backend]["pass"] = true
			}
		}
	}
	for backend, state := range seen {
		if !state["run"] || !state["pass"] {
			return fmt.Errorf("required test %q did not run and pass backend subtest %q", name, backend)
		}
	}
	return nil
}

func backendSubtestSuffixMatches(backend, suffix string) bool {
	switch backend {
	case "json":
		return suffix == "json" || suffix == "sqlite=false" || suffix == "sqlite=0"
	case "sqlite":
		return suffix == "sqlite" || suffix == "sqlite=true" || suffix == "sqlite=1"
	default:
		return suffix == backend
	}
}

func runCommandOutputWithTimeout(timeout time.Duration, name string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	cmd := exec.Command(name, args...)
	output, err := runManifestCommandOutput(ctx, cmd)
	if err != nil && ctx.Err() == context.DeadlineExceeded {
		return string(output), fmt.Errorf("%s %s exceeded selector watchdog", name, strings.Join(args, " "))
	}
	if err != nil {
		return string(output), fmt.Errorf("%s %s: %w\n%s", name, strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return string(output), nil
}

func runManifestCommandOutput(ctx context.Context, cmd *exec.Cmd) ([]byte, error) {
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := manifestprocess.Run(ctx, cmd)
	return combineManifestCommandOutput(stdout.Bytes(), stderr.Bytes()), err
}

func combineManifestCommandOutput(stdout, stderr []byte) []byte {
	output := make([]byte, 0, len(stdout)+len(stderr))
	output = append(output, stdout...)
	output = append(output, stderr...)
	return output
}

func hasExactListedTest(output, want string) bool {
	for _, line := range strings.Split(output, "\n") {
		if strings.TrimSpace(line) == want {
			return true
		}
	}
	return false
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
