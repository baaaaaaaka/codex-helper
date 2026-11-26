//go:build windows

package cli

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/baaaaaaaka/codex-helper/internal/core/lease"
	"github.com/baaaaaaaka/codex-helper/internal/helperpath"
)

const windowsHandoffFixtureAnchor = "CXP_CI_WINDOWS_TEAMS_HANDOFF_ANCHOR"

func init() {
	teamsWindowsHandoffTestChild = runTeamsWindowsHandoffTestChild
}

func runTeamsWindowsHandoffTestChild() {
	anchor := os.Getenv(windowsHandoffFixtureAnchor)
	if anchor == "" {
		return
	}
	if !strings.EqualFold(os.Args[0], os.Getenv("CXP_CI_WINDOWS_TEAMS_HANDOFF_RUNTIME")) {
		// Native stable entries start a versioned child and wait for it.
		child := exec.Command(os.Getenv("CXP_CI_WINDOWS_TEAMS_HANDOFF_RUNTIME"), os.Args[1:]...)
		child.Stdout, child.Stderr = os.Stdout, os.Stderr
		if err := child.Run(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		os.Exit(0)
	}
	if err := serveTeamsWindowsHandoffTestChild(anchor); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func serveTeamsWindowsHandoffTestChild(anchor string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	host, err := teamsHostname()
	if err != nil {
		return err
	}
	holder, err := teamsAcquireLease(ctx, anchor, host, lease.Scaled(3*time.Second), true, os.Stderr)
	if err != nil {
		return err
	}
	defer holder.Release()
	if err := holder.WaitQuiet(ctx); err != nil {
		return err
	}
	if err := holder.Activate(); err != nil {
		return err
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(os.Getenv("CXP_CI_WINDOWS_TEAMS_HANDOFF_GRAPH"))
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("fixture Graph poll returned %d", resp.StatusCode)
	}
	if err := writeTeamsPollStamp(filepath.Dir(anchor), teamsPollStamp{Host: host, PID: os.Getpid(), Generation: holder.Gen(), At: time.Now().UnixMilli()}); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "fixture polled Graph as pid %d\n", os.Getpid())
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-holder.Lost():
			return nil
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if _, err := os.Stat(anchor + ".stop-fixture"); err == nil {
				return nil
			}
		}
	}
}

type teamsWindowsHandoffTestBackend struct {
	teamsServiceWindowsTaskBackend
	task string // quoted unique fixture name
}

func (b teamsWindowsHandoffTestBackend) Run(ctx context.Context, action string) ([]byte, error) {
	if action != "restart" {
		return nil, fmt.Errorf("unexpected action %q", action)
	}
	return teamsServiceRunPowerShell(ctx, teamsServiceWindowsRestartPowerShell(b.task, ""))
}

func (b teamsWindowsHandoffTestBackend) Active(ctx context.Context) (bool, error) {
	out, err := teamsServiceRunPowerShell(ctx, "(Get-ScheduledTask -TaskName "+b.task+" -ErrorAction Stop).State")
	return strings.TrimSpace(string(out)) == "Running", err
}

func TestTeamsServiceWindowsNativePinsOldProcess(t *testing.T) {
	shell, err := exec.LookPath("powershell.exe")
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command(shell, "-NoProfile", "-NonInteractive", "-Command", "Start-Sleep -Seconds 30")
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = child.Process.Kill()
		_ = child.Wait()
	})
	owner := lease.Record{PID: child.Process.Pid, Wall: time.Now().UnixMilli()}
	exited, release := openTeamsWindowsBridgeExitProbe(owner)
	if exited == nil || exited() {
		t.Fatal("could not pin a live Windows fixture process")
	}
	defer release()
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.Wait()
	if !exited() {
		t.Fatal("Windows process handle did not observe fixture exit")
	}
}

func TestTeamsServiceWindowsNativeScheduledRestartHandoff(t *testing.T) {
	lockCLITestHooks(t)
	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	host, err := teamsHostname()
	if err != nil {
		t.Fatal(err)
	}

	graph := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer graph.Close()
	exe, err := helperpath.RawExecutable()
	if err != nil {
		t.Fatal(err)
	}
	bin, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	entry := filepath.Join(tmp, "cxp.exe")
	child := filepath.Join(tmp, ".cxp-runtime", "versions", "v0.0.0", "cxp.exe")
	writeCLIFile(t, entry, string(bin), 0o700)
	writeCLIFile(t, child, string(bin), 0o700)
	anchor := filepath.Join(tmp, "core.sqlite")
	spec := teamsServiceSpec{
		Executable: entry, WorkingDir: tmp, WindowsTaskLauncherPath: filepath.Join(tmp, "launcher path", "bridge.vbs"),
		Environment: map[string]string{
			windowsHandoffFixtureAnchor:            anchor,
			"CXP_CI_WINDOWS_TEAMS_HANDOFF_RUNTIME": child,
			"CXP_CI_WINDOWS_TEAMS_HANDOFF_GRAPH":   graph.URL,
			"LOCALAPPDATA":                         filepath.Join(tmp, "logs"),
		},
	}
	args := buildTeamsServiceRunArgs(spec)
	if err := writeTeamsServiceWindowsTaskLauncherFiles(spec.WindowsTaskLauncherPath, spec, args); err != nil {
		t.Fatal(err)
	}
	principal, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	xml := writeProbeScript(t, tmp, "task.xml", buildTeamsServiceWindowsTaskXMLWithPrincipalUser(spec, principal.Uid))
	task := powershellSingleQuote("Codex Helper CI isolated Teams handoff " + filepath.Base(tmp) + fmt.Sprint(os.Getpid(), "-", time.Now().UnixNano()))
	if out, err := teamsServiceRunPowerShell(ctx, "$xml = Get-Content -LiteralPath "+powershellSingleQuote(xml)+" -Raw; Register-ScheduledTask -TaskName "+task+" -Xml $xml -ErrorAction Stop | Out-Null"); err != nil {
		t.Fatalf("register fixture task: %v\n%s", err, out)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = os.WriteFile(anchor+".stop-fixture", nil, 0o600)
		_, _ = teamsServiceRunPowerShell(ctx, "Stop-ScheduledTask -TaskName "+task+" -ErrorAction SilentlyContinue; "+
			teamsServiceWindowsStopBridgeChildrenPowerShell(task)+"Unregister-ScheduledTask -TaskName "+task+" -Confirm:$false -ErrorAction SilentlyContinue")
	})
	if out, err := teamsServiceRunPowerShell(ctx, "Enable-ScheduledTask -TaskName "+task+" -ErrorAction Stop | Out-Null; Start-ScheduledTask -TaskName "+task+" -ErrorAction Stop"); err != nil {
		t.Fatalf("start fixture task: %v\n%s", err, out)
	}
	var old lease.Info
	for {
		old, err = lease.Read(anchor)
		if err != nil {
			t.Fatal(err)
		}
		if stamp, ok := readTeamsPollStamp(tmp); old.HeadOK && ok && stamp.Generation == old.HeadGen {
			break
		}
		if err := sleepContext(ctx, 100*time.Millisecond); err != nil {
			logs, _ := filepath.Glob(filepath.Join(tmp, "logs", "codex-helper", "teams", "*"))
			for _, path := range logs {
				content, _ := os.ReadFile(path)
				t.Logf("%s: %s", filepath.Base(path), content)
			}
			t.Fatalf("fixture did not start: %v", err)
		}
	}
	exited, release := teamsWindowsBridgeExitProbe(old.Head)
	if exited == nil || exited() {
		t.Fatal("could not open the original bridge process")
	}
	defer release()
	// A process with the same executable and bridge arguments, launched
	// outside this task, must survive the restart.
	unrelatedAnchor := filepath.Join(t.TempDir(), "core.sqlite")
	unrelated := exec.Command(child, args...)
	unrelated.Env = append(os.Environ(),
		windowsHandoffFixtureAnchor+"="+unrelatedAnchor,
		"CXP_CI_WINDOWS_TEAMS_HANDOFF_RUNTIME="+child,
		"CXP_CI_WINDOWS_TEAMS_HANDOFF_GRAPH="+graph.URL)
	if err := unrelated.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = unrelated.Process.Kill()
		_ = unrelated.Wait()
	})
	unrelatedExited, releaseUnrelated := teamsWindowsBridgeExitProbe(lease.Record{PID: unrelated.Process.Pid, Wall: time.Now().UnixMilli()})
	if unrelatedExited == nil || unrelatedExited() {
		t.Fatal("could not open the unrelated fixture process")
	}
	defer releaseUnrelated()
	// Check the capture independently so that failures include the process
	// identities before Task Scheduler has removed their parents.
	capture := teamsServiceWindowsBridgeIdentityPowerShell() + teamsServiceWindowsCaptureTaskBridgePowerShell(task)
	diagnostic := `$bridgeRuntimeIdentity | Format-List | Out-String -Width 240; Write-Output ('Task action: ' + (Get-ScheduledTask -TaskName ` + task + `).Actions.Arguments)
$known = ` + strconv.Itoa(old.Head.PID) + `
for ($i = 0; $i -lt 5 -and $known; $i++) {
    $fixtureProcess = Get-CimInstance Win32_Process -Filter ('ProcessId=' + $known)
    if ($null -eq $fixtureProcess) { break }
    $fixtureProcess | Format-List Name,ProcessId,ParentProcessId,ExecutablePath,CommandLine,CreationDate | Out-String -Width 240
    $known = $fixtureProcess.ParentProcessId
}`
	captured, err := teamsServiceRunPowerShell(ctx, capture+"$bridgeRuntimeProcesses | ForEach-Object { Write-Output $_.Id }; ")
	if err != nil || !strings.Contains("
"+strings.ReplaceAll(string(captured), "
", "
"), "
"+strconv.Itoa(old.Head.PID)+"
") {
		detail, _ := teamsServiceRunPowerShell(ctx, capture+diagnostic)
		t.Fatalf("Windows Task Scheduler fixture failed to capture pid %d: %v\nCaptured: %s\n%s", old.Head.PID, err, captured, detail)
	}
	var output bytes.Buffer
	backend := teamsWindowsHandoffTestBackend{task: task}
	if err := restartTeamsWindowsTask(ctx, &output, anchor, host, teamsBootID(), 12*time.Second, 5*time.Second, backend); err != nil {
		t.Fatalf("restart fixture task: %v\n%s", err, output.String())
	}
	current, err := lease.Read(anchor)
	if err != nil || current.HeadGen <= old.HeadGen || current.Head.PID == old.Head.PID || !exited() {
		t.Fatalf("ownership did not move away from the old PID: old=%+v current=%+v err=%v\n%s", old.Head, current.Head, err, output.String())
	}
	if !strings.Contains(output.String(), "has polled Teams") {
		t.Fatalf("new bridge did not poll Teams: %s", output.String())
	}
	if unrelatedExited() {
		t.Fatal("restart stopped an unrelated Teams process")
	}
	logs, err := filepath.Glob(filepath.Join(tmp, "logs", "codex-helper", "teams", "*.stdout.log"))
	if err != nil || len(logs) != 2 {
		t.Fatalf("want both launcher attempts' stdout files, got %v (%v)", logs, err)
	}
}
