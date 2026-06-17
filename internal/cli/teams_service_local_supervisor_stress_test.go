package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	teamsstore "github.com/baaaaaaaka/codex-helper/internal/teams/store"
	"github.com/gofrs/flock"
)

type localSupervisorTerminateCall struct {
	pgid int
	pid  int
}

func withLocalSupervisorStressProcessHooks(t *testing.T) {
	t.Helper()
	prevAlive := teamsLocalSupervisorProcessAlive
	prevProcessGroupID := teamsLocalSupervisorProcessGroupID
	prevCurrentProcessGroupID := teamsLocalSupervisorCurrentProcessGroupID
	prevTerminateProcessGroup := teamsLocalSupervisorTerminateProcessGroup
	prevVerifyProcessIdentity := teamsLocalSupervisorVerifyProcessIdentity
	prevVerifyChildIdentity := teamsLocalSupervisorVerifyChildIdentity
	prevProcessStartTime := teamsLocalSupervisorProcessStartTime
	prevProcessArgs := teamsLocalSupervisorProcessArgs
	prevProcessEnvironment := teamsLocalSupervisorProcessEnvironment
	prevReleaseWait := teamsServiceLocalSupervisorReleaseWait
	t.Cleanup(func() {
		teamsLocalSupervisorProcessAlive = prevAlive
		teamsLocalSupervisorProcessGroupID = prevProcessGroupID
		teamsLocalSupervisorCurrentProcessGroupID = prevCurrentProcessGroupID
		teamsLocalSupervisorTerminateProcessGroup = prevTerminateProcessGroup
		teamsLocalSupervisorVerifyProcessIdentity = prevVerifyProcessIdentity
		teamsLocalSupervisorVerifyChildIdentity = prevVerifyChildIdentity
		teamsLocalSupervisorProcessStartTime = prevProcessStartTime
		teamsLocalSupervisorProcessArgs = prevProcessArgs
		teamsLocalSupervisorProcessEnvironment = prevProcessEnvironment
		teamsServiceLocalSupervisorReleaseWait = prevReleaseWait
	})
}

func TestTeamsServiceLocalSupervisorStatusActiveStressMatrix(t *testing.T) {
	lockCLITestHooks(t)
	withLocalSupervisorStressProcessHooks(t)

	now := time.Now()
	tmp := t.TempDir()
	configPath := filepath.Join(tmp, "local-supervisor.json")
	exePath := filepath.Join(tmp, "bin", "codex-proxy")
	wantArgs := []string{exePath, "teams", "service", "local-supervisor", "--config", configPath}
	wantEnv := map[string]string{
		"CODEX_PROXY_INSTALL_DIR":          exePath,
		"CODEX_HELPER_TEAMS_SERVICE_MODE":  "local-supervisor",
		"CODEX_HELPER_TEAMS_SERVICE_OWNER": "stress",
	}

	alive := true
	verifyErr := error(nil)
	argsErr := error(nil)
	startErr := error(nil)
	envErr := error(nil)
	liveArgs := append([]string{}, wantArgs...)
	liveStart := "boot-100"
	liveEnv := map[string]string{}
	for key, value := range wantEnv {
		liveEnv[key] = value
	}
	teamsLocalSupervisorProcessAlive = func(pid int) bool {
		return alive && pid > 0
	}
	teamsLocalSupervisorVerifyProcessIdentity = func(int, string) error {
		return verifyErr
	}
	teamsLocalSupervisorProcessArgs = func(int) ([]string, error) {
		if argsErr != nil {
			return nil, argsErr
		}
		return append([]string{}, liveArgs...), nil
	}
	teamsLocalSupervisorProcessStartTime = func(int) (string, error) {
		if startErr != nil {
			return "", startErr
		}
		return liveStart, nil
	}
	teamsLocalSupervisorProcessEnvironment = func(int) (map[string]string, error) {
		if envErr != nil {
			return nil, envErr
		}
		out := map[string]string{}
		for key, value := range liveEnv {
			out[key] = value
		}
		return out, nil
	}

	baseStatus := func(pid int) teamsServiceLocalSupervisorStatus {
		env := map[string]string{}
		for key, value := range wantEnv {
			env[key] = value
		}
		return teamsServiceLocalSupervisorStatus{
			Version:       teamsServiceLocalSupervisorStatusVersion,
			ConfigPath:    configPath,
			SupervisorPID: pid,
			State:         "running",
			UpdatedAt:     now,
			SupervisorIdentity: &teamsServiceLocalSupervisorProcessIdentity{
				Executable:    exePath,
				Args:          append([]string{}, wantArgs...),
				Environment:   env,
				ProcStartTime: "boot-100",
			},
		}
	}

	cases := []struct {
		name      string
		mutate    func(*teamsServiceLocalSupervisorStatus)
		setup     func()
		want      bool
		wantAlive bool
	}{
		{
			name:      "fresh verified identity",
			want:      true,
			wantAlive: true,
		},
		{
			name: "fresh verified legacy status without recorded identity",
			mutate: func(status *teamsServiceLocalSupervisorStatus) {
				status.SupervisorIdentity = nil
			},
			want:      true,
			wantAlive: true,
		},
		{
			name:      "dead supervisor pid",
			want:      false,
			wantAlive: false,
		},
		{
			name: "missing config path",
			mutate: func(status *teamsServiceLocalSupervisorStatus) {
				status.ConfigPath = ""
			},
			want:      false,
			wantAlive: true,
		},
		{
			name: "zero heartbeat",
			mutate: func(status *teamsServiceLocalSupervisorStatus) {
				status.UpdatedAt = time.Time{}
			},
			want:      false,
			wantAlive: true,
		},
		{
			name: "stale heartbeat",
			mutate: func(status *teamsServiceLocalSupervisorStatus) {
				status.UpdatedAt = now.Add(-teamsServiceLocalSupervisorStatusFreshness - time.Second)
			},
			want:      false,
			wantAlive: true,
		},
		{
			name: "process identity probe failure",
			setup: func() {
				verifyErr = errors.New("identity probe failed")
			},
			want:      false,
			wantAlive: true,
		},
		{
			name: "recorded args missing from live process",
			setup: func() {
				liveArgs = []string{exePath, "teams", "run"}
			},
			want:      false,
			wantAlive: true,
		},
		{
			name: "recorded args unreadable",
			setup: func() {
				argsErr = errors.New("args unreadable")
			},
			want:      false,
			wantAlive: true,
		},
		{
			name: "recorded start time changed",
			setup: func() {
				liveStart = "boot-101"
			},
			want:      false,
			wantAlive: true,
		},
		{
			name: "recorded start time unreadable",
			setup: func() {
				startErr = errors.New("start time unreadable")
			},
			want:      false,
			wantAlive: true,
		},
		{
			name: "recorded environment missing",
			setup: func() {
				delete(liveEnv, "CODEX_HELPER_TEAMS_SERVICE_OWNER")
			},
			want:      false,
			wantAlive: true,
		},
		{
			name: "recorded environment unreadable",
			setup: func() {
				envErr = errors.New("environment unreadable")
			},
			want:      false,
			wantAlive: true,
		},
	}

	for round := 0; round < 16; round++ {
		for _, tc := range cases {
			tc := tc
			t.Run(fmt.Sprintf("round-%02d/%s", round, tc.name), func(t *testing.T) {
				alive = tc.wantAlive
				verifyErr = nil
				argsErr = nil
				startErr = nil
				envErr = nil
				liveArgs = append([]string{}, wantArgs...)
				liveStart = "boot-100"
				liveEnv = map[string]string{}
				for key, value := range wantEnv {
					liveEnv[key] = value
				}
				if tc.setup != nil {
					tc.setup()
				}
				status := baseStatus(10_000 + round)
				if tc.mutate != nil {
					tc.mutate(&status)
				}
				if got := teamsServiceLocalSupervisorStatusActive(status, now); got != tc.want {
					t.Fatalf("active = %t, want %t for status %#v", got, tc.want, status)
				}
			})
		}
	}
}

func TestTeamsServiceLocalSupervisorStopStressFailClosedMatrix(t *testing.T) {
	lockCLITestHooks(t)
	withLocalSupervisorStressProcessHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	configPath, err := teamsServiceLocalSupervisorConfigPath()
	if err != nil {
		t.Fatalf("config path: %v", err)
	}
	spec := teamsServiceSpec{
		Executable: filepath.Join(tmp, "bin", "codex-proxy"),
		WorkingDir: tmp,
	}
	if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Enabled: true,
		Spec:    spec,
	}); err != nil {
		t.Fatalf("write config: %v", err)
	}

	const currentPGID = 9000
	alivePids := map[int]bool{}
	processGroups := map[int]int{}
	verifySupervisorErr := error(nil)
	verifyChildErr := error(nil)
	terminateErr := error(nil)
	var terminateCalls []localSupervisorTerminateCall
	teamsLocalSupervisorCurrentProcessGroupID = func() int { return currentPGID }
	teamsLocalSupervisorProcessAlive = func(pid int) bool { return alivePids[pid] }
	teamsLocalSupervisorProcessGroupID = func(pid int) (int, error) {
		pgid, ok := processGroups[pid]
		if !ok {
			return 0, fmt.Errorf("unknown pid %d", pid)
		}
		return pgid, nil
	}
	teamsLocalSupervisorVerifyProcessIdentity = func(int, string) error {
		return verifySupervisorErr
	}
	teamsLocalSupervisorVerifyChildIdentity = func(int, teamsServiceSpec) error {
		return verifyChildErr
	}
	teamsLocalSupervisorTerminateProcessGroup = func(pgid int, pid int, _ time.Duration) error {
		terminateCalls = append(terminateCalls, localSupervisorTerminateCall{pgid: pgid, pid: pid})
		return terminateErr
	}

	baseStatus := func(pid int, pgid int) teamsServiceLocalSupervisorStatus {
		return teamsServiceLocalSupervisorStatus{
			Version:        teamsServiceLocalSupervisorStatusVersion,
			ConfigPath:     configPath,
			SupervisorPID:  pid,
			SupervisorPGID: pgid,
			State:          "running",
			UpdatedAt:      time.Now(),
		}
	}

	cases := []struct {
		name           string
		status         teamsServiceLocalSupervisorStatus
		alive          map[int]bool
		pgids          map[int]int
		verifySupErr   error
		verifyChildErr error
		terminateErr   error
		wantCalls      []localSupervisorTerminateCall
		wantErr        string
	}{
		{
			name:      "supervisor only",
			status:    baseStatus(2001, 3001),
			alive:     map[int]bool{2001: true},
			pgids:     map[int]int{2001: 3001},
			wantCalls: []localSupervisorTerminateCall{{pgid: 3001, pid: 2001}},
		},
		{
			name: "distinct child then supervisor",
			status: func() teamsServiceLocalSupervisorStatus {
				status := baseStatus(2002, 3002)
				status.ChildPID = 4002
				status.ChildPGID = 5002
				return status
			}(),
			alive:     map[int]bool{2002: true, 4002: true},
			pgids:     map[int]int{2002: 3002, 4002: 5002},
			wantCalls: []localSupervisorTerminateCall{{pgid: 5002, pid: 4002}, {pgid: 3002, pid: 2002}},
		},
		{
			name: "child in supervisor group terminates group once",
			status: func() teamsServiceLocalSupervisorStatus {
				status := baseStatus(2003, 3003)
				status.ChildPID = 4003
				status.ChildPGID = 3003
				return status
			}(),
			alive:     map[int]bool{2003: true, 4003: true},
			pgids:     map[int]int{2003: 3003},
			wantCalls: []localSupervisorTerminateCall{{pgid: 3003, pid: 2003}},
		},
		{
			name: "dead supervisor still verifies and stops live child",
			status: func() teamsServiceLocalSupervisorStatus {
				status := baseStatus(2004, 3004)
				status.ChildPID = 4004
				status.ChildPGID = 5004
				return status
			}(),
			alive:     map[int]bool{4004: true},
			pgids:     map[int]int{4004: 5004},
			wantCalls: []localSupervisorTerminateCall{{pgid: 5004, pid: 4004}},
		},
		{
			name:      "refuses supervisor current process group",
			status:    baseStatus(2005, currentPGID),
			alive:     map[int]bool{2005: true},
			pgids:     map[int]int{2005: currentPGID},
			wantErr:   "matches the current process group",
			wantCalls: nil,
		},
		{
			name: "refuses child current process group",
			status: func() teamsServiceLocalSupervisorStatus {
				status := baseStatus(2006, 3006)
				status.ChildPID = 4006
				status.ChildPGID = currentPGID
				return status
			}(),
			alive:     map[int]bool{2006: true, 4006: true},
			pgids:     map[int]int{2006: 3006},
			wantErr:   "matches the current process group",
			wantCalls: nil,
		},
		{
			name:    "refuses stale supervisor process group",
			status:  baseStatus(2007, 3333),
			alive:   map[int]bool{2007: true},
			pgids:   map[int]int{2007: 3007},
			wantErr: "stale local supervisor process group",
		},
		{
			name: "refuses stale child process group",
			status: func() teamsServiceLocalSupervisorStatus {
				status := baseStatus(2008, 3008)
				status.ChildPID = 4008
				status.ChildPGID = 4444
				return status
			}(),
			alive:     map[int]bool{2008: true, 4008: true},
			pgids:     map[int]int{2008: 3008, 4008: 5008},
			wantErr:   "stale local supervisor process group",
			wantCalls: nil,
		},
		{
			name: "refuses child process group without verifiable child pid",
			status: func() teamsServiceLocalSupervisorStatus {
				status := baseStatus(2009, 3009)
				status.ChildPGID = 5009
				return status
			}(),
			alive:     map[int]bool{2009: true},
			pgids:     map[int]int{2009: 3009},
			wantErr:   "has no child pid to verify",
			wantCalls: nil,
		},
		{
			name:         "refuses supervisor identity mismatch",
			status:       baseStatus(2010, 3010),
			alive:        map[int]bool{2010: true},
			pgids:        map[int]int{2010: 3010},
			verifySupErr: errors.New("supervisor identity mismatch"),
			wantErr:      "supervisor identity mismatch",
			wantCalls:    nil,
		},
		{
			name: "refuses child identity mismatch",
			status: func() teamsServiceLocalSupervisorStatus {
				status := baseStatus(2011, 3011)
				status.ChildPID = 4011
				status.ChildPGID = 5011
				return status
			}(),
			alive:          map[int]bool{2011: true, 4011: true},
			pgids:          map[int]int{2011: 3011, 4011: 5011},
			verifyChildErr: errors.New("child identity mismatch"),
			wantErr:        "child identity mismatch",
			wantCalls:      nil,
		},
		{
			name:         "surfaces supervisor termination failure",
			status:       baseStatus(2012, 3012),
			alive:        map[int]bool{2012: true},
			pgids:        map[int]int{2012: 3012},
			terminateErr: errors.New("terminate failed"),
			wantCalls:    []localSupervisorTerminateCall{{pgid: 3012, pid: 2012}},
			wantErr:      "terminate failed",
		},
	}

	for round := 0; round < 8; round++ {
		for _, tc := range cases {
			tc := tc
			t.Run(fmt.Sprintf("round-%02d/%s", round, tc.name), func(t *testing.T) {
				alivePids = map[int]bool{}
				for pid, alive := range tc.alive {
					alivePids[pid] = alive
				}
				processGroups = map[int]int{}
				for pid, pgid := range tc.pgids {
					processGroups[pid] = pgid
				}
				verifySupervisorErr = tc.verifySupErr
				verifyChildErr = tc.verifyChildErr
				terminateErr = tc.terminateErr
				terminateCalls = nil
				if err := writeTeamsServiceLocalSupervisorStatus(tc.status); err != nil {
					t.Fatalf("write status: %v", err)
				}
				_, err := (teamsServiceLocalSupervisorBackend{}).Run(context.Background(), "stop")
				if tc.wantErr != "" {
					if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
						t.Fatalf("stop err = %v, want containing %q", err, tc.wantErr)
					}
				} else if err != nil {
					t.Fatalf("stop err = %v, want nil", err)
				}
				if !reflect.DeepEqual(terminateCalls, tc.wantCalls) {
					t.Fatalf("terminate calls = %#v, want %#v", terminateCalls, tc.wantCalls)
				}
			})
		}
	}
}

func TestTeamsServiceLocalSupervisorStickyStressIgnoresBareLock(t *testing.T) {
	lockCLITestHooks(t)
	withLocalSupervisorStressProcessHooks(t)

	tmp := t.TempDir()
	isolateTeamsUserDirsForTest(t, tmp)
	systemdAvailable := true
	withTeamsServiceTestHooks(t, teamsServiceTestHooks{
		goos:                 "linux",
		exe:                  filepath.Join(tmp, "bin", "codex-proxy"),
		cwd:                  tmp,
		unitDir:              filepath.Join(tmp, "systemd", "user"),
		runner:               &recordingTeamsServiceRunner{},
		systemdUserAvailable: &systemdAvailable,
	})
	lockPath, err := teamsServiceLocalSupervisorLockPath()
	if err != nil {
		t.Fatalf("lock path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(lockPath), 0o700); err != nil {
		t.Fatalf("mkdir lock dir: %v", err)
	}
	lock := flock.New(lockPath)
	locked, err := lock.TryLock()
	if err != nil || !locked {
		t.Fatalf("acquire test lock locked=%v err=%v", locked, err)
	}
	defer func() { _ = lock.Unlock() }()

	for round := 0; round < 32; round++ {
		backend, err := teamsServiceBackendForCurrentPlatform()
		if err != nil {
			t.Fatalf("round %d backend: %v", round, err)
		}
		if got := backend.ID(); got != "systemd-user" {
			t.Fatalf("round %d backend ID = %q, want systemd-user for bare held lock", round, got)
		}
	}

	configPath, err := teamsServiceLocalSupervisorConfigPath()
	if err != nil {
		t.Fatalf("config path: %v", err)
	}
	if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
		Version: teamsServiceLocalSupervisorConfigVersion,
		Spec: teamsServiceSpec{
			Executable: filepath.Join(tmp, "bin", "codex-proxy"),
			WorkingDir: tmp,
		},
	}); err != nil {
		t.Fatalf("write disabled config: %v", err)
	}
	backend, err := teamsServiceBackendForCurrentPlatform()
	if err != nil {
		t.Fatalf("backend with disabled config: %v", err)
	}
	if got := backend.ID(); got != "systemd-user" {
		t.Fatalf("backend ID with disabled config and bare held lock = %q, want systemd-user", got)
	}

	teamsLocalSupervisorProcessAlive = func(pid int) bool { return pid == 3030 }
	teamsLocalSupervisorVerifyProcessIdentity = func(int, string) error { return nil }
	if err := writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
		Version:       teamsServiceLocalSupervisorStatusVersion,
		ConfigPath:    configPath,
		SupervisorPID: 3030,
		State:         "running",
		UpdatedAt:     time.Now(),
	}); err != nil {
		t.Fatalf("write active status: %v", err)
	}
	backend, err = teamsServiceBackendForCurrentPlatform()
	if err != nil {
		t.Fatalf("backend with active status: %v", err)
	}
	if got := backend.ID(); got != "local-supervisor" {
		t.Fatalf("backend ID with active status = %q, want local-supervisor", got)
	}
}

func TestTeamsServiceLocalSupervisorFilePathStressRejectsNonRegularFiles(t *testing.T) {
	lockCLITestHooks(t)

	cases := []struct {
		name string
		run  func(*testing.T) error
	}{
		{
			name: "status directory",
			run: func(t *testing.T) error {
				path, err := teamsServiceLocalSupervisorStatusPath()
				if err != nil {
					return err
				}
				if err := os.MkdirAll(path, 0o700); err != nil {
					return err
				}
				_, _, err = readTeamsServiceLocalSupervisorStatus()
				return err
			},
		},
		{
			name: "lock directory",
			run: func(t *testing.T) error {
				path, err := teamsServiceLocalSupervisorLockPath()
				if err != nil {
					return err
				}
				if err := os.MkdirAll(path, 0o700); err != nil {
					return err
				}
				_, err = teamsServiceLocalSupervisorLockHeld()
				return err
			},
		},
		{
			name: "log directory",
			run: func(t *testing.T) error {
				path, err := teamsServiceLocalSupervisorLogPath()
				if err != nil {
					return err
				}
				if err := os.MkdirAll(path, 0o700); err != nil {
					return err
				}
				file, err := openTeamsServiceLocalSupervisorLog(path)
				if file != nil {
					_ = file.Close()
				}
				return err
			},
		},
		{
			name: "config directory",
			run: func(t *testing.T) error {
				path, err := teamsServiceLocalSupervisorConfigPath()
				if err != nil {
					return err
				}
				if err := os.MkdirAll(path, 0o700); err != nil {
					return err
				}
				_, err = readTeamsServiceLocalSupervisorConfig(path)
				return err
			},
		},
	}

	for round := 0; round < 8; round++ {
		for _, tc := range cases {
			tc := tc
			t.Run(fmt.Sprintf("round-%02d/%s", round, tc.name), func(t *testing.T) {
				tmp := t.TempDir()
				isolateTeamsUserDirsForTest(t, tmp)
				err := tc.run(t)
				if err == nil || !strings.Contains(err.Error(), "must be a regular file") {
					t.Fatalf("err = %v, want regular-file rejection", err)
				}
			})
		}
	}
}

func TestTeamsServiceLocalSupervisorProcessEnvStressSanitizesInheritedEnv(t *testing.T) {
	lockCLITestHooks(t)

	blocked := map[string]string{
		"CODEX_HELPER_TEAMS_CHILD":      "1",
		"CODEX_HELPER_TEAMS_PARENT_PID": "123",
		"CODEX_HELPER_TEAMS_SERVICE":    "1",
		"CODEX_PROXY_DEBUG":             "1",
		"DBUS_SESSION_BUS_ADDRESS":      "unix:path=/tmp/dbus",
		"HTTP_PROXY":                    "http://proxy.example",
		"HTTPS_PROXY":                   "http://proxy.example",
		"LD_PRELOAD":                    "/tmp/inject.so",
		"NO_PROXY":                      "*",
		"SSL_CERT_FILE":                 "/tmp/cert.pem",
		"XDG_RUNTIME_DIR":               "/run/user/123",
	}
	for key, value := range blocked {
		t.Setenv(key, value)
	}
	t.Setenv("HOME", "/home/stress")
	t.Setenv("PATH", "/usr/bin")
	t.Setenv("LC_ALL", "C.UTF-8")
	t.Setenv("XDG_CONFIG_HOME", "/home/stress/.config")
	t.Setenv("XDG_STATE_HOME", "/home/stress/.local/state")

	env := teamsServiceLocalSupervisorProcessEnv(map[string]string{
		"CODEX_HELPER_TEAMS_SERVICE_MODE": "local-supervisor",
		"CODEX_PROXY_INSTALL_DIR":         "/opt/codex-proxy",
	})
	envMap := map[string]string{}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			envMap[key] = value
		}
	}
	for key := range blocked {
		if _, ok := envMap[key]; ok {
			t.Fatalf("blocked inherited env %s leaked into local supervisor env: %#v", key, env)
		}
	}
	for key, want := range map[string]string{
		"HOME":                            "/home/stress",
		"PATH":                            "/usr/bin",
		"LC_ALL":                          "C.UTF-8",
		"XDG_CONFIG_HOME":                 "/home/stress/.config",
		"XDG_STATE_HOME":                  "/home/stress/.local/state",
		"CODEX_HELPER_TEAMS_SERVICE_MODE": "local-supervisor",
		"CODEX_PROXY_INSTALL_DIR":         "/opt/codex-proxy",
	} {
		if got := envMap[key]; got != want {
			t.Fatalf("env %s = %q, want %q in %#v", key, got, want, env)
		}
	}
}

func TestTeamsServiceLocalSupervisorReleaseFailureStressCleanupErrors(t *testing.T) {
	lockCLITestHooks(t)
	withLocalSupervisorStressProcessHooks(t)

	const pid = 7100
	const pgid = 8100
	configPath := filepath.Join(t.TempDir(), "local-supervisor.json")
	releaseErr := errors.New("release failed")
	terminateErr := error(nil)
	var terminateCalls []localSupervisorTerminateCall
	teamsServiceLocalSupervisorReleaseWait = 10 * time.Millisecond
	teamsLocalSupervisorProcessAlive = func(gotPID int) bool { return gotPID == pid }
	teamsLocalSupervisorVerifyProcessIdentity = func(gotPID int, gotConfigPath string) error {
		if gotPID != pid || gotConfigPath != configPath {
			t.Fatalf("verify pid/config = %d %q, want %d %q", gotPID, gotConfigPath, pid, configPath)
		}
		return nil
	}
	teamsLocalSupervisorProcessGroupID = func(gotPID int) (int, error) {
		if gotPID != pid {
			return 0, fmt.Errorf("unexpected pid %d", gotPID)
		}
		return pgid, nil
	}
	teamsLocalSupervisorCurrentProcessGroupID = func() int { return 1 }
	teamsLocalSupervisorTerminateProcessGroup = func(gotPGID int, gotPID int, _ time.Duration) error {
		terminateCalls = append(terminateCalls, localSupervisorTerminateCall{pgid: gotPGID, pid: gotPID})
		return terminateErr
	}

	terminateErr = errors.New("permission denied")
	err := handleTeamsServiceLocalSupervisorReleaseFailure(pid, configPath, releaseErr, func() error {
		t.Fatal("wait should not be called when cleanup termination fails")
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "cleanup failed") || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("cleanup failure err = %v, want cleanup failed with permission denied", err)
	}
	if want := []localSupervisorTerminateCall{{pgid: pgid, pid: pid}}; !reflect.DeepEqual(terminateCalls, want) {
		t.Fatalf("cleanup failure terminate calls = %#v, want %#v", terminateCalls, want)
	}

	terminateErr = nil
	terminateCalls = nil
	waitBlock := make(chan struct{})
	err = handleTeamsServiceLocalSupervisorReleaseFailure(pid, configPath, releaseErr, func() error {
		<-waitBlock
		return nil
	})
	close(waitBlock)
	if err == nil || !strings.Contains(err.Error(), "cleanup wait timed out") {
		t.Fatalf("cleanup wait err = %v, want timeout", err)
	}
	if want := []localSupervisorTerminateCall{{pgid: pgid, pid: pid}}; !reflect.DeepEqual(terminateCalls, want) {
		t.Fatalf("cleanup wait terminate calls = %#v, want %#v", terminateCalls, want)
	}

	terminateCalls = nil
	teamsLocalSupervisorCurrentProcessGroupID = func() int { return pgid }
	err = handleTeamsServiceLocalSupervisorReleaseFailure(pid, configPath, releaseErr, nil)
	if err == nil || !strings.Contains(err.Error(), "matches the current process group") {
		t.Fatalf("current process group err = %v, want refusal", err)
	}
	if len(terminateCalls) != 0 {
		t.Fatalf("current process group terminate calls = %#v, want none", terminateCalls)
	}
}

func TestTeamsServiceLocalSupervisorActivationHandoffStressMatrix(t *testing.T) {
	lockCLITestHooks(t)

	currentSupervisorPID := 8001
	currentChildPID := 8002
	cases := []struct {
		name             string
		goos             string
		service          string
		backend          string
		marker           string
		writeStatus      bool
		statusSupervisor int
		statusChild      int
		statusUpdatedAt  func() time.Time
		detachedErr      error
		preActivation    bool
		wantScheduled    bool
		wantErr          string
		wantActivation   string
		wantDetached     bool
	}{
		{
			name:             "old supervisor schedules handoff",
			goos:             "linux",
			service:          "1",
			backend:          "local-supervisor",
			writeStatus:      true,
			statusSupervisor: currentSupervisorPID,
			statusChild:      currentChildPID,
			statusUpdatedAt:  time.Now,
			wantScheduled:    true,
			wantActivation:   "scheduled",
			wantDetached:     true,
		},
		{
			name:             "schedule failure is durable",
			goos:             "linux",
			service:          "1",
			backend:          "local-supervisor",
			writeStatus:      true,
			statusSupervisor: currentSupervisorPID,
			statusChild:      currentChildPID,
			statusUpdatedAt:  time.Now,
			detachedErr:      errors.New("detached start failed"),
			wantErr:          "detached start failed",
			wantActivation:   "failed",
			wantDetached:     true,
		},
		{
			name:           "current supervisor marker completes previous handoff",
			goos:           "linux",
			service:        "1",
			backend:        "local-supervisor",
			marker:         buildVersion(),
			preActivation:  true,
			wantActivation: "success",
		},
		{
			name:           "missing status skips",
			goos:           "linux",
			service:        "1",
			backend:        "local-supervisor",
			wantActivation: "",
		},
		{
			name:             "identity mismatch skips",
			goos:             "linux",
			service:          "1",
			backend:          "local-supervisor",
			writeStatus:      true,
			statusSupervisor: currentSupervisorPID + 1000,
			statusChild:      currentChildPID,
			statusUpdatedAt:  time.Now,
			wantActivation:   "",
		},
		{
			name:             "stale status skips",
			goos:             "linux",
			service:          "1",
			backend:          "local-supervisor",
			writeStatus:      true,
			statusSupervisor: currentSupervisorPID,
			statusChild:      currentChildPID,
			statusUpdatedAt:  func() time.Time { return time.Now().Add(-teamsServiceLocalSupervisorStatusFreshness - time.Minute) },
			wantActivation:   "",
		},
		{
			name:           "systemd backend skips",
			goos:           "linux",
			service:        "1",
			backend:        "systemd",
			wantActivation: "",
		},
		{
			name:           "non linux skips",
			goos:           "darwin",
			service:        "1",
			backend:        "local-supervisor",
			wantActivation: "",
		},
		{
			name:           "non service skips",
			goos:           "linux",
			backend:        "local-supervisor",
			wantActivation: "",
		},
	}

	for round := 0; round < 6; round++ {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("round-%02d/%s", round, tc.name), func(t *testing.T) {
				tmp := t.TempDir()
				isolateTeamsUserDirsForTest(t, tmp)
				exe := filepath.Join(tmp, "bin", "codex-proxy")
				withTeamsServiceTestHooks(t, teamsServiceTestHooks{
					goos: firstNonEmptyCLI(tc.goos, "linux"),
					exe:  exe,
					cwd:  tmp,
				})
				t.Setenv("CODEX_HELPER_TEAMS_SERVICE", tc.service)
				t.Setenv(envTeamsLinuxServiceBackend, tc.backend)
				t.Setenv(envTeamsLocalSupervisorVersion, tc.marker)
				configPath, err := teamsServiceLocalSupervisorConfigPath()
				if err != nil {
					t.Fatalf("config path: %v", err)
				}
				if err := writeTeamsServiceLocalSupervisorConfig(configPath, teamsServiceLocalSupervisorConfig{
					Version: teamsServiceLocalSupervisorConfigVersion,
					Enabled: true,
					Spec: teamsServiceSpec{
						Executable: exe,
						WorkingDir: tmp,
					},
				}); err != nil {
					t.Fatalf("write config: %v", err)
				}
				if tc.writeStatus {
					if err := writeTeamsServiceLocalSupervisorStatus(teamsServiceLocalSupervisorStatus{
						Version:       teamsServiceLocalSupervisorStatusVersion,
						ConfigPath:    configPath,
						SupervisorPID: tc.statusSupervisor,
						ChildPID:      tc.statusChild,
						State:         "running",
						UpdatedAt:     tc.statusUpdatedAt(),
					}); err != nil {
						t.Fatalf("write status: %v", err)
					}
				}
				if tc.preActivation {
					if err := writeTeamsServiceLocalSupervisorActivation(teamsServiceLocalSupervisorActivation{
						Version:       teamsServiceLocalSupervisorActivationVersion,
						Status:        "scheduled",
						TargetVersion: buildVersion(),
						ScheduledAt:   time.Now().Add(-time.Minute),
						UpdatedAt:     time.Now().Add(-time.Minute),
					}); err != nil {
						t.Fatalf("write activation: %v", err)
					}
				}

				prevAlive := teamsLocalSupervisorProcessAlive
				prevVerify := teamsLocalSupervisorVerifyProcessIdentity
				prevDetached := teamsServiceStartDetached
				prevWait := teamsLegacyLocalSupervisorActivationWait
				prevCurrentProcessID := teamsCurrentProcessID
				prevParentProcessID := teamsParentProcessID
				t.Cleanup(func() {
					teamsLocalSupervisorProcessAlive = prevAlive
					teamsLocalSupervisorVerifyProcessIdentity = prevVerify
					teamsServiceStartDetached = prevDetached
					teamsLegacyLocalSupervisorActivationWait = prevWait
					teamsCurrentProcessID = prevCurrentProcessID
					teamsParentProcessID = prevParentProcessID
					teamsLegacyLocalSupervisorRestartScheduled.Store(false)
				})
				teamsLegacyLocalSupervisorRestartScheduled.Store(false)
				teamsLegacyLocalSupervisorActivationWait = time.Minute
				teamsCurrentProcessID = func() int { return currentChildPID }
				teamsParentProcessID = func() int { return currentSupervisorPID }
				teamsLocalSupervisorProcessAlive = func(pid int) bool {
					return pid == currentSupervisorPID || pid == currentChildPID || pid == tc.statusSupervisor || pid == tc.statusChild
				}
				teamsLocalSupervisorVerifyProcessIdentity = func(pid int, gotConfigPath string) error {
					if pid != tc.statusSupervisor || gotConfigPath != configPath {
						return fmt.Errorf("unexpected supervisor identity probe pid=%d config=%q", pid, gotConfigPath)
					}
					return nil
				}
				detachedCalls := 0
				teamsServiceStartDetached = func(context.Context, string, ...string) error {
					detachedCalls++
					return tc.detachedErr
				}

				scheduled, err := maybeScheduleLegacyLocalSupervisorRestart(context.Background())
				if tc.wantErr != "" {
					if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
						t.Fatalf("err = %v, want containing %q", err, tc.wantErr)
					}
				} else if err != nil {
					t.Fatalf("unexpected err: %v", err)
				}
				if scheduled != tc.wantScheduled {
					t.Fatalf("scheduled = %v, want %v", scheduled, tc.wantScheduled)
				}
				if (detachedCalls > 0) != tc.wantDetached {
					t.Fatalf("detachedCalls = %d, want detached=%v", detachedCalls, tc.wantDetached)
				}
				activation, ok, readErr := readTeamsServiceLocalSupervisorActivation()
				if readErr != nil {
					t.Fatalf("read activation: %v", readErr)
				}
				if tc.wantActivation == "" {
					if ok {
						t.Fatalf("activation = %#v, want none", activation)
					}
					return
				}
				if !ok || activation.Status != tc.wantActivation {
					t.Fatalf("activation = %#v ok=%v, want status %q", activation, ok, tc.wantActivation)
				}
			})
		}
	}
}

func TestTeamsServiceLocalSupervisorActivationWaitStressMatrix(t *testing.T) {
	lockCLITestHooks(t)

	cases := []struct {
		name       string
		status     string
		target     string
		missing    bool
		wantStatus string
	}{
		{name: "scheduled current expires", status: "scheduled", target: buildVersion(), wantStatus: "expired"},
		{name: "scheduled other target stays", status: "scheduled", target: "v0.0.1", wantStatus: "scheduled"},
		{name: "success stays", status: "success", target: buildVersion(), wantStatus: "success"},
		{name: "failed stays", status: "failed", target: buildVersion(), wantStatus: "failed"},
		{name: "missing activation is ignored", missing: true},
	}
	for round := 0; round < 8; round++ {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("round-%02d/%s", round, tc.name), func(t *testing.T) {
				tmp := t.TempDir()
				isolateTeamsUserDirsForTest(t, tmp)
				prevWait := teamsLegacyLocalSupervisorActivationWait
				t.Cleanup(func() {
					teamsLegacyLocalSupervisorActivationWait = prevWait
				})
				teamsLegacyLocalSupervisorActivationWait = 0
				if !tc.missing {
					if err := writeTeamsServiceLocalSupervisorActivation(teamsServiceLocalSupervisorActivation{
						Version:       teamsServiceLocalSupervisorActivationVersion,
						Status:        tc.status,
						TargetVersion: tc.target,
						ScheduledAt:   time.Now().Add(-time.Minute),
						DeadlineAt:    time.Now().Add(-time.Second),
						UpdatedAt:     time.Now().Add(-time.Minute),
					}); err != nil {
						t.Fatalf("write activation: %v", err)
					}
				}
				if err := waitForLegacyLocalSupervisorActivationHandoff(context.Background()); err != nil {
					t.Fatalf("wait activation: %v", err)
				}
				activation, ok, err := readTeamsServiceLocalSupervisorActivation()
				if err != nil {
					t.Fatalf("read activation: %v", err)
				}
				if tc.missing {
					if ok {
						t.Fatalf("activation = %#v, want missing", activation)
					}
					return
				}
				if !ok || activation.Status != tc.wantStatus {
					t.Fatalf("activation = %#v ok=%v, want status %q", activation, ok, tc.wantStatus)
				}
			})
		}
	}
}

func TestLegacyLocalSupervisorActivationNoticeStressMatrix(t *testing.T) {
	lockCLITestHooks(t)

	cases := []struct {
		name      string
		status    string
		chatID    string
		wantQueue bool
	}{
		{name: "expired with control chat queues", status: "expired", chatID: "control-chat", wantQueue: true},
		{name: "failed with control chat queues", status: "failed", chatID: "control-chat", wantQueue: true},
		{name: "scheduled with control chat skips", status: "scheduled", chatID: "control-chat"},
		{name: "success with control chat skips", status: "success", chatID: "control-chat"},
		{name: "expired without control chat skips", status: "expired"},
	}
	for round := 0; round < 8; round++ {
		for _, tc := range cases {
			t.Run(fmt.Sprintf("round-%02d/%s", round, tc.name), func(t *testing.T) {
				tmp := t.TempDir()
				isolateTeamsUserDirsForTest(t, tmp)
				st, err := openTeamsStore()
				if err != nil {
					t.Fatalf("open store: %v", err)
				}
				if tc.chatID != "" {
					if err := st.Update(context.Background(), func(state *teamsstore.State) error {
						state.ControlChat.TeamsChatID = tc.chatID
						return nil
					}); err != nil {
						t.Fatalf("seed control chat: %v", err)
					}
				}
				activation := teamsServiceLocalSupervisorActivation{
					Version:          teamsServiceLocalSupervisorActivationVersion,
					Status:           tc.status,
					TargetVersion:    buildVersion(),
					OldSupervisorPID: 1234,
					Message:          "stress message",
				}
				for i := 0; i < 2; i++ {
					if err := queueLegacyLocalSupervisorActivationAttentionNotice(context.Background(), "", activation); err != nil {
						t.Fatalf("queue notice attempt %d: %v", i, err)
					}
				}
				state, err := st.Load(context.Background())
				if err != nil {
					t.Fatalf("load store: %v", err)
				}
				count := 0
				for _, msg := range state.OutboxMessages {
					if strings.HasPrefix(msg.Kind, "local-supervisor-activation-") {
						count++
						if msg.TeamsChatID != tc.chatID || !msg.MentionOwner || !msg.UpgradeNonBlocking || !strings.Contains(msg.Body, "stress message") {
							t.Fatalf("notice = %#v, want protected control notice", msg)
						}
					}
				}
				if tc.wantQueue {
					if count != 1 {
						t.Fatalf("queued notices = %d, want 1", count)
					}
				} else if count != 0 {
					t.Fatalf("queued notices = %d, want 0", count)
				}
			})
		}
	}
}
