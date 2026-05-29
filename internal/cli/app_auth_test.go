package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/spf13/cobra"

	"github.com/baaaaaaaka/codex-helper/internal/codexrunner"
	"github.com/baaaaaaaka/codex-helper/internal/config"
)

func TestRunCodexAppDeviceCodeAuthStartsLoginOpensBrowserAndWaits(t *testing.T) {
	lockCLITestHooks(t)

	transport := newFakeCodexAppAuthTransport(
		`{"id":1,"result":{"userAgent":"codex-test"}}`,
		`{"id":2,"result":{"account":null,"requiresOpenaiAuth":true}}`,
		`{"id":3,"result":{"type":"chatgptDeviceCode","loginId":"login-1","verificationUrl":"https://chatgpt.example/codex/device","userCode":"CODE-123"}}`,
		`{"method":"account/login/completed","params":{"loginId":"login-1","success":true}}`,
		`{"id":4,"result":{"account":{"type":"chatgpt","email":"user@example.com","planType":"pro"},"requiresOpenaiAuth":true}}`,
	)
	starter := &fakeCodexAppAuthStarter{transport: transport}

	prevStarter := codexAppAuthStarter
	prevOpen := codexAppOpenAuthURLFn
	t.Cleanup(func() {
		codexAppAuthStarter = prevStarter
		codexAppOpenAuthURLFn = prevOpen
	})
	codexAppAuthStarter = starter

	var openedURL string
	var openedProxy string
	codexAppOpenAuthURLFn = func(_ context.Context, rawURL string, proxyURL string, _ *execIdentity, _ io.Writer) error {
		openedURL = rawURL
		openedProxy = proxyURL
		return nil
	}

	var out bytes.Buffer
	err := runCodexAppDeviceCodeAuth(context.Background(), codexAppDeviceCodeAuthOptions{
		CodexPath:    "/tmp/codex",
		Cwd:          t.TempDir(),
		CodexHome:    filepath.Join(t.TempDir(), "codex-home"),
		ExtraEnv:     codexAppAuthEnv("/tmp/codex-home", "http://127.0.0.1:23123"),
		ExecIdentity: &execIdentity{UID: 0, Username: "alice", Home: filepath.Join(t.TempDir(), "alice-home")},
		ProxyURL:     "http://127.0.0.1:23123",
		Timeout:      time.Second,
		Out:          &out,
		Err:          io.Discard,
	})
	if err != nil {
		t.Fatalf("runCodexAppDeviceCodeAuth error: %v", err)
	}
	if !starter.configured {
		t.Fatal("app-server ConfigureCommand was not called")
	}
	if envValue(starter.configuredEnv, "HOME") == "" || envValue(starter.configuredEnv, "USER") != "alice" {
		t.Fatalf("app-server configured env missing effective identity: %#v", starter.configuredEnv)
	}
	if starter.request.Command != "/tmp/codex" {
		t.Fatalf("codex command = %q", starter.request.Command)
	}
	if !reflect.DeepEqual(starter.request.Args, []string{"app-server"}) {
		t.Fatalf("app-server args = %#v", starter.request.Args)
	}
	if envValue(starter.request.ExtraEnv, envCodexHome) != "/tmp/codex-home" {
		t.Fatalf("CODEX_HOME not passed to app-server: %#v", starter.request.ExtraEnv)
	}
	if envValue(starter.request.ExtraEnv, "HTTP_PROXY") != "http://127.0.0.1:23123" {
		t.Fatalf("proxy env not passed to app-server: %#v", starter.request.ExtraEnv)
	}
	if openedURL != "https://chatgpt.example/codex/device" || openedProxy != "http://127.0.0.1:23123" {
		t.Fatalf("opened URL/proxy = %q %q", openedURL, openedProxy)
	}

	methods := transport.writeMethods(t)
	wantMethods := []string{"initialize", "initialized", "account/read", "account/login/start", "account/read"}
	if !reflect.DeepEqual(methods, wantMethods) {
		t.Fatalf("app-server methods = %#v, want %#v\nwrites:\n%s", methods, wantMethods, transport.joinedWrites())
	}
	loginStart := transport.decodedWrite(t, 3)
	params := loginStart["params"].(map[string]any)
	if params["type"] != "chatgptDeviceCode" {
		t.Fatalf("login type = %#v", params["type"])
	}
	for _, want := range []string{"CODE-123", "https://chatgpt.example/codex/device", "Codex desktop app auth completed for user@example.com"} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("output missing %q:\n%s", want, out.String())
		}
	}
}

func TestRunCodexAppDeviceCodeAuthSkipsLoginWhenAlreadyChatGPT(t *testing.T) {
	lockCLITestHooks(t)

	transport := newFakeCodexAppAuthTransport(
		`{"id":1,"result":{"userAgent":"codex-test"}}`,
		`{"id":2,"result":{"account":{"type":"chatgpt","email":"user@example.com","planType":"pro"},"requiresOpenaiAuth":true}}`,
	)
	starter := &fakeCodexAppAuthStarter{transport: transport}

	prevStarter := codexAppAuthStarter
	prevOpen := codexAppOpenAuthURLFn
	t.Cleanup(func() {
		codexAppAuthStarter = prevStarter
		codexAppOpenAuthURLFn = prevOpen
	})
	codexAppAuthStarter = starter
	codexAppOpenAuthURLFn = func(context.Context, string, string, *execIdentity, io.Writer) error {
		t.Fatal("already authenticated flow must not open a browser")
		return nil
	}

	var out bytes.Buffer
	err := runCodexAppDeviceCodeAuth(context.Background(), codexAppDeviceCodeAuthOptions{
		CodexPath: "/tmp/codex",
		Cwd:       t.TempDir(),
		CodexHome: filepath.Join(t.TempDir(), "codex-home"),
		Timeout:   time.Second,
		Out:       &out,
		Err:       io.Discard,
	})
	if err != nil {
		t.Fatalf("runCodexAppDeviceCodeAuth error: %v", err)
	}
	methods := transport.writeMethods(t)
	wantMethods := []string{"initialize", "initialized", "account/read"}
	if !reflect.DeepEqual(methods, wantMethods) {
		t.Fatalf("app-server methods = %#v, want %#v", methods, wantMethods)
	}
	if !strings.Contains(out.String(), "already authenticated with ChatGPT as user@example.com") {
		t.Fatalf("output missing already-authenticated message:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "launch it with `cxp app`") {
		t.Fatalf("output missing next-step relaunch message:\n%s", out.String())
	}
}

func TestRunCodexAppAuthDirectUsesAppServerAuthHome(t *testing.T) {
	lockCLITestHooks(t)

	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: boolPtr(false)}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	codexPath := writeFakeCodexVersionCommand(t)
	transport := newFakeCodexAppAuthTransport(
		`{"id":1,"result":{"userAgent":"codex-test"}}`,
		`{"id":2,"result":{"account":null,"requiresOpenaiAuth":true}}`,
		`{"id":3,"result":{"type":"chatgptDeviceCode","loginId":"login-1","verificationUrl":"https://chatgpt.example/codex/device","userCode":"CODE-123"}}`,
		`{"method":"account/login/completed","params":{"loginId":"login-1","success":true}}`,
		`{"id":4,"result":{"account":{"type":"chatgpt","email":"user@example.com","planType":"pro"},"requiresOpenaiAuth":true}}`,
	)
	starter := &fakeCodexAppAuthStarter{transport: transport}

	prevGOOS := codexAppGOOS
	prevIsWSL := codexAppIsWSL
	prevEnsureProxy := ensureProxyPreferenceFunc
	prevStarter := codexAppAuthStarter
	prevOpen := codexAppOpenAuthURLFn
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevIsWSL
		ensureProxyPreferenceFunc = prevEnsureProxy
		codexAppAuthStarter = prevStarter
		codexAppOpenAuthURLFn = prevOpen
	})
	codexAppGOOS = func() string { return "darwin" }
	codexAppIsWSL = func() bool { return false }
	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		return false, config.Config{Version: config.CurrentVersion, ProxyEnabled: boolPtr(false)}, nil
	}
	codexAppAuthStarter = starter
	codexAppOpenAuthURLFn = func(context.Context, string, string, *execIdentity, io.Writer) error { return nil }

	var out bytes.Buffer
	var errOut bytes.Buffer
	cobraCmd := &cobra.Command{}
	cobraCmd.SetContext(context.Background())
	cobraCmd.SetOut(&out)
	cobraCmd.SetErr(&errOut)

	cwd := t.TempDir()
	err = runCodexAppAuth(cobraCmd, &rootOptions{configPath: cfgPath}, codexAppAuthOptions{
		cwd:       cwd,
		codexDir:  "codex-home",
		codexPath: codexPath,
		timeout:   time.Second,
	})
	if err != nil {
		t.Fatalf("runCodexAppAuth error: %v", err)
	}
	wantHome := filepath.Join(cwd, "codex-home")
	if envValue(starter.request.ExtraEnv, envCodexHome) != wantHome {
		t.Fatalf("CODEX_HOME = %q, want %q", envValue(starter.request.ExtraEnv, envCodexHome), wantHome)
	}
}

func TestRunCodexAppAuthExplicitProfileUsesProxyFlow(t *testing.T) {
	lockCLITestHooks(t)

	cfgPath := filepath.Join(t.TempDir(), "config.json")
	store, err := config.NewStore(cfgPath)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	profile := config.Profile{ID: "p1", Name: "dev"}
	enabled := true
	if err := store.Save(config.Config{Version: config.CurrentVersion, ProxyEnabled: &enabled, Profiles: []config.Profile{profile}}); err != nil {
		t.Fatalf("save config: %v", err)
	}

	codexPath := writeFakeCodexVersionCommand(t)
	transport := newFakeCodexAppAuthTransport(
		`{"id":1,"result":{"userAgent":"codex-test"}}`,
		`{"id":2,"result":{"account":null,"requiresOpenaiAuth":true}}`,
		`{"id":3,"result":{"type":"chatgptDeviceCode","loginId":"login-1","verificationUrl":"https://chatgpt.example/codex/device","userCode":"CODE-123"}}`,
		`{"method":"account/login/completed","params":{"loginId":"login-1","success":true}}`,
		`{"id":4,"result":{"account":{"type":"chatgpt","email":"user@example.com","planType":"pro"},"requiresOpenaiAuth":true}}`,
	)
	starter := &fakeCodexAppAuthStarter{transport: transport}

	prevGOOS := codexAppGOOS
	prevIsWSL := codexAppIsWSL
	prevEnsureProxy := ensureProxyPreferenceFunc
	prevEnsureProfile := ensureProfileFunc
	prevEnsureProxyURL := codexAppEnsureProxyURLFn
	prevStarter := codexAppAuthStarter
	prevOpen := codexAppOpenAuthURLFn
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevIsWSL
		ensureProxyPreferenceFunc = prevEnsureProxy
		ensureProfileFunc = prevEnsureProfile
		codexAppEnsureProxyURLFn = prevEnsureProxyURL
		codexAppAuthStarter = prevStarter
		codexAppOpenAuthURLFn = prevOpen
	})
	codexAppGOOS = func() string { return "darwin" }
	codexAppIsWSL = func() bool { return false }
	ensureProxyPreferenceFunc = func(context.Context, *config.Store, string, io.Writer) (bool, config.Config, error) {
		t.Fatal("explicit app auth profile should not ask for proxy preference")
		return false, config.Config{}, nil
	}
	ensureProfileFunc = func(_ context.Context, _ *config.Store, profileRef string, autoInit bool, _ io.Writer) (config.Profile, config.Config, error) {
		if profileRef != "dev" {
			t.Fatalf("profileRef = %q, want dev", profileRef)
		}
		if !autoInit {
			t.Fatal("app auth should auto-init missing proxy profiles")
		}
		return profile, config.Config{
			Version:      config.CurrentVersion,
			ProxyEnabled: &enabled,
			Profiles:     []config.Profile{profile},
			Instances:    []config.Instance{{ID: "inst-1", ProfileID: profile.ID}},
		}, nil
	}
	codexAppEnsureProxyURLFn = func(_ context.Context, _ *config.Store, gotProfile config.Profile, instances []config.Instance, _ io.Writer) (string, error) {
		if gotProfile.ID != profile.ID {
			t.Fatalf("profile = %#v, want %#v", gotProfile, profile)
		}
		if len(instances) != 1 || instances[0].ID != "inst-1" {
			t.Fatalf("instances = %#v", instances)
		}
		return "http://127.0.0.1:23123", nil
	}
	codexAppAuthStarter = starter
	var openedProxy string
	codexAppOpenAuthURLFn = func(_ context.Context, _ string, proxyURL string, _ *execIdentity, _ io.Writer) error {
		openedProxy = proxyURL
		return nil
	}

	cobraCmd := &cobra.Command{}
	cobraCmd.SetContext(context.Background())
	cobraCmd.SetOut(io.Discard)
	cobraCmd.SetErr(io.Discard)

	err = runCodexAppAuth(cobraCmd, &rootOptions{configPath: cfgPath}, codexAppAuthOptions{
		profileRef: "dev",
		cwd:        t.TempDir(),
		codexDir:   "codex-home",
		codexPath:  codexPath,
		timeout:    time.Second,
	})
	if err != nil {
		t.Fatalf("runCodexAppAuth error: %v", err)
	}
	if openedProxy != "http://127.0.0.1:23123" {
		t.Fatalf("opened proxy = %q", openedProxy)
	}
	if envValue(starter.request.ExtraEnv, "HTTP_PROXY") != "http://127.0.0.1:23123" {
		t.Fatalf("proxy env not passed to app-server: %#v", starter.request.ExtraEnv)
	}
}

func TestCodexAppAuthCodexHomeRejectsForeignRootPathWithoutIdentity(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("exec identity is not used on Windows")
	}
	lockCLITestHooks(t)

	prevRunningAsRoot := effectivePathsRunningAsRoot
	prevUserHome := effectivePathsUserHomeDir
	t.Cleanup(func() {
		effectivePathsRunningAsRoot = prevRunningAsRoot
		effectivePathsUserHomeDir = prevUserHome
	})
	effectivePathsRunningAsRoot = func() bool { return true }
	effectivePathsUserHomeDir = func() (string, error) { return "/root", nil }

	_, _, err := codexAppAuthCodexHome(&rootOptions{}, "/home/alice/.codex", "/tmp")
	if err == nil || !strings.Contains(err.Error(), "without a resolvable target uid/gid") {
		t.Fatalf("codexAppAuthCodexHome error = %v, want identity-required error", err)
	}
}

func TestCodexAppAuthEnvIncludesCodexHomeAndProxy(t *testing.T) {
	t.Setenv("NO_PROXY", "corp.local")
	got := codexAppAuthEnv("/tmp/codex-home", "http://127.0.0.1:23123")
	for key, want := range map[string]string{
		envCodexHome:  "tmp",
		"HTTP_PROXY":  "http://127.0.0.1:23123",
		"HTTPS_PROXY": "http://127.0.0.1:23123",
		"ALL_PROXY":   "http://127.0.0.1:23123",
		"WS_PROXY":    "http://127.0.0.1:23123",
		"WSS_PROXY":   "http://127.0.0.1:23123",
	} {
		value := envValue(got, key)
		if key == envCodexHome {
			if value != "/tmp/codex-home" {
				t.Fatalf("%s = %q", key, value)
			}
			continue
		}
		if value != want {
			t.Fatalf("%s = %q, want %q", key, value, want)
		}
	}
	if noProxy := envValue(got, "NO_PROXY"); !strings.Contains(noProxy, "corp.local") || !strings.Contains(noProxy, "127.0.0.1") || !strings.Contains(noProxy, "localhost") {
		t.Fatalf("NO_PROXY = %q, want loopback entries", noProxy)
	}
}

func TestRunCodexAppDeviceCodeAuthTimeoutCoversInitialize(t *testing.T) {
	lockCLITestHooks(t)

	transport := newFakeCodexAppAuthTransport()
	starter := &fakeCodexAppAuthStarter{transport: transport}

	prevStarter := codexAppAuthStarter
	t.Cleanup(func() {
		codexAppAuthStarter = prevStarter
	})
	codexAppAuthStarter = starter

	err := runCodexAppDeviceCodeAuth(context.Background(), codexAppDeviceCodeAuthOptions{
		CodexPath: "/tmp/codex",
		Cwd:       t.TempDir(),
		CodexHome: filepath.Join(t.TempDir(), "codex-home"),
		Timeout:   time.Millisecond,
		Out:       io.Discard,
		Err:       io.Discard,
	})
	if err == nil || !strings.Contains(err.Error(), "timed out waiting for Codex app-server to become ready") {
		t.Fatalf("auth error = %v, want timeout", err)
	}
}

func TestRunCodexAppDeviceCodeAuthRequiresMatchingLoginID(t *testing.T) {
	lockCLITestHooks(t)

	transport := newFakeCodexAppAuthTransport(
		`{"id":1,"result":{"userAgent":"codex-test"}}`,
		`{"id":2,"result":{"account":null,"requiresOpenaiAuth":true}}`,
		`{"id":3,"result":{"type":"chatgptDeviceCode","loginId":"login-1","verificationUrl":"https://chatgpt.example/codex/device","userCode":"CODE-123"}}`,
		`{"method":"account/login/completed","params":{"success":true}}`,
		`{"id":4,"result":{"account":{"type":"chatgpt","email":"user@example.com","planType":"pro"},"requiresOpenaiAuth":true}}`,
	)
	starter := &fakeCodexAppAuthStarter{transport: transport}

	prevStarter := codexAppAuthStarter
	prevOpen := codexAppOpenAuthURLFn
	t.Cleanup(func() {
		codexAppAuthStarter = prevStarter
		codexAppOpenAuthURLFn = prevOpen
	})
	codexAppAuthStarter = starter
	codexAppOpenAuthURLFn = func(context.Context, string, string, *execIdentity, io.Writer) error { return nil }

	err := runCodexAppDeviceCodeAuth(context.Background(), codexAppDeviceCodeAuthOptions{
		CodexPath: "/tmp/codex",
		Cwd:       t.TempDir(),
		CodexHome: filepath.Join(t.TempDir(), "codex-home"),
		Timeout:   time.Millisecond,
		Out:       io.Discard,
		Err:       io.Discard,
	})
	if err == nil || !strings.Contains(err.Error(), "timed out waiting for Codex desktop app auth to complete") {
		t.Fatalf("auth error = %v, want timeout after missing login id", err)
	}
}

func TestRunCodexAppDeviceCodeAuthRetriesUntilChatGPTAccountReadable(t *testing.T) {
	lockCLITestHooks(t)

	transport := newFakeCodexAppAuthTransport(
		`{"id":1,"result":{"userAgent":"codex-test"}}`,
		`{"id":2,"result":{"account":null,"requiresOpenaiAuth":true}}`,
		`{"id":3,"result":{"type":"chatgptDeviceCode","loginId":"login-1","verificationUrl":"https://chatgpt.example/codex/device","userCode":"CODE-123"}}`,
		`{"method":"account/login/completed","params":{"loginId":"login-1","success":true}}`,
		`{"id":4,"result":{"account":null,"requiresOpenaiAuth":true}}`,
		`{"id":5,"result":{"account":{"type":"chatgpt","email":"user@example.com","planType":"pro"},"requiresOpenaiAuth":true}}`,
	)
	starter := &fakeCodexAppAuthStarter{transport: transport}

	prevStarter := codexAppAuthStarter
	prevOpen := codexAppOpenAuthURLFn
	prevPoll := codexAppAuthAccountPollInterval
	t.Cleanup(func() {
		codexAppAuthStarter = prevStarter
		codexAppOpenAuthURLFn = prevOpen
		codexAppAuthAccountPollInterval = prevPoll
	})
	codexAppAuthStarter = starter
	codexAppOpenAuthURLFn = func(context.Context, string, string, *execIdentity, io.Writer) error { return nil }
	codexAppAuthAccountPollInterval = time.Millisecond

	var out bytes.Buffer
	err := runCodexAppDeviceCodeAuth(context.Background(), codexAppDeviceCodeAuthOptions{
		CodexPath: "/tmp/codex",
		Cwd:       t.TempDir(),
		CodexHome: filepath.Join(t.TempDir(), "codex-home"),
		Timeout:   time.Second,
		Out:       &out,
		Err:       io.Discard,
	})
	if err != nil {
		t.Fatalf("runCodexAppDeviceCodeAuth error: %v", err)
	}
	if !strings.Contains(out.String(), "Codex desktop app auth completed for user@example.com") {
		t.Fatalf("output missing success after retry:\n%s", out.String())
	}
	methods := transport.writeMethods(t)
	wantMethods := []string{"initialize", "initialized", "account/read", "account/login/start", "account/read", "account/read"}
	if !reflect.DeepEqual(methods, wantMethods) {
		t.Fatalf("methods = %#v, want %#v", methods, wantMethods)
	}
}

func TestRunCodexAppDeviceCodeAuthStressMatrix(t *testing.T) {
	lockCLITestHooks(t)

	prevStarter := codexAppAuthStarter
	prevOpen := codexAppOpenAuthURLFn
	prevPoll := codexAppAuthAccountPollInterval
	t.Cleanup(func() {
		codexAppAuthStarter = prevStarter
		codexAppOpenAuthURLFn = prevOpen
		codexAppAuthAccountPollInterval = prevPoll
	})
	codexAppAuthAccountPollInterval = time.Millisecond

	type stressCase struct {
		proxyURL          string
		noOpenBrowser     bool
		initialReadError  bool
		notifyBeforeStart bool
		delayedAccount    bool
		serverRequest     bool
	}
	var cases []stressCase
	for _, proxyURL := range []string{"", "http://127.0.0.1:23123"} {
		for _, noOpenBrowser := range []bool{false, true} {
			for _, initialReadError := range []bool{false, true} {
				for _, notifyBeforeStart := range []bool{false, true} {
					for _, delayedAccount := range []bool{false, true} {
						for _, serverRequest := range []bool{false, true} {
							cases = append(cases, stressCase{
								proxyURL:          proxyURL,
								noOpenBrowser:     noOpenBrowser,
								initialReadError:  initialReadError,
								notifyBeforeStart: notifyBeforeStart,
								delayedAccount:    delayedAccount,
								serverRequest:     serverRequest,
							})
						}
					}
				}
			}
		}
	}

	for i, tc := range cases {
		name := strings.Join([]string{
			"case", strconv.Itoa(i),
			boolName("proxy", tc.proxyURL != ""),
			boolName("no_open", tc.noOpenBrowser),
			boolName("initial_error", tc.initialReadError),
			boolName("early_notify", tc.notifyBeforeStart),
			boolName("delayed_account", tc.delayedAccount),
			boolName("server_request", tc.serverRequest),
		}, "_")
		t.Run(name, func(t *testing.T) {
			lines := []string{`{"id":1,"result":{"userAgent":"codex-test"}}`}
			if tc.initialReadError {
				lines = append(lines, `{"id":2,"error":{"code":-32603,"message":"account state unavailable"}}`)
			} else {
				lines = append(lines, `{"id":2,"result":{"account":null,"requiresOpenaiAuth":true}}`)
			}
			if tc.serverRequest && tc.notifyBeforeStart {
				lines = append(lines, `{"id":"server-start","method":"session/configure","params":{}}`)
			}
			if tc.notifyBeforeStart {
				lines = append(lines, `{"method":"account/login/completed","params":{"loginId":"login-1","success":true}}`)
			}
			lines = append(lines, `{"id":3,"result":{"type":"chatgptDeviceCode","loginId":"login-1","verificationUrl":"https://chatgpt.example/codex/device","userCode":"CODE-123"}}`)
			if tc.serverRequest && !tc.notifyBeforeStart {
				lines = append(lines, `{"id":"server-wait","method":"session/configure","params":{}}`)
			}
			if !tc.notifyBeforeStart {
				lines = append(lines, `{"method":"account/login/completed","params":{"loginId":"login-1","success":true}}`)
			}
			if tc.delayedAccount {
				lines = append(lines, `{"id":4,"result":{"account":null,"requiresOpenaiAuth":true}}`)
				lines = append(lines, `{"id":5,"result":{"account":{"type":"chatgpt","email":"user@example.com","planType":"pro"},"requiresOpenaiAuth":true}}`)
			} else {
				lines = append(lines, `{"id":4,"result":{"account":{"type":"chatgpt","email":"user@example.com","planType":"pro"},"requiresOpenaiAuth":true}}`)
			}

			transport := newFakeCodexAppAuthTransport(lines...)
			codexAppAuthStarter = &fakeCodexAppAuthStarter{transport: transport}
			var openedURL string
			var openedProxy string
			codexAppOpenAuthURLFn = func(_ context.Context, rawURL string, proxyURL string, _ *execIdentity, _ io.Writer) error {
				openedURL = rawURL
				openedProxy = proxyURL
				return nil
			}

			var out bytes.Buffer
			var errOut bytes.Buffer
			err := runCodexAppDeviceCodeAuth(context.Background(), codexAppDeviceCodeAuthOptions{
				CodexPath:     "/tmp/codex",
				Cwd:           t.TempDir(),
				CodexHome:     filepath.Join(t.TempDir(), "codex-home"),
				ExtraEnv:      codexAppAuthEnv("/tmp/codex-home", tc.proxyURL),
				ProxyURL:      tc.proxyURL,
				NoOpenBrowser: tc.noOpenBrowser,
				Timeout:       time.Second,
				Out:           &out,
				Err:           &errOut,
			})
			if err != nil {
				t.Fatalf("runCodexAppDeviceCodeAuth error: %v\nwrites:\n%s\nstderr:\n%s", err, transport.joinedWrites(), errOut.String())
			}
			if !strings.Contains(out.String(), "Codex desktop app auth completed for user@example.com") {
				t.Fatalf("output missing success:\n%s", out.String())
			}
			if tc.noOpenBrowser {
				if openedURL != "" {
					t.Fatalf("opened URL = %q, want no automatic browser", openedURL)
				}
			} else if openedURL != "https://chatgpt.example/codex/device" || openedProxy != tc.proxyURL {
				t.Fatalf("opened URL/proxy = %q/%q, want device URL/%q", openedURL, openedProxy, tc.proxyURL)
			}
			if tc.initialReadError && !strings.Contains(errOut.String(), "could not read existing Codex account state") {
				t.Fatalf("missing recoverable account/read warning:\n%s", errOut.String())
			}
			wantUnsupported := 0
			if tc.serverRequest {
				wantUnsupported = 1
			}
			if got := countUnsupportedServerResponses(t, transport); got != wantUnsupported {
				t.Fatalf("unsupported responses = %d, want %d\nwrites:\n%s", got, wantUnsupported, transport.joinedWrites())
			}
		})
	}
}

func TestRunCodexAppDeviceCodeAuthWarnsAndContinuesAfterInitialAccountReadError(t *testing.T) {
	lockCLITestHooks(t)

	transport := newFakeCodexAppAuthTransport(
		`{"id":1,"result":{"userAgent":"codex-test"}}`,
		`{"id":2,"error":{"code":-32603,"message":"account state unavailable"}}`,
		`{"id":3,"result":{"type":"chatgptDeviceCode","loginId":"login-1","verificationUrl":"https://chatgpt.example/codex/device","userCode":"CODE-123"}}`,
		`{"method":"account/login/completed","params":{"loginId":"login-1","success":true}}`,
		`{"id":4,"result":{"account":{"type":"chatgpt","email":"user@example.com","planType":"pro"},"requiresOpenaiAuth":true}}`,
	)
	starter := &fakeCodexAppAuthStarter{transport: transport}

	prevStarter := codexAppAuthStarter
	prevOpen := codexAppOpenAuthURLFn
	t.Cleanup(func() {
		codexAppAuthStarter = prevStarter
		codexAppOpenAuthURLFn = prevOpen
	})
	codexAppAuthStarter = starter
	codexAppOpenAuthURLFn = func(context.Context, string, string, *execIdentity, io.Writer) error {
		return nil
	}

	var errOut bytes.Buffer
	err := runCodexAppDeviceCodeAuth(context.Background(), codexAppDeviceCodeAuthOptions{
		CodexPath: "/tmp/codex",
		Cwd:       t.TempDir(),
		CodexHome: filepath.Join(t.TempDir(), "codex-home"),
		Timeout:   time.Second,
		Out:       io.Discard,
		Err:       &errOut,
	})
	if err != nil {
		t.Fatalf("runCodexAppDeviceCodeAuth error: %v", err)
	}
	if !strings.Contains(errOut.String(), "could not read existing Codex account state") || !strings.Contains(errOut.String(), "account state unavailable") {
		t.Fatalf("stderr missing account/read warning:\n%s", errOut.String())
	}
	methods := transport.writeMethods(t)
	wantMethods := []string{"initialize", "initialized", "account/read", "account/login/start", "account/read"}
	if !reflect.DeepEqual(methods, wantMethods) {
		t.Fatalf("methods = %#v, want %#v", methods, wantMethods)
	}
}

func TestRunCodexAppDeviceCodeAuthDoesNotRecoverCanceledInitialAccountRead(t *testing.T) {
	lockCLITestHooks(t)

	ctx, cancel := context.WithCancel(context.Background())
	transport := newFakeCodexAppAuthTransport(
		`{"id":1,"result":{"userAgent":"codex-test"}}`,
	)
	transport.onReadBlock = cancel
	starter := &fakeCodexAppAuthStarter{transport: transport}

	prevStarter := codexAppAuthStarter
	prevOpen := codexAppOpenAuthURLFn
	t.Cleanup(func() {
		codexAppAuthStarter = prevStarter
		codexAppOpenAuthURLFn = prevOpen
	})
	codexAppAuthStarter = starter
	codexAppOpenAuthURLFn = func(context.Context, string, string, *execIdentity, io.Writer) error {
		t.Fatal("canceled account/read must not continue to browser auth")
		return nil
	}

	err := runCodexAppDeviceCodeAuth(ctx, codexAppDeviceCodeAuthOptions{
		CodexPath: "/tmp/codex",
		Cwd:       t.TempDir(),
		CodexHome: filepath.Join(t.TempDir(), "codex-home"),
		Timeout:   time.Second,
		Out:       io.Discard,
		Err:       io.Discard,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runCodexAppDeviceCodeAuth error = %v, want context.Canceled", err)
	}
	methods := transport.writeMethods(t)
	wantMethods := []string{"initialize", "initialized", "account/read"}
	if !reflect.DeepEqual(methods, wantMethods) {
		t.Fatalf("methods = %#v, want %#v", methods, wantMethods)
	}
}

func TestRunCodexAppDeviceCodeAuthBuffersLoginCompletedBeforeStartResponse(t *testing.T) {
	lockCLITestHooks(t)

	transport := newFakeCodexAppAuthTransport(
		`{"id":1,"result":{"userAgent":"codex-test"}}`,
		`{"id":2,"result":{"account":null,"requiresOpenaiAuth":true}}`,
		`{"method":"account/login/completed","params":{"loginId":"login-1","success":true}}`,
		`{"id":3,"result":{"type":"chatgptDeviceCode","loginId":"login-1","verificationUrl":"https://chatgpt.example/codex/device","userCode":"CODE-123"}}`,
		`{"id":4,"result":{"account":{"type":"chatgpt","email":"user@example.com","planType":"pro"},"requiresOpenaiAuth":true}}`,
	)
	starter := &fakeCodexAppAuthStarter{transport: transport}

	prevStarter := codexAppAuthStarter
	prevOpen := codexAppOpenAuthURLFn
	t.Cleanup(func() {
		codexAppAuthStarter = prevStarter
		codexAppOpenAuthURLFn = prevOpen
	})
	codexAppAuthStarter = starter
	codexAppOpenAuthURLFn = func(context.Context, string, string, *execIdentity, io.Writer) error { return nil }

	err := runCodexAppDeviceCodeAuth(context.Background(), codexAppDeviceCodeAuthOptions{
		CodexPath: "/tmp/codex",
		Cwd:       t.TempDir(),
		CodexHome: filepath.Join(t.TempDir(), "codex-home"),
		Timeout:   time.Second,
		Out:       io.Discard,
		Err:       io.Discard,
	})
	if err != nil {
		t.Fatalf("runCodexAppDeviceCodeAuth error: %v", err)
	}
	methods := transport.writeMethods(t)
	wantMethods := []string{"initialize", "initialized", "account/read", "account/login/start", "account/read"}
	if !reflect.DeepEqual(methods, wantMethods) {
		t.Fatalf("methods = %#v, want %#v", methods, wantMethods)
	}
}

func TestRunCodexAppDeviceCodeAuthBufferedLoginFailureReturnsError(t *testing.T) {
	lockCLITestHooks(t)

	transport := newFakeCodexAppAuthTransport(
		`{"id":1,"result":{"userAgent":"codex-test"}}`,
		`{"id":2,"result":{"account":null,"requiresOpenaiAuth":true}}`,
		`{"method":"account/login/completed","params":{"loginId":"login-1","success":false,"error":"access denied"}}`,
		`{"id":3,"result":{"type":"chatgptDeviceCode","loginId":"login-1","verificationUrl":"https://chatgpt.example/codex/device","userCode":"CODE-123"}}`,
	)
	starter := &fakeCodexAppAuthStarter{transport: transport}

	prevStarter := codexAppAuthStarter
	prevOpen := codexAppOpenAuthURLFn
	t.Cleanup(func() {
		codexAppAuthStarter = prevStarter
		codexAppOpenAuthURLFn = prevOpen
	})
	codexAppAuthStarter = starter
	codexAppOpenAuthURLFn = func(context.Context, string, string, *execIdentity, io.Writer) error { return nil }

	err := runCodexAppDeviceCodeAuth(context.Background(), codexAppDeviceCodeAuthOptions{
		CodexPath: "/tmp/codex",
		Cwd:       t.TempDir(),
		CodexHome: filepath.Join(t.TempDir(), "codex-home"),
		Timeout:   time.Second,
		Out:       io.Discard,
		Err:       io.Discard,
	})
	if err == nil || !strings.Contains(err.Error(), "Codex desktop app auth failed: access denied") {
		t.Fatalf("auth error = %v, want buffered login failure", err)
	}
	methods := transport.writeMethods(t)
	wantMethods := []string{"initialize", "initialized", "account/read", "account/login/start"}
	if !reflect.DeepEqual(methods, wantMethods) {
		t.Fatalf("methods = %#v, want %#v", methods, wantMethods)
	}
}

func TestRunCodexAppDeviceCodeAuthRespondsToInterleavedServerRequests(t *testing.T) {
	lockCLITestHooks(t)

	transport := newFakeCodexAppAuthTransport(
		`{"id":1,"result":{"userAgent":"codex-test"}}`,
		`{"id":"server-read","method":"session/configure","params":{}}`,
		`{"id":2,"result":{"account":null,"requiresOpenaiAuth":true}}`,
		`{"id":3,"result":{"type":"chatgptDeviceCode","loginId":"login-1","verificationUrl":"https://chatgpt.example/codex/device","userCode":"CODE-123"}}`,
		`{"id":"server-wait","method":"session/configure","params":{}}`,
		`{"method":"account/login/completed","params":{"loginId":"login-1","success":true}}`,
		`{"id":4,"result":{"account":{"type":"chatgpt","email":"user@example.com","planType":"pro"},"requiresOpenaiAuth":true}}`,
	)
	starter := &fakeCodexAppAuthStarter{transport: transport}

	prevStarter := codexAppAuthStarter
	prevOpen := codexAppOpenAuthURLFn
	t.Cleanup(func() {
		codexAppAuthStarter = prevStarter
		codexAppOpenAuthURLFn = prevOpen
	})
	codexAppAuthStarter = starter
	codexAppOpenAuthURLFn = func(context.Context, string, string, *execIdentity, io.Writer) error { return nil }

	err := runCodexAppDeviceCodeAuth(context.Background(), codexAppDeviceCodeAuthOptions{
		CodexPath: "/tmp/codex",
		Cwd:       t.TempDir(),
		CodexHome: filepath.Join(t.TempDir(), "codex-home"),
		Timeout:   time.Second,
		Out:       io.Discard,
		Err:       io.Discard,
	})
	if err != nil {
		t.Fatalf("runCodexAppDeviceCodeAuth error: %v", err)
	}
	assertUnsupportedServerResponse(t, transport.decodedWrite(t, 3), "server-read")
	assertUnsupportedServerResponse(t, transport.decodedWrite(t, 5), "server-wait")
	methods := transport.writeMethods(t)
	wantMethods := []string{"initialize", "initialized", "account/read", "", "account/login/start", "", "account/read"}
	if !reflect.DeepEqual(methods, wantMethods) {
		t.Fatalf("methods = %#v, want %#v\nwrites:\n%s", methods, wantMethods, transport.joinedWrites())
	}
}

func TestCodexAppAuthWaitForChatGPTAccountMapsReadDeadline(t *testing.T) {
	client := &codexAppAuthClient{transport: newFakeCodexAppAuthTransport()}
	_, err := client.waitForChatGPTAccount(context.Background(), time.Millisecond, time.Millisecond)
	if err == nil || !strings.Contains(err.Error(), "timed out waiting for Codex desktop app auth to become readable") {
		t.Fatalf("waitForChatGPTAccount error = %v, want readable timeout", err)
	}
}

func TestCodexAppAuthWaitForChatGPTAccountPreservesCancellationBetweenPolls(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	transport := newFakeCodexAppAuthTransport(
		`{"id":1,"result":{"account":null,"requiresOpenaiAuth":true}}`,
	)
	transport.afterRead = func([]byte) {
		cancel()
	}
	client := &codexAppAuthClient{transport: transport}

	_, err := client.waitForChatGPTAccount(ctx, time.Second, time.Hour)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForChatGPTAccount error = %v, want context.Canceled", err)
	}
}

func TestEnsureCodexAppAuthCodexInstalledProbesEffectiveIdentity(t *testing.T) {
	lockCLITestHooks(t)

	codexPath := writeFakeCodexVersionCommand(t)
	identity := &execIdentity{UID: 1000, Username: "alice", Home: filepath.Join(t.TempDir(), "alice-home"), GroupsKnown: true}

	prevProbe := codexAppAuthProbeCodexForIdentityFn
	t.Cleanup(func() {
		codexAppAuthProbeCodexForIdentityFn = prevProbe
	})
	var gotPath string
	var gotIdentity *execIdentity
	codexAppAuthProbeCodexForIdentityFn = func(_ context.Context, path string, identity *execIdentity) error {
		gotPath = path
		gotIdentity = identity
		return errors.New("target user cannot run codex")
	}

	_, err := ensureCodexAppAuthCodexInstalled(context.Background(), codexPath, io.Discard, codexInstallOptions{}, identity)
	if err == nil || !strings.Contains(err.Error(), "target user cannot run codex") {
		t.Fatalf("ensureCodexAppAuthCodexInstalled error = %v, want identity probe error", err)
	}
	if gotPath != codexPath {
		t.Fatalf("probed path = %q, want %q", gotPath, codexPath)
	}
	if gotIdentity != identity {
		t.Fatalf("probed identity = %#v, want %#v", gotIdentity, identity)
	}
}

func TestEnsureCodexAppAuthCodexInstalledExplicitPathSkipsCurrentUserProbeForIdentity(t *testing.T) {
	lockCLITestHooks(t)

	codexPath := writeFailingCodexVersionCommand(t)
	identity := &execIdentity{UID: 1000, Username: "alice", Home: filepath.Join(t.TempDir(), "alice-home"), GroupsKnown: true}

	prevProbe := codexAppAuthProbeCodexForIdentityFn
	t.Cleanup(func() {
		codexAppAuthProbeCodexForIdentityFn = prevProbe
	})
	var gotPath string
	codexAppAuthProbeCodexForIdentityFn = func(_ context.Context, path string, gotIdentity *execIdentity) error {
		gotPath = path
		if gotIdentity != identity {
			t.Fatalf("probe identity = %#v, want %#v", gotIdentity, identity)
		}
		return nil
	}

	got, err := ensureCodexAppAuthCodexInstalled(context.Background(), codexPath, io.Discard, codexInstallOptions{}, identity)
	if err != nil {
		t.Fatalf("ensureCodexAppAuthCodexInstalled error: %v", err)
	}
	if got != codexPath || gotPath != codexPath {
		t.Fatalf("codex path/probe = %q/%q, want %q", got, gotPath, codexPath)
	}
}

func TestEnsureCodexAppAuthCodexInstalledPrefersTargetUserCandidate(t *testing.T) {
	lockCLITestHooks(t)

	home := t.TempDir()
	identity := &execIdentity{UID: 1000, Username: "alice", Home: home, GroupsKnown: true}
	var codexPath string
	if runtime.GOOS == "windows" {
		codexPath = filepath.Join(home, "AppData", "Local", "codex-proxy", "npm-global", "codex.cmd")
	} else {
		codexPath = filepath.Join(home, ".local", "share", "codex-proxy", "npm-global", "bin", "codex")
	}
	writeFakeCodexVersionCommandAt(t, codexPath)

	prevProbe := codexAppAuthProbeCodexForIdentityFn
	t.Cleanup(func() {
		codexAppAuthProbeCodexForIdentityFn = prevProbe
	})
	var probed []string
	codexAppAuthProbeCodexForIdentityFn = func(_ context.Context, path string, gotIdentity *execIdentity) error {
		if gotIdentity != identity {
			t.Fatalf("probe identity = %#v, want %#v", gotIdentity, identity)
		}
		probed = append(probed, path)
		if path == codexPath {
			return nil
		}
		return errors.New("not target candidate")
	}

	got, err := ensureCodexAppAuthCodexInstalled(context.Background(), "", io.Discard, codexInstallOptions{}, identity)
	if err != nil {
		t.Fatalf("ensureCodexAppAuthCodexInstalled error: %v", err)
	}
	if got != codexPath {
		t.Fatalf("codex path = %q, want target user candidate %q; probed %#v", got, codexPath, probed)
	}
}

func TestEnsureCodexAppAuthCodexInstalledForIdentityDoesNotPolluteCurrentCache(t *testing.T) {
	lockCLITestHooks(t)

	cacheRoot := t.TempDir()
	t.Setenv("XDG_CACHE_HOME", cacheRoot)
	t.Setenv("LOCALAPPDATA", cacheRoot)
	currentCached := filepath.Join(t.TempDir(), "current-codex")
	writeCachedCodexPath(currentCached)

	home := t.TempDir()
	identity := &execIdentity{UID: 1000, Username: "alice", Home: home, GroupsKnown: true}
	codexPath := filepath.Join(home, ".local", "share", "codex-proxy", "npm-global", "bin", "codex")
	if runtime.GOOS == "windows" {
		codexPath = filepath.Join(home, "AppData", "Local", "codex-proxy", "npm-global", "codex.cmd")
	}
	writeFakeCodexVersionCommandAt(t, codexPath)

	prevProbe := codexAppAuthProbeCodexForIdentityFn
	t.Cleanup(func() {
		codexAppAuthProbeCodexForIdentityFn = prevProbe
	})
	codexAppAuthProbeCodexForIdentityFn = func(_ context.Context, path string, _ *execIdentity) error {
		if path == codexPath {
			return nil
		}
		return errors.New("not target candidate")
	}

	got, err := ensureCodexAppAuthCodexInstalled(context.Background(), "", io.Discard, codexInstallOptions{}, identity)
	if err != nil {
		t.Fatalf("ensureCodexAppAuthCodexInstalled error: %v", err)
	}
	if got != codexPath {
		t.Fatalf("codex path = %q, want %q", got, codexPath)
	}
	if cached := readCachedCodexPath(); cached != currentCached {
		t.Fatalf("current cache = %q, want unchanged %q", cached, currentCached)
	}
}

func TestCodexAppAuthIdentityRuntimeEnvPrependsTargetManagedNode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("managed node layout differs on Windows")
	}
	home := t.TempDir()
	nodeDir := filepath.Join(home, ".cache", "codex-proxy", "node", "v22-linux-"+nodeRuntimeArch(runtime.GOARCH), "bin")
	if err := os.MkdirAll(nodeDir, 0o755); err != nil {
		t.Fatalf("create managed node dir: %v", err)
	}
	nodePath := filepath.Join(nodeDir, "node")
	if err := os.WriteFile(nodePath, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatalf("write managed node: %v", err)
	}

	envVars := codexAppAuthRuntimeEnvForIdentity([]string{
		"PATH=/usr/bin",
		"HTTP_PROXY=http://127.0.0.1:23123",
		"HTTPS_PROXY=http://127.0.0.1:23123",
	}, &execIdentity{UID: 1000, Username: "alice", Home: home, GroupsKnown: true})
	pathParts := filepath.SplitList(envValue(envVars, "PATH"))
	if len(pathParts) == 0 || pathParts[0] != nodeDir {
		t.Fatalf("PATH = %q, want target managed node first", envValue(envVars, "PATH"))
	}
	for key, want := range map[string]string{
		"HOME":                    home,
		"USER":                    "alice",
		"HTTP_PROXY":              "http://127.0.0.1:23123",
		"HTTPS_PROXY":             "http://127.0.0.1:23123",
		"CODEX_NPM_PREFIX":        filepath.Join(home, ".local", "share", "codex-proxy", "npm-global"),
		"CODEX_NODE_INSTALL_ROOT": filepath.Join(home, ".cache", "codex-proxy", "node"),
	} {
		if got := envValue(envVars, key); got != want {
			t.Fatalf("%s = %q, want %q", key, got, want)
		}
	}

	envVars = codexAppAuthRuntimeEnvForIdentity([]string{
		"PATH=" + nodeDir + string(os.PathListSeparator) + "/usr/bin",
	}, &execIdentity{UID: 1000, Username: "alice", Home: home, GroupsKnown: true})
	seenNodeDir := 0
	for _, part := range filepath.SplitList(envValue(envVars, "PATH")) {
		if part == nodeDir {
			seenNodeDir++
		}
	}
	if seenNodeDir != 1 {
		t.Fatalf("target managed node appears %d times in PATH %q, want once", seenNodeDir, envValue(envVars, "PATH"))
	}
}

func TestCodexAppAuthMacProxyBrowserUsesPerRunProfileAndProxyArgs(t *testing.T) {
	lockCLITestHooks(t)

	prevGOOS := codexAppGOOS
	prevCommandContext := codexAppCommandContext
	prevRunID := codexAppAuthBrowserRunIDFn
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppCommandContext = prevCommandContext
		codexAppAuthBrowserRunIDFn = prevRunID
	})
	codexAppGOOS = func() string { return "darwin" }
	codexAppAuthBrowserRunIDFn = func(string) string { return "run one" }

	var capturedName string
	var capturedArgs []string
	codexAppCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		capturedName = name
		capturedArgs = append([]string{}, args...)
		return appAuthTestCommandContext(ctx, 0)
	}

	home := t.TempDir()
	err := openCodexAppAuthURLWithMacProxyBrowser(context.Background(), "https://chatgpt.example/codex/device", "http://127.0.0.1:23123", &execIdentity{Home: home})
	if err != nil {
		t.Fatalf("open mac proxy browser: %v", err)
	}
	if capturedName != "open" {
		t.Fatalf("command = %q, want open", capturedName)
	}
	joined := strings.Join(capturedArgs, "\n")
	for _, want := range []string{
		"-na\nGoogle Chrome",
		"--args",
		"--proxy-server=http://127.0.0.1:23123",
		"--new-window",
		"--no-first-run",
		"--disable-extensions",
		"https://chatgpt.example/codex/device",
		filepath.Join(home, "Library", "Caches", "codex-helper", "app-auth-browser", "google-chrome"),
		"run-one-",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("mac open args missing %q:\n%s", want, joined)
		}
	}
}

func TestCodexAppAuthMacProxyBrowserFallsBackAcrossCandidates(t *testing.T) {
	lockCLITestHooks(t)

	prevGOOS := codexAppGOOS
	prevCommandContext := codexAppCommandContext
	prevRunID := codexAppAuthBrowserRunIDFn
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppCommandContext = prevCommandContext
		codexAppAuthBrowserRunIDFn = prevRunID
	})
	codexAppGOOS = func() string { return "darwin" }
	codexAppAuthBrowserRunIDFn = func(string) string { return "run one" }

	var appNames []string
	codexAppCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		for i, arg := range args {
			if arg == "-na" && i+1 < len(args) {
				appNames = append(appNames, args[i+1])
				break
			}
		}
		if len(appNames) == 1 {
			return appAuthTestCommandContext(ctx, 1)
		}
		return appAuthTestCommandContext(ctx, 0)
	}

	err := openCodexAppAuthURLWithMacProxyBrowser(context.Background(), "https://chatgpt.example/codex/device", "http://127.0.0.1:23123", &execIdentity{Home: t.TempDir()})
	if err != nil {
		t.Fatalf("open mac proxy browser: %v", err)
	}
	wantApps := []string{"Google Chrome", "Microsoft Edge"}
	if !reflect.DeepEqual(appNames, wantApps) {
		t.Fatalf("app attempts = %#v, want %#v", appNames, wantApps)
	}
}

func TestCodexAppAuthMacProxyBrowserReportsAllCandidateFailures(t *testing.T) {
	lockCLITestHooks(t)

	prevGOOS := codexAppGOOS
	prevCommandContext := codexAppCommandContext
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppCommandContext = prevCommandContext
	})
	codexAppGOOS = func() string { return "darwin" }
	codexAppCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
		return appAuthTestCommandContext(ctx, 1)
	}

	err := openCodexAppAuthURLWithMacProxyBrowser(context.Background(), "https://chatgpt.example/codex/device", "http://127.0.0.1:23123", &execIdentity{Home: t.TempDir()})
	if err == nil {
		t.Fatal("open mac proxy browser succeeded, want failure after all candidates")
	}
}

func TestCodexAppAuthMacProxyBrowserRejectsEffectiveIdentityAutoOpen(t *testing.T) {
	lockCLITestHooks(t)

	prevCommandContext := codexAppCommandContext
	t.Cleanup(func() {
		codexAppCommandContext = prevCommandContext
	})
	codexAppCommandContext = func(context.Context, string, ...string) *exec.Cmd {
		t.Fatal("mac proxy browser should not auto-open for non-root effective identity")
		return exec.Command("true")
	}

	err := openCodexAppAuthURLWithMacProxyBrowser(context.Background(), "https://chatgpt.example/codex/device", "http://127.0.0.1:23123", &execIdentity{UID: 1000, Home: t.TempDir()})
	if err == nil || !strings.Contains(err.Error(), "effective target identity") {
		t.Fatalf("open error = %v, want effective identity guidance", err)
	}
}

func TestOpenCodexAppAuthURLWithProxyWSLRequiresWindowsProxyReachability(t *testing.T) {
	lockCLITestHooks(t)

	prevGOOS := codexAppGOOS
	prevIsWSL := codexAppIsWSL
	prevReachable := codexAppAuthWindowsProxyReachableFn
	prevRunCommand := codexAppRunCommand
	prevCommandOutput := codexAppCommandOutput
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevIsWSL
		codexAppAuthWindowsProxyReachableFn = prevReachable
		codexAppRunCommand = prevRunCommand
		codexAppCommandOutput = prevCommandOutput
	})
	codexAppGOOS = func() string { return "linux" }
	codexAppIsWSL = func() bool { return true }
	codexAppAuthWindowsProxyReachableFn = func(context.Context, string) error {
		return errors.New("connection refused")
	}
	codexAppRunCommand = func(context.Context, io.Writer, string, ...string) error {
		t.Fatal("Windows browser should not be opened when proxy is unreachable from Windows")
		return nil
	}
	codexAppCommandOutput = func(context.Context, string, ...string) ([]byte, error) {
		t.Fatal("Windows browser should not be opened when proxy is unreachable from Windows")
		return nil, nil
	}

	err := openCodexAppAuthURLWithProxy(context.Background(), "https://chatgpt.example/codex/device", "http://127.0.0.1:23123", nil)
	if err == nil || !strings.Contains(err.Error(), "Windows browser cannot reach the selected WSL proxy") {
		t.Fatalf("open error = %v, want WSL proxy reachability error", err)
	}
}

func TestOpenCodexAppAuthURLWithProxyWSLReachableUsesWindowsBrowser(t *testing.T) {
	lockCLITestHooks(t)

	prevGOOS := codexAppGOOS
	prevIsWSL := codexAppIsWSL
	prevReachable := codexAppAuthWindowsProxyReachableFn
	prevLookPath := codexAppLookPath
	prevCommandOutput := codexAppCommandOutput
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevIsWSL
		codexAppAuthWindowsProxyReachableFn = prevReachable
		codexAppLookPath = prevLookPath
		codexAppCommandOutput = prevCommandOutput
	})
	codexAppGOOS = func() string { return "linux" }
	codexAppIsWSL = func() bool { return true }
	var checkedProxy string
	codexAppAuthWindowsProxyReachableFn = func(_ context.Context, proxyURL string) error {
		checkedProxy = proxyURL
		return nil
	}
	codexAppLookPath = func(name string) (string, error) {
		return name, nil
	}
	var commandName string
	var commandArgs []string
	codexAppCommandOutput = func(_ context.Context, name string, args ...string) ([]byte, error) {
		commandName = name
		commandArgs = append([]string{}, args...)
		return []byte(""), nil
	}

	err := openCodexAppAuthURLWithProxy(context.Background(), "https://chatgpt.example/codex/device", "http://127.0.0.1:23123", nil)
	if err != nil {
		t.Fatalf("open WSL proxy browser: %v", err)
	}
	if checkedProxy != "http://127.0.0.1:23123" {
		t.Fatalf("checked proxy = %q", checkedProxy)
	}
	if commandName == "" || !strings.Contains(strings.Join(commandArgs, "\n"), "--proxy-server=") {
		t.Fatalf("Windows browser command not invoked with proxy args: %q %#v", commandName, commandArgs)
	}
}

func TestOpenCodexAppAuthURLWithProxyPlatformStressMatrix(t *testing.T) {
	lockCLITestHooks(t)

	prevGOOS := codexAppGOOS
	prevIsWSL := codexAppIsWSL
	prevReachable := codexAppAuthWindowsProxyReachableFn
	prevLookPath := codexAppLookPath
	prevCommandOutput := codexAppCommandOutput
	prevCommandContext := codexAppCommandContext
	t.Cleanup(func() {
		codexAppGOOS = prevGOOS
		codexAppIsWSL = prevIsWSL
		codexAppAuthWindowsProxyReachableFn = prevReachable
		codexAppLookPath = prevLookPath
		codexAppCommandOutput = prevCommandOutput
		codexAppCommandContext = prevCommandContext
	})

	cases := []struct {
		name        string
		goos        string
		wsl         bool
		identity    *execIdentity
		reachErr    error
		wantErr     string
		wantMac     bool
		wantWindows bool
	}{
		{name: "mac ok", goos: "darwin", wantMac: true},
		{name: "mac target identity blocked", goos: "darwin", identity: &execIdentity{UID: 1000, Home: t.TempDir()}, wantErr: "effective target identity"},
		{name: "windows ok", goos: "windows", wantWindows: true},
		{name: "linux unsupported", goos: "linux", wantErr: "not supported"},
		{name: "wsl unreachable", goos: "linux", wsl: true, reachErr: errors.New("connection refused"), wantErr: "Windows browser cannot reach"},
		{name: "wsl reachable", goos: "linux", wsl: true, wantWindows: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			codexAppGOOS = func() string { return tc.goos }
			codexAppIsWSL = func() bool { return tc.wsl }
			codexAppAuthWindowsProxyReachableFn = func(context.Context, string) error { return tc.reachErr }
			codexAppLookPath = func(name string) (string, error) { return name, nil }
			var macRuns int
			var windowsRuns int
			codexAppCommandContext = func(ctx context.Context, name string, args ...string) *exec.Cmd {
				macRuns++
				return appAuthTestCommandContext(ctx, 0)
			}
			codexAppCommandOutput = func(context.Context, string, ...string) ([]byte, error) {
				windowsRuns++
				return []byte(""), nil
			}

			err := openCodexAppAuthURLWithProxy(context.Background(), "https://chatgpt.example/codex/device", "http://127.0.0.1:23123", tc.identity)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("open error = %v, want %q", err, tc.wantErr)
				}
			} else if err != nil {
				t.Fatalf("open error: %v", err)
			}
			if (macRuns > 0) != tc.wantMac {
				t.Fatalf("mac runs = %d, wantMac %v", macRuns, tc.wantMac)
			}
			if (windowsRuns > 0) != tc.wantWindows {
				t.Fatalf("windows runs = %d, wantWindows %v", windowsRuns, tc.wantWindows)
			}
		})
	}
}

func TestPrintCodexAppAuthPromptNoOpenBrowserDoesNotPromiseAutoOpen(t *testing.T) {
	var out bytes.Buffer
	printCodexAppAuthPrompt(&out, codexAppAuthStartResult{
		VerificationURL: "https://chatgpt.example/codex/device",
		UserCode:        "CODE-123",
	}, "http://127.0.0.1:23123", true)
	if strings.Contains(out.String(), "A browser window will be opened") {
		t.Fatalf("no-open prompt promised auto-open:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "Open the URL manually in a browser configured to use that proxy") {
		t.Fatalf("no-open prompt missing manual proxy guidance:\n%s", out.String())
	}
}

func TestCodexAppAuthWindowsProxyReachabilityScriptChecksHealthEndpoint(t *testing.T) {
	script := codexAppAuthWindowsProxyReachabilityScript("127.0.0.1", "23123")
	for _, want := range []string{
		"http://127.0.0.1:23123/_codex_proxy/health",
		"Invoke-RestMethod",
		"$response.ok",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("reachability script missing %q:\n%s", want, script)
		}
	}
}

func TestCodexAppAuthWindowsProxyBrowserScriptUsesChromiumProxy(t *testing.T) {
	script := codexAppAuthWindowsProxyBrowserScript("https://chatgpt.example/codex/device", "http://127.0.0.1:23123", "run one")
	for _, want := range []string{
		"$profileRoot = Join-Path $env:LOCALAPPDATA 'codex-helper\\app-auth-browser'",
		"foreach ($browser in $browserPaths)",
		"$profile = Join-Path $profileRoot ($runId + '-' + $browserId)",
		"$env:LOCALAPPDATA 'Microsoft\\Edge\\Application\\msedge.exe'",
		"$env:LOCALAPPDATA 'Google\\Chrome\\Application\\chrome.exe'",
		"Microsoft\\Edge\\Application\\msedge.exe",
		"Google\\Chrome\\Application\\chrome.exe",
		"--user-data-dir=",
		"--proxy-server=",
		"--new-window",
		"--no-first-run",
		"--disable-extensions",
		"http://127.0.0.1:23123",
		"https://chatgpt.example/codex/device",
		"& $browser @args",
		"$failures += ($browser + ': ' + $_.Exception.Message)",
		"Could not launch a proxy-managed Edge or Chrome browser",
	} {
		if !strings.Contains(script, want) {
			t.Fatalf("script missing %q:\n%s", want, script)
		}
	}
}

func TestCodexAppAuthWindowsPowerShellScriptsParseWhenAvailable(t *testing.T) {
	name := teamsServicePowerShellExecutable()
	path, err := exec.LookPath(name)
	if err != nil {
		t.Skipf("%s not found", name)
	}
	scripts := map[string]string{
		"reachability": codexAppAuthWindowsProxyReachabilityScript("127.0.0.1", "23123"),
		"browser":      codexAppAuthWindowsProxyBrowserScript("https://chatgpt.example/codex/device", "http://127.0.0.1:23123", "run one"),
	}
	for label, script := range scripts {
		t.Run(label, func(t *testing.T) {
			cmd := exec.Command(path, "-NoProfile", "-NonInteractive", "-ExecutionPolicy", "Bypass", "-Command", "[scriptblock]::Create("+powershellSingleQuote(script)+") | Out-Null")
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("PowerShell script parse failed: %v\n%s", err, string(out))
			}
		})
	}
}

func TestAppCommandWiresAuthSubcommand(t *testing.T) {
	cmd := newAppCmd(&rootOptions{})
	sub, _, err := cmd.Find([]string{"auth"})
	if err != nil {
		t.Fatalf("find app auth: %v", err)
	}
	if sub.Name() != "auth" {
		t.Fatalf("app auth subcommand name = %q", sub.Name())
	}
	for _, flag := range []string{"codex-dir", "cwd", "profile", "codex-path", "no-open-browser", "timeout"} {
		if sub.Flags().Lookup(flag) == nil {
			t.Fatalf("app auth missing --%s flag", flag)
		}
	}
}

func TestAppCommandProfileFlagStillSupportsAuthProfileName(t *testing.T) {
	profile, err := parseCodexAppArgs(nil, "auth")
	if err != nil {
		t.Fatalf("parse app --profile auth: %v", err)
	}
	if profile != "auth" {
		t.Fatalf("profile = %q, want auth", profile)
	}
	profile, err = parseCodexAppArgs(nil, "app")
	if err != nil {
		t.Fatalf("parse app --profile app: %v", err)
	}
	if profile != "app" {
		t.Fatalf("profile = %q, want app", profile)
	}
}

func TestAppCommandDispatchKeepsProfileNamesSeparateFromAuthSubcommand(t *testing.T) {
	cmd := newAppCmd(&rootOptions{})
	sub, remaining, err := cmd.Find([]string{"--profile", "auth"})
	if err != nil {
		t.Fatalf("find app --profile auth: %v", err)
	}
	if sub != cmd {
		t.Fatalf("app --profile auth dispatched to %q, want app command", sub.Name())
	}
	if !reflect.DeepEqual(remaining, []string{"--profile", "auth"}) {
		t.Fatalf("remaining args = %#v", remaining)
	}

	sub, remaining, err = cmd.Find([]string{"auth", "--profile", "auth"})
	if err != nil {
		t.Fatalf("find app auth --profile auth: %v", err)
	}
	if sub.Name() != "auth" {
		t.Fatalf("app auth --profile auth dispatched to %q, want auth subcommand", sub.Name())
	}
	if !reflect.DeepEqual(remaining, []string{"--profile", "auth"}) {
		t.Fatalf("auth remaining args = %#v", remaining)
	}
}

func assertUnsupportedServerResponse(t *testing.T, got map[string]any, wantID string) {
	t.Helper()
	if got["id"] != wantID {
		t.Fatalf("unsupported response id = %#v, want %q; response %#v", got["id"], wantID, got)
	}
	errField, ok := got["error"].(map[string]any)
	if !ok {
		t.Fatalf("unsupported response missing error field: %#v", got)
	}
	if errField["message"] != "session/configure is not supported by codex-helper app auth" {
		t.Fatalf("unsupported response message = %#v", errField["message"])
	}
	if errField["code"] != float64(-32601) {
		t.Fatalf("unsupported response code = %#v", errField["code"])
	}
}

func countUnsupportedServerResponses(t *testing.T, transport *fakeCodexAppAuthTransport) int {
	t.Helper()
	transport.mu.Lock()
	defer transport.mu.Unlock()
	count := 0
	for _, line := range transport.writes {
		var got struct {
			Error *struct {
				Code    float64 `json:"code"`
				Message string  `json:"message"`
			} `json:"error"`
		}
		if err := json.Unmarshal(line, &got); err != nil {
			t.Fatalf("decode write: %v\n%s", err, string(line))
		}
		if got.Error != nil && got.Error.Code == -32601 && strings.Contains(got.Error.Message, "is not supported by codex-helper app auth") {
			count++
		}
	}
	return count
}

func boolName(name string, value bool) string {
	if value {
		return name
	}
	return "no_" + name
}

type fakeCodexAppAuthStarter struct {
	request       codexrunner.AppServerStartRequest
	transport     codexrunner.AppServerLineTransport
	configured    bool
	configuredEnv []string
}

func (s *fakeCodexAppAuthStarter) StartAppServer(_ context.Context, req codexrunner.AppServerStartRequest) (codexrunner.AppServerLineTransport, error) {
	s.request = req
	if req.ConfigureCommand != nil {
		cmd := &exec.Cmd{Env: []string{"PATH=/usr/bin"}}
		if err := req.ConfigureCommand(cmd); err != nil {
			return nil, err
		}
		s.configured = true
		s.configuredEnv = append([]string{}, cmd.Env...)
	}
	return s.transport, nil
}

type fakeCodexAppAuthTransport struct {
	reads         chan []byte
	onReadBlock   func()
	onReadBlockMu sync.Once
	afterRead     func([]byte)
	afterReadMu   sync.Once
	mu            sync.Mutex
	writes        [][]byte
}

func newFakeCodexAppAuthTransport(lines ...string) *fakeCodexAppAuthTransport {
	ch := make(chan []byte, len(lines))
	for _, line := range lines {
		ch <- []byte(line)
	}
	return &fakeCodexAppAuthTransport{reads: ch}
}

func (t *fakeCodexAppAuthTransport) WriteLine(_ context.Context, line []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.writes = append(t.writes, append([]byte{}, line...))
	return nil
}

func (t *fakeCodexAppAuthTransport) ReadLine(ctx context.Context) ([]byte, error) {
	select {
	case line := <-t.reads:
		if t.afterRead != nil {
			t.afterReadMu.Do(func() { t.afterRead(append([]byte{}, line...)) })
		}
		return append([]byte{}, line...), nil
	default:
		if t.onReadBlock != nil {
			t.onReadBlockMu.Do(t.onReadBlock)
		}
	}
	select {
	case line := <-t.reads:
		if t.afterRead != nil {
			t.afterReadMu.Do(func() { t.afterRead(append([]byte{}, line...)) })
		}
		return append([]byte{}, line...), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (t *fakeCodexAppAuthTransport) Close() error {
	return nil
}

func (t *fakeCodexAppAuthTransport) decodedWrite(tb testing.TB, index int) map[string]any {
	tb.Helper()
	t.mu.Lock()
	defer t.mu.Unlock()
	if index < 0 || index >= len(t.writes) {
		tb.Fatalf("write index %d out of range, writes:\n%s", index, t.joinedWritesLocked())
	}
	var got map[string]any
	if err := json.Unmarshal(t.writes[index], &got); err != nil {
		tb.Fatalf("decode write %d: %v\n%s", index, err, string(t.writes[index]))
	}
	return got
}

func (t *fakeCodexAppAuthTransport) writeMethods(tb testing.TB) []string {
	tb.Helper()
	t.mu.Lock()
	defer t.mu.Unlock()
	methods := make([]string, 0, len(t.writes))
	for _, line := range t.writes {
		var got struct {
			Method string `json:"method"`
		}
		if err := json.Unmarshal(line, &got); err != nil {
			tb.Fatalf("decode write: %v\n%s", err, string(line))
		}
		methods = append(methods, got.Method)
	}
	return methods
}

func (t *fakeCodexAppAuthTransport) joinedWrites() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.joinedWritesLocked()
}

func (t *fakeCodexAppAuthTransport) joinedWritesLocked() string {
	var parts []string
	for _, line := range t.writes {
		parts = append(parts, string(line))
	}
	return strings.Join(parts, "\n")
}

func writeFakeCodexVersionCommand(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if runtime.GOOS == "windows" {
		return writeFakeCodexVersionCommandAt(t, filepath.Join(dir, "codex.cmd"))
	}
	return writeFakeCodexVersionCommandAt(t, filepath.Join(dir, "codex"))
}

func writeFakeCodexVersionCommandAt(t *testing.T, path string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("create fake codex dir: %v", err)
	}
	if runtime.GOOS == "windows" {
		script := "@echo off\r\nif \"%~1\"==\"--version\" (\r\n  echo codex 0.0.0\r\n  exit /b 0\r\n)\r\nexit /b 0\r\n"
		if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
			t.Fatalf("write fake codex: %v", err)
		}
		return path
	}
	script := "#!/bin/sh\nif [ \"$1\" = \"--version\" ]; then echo 'codex 0.0.0'; exit 0; fi\nexit 0\n"
	if err := os.WriteFile(path, []byte(script), 0o700); err != nil {
		t.Fatalf("write fake codex: %v", err)
	}
	return path
}

func writeFailingCodexVersionCommand(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if runtime.GOOS == "windows" {
		path := filepath.Join(dir, "codex.cmd")
		if err := os.WriteFile(path, []byte("@echo off\r\nexit /b 42\r\n"), 0o700); err != nil {
			t.Fatalf("write failing codex: %v", err)
		}
		return path
	}
	path := filepath.Join(dir, "codex")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 42\n"), 0o700); err != nil {
		t.Fatalf("write failing codex: %v", err)
	}
	return path
}

func appAuthTestCommandContext(ctx context.Context, exitCode int) *exec.Cmd {
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=TestCodexAppAuthHelperProcess")
	cmd.Env = append(os.Environ(),
		"CODEX_APP_AUTH_HELPER_PROCESS=1",
		"CODEX_APP_AUTH_HELPER_EXIT="+strconv.Itoa(exitCode),
	)
	return cmd
}

func TestCodexAppAuthHelperProcess(t *testing.T) {
	if os.Getenv("CODEX_APP_AUTH_HELPER_PROCESS") != "1" {
		return
	}
	code, _ := strconv.Atoi(os.Getenv("CODEX_APP_AUTH_HELPER_EXIT"))
	os.Exit(code)
}
