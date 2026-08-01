package stack

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/config"
	"github.com/baaaaaaaka/codex-helper/internal/ssh"
)

func TestProbeHostNetworkConfigFileWaitsForResolvedEndpoint(t *testing.T) {
	profile := config.Profile{
		Host:    "corp-alias",
		Port:    22,
		SSHArgs: []string{"-F", "/tmp/corp-ssh-config"},
		User:    "alice",
	}
	var resolved int
	var dials int
	deps := hostProbeDependencies{
		listInterfaces: func() ([]net.Interface, error) {
			return []net.Interface{{Name: "wifi", Flags: net.FlagUp}}, nil
		},
		resolveEndpoint: func(context.Context, string, []string) (ssh.EffectiveEndpoint, error) {
			resolved++
			return ssh.EffectiveEndpoint{Host: "ssh.corp.example", Port: 2207}, nil
		},
		dialContext: func(context.Context, string, string) (net.Conn, error) {
			dials++
			if dials == 1 {
				return nil, &net.DNSError{Err: "temporary enterprise DNS failure", Name: "ssh.corp.example"}
			}
			left, right := net.Pipe()
			_ = right.Close()
			return left, nil
		},
	}
	oldDeps := hostProbeDependenciesFn
	hostProbeDependenciesFn = func() hostProbeDependencies { return deps }
	t.Cleanup(func() { hostProbeDependenciesFn = oldDeps })

	if err := ProbeHostNetwork(context.Background(), profile); err == nil {
		t.Fatal("network probe succeeded while the resolved SSH endpoint was unavailable")
	}
	if resolved != 1 || dials != 1 {
		t.Fatalf("first probe calls = resolve %d/dial %d, want one each", resolved, dials)
	}
	if err := ProbeHostNetwork(context.Background(), profile); err != nil {
		t.Fatalf("network probe did not admit the endpoint after DNS recovery: %v", err)
	}
	if resolved != 2 || dials != 2 {
		t.Fatalf("recovery probe calls = resolve %d/dial %d, want two each", resolved, dials)
	}
}

func TestProbeHostNetworkExplicitRouteTargetOverridesSSHConfigResolution(t *testing.T) {
	profile := config.Profile{
		Host:            "corp-alias",
		Port:            22,
		SSHArgs:         []string{"-F", "/tmp/corp-ssh-config"},
		RouteTargetHost: "api.corp.example",
		RouteTargetPort: 443,
	}
	deps := hostProbeDependencies{
		listInterfaces: func() ([]net.Interface, error) {
			return []net.Interface{{Name: "wifi", Flags: net.FlagUp}}, nil
		},
		resolveEndpoint: func(context.Context, string, []string) (ssh.EffectiveEndpoint, error) {
			return ssh.EffectiveEndpoint{}, errors.New("ssh -G must not be used when RouteTarget is explicit")
		},
		dialContext: func(_ context.Context, network, address string) (net.Conn, error) {
			if network != "tcp" || address != "api.corp.example:443" {
				t.Fatalf("explicit route dial = %s %s", network, address)
			}
			left, right := net.Pipe()
			_ = right.Close()
			return left, nil
		},
	}
	if err := probeHostNetworkWith(context.Background(), profile, deps); err != nil {
		t.Fatalf("explicit route target probe = %v", err)
	}
}

func TestProbeHostNetworkProxyCommandDefersToSSHCandidate(t *testing.T) {
	profile := config.Profile{Host: "corp-alias", Port: 22, SSHArgs: []string{"-F", "/tmp/corp-ssh-config"}}
	dialed := false
	deps := hostProbeDependencies{
		listInterfaces: func() ([]net.Interface, error) {
			return []net.Interface{{Name: "wifi", Flags: net.FlagUp}}, nil
		},
		resolveEndpoint: func(context.Context, string, []string) (ssh.EffectiveEndpoint, error) {
			return ssh.EffectiveEndpoint{
				Host:         "ssh.corp.example",
				Port:         2207,
				ProxyCommand: "ssh jump.corp -W %h:%p",
			}, nil
		},
		dialContext: func(context.Context, string, string) (net.Conn, error) {
			dialed = true
			return nil, errors.New("unexpected direct dial")
		},
	}
	if err := probeHostNetworkWith(context.Background(), profile, deps); err != nil {
		t.Fatalf("ProxyCommand admission should defer to SSH candidate: %v", err)
	}
	if dialed {
		t.Fatal("ProxyCommand admission directly dialed the final SSH endpoint")
	}
}

func TestProbeHostNetworkRejectsInterfaceOnlyWake(t *testing.T) {
	profile := config.Profile{Host: "corp-alias", Port: 22, SSHArgs: []string{"-F", "/tmp/corp-ssh-config"}}
	deps := hostProbeDependencies{
		listInterfaces: func() ([]net.Interface, error) {
			return []net.Interface{{Name: "wifi", Flags: net.FlagUp}}, nil
		},
		resolveEndpoint: func(context.Context, string, []string) (ssh.EffectiveEndpoint, error) {
			return ssh.EffectiveEndpoint{}, errors.New("enterprise DNS is not ready")
		},
	}
	if err := probeHostNetworkWith(context.Background(), profile, deps); err == nil {
		t.Fatal("interface-only wake was admitted while ssh -G could not resolve the endpoint")
	}
}

func TestProbeHostNetworkRequiresUsableInterface(t *testing.T) {
	deps := hostProbeDependencies{
		listInterfaces: func() ([]net.Interface, error) {
			return []net.Interface{{Name: "wifi", Flags: net.FlagLoopback}}, nil
		},
	}
	if err := probeHostNetworkWith(context.Background(), config.Profile{Host: "host", Port: 22}, deps); err == nil {
		t.Fatal("probe succeeded without a usable non-loopback interface")
	}
}
