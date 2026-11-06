package userpath

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

// Mode describes the source of the PATH exposed to a Teams-launched Codex.
type Mode string

const (
	ModeAccountDefault Mode = "account-default"
	ModeExplicit       Mode = "explicit"
)

// Modes saved by earlier releases: a captured terminal PATH was saved like
// an explicit one, and the service mode now resolves the account default.
const (
	legacyModeCapturedTerminal = "captured-terminal"
	legacyModeService          = "service"
)

type Policy struct {
	Mode          Mode
	ExplicitPath  string
	ShellOverride string
}

type TargetIdentity struct {
	UID         uint32
	GID         uint32
	Groups      []uint32
	GroupsKnown bool
	Username    string
	Home        string
	SID         string
	WSLDistro   string
}

type Request struct {
	Policy             Policy
	Target             TargetIdentity
	ServiceEnvironment []string
	HelperExecutable   string
	WorkingDir         string
	Timeout            time.Duration
	ConfigureCommand   func(*exec.Cmd) error
}

type Result struct {
	Path           string
	Source         string
	Mode           Mode
	Target         TargetIdentity
	AccountShell   string
	Adapter        string
	BaselineSource string
	CapturedAt     time.Time
	Fingerprint    string
	EntryCount     int
}

type Resolver interface {
	Resolve(context.Context, Request) (Result, error)
}

type DefaultResolver struct{}

func (DefaultResolver) Resolve(ctx context.Context, req Request) (Result, error) {
	mode, err := normalizeMode(req.Policy.Mode)
	if err != nil {
		return Result{}, err
	}
	if req.Timeout <= 0 {
		req.Timeout = 15 * time.Second
	}

	var result Result
	switch mode {
	case ModeExplicit:
		if req.Policy.ExplicitPath == "" {
			return Result{}, fmt.Errorf("Teams Codex PATH mode %s requires a non-empty path", mode)
		}
		result = Result{Path: req.Policy.ExplicitPath, Source: "explicit-config", Mode: mode, Target: req.Target}
	case ModeAccountDefault:
		result, err = resolveAccountDefault(ctx, req)
		if err != nil {
			return Result{}, err
		}
	default:
		return Result{}, fmt.Errorf("unsupported Teams Codex PATH mode %q", mode)
	}
	if strings.IndexByte(result.Path, 0) >= 0 {
		return Result{}, fmt.Errorf("Teams Codex PATH contains a NUL byte")
	}

	result.CapturedAt = time.Now().UTC()
	result.Fingerprint = PathFingerprint(result.Path)
	result.EntryCount = len(filepath.SplitList(result.Path))
	return result, nil
}

func normalizeMode(mode Mode) (Mode, error) {
	switch Mode(strings.ToLower(strings.TrimSpace(string(mode)))) {
	case "", ModeAccountDefault, legacyModeService:
		return ModeAccountDefault, nil
	case ModeExplicit, legacyModeCapturedTerminal:
		return ModeExplicit, nil
	default:
		return "", fmt.Errorf("unknown Teams Codex PATH mode %q", mode)
	}
}

func PathFingerprint(pathValue string) string {
	digest := sha256.Sum256([]byte(pathValue))
	return hex.EncodeToString(digest[:8])
}

// EnvironmentValue returns the last value for key. Windows environment keys
// are case-insensitive, while Unix keys are not.
func EnvironmentValue(environment []string, key string, caseInsensitive bool) (string, bool) {
	for i := len(environment) - 1; i >= 0; i-- {
		entryKey, value, ok := strings.Cut(environment[i], "=")
		if !ok {
			continue
		}
		if entryKey == key || (caseInsensitive && strings.EqualFold(entryKey, key)) {
			return value, true
		}
	}
	return "", false
}
