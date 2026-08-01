// Package appgateway owns the stable client-facing proxy registration used by
// `cxp app`. It is deliberately separate from the legacy proxy instance
// registry: old binaries may rewrite config.json and must not be able to erase
// the port that an already-running desktop application uses.
package appgateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gofrs/flock"
)

const (
	CurrentSchema    = 1
	StateReady       = "ready"
	StatePending     = "pending"
	StateBlocked     = "blocked"
	eventLogMaxBytes = 2 << 20
)

var (
	ErrRegistrationNotFound  = errors.New("app gateway registration not found")
	ErrInvalidRegistration   = errors.New("invalid app gateway registration")
	ErrPortInUse             = errors.New("app gateway port is already in use")
	ErrRegistrationLeaseHeld = errors.New("app gateway registration is already owned")
)

// Registration is the durable identity of the client-facing proxy. The
// backend process and its ephemeral SOCKS port are intentionally absent: a
// backend may be replaced without changing this record or the ChatGPT command
// line.
type Registration struct {
	Schema             int       `json:"schema"`
	ID                 string    `json:"id"`
	ProfileID          string    `json:"profileId"`
	ProfileFingerprint string    `json:"profileFingerprint,omitempty"`
	HTTPPort           int       `json:"httpPort"`
	LegacyPorts        []int     `json:"legacyPorts,omitempty"`
	State              string    `json:"state"`
	MigrationStage     string    `json:"migrationStage,omitempty"`
	OwnerGeneration    string    `json:"ownerGeneration,omitempty"`
	OwnerPID           int       `json:"ownerPid,omitempty"`
	OwnerStartedAt     time.Time `json:"ownerStartedAt,omitempty"`
	OwnerExecutable    string    `json:"ownerExecutable,omitempty"`
	OwnerCommandLine   string    `json:"ownerCommandLine,omitempty"`
	BackendGeneration  string    `json:"backendGeneration,omitempty"`
	CreatedAt          time.Time `json:"createdAt"`
	UpdatedAt          time.Time `json:"updatedAt"`
	LastReadyAt        time.Time `json:"lastReadyAt,omitempty"`
	LastError          string    `json:"lastError,omitempty"`
	RecoveryAttempts   int       `json:"recoveryAttempts,omitempty"`
	RecoveryWindowAt   time.Time `json:"recoveryWindowAt,omitempty"`
	RecoveryBlocked    bool      `json:"recoveryBlocked,omitempty"`
	ReplacedInstanceID string    `json:"replacedInstanceId,omitempty"`
}

// Event is an append-only audit record for lifecycle and recovery transitions.
// It keeps the evidence needed to explain a port handoff without relying on a
// legacy config entry that may have been pruned by an older binary.
type Event struct {
	At                time.Time `json:"at"`
	RegistrationID    string    `json:"registrationId"`
	ProfileID         string    `json:"profileId"`
	Event             string    `json:"event"`
	HTTPPort          int       `json:"httpPort,omitempty"`
	BackendGeneration string    `json:"backendGeneration,omitempty"`
	State             string    `json:"state,omitempty"`
	Error             string    `json:"error,omitempty"`
	Details           string    `json:"details,omitempty"`
}

// Registry stores one bounded file per profile. dir is injected so unit tests
// do not touch the user's state; the CLI supplies appdirs.StatePath.
type Registry struct {
	dir string
	mu  sync.Mutex
}

func NewRegistry(dir string) (*Registry, error) {
	dir = filepath.Clean(strings.TrimSpace(dir))
	if dir == "" || dir == "." {
		return nil, errors.New("app gateway state directory is required")
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create app gateway state directory: %w", err)
	}
	return &Registry{dir: dir}, nil
}

func (r *Registry) Dir() string {
	if r == nil {
		return ""
	}
	return r.dir
}

func profileFilePart(profileID string) string {
	h := sha256.Sum256([]byte(strings.TrimSpace(profileID)))
	return hex.EncodeToString(h[:])
}

func (r *Registry) path(profileID string) string {
	return filepath.Join(r.dir, profileFilePart(profileID)+".json")
}

func (r *Registry) eventPath(profileID string) string {
	return filepath.Join(r.dir, profileFilePart(profileID)+".jsonl")
}

func (r *Registry) lockPath(profileID string) string {
	return r.path(profileID) + ".lock"
}

func (r *Registry) leasePath(profileID string) string {
	return r.path(profileID) + ".lease"
}

// AcquireLease fences duplicate Frontend processes. The lease is held for the
// complete daemon lifetime, not only during a state-file write.
func (r *Registry) AcquireLease(ctx context.Context, profileID string) (func(), error) {
	if r == nil {
		return nil, errors.New("nil app gateway registry")
	}
	profileID = strings.TrimSpace(profileID)
	if profileID == "" {
		return nil, fmt.Errorf("%w: profile id is required", ErrInvalidRegistration)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// Keep the lifetime lease separate from the short state-file transaction
	// lock. The daemon holds this lock while it repeatedly persists health and
	// recovery state; using the same path would make those writes self-deadlock
	// on platforms whose flock implementation is not re-entrant.
	lock := flock.New(r.leasePath(profileID))
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// A lifetime lease is an ownership election, not a queue. A second
	// scheduler invocation must exit immediately and let the existing owner
	// continue serving the stable listener.
	locked, err := lock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("lock app gateway lifetime lease: %w", err)
	}
	if !locked {
		return nil, ErrRegistrationLeaseHeld
	}
	return func() { _ = lock.Unlock() }, nil
}

func (r *Registry) Load(profileID string) (Registration, error) {
	if r == nil {
		return Registration{}, errors.New("nil app gateway registry")
	}
	profileID = strings.TrimSpace(profileID)
	if profileID == "" {
		return Registration{}, fmt.Errorf("%w: profile id is required", ErrInvalidRegistration)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.load(profileID)
}

func (r *Registry) load(profileID string) (Registration, error) {
	path := r.path(profileID)
	b, err := os.ReadFile(path)
	if err == nil {
		if reg, decodeErr := decodeRegistration(b); decodeErr == nil {
			return reg, nil
		} else if backup, backupErr := os.ReadFile(path + ".bak"); backupErr == nil {
			if reg, restoreErr := decodeRegistration(backup); restoreErr == nil {
				return reg, nil
			}
			return Registration{}, decodeErr
		} else {
			return Registration{}, decodeErr
		}
	}
	if errors.Is(err, os.ErrNotExist) {
		return Registration{}, ErrRegistrationNotFound
	}
	return Registration{}, fmt.Errorf("read app gateway registration: %w", err)
}

func decodeRegistration(b []byte) (Registration, error) {
	var reg Registration
	if err := json.Unmarshal(b, &reg); err != nil {
		return Registration{}, fmt.Errorf("parse app gateway registration: %w", err)
	}
	if err := Validate(reg); err != nil {
		return Registration{}, err
	}
	return reg, nil
}

// Ensure creates or updates a registration without changing a previously
// assigned HTTP port. The caller must still start and health-check the
// Gateway before treating the result as active.
func (r *Registry) Ensure(ctx context.Context, profileID, fingerprint string) (Registration, error) {
	return r.ensure(ctx, profileID, fingerprint, 0)
}

// EnsureWithPort is used only during legacy migration. preferredPort is
// accepted for a new registration and never replaces an already committed
// stable port.
func (r *Registry) EnsureWithPort(ctx context.Context, profileID, fingerprint string, preferredPort int) (Registration, error) {
	return r.ensure(ctx, profileID, fingerprint, preferredPort)
}

func (r *Registry) ensure(ctx context.Context, profileID, fingerprint string, preferredPort int) (Registration, error) {
	if r == nil {
		return Registration{}, errors.New("nil app gateway registry")
	}
	profileID = strings.TrimSpace(profileID)
	if profileID == "" {
		return Registration{}, fmt.Errorf("%w: profile id is required", ErrInvalidRegistration)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	lock := flock.New(r.lockPath(profileID))
	locked, err := lock.TryLockContext(ctx, 5*time.Second)
	if err != nil {
		return Registration{}, fmt.Errorf("lock app gateway registration: %w", err)
	}
	if !locked {
		return Registration{}, errors.New("app gateway registration lock was not acquired")
	}
	defer func() { _ = lock.Unlock() }()

	r.mu.Lock()
	defer r.mu.Unlock()
	reg, err := r.load(profileID)
	if err == nil {
		changed := false
		if preferredPort > 0 && preferredPort != reg.HTTPPort && reg.State != StateReady && reg.LastReadyAt.IsZero() && reg.OwnerPID == 0 {
			// A registration that has never reached ready may safely adopt the
			// client's legacy port during the first migration. Once the Gateway
			// has served a client, its canonical port is immutable.
			reg.LegacyPorts = appendUniquePort(reg.LegacyPorts, reg.HTTPPort)
			reg.HTTPPort = preferredPort
			reg.MigrationStage = "legacy-port-adopted"
			changed = true
		}
		if strings.TrimSpace(fingerprint) != "" && reg.ProfileFingerprint != strings.TrimSpace(fingerprint) {
			reg.ProfileFingerprint = strings.TrimSpace(fingerprint)
			changed = true
		}
		if reg.State == "" {
			reg.State = StatePending
			changed = true
		}
		if changed {
			reg.UpdatedAt = time.Now()
			if err := r.save(reg); err != nil {
				return Registration{}, err
			}
		}
		return reg, nil
	}
	if !errors.Is(err, ErrRegistrationNotFound) {
		return Registration{}, err
	}
	port := preferredPort
	if port < 1 || port > 65535 {
		port, err = allocatePort()
		if err != nil {
			return Registration{}, fmt.Errorf("allocate stable app gateway port: %w", err)
		}
	}
	now := time.Now()
	reg = Registration{
		Schema:             CurrentSchema,
		ID:                 profileFilePart(profileID),
		ProfileID:          profileID,
		ProfileFingerprint: strings.TrimSpace(fingerprint),
		HTTPPort:           port,
		State:              StatePending,
		MigrationStage:     migrationStageForPreferredPort(preferredPort),
		CreatedAt:          now,
		UpdatedAt:          now,
	}
	if err := r.save(reg); err != nil {
		return Registration{}, err
	}
	return reg, nil
}

func migrationStageForPreferredPort(port int) string {
	if port > 0 {
		return "legacy-port-adopted"
	}
	return ""
}

func appendUniquePort(ports []int, port int) []int {
	for _, existing := range ports {
		if existing == port {
			return ports
		}
	}
	return append(ports, port)
}

func (r *Registry) Save(reg Registration) error {
	if r == nil {
		return errors.New("nil app gateway registry")
	}
	if err := Validate(reg); err != nil {
		return err
	}
	lock := flock.New(r.lockPath(reg.ProfileID))
	locked, err := lock.TryLockContext(context.Background(), 5*time.Second)
	if err != nil {
		return fmt.Errorf("lock app gateway registration: %w", err)
	}
	if !locked {
		return errors.New("app gateway registration lock was not acquired")
	}
	defer func() { _ = lock.Unlock() }()
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.save(reg)
}

// AppendEvent records a bounded-size lifecycle line. Callers should emit only
// state transitions and recovery attempts, not every request, so wake storms
// cannot turn the audit file into an unbounded write loop.
func (r *Registry) AppendEvent(event Event) error {
	if r == nil {
		return errors.New("nil app gateway registry")
	}
	event.ProfileID = strings.TrimSpace(event.ProfileID)
	if event.ProfileID == "" {
		return fmt.Errorf("%w: event profile id is required", ErrInvalidRegistration)
	}
	if strings.TrimSpace(event.RegistrationID) == "" {
		event.RegistrationID = profileFilePart(event.ProfileID)
	}
	if strings.TrimSpace(event.Event) == "" {
		return errors.New("app gateway event name is required")
	}
	if event.At.IsZero() {
		event.At = time.Now().UTC()
	}
	b, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal app gateway event: %w", err)
	}
	b = append(b, '\n')
	lock := flock.New(r.eventPath(event.ProfileID) + ".lock")
	locked, err := lock.TryLockContext(context.Background(), 5*time.Second)
	if err != nil {
		return fmt.Errorf("lock app gateway event log: %w", err)
	}
	if !locked {
		return errors.New("app gateway event log lock was not acquired")
	}
	defer func() { _ = lock.Unlock() }()
	if info, statErr := os.Stat(r.eventPath(event.ProfileID)); statErr == nil && info.Size()+int64(len(b)) > eventLogMaxBytes {
		backup := r.eventPath(event.ProfileID) + ".1"
		_ = os.Remove(backup)
		if err := os.Rename(r.eventPath(event.ProfileID), backup); err != nil {
			return fmt.Errorf("rotate app gateway event log: %w", err)
		}
	} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return fmt.Errorf("stat app gateway event log: %w", statErr)
	}
	f, err := os.OpenFile(r.eventPath(event.ProfileID), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open app gateway event log: %w", err)
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		return fmt.Errorf("write app gateway event: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("flush app gateway event: %w", err)
	}
	return nil
}

func (r *Registry) save(reg Registration) error {
	if err := Validate(reg); err != nil {
		return err
	}
	reg.Schema = CurrentSchema
	reg.UpdatedAt = time.Now()
	b, err := json.MarshalIndent(reg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal app gateway registration: %w", err)
	}
	b = append(b, '\n')
	tmp, err := os.CreateTemp(r.dir, ".registration-*.tmp")
	if err != nil {
		return fmt.Errorf("create app gateway registration temporary file: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("protect app gateway registration: %w", err)
	}
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write app gateway registration: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("flush app gateway registration: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close app gateway registration: %w", err)
	}
	// Keep the last committed registration available for deterministic repair
	// after a power loss. The backup is replaced atomically in the same
	// directory before the new state becomes visible.
	if previous, readErr := os.ReadFile(r.path(reg.ProfileID)); readErr == nil {
		if _, decodeErr := decodeRegistration(previous); decodeErr == nil {
			backup, createErr := os.CreateTemp(r.dir, ".registration-backup-*.tmp")
			if createErr != nil {
				return fmt.Errorf("create app gateway registration backup: %w", createErr)
			}
			backupName := backup.Name()
			defer func() { _ = os.Remove(backupName) }()
			if err := backup.Chmod(0o600); err != nil {
				_ = backup.Close()
				return fmt.Errorf("protect app gateway registration backup: %w", err)
			}
			if _, err := backup.Write(previous); err != nil {
				_ = backup.Close()
				return fmt.Errorf("write app gateway registration backup: %w", err)
			}
			if err := backup.Sync(); err != nil {
				_ = backup.Close()
				return fmt.Errorf("flush app gateway registration backup: %w", err)
			}
			if err := backup.Close(); err != nil {
				return fmt.Errorf("close app gateway registration backup: %w", err)
			}
			if err := os.Rename(backupName, r.path(reg.ProfileID)+".bak"); err != nil {
				return fmt.Errorf("commit app gateway registration backup: %w", err)
			}
		}
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return fmt.Errorf("read previous app gateway registration: %w", readErr)
	}

	if err := os.Rename(tmpName, r.path(reg.ProfileID)); err != nil {
		return fmt.Errorf("commit app gateway registration: %w", err)
	}
	return nil
}

func Validate(reg Registration) error {
	if reg.Schema <= 0 || reg.Schema > CurrentSchema {
		return fmt.Errorf("%w: unsupported schema %d", ErrInvalidRegistration, reg.Schema)
	}
	if strings.TrimSpace(reg.ID) == "" || strings.TrimSpace(reg.ProfileID) == "" {
		return fmt.Errorf("%w: id and profile id are required", ErrInvalidRegistration)
	}
	if reg.ID != profileFilePart(reg.ProfileID) {
		return fmt.Errorf("%w: registration id does not match profile", ErrInvalidRegistration)
	}
	if reg.HTTPPort < 1 || reg.HTTPPort > 65535 {
		return fmt.Errorf("%w: invalid HTTP port %d", ErrInvalidRegistration, reg.HTTPPort)
	}
	switch reg.State {
	case StateReady, StatePending, StateBlocked:
	default:
		return fmt.Errorf("%w: unsupported state %q", ErrInvalidRegistration, reg.State)
	}
	for _, port := range reg.LegacyPorts {
		if port < 1 || port > 65535 {
			return fmt.Errorf("%w: invalid legacy port %d", ErrInvalidRegistration, port)
		}
	}
	return nil
}

func allocatePort() (int, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer listener.Close()
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok || addr.Port <= 0 {
		return 0, errors.New("operating system returned an invalid loopback port")
	}
	return addr.Port, nil
}

// PortAvailable is a conservative check used before adopting a port from a
// legacy ChatGPT command line. It never chooses a replacement port when the
// requested port is occupied.
func PortAvailable(port int) bool {
	if port < 1 || port > 65535 {
		return false
	}
	listener, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return false
	}
	_ = listener.Close()
	return true
}
