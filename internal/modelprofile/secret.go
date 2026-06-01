package modelprofile

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	SecretRefPrefix = "secret:"
	EnvRefPrefix    = "env:"
)

type SecretStore struct {
	path string
}

type secretFile struct {
	Version int               `json:"version"`
	Secrets map[string]string `json:"secrets,omitempty"`
}

func SecretRefForProfile(name string) string {
	return SecretRefPrefix + "model-profile/" + strings.TrimSpace(name) + "/api-key"
}

func SecretRefForCredentialScope(scope string) string {
	return SecretRefPrefix + "model-credential/" + strings.TrimSpace(scope) + "/default/api-key"
}

func SecretPathForConfig(configPath string) string {
	dir := filepath.Dir(filepath.Clean(configPath))
	return filepath.Join(dir, "model-profile-secrets.json")
}

func NewSecretStore(path string) *SecretStore {
	return &SecretStore{path: strings.TrimSpace(path)}
}

func (s *SecretStore) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

func (s *SecretStore) Get(ref string) (string, bool, error) {
	key, ok := secretKey(ref)
	if !ok {
		return "", false, nil
	}
	file, err := s.load()
	if err != nil {
		return "", false, err
	}
	value, ok := file.Secrets[key]
	return value, ok, nil
}

func (s *SecretStore) Put(ref string, value string) error {
	key, ok := secretKey(ref)
	if !ok {
		return fmt.Errorf("not a secret ref: %q", ref)
	}
	file, err := s.load()
	if err != nil {
		return err
	}
	if file.Secrets == nil {
		file.Secrets = map[string]string{}
	}
	file.Secrets[key] = value
	return s.save(file)
}

func ResolveAPIKey(ref string, secrets *SecretStore, env func(string) string) (string, error) {
	ref = strings.TrimSpace(ref)
	switch {
	case ref == "":
		return "", nil
	case strings.HasPrefix(ref, EnvRefPrefix):
		name := strings.TrimSpace(strings.TrimPrefix(ref, EnvRefPrefix))
		if name == "" {
			return "", fmt.Errorf("empty env api key ref")
		}
		if env == nil {
			env = os.Getenv
		}
		value := strings.TrimSpace(env(name))
		if value == "" {
			return "", fmt.Errorf("environment variable %s is not set", name)
		}
		return value, nil
	case strings.HasPrefix(ref, SecretRefPrefix):
		if secrets == nil {
			return "", fmt.Errorf("secret store is not configured")
		}
		value, ok, err := secrets.Get(ref)
		if err != nil {
			return "", err
		}
		if !ok || strings.TrimSpace(value) == "" {
			return "", fmt.Errorf("secret %s is not set", MaskRef(ref))
		}
		return value, nil
	default:
		return "", fmt.Errorf("unsupported api key ref %q", MaskRef(ref))
	}
}

func MaskRef(ref string) string {
	ref = strings.TrimSpace(ref)
	switch {
	case ref == "":
		return ""
	case strings.HasPrefix(ref, EnvRefPrefix):
		return ref
	case strings.HasPrefix(ref, SecretRefPrefix):
		return SecretRefPrefix + "<saved>"
	default:
		return "<redacted>"
	}
}

func Fingerprint(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])[:16]
}

func secretKey(ref string) (string, bool) {
	ref = strings.TrimSpace(ref)
	if !strings.HasPrefix(ref, SecretRefPrefix) {
		return "", false
	}
	key := strings.TrimSpace(strings.TrimPrefix(ref, SecretRefPrefix))
	return key, key != ""
}

func (s *SecretStore) load() (secretFile, error) {
	if s == nil || strings.TrimSpace(s.path) == "" {
		return secretFile{}, fmt.Errorf("secret store path is empty")
	}
	raw, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			return secretFile{Version: 1, Secrets: map[string]string{}}, nil
		}
		return secretFile{}, err
	}
	var file secretFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return secretFile{}, fmt.Errorf("parse secret store: %w", err)
	}
	if file.Version == 0 {
		file.Version = 1
	}
	if file.Version != 1 {
		return secretFile{}, fmt.Errorf("unsupported secret store version %d", file.Version)
	}
	if file.Secrets == nil {
		file.Secrets = map[string]string{}
	}
	return file, nil
}

func (s *SecretStore) save(file secretFile) error {
	if s == nil || strings.TrimSpace(s.path) == "" {
		return fmt.Errorf("secret store path is empty")
	}
	file.Version = 1
	raw, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}
