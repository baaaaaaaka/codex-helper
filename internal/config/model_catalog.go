package config

import (
	"fmt"
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// ModelCatalog is a user-configured source of provider/model definitions.
// Git catalogs are refreshed by the long-lived Teams service; managed-json
// catalogs are imported atomically by the CLI and are intentionally not
// watched implicitly.
type ModelCatalog struct {
	Type        string    `json:"type"`
	URL         string    `json:"url,omitempty"`
	Ref         string    `json:"ref,omitempty"`
	File        string    `json:"file,omitempty"`
	ManagedFile string    `json:"managedFile,omitempty"`
	AutoSync    bool      `json:"autoSync,omitempty"`
	Revision    string    `json:"revision,omitempty"`
	SyncedAt    time.Time `json:"syncedAt,omitempty"`
}

// ModelProviderBinding is the local part of a provider definition. Provider
// transport and model policies come from the catalog; the secret reference is
// deliberately kept in the local config/secret store and never in a catalog
// document.
type ModelProviderBinding struct {
	Catalog          string            `json:"catalog,omitempty"`
	SecretRef        string            `json:"secretRef,omitempty"`
	InterfaceSecrets map[string]string `json:"interfaceSecrets,omitempty"`
	Enabled          bool              `json:"enabled,omitempty"`
}

const (
	ModelCatalogTypeGit         = "git"
	ModelCatalogTypeManagedJSON = "managed-json"
)

var modelCatalogIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

func ValidateModelCatalogID(id string) error {
	id = strings.TrimSpace(id)
	if !modelCatalogIDPattern.MatchString(id) {
		return fmt.Errorf("invalid model catalog name %q (use letters, digits, '.', '_' or '-')", id)
	}
	return nil
}

func ValidateModelProviderID(id string) error {
	id = strings.TrimSpace(id)
	if !modelCatalogIDPattern.MatchString(id) || strings.Contains(id, "/") {
		return fmt.Errorf("invalid model provider name %q (use a flat identifier without '/')", id)
	}
	return nil
}

func ValidateModelID(id string) error {
	id = strings.TrimSpace(id)
	if !modelCatalogIDPattern.MatchString(id) || strings.Contains(id, "/") {
		return fmt.Errorf("invalid model name %q (use a flat identifier without '/')", id)
	}
	return nil
}

func (c ModelCatalog) Validate(name string) error {
	if err := ValidateModelCatalogID(name); err != nil {
		return err
	}
	typ := strings.ToLower(strings.TrimSpace(c.Type))
	if typ != strings.TrimSpace(c.Type) {
		return fmt.Errorf("model catalog %q type must be lowercase", name)
	}
	switch typ {
	case ModelCatalogTypeGit:
		urlValue := strings.TrimSpace(c.URL)
		parsed, err := url.Parse(urlValue)
		localPath := filepath.IsAbs(urlValue) || strings.HasPrefix(urlValue, "./") || strings.HasPrefix(urlValue, "../")
		scpLike := strings.HasPrefix(urlValue, "git@") && strings.Contains(urlValue, ":") && !strings.Contains(urlValue, "://") && !strings.ContainsAny(urlValue, " \t\r\n")
		if (err != nil && !localPath && !scpLike) || (!localPath && !scpLike && (parsed == nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https" && parsed.Scheme != "ssh" && parsed.Scheme != "git"))) {
			return fmt.Errorf("model catalog %q requires a Git URL or local repository path", name)
		}
		if parsed != nil && parsed.User != nil {
			return fmt.Errorf("model catalog %q URL must not contain credentials", name)
		}
		if strings.TrimSpace(c.ManagedFile) != "" {
			return fmt.Errorf("git model catalog %q must not set managedFile", name)
		}
		if strings.TrimSpace(c.File) == "" {
			return fmt.Errorf("git model catalog %q requires file", name)
		}
	case ModelCatalogTypeManagedJSON:
		if strings.TrimSpace(c.URL) != "" || strings.TrimSpace(c.Ref) != "" {
			return fmt.Errorf("managed JSON catalog %q must not set url or ref", name)
		}
		if strings.TrimSpace(c.ManagedFile) == "" {
			return fmt.Errorf("managed JSON catalog %q requires managedFile", name)
		}
		managedClean := filepath.ToSlash(filepath.Clean(c.ManagedFile))
		if filepath.IsAbs(c.ManagedFile) || managedClean == "." || managedClean == "" || strings.HasPrefix(managedClean, "../") || managedClean == ".." {
			return fmt.Errorf("managed JSON catalog %q managedFile must be relative", name)
		}
		if strings.TrimSpace(c.File) != "" {
			return fmt.Errorf("managed JSON catalog %q must not set file; managedFile is the canonical source path", name)
		}
	default:
		return fmt.Errorf("model catalog %q has unsupported type %q", name, c.Type)
	}
	if typ != ModelCatalogTypeGit {
		return nil
	}
	cleanFile := filepath.ToSlash(filepath.Clean(c.File))
	parts := strings.Split(cleanFile, "/")
	for _, part := range parts {
		if part == ".." {
			return fmt.Errorf("model catalog %q file must be repository-relative", name)
		}
	}
	if filepath.IsAbs(c.File) || cleanFile == "." || cleanFile == "" {
		return fmt.Errorf("model catalog %q file must be repository-relative", name)
	}
	return nil
}

func (b ModelProviderBinding) Validate(provider string, catalogs map[string]ModelCatalog) error {
	if err := ValidateModelProviderID(provider); err != nil {
		return err
	}
	if catalog := strings.TrimSpace(b.Catalog); catalog != "" {
		if _, ok := findModelCatalog(catalogs, catalog); !ok {
			return fmt.Errorf("model provider %q references missing catalog %q", provider, catalog)
		}
	}
	if ref := strings.TrimSpace(b.SecretRef); ref != "" && strings.ContainsAny(ref, "\r\n") {
		return fmt.Errorf("model provider %q secretRef must be a single line", provider)
	}
	for interfaceName, ref := range b.InterfaceSecrets {
		if strings.TrimSpace(interfaceName) == "" || strings.ContainsAny(interfaceName, "\r\n\t ") {
			return fmt.Errorf("model provider %q has invalid interface secret name %q", provider, interfaceName)
		}
		if strings.TrimSpace(ref) == "" || strings.ContainsAny(ref, "\r\n") {
			return fmt.Errorf("model provider %q interface %q secretRef must be a single non-empty line", provider, interfaceName)
		}
	}
	if b.Enabled && strings.TrimSpace(b.SecretRef) == "" && len(b.InterfaceSecrets) == 0 {
		return fmt.Errorf("model provider %q cannot be enabled without secretRef", provider)
	}
	return nil
}

func findModelCatalog(catalogs map[string]ModelCatalog, ref string) (ModelCatalog, bool) {
	for name, catalog := range catalogs {
		if strings.EqualFold(strings.TrimSpace(name), strings.TrimSpace(ref)) {
			return catalog, true
		}
	}
	return ModelCatalog{}, false
}
