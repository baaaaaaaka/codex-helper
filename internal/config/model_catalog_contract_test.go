package config

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestModelCatalogContractValidatesEverySourceShape(t *testing.T) {
	tests := []struct {
		name string
		cat  ModelCatalog
	}{
		{
			name: "git https",
			cat:  ModelCatalog{Type: ModelCatalogTypeGit, URL: "https://example.invalid/models.git", Ref: "main", File: "catalog.json", AutoSync: true},
		},
		{
			name: "git ssh scp",
			cat:  ModelCatalog{Type: ModelCatalogTypeGit, URL: "git@example.invalid:team/models.git", File: "catalog.json"},
		},
		{
			name: "git local",
			cat:  ModelCatalog{Type: ModelCatalogTypeGit, URL: "../models", File: "nested/catalog.json"},
		},
		{
			name: "managed json",
			cat:  ModelCatalog{Type: ModelCatalogTypeManagedJSON, ManagedFile: "model-catalogs/test.json"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cat.Validate("catalog"); err != nil {
				t.Fatalf("Validate() = %v", err)
			}
		})
	}
}

func TestModelCatalogContractRejectsInvalidSourceCombinations(t *testing.T) {
	baseGit := ModelCatalog{Type: ModelCatalogTypeGit, URL: "https://example.invalid/models.git", File: "catalog.json"}
	baseManaged := ModelCatalog{Type: ModelCatalogTypeManagedJSON, ManagedFile: "model-catalogs/test.json"}
	tests := []struct {
		name string
		cat  ModelCatalog
		want string
	}{
		{"unknown type", ModelCatalog{Type: "json", ManagedFile: "catalog.json"}, "unsupported type"},
		{"git missing file", ModelCatalog{Type: ModelCatalogTypeGit, URL: baseGit.URL}, "requires file"},
		{"git managed file", ModelCatalog{Type: ModelCatalogTypeGit, URL: baseGit.URL, File: baseGit.File, ManagedFile: "catalog.json"}, "must not set managedFile"},
		{"git embedded credentials", ModelCatalog{Type: ModelCatalogTypeGit, URL: "https://user:secret@example.invalid/models.git", File: "catalog.json"}, "credentials"},
		{"git absolute manifest", ModelCatalog{Type: ModelCatalogTypeGit, URL: baseGit.URL, File: "/tmp/catalog.json"}, "repository-relative"},
		{"managed url", ModelCatalog{Type: ModelCatalogTypeManagedJSON, URL: baseGit.URL, ManagedFile: baseManaged.ManagedFile}, "must not set url"},
		{"managed ref", ModelCatalog{Type: ModelCatalogTypeManagedJSON, Ref: "main", ManagedFile: baseManaged.ManagedFile}, "must not set url or ref"},
		{"managed file field", ModelCatalog{Type: ModelCatalogTypeManagedJSON, File: "catalog.json", ManagedFile: baseManaged.ManagedFile}, "must not set file"},
		{"managed absolute path", ModelCatalog{Type: ModelCatalogTypeManagedJSON, ManagedFile: "/tmp/catalog.json"}, "must be relative"},
		{"managed traversal", ModelCatalog{Type: ModelCatalogTypeManagedJSON, ManagedFile: "../catalog.json"}, "must be relative"},
		{"uppercase type", ModelCatalog{Type: "GIT", URL: baseGit.URL, File: baseGit.File}, "must be lowercase"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.cat.Validate("catalog"); err == nil || !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.want)) {
				t.Fatalf("Validate() = %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestModelProviderBindingContractValidatesCredentialScopes(t *testing.T) {
	catalogs := map[string]ModelCatalog{
		"hub": {Type: ModelCatalogTypeManagedJSON, ManagedFile: "model-catalogs/hub.json"},
	}
	tests := []struct {
		name string
		bind ModelProviderBinding
		want string
	}{
		{name: "shared secret", bind: ModelProviderBinding{Catalog: "HUB", SecretRef: "env:HUB_KEY"}},
		{name: "interface secrets", bind: ModelProviderBinding{Catalog: "hub", InterfaceSecrets: map[string]string{"anthropic": "env:ANTHROPIC_KEY", "beta": "file:beta"}}},
		{name: "disabled without secret", bind: ModelProviderBinding{Catalog: "hub"}},
		{name: "enabled with shared secret", bind: ModelProviderBinding{Catalog: "hub", SecretRef: "env:HUB_KEY", Enabled: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.bind.Validate("deepseek", catalogs); err != nil {
				t.Fatalf("Validate() = %v", err)
			}
		})
	}
	invalid := []struct {
		name string
		bind ModelProviderBinding
		want string
	}{
		{"missing catalog", ModelProviderBinding{Catalog: "missing"}, "missing catalog"},
		{"secret newline", ModelProviderBinding{SecretRef: "env:KEY\nleak"}, "single line"},
		{"interface whitespace", ModelProviderBinding{InterfaceSecrets: map[string]string{"bad name": "env:KEY"}}, "invalid interface"},
		{"interface empty ref", ModelProviderBinding{InterfaceSecrets: map[string]string{"chat": ""}}, "single non-empty"},
		{"enabled without secret", ModelProviderBinding{Enabled: true}, "without secretRef"},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.bind.Validate("deepseek", catalogs); err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Validate() = %v, want error containing %q", err, tt.want)
			}
		})
	}
}

func TestModelCatalogContractFailedUpdateDoesNotWriteConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	store, err := NewStore(path)
	if err != nil {
		t.Fatal(err)
	}
	initial := Config{
		Version:            CurrentVersion,
		Profiles:           []Profile{},
		ModelConfigVersion: CurrentModelConfigVersion,
		ModelCatalogs: map[string]ModelCatalog{
			"hub": {Type: ModelCatalogTypeManagedJSON, ManagedFile: "model-catalogs/hub.json"},
		},
	}
	if err := store.Save(initial); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	err = store.Update(func(cfg *Config) error {
		cfg.ModelCatalogs["new"] = ModelCatalog{Type: ModelCatalogTypeManagedJSON, ManagedFile: "model-catalogs/new.json"}
		return errors.New("synthetic validation failure")
	})
	if err == nil || !strings.Contains(err.Error(), "synthetic validation failure") {
		t.Fatalf("Update() = %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatalf("failed catalog update changed config:\nbefore=%s\nafter=%s", before, after)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := loaded.ModelCatalogs["new"]; ok {
		t.Fatal("failed catalog update left new catalog in durable config")
	}
}
