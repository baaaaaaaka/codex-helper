package config

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const CurrentCatalogSchemaVersion = 2

// CatalogManifest is the small index committed by a model subscription. The
// referenced files are the source of truth; the manifest never contains
// credentials or machine-local verification state.
type CatalogManifest struct {
	SchemaVersion int                  `json:"schemaVersion"`
	Kind          string               `json:"kind"`
	Providers     []CatalogProviderRef `json:"providers"`
	Models        []CatalogModelRef    `json:"models"`
	Digest        string               `json:"digest,omitempty"`
}

type CatalogProviderRef struct {
	ID   string `json:"id"`
	File string `json:"file"`
}

type CatalogModelRef struct {
	Provider string `json:"provider"`
	ID       string `json:"id"`
	File     string `json:"file"`
}

type CatalogProviderDocument struct {
	SchemaVersion int                         `json:"schemaVersion"`
	Kind          string                      `json:"kind"`
	ID            string                      `json:"id"`
	Interfaces    map[string]CatalogInterface `json:"interfaces"`
}

type CatalogAuth struct {
	Type   string `json:"type,omitempty"`
	Header string `json:"header,omitempty"`
}

type CatalogInterface struct {
	Adapter       string            `json:"adapter"`
	Protocol      string            `json:"protocol"`
	BaseURL       string            `json:"baseUrl"`
	Auth          CatalogAuth       `json:"auth,omitempty"`
	CredentialRef string            `json:"credentialRef,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	Endpoints     map[string]string `json:"endpoints,omitempty"`
	HTTP          ModelHTTPPolicy   `json:"http,omitempty"`
	Stream        ModelStreamPolicy `json:"stream,omitempty"`
	SSHProxy      string            `json:"sshProxy,omitempty"`
}

type CatalogModelDocument struct {
	SchemaVersion int                    `json:"schemaVersion"`
	Kind          string                 `json:"kind"`
	Provider      string                 `json:"provider"`
	ID            string                 `json:"id"`
	UpstreamModel string                 `json:"upstreamModel"`
	DisplayName   string                 `json:"displayName,omitempty"`
	Aliases       []string               `json:"aliases,omitempty"`
	Description   string                 `json:"description,omitempty"`
	Priority      int                    `json:"priority,omitempty"`
	Capabilities  CatalogCapabilities    `json:"capabilities,omitempty"`
	Limits        ModelLimits            `json:"limits,omitempty"`
	Reasoning     CatalogReasoningPolicy `json:"reasoning,omitempty"`
	Tools         ModelToolPolicy        `json:"tools,omitempty"`
	Messages      ModelMessagePolicy     `json:"messages,omitempty"`
	Sampling      ModelSamplingPolicy    `json:"sampling,omitempty"`
	Stream        ModelStreamPolicy      `json:"stream,omitempty"`
	HTTP          ModelHTTPPolicy        `json:"http,omitempty"`
	Cache         ModelCachePolicy       `json:"cache,omitempty"`
	Responses     ModelResponsesPolicy   `json:"responses,omitempty"`
	Routes        map[string]ModelRoute  `json:"routes,omitempty"`
	Search        ModelSearchPolicy      `json:"search,omitempty"`
}

// CatalogCapabilities uses explicit modes instead of the legacy bool fields.
// Every field is required by the schema-v2 loader; use "unknown" when the
// provider's behavior has not been verified rather than silently inheriting a
// binary default.
type CatalogCapabilities struct {
	Tools            string `json:"tools,omitempty"`
	ParallelTools    string `json:"parallelTools,omitempty"`
	Vision           string `json:"vision,omitempty"`
	Reasoning        string `json:"reasoning,omitempty"`
	ReasoningSummary string `json:"reasoningSummary,omitempty"`
	WebSearch        string `json:"webSearch,omitempty"`
}

type CatalogReasoningPolicy struct {
	Efforts                  []string          `json:"efforts,omitempty"`
	Default                  string            `json:"default,omitempty"`
	Map                      map[string]string `json:"map,omitempty"`
	ThinkingMode             string            `json:"thinkingMode,omitempty"`
	EnabledRequest           map[string]any    `json:"enabledRequest,omitempty"`
	DisabledRequest          map[string]any    `json:"disabledRequest,omitempty"`
	StripSamplingWhenEnabled *bool             `json:"stripSamplingWhenEnabled,omitempty"`
	HistoryPolicy            string            `json:"historyPolicy,omitempty"`
	ResponseField            string            `json:"responseField,omitempty"`
}

type CatalogSnapshot struct {
	Config      Config
	Digest      string
	ProviderIDs []string
	ModelIDs    []string
}

// ParseCatalogV2 reads and validates a manifest plus all referenced files.
// It deliberately returns a normal Config snapshot so existing launchers can
// consume the new source format while the persisted source remains split.
func ParseCatalogV2(root string) (CatalogSnapshot, error) {
	root = filepath.Clean(strings.TrimSpace(root))
	if root == "" || root == "." {
		return CatalogSnapshot{}, fmt.Errorf("catalog root is required")
	}
	manifestPath := filepath.Join(root, "manifest.json")
	manifestRaw, err := os.ReadFile(manifestPath)
	if err != nil {
		return CatalogSnapshot{}, fmt.Errorf("read catalog manifest: %w", err)
	}
	var manifest CatalogManifest
	if err := decodeCatalogJSON(manifestRaw, &manifest); err != nil {
		return CatalogSnapshot{}, fmt.Errorf("parse catalog manifest: %w", err)
	}
	if manifest.SchemaVersion != CurrentCatalogSchemaVersion || strings.ToLower(strings.TrimSpace(manifest.Kind)) != "catalog" {
		return CatalogSnapshot{}, fmt.Errorf("unsupported catalog manifest schema %d/%q", manifest.SchemaVersion, manifest.Kind)
	}
	if len(manifest.Providers) == 0 || len(manifest.Models) == 0 {
		return CatalogSnapshot{}, fmt.Errorf("catalog manifest must list at least one provider and model")
	}

	providers := make(map[string]ModelProvider, len(manifest.Providers))
	credentials := make(map[string]ModelCredential, len(manifest.Providers))
	providerInterfaces := make(map[string]string, len(manifest.Providers))
	interfaceContracts := make(map[string]CatalogInterface, len(manifest.Providers))
	providerIDs := make([]string, 0, len(manifest.Providers))
	seenReferencedFiles := map[string]string{}
	for _, ref := range manifest.Providers {
		id := strings.TrimSpace(ref.ID)
		if id == "" || strings.TrimSpace(ref.File) == "" {
			return CatalogSnapshot{}, fmt.Errorf("catalog provider reference requires id and file")
		}
		key := strings.ToLower(id)
		if _, exists := providers[key]; exists {
			return CatalogSnapshot{}, fmt.Errorf("duplicate catalog provider %q", id)
		}
		path, err := safeCatalogPath(root, ref.File)
		if err != nil {
			return CatalogSnapshot{}, fmt.Errorf("provider %q: %w", id, err)
		}
		if previous, exists := seenReferencedFiles[path]; exists {
			return CatalogSnapshot{}, fmt.Errorf("catalog provider file %q is referenced more than once (also used by %s)", ref.File, previous)
		}
		seenReferencedFiles[path] = "provider " + id
		raw, err := os.ReadFile(path)
		if err != nil {
			return CatalogSnapshot{}, fmt.Errorf("read provider %q: %w", id, err)
		}
		var doc CatalogProviderDocument
		if err := decodeCatalogJSON(raw, &doc); err != nil {
			return CatalogSnapshot{}, fmt.Errorf("provider %q: %w", id, err)
		}
		if doc.SchemaVersion != CurrentCatalogSchemaVersion || strings.ToLower(strings.TrimSpace(doc.Kind)) != "provider" || !strings.EqualFold(doc.ID, id) {
			return CatalogSnapshot{}, fmt.Errorf("provider %q has mismatched schema, kind, or id", id)
		}
		if len(doc.Interfaces) != 1 {
			return CatalogSnapshot{}, fmt.Errorf("provider %q must expose exactly one interface until operation-aware dispatch is enabled", id)
		}
		interfaceNames := make([]string, 0, len(doc.Interfaces))
		for name := range doc.Interfaces {
			interfaceNames = append(interfaceNames, name)
		}
		sort.Strings(interfaceNames)
		interfaceName := interfaceNames[0]
		iface := doc.Interfaces[interfaceName]
		if err := validateCatalogInterface(id, interfaceName, iface); err != nil {
			return CatalogSnapshot{}, err
		}
		provider := ModelProvider{
			Protocol: iface.Protocol, BaseURL: iface.BaseURL, Headers: cloneCatalogStringMap(iface.Headers), Endpoints: cloneCatalogStringMap(iface.Endpoints),
			HTTP: iface.HTTP, Stream: iface.Stream, SSHProxy: strings.TrimSpace(iface.SSHProxy),
		}
		credentialRef := strings.TrimSpace(iface.CredentialRef)
		if credentialRef != "" {
			if err := validateCredentialRef(credentialRef); err != nil {
				return CatalogSnapshot{}, fmt.Errorf("provider %q: %w", id, err)
			}
			provider.Credential = credentialRef
			candidate := ModelCredential{AuthType: strings.ToLower(firstNonEmptyString(iface.Auth.Type, "bearer")), Header: strings.TrimSpace(iface.Auth.Header), Pending: true}
			if previous, exists := credentials[credentialRef]; exists && (previous.AuthType != candidate.AuthType || previous.Header != candidate.Header) {
				return CatalogSnapshot{}, fmt.Errorf("credentialRef %q is reused with incompatible auth settings", credentialRef)
			}
			credentials[credentialRef] = candidate
		}
		providers[key] = provider
		providerInterfaces[key] = interfaceName
		interfaceContracts[key] = iface
		providerIDs = append(providerIDs, id)
	}

	models := make(map[string]ModelDefinition, len(manifest.Models))
	profiles := make(map[string]ModelProfile, len(manifest.Models))
	modelIDs := make([]string, 0, len(manifest.Models))
	seenAliases := map[string]string{}
	for _, ref := range manifest.Models {
		providerID := strings.TrimSpace(ref.Provider)
		modelID := strings.TrimSpace(ref.ID)
		if providerID == "" || modelID == "" || strings.TrimSpace(ref.File) == "" {
			return CatalogSnapshot{}, fmt.Errorf("catalog model reference requires provider, id, and file")
		}
		providerKey := strings.ToLower(providerID)
		if _, exists := providers[providerKey]; !exists {
			return CatalogSnapshot{}, fmt.Errorf("model %q references missing provider %q", modelID, providerID)
		}
		path, err := safeCatalogPath(root, ref.File)
		if err != nil {
			return CatalogSnapshot{}, fmt.Errorf("model %s/%s: %w", providerID, modelID, err)
		}
		if previous, exists := seenReferencedFiles[path]; exists {
			return CatalogSnapshot{}, fmt.Errorf("catalog model file %q is referenced more than once (also used by %s)", ref.File, previous)
		}
		seenReferencedFiles[path] = "model " + providerID + "/" + modelID
		raw, err := os.ReadFile(path)
		if err != nil {
			return CatalogSnapshot{}, fmt.Errorf("read model %s/%s: %w", providerID, modelID, err)
		}
		var doc CatalogModelDocument
		if err := decodeCatalogJSON(raw, &doc); err != nil {
			return CatalogSnapshot{}, fmt.Errorf("model %s/%s: %w", providerID, modelID, err)
		}
		if doc.SchemaVersion != CurrentCatalogSchemaVersion || strings.ToLower(strings.TrimSpace(doc.Kind)) != "model" || !strings.EqualFold(doc.Provider, providerID) || !strings.EqualFold(doc.ID, modelID) {
			return CatalogSnapshot{}, fmt.Errorf("model %s/%s has mismatched schema, kind, provider, or id", providerID, modelID)
		}
		if strings.TrimSpace(doc.UpstreamModel) == "" {
			return CatalogSnapshot{}, fmt.Errorf("model %s/%s requires upstreamModel", providerID, modelID)
		}
		definition, err := catalogModelDefinition(providerID, doc)
		if err != nil {
			return CatalogSnapshot{}, fmt.Errorf("model %s/%s: %w", providerID, modelID, err)
		}
		interfaceName := providerInterfaces[providerKey]
		interfaceContract := interfaceContracts[providerKey]
		if len(definition.Routes) == 0 {
			return CatalogSnapshot{}, fmt.Errorf("model %s/%s must declare at least one route", providerID, modelID)
		}
		for operation, route := range definition.Routes {
			if !strings.EqualFold(strings.TrimSpace(operation), "responses") {
				return CatalogSnapshot{}, fmt.Errorf("model %s/%s route %q requires an operation-aware dispatcher that this runtime does not provide", providerID, modelID, operation)
			}
			if !strings.EqualFold(strings.TrimSpace(route.Interface), interfaceName) {
				return CatalogSnapshot{}, fmt.Errorf("model %s/%s route %q references interface %q, want %q", providerID, modelID, operation, route.Interface, interfaceName)
			}
			if !strings.EqualFold(strings.TrimSpace(route.Adapter), strings.TrimSpace(interfaceContract.Adapter)) || !strings.EqualFold(strings.TrimSpace(route.Protocol), strings.TrimSpace(interfaceContract.Protocol)) {
				return CatalogSnapshot{}, fmt.Errorf("model %s/%s route %q does not match provider interface %q", providerID, modelID, operation, interfaceName)
			}
			if err := validateCatalogRoute(providerID, modelID, operation, route); err != nil {
				return CatalogSnapshot{}, err
			}
		}
		qualified := QualifiedModelID(providerID, modelID)
		if _, exists := models[strings.ToLower(qualified)]; exists {
			return CatalogSnapshot{}, fmt.Errorf("duplicate catalog model %q", qualified)
		}
		for _, alias := range append([]string{modelID}, definition.Aliases...) {
			aliasKey := strings.ToLower(strings.TrimSpace(alias))
			if aliasKey == "" {
				continue
			}
			scopeKey := providerKey + "\x00" + aliasKey
			if previous, exists := seenAliases[scopeKey]; exists && !strings.EqualFold(previous, qualified) {
				return CatalogSnapshot{}, fmt.Errorf("alias %q conflicts within provider %q between %q and %q", alias, providerID, previous, qualified)
			}
			seenAliases[scopeKey] = qualified
		}
		models[strings.ToLower(qualified)] = definition
		credentialRef := strings.TrimSpace(providers[providerKey].Credential)
		profiles[qualified] = ModelProfile{
			Provider: definition.Provider, Model: qualified, Credential: credentialRef, Revision: 1,
		}
		modelIDs = append(modelIDs, qualified)
	}

	cfg := Config{ModelConfigVersion: 1, ModelCredentials: credentials, ModelProviders: providers, Models: models, ModelProfiles: profiles}
	if err := ValidateModelConfig(cfg); err != nil {
		return CatalogSnapshot{}, fmt.Errorf("validate catalog snapshot: %w", err)
	}
	digest, err := catalogContentDigest(root, manifest, manifestRaw)
	if err != nil {
		return CatalogSnapshot{}, err
	}
	if expected := strings.TrimSpace(manifest.Digest); expected != "" && !strings.EqualFold(expected, digest) {
		return CatalogSnapshot{}, fmt.Errorf("catalog digest mismatch: manifest=%s actual=%s", expected, digest)
	}
	sort.Strings(providerIDs)
	sort.Strings(modelIDs)
	return CatalogSnapshot{Config: cfg, Digest: digest, ProviderIDs: providerIDs, ModelIDs: modelIDs}, nil
}

func decodeCatalogJSON(raw []byte, out any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON")
		}
		return err
	}
	return nil
}

func safeCatalogPath(root, name string) (string, error) {
	name = filepath.Clean(strings.TrimSpace(name))
	if name == "." || filepath.IsAbs(name) || name == ".." || strings.HasPrefix(name, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("catalog file must be repository-relative")
	}
	path := filepath.Join(root, name)
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	realPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(realRoot, realPath)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("catalog file escapes repository")
	}
	return realPath, nil
}

func validateCatalogInterface(providerID, name string, iface CatalogInterface) error {
	if strings.TrimSpace(iface.Adapter) == "" || strings.TrimSpace(iface.Protocol) == "" || strings.TrimSpace(iface.BaseURL) == "" {
		return fmt.Errorf("provider %q interface %q requires adapter, protocol, and baseUrl", providerID, name)
	}
	switch strings.ToLower(strings.TrimSpace(iface.Protocol)) {
	case "responses", "chat-completions", "anthropic", "beta", "fim":
	default:
		return fmt.Errorf("provider %q interface %q has invalid protocol %q", providerID, name, iface.Protocol)
	}
	parsed, err := url.Parse(strings.TrimSpace(iface.BaseURL))
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil {
		return fmt.Errorf("provider %q interface %q requires an absolute credential-free http(s) baseUrl", providerID, name)
	}
	if err := validateHTTPPolicy("provider "+providerID+" interface "+name, iface.HTTP); err != nil {
		return err
	}
	if err := validateStreamPolicy("provider "+providerID+" interface "+name, iface.Stream); err != nil {
		return err
	}
	for header, value := range iface.Headers {
		lower := strings.ToLower(strings.TrimSpace(header))
		if lower == "authorization" || lower == "api-key" || lower == "x-api-key" || strings.Contains(lower, "token") || strings.Contains(lower, "secret") {
			return fmt.Errorf("provider %q interface %q must not contain credential header %q", providerID, name, header)
		}
		if looksLikeSecret(value) {
			return fmt.Errorf("provider %q interface %q contains secret-like header value", providerID, name)
		}
	}
	if iface.CredentialRef != "" {
		if err := validateCredentialRef(iface.CredentialRef); err != nil {
			return fmt.Errorf("provider %q interface %q: %w", providerID, name, err)
		}
	}
	if err := validateCatalogAuth(providerID, name, iface.Auth, iface.CredentialRef); err != nil {
		return err
	}
	return nil
}

func validateCatalogAuth(providerID, interfaceName string, auth CatalogAuth, credentialRef string) error {
	typeName := strings.ToLower(strings.TrimSpace(auth.Type))
	switch typeName {
	case "", "bearer":
	case "header":
		if strings.TrimSpace(auth.Header) == "" {
			return fmt.Errorf("provider %q interface %q header auth requires auth.header", providerID, interfaceName)
		}
		if strings.ContainsAny(auth.Header, "\r\n") || strings.TrimSpace(auth.Header) != auth.Header {
			return fmt.Errorf("provider %q interface %q has invalid auth.header", providerID, interfaceName)
		}
	default:
		return fmt.Errorf("provider %q interface %q has invalid auth.type %q", providerID, interfaceName, auth.Type)
	}
	if strings.TrimSpace(credentialRef) == "" && (typeName != "" || strings.TrimSpace(auth.Header) != "") {
		return fmt.Errorf("provider %q interface %q declares auth without credentialRef", providerID, interfaceName)
	}
	return nil
}

func validateCatalogRoute(providerID, modelID, operation string, route ModelRoute) error {
	protocol := strings.ToLower(strings.TrimSpace(route.Protocol))
	adapter := strings.ToLower(strings.TrimSpace(route.Adapter))
	if !strings.EqualFold(strings.TrimSpace(operation), "responses") {
		return fmt.Errorf("model %s/%s route %q is unsupported", providerID, modelID, operation)
	}
	switch {
	case protocol == "chat-completions" && adapter == "openai-chat":
		return nil
	case protocol == "responses" && adapter == "openai-responses":
		return nil
	default:
		return fmt.Errorf("model %s/%s route %q uses unsupported adapter/protocol %q/%q", providerID, modelID, operation, route.Adapter, route.Protocol)
	}
}

func validateCredentialRef(ref string) error {
	ref = strings.TrimSpace(ref)
	if ref == "" || len(ref) > 128 || strings.ContainsAny(ref, " \t\r\n") || strings.Contains(ref, "//") {
		return fmt.Errorf("credentialRef %q is invalid", ref)
	}
	if looksLikeSecret(ref) || strings.Contains(strings.ToLower(ref), "bearer ") {
		return fmt.Errorf("credentialRef must be a symbolic reference, not a token")
	}
	return nil
}

func looksLikeSecret(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	lower := strings.ToLower(value)
	return strings.HasPrefix(lower, "sk-") || strings.HasPrefix(lower, "bearer ") || len(value) >= 32 && !strings.ContainsAny(value, " :/{}_[]")
}

func catalogModelDefinition(providerID string, doc CatalogModelDocument) (ModelDefinition, error) {
	if err := requireCatalogCapabilities(doc.Capabilities); err != nil {
		return ModelDefinition{}, err
	}
	toolsMode, err := catalogCapabilityMode(doc.Capabilities.Tools)
	if err != nil {
		return ModelDefinition{}, fmt.Errorf("capabilities.tools: %w", err)
	}
	parallelToolsMode, err := catalogCapabilityMode(doc.Capabilities.ParallelTools)
	if err != nil {
		return ModelDefinition{}, fmt.Errorf("capabilities.parallelTools: %w", err)
	}
	visionMode, err := catalogCapabilityMode(doc.Capabilities.Vision)
	if err != nil {
		return ModelDefinition{}, fmt.Errorf("capabilities.vision: %w", err)
	}
	reasoningMode, err := catalogCapabilityMode(doc.Capabilities.Reasoning)
	if err != nil {
		return ModelDefinition{}, fmt.Errorf("capabilities.reasoning: %w", err)
	}
	reasoningSummaryMode, err := catalogCapabilityMode(doc.Capabilities.ReasoningSummary)
	if err != nil {
		return ModelDefinition{}, fmt.Errorf("capabilities.reasoningSummary: %w", err)
	}
	webSearchMode, err := catalogWebSearchMode(doc.Capabilities.WebSearch)
	if err != nil {
		return ModelDefinition{}, fmt.Errorf("capabilities.webSearch: %w", err)
	}
	tools, err := catalogCapabilityBool(toolsMode)
	if err != nil {
		return ModelDefinition{}, fmt.Errorf("capabilities.tools: %w", err)
	}
	parallelTools, err := catalogCapabilityBool(parallelToolsMode)
	if err != nil {
		return ModelDefinition{}, fmt.Errorf("capabilities.parallelTools: %w", err)
	}
	vision, err := catalogCapabilityBool(visionMode)
	if err != nil {
		return ModelDefinition{}, fmt.Errorf("capabilities.vision: %w", err)
	}
	reasoning, err := catalogCapabilityBool(reasoningMode)
	if err != nil {
		return ModelDefinition{}, fmt.Errorf("capabilities.reasoning: %w", err)
	}
	reasoningSummary, err := catalogCapabilityBool(reasoningSummaryMode)
	if err != nil {
		return ModelDefinition{}, fmt.Errorf("capabilities.reasoningSummary: %w", err)
	}
	nativeSearch, err := catalogWebSearchBool(webSearchMode)
	if err != nil {
		return ModelDefinition{}, fmt.Errorf("capabilities.webSearch: %w", err)
	}
	definition := ModelDefinition{
		Provider: providerID, UpstreamModel: strings.TrimSpace(doc.UpstreamModel), DisplayName: strings.TrimSpace(doc.DisplayName),
		Aliases: append([]string(nil), doc.Aliases...), Description: strings.TrimSpace(doc.Description), Priority: doc.Priority,
		Capabilities:    ModelCapabilities{Tools: tools, ParallelTools: parallelTools, Vision: vision, Reasoning: reasoning, ReasoningSummary: reasoningSummary, NativeWebSearch: nativeSearch},
		CapabilityModes: ModelCapabilityModes{Tools: toolsMode, ParallelTools: parallelToolsMode, Vision: visionMode, Reasoning: reasoningMode, ReasoningSummary: reasoningSummaryMode, WebSearch: webSearchMode},
		Limits:          doc.Limits,
		Reasoning:       ModelReasoningPolicy{SupportedEfforts: append([]string(nil), doc.Reasoning.Efforts...), DefaultEffort: strings.TrimSpace(doc.Reasoning.Default), EffortMap: cloneCatalogStringMap(doc.Reasoning.Map), ThinkingMode: doc.Reasoning.ThinkingMode, EnabledRequest: cloneAnyMap(doc.Reasoning.EnabledRequest), DisabledRequest: cloneAnyMap(doc.Reasoning.DisabledRequest), StripSamplingWhenEnabled: doc.Reasoning.StripSamplingWhenEnabled, HistoryPolicy: doc.Reasoning.HistoryPolicy, ResponseField: doc.Reasoning.ResponseField},
		Tools:           doc.Tools, Messages: doc.Messages, Sampling: doc.Sampling, Stream: doc.Stream, HTTP: doc.HTTP, Cache: doc.Cache, Responses: doc.Responses, Routes: cloneRoutes(doc.Routes), Search: doc.Search,
	}
	if doc.Capabilities.WebSearch != "" {
		definition.Search.Native = nativeSearch
	}
	return definition, validateModelDefinition(QualifiedModelID(providerID, doc.ID), definition)
}

func requireCatalogCapabilities(capabilities CatalogCapabilities) error {
	for _, declaration := range []struct {
		name  string
		value string
	}{
		{name: "tools", value: capabilities.Tools},
		{name: "parallelTools", value: capabilities.ParallelTools},
		{name: "vision", value: capabilities.Vision},
		{name: "reasoning", value: capabilities.Reasoning},
		{name: "reasoningSummary", value: capabilities.ReasoningSummary},
		{name: "webSearch", value: capabilities.WebSearch},
	} {
		if strings.TrimSpace(declaration.value) == "" {
			return fmt.Errorf("capabilities.%s must be explicitly declared (use unknown when unverified)", declaration.name)
		}
	}
	return nil
}

func catalogCapabilityMode(mode string) (string, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	switch mode {
	case "native", "translated", "plugin", "advisory", "unsupported", "unknown":
		return mode, nil
	default:
		return "", fmt.Errorf("invalid mode %q", mode)
	}
}

func catalogWebSearchMode(mode string) (string, error) {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "fallback" {
		return mode, nil
	}
	return catalogCapabilityMode(mode)
}

func catalogCapabilityBool(mode string) (*bool, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "unknown":
		return nil, nil
	case "native", "translated", "plugin", "advisory":
		value := true
		return &value, nil
	case "unsupported":
		value := false
		return &value, nil
	default:
		return nil, fmt.Errorf("invalid mode %q", mode)
	}
}

func catalogWebSearchBool(mode string) (*bool, error) {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", "unknown":
		return nil, nil
	case "native":
		value := true
		return &value, nil
	case "translated", "plugin", "fallback", "advisory", "unsupported":
		value := false
		return &value, nil
	default:
		return nil, fmt.Errorf("invalid mode %q", mode)
	}
}

func catalogContentDigest(root string, manifest CatalogManifest, manifestRaw []byte) (string, error) {
	paths := make([]string, 0, len(manifest.Providers)+len(manifest.Models))
	for _, ref := range manifest.Providers {
		paths = append(paths, filepath.Clean(ref.File))
	}
	for _, ref := range manifest.Models {
		paths = append(paths, filepath.Clean(ref.File))
	}
	sort.Strings(paths)
	hash := sha256.New()
	for _, rel := range paths {
		path, err := safeCatalogPath(root, rel)
		if err != nil {
			return "", err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return "", err
		}
		_, _ = io.WriteString(hash, rel)
		_, _ = hash.Write([]byte{0})
		_, _ = hash.Write(raw)
		_, _ = hash.Write([]byte{0})
	}
	_ = manifestRaw // retained in the signature to keep future digest policy explicit
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func cloneRoutes(in map[string]ModelRoute) map[string]ModelRoute {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]ModelRoute, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneCatalogStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func cloneAnyMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
