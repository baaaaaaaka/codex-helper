// Package modelcatalog contains the provider/model JSON contract used by Git
// subscriptions and manually managed catalog files. Catalogs contain public
// endpoint metadata only; credentials are supplied by local bindings.
package modelcatalog

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/config"
)

// Catalog v2 intentionally has no compatibility reader. There are no users
// of the external catalog format yet, so a malformed or old document must be
// rejected instead of being silently projected into a partial runtime route.
const CurrentVersion = 2

type Document struct {
	CatalogVersion int                 `json:"catalogVersion"`
	Providers      map[string]Provider `json:"providers"`
}

// Provider owns named upstream interfaces. A model selects the interface for
// each feature; this represents providers such as DeepSeek that expose Chat,
// Anthropic, and Beta/FIM surfaces for the same model family.
type Provider struct {
	DefaultInterface string               `json:"defaultInterface,omitempty"`
	Interfaces       map[string]Interface `json:"interfaces"`
	Models           map[string]Model     `json:"models"`
}

type Interface struct {
	Adapter    string                   `json:"adapter"`
	Protocol   string                   `json:"protocol,omitempty"`
	BaseURL    string                   `json:"baseUrl"`
	Conversion config.ModelConversion   `json:"conversion,omitempty"`
	Auth       Auth                     `json:"auth,omitempty"`
	Headers    map[string]string        `json:"headers,omitempty"`
	Endpoints  map[string]string        `json:"endpoints,omitempty"`
	HTTP       config.ModelHTTPPolicy   `json:"http,omitempty"`
	Stream     config.ModelStreamPolicy `json:"stream,omitempty"`
}

// Auth describes how a local credential is projected into an interface.
// It intentionally has no secret value.
type Auth struct {
	Type   string `json:"type,omitempty"`
	Header string `json:"header,omitempty"`
}

type Model struct {
	UpstreamModel    string                      `json:"upstreamModel"`
	DisplayName      string                      `json:"displayName,omitempty"`
	Aliases          []string                    `json:"aliases,omitempty"`
	Description      string                      `json:"description,omitempty"`
	Priority         int                         `json:"priority,omitempty"`
	DefaultInterface string                      `json:"defaultInterface,omitempty"`
	Features         map[string]Feature          `json:"features,omitempty"`
	Limits           config.ModelLimits          `json:"limits,omitempty"`
	Reasoning        config.ModelReasoningPolicy `json:"reasoning,omitempty"`
	Tools            config.ModelToolPolicy      `json:"tools,omitempty"`
	Messages         config.ModelMessagePolicy   `json:"messages,omitempty"`
	Sampling         config.ModelSamplingPolicy  `json:"sampling,omitempty"`
	Responses        config.ModelResponsesPolicy `json:"responses,omitempty"`
	Stream           config.ModelStreamPolicy    `json:"stream,omitempty"`
	HTTP             config.ModelHTTPPolicy      `json:"http,omitempty"`
	Cache            config.ModelCachePolicy     `json:"cache,omitempty"`
}

type Feature struct {
	Support        string                    `json:"support"`
	Interface      string                    `json:"interface,omitempty"`
	Operation      string                    `json:"operation,omitempty"`
	Fallback       *Fallback                 `json:"fallback,omitempty"`
	RequireSources bool                      `json:"requireSources,omitempty"`
	NativeTool     *config.ModelNativeTool   `json:"nativeTool,omitempty"`
	Sources        *config.ModelSourcePolicy `json:"sources,omitempty"`
}

type Fallback struct {
	Selector string   `json:"selector"`
	Effort   string   `json:"effort,omitempty"`
	Tier     string   `json:"tier,omitempty"`
	On       []string `json:"on,omitempty"`
}

// Route is the flattened runtime view of one provider/model selector.
type Route struct {
	ProviderID  string
	ModelID     string
	Selector    string
	InterfaceID string
	Provider    config.ModelProvider
	Model       config.ModelDefinition
	AuthType    string
	AuthHeader  string
	Credential  string
}

func Parse(raw []byte) (Document, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var doc Document
	if err := decoder.Decode(&doc); err != nil {
		return Document{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err == nil {
		return Document{}, fmt.Errorf("catalog contains trailing JSON")
	} else if err != io.EOF {
		return Document{}, err
	}
	if err := doc.Validate(); err != nil {
		return Document{}, err
	}
	return doc, nil
}

func Marshal(doc Document) ([]byte, error) {
	if err := doc.Validate(); err != nil {
		return nil, err
	}
	return json.MarshalIndent(doc, "", "  ")
}

func (d Document) Validate() error {
	if d.CatalogVersion != CurrentVersion {
		return fmt.Errorf("unsupported catalogVersion %d (supported: %d)", d.CatalogVersion, CurrentVersion)
	}
	if len(d.Providers) == 0 {
		return fmt.Errorf("catalog must contain at least one provider")
	}
	names := make([]string, 0, len(d.Providers))
	seen := map[string]string{}
	for name := range d.Providers {
		key := strings.ToLower(strings.TrimSpace(name))
		if previous, ok := seen[key]; ok {
			return fmt.Errorf("provider names %q and %q differ only by case", previous, name)
		}
		seen[key] = name
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if err := validateProvider(name, d.Providers[name]); err != nil {
			return err
		}
	}
	return nil
}

var supportedAdapters = map[string]bool{
	"openai-chat": true, "openai-responses": true,
	"deepseek-openai": true, "deepseek-anthropic": true, "deepseek-beta": true,
	"mimo-chat": true, "mimo-responses": true,
}

var supportedProtocols = map[string]bool{
	"chat-completions": true,
	"responses":        true,
	"messages":         true,
	"anthropic":        true,
	"beta":             true,
	"fim":              true,
}

var supportedFeatures = map[string]bool{
	"chat": true, "responses": true, "streaming": true, "reasoning": true,
	"tools": true, "parallelTools": true, "strictTools": true,
	"structuredOutput": true, "webSearch": true, "vision": true,
	"audioInput": true, "videoInput": true, "logprobs": true,
	"cacheUsage": true, "prefix": true, "fim": true,
}

func validateProvider(name string, provider Provider) error {
	if err := config.ValidateModelProviderID(name); err != nil {
		return fmt.Errorf("provider %q: %w", name, err)
	}
	if len(provider.Interfaces) == 0 {
		return fmt.Errorf("provider %q must contain at least one interface", name)
	}
	if len(provider.Models) == 0 {
		return fmt.Errorf("provider %q must contain at least one model", name)
	}
	interfaceNames := make([]string, 0, len(provider.Interfaces))
	for interfaceName := range provider.Interfaces {
		interfaceNames = append(interfaceNames, interfaceName)
	}
	sort.Strings(interfaceNames)
	for _, interfaceName := range interfaceNames {
		if err := config.ValidateModelCatalogID(interfaceName); err != nil {
			return fmt.Errorf("provider %q interface %q: %w", name, interfaceName, err)
		}
		if err := validateInterface(name+"/"+interfaceName, provider.Interfaces[interfaceName]); err != nil {
			return err
		}
	}
	if provider.DefaultInterface != "" && !hasInterface(provider.Interfaces, provider.DefaultInterface) {
		return fmt.Errorf("provider %q references missing defaultInterface %q", name, provider.DefaultInterface)
	}
	seen := map[string]string{}
	modelNames := make([]string, 0, len(provider.Models))
	for modelName := range provider.Models {
		modelNames = append(modelNames, modelName)
	}
	sort.Strings(modelNames)
	for _, modelName := range modelNames {
		model := provider.Models[modelName]
		if err := config.ValidateModelID(modelName); err != nil {
			return fmt.Errorf("provider %q model %q: %w", name, modelName, err)
		}
		if strings.TrimSpace(model.UpstreamModel) == "" {
			return fmt.Errorf("provider %q model %q requires upstreamModel", name, modelName)
		}
		if model.DefaultInterface != "" && !hasInterface(provider.Interfaces, model.DefaultInterface) {
			return fmt.Errorf("provider %q model %q references missing defaultInterface %q", name, modelName, model.DefaultInterface)
		}
		if err := validateModelFeatures(name+"/"+modelName, provider.Interfaces, model); err != nil {
			return err
		}
		featurePolicies := make(map[string]config.ModelFeature, len(model.Features))
		for featureName, feature := range model.Features {
			featurePolicies[featureName] = config.ModelFeature{Support: feature.Support, Interface: feature.Interface, Operation: feature.Operation}
		}
		if err := config.ValidateModelFeatureRoutes(name+"/"+modelName, featurePolicies); err != nil {
			return err
		}
		if model.Priority < 0 {
			return fmt.Errorf("model %q has invalid priority", name+"/"+modelName)
		}
		if err := validateLimitsAndPolicies(name+"/"+modelName, model); err != nil {
			return err
		}
		for _, alias := range append([]string{modelName}, model.Aliases...) {
			alias = strings.TrimSpace(alias)
			if alias == "" {
				continue
			}
			if strings.ContainsAny(alias, "\r\n\t ") {
				return fmt.Errorf("provider %q model %q has invalid alias %q", name, modelName, alias)
			}
			key := strings.ToLower(alias)
			if previous, ok := seen[key]; ok && previous != modelName {
				return fmt.Errorf("provider %q model alias %q conflicts between %q and %q", name, alias, previous, modelName)
			}
			seen[key] = modelName
		}
	}
	return nil
}

func validateInterface(name string, iface Interface) error {
	adapter := strings.ToLower(strings.TrimSpace(iface.Adapter))
	if !supportedAdapters[adapter] {
		return fmt.Errorf("interface %q has unsupported adapter %q", name, iface.Adapter)
	}
	parsed, err := url.Parse(strings.TrimSpace(iface.BaseURL))
	if err != nil || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
		return fmt.Errorf("interface %q requires an absolute http(s) baseUrl", name)
	}
	if parsed.User != nil {
		return fmt.Errorf("interface %q baseUrl must not contain credentials", name)
	}
	protocol := strings.ToLower(strings.TrimSpace(iface.Protocol))
	if protocol == "" {
		protocol = protocolForAdapter(adapter)
	}
	if !supportedProtocols[protocol] {
		return fmt.Errorf("interface %q has unsupported protocol %q", name, iface.Protocol)
	}
	if expected := protocolForAdapter(adapter); expected != "" && !protocolsCompatible(expected, protocol) {
		return fmt.Errorf("interface %q protocol %q is incompatible with adapter %q", name, protocol, iface.Adapter)
	}
	if err := validateAuth(name, iface.Auth); err != nil {
		return err
	}
	if err := validateConversion(name, iface.Conversion); err != nil {
		return err
	}
	for header := range iface.Headers {
		switch strings.ToLower(strings.TrimSpace(header)) {
		case "authorization", "api-key", "x-api-key", "x-goog-api-key":
			return fmt.Errorf("interface %q headers must not contain credential header %q; use auth and a local binding", name, header)
		}
	}
	for header, value := range iface.Headers {
		if strings.TrimSpace(header) == "" || strings.ContainsAny(header, "\r\n:") {
			return fmt.Errorf("interface %q has invalid header name %q", name, header)
		}
		if strings.ContainsAny(value, "\r\n") {
			return fmt.Errorf("interface %q header %q contains a newline", name, header)
		}
	}
	if err := validateHTTPPolicy("interface "+name, iface.HTTP); err != nil {
		return err
	}
	return validateStreamPolicy("interface "+name, iface.Stream)
}

func validateAuth(name string, auth Auth) error {
	switch strings.ToLower(strings.TrimSpace(auth.Type)) {
	case "", "bearer":
		if strings.TrimSpace(auth.Header) != "" {
			return fmt.Errorf("interface %q bearer auth must not specify header", name)
		}
	case "header":
		header := strings.TrimSpace(auth.Header)
		if header == "" || strings.ContainsAny(header, "\r\n:") || strings.EqualFold(header, "authorization") {
			return fmt.Errorf("interface %q header auth requires a non-Authorization header", name)
		}
	default:
		return fmt.Errorf("interface %q has invalid auth type %q", name, auth.Type)
	}
	return nil
}

func validateConversion(name string, conversion config.ModelConversion) error {
	profile := strings.TrimSpace(conversion.Profile)
	if conversion.Enabled && profile == "" {
		return fmt.Errorf("interface %q conversion.enabled requires conversion.profile", name)
	}
	if !conversion.Enabled && profile != "" {
		return fmt.Errorf("interface %q conversion.profile requires conversion.enabled", name)
	}
	if conversion.Strict != nil && !conversion.Enabled {
		return fmt.Errorf("interface %q conversion.strict requires conversion.enabled", name)
	}
	if len(profile) > 128 || strings.ContainsAny(profile, " \t\r\n/\\") {
		return fmt.Errorf("interface %q conversion.profile %q is invalid", name, profile)
	}
	return nil
}

func validateModelFeatures(name string, interfaces map[string]Interface, model Model) error {
	for featureName, feature := range model.Features {
		if !supportedFeatures[featureName] {
			return fmt.Errorf("model %q has unsupported feature %q", name, featureName)
		}
		switch feature.Support {
		case "native", "translated", "plugin", "unsupported":
		default:
			return fmt.Errorf("model %q feature %q has invalid support %q", name, featureName, feature.Support)
		}
		featureName = strings.TrimSpace(featureName)
		if featureName == "" {
			return fmt.Errorf("model %q has an empty feature name", name)
		}
		if feature.RequireSources || feature.Sources != nil || feature.NativeTool != nil || feature.Fallback != nil {
			if featureName != "webSearch" {
				return fmt.Errorf("model %q feature %q has web-search-only fields", name, featureName)
			}
		}
		if feature.Interface != "" && !featureRouteCapable(featureName, feature.Operation) {
			return fmt.Errorf("model %q feature %q interface is not bound to an operation", name, featureName)
		}
		if feature.Support == "unsupported" && (feature.Interface != "" || feature.Operation != "" || feature.Fallback != nil || feature.NativeTool != nil) {
			return fmt.Errorf("model %q unsupported feature %q must not declare a route or fallback", name, featureName)
		}
		if feature.Interface != "" && !hasInterface(interfaces, feature.Interface) {
			return fmt.Errorf("model %q feature %q references missing interface %q", name, featureName, feature.Interface)
		}
		if err := validateOperation(name, featureName, feature.Operation); err != nil {
			return err
		}
		if (feature.Support == "native" || feature.Support == "translated") && featureRouteCapable(featureName, feature.Operation) && feature.Interface == "" {
			return fmt.Errorf("model %q %s feature %q requires interface", name, feature.Support, featureName)
		}
		if feature.Support == "plugin" && (feature.Interface != "" || feature.Operation != "") {
			return fmt.Errorf("model %q plugin feature %q must not declare an upstream route", name, featureName)
		}
		if feature.NativeTool != nil {
			if feature.Support != "native" {
				return fmt.Errorf("model %q feature %q nativeTool requires support=native", name, featureName)
			}
			if err := validateNativeTool(name, featureName, *feature.NativeTool); err != nil {
				return err
			}
		}
		if feature.Sources != nil {
			mode := strings.ToLower(strings.TrimSpace(feature.Sources.Mode))
			switch mode {
			case "", "annotations", "text", "unsupported":
			default:
				return fmt.Errorf("model %q feature %q has invalid sources.mode %q", name, featureName, feature.Sources.Mode)
			}
			if mode == "unsupported" && (feature.Sources.RequireURL || feature.Sources.RequireSources || feature.RequireSources) {
				return fmt.Errorf("model %q feature %q cannot require sources when sources.mode is unsupported", name, featureName)
			}
			if mode == "text" && (feature.Sources.RequireURL || feature.Sources.RequireSources || feature.RequireSources) {
				return fmt.Errorf("model %q feature %q cannot require URL/structured sources when sources.mode is text", name, featureName)
			}
		}
		if feature.Support == "plugin" && featureName == "webSearch" && (feature.Fallback == nil || strings.TrimSpace(feature.Fallback.Selector) == "") {
			return fmt.Errorf("model %q plugin feature %q requires fallback.selector", name, featureName)
		}
		if feature.Fallback != nil {
			selector := strings.TrimSpace(feature.Fallback.Selector)
			providerPart, modelPart, qualified := strings.Cut(selector, "/")
			if selector == "" || strings.ContainsAny(selector, " \t\r\n") || !qualified || strings.Contains(modelPart, "/") || config.ValidateModelProviderID(providerPart) != nil || config.ValidateModelID(modelPart) != nil {
				return fmt.Errorf("model %q feature %q fallback selector %q is invalid", name, featureName, selector)
			}
			if strings.ContainsAny(feature.Fallback.Effort, " \t\r\n") || strings.ContainsAny(feature.Fallback.Tier, " \t\r\n") {
				return fmt.Errorf("model %q feature %q fallback effort/tier must be single values", name, featureName)
			}
		}
	}
	return nil
}

func validateNativeTool(model, feature string, tool config.ModelNativeTool) error {
	if len(tool.InputTypes) == 0 {
		return fmt.Errorf("model %q feature %q nativeTool requires inputTypes", model, feature)
	}
	if strings.TrimSpace(tool.UpstreamType) == "" || strings.ContainsAny(tool.UpstreamType, " \t\r\n") {
		return fmt.Errorf("model %q feature %q nativeTool requires a single upstreamType", model, feature)
	}
	seen := map[string]bool{}
	for _, value := range tool.InputTypes {
		value = strings.TrimSpace(value)
		if value == "" || strings.ContainsAny(value, " \t\r\n") || seen[strings.ToLower(value)] {
			return fmt.Errorf("model %q feature %q nativeTool has invalid inputType %q", model, feature, value)
		}
		seen[strings.ToLower(value)] = true
	}
	for _, field := range tool.AllowedFields {
		if strings.TrimSpace(field) == "" || strings.ContainsAny(field, " \t\r\n") {
			return fmt.Errorf("model %q feature %q nativeTool has invalid allowed field %q", model, feature, field)
		}
	}
	return nil
}

func validateOperation(model, feature, operation string) error {
	operation = strings.ToLower(strings.TrimSpace(operation))
	feature = strings.ToLower(strings.TrimSpace(feature))
	if operation == "" {
		if feature == "fim" || feature == "prefix" {
			return fmt.Errorf("model %q feature %q requires operation %q", model, feature, feature)
		}
		return nil
	}
	switch operation {
	case "chat", "responses", "prefix", "fim":
	default:
		return fmt.Errorf("model %q feature %q has invalid operation %q (allowed: chat, responses, prefix, fim)", model, feature, operation)
	}
	if (feature == "fim" || feature == "prefix") && operation != feature {
		return fmt.Errorf("model %q feature %q must use operation %q", model, feature, feature)
	}
	return nil
}

func featureRouteCapable(feature, operation string) bool {
	feature = strings.ToLower(strings.TrimSpace(feature))
	operation = strings.ToLower(strings.TrimSpace(operation))
	if operation != "" {
		return true
	}
	switch feature {
	case "chat", "responses", "websearch", "prefix", "fim":
		return true
	default:
		return false
	}
}

func validateLimitsAndPolicies(name string, model Model) error {
	return config.ValidateModelDefinition(name, config.ModelDefinition{
		Limits:    model.Limits,
		Reasoning: model.Reasoning,
		Tools:     model.Tools,
		Messages:  model.Messages,
		Sampling:  model.Sampling,
		Responses: model.Responses,
		Stream:    model.Stream,
		HTTP:      model.HTTP,
	})
}

func validateHTTPPolicy(name string, policy config.ModelHTTPPolicy) error {
	if policy.TimeoutSeconds < 0 || policy.ResponseHeaderTimeoutSeconds < 0 || policy.MaxConcurrentRequests < 0 {
		return fmt.Errorf("%s has invalid HTTP timeout/concurrency", name)
	}
	if policy.MaxRetries != nil && *policy.MaxRetries < 0 {
		return fmt.Errorf("%s has invalid maxRetries", name)
	}
	return nil
}

func validateStreamPolicy(name string, policy config.ModelStreamPolicy) error {
	if policy.IdleTimeoutSeconds < 0 {
		return fmt.Errorf("%s has invalid stream idle timeout", name)
	}
	return nil
}

func hasInterface(interfaces map[string]Interface, name string) bool {
	for candidate := range interfaces {
		if strings.EqualFold(strings.TrimSpace(candidate), strings.TrimSpace(name)) {
			return true
		}
	}
	return false
}

func (d Document) Routes() []Route {
	providerNames := make([]string, 0, len(d.Providers))
	for name := range d.Providers {
		providerNames = append(providerNames, name)
	}
	sort.Strings(providerNames)
	routes := make([]Route, 0)
	for _, providerID := range providerNames {
		provider := d.Providers[providerID]
		interfaceNames := make([]string, 0, len(provider.Interfaces))
		for name := range provider.Interfaces {
			interfaceNames = append(interfaceNames, name)
		}
		sort.Strings(interfaceNames)
		providerDefaultInterface := strings.TrimSpace(provider.DefaultInterface)
		if providerDefaultInterface == "" && len(interfaceNames) > 0 {
			providerDefaultInterface = interfaceNames[0]
		}
		providerDefaultInterface = canonicalInterfaceName(provider.Interfaces, providerDefaultInterface)
		providerDefault := provider.Interfaces[providerDefaultInterface]
		providerProtocol := strings.TrimSpace(providerDefault.Protocol)
		if providerProtocol == "" {
			providerProtocol = protocolForAdapter(providerDefault.Adapter)
		}
		modelNames := make([]string, 0, len(provider.Models))
		for name := range provider.Models {
			modelNames = append(modelNames, name)
		}
		sort.Strings(modelNames)
		for _, modelID := range modelNames {
			model := provider.Models[modelID]
			defaultInterface := strings.TrimSpace(model.DefaultInterface)
			if defaultInterface == "" && len(interfaceNames) > 0 {
				defaultInterface = interfaceNames[0]
			}
			ifaceName := effectiveModelInterface(provider, model, defaultInterface)
			iface := provider.Interfaces[ifaceName]
			p := config.ModelProvider{
				Protocol: providerProtocol, BaseURL: providerDefault.BaseURL, Headers: cloneStrings(providerDefault.Headers), Endpoints: cloneStrings(providerDefault.Endpoints),
				HTTP: providerDefault.HTTP, Stream: providerDefault.Stream, DefaultInterface: providerDefaultInterface, AdapterProfile: strings.TrimSpace(providerDefault.Adapter),
				Interfaces: flattenInterfaces(provider.Interfaces),
			}
			m := config.ModelDefinition{
				Provider: providerID, UpstreamModel: model.UpstreamModel, DefaultInterface: ifaceName, DisplayName: model.DisplayName, Aliases: append([]string(nil), model.Aliases...),
				Description: model.Description, Priority: model.Priority, Limits: model.Limits, Reasoning: model.Reasoning, Tools: model.Tools,
				Messages: model.Messages, Sampling: model.Sampling, Responses: model.Responses, Stream: model.Stream, HTTP: model.HTTP, Cache: model.Cache,
				Features: flattenFeatures(model.Features), Capabilities: capabilitiesFromFeatures(model.Features), Search: searchFromFeature(model.Features["webSearch"]),
			}
			authType := strings.ToLower(strings.TrimSpace(iface.Auth.Type))
			routes = append(routes, Route{ProviderID: providerID, ModelID: modelID, Selector: providerID + "/" + modelID, InterfaceID: ifaceName, Provider: p, Model: m, AuthType: authType, AuthHeader: strings.TrimSpace(iface.Auth.Header), Credential: "catalog-provider/" + providerID + "/" + ifaceName})
		}
	}
	return routes
}

func effectiveModelInterface(provider Provider, model Model, fallback string) string {
	// InterfaceID is the model's ordinary/default route. Feature-specific
	// interfaces remain in Model.Features and are materialized into operation
	// routes later; collapsing a native search interface into this field would
	// make normal chat use the search credential and wire protocol.
	return canonicalInterfaceName(provider.Interfaces, strings.TrimSpace(fallback))
}

func protocolForAdapter(adapter string) string {
	switch strings.ToLower(strings.TrimSpace(adapter)) {
	case "openai-responses", "mimo-responses":
		return "responses"
	case "deepseek-anthropic":
		return "messages"
	case "deepseek-beta":
		return "beta"
	default:
		return "chat-completions"
	}
}

func protocolsCompatible(expected, actual string) bool {
	expected = strings.ToLower(strings.TrimSpace(expected))
	actual = strings.ToLower(strings.TrimSpace(actual))
	if expected == actual {
		return true
	}
	return (expected == "messages" && actual == "anthropic") || (expected == "anthropic" && actual == "messages") || (expected == "beta" && actual == "fim") || (expected == "fim" && actual == "beta")
}

func canonicalInterfaceName(interfaces map[string]Interface, name string) string {
	for candidate := range interfaces {
		if strings.EqualFold(candidate, name) {
			return candidate
		}
	}
	return name
}

func flattenInterfaces(in map[string]Interface) map[string]config.ModelInterface {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]config.ModelInterface, len(in))
	for name, iface := range in {
		protocol := strings.TrimSpace(iface.Protocol)
		if protocol == "" {
			protocol = protocolForAdapter(iface.Adapter)
		}
		out[name] = config.ModelInterface{Adapter: iface.Adapter, Protocol: protocol, BaseURL: iface.BaseURL, Conversion: iface.Conversion, Auth: config.ModelInterfaceAuth{Type: iface.Auth.Type, Header: iface.Auth.Header}, Headers: cloneStrings(iface.Headers), Endpoints: cloneStrings(iface.Endpoints), HTTP: iface.HTTP, Stream: iface.Stream}
	}
	return out
}

func flattenFeatures(in map[string]Feature) map[string]config.ModelFeature {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]config.ModelFeature, len(in))
	for name, feature := range in {
		converted := config.ModelFeature{Support: feature.Support, Interface: feature.Interface, Operation: feature.Operation, RequireSources: feature.RequireSources}
		if feature.NativeTool != nil {
			copy := *feature.NativeTool
			copy.InputTypes = append([]string(nil), feature.NativeTool.InputTypes...)
			copy.AllowedFields = append([]string(nil), feature.NativeTool.AllowedFields...)
			converted.NativeTool = &copy
		}
		if feature.Sources != nil {
			copy := *feature.Sources
			converted.Sources = &copy
		}
		if feature.Fallback != nil {
			converted.Fallback = &config.ModelFeatureFallback{Selector: feature.Fallback.Selector, Effort: feature.Fallback.Effort, Tier: feature.Fallback.Tier, On: append([]string(nil), feature.Fallback.On...)}
		}
		out[name] = converted
	}
	return out
}

func capabilitiesFromFeatures(features map[string]Feature) config.ModelCapabilities {
	var out config.ModelCapabilities
	set := func(name string, dst **bool) {
		if feature, ok := features[name]; ok {
			// plugin is an external capability, not something the CXP route
			// can execute by merely loading this catalog. Do not project it as
			// a native/runtime capability until a plugin route is attached.
			value := feature.Support == "native" || feature.Support == "translated"
			*dst = &value
		}
	}
	set("tools", &out.Tools)
	set("parallelTools", &out.ParallelTools)
	set("vision", &out.Vision)
	set("reasoning", &out.Reasoning)
	if feature, ok := features["webSearch"]; ok {
		value := feature.Support == "native"
		out.NativeWebSearch = &value
	}
	return out
}

func searchFromFeature(feature Feature) config.ModelSearchPolicy {
	if feature.Support == "" {
		return config.ModelSearchPolicy{}
	}
	native := feature.Support == "native"
	policy := config.ModelSearchPolicy{Native: &native}
	if feature.Fallback != nil {
		fallback := config.ModelSearchFallback{Enabled: boolPtr(true), Model: feature.Fallback.Selector, Effort: feature.Fallback.Effort}
		policy.Fallback = fallback
	}
	return policy
}

func boolPtr(value bool) *bool { return &value }

func cloneStrings(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
