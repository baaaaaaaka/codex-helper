package responsesadapter

import (
	"fmt"
	"net/http"
	"strings"
)

type ProviderRuntime struct {
	Adapter        ProviderAdapter
	ProviderID     string
	PublicModel    string
	Model          string
	KeyFingerprint string
	BaseURLHash    string
	ProfileVersion string
	CustomToolMode string
	// UnsupportedToolPolicy is "error" for catalog routes that declare a
	// native feature the selected adapter cannot translate. The default remains
	// drop for legacy programmatic providers that do not declare capabilities.
	UnsupportedToolPolicy   string
	ConversionProfile       string
	StrictConversion        bool
	Operation               string
	NativeTools             []NativeToolSpec
	SourcePolicy            SourcePolicy
	ResponsesPolicy         ResponsesPolicy
	ParallelToolEnforcement string
	Route                   Route
}

// Route selects a wire adapter for one operation.
type Route struct {
	Interface  string `json:"interface"`
	Adapter    string `json:"adapter"`
	Protocol   string `json:"protocol"`
	Conversion string `json:"conversion,omitempty"`
}

// StructuredOutputPolicy distinguishes json_object from json_schema;
// providers commonly support one format reliably but not the other.
type StructuredOutputPolicy struct {
	JSONObject string `json:"jsonObject,omitempty"`
	JSONSchema string `json:"jsonSchema,omitempty"`
}

// ResponsesPolicy advertises fields whose semantics differ between provider
// APIs. A value of "unsupported" makes the facade fail closed.
type ResponsesPolicy struct {
	PreviousResponseID string                 `json:"previousResponseId,omitempty"`
	Background         string                 `json:"background,omitempty"`
	ContextManagement  string                 `json:"contextManagement,omitempty"`
	StructuredOutput   StructuredOutputPolicy `json:"structuredOutput,omitempty"`
}

func validateSourcePolicy(policy SourcePolicy) error {
	mode := strings.ToLower(strings.TrimSpace(policy.Mode))
	switch mode {
	case "", "annotations", "text", "unsupported":
	default:
		return fmt.Errorf("invalid source policy mode %q", policy.Mode)
	}
	if mode == "unsupported" && (policy.RequireURL || policy.RequireSources) {
		return fmt.Errorf("source policy cannot require citations when mode is unsupported")
	}
	// Text mode intentionally exposes provider text rather than structured
	// citation events. Requiring a URL or a structured source would therefore
	// make every otherwise valid response fail at the facade boundary.
	if mode == "text" && (policy.RequireURL || policy.RequireSources) {
		return fmt.Errorf("source policy cannot require URL/structured citations when mode is text")
	}
	return nil
}

type ProviderRouter interface {
	Resolve(*http.Request, ResponsesRequest) (ProviderRuntime, error)
	Models() []ModelInfo
}

type RouteError struct {
	Status  int
	Message string
}

func (e RouteError) Error() string {
	return e.Message
}

func routeErrorStatus(err error) int {
	if err == nil {
		return http.StatusOK
	}
	var routeErr RouteError
	if ok := asRouteError(err, &routeErr); ok && routeErr.Status > 0 {
		return routeErr.Status
	}
	return http.StatusBadRequest
}

func asRouteError(err error, target *RouteError) bool {
	if err == nil {
		return false
	}
	if routeErr, ok := err.(RouteError); ok {
		*target = routeErr
		return true
	}
	if routeErr, ok := err.(*RouteError); ok && routeErr != nil {
		*target = *routeErr
		return true
	}
	return false
}

func (f *Facade) resolveRuntime(r *http.Request, req ResponsesRequest) (ProviderRuntime, error) {
	if f.Router != nil {
		return f.Router.Resolve(r, req)
	}
	model := firstNonEmpty(req.Model, f.DefaultModel)
	if model == "" {
		return ProviderRuntime{}, RouteError{Status: http.StatusBadRequest, Message: "model is required"}
	}
	if f.Adapter == nil {
		return ProviderRuntime{}, RouteError{Status: http.StatusInternalServerError, Message: "adapter is not configured"}
	}
	return ProviderRuntime{
		Adapter:               f.Adapter,
		ProviderID:            firstNonEmpty(f.ProviderID, "adapter"),
		Model:                 model,
		KeyFingerprint:        f.KeyFingerprint,
		BaseURLHash:           f.BaseURLHash,
		ProfileVersion:        firstNonEmpty(f.ProfileVersion, "v1"),
		UnsupportedToolPolicy: "drop",
	}, nil
}

func validateResponsesPolicy(req ResponsesRequest, policy ResponsesPolicy) error {
	if err := validateStructuredOutputRequest(req.ResponseFormat, policy.StructuredOutput); err != nil {
		return err
	}
	previousPolicy := strings.ToLower(strings.TrimSpace(policy.PreviousResponseID))
	if strings.TrimSpace(req.PreviousResponseID) != "" {
		switch previousPolicy {
		case "unsupported":
			return fmt.Errorf("provider does not support previous_response_id")
		case "delegated":
			// The facade resolves previous_response_id through its local response
			// store and sends the reconstructed history upstream.
		}
	}
	backgroundPolicy := strings.ToLower(strings.TrimSpace(policy.Background))
	if req.Background != nil {
		switch backgroundPolicy {
		case "unsupported":
			return fmt.Errorf("provider does not support background responses")
		case "delegated":
			return fmt.Errorf("background responses are marked delegated but no local background runner is configured")
		}
	}
	contextPolicy := strings.ToLower(strings.TrimSpace(policy.ContextManagement))
	if len(req.ContextManagement) > 0 {
		switch contextPolicy {
		case "unsupported":
			return fmt.Errorf("provider does not support context_management")
		case "delegated":
			return fmt.Errorf("context_management is marked delegated but no local context manager is configured")
		}
	}
	return nil
}
