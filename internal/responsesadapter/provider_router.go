package responsesadapter

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/baaaaaaaka/codex-helper/internal/config"
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
	Route                   config.ModelRoute
}

// ResponsesPolicy is the runtime view of the JSON model contract. Keeping an
// alias here prevents structured-output declarations from being dropped when
// a catalog is materialized into the adapter registry.
type ResponsesPolicy = config.ModelResponsesPolicy

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

// ValidateDirectSourcePolicy is the fail-closed boundary for a native
// Responses pass-through. Unlike the translated Chat facade, the native
// proxy does not inspect provider response events, so it cannot enforce
// annotation/text/source requirements. An explicitly unsupported policy is
// safe; every other source mode must use a route with response translation.
func ValidateDirectSourcePolicy(policy SourcePolicy) error {
	mode := strings.ToLower(strings.TrimSpace(policy.Mode))
	if mode == "" || mode == "unsupported" {
		if mode == "unsupported" && (policy.RequireURL || policy.RequireSources) {
			return fmt.Errorf("direct Responses route cannot require sources when source mode is unsupported")
		}
		return nil
	}
	return fmt.Errorf("direct Responses route cannot enforce source policy %q; use a translated route", policy.Mode)
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

func routeErrorf(status int, format string, args ...any) RouteError {
	return RouteError{Status: status, Message: fmt.Sprintf(format, args...)}
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

// ValidateResponsesRequestPolicy exposes the same catalog policy check to
// transports that forward a provider's native Responses wire directly. This
// keeps a direct native route from bypassing JSON-declared unsupported or
// delegated field semantics.
func ValidateResponsesRequestPolicy(req ResponsesRequest, policy ResponsesPolicy) error {
	return validateResponsesPolicy(req, policy)
}

func validateProviderResponseFields(req ProviderRequest, adapter string) error {
	if req.Background != nil {
		return fmt.Errorf("%s adapter cannot forward background responses", adapter)
	}
	if len(req.ContextManagement) > 0 {
		return fmt.Errorf("%s adapter cannot forward context_management", adapter)
	}
	return nil
}
