package responsesadapter

import (
	"fmt"
	"net/http"
)

type ProviderRuntime struct {
	Adapter        ProviderAdapter
	ProviderID     string
	PublicModel    string
	Model          string
	KeyFingerprint string
	BaseURLHash    string
	ProfileVersion string
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
		Adapter:        f.Adapter,
		ProviderID:     firstNonEmpty(f.ProviderID, "adapter"),
		Model:          model,
		KeyFingerprint: f.KeyFingerprint,
		BaseURLHash:    f.BaseURLHash,
		ProfileVersion: firstNonEmpty(f.ProfileVersion, "v1"),
	}, nil
}

func routeErrorf(status int, format string, args ...any) RouteError {
	return RouteError{Status: status, Message: fmt.Sprintf(format, args...)}
}
