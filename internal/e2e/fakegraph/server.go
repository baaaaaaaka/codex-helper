package fakegraph

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TenantID is the fake tenant id used in tokens, URLs and identities.
const TenantID = "e2e00000-0000-4000-8000-00000000te57"

const (
	// HeaderOriginalHost, HeaderOriginalScheme and HeaderInstance are set by
	// cxp's test seam (CXP_TEST_ONLY_SEAM, docs/teams-e2e-test-seam.md) on
	// every request it redirects here: the host and scheme production would
	// have used, and the instance tag.
	HeaderOriginalHost   = "X-Cxp-Test-Original-Host"
	HeaderOriginalScheme = "X-Cxp-Test-Original-Scheme"
	HeaderInstance       = "X-Cxp-Test-Instance"

	graphHost = "graph.microsoft.com"
	loginHost = "login.microsoftonline.com"

	// maxRequestBody fits one upload-session fragment (< 60 MiB).
	maxRequestBody = 64 << 20
	// maxMessageContentBytes approximates the documented Teams limit for a
	// chat message body (~100 KB of HTML).
	maxMessageContentBytes = 102289
)

// Options configures a Server.
type Options struct {
	Owner           User
	Coworker        User
	DefaultPageSize int
	// StripHTMLComments makes the fake drop <!-- … --> from stored HTML,
	// modelling a Teams service that sanitises comments. Off by default
	// because real Teams behaviour is not yet confirmed (CLEANUP-PLAN §8.3b).
	StripHTMLComments bool
	// PreviewLag delays lastMessagePreview on GET /me/chats: a message
	// becomes the preview (and moves its chat in
	// $orderby=lastMessagePreview/createdDateTime desc) only PreviewLag after
	// it was stored. Zero means immediate. See FIDELITY.md A9.
	PreviewLag time.Duration
}

// Server is the fake Graph + login service.
type Server struct {
	opts Options
	ts   *httptest.Server

	mu            sync.Mutex
	users         map[string]User
	chats         map[string]*Chat
	chatOrder     []string
	meetings      map[string]*Meeting
	meetingByExt  map[string]string
	lastStamp     time.Time
	seq           int64
	idCounter     int
	requests      []Request
	violations    []Violation
	blocked       []BlockedRequest
	faults        []*FaultHandle
	hosted        map[hostedKey][]byte
	drive         map[string]driveFile
	items         map[string]*driveItem
	driveOrder    []string
	lastFragments map[string]int
	sessions      map[string]*uploadSession
	workflows     map[string]*workflow
	cards         []WorkflowCard
	accessTokens  map[string]bool
	refreshTokens map[string]bool
	inflight      int
}

// New starts a fake server on a loopback listener.
func New(opts Options) *Server {
	if opts.Owner.ID == "" {
		opts.Owner = User{ID: "11111111-e2e0-4000-8000-0000000000aa", DisplayName: "E2E Owner", UserPrincipalName: "owner@e2e.example"}
	}
	if opts.Coworker.ID == "" {
		opts.Coworker = User{ID: "22222222-e2e0-4000-8000-0000000000bb", DisplayName: "E2E Coworker", UserPrincipalName: "coworker@e2e.example"}
	}
	if opts.DefaultPageSize <= 0 {
		opts.DefaultPageSize = 20
	}
	s := &Server{
		opts:          opts,
		users:         map[string]User{opts.Owner.ID: opts.Owner, opts.Coworker.ID: opts.Coworker},
		chats:         map[string]*Chat{},
		meetings:      map[string]*Meeting{},
		meetingByExt:  map[string]string{},
		accessTokens:  map[string]bool{},
		refreshTokens: map[string]bool{},
	}
	s.ts = httptest.NewServer(http.HandlerFunc(s.serveHTTP))
	return s
}

// URL is the base URL of the loopback listener.
func (s *Server) URL() string { return s.ts.URL }

// Close stops the listener.
func (s *Server) Close() {
	s.ts.CloseClientConnections()
	s.ts.Close()
}

// Owner returns the signed-in user.
func (s *Server) Owner() User { return s.opts.Owner }

// Coworker returns the second account.
func (s *Server) Coworker() User { return s.opts.Coworker }

// IssueTokens registers a fresh access/refresh token pair.
func (s *Server) IssueTokens(label string) (access string, refresh string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.idCounter++
	access = fmt.Sprintf("e2e-access-%s-%d", label, s.idCounter)
	refresh = fmt.Sprintf("e2e-refresh-%s-%d", label, s.idCounter)
	s.accessTokens[access] = true
	s.refreshTokens[refresh] = true
	return access, refresh
}

// ---------------------------------------------------------------------------
// HTTP entry point

type response struct {
	status     int
	body       any
	raw        []byte
	rawType    string
	headers    map[string]string
	violation  *Violation
	afterApply func()
}

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	body, _ := io.ReadAll(io.LimitReader(r.Body, maxRequestBody))
	host := strings.ToLower(r.Header.Get(HeaderOriginalHost))
	if host == "" {
		host = graphHost
	}
	scheme := r.Header.Get(HeaderOriginalScheme)
	if scheme == "" {
		scheme = "https"
	}
	if !knownHost(host) {
		// Anything but the Microsoft endpoints the fake serves is a request
		// the offline sandbox would have refused: recorded, never answered.
		s.RecordBlocked(BlockedRequest{At: start, Instance: r.Header.Get(HeaderInstance), URL: scheme + "://" + host + r.URL.RequestURI()})
		dropConnection(w)
		return
	}
	rec := Request{
		At:       start,
		Instance: r.Header.Get(HeaderInstance),
		Host:     host,
		Method:   r.Method,
		RawPath:  r.URL.RequestURI(),
		Query:    r.URL.RawQuery,
		Body:     truncate(string(body), 4096),
	}
	s.mu.Lock()
	s.seq++
	rec.Seq = s.seq
	s.inflight++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.inflight--
		s.mu.Unlock()
	}()

	var route routeMatch
	if host == loginHost {
		route = matchLoginRoute(r.Method, r.URL.Path)
	} else if host == SharePointHost {
		route = routeMatch{path: r.URL.Path, name: r.Method + " {uploadUrl}"}
	} else if host == WorkflowHost {
		route = routeMatch{path: r.URL.Path, name: r.Method + " {workflow}"}
	} else {
		route = matchGraphRoute(r.Method, strings.TrimPrefix(r.URL.Path, "/v1.0"))
	}
	rec.Path = route.path
	rec.Route = route.name
	rec.ChatID = route.chatID

	// Fault injection happens before authentication and validation, like a
	// gateway in front of Graph.
	if fault := s.matchFault(rec); fault != nil {
		f := fault.fault
		if f.Latency > 0 {
			select {
			case <-time.After(f.Latency):
			case <-r.Context().Done():
			}
		}
		rec.Fault = f.Name
		switch f.Kind {
		case FaultStatus:
			hdr := map[string]string{}
			if f.RetryAfter > 0 {
				secs := int((f.RetryAfter + time.Second - 1) / time.Second)
				hdr["Retry-After"] = strconv.Itoa(secs)
				rec.RetryAfter = time.Duration(secs) * time.Second
			}
			code := "ServiceUnavailable"
			if f.Status == http.StatusTooManyRequests {
				code = "TooManyRequests"
			}
			s.recordWorkflowFault(rec, body, f.Status)
			s.finish(w, &rec, start, response{status: f.Status, headers: hdr, body: graphError(code, "e2e injected fault "+f.Name)})
			return
		case FaultDropBeforeAccept:
			s.recordWorkflowFault(rec, body, 0)
			rec.Status = 0
			rec.Outcome = "dropped"
			s.record(rec, start)
			dropConnection(w)
			return
		case FaultAcceptedLost:
			resp := s.dispatch(host, route, r, body, &rec)
			rec.Status = resp.status
			rec.Outcome = "dropped"
			if resp.violation != nil {
				s.addViolation(*resp.violation, rec)
			}
			s.record(rec, start)
			dropConnection(w)
			return
		case FaultAcceptedHold:
			// Apply, then never answer: the client waits until it gives up
			// or its process dies (the harness SIGKILLs it meanwhile).
			resp := s.dispatch(host, route, r, body, &rec)
			rec.Status = resp.status
			rec.Outcome = "held"
			if resp.violation != nil {
				s.addViolation(*resp.violation, rec)
			}
			s.record(rec, start)
			<-r.Context().Done()
			return
		case FaultLatency:
			// fall through to normal handling
		}
	}

	resp := s.dispatch(host, route, r, body, &rec)
	s.finish(w, &rec, start, resp)
}

func (s *Server) dispatch(host string, route routeMatch, r *http.Request, body []byte, rec *Request) response {
	if scheme := r.Header.Get(HeaderOriginalScheme); scheme != "" && scheme != "https" {
		return violationResponse(rec, "graph-contract", "plain "+scheme+" request to "+host+" (Microsoft endpoints are https only)")
	}
	if host == loginHost {
		return s.handleLogin(route, r, body)
	}
	if host == SharePointHost {
		return s.handleUploadURL(r, body, rec)
	}
	if host == WorkflowHost {
		return s.handleWorkflow(r, body, rec)
	}
	if host != graphHost {
		return violationResponse(rec, "unmodeled", "unknown host "+host)
	}
	if route.name == "" {
		return violationResponse(rec, "unmodeled", "no such Graph route in the fake (real Graph may or may not support it)")
	}
	if !strings.HasPrefix(r.URL.Path, "/v1.0/") {
		return violationResponse(rec, "graph-contract", "path is not under /v1.0")
	}
	auth := r.Header.Get("Authorization")
	token := strings.TrimPrefix(auth, "Bearer ")
	s.mu.Lock()
	valid := token != auth && s.accessTokens[token]
	s.mu.Unlock()
	if !valid {
		return response{status: http.StatusUnauthorized, body: graphError("InvalidAuthenticationToken", "Access token is empty or invalid.")}
	}
	if (r.Method == http.MethodPost || r.Method == http.MethodPatch) && len(body) > 0 {
		ct := r.Header.Get("Content-Type")
		if !strings.HasPrefix(ct, "application/json") {
			return violationResponse(rec, "graph-contract", "write body Content-Type must be application/json, got "+ct)
		}
	}
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		return violationResponse(rec, "graph-contract", "malformed query: "+err.Error())
	}
	return s.handleGraph(route, q, body, rec)
}

// knownHost reports whether the fake serves host.
func knownHost(host string) bool {
	switch host {
	case graphHost, loginHost, SharePointHost, WorkflowHost:
		return true
	}
	return false
}

func (s *Server) finish(w http.ResponseWriter, rec *Request, start time.Time, resp response) {
	if resp.violation != nil {
		s.addViolation(*resp.violation, *rec)
	}
	rec.Status = resp.status
	if rec.Outcome == "" {
		switch {
		case resp.violation != nil:
			rec.Outcome = "violation"
		case rec.Fault != "":
			rec.Outcome = "fault"
		case resp.status >= 400:
			rec.Outcome = "error"
		default:
			rec.Outcome = "ok"
		}
	}
	s.record(*rec, start)
	for k, v := range resp.headers {
		w.Header().Set(k, v)
	}
	w.Header().Set("request-id", fmt.Sprintf("e2e-%d", rec.Seq))
	if resp.raw != nil {
		w.Header().Set("Content-Type", resp.rawType)
		w.WriteHeader(resp.status)
		_, _ = w.Write(resp.raw)
		return
	}
	if resp.body == nil {
		w.WriteHeader(resp.status)
		return
	}
	data, _ := json.Marshal(resp.body)
	w.Header().Set("Content-Type", "application/json; odata.metadata=minimal; charset=utf-8")
	w.WriteHeader(resp.status)
	_, _ = w.Write(data)
}

func (s *Server) record(rec Request, start time.Time) {
	rec.Duration = time.Since(start)
	s.mu.Lock()
	s.requests = append(s.requests, rec)
	s.mu.Unlock()
}

func (s *Server) addViolation(v Violation, rec Request) {
	v.Seq = rec.Seq
	v.At = rec.At
	v.Method = rec.Method
	v.Path = rec.RawPath
	v.Route = rec.Route
	if v.Route == "" {
		v.Route = rec.Method + " " + templatePath(rec.Path)
	}
	s.mu.Lock()
	s.violations = append(s.violations, v)
	s.mu.Unlock()
}

func dropConnection(w http.ResponseWriter) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		panic(http.ErrAbortHandler)
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		panic(http.ErrAbortHandler)
	}
	_ = conn.Close()
}

func (s *Server) matchFault(rec Request) *FaultHandle {
	s.mu.Lock()
	faults := append([]*FaultHandle(nil), s.faults...)
	s.mu.Unlock()
	for _, f := range faults {
		if f.consider(rec) {
			return f
		}
	}
	return nil
}

// AddFault installs an injection rule.
func (s *Server) AddFault(f Fault) *FaultHandle {
	h := &FaultHandle{fault: f}
	s.mu.Lock()
	s.faults = append(s.faults, h)
	s.mu.Unlock()
	return h
}

// ClearFaults disarms every rule.
func (s *Server) ClearFaults() {
	s.mu.Lock()
	faults := s.faults
	s.faults = nil
	s.mu.Unlock()
	for _, f := range faults {
		f.Disarm()
	}
}

func violationResponse(rec *Request, kind string, reason string) response {
	return response{
		status:    http.StatusBadRequest,
		body:      graphError("BadRequest", "e2e fake rejected request: "+reason),
		violation: &Violation{Kind: kind, Reason: reason},
	}
}

func graphError(code, message string) map[string]any {
	return map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": message,
			"innerError": map[string]any{
				"date":       time.Now().UTC().Format(time.RFC3339),
				"request-id": "e2e",
			},
		},
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

// ---------------------------------------------------------------------------
// Routing

type routeMatch struct {
	name      string // "METHOD /template"
	path      string
	chatID    string
	messageID string
	meetingID string
	extra     string
}

func templatePath(p string) string {
	parts := strings.Split(p, "/")
	for i := 1; i < len(parts); i++ {
		switch parts[i-1] {
		case "chats":
			parts[i] = "{chat}"
		case "messages":
			if parts[i] != "replyWithQuote" {
				parts[i] = "{message}"
			}
		case "onlineMeetings":
			if parts[i] != "createOrGet" {
				parts[i] = "{meeting}"
			}
		case "hostedContents", "items", "shares":
			parts[i] = "{id}"
		}
	}
	return strings.Join(parts, "/")
}

func matchGraphRoute(method, p string) routeMatch {
	m := routeMatch{path: p}
	if dm, ok := drivePathRoute(method, p, m); ok {
		return dm
	}
	parts := strings.Split(strings.Trim(p, "/"), "/")
	set := func(tmpl string) routeMatch {
		m.name = method + " " + tmpl
		return m
	}
	switch {
	case len(parts) == 1 && parts[0] == "$batch" && method == http.MethodPost:
		return set("/$batch")
	case len(parts) == 2 && parts[0] == "search" && parts[1] == "query" && method == http.MethodPost:
		return set("/search/query")
	case len(parts) == 4 && parts[0] == "me" && parts[1] == "drive" && parts[2] == "items" && method == http.MethodGet:
		m.extra = parts[3]
		return set("/me/drive/items/{id}")
	case len(parts) == 5 && parts[0] == "me" && parts[1] == "drive" && parts[2] == "items" && parts[4] == "content" && method == http.MethodGet:
		m.extra = parts[3]
		return set("/me/drive/items/{id}/content")
	case len(parts) == 1 && parts[0] == "me" && method == http.MethodGet:
		return set("/me")
	case len(parts) == 2 && parts[0] == "me" && parts[1] == "chats" && method == http.MethodGet:
		return set("/me/chats")
	case len(parts) == 1 && parts[0] == "chats" && method == http.MethodPost:
		return set("/chats")
	case len(parts) == 2 && parts[0] == "me" && parts[1] == "onlineMeetings" && method == http.MethodPost:
		return set("/me/onlineMeetings")
	case len(parts) == 3 && parts[0] == "me" && parts[1] == "onlineMeetings" && parts[2] == "createOrGet" && method == http.MethodPost:
		return set("/me/onlineMeetings/createOrGet")
	case len(parts) == 3 && parts[0] == "me" && parts[1] == "onlineMeetings" && (method == http.MethodGet || method == http.MethodPatch):
		m.meetingID = parts[2]
		return set("/me/onlineMeetings/{meeting}")
	case len(parts) == 3 && parts[0] == "shares" && parts[2] == "driveItem" && method == http.MethodGet:
		m.extra = parts[1]
		return set("/shares/{id}/driveItem")
	case len(parts) == 4 && parts[0] == "shares" && parts[2] == "driveItem" && parts[3] == "content" && method == http.MethodGet:
		m.extra = parts[1]
		return set("/shares/{id}/driveItem/content")
	case len(parts) >= 2 && parts[0] == "chats":
		m.chatID = parts[1]
		rest := parts[2:]
		switch {
		case len(rest) == 0 && (method == http.MethodGet || method == http.MethodPatch):
			return set("/chats/{chat}")
		case len(rest) == 1 && rest[0] == "members" && method == http.MethodGet:
			return set("/chats/{chat}/members")
		case len(rest) == 1 && rest[0] == "unhideForUser" && method == http.MethodPost:
			return set("/chats/{chat}/unhideForUser")
		case len(rest) == 1 && rest[0] == "markChatUnreadForUser" && method == http.MethodPost:
			return set("/chats/{chat}/markChatUnreadForUser")
		case len(rest) == 1 && rest[0] == "messages" && (method == http.MethodGet || method == http.MethodPost):
			return set("/chats/{chat}/messages")
		case len(rest) == 2 && rest[0] == "messages" && rest[1] == "replyWithQuote" && method == http.MethodPost:
			return set("/chats/{chat}/messages/replyWithQuote")
		case len(rest) == 2 && rest[0] == "messages" && (method == http.MethodGet || method == http.MethodPatch):
			m.messageID = rest[1]
			return set("/chats/{chat}/messages/{message}")
		case len(rest) == 5 && rest[0] == "messages" && rest[2] == "hostedContents" && rest[4] == "$value" && method == http.MethodGet:
			m.messageID = rest[1]
			m.extra = rest[3]
			return set("/chats/{chat}/messages/{message}/hostedContents/{id}/$value")
		}
	}
	// Unmodeled: still give the log a stable template.
	m.name = ""
	return m
}

func matchLoginRoute(method, p string) routeMatch {
	m := routeMatch{path: p}
	parts := strings.Split(strings.Trim(p, "/"), "/")
	if len(parts) == 4 && parts[1] == "oauth2" && parts[2] == "v2.0" && method == http.MethodPost {
		m.extra = parts[0] // tenant
		switch parts[3] {
		case "token":
			m.name = "POST /{tenant}/oauth2/v2.0/token"
		case "devicecode":
			m.name = "POST /{tenant}/oauth2/v2.0/devicecode"
		}
	}
	return m
}

// ---------------------------------------------------------------------------
// Login

func (s *Server) handleLogin(route routeMatch, r *http.Request, body []byte) response {
	rec := &Request{}
	switch route.name {
	case "POST /{tenant}/oauth2/v2.0/token":
		form, err := url.ParseQuery(string(body))
		if err != nil {
			return violationResponse(rec, "graph-contract", "token request body is not form encoded")
		}
		if route.extra != TenantID {
			return response{status: http.StatusBadRequest, body: map[string]any{"error": "invalid_request", "error_description": "AADSTS90002: Tenant not found."}}
		}
		switch form.Get("grant_type") {
		case "refresh_token":
			s.mu.Lock()
			ok := s.refreshTokens[form.Get("refresh_token")]
			s.mu.Unlock()
			if !ok {
				return response{status: http.StatusBadRequest, body: map[string]any{"error": "invalid_grant", "error_description": "AADSTS70000: refresh token is invalid"}}
			}
			access, refresh := s.IssueTokens("refreshed")
			return response{status: http.StatusOK, body: map[string]any{
				"token_type":    "Bearer",
				"scope":         form.Get("scope"),
				"expires_in":    3600,
				"access_token":  access,
				"refresh_token": refresh,
			}}
		default:
			return response{status: http.StatusBadRequest, violation: &Violation{Kind: "unmodeled", Reason: "token grant_type " + form.Get("grant_type") + " (interactive login) is not supported in E2E"}, body: map[string]any{"error": "unsupported_grant_type"}}
		}
	case "POST /{tenant}/oauth2/v2.0/devicecode":
		return response{status: http.StatusBadRequest, violation: &Violation{Kind: "unmodeled", Reason: "device-code login attempted; E2E tokens must come from the cache"}, body: map[string]any{"error": "invalid_request"}}
	}
	return response{status: http.StatusNotFound, violation: &Violation{Kind: "unmodeled", Reason: "unknown login endpoint"}, body: map[string]any{"error": "not_found"}}
}

// ---------------------------------------------------------------------------
// Graph handlers

func (s *Server) handleGraph(route routeMatch, q url.Values, body []byte, rec *Request) response {
	switch route.name {
	case "POST /$batch":
		return s.readBatch(body, rec)
	case "POST /search/query":
		return s.searchMessages(body, rec)
	case "GET /me":
		if v := checkSelect(q, "id", "displayName", "userPrincipalName", "mail", "givenName", "surname", "jobTitle", "officeLocation"); v != "" {
			return violationResponse(rec, "graph-contract", v)
		}
		owner := s.opts.Owner
		return response{status: 200, body: map[string]any{
			"@odata.context":    "https://graph.microsoft.com/v1.0/$metadata#users/$entity",
			"id":                owner.ID,
			"displayName":       owner.DisplayName,
			"userPrincipalName": owner.UserPrincipalName,
		}}
	case "GET /me/chats":
		return s.listChats(q, rec)
	case "PUT /me/drive/root:/{path}:/content":
		return s.drivePutContent(route, q, body, rec)
	case "POST /me/drive/root:/{path}:/createUploadSession":
		return s.driveCreateUploadSession(route, q, body, rec)
	case "GET /me/drive/items/{id}":
		return s.driveGetItem(route.extra, false, q, rec)
	case "GET /me/drive/items/{id}/content":
		return s.driveGetItem(route.extra, true, q, rec)
	case "POST /chats":
		if len(q) > 0 {
			return violationResponse(rec, "graph-contract", "POST /chats takes no query options")
		}
		return s.createGroupChat(body, rec)
	case "POST /me/onlineMeetings":
		if len(q) > 0 {
			return violationResponse(rec, "graph-contract", "POST /me/onlineMeetings takes no query options")
		}
		return s.createMeeting(body, rec, false)
	case "POST /me/onlineMeetings/createOrGet":
		if len(q) > 0 {
			return violationResponse(rec, "graph-contract", "createOrGet takes no query options")
		}
		return s.createMeeting(body, rec, true)
	case "GET /me/onlineMeetings/{meeting}":
		if v := checkSelect(q, "id", "subject", "joinWebUrl", "startDateTime", "endDateTime", "expiryDateTime", "chatInfo", "externalId", "participants", "creationDateTime"); v != "" {
			return violationResponse(rec, "graph-contract", v)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		mt := s.meetings[route.meetingID]
		if mt == nil {
			return response{status: 404, body: graphError("NotFound", "meeting not found")}
		}
		return response{status: 200, body: mt.toGraph()}
	case "PATCH /me/onlineMeetings/{meeting}":
		if len(q) > 0 {
			return violationResponse(rec, "graph-contract", "PATCH onlineMeeting takes no query options")
		}
		return s.patchMeeting(route.meetingID, body, rec)
	case "GET /chats/{chat}":
		if v := checkSelect(q, "id", "topic", "chatType", "webUrl", "createdDateTime", "lastUpdatedDateTime", "tenantId", "onlineMeetingInfo", "viewpoint"); v != "" {
			return violationResponse(rec, "graph-contract", v)
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		c, resp := s.memberChatLocked(route.chatID)
		if c == nil {
			return resp
		}
		return response{status: 200, body: c.toGraph(TenantID)}
	case "PATCH /chats/{chat}":
		if len(q) > 0 {
			return violationResponse(rec, "graph-contract", "PATCH chat takes no query options")
		}
		return s.patchChat(route.chatID, body, rec)
	case "GET /chats/{chat}/members":
		if len(q) > 0 {
			return violationResponse(rec, "unmodeled", "query options on chat members are not modelled")
		}
		return s.listMembers(route.chatID)
	case "POST /chats/{chat}/unhideForUser", "POST /chats/{chat}/markChatUnreadForUser":
		if len(q) > 0 {
			return violationResponse(rec, "graph-contract", "chat actions take no query options")
		}
		return s.chatUserAction(route, body, rec)
	case "GET /chats/{chat}/messages":
		return s.listMessages(route.chatID, q, rec)
	case "POST /chats/{chat}/messages":
		if len(q) > 0 {
			return violationResponse(rec, "graph-contract", "POST message takes no query options")
		}
		return s.postMessage(route.chatID, body, rec, false)
	case "POST /chats/{chat}/messages/replyWithQuote":
		if len(q) > 0 {
			return violationResponse(rec, "graph-contract", "replyWithQuote takes no query options")
		}
		return s.postMessage(route.chatID, body, rec, true)
	case "GET /chats/{chat}/messages/{message}":
		if len(q) > 0 {
			return violationResponse(rec, "graph-contract", "GET message: unsupported query options "+keys(q))
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		c, resp := s.memberChatLocked(route.chatID)
		if c == nil {
			return resp
		}
		for _, m := range c.Messages {
			if m.ID == route.messageID {
				return response{status: 200, body: m.toGraph()}
			}
		}
		return response{status: 404, body: graphError("NotFound", "message not found")}
	case "PATCH /chats/{chat}/messages/{message}":
		if len(q) > 0 {
			return violationResponse(rec, "graph-contract", "PATCH message takes no query options")
		}
		return s.patchMessage(route.chatID, route.messageID, body, rec)
	case "GET /chats/{chat}/messages/{message}/hostedContents/{id}/$value":
		return s.hostedContent(route)
	case "GET /shares/{id}/driveItem":
		if v := checkSelect(q, "id", "name", "size", "eTag", "webUrl", "webDavUrl", "file"); v != "" {
			return violationResponse(rec, "graph-contract", v)
		}
		return s.shareItem(route, false, rec)
	case "GET /shares/{id}/driveItem/content":
		return s.shareItem(route, true, rec)
	}
	return violationResponse(rec, "unmodeled", "route "+route.name+" not implemented")
}

func keys(q url.Values) string {
	out := make([]string, 0, len(q))
	for k := range q {
		out = append(out, k)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// checkSelect validates an optional $select list and rejects every other
// query option.
func checkSelect(q url.Values, allowed ...string) string {
	for k := range q {
		if k != "$select" {
			return "unsupported query option " + k
		}
	}
	vals := q["$select"]
	if len(vals) == 0 {
		return ""
	}
	if len(vals) > 1 {
		return "$select given more than once"
	}
	ok := map[string]bool{}
	for _, a := range allowed {
		ok[a] = true
	}
	for _, f := range strings.Split(vals[0], ",") {
		if !ok[strings.TrimSpace(f)] {
			return "unknown $select property " + strings.TrimSpace(f)
		}
	}
	return ""
}

func (s *Server) memberChatLocked(chatID string) (*Chat, response) {
	c := s.chats[chatID]
	if c == nil {
		return nil, response{status: 404, body: graphError("NotFound", "chat not found")}
	}
	member := false
	for _, id := range c.Members {
		if id == s.opts.Owner.ID {
			member = true
		}
	}
	if !member {
		return nil, response{status: 403, body: graphError("Forbidden", "caller is not a member of the chat")}
	}
	return c, response{}
}

func (s *Server) nextStampLocked() time.Time {
	now := time.Now().UTC().Truncate(time.Millisecond)
	if !now.After(s.lastStamp) {
		now = s.lastStamp.Add(time.Millisecond)
	}
	s.lastStamp = now
	return now
}

func (s *Server) newChatIDLocked(kind string) string {
	s.idCounter++
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s-%d-%d", kind, s.idCounter, time.Now().UnixNano())))
	// Letters and digits only, as in real meeting IDs (base64 of a GUID's
	// text). A '_' in the ID can turn into Markdown emphasis where a reply
	// quotes it (rename failures), and the transcript then misses the
	// <chat:N> mask: a golden that fails about one run in a few hundred.
	enc := strings.Map(func(r rune) rune {
		switch r {
		case '-':
			return 'x'
		case '_':
			return 'y'
		}
		return r
	}, base64.RawURLEncoding.EncodeToString(sum[:]))[:32]
	if kind == "meeting" {
		return "19:meeting_" + enc + "@thread.v2"
	}
	return "19:" + hex.EncodeToString(sum[:16]) + "@thread.v2"
}

func (s *Server) addChatLocked(kind, topic string, members []string) *Chat {
	c := &Chat{
		ID:       s.newChatIDLocked(kind),
		Topic:    topic,
		ChatType: kind,
		Created:  s.nextStampLocked(),
		Members:  members,
	}
	s.chats[c.ID] = c
	s.chatOrder = append(s.chatOrder, c.ID)
	return c
}

func (s *Server) listChats(q url.Values, rec *Request) response {
	expand, orderByPreview := false, false
	for k := range q {
		switch k {
		case "$top", "$skiptoken":
		case "$expand":
			if q.Get(k) != "lastMessagePreview" {
				return violationResponse(rec, "graph-contract", "/me/chats $expand supports only lastMessagePreview, got "+q.Get(k))
			}
			expand = true
		case "$orderby":
			if q.Get(k) != "lastMessagePreview/createdDateTime desc" {
				return violationResponse(rec, "graph-contract", "/me/chats $orderby supports only lastMessagePreview/createdDateTime desc, got "+q.Get(k))
			}
			orderByPreview = true
		case "$filter":
			return violationResponse(rec, "unmodeled", "/me/chats $filter is not modelled")
		default:
			return violationResponse(rec, "graph-contract", "/me/chats unsupported query option "+k)
		}
	}
	top := 50
	if v := q.Get("$top"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > 50 {
			return violationResponse(rec, "graph-contract", "/me/chats $top must be 1..50, got "+v)
		}
		top = n
	}
	offset := 0
	if tok := q.Get("$skiptoken"); tok != "" {
		n, err := strconv.Atoi(strings.TrimPrefix(tok, "chats-"))
		if err != nil || !strings.HasPrefix(tok, "chats-") {
			return violationResponse(rec, "graph-contract", "invalid $skiptoken")
		}
		offset = n
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// Stamps are strictly monotonic and may run a few ms ahead of the wall
	// clock, so "no lag" must mean "every stored message", not "<= now".
	visibleBefore := time.Now().Add(-s.opts.PreviewLag)
	if s.opts.PreviewLag <= 0 {
		visibleBefore = time.Unix(1<<40, 0)
	}
	type listed struct {
		chat    *Chat
		preview *Message
	}
	var all []listed
	for i := len(s.chatOrder) - 1; i >= 0; i-- {
		c := s.chats[s.chatOrder[i]]
		for _, m := range c.Members {
			if m == s.opts.Owner.ID {
				all = append(all, listed{chat: c, preview: c.previewLocked(visibleBefore)})
				break
			}
		}
	}
	if orderByPreview {
		// Chats without a (visible) last message sort after every chat that
		// has one; ties keep the newest-created-first order.
		sort.SliceStable(all, func(i, j int) bool {
			pi, pj := all[i].preview, all[j].preview
			switch {
			case pi == nil:
				return false
			case pj == nil:
				return true
			default:
				return pi.Created.After(pj.Created)
			}
		})
	}
	var page []any
	for i := offset; i < len(all) && len(page) < top; i++ {
		g := all[i].chat.toGraph(TenantID)
		if expand {
			g["lastMessagePreview@odata.context"] = "https://graph.microsoft.com/v1.0/$metadata#users('" + s.opts.Owner.ID + "')/chats('" + all[i].chat.ID + "')/lastMessagePreview/$entity"
			if p := all[i].preview; p != nil {
				g["lastMessagePreview"] = p.previewGraph()
			} else {
				g["lastMessagePreview"] = nil
			}
		}
		page = append(page, g)
	}
	out := map[string]any{"value": nonNil(page)}
	if offset+top < len(all) {
		next := url.Values{}
		if expand {
			next.Set("$expand", "lastMessagePreview")
		}
		if orderByPreview {
			next.Set("$orderby", "lastMessagePreview/createdDateTime desc")
		}
		next.Set("$top", strconv.Itoa(top))
		next.Set("$skiptoken", fmt.Sprintf("chats-%d", offset+top))
		out["@odata.nextLink"] = "https://graph.microsoft.com/v1.0/me/chats?" + next.Encode()
	}
	return response{status: 200, body: out}
}

func nonNil(v []any) []any {
	if v == nil {
		return []any{}
	}
	return v
}

func (s *Server) createGroupChat(body []byte, rec *Request) response {
	var payload struct {
		ChatType string `json:"chatType"`
		Topic    string `json:"topic"`
		Members  []struct {
			Type     string   `json:"@odata.type"`
			Roles    []string `json:"roles"`
			UserBind string   `json:"user@odata.bind"`
		} `json:"members"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return violationResponse(rec, "graph-contract", "POST /chats body is not JSON")
	}
	if payload.ChatType != "group" && payload.ChatType != "oneOnOne" {
		return violationResponse(rec, "graph-contract", "chatType must be group or oneOnOne")
	}
	var members []string
	for _, m := range payload.Members {
		bind := m.UserBind
		i := strings.Index(bind, "users('")
		if i < 0 {
			return violationResponse(rec, "graph-contract", "member user@odata.bind is malformed")
		}
		id := strings.TrimSuffix(bind[i+len("users('"):], "')")
		members = append(members, id)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c := s.addChatLocked("group", payload.Topic, members)
	return response{status: 201, body: c.toGraph(TenantID)}
}

func (s *Server) createMeeting(body []byte, rec *Request, createOrGet bool) response {
	var payload struct {
		Subject       string `json:"subject"`
		ExternalID    string `json:"externalId"`
		StartDateTime string `json:"startDateTime"`
		EndDateTime   string `json:"endDateTime"`
	}
	if err := json.Unmarshal(body, &payload); err != nil {
		return violationResponse(rec, "graph-contract", "meeting body is not JSON")
	}
	start, err1 := time.Parse(time.RFC3339, payload.StartDateTime)
	end, err2 := time.Parse(time.RFC3339, payload.EndDateTime)
	if err1 != nil || err2 != nil || !end.After(start) {
		return violationResponse(rec, "graph-contract", "meeting startDateTime/endDateTime invalid")
	}
	if createOrGet && strings.TrimSpace(payload.ExternalID) == "" {
		return violationResponse(rec, "graph-contract", "createOrGet requires externalId")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if createOrGet {
		if id, ok := s.meetingByExt[payload.ExternalID]; ok {
			return response{status: 200, body: s.meetings[id].toGraph()}
		}
	}
	c := s.addChatLocked("meeting", payload.Subject, []string{s.opts.Owner.ID})
	s.idCounter++
	mt := &Meeting{
		ID:         fmt.Sprintf("MSo%08d_e2e_meeting", s.idCounter),
		Subject:    payload.Subject,
		ExternalID: payload.ExternalID,
		Start:      start,
		End:        end,
		ChatID:     c.ID,
		OwnerID:    s.opts.Owner.ID,
	}
	c.MeetingID = mt.ID
	c.ExternalID = payload.ExternalID
	s.meetings[mt.ID] = mt
	if payload.ExternalID != "" {
		s.meetingByExt[payload.ExternalID] = mt.ID
	}
	return response{status: 201, body: mt.toGraph()}
}

func (s *Server) patchMeeting(id string, body []byte, rec *Request) response {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		return violationResponse(rec, "graph-contract", "PATCH meeting body is not JSON")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	mt := s.meetings[id]
	if mt == nil {
		return response{status: 404, body: graphError("NotFound", "meeting not found")}
	}
	parse := func(key string, dst *time.Time) bool {
		raw, ok := payload[key]
		if !ok {
			return true
		}
		var v string
		if json.Unmarshal(raw, &v) != nil {
			return false
		}
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return false
		}
		*dst = t
		return true
	}
	start, end := mt.Start, mt.End
	if !parse("startDateTime", &start) || !parse("endDateTime", &end) || !end.After(start) {
		return violationResponse(rec, "graph-contract", "PATCH meeting has invalid window")
	}
	mt.Start, mt.End = start, end
	if raw, ok := payload["subject"]; ok {
		_ = json.Unmarshal(raw, &mt.Subject)
	}
	return response{status: 200, body: mt.toGraph()}
}

func (s *Server) patchChat(chatID string, body []byte, rec *Request) response {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(body, &payload); err != nil {
		return violationResponse(rec, "graph-contract", "PATCH chat body is not JSON")
	}
	for k := range payload {
		if k != "topic" {
			return violationResponse(rec, "graph-contract", "PATCH chat: only topic is updatable, got "+k)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c, resp := s.memberChatLocked(chatID)
	if c == nil {
		return resp
	}
	var topic string
	_ = json.Unmarshal(payload["topic"], &topic)
	c.Topic = topic
	c.TopicLog = append(c.TopicLog, topic)
	return response{status: 204}
}

func (s *Server) listMembers(chatID string) response {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, resp := s.memberChatLocked(chatID)
	if c == nil {
		return resp
	}
	var out []any
	for _, id := range c.Members {
		u := s.users[id]
		out = append(out, map[string]any{
			"@odata.type":                 "#microsoft.graph.aadUserConversationMember",
			"id":                          "member-" + id,
			"roles":                       []string{"owner"},
			"displayName":                 u.DisplayName,
			"visibleHistoryStartDateTime": "0001-01-01T00:00:00Z",
			"userId":                      u.ID,
			"email":                       u.UserPrincipalName,
			"tenantId":                    TenantID,
		})
	}
	return response{status: 200, body: map[string]any{"@odata.count": len(out), "value": nonNil(out)}}
}

func (s *Server) chatUserAction(route routeMatch, body []byte, rec *Request) response {
	var payload struct {
		User struct {
			ID       string `json:"id"`
			TenantID string `json:"tenantId"`
		} `json:"user"`
		LastMessageReadDateTime string `json:"lastMessageReadDateTime"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.User.ID == "" {
		return violationResponse(rec, "graph-contract", "chat user action requires user.id")
	}
	if payload.User.ID != s.opts.Owner.ID {
		return response{status: 403, body: graphError("Forbidden", "can only act for the signed-in user")}
	}
	if payload.LastMessageReadDateTime != "" {
		if _, err := time.Parse(time.RFC3339Nano, payload.LastMessageReadDateTime); err != nil {
			return violationResponse(rec, "graph-contract", "lastMessageReadDateTime is not ISO 8601")
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c, resp := s.memberChatLocked(route.chatID)
	if c == nil {
		return resp
	}
	if strings.HasSuffix(route.name, "unhideForUser") {
		c.Hidden = false
	} else {
		c.UnreadMark++
	}
	return response{status: 204}
}

// ---------------------------------------------------------------------------
// Messages

type listOrder string

const (
	orderLastModified listOrder = "lastModifiedDateTime"
	orderCreated      listOrder = "createdDateTime"
)

type pageToken struct {
	Order   listOrder `json:"o"`
	GT      int64     `json:"gt,omitempty"`
	LT      int64     `json:"lt,omitempty"`
	AfterTS int64     `json:"ts"`
	AfterID string    `json:"id"`
	Top     int       `json:"top"`
	ChatID  string    `json:"c"`
}

func (s *Server) listMessages(chatID string, q url.Values, rec *Request) response {
	for k, v := range q {
		switch k {
		case "$top", "$orderby", "$filter", "$skiptoken":
			if len(v) != 1 {
				return violationResponse(rec, "graph-contract", k+" given more than once")
			}
		default:
			return violationResponse(rec, "graph-contract", "list chat messages does not support query option "+k)
		}
	}
	top := s.opts.DefaultPageSize
	if v := q.Get("$top"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 || n > 50 {
			return violationResponse(rec, "graph-contract", "list chat messages $top must be 1..50, got "+v)
		}
		top = n
	}
	var tok pageToken
	if raw := q.Get("$skiptoken"); raw != "" {
		data, err := base64.RawURLEncoding.DecodeString(raw)
		if err != nil || json.Unmarshal(data, &tok) != nil || tok.ChatID != chatID {
			return violationResponse(rec, "graph-contract", "invalid $skiptoken")
		}
		if q.Get("$top") == "" {
			top = tok.Top
		}
	} else {
		tok.Order = orderLastModified
		if ob := q.Get("$orderby"); ob != "" {
			switch ob {
			case "lastModifiedDateTime desc":
				tok.Order = orderLastModified
			case "createdDateTime desc":
				tok.Order = orderCreated
			default:
				return violationResponse(rec, "graph-contract", "list chat messages $orderby supports only 'lastModifiedDateTime desc' or 'createdDateTime desc', got '"+ob+"'")
			}
		}
		if f := q.Get("$filter"); f != "" {
			gt, lt, prop, reason := parseMessageFilter(f)
			if reason != "" {
				return violationResponse(rec, "graph-contract", reason)
			}
			if q.Get("$orderby") == "" || listOrder(prop) != tok.Order {
				return violationResponse(rec, "graph-contract", "$filter on "+prop+" requires $orderby="+prop+" desc")
			}
			if prop == string(orderCreated) && gt != 0 {
				return violationResponse(rec, "graph-contract", "createdDateTime filter supports only lt")
			}
			tok.GT, tok.LT = gt, lt
		}
		tok.ChatID = chatID
	}
	tok.Top = top

	s.mu.Lock()
	defer s.mu.Unlock()
	c, resp := s.memberChatLocked(chatID)
	if c == nil {
		return resp
	}
	key := func(m *Message) int64 {
		if tok.Order == orderCreated {
			return m.Created.UnixMilli()
		}
		return m.Modified.UnixMilli()
	}
	var matched []*Message
	for _, m := range c.Messages {
		k := key(m)
		if tok.GT != 0 && k <= tok.GT {
			continue
		}
		if tok.LT != 0 && k >= tok.LT {
			continue
		}
		matched = append(matched, m)
	}
	sort.SliceStable(matched, func(i, j int) bool {
		ki, kj := key(matched[i]), key(matched[j])
		if ki != kj {
			return ki > kj
		}
		return matched[i].ID > matched[j].ID
	})
	var page []any
	var last *Message
	for _, m := range matched {
		if tok.AfterID != "" {
			k := key(m)
			if k > tok.AfterTS || (k == tok.AfterTS && m.ID >= tok.AfterID) {
				continue
			}
		}
		if len(page) == top {
			break
		}
		page = append(page, m.toGraph())
		last = m
	}
	out := map[string]any{
		"@odata.context": "https://graph.microsoft.com/v1.0/$metadata#chats('" + chatID + "')/messages",
		"@odata.count":   len(page),
		"value":          nonNil(page),
	}
	if last != nil && len(page) == top {
		// Only emit a nextLink when more records exist.
		more := false
		lk := key(last)
		for _, m := range matched {
			k := key(m)
			if k < lk || (k == lk && m.ID < last.ID) {
				more = true
				break
			}
		}
		if more {
			next := tok
			next.AfterTS = lk
			next.AfterID = last.ID
			data, _ := json.Marshal(next)
			out["@odata.nextLink"] = "https://graph.microsoft.com/v1.0/chats/" + url.PathEscape(chatID) + "/messages?$top=" + strconv.Itoa(top) + "&$skiptoken=" + base64.RawURLEncoding.EncodeToString(data)
		}
	}
	return response{status: 200, body: out}
}

// parseMessageFilter accepts exactly what Graph documents for chat messages:
// lastModifiedDateTime with gt and/or lt, or createdDateTime with lt.
func parseMessageFilter(f string) (gt int64, lt int64, prop string, reason string) {
	clauses := strings.Split(f, " and ")
	if len(clauses) > 2 {
		return 0, 0, "", "list chat messages $filter supports at most two range clauses"
	}
	for _, c := range clauses {
		fields := strings.Fields(c)
		if len(fields) != 3 {
			return 0, 0, "", "malformed $filter clause '" + c + "'"
		}
		p, op, val := fields[0], fields[1], fields[2]
		if p != string(orderLastModified) && p != string(orderCreated) {
			return 0, 0, "", "list chat messages $filter does not support property " + p
		}
		if prop != "" && prop != p {
			return 0, 0, "", "list chat messages $filter cannot mix properties"
		}
		prop = p
		t, err := time.Parse(time.RFC3339Nano, val)
		if err != nil {
			return 0, 0, "", "$filter value is not an unquoted ISO 8601 timestamp: " + val
		}
		switch op {
		case "gt":
			gt = t.UnixMilli()
			if t.Sub(time.UnixMilli(gt)) > 0 {
				// Sub-millisecond bound: everything in that millisecond is gt.
				gt = t.Truncate(time.Millisecond).UnixMilli()
			}
		case "lt":
			lt = t.UnixMilli()
			if t.Sub(time.UnixMilli(lt)) > 0 {
				lt++
			}
		default:
			return 0, 0, "", "list chat messages $filter supports only gt/lt, got '" + op + "' (" + p + ")"
		}
	}
	if gt == 0 && lt == 0 {
		return 0, 0, "", "empty $filter"
	}
	return gt, lt, prop, ""
}

type postedMessage struct {
	Body struct {
		ContentType string `json:"contentType"`
		Content     string `json:"content"`
	} `json:"body"`
	Attachments    []json.RawMessage `json:"attachments"`
	Mentions       []json.RawMessage `json:"mentions"`
	HostedContents []json.RawMessage `json:"hostedContents"`
}

func (s *Server) postMessage(chatID string, body []byte, rec *Request, quote bool) response {
	var pm postedMessage
	var quoted []string
	if quote {
		var payload struct {
			MessageIDs   []string        `json:"messageIds"`
			ReplyMessage json.RawMessage `json:"replyMessage"`
		}
		if err := json.Unmarshal(body, &payload); err != nil || len(payload.MessageIDs) == 0 || len(payload.ReplyMessage) == 0 {
			return violationResponse(rec, "graph-contract", "replyWithQuote requires messageIds and replyMessage")
		}
		if err := json.Unmarshal(payload.ReplyMessage, &pm); err != nil {
			return violationResponse(rec, "graph-contract", "replyMessage is not a chatMessage")
		}
		quoted = payload.MessageIDs
	} else if err := json.Unmarshal(body, &pm); err != nil {
		return violationResponse(rec, "graph-contract", "POST message body is not a chatMessage JSON object")
	}
	if reason := validateMessagePayload(pm, true); reason != "" {
		return violationResponse(rec, "graph-contract", reason)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c, resp := s.memberChatLocked(chatID)
	if c == nil {
		return resp
	}
	if reason := s.checkReferenceAttachmentsLocked(pm.Attachments); reason != "" {
		return violationResponse(rec, "graph-contract", reason)
	}
	for _, id := range quoted {
		found := false
		for _, m := range c.Messages {
			if m.ID == id {
				found = true
			}
		}
		if !found {
			return response{status: 404, body: graphError("NotFound", "quoted message not found")}
		}
	}
	m := s.appendMessageLocked(c, OriginSUT, s.opts.Owner, pm.Body.ContentType, pm.Body.Content)
	m.Attachments = pm.Attachments
	m.Mentions = pm.Mentions
	m.Hosted = len(pm.HostedContents)
	m.HostedHashes = hostedHashes(pm.HostedContents)
	m.QuotedIDs = quoted
	m.Instance = rec.Instance
	m.RequestSeq = rec.Seq
	return response{status: 201, body: m.toGraph()}
}

var atTagID = func(content string) []string {
	var ids []string
	rest := content
	for {
		i := strings.Index(rest, "<at id=")
		if i < 0 {
			return ids
		}
		rest = rest[i+len("<at id="):]
		q := byte('"')
		if len(rest) > 0 && (rest[0] == '"' || rest[0] == '\'') {
			q = rest[0]
			rest = rest[1:]
		}
		j := strings.IndexByte(rest, q)
		if j < 0 {
			return ids
		}
		ids = append(ids, rest[:j])
	}
}

// hostedHashes returns "sha256:<hex>" of each posted hosted content's
// contentBytes ("sha256:invalid" when it is not base64).
func hostedHashes(items []json.RawMessage) []string {
	var out []string
	for _, raw := range items {
		var hc struct {
			ContentBytes string `json:"contentBytes"`
		}
		_ = json.Unmarshal(raw, &hc)
		data, err := base64.StdEncoding.DecodeString(hc.ContentBytes)
		if err != nil {
			out = append(out, "sha256:invalid")
			continue
		}
		out = append(out, HashBytes(data))
	}
	return out
}

// HashBytes is the "sha256:<hex>" form used for hosted contents and files.
func HashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func validateMessagePayload(pm postedMessage, requireBody bool) string {
	ct := pm.Body.ContentType
	if ct == "" {
		ct = "text"
	}
	if ct != "html" && ct != "text" {
		return "body.contentType must be html or text, got " + ct
	}
	if requireBody && strings.TrimSpace(pm.Body.Content) == "" && len(pm.Attachments) == 0 {
		return "message body content is empty"
	}
	if len(pm.Body.Content) > maxMessageContentBytes {
		return fmt.Sprintf("message body is %d bytes, over the Teams limit of %d", len(pm.Body.Content), maxMessageContentBytes)
	}
	tagIDs := atTagID(pm.Body.Content)
	mentionIDs := map[string]bool{}
	for _, raw := range pm.Mentions {
		var mention struct {
			ID          json.Number `json:"id"`
			MentionText string      `json:"mentionText"`
			Mentioned   struct {
				User *struct {
					ID string `json:"id"`
				} `json:"user"`
			} `json:"mentioned"`
		}
		if err := json.Unmarshal(raw, &mention); err != nil {
			return "mention is malformed: " + err.Error()
		}
		if mention.Mentioned.User == nil || mention.Mentioned.User.ID == "" {
			return "mention.mentioned.user.id is required"
		}
		mentionIDs[mention.ID.String()] = true
	}
	tagSet := map[string]bool{}
	for _, id := range tagIDs {
		tagSet[id] = true
		if !mentionIDs[id] {
			return "content has <at id=\"" + id + "\"> without a matching mentions[] entry"
		}
	}
	for id := range mentionIDs {
		if !tagSet[id] {
			return "mentions[] entry " + id + " has no matching <at> tag in content"
		}
	}
	for _, raw := range pm.Attachments {
		var att struct {
			ID          string `json:"id"`
			ContentType string `json:"contentType"`
		}
		if err := json.Unmarshal(raw, &att); err != nil || att.ID == "" {
			return "attachment requires id"
		}
		if !strings.Contains(pm.Body.Content, `<attachment id="`+att.ID+`"`) {
			return "attachment " + att.ID + " is not referenced by an <attachment> tag in content"
		}
	}
	return ""
}

func (s *Server) appendMessageLocked(c *Chat, origin Origin, from User, contentType, content string) *Message {
	if contentType == "" {
		contentType = "text"
	}
	if s.opts.StripHTMLComments && contentType == "html" {
		content = stripComments(content)
	}
	stamp := s.nextStampLocked()
	m := &Message{
		ID:          strconv.FormatInt(stamp.UnixMilli(), 10),
		ChatID:      c.ID,
		Created:     stamp,
		Modified:    stamp,
		MessageType: "message",
		FromUserID:  from.ID,
		FromName:    from.DisplayName,
		ContentType: contentType,
		Content:     content,
		Origin:      origin,
	}
	c.Messages = append(c.Messages, m)
	return m
}

func stripComments(s string) string {
	for {
		i := strings.Index(s, "<!--")
		if i < 0 {
			return s
		}
		j := strings.Index(s[i:], "-->")
		if j < 0 {
			return s[:i]
		}
		s = s[:i] + s[i+j+3:]
	}
}

func (s *Server) patchMessage(chatID, messageID string, body []byte, rec *Request) response {
	var pm postedMessage
	if err := json.Unmarshal(body, &pm); err != nil {
		return violationResponse(rec, "graph-contract", "PATCH message body is not JSON")
	}
	var raw map[string]json.RawMessage
	_ = json.Unmarshal(body, &raw)
	for k := range raw {
		switch k {
		case "body", "attachments", "mentions", "hostedContents", "subject", "importance", "reactions", "policyViolation":
		default:
			return violationResponse(rec, "graph-contract", "PATCH message: property "+k+" is not updatable")
		}
	}
	if _, ok := raw["body"]; ok {
		if reason := validateMessagePayload(pm, false); reason != "" {
			return violationResponse(rec, "graph-contract", reason)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c, resp := s.memberChatLocked(chatID)
	if c == nil {
		return resp
	}
	for _, m := range c.Messages {
		if m.ID != messageID {
			continue
		}
		if m.FromUserID != s.opts.Owner.ID {
			return response{status: 403, body: graphError("Forbidden", "only the sender can edit a message")}
		}
		if _, ok := raw["body"]; ok {
			m.PatchHistory = append(m.PatchHistory, m.Content)
			ct := pm.Body.ContentType
			if ct == "" {
				ct = "text"
			}
			content := pm.Body.Content
			if s.opts.StripHTMLComments && ct == "html" {
				content = stripComments(content)
			}
			m.ContentType = ct
			m.Content = content
		}
		if _, ok := raw["attachments"]; ok {
			m.Attachments = pm.Attachments
		}
		if _, ok := raw["mentions"]; ok {
			m.Mentions = pm.Mentions
		}
		m.PatchCount++
		m.Modified = s.nextStampLocked()
		return response{status: 204}
	}
	return response{status: 404, body: graphError("NotFound", "message not found")}
}
