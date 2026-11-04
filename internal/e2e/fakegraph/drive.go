package fakegraph

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Outbound files (`helper file`, artifact manifests) as Graph documents
// them for the signed-in user's OneDrive:
//
//   - PUT /me/drive/root:/{path}:/content                small file, one request
//   - POST /me/drive/root:/{path}:/createUploadSession   large file: returns
//     an uploadUrl on the SharePoint host; the client PUTs byte ranges to it
//     WITHOUT an Authorization header (Content-Range, 320 KiB-aligned
//     fragments < 60 MiB), gets 202 + nextExpectedRanges, and 200/201 + the
//     driveItem on the last fragment; GET uploadUrl reports
//     nextExpectedRanges, DELETE cancels;
//   - GET /me/drive/items/{id}[/content]
//
// then POST /chats/{id}/messages with a reference attachment whose id is the
// GUID of the item's eTag and whose contentUrl is the item's webUrl or
// webDavUrl. See FIDELITY.md ("Outbound files").

const (
	uploadFragmentAlign = 320 * 1024
	uploadFragmentMax   = 60 * 1024 * 1024
	uploadPathPrefix    = "/_e2e/uploadSession/"
)

var guidRE = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)

type driveItem struct {
	id       string
	path     string // "/Microsoft Teams Chat Files/x.txt" (decoded)
	name     string
	data     []byte
	guid     string
	version  int
	created  time.Time
	modified time.Time
	via      []string // "put" | "session", one per write
}

func (d *driveItem) webURL() string {
	return "https://" + SharePointHost + "/personal/owner_e2e_example/Documents" + escapeDrivePath(d.path)
}

func (d *driveItem) webDavURL() string {
	return "https://" + SharePointHost + "/personal/owner_e2e_example/Documents" + escapeDrivePath(d.path)
}

func escapeDrivePath(p string) string {
	parts := strings.Split(p, "/")
	for i, s := range parts {
		parts[i] = url.PathEscape(s)
	}
	return strings.Join(parts, "/")
}

func (d *driveItem) toGraph() map[string]any {
	parent := d.path[:strings.LastIndex(d.path, "/")]
	sum := sha256.Sum256(d.data)
	return map[string]any{
		"@odata.context":       "https://graph.microsoft.com/v1.0/$metadata#drives('b!e2e')/items/$entity",
		"id":                   d.id,
		"name":                 d.name,
		"size":                 len(d.data),
		"eTag":                 fmt.Sprintf(`"{%s},%d"`, strings.ToUpper(d.guid), d.version),
		"cTag":                 fmt.Sprintf(`"c:{%s},%d"`, strings.ToUpper(d.guid), d.version),
		"createdDateTime":      formatTime(d.created),
		"lastModifiedDateTime": formatTime(d.modified),
		"webUrl":               d.webURL(),
		"file":                 map[string]any{"mimeType": "application/octet-stream", "hashes": map[string]any{"sha256Hash": strings.ToUpper(hex.EncodeToString(sum[:]))}},
		"parentReference":      map[string]any{"driveType": "business", "driveId": "b!e2e", "path": "/drive/root:" + parent},
	}
}

type uploadSession struct {
	id       string
	path     string
	name     string
	conflict string
	total    int64 // -1 until the first fragment
	data     []byte
	expires  time.Time
	done     bool
	fragment int
}

// DriveItemInfo is a snapshot of one file in the owner's fake OneDrive.
type DriveItemInfo struct {
	ID        string
	Path      string
	Name      string
	Data      []byte
	SHA256    string
	ETagGUID  string
	WebURL    string
	Writes    []string // how each write arrived: "put" or "session"
	Fragments int      // upload-session fragments of the last session write
}

// DriveItems returns every file uploaded through the drive API, oldest
// first.
func (s *Server) DriveItems() []DriveItemInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []DriveItemInfo
	for _, id := range s.driveOrder {
		d := s.items[id]
		sum := sha256.Sum256(d.data)
		out = append(out, DriveItemInfo{
			ID: d.id, Path: d.path, Name: d.name, Data: append([]byte(nil), d.data...),
			SHA256: hex.EncodeToString(sum[:]), ETagGUID: strings.ToLower(d.guid), WebURL: d.webURL(),
			Writes: append([]string(nil), d.via...), Fragments: s.lastFragments[d.id],
		})
	}
	return out
}

func (s *Server) newGUIDLocked() string {
	s.idCounter++
	return fmt.Sprintf("e2ed41fe-%04x-4000-8000-%012x", s.idCounter&0xffff, s.idCounter)
}

func (s *Server) itemByPathLocked(p string) *driveItem {
	if s.items == nil {
		return nil
	}
	for _, d := range s.items {
		if strings.EqualFold(d.path, p) {
			return d
		}
	}
	return nil
}

// writeItemLocked creates or replaces the item at p. created reports
// whether it is new.
func (s *Server) writeItemLocked(p, name string, data []byte, via string) (*driveItem, bool) {
	if s.items == nil {
		s.items = map[string]*driveItem{}
		s.lastFragments = map[string]int{}
	}
	now := s.nextStampLocked()
	d := s.itemByPathLocked(p)
	created := d == nil
	if created {
		s.idCounter++
		d = &driveItem{id: fmt.Sprintf("01E2EDRIVE%08dITEM", s.idCounter), path: p, name: name, guid: s.newGUIDLocked(), created: now}
		s.items[d.id] = d
		s.driveOrder = append(s.driveOrder, d.id)
	}
	d.data = append([]byte(nil), data...)
	d.version++
	d.modified = now
	d.via = append(d.via, via)
	// Make the file resolvable like an inbound reference attachment, too.
	if s.drive == nil {
		s.drive = map[string]driveFile{}
	}
	s.drive[d.webURL()] = driveFile{name: d.name, data: d.data}
	return d, created
}

// drivePathRoute recognises /me/drive/root:/{path}:/{action}.
func drivePathRoute(method, p string, m routeMatch) (routeMatch, bool) {
	const prefix = "/me/drive/root:/"
	if !strings.HasPrefix(p, prefix) {
		return m, false
	}
	rest := p[len(prefix)-1:]
	i := strings.LastIndex(rest, ":/")
	if i <= 0 {
		return m, false
	}
	m.extra = rest[:i]
	switch action := rest[i+2:]; {
	case action == "content" && method == http.MethodPut:
		m.name = "PUT /me/drive/root:/{path}:/content"
	case action == "createUploadSession" && method == http.MethodPost:
		m.name = "POST /me/drive/root:/{path}:/createUploadSession"
	default:
		return m, false
	}
	return m, true
}

func validDrivePath(p string) string {
	if p == "" || !strings.HasPrefix(p, "/") || strings.HasSuffix(p, "/") {
		return "drive path must be /folder/name"
	}
	for _, seg := range strings.Split(p[1:], "/") {
		if seg == "" || seg == "." || seg == ".." || strings.ContainsAny(seg, `"*:<>?\|`) {
			return "drive path segment " + strconv.Quote(seg) + " is not a valid OneDrive name"
		}
	}
	return ""
}

func conflictBehavior(q url.Values, def string) (string, string) {
	v := q.Get("@microsoft.graph.conflictBehavior")
	if v == "" {
		return def, ""
	}
	switch v {
	case "fail", "replace", "rename":
		if v == "rename" {
			return "", "conflictBehavior rename is not modelled"
		}
		return v, ""
	}
	return "", "invalid @microsoft.graph.conflictBehavior " + v
}

func (s *Server) drivePutContent(route routeMatch, q url.Values, body []byte, rec *Request) response {
	for k := range q {
		if k != "@microsoft.graph.conflictBehavior" {
			return violationResponse(rec, "graph-contract", "PUT :/content unsupported query option "+k)
		}
	}
	if v := validDrivePath(route.extra); v != "" {
		return violationResponse(rec, "graph-contract", v)
	}
	behavior, v := conflictBehavior(q, "replace")
	if v != "" {
		return violationResponse(rec, "unmodeled", v)
	}
	if len(body) > 250*1024*1024 {
		return response{status: 413, body: graphError("requestTooLarge", "simple upload is limited to 250 MB")}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if behavior == "fail" && s.itemByPathLocked(route.extra) != nil {
		return response{status: 409, body: graphError("nameAlreadyExists", "The specified item name already exists.")}
	}
	name := route.extra[strings.LastIndex(route.extra, "/")+1:]
	d, created := s.writeItemLocked(route.extra, name, body, "put")
	status := 200
	if created {
		status = 201
	}
	return response{status: status, body: d.toGraph()}
}

func (s *Server) driveCreateUploadSession(route routeMatch, q url.Values, body []byte, rec *Request) response {
	if len(q) > 0 {
		return violationResponse(rec, "graph-contract", "createUploadSession takes no query options (conflict behaviour goes in the body)")
	}
	if v := validDrivePath(route.extra); v != "" {
		return violationResponse(rec, "graph-contract", v)
	}
	var payload struct {
		Item map[string]json.RawMessage `json:"item"`
	}
	if len(body) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			return violationResponse(rec, "graph-contract", "createUploadSession body must be {\"item\":{...}}")
		}
	}
	behavior := "replace"
	name := route.extra[strings.LastIndex(route.extra, "/")+1:]
	for k, raw := range payload.Item {
		var sv string
		_ = json.Unmarshal(raw, &sv)
		switch k {
		case "@microsoft.graph.conflictBehavior":
			if sv != "fail" && sv != "replace" {
				return violationResponse(rec, "unmodeled", "conflictBehavior "+sv+" is not modelled")
			}
			behavior = sv
		case "name":
			if sv != name {
				return violationResponse(rec, "graph-contract", "item.name "+strconv.Quote(sv)+" does not match the path name "+strconv.Quote(name))
			}
		case "@odata.type", "description", "fileSize", "fileSystemInfo":
		default:
			return violationResponse(rec, "graph-contract", "createUploadSession item has unsupported property "+k)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if behavior == "fail" && s.itemByPathLocked(route.extra) != nil {
		return response{status: 409, body: graphError("nameAlreadyExists", "The specified item name already exists.")}
	}
	if s.sessions == nil {
		s.sessions = map[string]*uploadSession{}
	}
	s.idCounter++
	us := &uploadSession{
		id: fmt.Sprintf("e2eup%06d", s.idCounter), path: route.extra, name: name, conflict: behavior,
		total: -1, expires: time.Now().Add(15 * time.Minute),
	}
	s.sessions[us.id] = us
	return response{status: 200, body: map[string]any{
		"@odata.context":     "https://graph.microsoft.com/v1.0/$metadata#microsoft.graph.uploadSession",
		"uploadUrl":          "https://" + SharePointHost + uploadPathPrefix + us.id + "?tempauth=e2e-" + us.id,
		"expirationDateTime": us.expires.UTC().Format(time.RFC3339Nano),
		"nextExpectedRanges": []string{"0-"},
	}}
}

func (s *Server) driveGetItem(id string, content bool, q url.Values, rec *Request) response {
	if content && len(q) > 0 {
		return violationResponse(rec, "graph-contract", "GET item content takes no query options")
	}
	var sel []string
	for k := range q {
		if k != "$select" {
			return violationResponse(rec, "graph-contract", "GET driveItem unsupported query option "+k)
		}
		sel = strings.Split(q.Get("$select"), ",")
	}
	s.mu.Lock()
	d := s.items[id]
	s.mu.Unlock()
	if d == nil {
		return response{status: 404, body: graphError("itemNotFound", "The resource could not be found.")}
	}
	if content {
		return response{status: 200, raw: d.data, rawType: "application/octet-stream"}
	}
	full := d.toGraph()
	full["webDavUrl"] = d.webDavURL()
	if len(sel) == 0 {
		delete(full, "webDavUrl") // not part of the default projection (assumed)
		return response{status: 200, body: full}
	}
	out := map[string]any{"@odata.context": full["@odata.context"]}
	for _, f := range sel {
		f = strings.TrimSpace(f)
		v, ok := full[f]
		if !ok {
			return violationResponse(rec, "graph-contract", "unknown driveItem property in $select: "+f)
		}
		out[f] = v
	}
	return response{status: 200, body: out}
}

// handleUploadURL serves the SharePoint-host upload session URL.
func (s *Server) handleUploadURL(r *http.Request, body []byte, rec *Request) response {
	if !strings.HasPrefix(r.URL.Path, uploadPathPrefix) {
		return violationResponse(rec, "unmodeled", "no such SharePoint route in the fake: "+r.URL.Path)
	}
	if r.Header.Get("Authorization") != "" {
		return violationResponse(rec, "graph-contract", "upload session requests must not carry an Authorization header (the uploadUrl is pre-authenticated)")
	}
	id := strings.TrimPrefix(r.URL.Path, uploadPathPrefix)
	s.mu.Lock()
	defer s.mu.Unlock()
	us := s.sessions[id]
	if us == nil || us.done || time.Now().After(us.expires) {
		return response{status: 404, body: graphError("itemNotFound", "The upload session was not found.")}
	}
	next := func() map[string]any {
		return map[string]any{"expirationDateTime": us.expires.UTC().Format(time.RFC3339Nano), "nextExpectedRanges": []string{fmt.Sprintf("%d-", len(us.data))}}
	}
	switch r.Method {
	case http.MethodGet:
		return response{status: 200, body: next()}
	case http.MethodDelete:
		delete(s.sessions, id)
		return response{status: 204}
	case http.MethodPut:
	default:
		return violationResponse(rec, "graph-contract", r.Method+" is not valid on an upload session URL")
	}
	var start, end, total int64
	if _, err := fmt.Sscanf(r.Header.Get("Content-Range"), "bytes %d-%d/%d", &start, &end, &total); err != nil {
		return violationResponse(rec, "graph-contract", "fragment PUT requires Content-Range: bytes start-end/total, got "+strconv.Quote(r.Header.Get("Content-Range")))
	}
	length := end - start + 1
	switch {
	case length <= 0 || end >= total:
		return violationResponse(rec, "graph-contract", "Content-Range is inconsistent")
	case int64(len(body)) != length:
		return violationResponse(rec, "graph-contract", fmt.Sprintf("fragment body is %d bytes, Content-Range says %d", len(body), length))
	case length >= uploadFragmentMax:
		return violationResponse(rec, "graph-contract", "fragments must be smaller than 60 MiB")
	case end+1 < total && length%uploadFragmentAlign != 0:
		return violationResponse(rec, "graph-contract", "non-final fragments must be a multiple of 320 KiB")
	case us.total >= 0 && total != us.total:
		return violationResponse(rec, "graph-contract", "Content-Range total changed within one session")
	}
	if start != int64(len(us.data)) {
		return response{status: http.StatusRequestedRangeNotSatisfiable, body: graphError("invalidRange", "The uploaded fragment does not start at the next expected range.")}
	}
	us.total = total
	us.data = append(us.data, body...)
	us.fragment++
	if int64(len(us.data)) < total {
		return response{status: 202, body: next()}
	}
	us.done = true
	if us.conflict == "fail" && s.itemByPathLocked(us.path) != nil {
		return response{status: 409, body: graphError("nameAlreadyExists", "The specified item name already exists.")}
	}
	d, created := s.writeItemLocked(us.path, us.name, us.data, "session")
	s.lastFragments[d.id] = us.fragment
	status := 200
	if created {
		status = 201
	}
	return response{status: status, body: d.toGraph()}
}

// checkReferenceAttachmentsLocked validates reference attachments in a
// posted message against the drive: the contentUrl must be an uploaded
// item's webUrl/webDavUrl (or a known inbound file) and the id the GUID of
// its eTag.
func (s *Server) checkReferenceAttachmentsLocked(atts []json.RawMessage) string {
	for _, raw := range atts {
		var att struct {
			ID          string `json:"id"`
			ContentType string `json:"contentType"`
			ContentURL  string `json:"contentUrl"`
			Name        string `json:"name"`
		}
		_ = json.Unmarshal(raw, &att)
		if att.ContentType != "reference" {
			continue
		}
		var match *driveItem
		for _, d := range s.items {
			if att.ContentURL == d.webURL() || att.ContentURL == d.webDavURL() {
				match = d
			}
		}
		if match == nil {
			if _, inbound := s.drive[att.ContentURL]; inbound {
				continue
			}
			return "reference attachment contentUrl " + att.ContentURL + " is not a file in the drive"
		}
		if !strings.EqualFold(att.ID, match.guid) {
			return "reference attachment id " + att.ID + " is not the eTag GUID of " + match.path
		}
		if strings.TrimSpace(att.Name) == "" {
			return "reference attachment requires a name"
		}
	}
	return ""
}

// OutboundFile is a reference attachment the SUT posted, resolved against
// the drive.
type OutboundFile struct {
	Message Message
	Name    string
	Item    DriveItemInfo
}

// OutboundFiles returns every SUT message attachment that points at a file
// uploaded to the drive, in chat order.
func (s *Server) OutboundFiles(chatID string) []OutboundFile {
	items := s.DriveItems()
	var out []OutboundFile
	for _, m := range s.Messages(chatID) {
		if m.Origin != OriginSUT {
			continue
		}
		for _, raw := range m.Attachments {
			var att struct {
				ContentType string `json:"contentType"`
				ContentURL  string `json:"contentUrl"`
				Name        string `json:"name"`
			}
			_ = json.Unmarshal(raw, &att)
			if att.ContentType != "reference" {
				continue
			}
			for _, it := range items {
				if strings.HasPrefix(att.ContentURL, "https://"+SharePointHost+"/personal/owner_e2e_example/Documents") && att.ContentURL == it.WebURL {
					out = append(out, OutboundFile{Message: m, Name: att.Name, Item: it})
				}
			}
		}
	}
	return out
}
