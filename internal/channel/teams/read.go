package teams

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/baaaaaaaka/codex-helper/internal/channel"
)

type readRecord struct {
	graphMessage
	ETag    string `json:"etag"`
	ReplyTo string `json:"replyToId"`
}
type readPage struct {
	Value []readRecord `json:"value"`
	Next  string       `json:"@odata.nextLink"`
}
type readCursor struct {
	Target    ReadTarget `json:"target"`
	Kind      string     `json:"kind"`
	Remaining int        `json:"remaining"`
	Bound     string     `json:"bound,omitempty"`
	Next      string     `json:"next,omitempty"`
	Skip      []string   `json:"skip,omitempty"`
	Offset    int        `json:"offset,omitempty"`
	Revision  string     `json:"revision,omitempty"`
}
type attachmentRef struct {
	Target ReadTarget `json:"target"`
	Kind   string     `json:"kind"`
	ID     string     `json:"id"`
}

const attachmentPrefix = "cxp-teams-file-v1:"

func (c readCursor) encode() string { return encodeReadToken(cursorPrefix, c) }
func (c readCursor) validate(tenant string) error {
	if e := c.Target.validate(tenant); e != nil {
		return e
	}
	if c.Remaining < 0 || c.Remaining > 10000 || c.Offset < 0 || c.Offset > 64<<20 || len(c.Skip) > 50 || len(c.Revision) > 4096 {
		return errors.New("invalid read cursor bounds")
	}
	for _, s := range c.Skip {
		if !validReadID(s) {
			return errors.New("invalid cursor message ID")
		}
	}
	if c.Kind != "body" && (c.Offset != 0 || c.Revision != "") || (c.Kind == "body" || c.Kind == "direct") && (len(c.Skip) != 0 || c.Bound != "") || c.Kind != "before" && c.Bound != "" {
		return errors.New("conflicting read cursor fields")
	}
	switch c.Kind {
	case "direct":
		if c.Target.Message == "" || c.Remaining != 1 || c.Next != "" || c.Offset != 0 {
			return errors.New("invalid direct cursor")
		}
	case "body":
		if c.Target.Message == "" || c.Remaining != 1 || c.Next != "" || c.Offset == 0 || c.Revision == "" {
			return errors.New("invalid body cursor")
		}
	case "before":
		if c.Target.Kind != "chat_message" || c.Remaining == 0 {
			return errors.New("invalid before cursor")
		}
		if _, e := time.Parse(time.RFC3339Nano, c.Bound); e != nil {
			return errors.New("invalid cursor time")
		}
	case "latest":
		if c.Target.Chat == "" || c.Remaining == 0 {
			return errors.New("invalid latest cursor")
		}
	case "replies":
		if c.Target.Kind != "channel_message" || c.Remaining == 0 {
			return errors.New("invalid replies cursor")
		}
	default:
		return errors.New("unsupported cursor kind")
	}
	return nil
}

// ValidateRead checks the complete command before acquiring credentials or IO.
func ValidateRead(targets []string, o ReadOptions, tenant string) error {
	if e := o.Budget.validate(); e != nil {
		return e
	}
	if o.Before < 0 || o.Before > 10000 || o.Latest < 0 || o.Latest > 10000 || o.Limit < 1 || o.Limit > 10000 {
		return errors.New("range counts must be 1..10000")
	}
	if o.Cursor != "" {
		if len(targets) > 0 || o.Before != 0 || o.Latest != 0 || o.Replies {
			return errors.New("cursor cannot be combined with targets or new ranges")
		}
		var c readCursor
		if e := decodeReadToken(o.Cursor, cursorPrefix, &c); e != nil {
			return e
		}
		return c.validate(tenant)
	}
	if len(targets) == 0 || len(targets) > 200 {
		return errors.New("read requires 1..200 targets")
	}
	if len(targets) > 1 && (o.Before > 0 || o.Latest > 0 || o.Replies) {
		return errors.New("multiple targets support direct message reads only")
	}
	for _, raw := range targets {
		t, e := ParseReadTarget(raw, tenant)
		if e != nil {
			return e
		}
		if len(targets) > 1 && t.Message == "" {
			return errors.New("multiple targets must be messages")
		}
		if o.Before > 0 && t.Kind != "chat_message" || o.Latest > 0 && t.Chat == "" || o.Replies && t.Kind != "channel_message" {
			return errors.New("range is not supported for this resource type")
		}
	}
	return nil
}

type readRun struct {
	r                                         *Reader
	g                                         *graph
	ctx                                       context.Context
	result                                    ReadResult
	cache                                     map[string]readRecord
	cacheErrors                               map[string]error
	pages                                     map[string]readPageResult
	emitted                                   map[string]int
	chars, maxChars, maxMessages, outputBytes int
	failed                                    bool
}

func (r *Reader) Read(ctx context.Context, targets []string, o ReadOptions) (ReadResult, error) {
	out := ReadResult{Version: 1, FetchedAt: r.a.cfg.Now().UTC(), Resources: []ReadResource{}, Messages: []ReadMessage{}, Ranges: []ReadRange{}}
	if e := ValidateRead(targets, o, r.a.cfg.TenantID); e != nil {
		return out, e
	}
	ctx, cancel, g, m, e := r.session(ctx, o.Budget)
	if e != nil {
		return out, e
	}
	defer cancel()
	run := readRun{r: r, g: g, ctx: ctx, result: out, cache: map[string]readRecord{}, cacheErrors: map[string]error{}, emitted: map[string]int{}, maxChars: o.Budget.MaxChars, maxMessages: o.Budget.MaxMessages}
	if o.Cursor != "" {
		var c readCursor
		_ = decodeReadToken(o.Cursor, cursorPrefix, &c)
		if c.Kind == "body" {
			run.body(c)
		} else if c.Kind == "direct" {
			run.direct(c.Target)
		} else {
			run.walk(c)
		}
	} else {
		ts := make([]ReadTarget, 0, len(targets))
		seen := map[string]bool{}
		for _, raw := range targets {
			t, _ := ParseReadTarget(raw, r.a.cfg.TenantID)
			if !seen[t.Ref()] {
				ts = append(ts, t)
				seen[t.Ref()] = true
			}
		}
		if len(ts) > 1 {
			run.prefetch(ts[:min(len(ts), run.maxMessages)])
		}
		for _, t := range ts {
			run.direct(t)
		}
		if len(ts) == 1 {
			t := ts[0]
			if o.Before > 0 && o.Latest > 0 {
				if anchor, ok := run.cache[t.Ref()]; ok {
					if _, err := time.Parse(time.RFC3339Nano, anchor.Created); err == nil {
						run.prefetchWindows([]readCursor{{Target: t, Kind: "before", Remaining: o.Before, Bound: anchor.Created}, {Target: t, Kind: "latest", Remaining: o.Latest}})
					}
				}
			}
			if o.Before > 0 {
				anchor, ok := run.cache[t.Ref()]
				if ok {
					if _, e := time.Parse(time.RFC3339Nano, anchor.Created); e != nil {
						run.rangeError("before", t, o.Before, "anchor has no valid creation time")
					} else {
						run.walk(readCursor{Target: t, Kind: "before", Remaining: o.Before, Bound: anchor.Created})
					}
				} else {
					run.rangeError("before", t, o.Before, "anchor could not be read")
				}
			}
			if o.Latest > 0 {
				run.walk(readCursor{Target: t, Kind: "latest", Remaining: o.Latest})
			}
			if o.Replies {
				root := t.root()
				if root != t {
					run.direct(root)
				}
				run.walk(readCursor{Target: root, Kind: "replies", Remaining: o.Limit})
			}
		}
	}
	run.result.Usage = m.usage(run.chars)
	for i := range run.result.Ranges {
		rg := &run.result.Ranges[i]
		if len(rg.NextCursor) > maxReadToken {
			rg.NextCursor = ""
			rg.State = "error"
			rg.Error = "continuation exceeds reference limit; reduce the requested range and read again"
			run.failed = true
		}
	}
	if run.failed {
		return run.result, errors.New("one or more Teams ranges failed; inspect ranges")
	}
	return run.result, nil
}
func (r *readRun) rangeError(kind string, t ReadTarget, n int, msg string) {
	r.failed = true
	r.result.Ranges = append(r.result.Ranges, ReadRange{Kind: kind, Target: t, Requested: n, Refs: []string{}, State: "error", Error: msg, Order: "created_asc_within_window"})
}
func (r *readRun) get(t ReadTarget) (readRecord, error) {
	if m, ok := r.cache[t.Ref()]; ok {
		return m, nil
	}
	if e := r.cacheErrors[t.Ref()]; e != nil {
		return readRecord{}, e
	}
	var m readRecord
	if e := r.g.getJSON(r.ctx, t.path(), smallBodyLimit, &m); e != nil {
		return m, e
	}
	if m.ID == "" || m.ID != t.Message {
		return m, errors.New("Graph returned a mismatched message ID")
	}
	r.cache[t.Ref()] = m
	return m, nil
}
func (r *readRun) direct(t ReadTarget) {
	if t.Message == "" {
		var meta struct {
			ID    string `json:"id"`
			Topic string `json:"topic"`
			Name  string `json:"displayName"`
		}
		if e := r.g.getJSON(r.ctx, t.path(), smallBodyLimit, &meta); e != nil {
			r.rangeError("resource", t, 1, readFailure(e))
			return
		}
		want := t.Chat
		if want == "" {
			want = t.Channel
		}
		if meta.ID != want {
			r.rangeError("resource", t, 1, "Graph returned a mismatched resource")
			return
		}
		r.result.Resources = append(r.result.Resources, ReadResource{t, t.Ref(), t.webURL(), first(meta.Topic, meta.Name)})
		return
	}
	c := readCursor{Target: t, Kind: "direct", Remaining: 1}
	rg := ReadRange{Kind: "direct", Target: t, Requested: 1, Refs: []string{}, State: "complete", Order: "created_asc_within_window"}
	if len(r.result.Messages) >= r.maxMessages || r.chars >= r.maxChars {
		rg.State = "truncated"
		rg.NextCursor = c.encode()
	} else {
		m, e := r.get(t)
		if e != nil {
			rg.State = "error"
			rg.Error = readFailure(e)
			r.failed = true
			rg.NextCursor = c.encode()
		} else if ok, e := r.add(t, m, 0, ""); e != nil {
			rg.State = "error"
			rg.Error = e.Error()
			r.failed = true
			rg.NextCursor = c.encode()
		} else if !ok {
			rg.State = "truncated"
			rg.NextCursor = c.encode()
		} else {
			rg.Refs = append(rg.Refs, t.Ref())
			rg.Oldest, rg.Newest = m.Created, m.Created
			if r.result.Messages[r.emitted[t.Ref()]].Truncated {
				rg.State = "truncated"
				rg.NextCursor = r.result.Messages[r.emitted[t.Ref()]].NextCursor
			}
		}
	}
	r.result.Ranges = append(r.result.Ranges, rg)
}
func (r *readRun) add(t ReadTarget, m readRecord, offset int, revision string) (bool, error) {
	if _, ok := r.emitted[t.Ref()]; ok {
		return true, nil
	}
	if len(r.result.Messages) >= r.maxMessages || r.chars >= r.maxChars || r.outputBytes >= 256<<10 {
		return false, nil
	}
	text := m.Body.Content
	if m.Body.ContentType != "text" {
		text = channel.WithLinks(strings.TrimSpace(plainText(text)), namedLinks(text))
	}
	attachments := r.attachments(t, m)
	for _, at := range m.Attachments {
		if at.Content != "" && at.ContentType != "reference" && at.ContentType != "messageReference" && at.ContentType != "forwardedMessageReference" {
			text += "\n\nAttachment (" + at.ContentType + "):\n" + at.Content
		}
	}
	sum := sha256.Sum256([]byte(m.ETag + "\x00" + m.Modified + "\x00" + text))
	rev := fmt.Sprintf("%x", sum)
	if revision != "" && revision != rev {
		return false, errors.New("message changed during body continuation; read the message again")
	}
	if offset > utf8.RuneCountInString(text) {
		return false, errors.New("body continuation offset no longer exists")
	}
	rs := []rune(text)
	remain := r.maxChars - r.chars
	end := min(len(rs), offset+remain)
	out := ReadMessage{Ref: t.Ref(), ID: m.ID, Target: t, URL: t.webURL(), Created: m.Created, Modified: m.Modified, Deleted: m.Deleted != nil && *m.Deleted != "", Type: m.MessageType, Text: string(rs[offset:end]), Attachments: attachments}
	if m.From != nil {
		out.Author = m.From.User
		if out.Author == nil {
			out.Author = m.From.Application
		}
	}
	if end < len(rs) {
		out.Truncated = true
		out.NextCursor = (readCursor{Target: t, Kind: "body", Remaining: 1, Offset: end, Revision: rev}).encode()
	}
	b, _ := json.Marshal(out)
	if len(b) > 256<<10-r.outputBytes {
		return false, errors.New("message metadata/output exceeds budget; use a smaller read")
	}
	r.outputBytes += len(b) + 2*len(out.Ref) + 256
	r.chars += end - offset
	r.emitted[out.Ref] = len(r.result.Messages)
	r.result.Messages = append(r.result.Messages, out)
	return true, nil
}
func (r *readRun) body(c readCursor) {
	rg := ReadRange{Kind: "body", Target: c.Target, Requested: 1, Refs: []string{}, State: "complete", Order: "body_continuation"}
	m, e := r.get(c.Target)
	if e == nil {
		var ok bool
		ok, e = r.add(c.Target, m, c.Offset, c.Revision)
		if !ok && e == nil {
			rg.State = "truncated"
			rg.NextCursor = c.encode()
		} else if e == nil {
			rg.Refs = []string{c.Target.Ref()}
			rg.Oldest, rg.Newest = m.Created, m.Created
			if r.result.Messages[0].Truncated {
				rg.State = "truncated"
				rg.NextCursor = r.result.Messages[0].NextCursor
			}
		}
	}
	if e != nil {
		rg.State = "error"
		rg.Error = readFailure(e)
		if strings.Contains(e.Error(), "message changed") || strings.Contains(e.Error(), "offset") {
			rg.Error = e.Error()
		}
		r.failed = true
	}
	r.result.Ranges = append(r.result.Ranges, rg)
}
func (c readCursor) pathQuery() (string, string) {
	path := c.Target.collection()
	v := url.Values{"$top": {strconv.Itoa(min(50, c.Remaining))}}
	if c.Kind == "replies" {
		path = c.Target.root().path() + "/replies"
	} else {
		v.Set("$orderby", "createdDateTime desc")
		if c.Kind == "before" {
			v.Set("$filter", "createdDateTime lt "+c.Bound)
		}
	}
	return path, v.Encode()
}
func (g *graph) readNext(raw, path string) (string, error) {
	u, e := url.Parse(raw)
	if e != nil || u.User != nil || u.Fragment != "" || u.Scheme != "https" && !(u.Scheme == g.base.Scheme && u.Host == g.base.Host) {
		return "", errors.New("invalid Graph pagination URL")
	}
	if !(u.Host == g.base.Host || u.Host == "graph.microsoft.com" && u.Scheme == "https") {
		return "", errors.New("foreign Graph pagination origin")
	}
	expected, e := url.PathUnescape(strings.TrimRight(g.base.Path, "/") + path)
	if e != nil || u.Path != expected {
		return "", errors.New("Graph pagination changed resource or API version")
	}
	return path + "?" + u.RawQuery, nil
}
func (g *graph) rangeNext(raw, path string, c readCursor) (string, error) {
	next, e := g.readNext(raw, path)
	if e != nil {
		return "", e
	}
	u, _ := url.Parse(raw)
	q, e := url.ParseQuery(u.RawQuery)
	if e != nil {
		return "", errors.New("invalid pagination query")
	}
	for k, values := range q {
		if len(values) != 1 {
			return "", errors.New("duplicate pagination query field")
		}
		switch k {
		case "$skiptoken":
		case "$skip":
			n, err := strconv.Atoi(values[0])
			if err != nil || n < 0 {
				return "", errors.New("invalid pagination offset")
			}
		case "$top":
			n, err := strconv.Atoi(values[0])
			if err != nil || n < 1 || n > 50 {
				return "", errors.New("invalid pagination size")
			}
		case "$orderby":
			if c.Kind == "replies" || values[0] != "createdDateTime desc" {
				return "", errors.New("pagination changed ordering")
			}
		case "$filter":
			if c.Kind != "before" || values[0] != "createdDateTime lt "+c.Bound {
				return "", errors.New("pagination changed filter")
			}
		default:
			return "", errors.New("unsupported pagination query field")
		}
	}
	return next, nil
}
func (r *readRun) walk(c readCursor) {
	rg := ReadRange{Kind: c.Kind, Target: c.Target, Requested: c.Remaining, Refs: []string{}, State: "complete", Order: "created_asc_within_window"}
	path, query := c.pathQuery()
	current := path + "?" + query
	if c.Next != "" {
		var e error
		current, e = r.g.rangeNext(c.Next, path, c)
		if e != nil {
			r.rangeError(c.Kind, c.Target, c.Remaining, "invalid continuation resource")
			return
		}
	}
	visited := map[string]bool{}
	seen := map[string]bool{}
	skipped := map[string]bool{}
	for _, id := range c.Skip {
		skipped[id] = true
	}
	for c.Remaining > 0 {
		if visited[current] {
			rg.State = "error"
			rg.Error = "Graph pagination loop"
			r.failed = true
			break
		}
		visited[current] = true
		if len(r.result.Messages) >= r.maxMessages || r.chars >= r.maxChars {
			rg.State = "truncated"
			rg.NextCursor = c.encode()
			break
		}
		var pg readPage
		var pageErr error
		if initial, ok := r.pages[current]; ok {
			pg = initial.page
			pageErr = initial.err
			delete(r.pages, current)
		} else {
			pageErr = r.g.getJSON(r.ctx, current, smallBodyLimit, &pg)
		}
		if e := pageErr; e != nil {
			rg.State = "error"
			rg.Error = readFailure(e)
			rg.NextCursor = c.encode()
			r.failed = true
			break
		}
		if len(pg.Value) > 50 {
			rg.State = "error"
			rg.Error = "Graph returned an oversized message page"
			r.failed = true
			break
		}
		consumed := append([]string(nil), c.Skip...)
		stopped := false
		for _, m := range pg.Value {
			if !validReadID(m.ID) {
				rg.State = "error"
				rg.Error = "Graph returned an invalid message ID"
				r.failed = true
				stopped = true
				break
			}
			if skipped[m.ID] || seen[m.ID] {
				continue
			}
			tm, e := time.Parse(time.RFC3339Nano, m.Created)
			if e != nil {
				rg.State = "error"
				rg.Error = "Graph returned an invalid creation time"
				r.failed = true
				stopped = true
				break
			}
			if c.Kind == "before" {
				bound, _ := time.Parse(time.RFC3339Nano, c.Bound)
				if !tm.Before(bound) {
					rg.State = "error"
					rg.Error = "Graph ignored the requested time filter"
					r.failed = true
					stopped = true
					break
				}
			}
			t := c.Target
			t.Message = m.ID
			if c.Kind == "replies" {
				t.Parent = c.Target.root().Message
			} else {
				t.Kind = "chat_message"
			}
			ok, e := r.add(t, m, 0, "")
			if e != nil {
				rg.State = "error"
				rg.Error = e.Error()
				r.failed = true
				stopped = true
				break
			}
			if !ok {
				rg.State = "truncated"
				stopped = true
				break
			}
			seen[m.ID] = true
			consumed = append(consumed, m.ID)
			rg.Refs = append(rg.Refs, t.Ref())
			c.Remaining--
			if rg.Oldest == "" || earlier(m.Created, rg.Oldest) {
				rg.Oldest = m.Created
			}
			if rg.Newest == "" || earlier(rg.Newest, m.Created) {
				rg.Newest = m.Created
			}
			if c.Remaining == 0 {
				break
			}
		}
		if stopped {
			if len(consumed) > 50 {
				rg.State = "error"
				rg.Error = "message page changed during continuation; read the range again"
				r.failed = true
				break
			}
			c.Skip = consumed
			// Keep this page's original query, including $top. Rebuilding it
			// from Remaining can shrink the page and replay previously read
			// messages after its nextLink, losing the intended position.
			c.Next, _ = r.g.resolve(current)
			rg.NextCursor = c.encode()
			break
		}
		c.Skip = nil
		skipped = map[string]bool{}
		if c.Remaining == 0 || pg.Next == "" {
			break
		}
		next, e := r.g.rangeNext(pg.Next, path, c)
		if e != nil {
			rg.State = "error"
			rg.Error = "Graph returned a foreign pagination resource"
			r.failed = true
			break
		}
		c.Next = pg.Next
		current = next
	}
	sort.SliceStable(rg.Refs, func(i, j int) bool {
		return earlier(r.result.Messages[r.emitted[rg.Refs[i]]].Created, r.result.Messages[r.emitted[rg.Refs[j]]].Created)
	})
	r.result.Ranges = append(r.result.Ranges, rg)
}

type readPageResult struct {
	path string
	page readPage
	err  error
}

func (r *readRun) prefetchWindows(cs []readCursor) {
	if r.chars >= r.maxChars || len(r.result.Messages) >= r.maxMessages {
		return
	}
	m := r.g.http.Transport.(*readMeter)
	m.mu.Lock()
	enough := m.remaining >= len(cs)
	m.mu.Unlock()
	if !enough {
		return
	}
	results := make(chan readPageResult, len(cs))
	for _, c := range cs {
		path, query := c.pathQuery()
		target := path + "?" + query
		go func() {
			v := readPageResult{path: target}
			v.err = r.g.getJSON(r.ctx, target, smallBodyLimit, &v.page)
			results <- v
		}()
	}
	r.pages = map[string]readPageResult{}
	for range cs {
		v := <-results
		r.pages[v.path] = v
	}
}
func earlier(a, b string) bool {
	x, _ := time.Parse(time.RFC3339Nano, a)
	y, _ := time.Parse(time.RFC3339Nano, b)
	return x.Before(y)
}

func (r *readRun) attachments(t ReadTarget, m readRecord) []ReadAttachment {
	var out []ReadAttachment
	seen := map[string]bool{}
	hosted := func(id string) {
		if seen[id] || !validReadID(id) {
			return
		}
		seen[id] = true
		out = append(out, ReadAttachment{Kind: "hosted", Ref: encodeReadToken(attachmentPrefix, attachmentRef{t, "hosted", id}), State: "available"})
	}
	for _, match := range hostedRE.FindAllStringSubmatch(m.Body.Content, -1) {
		hosted(match[1])
	}
	for _, at := range m.Attachments {
		a := ReadAttachment{Kind: "content", Name: at.Name, ContentType: at.ContentType, State: "inline"}
		if at.ContentType == "reference" {
			a.Kind = "file"
			a.State = "unresolved"
			u, e := url.Parse(at.ContentURL)
			if e == nil && u.Scheme == "https" && u.User == nil && r.r.a.allowedHost(u.Hostname()) && validReadID(at.ID) {
				a.Ref = encodeReadToken(attachmentPrefix, attachmentRef{t, "file", at.ID})
				a.State = "available"
			}
		}
		out = append(out, a)
		for _, match := range hostedRE.FindAllStringSubmatch(at.Content, -1) {
			hosted(match[1])
		}
	}
	// Plain text and fully identified hosted images need no extra request.
	// A visible image with no hosted ID can still be exposed by Graph's list.
	if len(seen) == 0 && strings.Contains(strings.ToLower(m.Body.Content), "<img") {
		var pg hostedReadPage
		if e := r.g.getJSON(r.ctx, t.path()+"/hostedContents", smallBodyLimit, &pg); e != nil {
			out = append(out, ReadAttachment{Kind: "hosted", State: "unresolved", Error: readFailure(e)})
		} else {
			for _, item := range pg.Value {
				hosted(item.ID)
			}
			if pg.Next != "" {
				out = append(out, ReadAttachment{Kind: "hosted", State: "unresolved", Error: "additional hosted content is not included in this message view"})
			}
		}
	}
	return out
}

type hostedReadPage struct {
	Value []struct {
		ID string `json:"id"`
	} `json:"value"`
	Next string `json:"@odata.nextLink"`
}

// Batch only explicit direct reads. Preserve per-item errors without retrying
// a throttled item immediately. Logical subrequests also consume budget.
func (r *readRun) prefetch(ts []ReadTarget) {
	for start := 0; start < len(ts); start += 20 {
		end := min(start+20, len(ts))
		n := end - start
		m := r.g.http.Transport.(*readMeter)
		m.mu.Lock()
		enough := m.remaining >= n
		m.mu.Unlock()
		if !enough {
			return
		}
		requests := make([]map[string]string, 0, n)
		for i, t := range ts[start:end] {
			requests = append(requests, map[string]string{"id": strconv.Itoa(i), "method": "GET", "url": t.path()})
		}
		b, _ := json.Marshal(map[string]any{"requests": requests})
		resp, e := r.g.do(r.ctx, call{method: http.MethodPost, url: "/$batch", body: b, limit: smallBodyLimit})
		if e != nil {
			return
		}
		var env struct {
			Responses []struct {
				ID      string            `json:"id"`
				Status  int               `json:"status"`
				Headers map[string]string `json:"headers"`
				Body    json.RawMessage   `json:"body"`
			} `json:"responses"`
		}
		if json.Unmarshal(resp.body, &env) != nil {
			return
		}
		seen := map[int]bool{}
		for _, item := range env.Responses {
			idx, e := strconv.Atoi(item.ID)
			if e != nil || idx < 0 || idx >= n || seen[idx] {
				continue
			}
			seen[idx] = true
			if item.Status != 200 {
				h := http.Header{}
				for k, v := range item.Headers {
					h.Set(k, v)
				}
				r.cacheErrors[ts[start+idx].Ref()] = r.g.classify(call{method: http.MethodGet, url: ts[start+idx].path()}, response{status: item.Status, header: h, body: item.Body})
				continue
			}
			var gm readRecord
			if json.Unmarshal(item.Body, &gm) == nil && gm.ID == ts[start+idx].Message {
				r.cache[ts[start+idx].Ref()] = gm
			}
		}
	}
}
