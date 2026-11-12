package teams

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"
)

type searchCursor struct {
	Tenant    string `json:"tenant"`
	Query     string `json:"query"`
	From      int    `json:"from"`
	Remaining int    `json:"remaining"`
	Window    int    `json:"window"`
	PageSkip  int    `json:"page_skip,omitempty"`
}
type graphSearch struct {
	Value []struct {
		Containers []struct {
			More bool `json:"moreResultsAvailable"`
			Hits []struct {
				Summary  string `json:"summary"`
				Resource struct {
					ID      string `json:"id"`
					Chat    string `json:"chatId"`
					URL     string `json:"webUrl"`
					WebLink string `json:"webLink"`
					Channel struct {
						Team    string `json:"teamId"`
						Channel string `json:"channelId"`
					} `json:"channelIdentity"`
				} `json:"resource"`
			} `json:"hits"`
		} `json:"hitsContainers"`
	} `json:"value"`
}

func ValidateSearch(query string, o SearchOptions, tenant string) error {
	if e := o.Budget.validate(); e != nil {
		return e
	}
	if o.Limit < 1 || o.Limit > 10000 {
		return errors.New("search limit must be 1..10000")
	}
	if o.Cursor != "" {
		if query != "" {
			return errors.New("cursor cannot be combined with a new search query")
		}
		var c searchCursor
		if e := decodeReadToken(o.Cursor, cursorPrefix, &c); e != nil {
			return e
		}
		if tenant != "" && c.Tenant != tenant || c.From < 0 || c.From > 100000 || c.Window < 1 || c.Window > 10000 || c.Remaining < 1 || c.Remaining > c.Window || c.PageSkip < 0 || c.PageSkip > 20 || len(c.Query) > 4096 || strings.TrimSpace(c.Query) == "" {
			return errors.New("invalid search cursor")
		}
		return nil
	}
	if strings.TrimSpace(query) == "" || len(query) > 4096 {
		return errors.New("search requires a query of at most 4096 bytes")
	}
	return nil
}
func (r *Reader) Search(ctx context.Context, query string, o SearchOptions) (SearchResult, error) {
	out := SearchResult{Version: 1, FetchedAt: r.a.cfg.Now().UTC(), Scope: "signed_in_user_index", Hits: []SearchHit{}, State: "complete"}
	if e := ValidateSearch(query, o, r.a.cfg.TenantID); e != nil {
		return out, e
	}
	c := searchCursor{Tenant: r.a.cfg.TenantID, Query: query, Remaining: o.Limit, Window: o.Limit}
	if o.Cursor != "" {
		_ = decodeReadToken(o.Cursor, cursorPrefix, &c)
	}
	out.Query = c.Query
	ctx, cancel, g, m, e := r.session(ctx, o.Budget)
	if e != nil {
		return out, e
	}
	defer cancel()
	chars := 0
	finish := func(state string, err error) (SearchResult, error) {
		out.State = state
		if err != nil {
			out.Error = err.Error()
		}
		if state != "complete" && c.Remaining > 0 && c.From <= 100000 {
			out.NextCursor = encodeReadToken(cursorPrefix, c)
		}
		out.Usage = m.usage(chars)
		return out, err
	}
	for c.Remaining > 0 {
		if len(out.Hits) >= o.Budget.MaxMessages || chars >= o.Budget.MaxChars {
			return finish("truncated", nil)
		}
		size := min(20, c.Remaining+c.PageSkip)
		b, _ := json.Marshal(map[string]any{"requests": []any{map[string]any{"entityTypes": []string{"chatMessage"}, "query": map[string]string{"queryString": c.Query}, "from": c.From, "size": size}}})
		resp, e := g.do(ctx, call{method: http.MethodPost, url: "/search/query", body: b, limit: smallBodyLimit})
		if e != nil {
			return finish("error", errors.New(readFailure(e)))
		}
		var pg graphSearch
		if json.Unmarshal(resp.body, &pg) != nil || len(pg.Value) != 1 || len(pg.Value[0].Containers) != 1 {
			return finish("error", errors.New("invalid Graph search response"))
		}
		hc := pg.Value[0].Containers[0]
		if len(hc.Hits) > size || c.PageSkip > len(hc.Hits) {
			return finish("error", errors.New("search page changed; restart the query"))
		}
		for idx := c.PageSkip; idx < len(hc.Hits); idx++ {
			h := hc.Hits[idx]
			n := utf8.RuneCountInString(h.Summary)
			if len(out.Hits) >= o.Budget.MaxMessages || chars >= o.Budget.MaxChars {
				c.PageSkip = idx
				return finish("truncated", nil)
			}
			hit := SearchHit{Summary: h.Summary, State: "unresolved", Reason: "search result lacks a supported message locator or channel reply parent"}
			if n > o.Budget.MaxChars-chars {
				n = o.Budget.MaxChars - chars
				hit.Summary = string([]rune(hit.Summary)[:n])
				hit.SummaryTruncated = true
			}
			t := ReadTarget{Tenant: r.a.cfg.TenantID}
			if h.Resource.Channel.Team != "" {
				// Search does not always include reply parent; only an unambiguous Teams URL is resolvable.
				if u, e := url.Parse(h.Resource.URL); e == nil && u.Host == "teams.microsoft.com" {
					if parsed, e := ParseReadTarget(h.Resource.URL, r.a.cfg.TenantID); e == nil && parsed.Team == h.Resource.Channel.Team && parsed.Channel == h.Resource.Channel.Channel && parsed.Message == h.Resource.ID {
						t = parsed
					}
				}
			} else {
				t.Kind = "chat_message"
				t.Chat = h.Resource.Chat
				t.Message = h.Resource.ID
			}
			if t.validate(r.a.cfg.TenantID) == nil {
				hit.Ref = t.Ref()
				hit.URL = t.webURL()
				hit.State = "resolvable"
				hit.Reason = ""
			}
			out.Hits = append(out.Hits, hit)
			chars += n
			c.Remaining--
			c.PageSkip = idx + 1
			if c.Remaining == 0 {
				break
			}
		}
		if c.Remaining == 0 {
			if !hc.More && c.PageSkip >= len(hc.Hits) {
				return finish("complete", nil)
			}
			if c.PageSkip >= len(hc.Hits) {
				c.From += len(hc.Hits)
				c.PageSkip = 0
			}
			if c.From > 100000 {
				return finish("error", errors.New("search offset limit reached; narrow the query"))
			}
			c.Remaining = c.Window
			return finish("more", nil)
		}
		if !hc.More {
			return finish("complete", nil)
		}
		if len(hc.Hits) == 0 {
			return finish("error", errors.New("Graph search returned an empty page with more results"))
		}
		c.From += len(hc.Hits)
		c.PageSkip = 0
		if c.From > 100000 {
			return finish("error", errors.New("search offset limit reached; narrow the query"))
		}
	}
	return finish("complete", nil)
}
