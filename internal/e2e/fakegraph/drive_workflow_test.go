package fakegraph

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// raw sends a request as if to host (through the harness transport).
func (c *client) raw(method, host, pathQuery string, hdr map[string]string, body []byte) (int, []byte) {
	c.t.Helper()
	req, err := http.NewRequest(method, c.s.URL()+pathQuery, bytes.NewReader(body))
	if err != nil {
		c.t.Fatal(err)
	}
	req.Header.Set(HeaderOriginalHost, host)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, data
}

func (c *client) graphRaw(method, pathQuery string, body []byte, ct string) (int, map[string]any) {
	c.t.Helper()
	hdr := map[string]string{"Authorization": "Bearer " + c.token}
	if ct != "" {
		hdr["Content-Type"] = ct
	}
	status, data := c.raw(method, graphHost, "/v1.0"+pathQuery, hdr, body)
	var out map[string]any
	_ = json.Unmarshal(data, &out)
	return status, out
}

func TestDriveSimplePutReplaceAndFail(t *testing.T) {
	c := newClient(t)
	p := "/me/drive/root:/Microsoft%20Teams%20Chat%20Files/a.txt:/content"
	status, item := c.graphRaw("PUT", p, []byte("hello"), "text/plain")
	if status != 201 || item["name"] != "a.txt" || item["size"].(float64) != 5 {
		t.Fatalf("first PUT: %d %v", status, item)
	}
	if !guidRE.MatchString(item["eTag"].(string)) {
		t.Fatalf("eTag %v has no GUID", item["eTag"])
	}
	status, item2 := c.graphRaw("PUT", p, []byte("hello2"), "text/plain")
	if status != 200 || item2["id"] != item["id"] || item2["eTag"] == item["eTag"] {
		t.Fatalf("replace PUT: %d %v (first %v)", status, item2, item)
	}
	status, _ = c.graphRaw("PUT", p+"?@microsoft.graph.conflictBehavior=fail", []byte("x"), "text/plain")
	if status != 409 {
		t.Fatalf("conflictBehavior=fail on existing: %d, want 409", status)
	}
	items := c.s.DriveItems()
	if len(items) != 1 || string(items[0].Data) != "hello2" || items[0].Path != "/Microsoft Teams Chat Files/a.txt" || len(items[0].Writes) != 2 {
		t.Fatalf("drive = %+v", items)
	}
	status, got := c.graphRaw("GET", "/me/drive/items/"+items[0].ID+"?$select=id,name,size,eTag,webUrl,webDavUrl", nil, "")
	if status != 200 || got["webDavUrl"] == nil || got["webUrl"] == nil {
		t.Fatalf("GET item: %d %v", status, got)
	}
	status, data := c.raw("GET", graphHost, "/v1.0/me/drive/items/"+items[0].ID+"/content", map[string]string{"Authorization": "Bearer " + c.token}, nil)
	if status != 200 || string(data) != "hello2" {
		t.Fatalf("GET content: %d %q", status, data)
	}
}

func TestDriveUploadSession(t *testing.T) {
	c := newClient(t)
	const total = 3*uploadFragmentAlign + 1000
	data := bytes.Repeat([]byte("0123456789abcdef"), total/16+1)[:total]
	body, _ := json.Marshal(map[string]any{"item": map[string]any{"@microsoft.graph.conflictBehavior": "replace", "name": "big.bin"}})
	status, sess := c.graphRaw("POST", "/me/drive/root:/Microsoft%20Teams%20Chat%20Files/big.bin:/createUploadSession", body, "application/json")
	if status != 200 {
		t.Fatalf("createUploadSession: %d %v", status, sess)
	}
	u, err := url.Parse(sess["uploadUrl"].(string))
	if err != nil || u.Host != SharePointHost || u.Scheme != "https" {
		t.Fatalf("uploadUrl %v", sess["uploadUrl"])
	}
	put := func(start, end int, extra map[string]string) (int, map[string]any) {
		hdr := map[string]string{"Content-Range": fmt.Sprintf("bytes %d-%d/%d", start, end, total)}
		for k, v := range extra {
			hdr[k] = v
		}
		status, raw := c.raw("PUT", SharePointHost, u.RequestURI(), hdr, data[start:end+1])
		var out map[string]any
		_ = json.Unmarshal(raw, &out)
		return status, out
	}
	if status, _ := put(0, uploadFragmentAlign-1, map[string]string{"Authorization": "Bearer " + c.token}); status != 400 {
		t.Fatalf("fragment with Authorization: %d, want 400", status)
	}
	if status, _ := put(0, 1000, nil); status != 400 {
		t.Fatalf("misaligned non-final fragment: %d, want 400", status)
	}
	if status, _ := put(uploadFragmentAlign, 2*uploadFragmentAlign-1, nil); status != http.StatusRequestedRangeNotSatisfiable {
		t.Fatalf("out-of-order fragment: %d, want 416", status)
	}
	status, out := put(0, 2*uploadFragmentAlign-1, nil)
	if status != 202 || fmt.Sprint(out["nextExpectedRanges"]) != fmt.Sprintf("[%d-]", 2*uploadFragmentAlign) {
		t.Fatalf("first fragment: %d %v", status, out)
	}
	status, raw := c.raw("GET", SharePointHost, u.RequestURI(), nil, nil)
	if status != 200 || !strings.Contains(string(raw), fmt.Sprintf(`"%d-"`, 2*uploadFragmentAlign)) {
		t.Fatalf("status query: %d %s", status, raw)
	}
	status, item := put(2*uploadFragmentAlign, total-1, nil)
	if status != 201 || item["name"] != "big.bin" || int(item["size"].(float64)) != total {
		t.Fatalf("final fragment: %d %v", status, item)
	}
	if status, _ := c.raw("GET", SharePointHost, u.RequestURI(), nil, nil); status != 404 {
		t.Fatalf("completed session status query: %d, want 404", status)
	}
	items := c.s.DriveItems()
	if len(items) != 1 || !bytes.Equal(items[0].Data, data) || items[0].Fragments != 2 || items[0].Writes[0] != "session" {
		t.Fatalf("drive = %+v", items[0].Writes)
	}
}

func TestReferenceAttachmentMustPointAtDriveItem(t *testing.T) {
	c := newClient(t)
	chat := c.chat()
	_, item := c.graphRaw("PUT", "/me/drive/root:/Microsoft%20Teams%20Chat%20Files/r.txt:/content", []byte("r"), "text/plain")
	guid := strings.ToLower(guidRE.FindString(item["eTag"].(string)))
	msg := func(id, contentURL string) map[string]any {
		return map[string]any{
			"body":        map[string]any{"contentType": "html", "content": `Codex: file <attachment id="` + id + `"></attachment>`},
			"attachments": []any{map[string]any{"id": id, "contentType": "reference", "contentUrl": contentURL, "name": "r.txt"}},
		}
	}
	if status, _ := c.do("POST", "/chats/"+url.PathEscape(chat)+"/messages", msg("e2e00000-0000-4000-8000-000000000000", item["webUrl"].(string))); status != 400 {
		t.Fatalf("wrong attachment id accepted: %d", status)
	}
	if status, _ := c.do("POST", "/chats/"+url.PathEscape(chat)+"/messages", msg(guid, "https://"+SharePointHost+"/nope")); status != 400 {
		t.Fatalf("unknown contentUrl accepted: %d", status)
	}
	if status, out := c.do("POST", "/chats/"+url.PathEscape(chat)+"/messages", msg(guid, item["webUrl"].(string))); status != 201 {
		t.Fatalf("valid reference attachment: %d %v", status, out)
	}
	files := c.s.OutboundFiles(chat)
	if len(files) != 1 || files[0].Name != "r.txt" || string(files[0].Item.Data) != "r" {
		t.Fatalf("OutboundFiles = %+v", files)
	}
}

func TestWorkflowWebhook(t *testing.T) {
	c := newClient(t)
	hook := c.s.NewWorkflowWebhook()
	u, _ := url.Parse(hook)
	if u.Hostname() != WorkflowHost || u.Scheme != "https" {
		t.Fatalf("webhook URL %s", hook)
	}
	card := []byte(`{"type":"message","attachments":[{"contentType":"application/vnd.microsoft.card.adaptive","contentUrl":null,"content":{"type":"AdaptiveCard","version":"1.2","body":[{"type":"TextBlock","text":"Codex finished"},{"type":"Container","items":[{"type":"TextBlock","text":"inner"}]}],"actions":[{"type":"Action.OpenUrl","title":"Open","url":"https://teams.microsoft.com/l/chat/x"}]}}]}`)
	json := map[string]string{"Content-Type": "application/json"}
	if status, _ := c.raw("POST", WorkflowHost, u.RequestURI(), json, card); status != 202 {
		t.Fatalf("valid card: %d", status)
	}
	bad := strings.Replace(u.RequestURI(), "sig=", "sig=x", 1)
	if status, _ := c.raw("POST", WorkflowHost, bad, json, card); status != 401 {
		t.Fatalf("bad sig: %d, want 401", status)
	}
	if status, _ := c.raw("POST", WorkflowHost, u.RequestURI(), json, []byte(`{"text":"hi"}`)); status != 400 {
		t.Fatalf("non-card body: %d, want 400", status)
	}
	c.s.AddFault(Fault{Name: "wf500", Host: WorkflowHost, Kind: FaultStatus, Status: 500, Times: 1})
	if status, _ := c.raw("POST", WorkflowHost, u.RequestURI(), json, card); status != 500 {
		t.Fatalf("fault: %d, want 500", status)
	}
	all := c.s.WorkflowCards()
	acc := c.s.AcceptedWorkflowCards()
	if len(acc) != 1 || acc[0].Text() != "Codex finished\ninner" || len(acc[0].Actions) != 1 {
		t.Fatalf("accepted = %+v", acc)
	}
	if len(all) != 2 || all[1].Status != 500 || all[1].Fault != "wf500" {
		t.Fatalf("all cards = %+v", all)
	}
}
