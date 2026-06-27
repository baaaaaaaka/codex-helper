package responsesadapter

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type controlledWebSocketAdapter struct {
	events <-chan ProviderEvent
}

func (a controlledWebSocketAdapter) Stream(context.Context, ProviderRequest) (<-chan ProviderEvent, error) {
	return a.events, nil
}

func TestFacadeStreamsResponsesOverWebSocket(t *testing.T) {
	facade := newTestFacade(NewMemoryStore(), fakeAdapter{
		events: []ProviderEvent{
			{Kind: ProviderEventTextDelta, Delta: "websocket complete"},
			{Kind: ProviderEventDone},
		},
	})
	server := httptest.NewServer(facade)
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses"
	connection, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := connection.WriteJSON(map[string]any{
		"type":  "response.create",
		"model": "model-a",
		"input": "say hello",
	}); err != nil {
		t.Fatal(err)
	}
	_ = connection.SetReadDeadline(time.Now().Add(3 * time.Second))
	var eventTypes []string
	var completed map[string]any
	for completed == nil {
		messageType, raw, err := connection.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		if messageType != websocket.TextMessage {
			t.Fatalf("message type = %d, want text", messageType)
		}
		var event map[string]any
		if err := json.Unmarshal(raw, &event); err != nil {
			t.Fatalf("invalid event %s: %v", raw, err)
		}
		eventType, _ := event["type"].(string)
		eventTypes = append(eventTypes, eventType)
		if eventType == "response.completed" {
			completed = event
		}
	}
	joined := strings.Join(eventTypes, ",")
	for _, want := range []string{"response.created", "response.output_text.delta", "response.completed"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("events = %v, missing %s", eventTypes, want)
		}
	}
	encoded, _ := json.Marshal(completed)
	if !strings.Contains(string(encoded), "websocket complete") {
		t.Fatalf("completed event = %s", encoded)
	}
}

func TestFacadeWebSocketForwardsDeltaBeforeProviderCompletes(t *testing.T) {
	events := make(chan ProviderEvent, 4)
	facade := newTestFacade(NewMemoryStore(), controlledWebSocketAdapter{events: events})
	server := httptest.NewServer(facade)
	defer server.Close()
	connection, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if err := connection.WriteJSON(map[string]any{"type": "response.create", "model": "model-a", "input": "stream"}); err != nil {
		t.Fatal(err)
	}
	_ = connection.SetReadDeadline(time.Now().Add(3 * time.Second))

	// response.created is emitted before the provider yields its first token.
	var created map[string]any
	if err := connection.ReadJSON(&created); err != nil {
		t.Fatal(err)
	}
	if created["type"] != "response.created" {
		t.Fatalf("first event = %#v, want response.created", created)
	}
	events <- ProviderEvent{Kind: ProviderEventTextDelta, Delta: "early delta"}
	var delta map[string]any
	for attempts := 0; attempts < 10; attempts++ {
		if err := connection.ReadJSON(&delta); err != nil {
			t.Fatal(err)
		}
		if delta["type"] == "response.output_text.delta" {
			break
		}
	}
	if delta["type"] != "response.output_text.delta" || delta["delta"] != "early delta" {
		t.Fatalf("delta event before completion = %#v", delta)
	}

	// Only now allow the provider to complete. A recorder-backed bridge would
	// have blocked the earlier ReadJSON until this point.
	events <- ProviderEvent{Kind: ProviderEventDone}
	close(events)
	for {
		var event map[string]any
		if err := connection.ReadJSON(&event); err != nil {
			t.Fatal(err)
		}
		if event["type"] == "response.completed" {
			break
		}
	}
}
