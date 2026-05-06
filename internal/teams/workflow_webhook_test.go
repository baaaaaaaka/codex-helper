package teams

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestPostWorkflowWebhookSendsAdaptiveCard(t *testing.T) {
	var seen map[string]any
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s", r.Method)
		}
		if r.URL.String() != "https://workflow.example.test/hook" {
			t.Fatalf("url = %s", r.URL)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Fatalf("content-type = %q", got)
		}
		if err := json.NewDecoder(r.Body).Decode(&seen); err != nil {
			t.Fatalf("decode payload: %v", err)
		}
		return &http.Response{
			StatusCode: http.StatusAccepted,
			Status:     "202 Accepted",
			Body:       io.NopCloser(strings.NewReader("1")),
			Header:     make(http.Header),
		}, nil
	})}

	result, err := PostWorkflowWebhook(context.Background(), client, "https://workflow.example.test/hook", WorkflowWebhookMessage{
		Title:          "✅ Codex finished",
		ChatTitle:      "💬 test-host - Fix installer",
		RequestSummary: "Fix the installer regression",
		Hint:           "The answer is ready in the work chat.",
		Actions:        []OpenURLCardAction{{Title: "Open answer", URL: "https://teams.microsoft.com/l/chat/19%3Atest%40thread.v2/0"}},
	})
	if err != nil {
		t.Fatalf("PostWorkflowWebhook error: %v", err)
	}
	if result.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d", result.StatusCode)
	}
	if seen["type"] != "message" {
		t.Fatalf("payload type = %#v", seen["type"])
	}
	raw, _ := json.Marshal(seen)
	for _, want := range []string{"application/vnd.microsoft.card.adaptive", "✅ Codex finished", "Chat", "💬 test-host - Fix installer", "Request", "Fix the installer regression", "Open answer"} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("payload missing %q: %s", want, raw)
		}
	}
}

func TestPostWorkflowWebhookRejectsUnsafeActionURL(t *testing.T) {
	_, err := PostWorkflowWebhook(context.Background(), nil, "https://example.test/webhook", WorkflowWebhookMessage{
		Text:    "unsafe",
		Actions: []OpenURLCardAction{{Title: "Open", URL: "https://example.com/not-teams"}},
	})
	if err == nil || !strings.Contains(err.Error(), "unsafe") {
		t.Fatalf("expected unsafe URL error, got %v", err)
	}
}

func TestPostWorkflowWebhookRejectsNonAbsoluteWebhookURL(t *testing.T) {
	_, err := PostWorkflowWebhook(context.Background(), nil, "https:///missing-host", WorkflowWebhookMessage{Text: "bad"})
	if err == nil || !strings.Contains(err.Error(), "absolute https URL") {
		t.Fatalf("expected absolute https URL error, got %v", err)
	}
}

func TestPostWorkflowWebhookRejectsOversizedPayload(t *testing.T) {
	_, err := PostWorkflowWebhook(context.Background(), nil, "https://example.test/webhook", WorkflowWebhookMessage{
		Text: strings.Repeat("x", MaxWorkflowWebhookPayloadBytes),
	})
	if err == nil || !strings.Contains(err.Error(), "maximum") {
		t.Fatalf("expected maximum size error, got %v", err)
	}
}

func TestPostWorkflowWebhookRetriesTooManyRequests(t *testing.T) {
	var attempts int
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		attempts++
		if attempts == 1 {
			return &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Status:     "429 Too Many Requests",
				Header:     http.Header{"Retry-After": []string{"1"}},
				Body:       io.NopCloser(strings.NewReader("slow")),
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Body:       io.NopCloser(bytes.NewReader(nil)),
			Header:     make(http.Header),
		}, nil
	})}

	start := time.Now()
	_, err := PostWorkflowWebhook(context.Background(), client, "https://workflow.example.test/hook", WorkflowWebhookMessage{Text: "retry"})
	if err != nil {
		t.Fatalf("PostWorkflowWebhook error: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	if time.Since(start) < 900*time.Millisecond {
		t.Fatalf("Retry-After was not honored")
	}
}

func TestPostWorkflowWebhookClassifiesServerErrorAsDeliveryUncertain(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusBadGateway,
			Status:     "502 Bad Gateway",
			Body:       io.NopCloser(strings.NewReader("maybe delivered")),
			Header:     make(http.Header),
		}, nil
	})}

	_, err := PostWorkflowWebhook(context.Background(), client, "https://workflow.example.test/hook", WorkflowWebhookMessage{Text: "maybe"})
	if err == nil {
		t.Fatalf("expected server error")
	}
	if !workflowWebhookDeliveryUncertain(err) || workflowWebhookRetryable(err) {
		t.Fatalf("error classification = deliveryUncertain:%v retryable:%v, want delivery-uncertain only", workflowWebhookDeliveryUncertain(err), workflowWebhookRetryable(err))
	}
}

func TestPostWorkflowWebhookClassifiesTransportErrorAsDeliveryUncertain(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return nil, io.ErrUnexpectedEOF
	})}

	_, err := PostWorkflowWebhook(context.Background(), client, "https://workflow.example.test/hook", WorkflowWebhookMessage{Text: "maybe"})
	if err == nil {
		t.Fatalf("expected transport error")
	}
	if !workflowWebhookDeliveryUncertain(err) || workflowWebhookRetryable(err) {
		t.Fatalf("error classification = deliveryUncertain:%v retryable:%v, want delivery-uncertain only", workflowWebhookDeliveryUncertain(err), workflowWebhookRetryable(err))
	}
}

func TestPostWorkflowWebhookClassifiesInterruptedRateLimitWaitAsRetryable(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Status:     "429 Too Many Requests",
			Header:     http.Header{"Retry-After": []string{"1"}},
			Body:       io.NopCloser(strings.NewReader("slow")),
		}, nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := PostWorkflowWebhook(ctx, client, "https://workflow.example.test/hook", WorkflowWebhookMessage{Text: "retry later"})
	if err == nil {
		t.Fatalf("expected interrupted rate-limit wait error")
	}
	if !workflowWebhookRetryable(err) || workflowWebhookDeliveryUncertain(err) {
		t.Fatalf("error classification = retryable:%v deliveryUncertain:%v, want retryable only", workflowWebhookRetryable(err), workflowWebhookDeliveryUncertain(err))
	}
}
