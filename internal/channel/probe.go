package channel

import (
	"context"
	"errors"
	"fmt"
)

// MarkerStatus is the result of a marker self-test.
type MarkerStatus string

const (
	MarkerPreserved MarkerStatus = "preserved" // the service stored the hidden marker
	MarkerStripped  MarkerStatus = "stripped"  // the message arrived without it
)

// ProbeMarker is the self-test primitive for hidden markers: it sends a
// one-part probe document to chat with token as the marker, reads the message
// back by id and reports whether the marker survived. Policy (how often to
// run it, what to do with the answer, where to persist it) belongs to the
// core. The probe message itself stays in the chat; the returned Sent lets
// the core record it in its outbox so it is recognised as its own echo.
func ProbeMarker(ctx context.Context, ch Channel, chat string, doc RichDoc, token string) (MarkerStatus, Sent, error) {
	if !ch.Caps().Markers {
		return MarkerStripped, Sent{}, errors.New("channel: markers not supported")
	}
	parts, err := ch.Render(ctx, doc)
	if err != nil {
		return "", Sent{}, err
	}
	if len(parts) != 1 {
		return "", Sent{}, fmt.Errorf("channel: marker probe must render to one part, got %d", len(parts))
	}
	sent, err := ch.Send(ctx, chat, parts[0], token)
	if err != nil {
		return "", Sent{}, err
	}
	msg, err := ch.Get(ctx, chat, sent.ID)
	if err != nil {
		return "", sent, err
	}
	if msg.Marker == token {
		return MarkerPreserved, sent, nil
	}
	return MarkerStripped, sent, nil
}
