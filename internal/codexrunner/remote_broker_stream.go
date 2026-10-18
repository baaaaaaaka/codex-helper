package codexrunner

import (
	"context"
	"encoding/json"
	"io"

	"github.com/gorilla/websocket"
)

const (
	defaultRemoteBrokerClassificationPrefixLimit = 64 * 1024
	defaultRemoteBrokerApprovalCaptureLimit      = defaultAppServerProcessStdoutLineLimit
)

type remoteBrokerRecordMode uint8

const (
	remoteBrokerRecordUndecided remoteBrokerRecordMode = iota
	remoteBrokerRecordPassthrough
	remoteBrokerRecordApprovalCandidate
)

type remoteBrokerRecordWriter interface {
	WriteChunk([]byte) error
	Finish() error
	Abort()
}

// remoteBrokerRecordGate holds only a bounded classification prefix or one
// bounded approval candidate. Ordinary records switch to passthrough as soon
// as their top-level fields prove they are not a request this handler may
// resolve.
type remoteBrokerRecordGate struct {
	ctx            context.Context
	broker         *RemoteBroker
	connection     *websocket.Conn
	handler        AppServerServerRequestHandler
	mode           remoteBrokerRecordMode
	scanner        appServerTopLevelJSONScanner
	prefix         []byte
	candidate      []byte
	method         string
	hasMethod      bool
	hasID          bool
	prefixLimit    int
	candidateLimit int
	writer         remoteBrokerRecordWriter
}

func newRemoteBrokerRecordGate(ctx context.Context, broker *RemoteBroker, connection *websocket.Conn) *remoteBrokerRecordGate {
	gate := &remoteBrokerRecordGate{
		ctx:            ctx,
		broker:         broker,
		connection:     connection,
		handler:        broker.handler,
		prefixLimit:    broker.classificationPrefixLimit,
		candidateLimit: broker.approvalCaptureLimit,
		writer:         &remoteBrokerWSRecordWriter{broker: broker, connection: connection},
	}
	if gate.handler == nil {
		gate.mode = remoteBrokerRecordPassthrough
	}
	return gate
}

func (g *remoteBrokerRecordGate) WriteChunk(chunk []byte) error {
	for index := 0; index < len(chunk); index++ {
		switch g.mode {
		case remoteBrokerRecordPassthrough:
			return g.writer.WriteChunk(chunk[index:])
		case remoteBrokerRecordApprovalCandidate:
			return g.appendCandidate(chunk[index:])
		}

		g.prefix = append(g.prefix, chunk[index])
		event := g.scanner.FeedByte(chunk[index])
		if err := g.observe(event); err != nil {
			return err
		}
		if g.mode == remoteBrokerRecordPassthrough {
			if err := g.writer.WriteChunk(g.prefix); err != nil {
				return err
			}
			g.prefix = nil
			return g.writer.WriteChunk(chunk[index+1:])
		}
		if g.mode == remoteBrokerRecordApprovalCandidate {
			g.candidate = append(g.candidate[:0], g.prefix...)
			g.prefix = nil
			if len(g.candidate) > g.candidateLimit {
				return g.fallbackCandidate(chunk[index+1:])
			}
			if err := g.appendCandidate(chunk[index+1:]); err != nil {
				return err
			}
			return nil
		}
		if len(g.prefix) >= g.prefixLimit {
			g.switchToPassthrough()
			if err := g.writer.WriteChunk(g.prefix); err != nil {
				return err
			}
			g.prefix = nil
			return g.writer.WriteChunk(chunk[index+1:])
		}
	}
	return nil
}

func (g *remoteBrokerRecordGate) observe(event appServerJSONScanEvent) error {
	if event.Invalid {
		g.switchToPassthrough()
		return nil
	}
	if event.FieldStart == "result" || event.FieldStart == "error" {
		if !g.hasMethod || g.method == "" || !g.mayHandle(g.method) {
			g.switchToPassthrough()
			return nil
		}
	}
	switch event.FieldComplete {
	case "id":
		g.hasID = true
	case "method":
		method, ok := decodeAppServerMethod(event.RawValue)
		g.method = method
		g.hasMethod = ok
	}
	if g.hasMethod && g.method != "" {
		if !g.mayHandle(g.method) {
			g.switchToPassthrough()
			return nil
		}
		if g.hasID {
			g.mode = remoteBrokerRecordApprovalCandidate
			return nil
		}
	}
	if g.scanner.Complete() {
		g.switchToPassthrough()
	}
	return nil
}

func (g *remoteBrokerRecordGate) mayHandle(method string) bool {
	if g.handler == nil {
		return false
	}
	if classifier, ok := g.handler.(AppServerServerRequestMethodClassifier); ok {
		return classifier.MayHandleServerRequest(method)
	}
	return true
}

func (g *remoteBrokerRecordGate) switchToPassthrough() {
	if g.mode != remoteBrokerRecordUndecided {
		return
	}
	g.mode = remoteBrokerRecordPassthrough
}

func (g *remoteBrokerRecordGate) appendCandidate(chunk []byte) error {
	if len(chunk) == 0 {
		return nil
	}
	if len(g.candidate)+len(chunk) <= g.candidateLimit {
		g.candidate = append(g.candidate, chunk...)
		return nil
	}
	// Keep the request available to the official TUI for manual review if the
	// existing full-params approval API would exceed its bounded capture size.
	return g.fallbackCandidate(chunk)
}

func (g *remoteBrokerRecordGate) fallbackCandidate(chunk []byte) error {
	g.mode = remoteBrokerRecordPassthrough
	if err := g.writer.WriteChunk(g.candidate); err != nil {
		return err
	}
	g.candidate = nil
	return g.writer.WriteChunk(chunk)
}

func (g *remoteBrokerRecordGate) Finish() error {
	if g.mode == remoteBrokerRecordUndecided {
		g.switchToPassthrough()
		if err := g.writer.WriteChunk(g.prefix); err != nil {
			return err
		}
		g.prefix = nil
	}
	if g.mode == remoteBrokerRecordApprovalCandidate {
		var request appServerMessage
		forwarded := false
		if json.Unmarshal(g.candidate, &request) == nil &&
			appServerMessageIsServerRequest(request) && g.mayHandle(request.Method) {
			if err := g.broker.handleServerRequest(g.ctx, g.connection, request, g.candidate); err != nil {
				return err
			}
		} else if err := g.writer.WriteChunk(g.candidate); err != nil {
			return err
		} else {
			forwarded = true
		}
		g.candidate = nil
		if forwarded {
			return g.writer.Finish()
		}
		return nil
	}
	return g.writer.Finish()
}

func (g *remoteBrokerRecordGate) Abort() {
	if g.writer != nil {
		g.writer.Abort()
	}
}

func readAppServerRecord(ctx context.Context, transport AppServerLineTransport, writeChunk func([]byte) error) (bool, error) {
	if streamed, ok := transport.(AppServerRecordTransport); ok {
		return streamed.ReadRecord(ctx, writeChunk)
	}
	line, err := transport.ReadLine(ctx)
	if err != nil {
		return false, err
	}
	if len(line) > 0 {
		if err := writeChunk(line); err != nil {
			return true, err
		}
	}
	return true, nil
}

type remoteBrokerWSRecordWriter struct {
	broker     *RemoteBroker
	connection *websocket.Conn
	writer     io.WriteCloser
	locked     bool
}

func (w *remoteBrokerWSRecordWriter) WriteChunk(chunk []byte) error {
	if len(chunk) == 0 {
		return nil
	}
	if w.writer == nil {
		w.broker.clientWriteMu.Lock()
		w.locked = true
		writer, err := w.connection.NextWriter(websocket.TextMessage)
		if err != nil {
			w.unlock()
			return err
		}
		w.writer = writer
	}
	written, err := w.writer.Write(chunk)
	if err == nil && written != len(chunk) {
		err = io.ErrShortWrite
	}
	return err
}

func (w *remoteBrokerWSRecordWriter) Finish() error {
	if w.writer == nil {
		return w.broker.writeClient(w.connection, []byte{})
	}
	err := w.writer.Close()
	w.writer = nil
	w.unlock()
	return err
}

func (w *remoteBrokerWSRecordWriter) Abort() {
	// Do not close the writer: doing so would publish a truncated JSON message.
	// The broker closes the WebSocket after the stream error is reported.
	w.writer = nil
	w.unlock()
}

func (w *remoteBrokerWSRecordWriter) unlock() {
	if w.locked {
		w.locked = false
		w.broker.clientWriteMu.Unlock()
	}
}
