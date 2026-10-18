package codexrunner

import (
	"bytes"
	"hash/crc32"
	"testing"
)

func TestRemoteBrokerRecordGateDoesNotCaptureLargeResultBody(t *testing.T) {
	const payloadSize = defaultAppServerProcessStdoutLineLimit + 4096
	prefix := []byte(`{"id":7,"result":{"plugins":"`)
	suffix := []byte(`"}}`)
	frame := make([]byte, 0, len(prefix)+payloadSize+len(suffix))
	frame = append(frame, prefix...)
	frame = append(frame, bytes.Repeat([]byte{'p'}, payloadSize)...)
	frame = append(frame, suffix...)

	sink := &countingRemoteBrokerRecordWriter{}
	gate := &remoteBrokerRecordGate{
		handler:        AutomaticApprovalHandler{},
		prefixLimit:    defaultRemoteBrokerClassificationPrefixLimit,
		candidateLimit: defaultRemoteBrokerApprovalCaptureLimit,
		writer:         sink,
	}
	for offset := 0; offset < len(frame); offset += appServerProcessStdoutChunkSize {
		end := offset + appServerProcessStdoutChunkSize
		if end > len(frame) {
			end = len(frame)
		}
		if err := gate.WriteChunk(frame[offset:end]); err != nil {
			t.Fatal(err)
		}
	}
	if err := gate.Finish(); err != nil {
		t.Fatal(err)
	}
	if sink.bytes != len(frame) {
		t.Fatalf("streamed bytes = %d, want %d", sink.bytes, len(frame))
	}
	if want := crc32.ChecksumIEEE(frame); sink.checksum != want {
		t.Fatalf("streamed checksum = %08x, want %08x", sink.checksum, want)
	}
	if len(gate.prefix) != 0 || len(gate.candidate) != 0 {
		t.Fatalf("ordinary result retained %d prefix bytes and %d candidate bytes", len(gate.prefix), len(gate.candidate))
	}
}

func BenchmarkRemoteBrokerLargeResultPassthrough(b *testing.B) {
	const payloadSize = defaultAppServerProcessStdoutLineLimit + 4096
	prefix := []byte(`{"id":7,"result":{"plugins":"`)
	suffix := []byte(`"}}`)
	frame := make([]byte, 0, len(prefix)+payloadSize+len(suffix))
	frame = append(frame, prefix...)
	frame = append(frame, bytes.Repeat([]byte{'p'}, payloadSize)...)
	frame = append(frame, suffix...)
	chunks := make([][]byte, 0, (len(frame)+appServerProcessStdoutChunkSize-1)/appServerProcessStdoutChunkSize)
	for offset := 0; offset < len(frame); offset += appServerProcessStdoutChunkSize {
		end := offset + appServerProcessStdoutChunkSize
		if end > len(frame) {
			end = len(frame)
		}
		chunks = append(chunks, frame[offset:end])
	}
	wantChecksum := crc32.ChecksumIEEE(frame)
	b.SetBytes(int64(len(frame)))
	b.ReportAllocs()
	b.ResetTimer()
	for iteration := 0; iteration < b.N; iteration++ {
		sink := &countingRemoteBrokerRecordWriter{}
		gate := &remoteBrokerRecordGate{
			handler:        AutomaticApprovalHandler{},
			prefixLimit:    defaultRemoteBrokerClassificationPrefixLimit,
			candidateLimit: defaultRemoteBrokerApprovalCaptureLimit,
			writer:         sink,
		}
		for _, chunk := range chunks {
			if err := gate.WriteChunk(chunk); err != nil {
				b.Fatal(err)
			}
		}
		if err := gate.Finish(); err != nil {
			b.Fatal(err)
		}
		if sink.bytes != len(frame) || sink.checksum != wantChecksum {
			b.Fatalf("streamed %d bytes with checksum %08x, want %d bytes and checksum %08x", sink.bytes, sink.checksum, len(frame), wantChecksum)
		}
	}
}

type countingRemoteBrokerRecordWriter struct {
	bytes    int
	checksum uint32
}

func (w *countingRemoteBrokerRecordWriter) WriteChunk(chunk []byte) error {
	w.bytes += len(chunk)
	w.checksum = crc32.Update(w.checksum, crc32.IEEETable, chunk)
	return nil
}

func (*countingRemoteBrokerRecordWriter) Finish() error { return nil }

func (*countingRemoteBrokerRecordWriter) Abort() {}
