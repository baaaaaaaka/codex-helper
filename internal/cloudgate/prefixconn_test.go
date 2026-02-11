package cloudgate

import (
	"io"
	"net"
	"testing"
)

func TestPrefixConnReadAll(t *testing.T) {
	prefix := []byte("HELLO")
	connData := []byte(" WORLD")

	client, server := net.Pipe()
	defer client.Close()

	go func() {
		_, _ = server.Write(connData)
		_ = server.Close()
	}()

	pc := newPrefixConn(prefix, client)

	got, err := io.ReadAll(pc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}

	want := "HELLO WORLD"
	if string(got) != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestPrefixConnSmallReads(t *testing.T) {
	prefix := []byte("ABCDE")
	connData := []byte("FGH")

	client, server := net.Pipe()
	defer client.Close()

	go func() {
		_, _ = server.Write(connData)
		_ = server.Close()
	}()

	pc := newPrefixConn(prefix, client)

	// Read 2 bytes at a time.
	var result []byte
	buf := make([]byte, 2)
	for {
		n, err := pc.Read(buf)
		result = append(result, buf[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
	}

	want := "ABCDEFGH"
	if string(result) != want {
		t.Errorf("got %q, want %q", result, want)
	}
}

func TestPrefixConnEmptyPrefix(t *testing.T) {
	connData := []byte("DATA")

	client, server := net.Pipe()
	defer client.Close()

	go func() {
		_, _ = server.Write(connData)
		_ = server.Close()
	}()

	pc := newPrefixConn(nil, client)

	got, err := io.ReadAll(pc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "DATA" {
		t.Errorf("got %q, want %q", got, "DATA")
	}
}

func TestPrefixConnPrefixOnly(t *testing.T) {
	prefix := []byte("PREFIX")

	client, server := net.Pipe()
	go func() {
		_ = server.Close()
	}()
	defer client.Close()

	pc := newPrefixConn(prefix, client)

	got, err := io.ReadAll(pc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "PREFIX" {
		t.Errorf("got %q, want %q", got, "PREFIX")
	}
}

func TestPrefixConnExactBoundary(t *testing.T) {
	// Read buffer exactly equals prefix length.
	prefix := []byte("ABC")
	connData := []byte("DEF")

	client, server := net.Pipe()
	defer client.Close()

	go func() {
		_, _ = server.Write(connData)
		_ = server.Close()
	}()

	pc := newPrefixConn(prefix, client)

	buf := make([]byte, 3)
	n, err := pc.Read(buf)
	if err != nil {
		t.Fatalf("first Read: %v", err)
	}
	if n != 3 || string(buf[:n]) != "ABC" {
		t.Errorf("first read: got %q, want %q", buf[:n], "ABC")
	}

	n, err = pc.Read(buf)
	if err != nil {
		t.Fatalf("second Read: %v", err)
	}
	if n != 3 || string(buf[:n]) != "DEF" {
		t.Errorf("second read: got %q, want %q", buf[:n], "DEF")
	}
}
