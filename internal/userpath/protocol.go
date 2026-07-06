package userpath

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	probeMagic      = "CXPUPTH1"
	probeVersion    = uint16(3)
	maxProbeField   = 1 << 20
	maxProbeGroups  = 1 << 16
	probeNonceBytes = 32
)

type probePayload struct {
	Nonce  string
	Path   string
	Home   string
	Cwd    string
	UID    uint32
	EUID   uint32
	GID    uint32
	Groups []uint32
}

// WriteProbe writes the current process PATH and identity to a dedicated
// descriptor. The production Unix shell path uses WriteProbeSocket because
// several shells close unknown inherited descriptors; this entrypoint remains
// useful for the framed-protocol contract and compatible internal callers.
func WriteProbe(fd uintptr, nonce string) error {
	file := os.NewFile(fd, "cxp-user-path-probe")
	if file == nil {
		return fmt.Errorf("invalid user PATH probe descriptor %d", fd)
	}
	defer file.Close()
	return writeCurrentProbe(file, nonce)
}

func writeCurrentProbe(w io.Writer, nonce string) error {
	if len(nonce) != probeNonceBytes*2 {
		return fmt.Errorf("invalid user PATH probe nonce")
	}
	cwd, _ := os.Getwd()
	uid, euid, gid := currentProcessIDs()
	return writeProbePayload(w, probePayload{
		Nonce:  nonce,
		Path:   os.Getenv("PATH"),
		Home:   os.Getenv("HOME"),
		Cwd:    cwd,
		UID:    uid,
		EUID:   euid,
		GID:    gid,
		Groups: currentProcessGroups(),
	})
}

func writeProbePayload(w io.Writer, payload probePayload) error {
	fields := []string{payload.Nonce, payload.Path, payload.Home, payload.Cwd}
	for _, field := range fields {
		if len(field) > maxProbeField {
			return fmt.Errorf("user PATH probe field exceeds %d bytes", maxProbeField)
		}
	}
	if len(payload.Groups) > maxProbeGroups {
		return fmt.Errorf("user PATH probe has too many supplementary groups")
	}
	if _, err := io.WriteString(w, probeMagic); err != nil {
		return err
	}
	values := []any{
		probeVersion,
		uint16(len(payload.Nonce)),
		uint32(len(payload.Path)),
		uint32(len(payload.Home)),
		uint32(len(payload.Cwd)),
		payload.UID,
		payload.EUID,
		payload.GID,
		uint32(len(payload.Groups)),
	}
	for _, value := range values {
		if err := binary.Write(w, binary.BigEndian, value); err != nil {
			return err
		}
	}
	for _, group := range payload.Groups {
		if err := binary.Write(w, binary.BigEndian, group); err != nil {
			return err
		}
	}
	for _, field := range fields {
		if _, err := io.WriteString(w, field); err != nil {
			return err
		}
	}
	return nil
}

func readProbePayload(r io.Reader, expectedNonce string) (probePayload, error) {
	magic := make([]byte, len(probeMagic))
	if _, err := io.ReadFull(r, magic); err != nil {
		return probePayload{}, fmt.Errorf("read user PATH probe magic: %w", err)
	}
	if string(magic) != probeMagic {
		return probePayload{}, fmt.Errorf("invalid user PATH probe magic")
	}
	var version uint16
	var nonceLen uint16
	var pathLen, homeLen, cwdLen uint32
	var uid, euid, gid uint32
	var groupCount uint32
	for _, value := range []any{&version, &nonceLen, &pathLen, &homeLen, &cwdLen, &uid, &euid, &gid, &groupCount} {
		if err := binary.Read(r, binary.BigEndian, value); err != nil {
			return probePayload{}, fmt.Errorf("read user PATH probe header: %w", err)
		}
	}
	if version != probeVersion {
		return probePayload{}, fmt.Errorf("unsupported user PATH probe version %d", version)
	}
	if int(nonceLen) > maxProbeField || pathLen > maxProbeField || homeLen > maxProbeField || cwdLen > maxProbeField {
		return probePayload{}, fmt.Errorf("user PATH probe frame is too large")
	}
	if groupCount > maxProbeGroups {
		return probePayload{}, fmt.Errorf("user PATH probe has too many supplementary groups")
	}
	groups := make([]uint32, int(groupCount))
	for index := range groups {
		if err := binary.Read(r, binary.BigEndian, &groups[index]); err != nil {
			return probePayload{}, fmt.Errorf("read user PATH probe groups: %w", err)
		}
	}
	readField := func(length uint32) (string, error) {
		value := make([]byte, int(length))
		_, err := io.ReadFull(r, value)
		return string(value), err
	}
	nonce, err := readField(uint32(nonceLen))
	if err != nil {
		return probePayload{}, fmt.Errorf("read user PATH probe nonce: %w", err)
	}
	pathValue, err := readField(pathLen)
	if err != nil {
		return probePayload{}, fmt.Errorf("read user PATH probe PATH: %w", err)
	}
	home, err := readField(homeLen)
	if err != nil {
		return probePayload{}, fmt.Errorf("read user PATH probe HOME: %w", err)
	}
	cwd, err := readField(cwdLen)
	if err != nil {
		return probePayload{}, fmt.Errorf("read user PATH probe cwd: %w", err)
	}
	if nonce != expectedNonce {
		return probePayload{}, fmt.Errorf("user PATH probe nonce mismatch")
	}
	return probePayload{Nonce: nonce, Path: pathValue, Home: home, Cwd: cwd, UID: uid, EUID: euid, GID: gid, Groups: groups}, nil
}
