package helperruntime

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// Only the explicit installer reads protocol-one JSON. Its Good/Previous
// runtimes cannot become protocol-two rollback targets. Old trial evidence is
// retained in the archived source, never rearmed by migration.
func readLegacyBootForInstall(root string) (s BootState, found bool, err error) {
	path := filepath.Join(root, "boot.json")
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return s, false, nil
	}
	if err != nil {
		return s, false, err
	}
	if !info.Mode().IsRegular() || info.Size() > 8192 {
		return s, true, fmt.Errorf("invalid legacy boot record")
	}
	f, err := openVersionPointerFile(path)
	if err != nil {
		return s, true, err
	}
	defer f.Close()
	d := json.NewDecoder(io.LimitReader(f, 8193))
	if err = d.Decode(&s); err != nil {
		return s, true, err
	}
	if _, err = d.Token(); err != io.EOF {
		return s, true, fmt.Errorf("trailing legacy boot record")
	}
	if s.Schema != 1 {
		return s, true, fmt.Errorf("unsupported legacy boot protocol %d", s.Schema)
	}
	checked := s
	checked.Schema = BootProtocol
	if err = validateBoot(checked); err != nil {
		return s, true, err
	}
	return s, true, nil
}

func readVersionPointer(root string, name string) (string, error) {
	path := filepath.Join(filepath.Clean(root), name)
	info, err := os.Lstat(path)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Size() > 256 {
		return "", fmt.Errorf("invalid cxp runtime pointer")
	}
	file, err := openVersionPointerFile(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	raw, err := io.ReadAll(io.LimitReader(file, 257))
	if err != nil {
		return "", err
	}
	if len(raw) > 256 {
		return "", fmt.Errorf("cxp runtime pointer too large")
	}
	value, ok := NormalizeVersion(string(raw))
	if !ok {
		return "", fmt.Errorf("invalid cxp %s runtime value %q", name, strings.TrimSpace(string(raw)))
	}
	return value, nil
}
