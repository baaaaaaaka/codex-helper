package teams

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"path/filepath"
	"regexp"
	"strings"
)

const ArtifactManifestVersion = 1

var windowsDrivePathPattern = regexp.MustCompile(`^[A-Za-z]:(?:[\\/]|$)`)

type ArtifactManifestEntry struct {
	Path        string `json:"path"`
	Name        string `json:"name,omitempty"`
	ContentType string `json:"content_type,omitempty"`
}

type ArtifactManifestPlan struct {
	Version int
	Files   []ArtifactManifestFile
}

type ArtifactManifestFile struct {
	Entry          ArtifactManifestEntry
	CleanPath      string
	LocalPath      string
	Name           string
	UploadNameSeed string
	Size           int64
}

type ArtifactManifestOptions struct {
	OutboundRoot string
	SessionID    string
	TurnID       string
	MaxBytes     int64
	AllowedExts  map[string]bool
	ValidateFile ArtifactManifestValidationHook
}

type ArtifactManifestValidationRequest struct {
	Entry     ArtifactManifestEntry
	CleanPath string
	LocalPath string
	Root      string
}

type ArtifactManifestFileInfo struct {
	Size      int64
	IsDir     bool
	IsSymlink bool
}

type ArtifactManifestValidationHook func(ArtifactManifestValidationRequest) (ArtifactManifestFileInfo, error)

func ParseArtifactManifest(data []byte, opts ArtifactManifestOptions) (ArtifactManifestPlan, error) {
	var payload struct {
		Version int                     `json:"version"`
		Files   []ArtifactManifestEntry `json:"files"`
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&payload); err != nil {
		return ArtifactManifestPlan{}, fmt.Errorf("parse artifact manifest: %w", err)
	}
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		if err != nil {
			return ArtifactManifestPlan{}, fmt.Errorf("artifact manifest has trailing data: %w", err)
		}
		return ArtifactManifestPlan{}, fmt.Errorf("artifact manifest has trailing data")
	}
	version := payload.Version
	if version == 0 {
		version = ArtifactManifestVersion
	}
	if version != ArtifactManifestVersion {
		return ArtifactManifestPlan{}, fmt.Errorf("unsupported artifact manifest version %d", payload.Version)
	}

	maxBytes := opts.MaxBytes
	if maxBytes <= 0 {
		maxBytes = maxOutboundAttachmentBytes
	}
	root := strings.TrimSpace(opts.OutboundRoot)
	rootAbs := ""
	if root != "" {
		var err error
		rootAbs, err = filepath.Abs(root)
		if err != nil {
			return ArtifactManifestPlan{}, fmt.Errorf("resolve artifact root: %w", err)
		}
	}

	files := make([]ArtifactManifestFile, 0, len(payload.Files))
	for i, entry := range payload.Files {
		cleanPath, err := CleanArtifactManifestPath(entry.Path)
		if err != nil {
			return ArtifactManifestPlan{}, fmt.Errorf("artifact file %d: %w", i+1, err)
		}
		ext := strings.ToLower(path.Ext(cleanPath))
		if !allowedOutboundExtension(ext, opts.AllowedExts) {
			return ArtifactManifestPlan{}, fmt.Errorf("artifact file %d: file extension %q is not allowed for Teams upload", i+1, ext)
		}

		localPath := filepath.FromSlash(cleanPath)
		if rootAbs != "" {
			localPath = filepath.Join(rootAbs, filepath.FromSlash(cleanPath))
			if err := ensureArtifactLocalPathUnderRoot(rootAbs, localPath); err != nil {
				return ArtifactManifestPlan{}, fmt.Errorf("artifact file %d: %w", i+1, err)
			}
		}
		name := strings.TrimSpace(entry.Name)
		if name == "" {
			name = path.Base(cleanPath)
		}
		name = safeAttachmentName(name)
		if name == "" || strings.HasPrefix(name, ".") {
			return ArtifactManifestPlan{}, fmt.Errorf("artifact file %d: unsafe upload file name", i+1)
		}

		var size int64
		if opts.ValidateFile != nil {
			info, err := opts.ValidateFile(ArtifactManifestValidationRequest{
				Entry:     entry,
				CleanPath: cleanPath,
				LocalPath: localPath,
				Root:      rootAbs,
			})
			if err != nil {
				return ArtifactManifestPlan{}, fmt.Errorf("artifact file %d: %w", i+1, err)
			}
			if info.IsSymlink {
				return ArtifactManifestPlan{}, fmt.Errorf("artifact file %d: refusing to upload symlink: %s", i+1, path.Base(cleanPath))
			}
			if info.IsDir {
				return ArtifactManifestPlan{}, fmt.Errorf("artifact file %d: refusing to upload directory: %s", i+1, path.Base(cleanPath))
			}
			if info.Size > maxBytes {
				return ArtifactManifestPlan{}, fmt.Errorf("artifact file %d: refusing to upload %d-byte file; limit is %d bytes", i+1, info.Size, maxBytes)
			}
			size = info.Size
		}

		files = append(files, ArtifactManifestFile{
			Entry:          entry,
			CleanPath:      cleanPath,
			LocalPath:      localPath,
			Name:           name,
			UploadNameSeed: ArtifactUploadNameSeed(opts.SessionID, opts.TurnID, name),
			Size:           size,
		})
	}
	return ArtifactManifestPlan{Version: version, Files: files}, nil
}

func CleanArtifactManifestPath(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("path is required")
	}
	if strings.ContainsRune(raw, 0) {
		return "", fmt.Errorf("path contains NUL byte")
	}
	if windowsDrivePathPattern.MatchString(raw) {
		return "", fmt.Errorf("absolute paths are not allowed")
	}
	slashPath := strings.ReplaceAll(raw, "\\", "/")
	if strings.HasPrefix(slashPath, "//") || path.IsAbs(slashPath) {
		return "", fmt.Errorf("absolute paths are not allowed")
	}
	clean := path.Clean(slashPath)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", fmt.Errorf("path must stay under Teams outbound root")
	}
	return clean, nil
}

func ArtifactUploadNameSeed(sessionID string, turnID string, name string) string {
	sessionID = safePathPart(sessionID)
	turnID = safePathPart(turnID)
	name = safeAttachmentName(path.Base(strings.ReplaceAll(name, "\\", "/")))
	if name == "" || strings.HasPrefix(name, ".") {
		name = "artifact"
	}
	return "codex-artifact-" + sessionID + "-" + turnID + "-" + name
}

func ensureArtifactLocalPathUnderRoot(root string, localPath string) error {
	resolved, err := filepath.Abs(localPath)
	if err != nil {
		return err
	}
	rel, err := filepath.Rel(root, resolved)
	if err != nil {
		return err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return fmt.Errorf("path must stay under Teams outbound root")
	}
	return nil
}
