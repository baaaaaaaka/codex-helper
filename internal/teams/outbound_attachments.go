package teams

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	defaultOutboundUploadFolder = "Microsoft Teams Chat Files"
	maxOutboundAttachmentBytes  = 20 << 20
)

type OutboundAttachmentOptions struct {
	Root          string
	AllowAnyPath  bool
	UploadFolder  string
	Message       string
	MaxBytes      int64
	AllowedExts   map[string]bool
	GeneratedName string
}

type OutboundAttachmentFile struct {
	Path        string
	Name        string
	UploadName  string
	ContentType string
	Bytes       []byte
	Size        int64
}

type OutboundAttachmentResult struct {
	File    OutboundAttachmentFile
	Item    DriveItem
	Message ChatMessage
}

func DefaultOutboundRoot() (string, error) {
	base, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(base, "codex-helper", "teams-outbound"), nil
}

func DefaultOutboundUploadFolder() string {
	return defaultOutboundUploadFolder
}

func NewFileWriteGraphClient(out io.Writer) (*GraphClient, error) {
	return NewFileWriteGraphClientWithHTTPClient(out, nil)
}

func NewFileWriteGraphClientWithHTTPClient(out io.Writer, client *http.Client) (*GraphClient, error) {
	cfg, err := DefaultFileWriteAuthConfig()
	if err != nil {
		return nil, err
	}
	return newGraphClientWithHTTPClient(newNonInteractiveAuthManagerWithHTTPClient(cfg, client, "Teams file upload", "codex-proxy teams auth file-write"), out, client), nil
}

func FileWriteAuthCacheAvailable() (string, bool, error) {
	cfg, err := DefaultFileWriteAuthConfig()
	if err != nil {
		return "", false, err
	}
	tok, err := readTokenCache(cfg.CachePath)
	if errors.Is(err, os.ErrNotExist) {
		return cfg.CachePath, false, nil
	}
	if err != nil {
		return cfg.CachePath, false, err
	}
	return cfg.CachePath, tok.AccessToken != "" || tok.RefreshToken != "", nil
}

func PrepareOutboundAttachment(path string, opts OutboundAttachmentOptions) (OutboundAttachmentFile, error) {
	root := strings.TrimSpace(opts.Root)
	if root == "" {
		var err error
		root, err = DefaultOutboundRoot()
		if err != nil {
			return OutboundAttachmentFile{}, err
		}
	}
	if !opts.AllowAnyPath {
		if err := ensureOutboundRoot(root); err != nil {
			return OutboundAttachmentFile{}, err
		}
	}
	resolved, err := resolveOutboundPath(path, root, opts.AllowAnyPath)
	if err != nil {
		return OutboundAttachmentFile{}, err
	}
	maxBytes := opts.MaxBytes
	if maxBytes <= 0 {
		maxBytes = maxOutboundAttachmentBytes
	}
	data, size, err := readOutboundAttachmentFile(resolved, root, opts.AllowAnyPath, maxBytes)
	if err != nil {
		return OutboundAttachmentFile{}, err
	}
	name := safeAttachmentName(filepath.Base(resolved))
	if name == "" || strings.HasPrefix(name, ".") {
		return OutboundAttachmentFile{}, fmt.Errorf("unsafe upload file name")
	}
	ext := strings.ToLower(filepath.Ext(name))
	if !allowedOutboundExtension(ext, opts.AllowedExts) {
		return OutboundAttachmentFile{}, fmt.Errorf("file extension %q is not allowed for Teams upload", ext)
	}
	contentType := outboundContentType(ext)
	uploadName := strings.TrimSpace(opts.GeneratedName)
	if uploadName == "" {
		uploadName = outboundUploadName(name, time.Now())
	}
	if !safeDrivePathSegment(uploadName) {
		return OutboundAttachmentFile{}, fmt.Errorf("unsafe generated upload file name")
	}
	return OutboundAttachmentFile{
		Path:        resolved,
		Name:        name,
		UploadName:  uploadName,
		ContentType: contentType,
		Bytes:       data,
		Size:        size,
	}, nil
}

func SendOutboundAttachment(ctx context.Context, graph *GraphClient, chatID string, file OutboundAttachmentFile, opts OutboundAttachmentOptions) (OutboundAttachmentResult, error) {
	meta, err := UploadOutboundAttachment(ctx, graph, file, opts)
	if err != nil {
		return OutboundAttachmentResult{}, err
	}
	message := strings.TrimSpace(opts.Message)
	if message == "" {
		message = "file attached: " + file.Name
	}
	msg, err := graph.SendDriveItemAttachment(ctx, chatID, meta, message)
	if err != nil {
		return OutboundAttachmentResult{}, err
	}
	return OutboundAttachmentResult{File: file, Item: meta, Message: msg}, nil
}

func UploadOutboundAttachment(ctx context.Context, graph *GraphClient, file OutboundAttachmentFile, opts OutboundAttachmentOptions) (DriveItem, error) {
	if graph == nil {
		return DriveItem{}, fmt.Errorf("Graph client is required")
	}
	uploadFolder := strings.TrimSpace(opts.UploadFolder)
	if uploadFolder == "" {
		uploadFolder = defaultOutboundUploadFolder
	}
	item, err := graph.UploadSmallDriveItem(ctx, uploadFolder, file.UploadName, file.Bytes, file.ContentType)
	if err != nil {
		return DriveItem{}, err
	}
	meta, err := graph.GetDriveItemMetadata(ctx, item.ID)
	if err != nil {
		return DriveItem{}, err
	}
	return meta, nil
}

func resolveOutboundPath(path string, root string, allowAny bool) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("file path is required")
	}
	if allowAny {
		return filepath.Abs(path)
	}
	if filepath.IsAbs(path) {
		return "", fmt.Errorf("absolute paths require --allow-local-path")
	}
	clean := filepath.Clean(path)
	if clean == "." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) || clean == ".." {
		return "", fmt.Errorf("path must stay under Teams outbound root")
	}
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.Abs(filepath.Join(rootAbs, clean))
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(rootAbs, resolved)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("path must stay under Teams outbound root")
	}
	return resolved, nil
}

func ensureOutboundRoot(root string) error {
	info, err := os.Lstat(root)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(root, 0o700); err != nil {
			return outboundPathError("create Teams outbound root", err)
		}
		return os.Chmod(root, 0o700)
	}
	if err != nil {
		return outboundPathError("inspect Teams outbound root", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("Teams outbound root must not be a symlink")
	}
	if !info.IsDir() {
		return fmt.Errorf("Teams outbound root is not a directory")
	}
	return os.Chmod(root, 0o700)
}

func readOutboundAttachmentFile(path string, root string, allowAny bool, maxBytes int64) ([]byte, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, outboundPathError("open Teams upload file", err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, 0, outboundPathError("inspect Teams upload file", err)
	}
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return nil, 0, outboundPathError("inspect Teams upload path", err)
	}
	if pathInfo.Mode()&os.ModeSymlink != 0 {
		return nil, 0, fmt.Errorf("refusing to upload symlink: %s", filepath.Base(path))
	}
	if !info.Mode().IsRegular() {
		return nil, 0, fmt.Errorf("refusing to upload non-regular file: %s", filepath.Base(path))
	}
	if info.Size() > maxBytes {
		return nil, 0, fmt.Errorf("refusing to upload %d-byte file; limit is %d bytes", info.Size(), maxBytes)
	}
	if !allowAny {
		if err := ensureResolvedPathUnderRoot(path, root); err != nil {
			return nil, 0, err
		}
		currentInfo, err := os.Stat(path)
		if err != nil {
			return nil, 0, outboundPathError("inspect Teams upload path", err)
		}
		if !os.SameFile(info, currentInfo) {
			return nil, 0, fmt.Errorf("Teams upload file changed during safety checks")
		}
	}
	data, err := io.ReadAll(io.LimitReader(f, maxBytes+1))
	if err != nil {
		return nil, 0, outboundPathError("read Teams upload file", err)
	}
	if int64(len(data)) > maxBytes {
		return nil, 0, fmt.Errorf("refusing to upload file larger than %d bytes", maxBytes)
	}
	return data, int64(len(data)), nil
}

func ensureResolvedPathUnderRoot(path string, root string) error {
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return outboundPathError("resolve Teams outbound root", err)
	}
	realPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return outboundPathError("resolve Teams upload path", err)
	}
	realRoot, err = filepath.Abs(realRoot)
	if err != nil {
		return outboundPathError("resolve Teams outbound root", err)
	}
	realPath, err = filepath.Abs(realPath)
	if err != nil {
		return outboundPathError("resolve Teams upload path", err)
	}
	rel, err := filepath.Rel(realRoot, realPath)
	if err != nil {
		return outboundPathError("compare Teams upload path", err)
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return fmt.Errorf("path must stay under Teams outbound root")
	}
	return nil
}

func outboundPathError(action string, err error) error {
	if errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("%s: file not found", action)
	}
	if errors.Is(err, os.ErrPermission) {
		return fmt.Errorf("%s: permission denied", action)
	}
	var pathErr *os.PathError
	if errors.As(err, &pathErr) {
		return fmt.Errorf("%s: %v", action, pathErr.Err)
	}
	return fmt.Errorf("%s: %v", action, err)
}

func allowedOutboundExtension(ext string, allowed map[string]bool) bool {
	if len(allowed) == 0 {
		allowed = defaultOutboundExtensions()
	}
	return allowed[strings.ToLower(ext)]
}

func defaultOutboundExtensions() map[string]bool {
	return map[string]bool{
		".txt":   true,
		".md":    true,
		".log":   true,
		".json":  true,
		".csv":   true,
		".patch": true,
		".diff":  true,
		".png":   true,
		".jpg":   true,
		".jpeg":  true,
		".gif":   true,
		".webp":  true,
		".pdf":   true,
	}
}

func outboundContentType(ext string) string {
	switch strings.ToLower(ext) {
	case ".md", ".log", ".patch", ".diff":
		return "text/plain"
	}
	if ctype := mime.TypeByExtension(ext); ctype != "" {
		return ctype
	}
	return "application/octet-stream"
}

func outboundUploadName(name string, now time.Time) string {
	name = safeAttachmentName(name)
	if name == "" {
		name = "attachment.txt"
	}
	stamp := now.UTC().Format("20060102T150405.000000000")
	return "codex-helper-" + stamp + "-" + name
}
