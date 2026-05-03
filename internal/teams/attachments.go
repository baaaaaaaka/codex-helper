package teams

import (
	"context"
	"fmt"
	"mime"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const maxHostedContentPerMessage = 5
const maxReferenceAttachmentsPerMessage = 5

var hostedContentRefPattern = regexp.MustCompile(`(?i)/hostedContents/([^/"'<>\s]+)/\$value`)

type LocalAttachment struct {
	Path        string
	PromptPath  string
	ContentType string
	SourceID    string
}

func HostedContentIDsFromHTML(content string) []string {
	ids, _ := hostedContentIDsFromHTML(content, maxHostedContentPerMessage)
	return ids
}

func hostedContentIDsFromHTML(content string, limit int) ([]string, bool) {
	if limit <= 0 {
		limit = maxHostedContentPerMessage
	}
	matches := hostedContentRefPattern.FindAllStringSubmatch(content, -1)
	seen := make(map[string]bool)
	var ids []string
	truncated := false
	for _, match := range matches {
		if len(match) < 2 {
			continue
		}
		id := strings.TrimSpace(match[1])
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true
		if len(ids) >= limit {
			truncated = true
			continue
		}
		ids = append(ids, id)
	}
	return ids, truncated
}

func (b *Bridge) downloadHostedContentAttachments(ctx context.Context, session *Session, chatID string, msg ChatMessage) ([]LocalAttachment, func(), string, error) {
	ids, truncated := hostedContentIDsFromHTML(msg.Body.Content, maxHostedContentPerMessage)
	if len(ids) == 0 {
		if truncated {
			return nil, func() {}, fmt.Sprintf("Teams message has more than %d inline images. Please send fewer images in one message.", maxHostedContentPerMessage), nil
		}
		return nil, func() {}, "", nil
	}
	if truncated {
		return nil, func() {}, fmt.Sprintf("Teams message has more than %d inline images. Please send fewer images in one message.", maxHostedContentPerMessage), nil
	}
	dir, err := createAttachmentTempDir(session, msg)
	if err != nil {
		return nil, func() {}, "", err
	}
	cleanup := func() {
		_ = os.RemoveAll(dir)
	}
	var files []LocalAttachment
	for i, id := range ids {
		value, err := b.readClient().GetHostedContentValue(ctx, chatID, msg.ID, id)
		if err != nil {
			cleanup()
			return nil, func() {}, "", err
		}
		ext := attachmentExtension(value.ContentType)
		path := filepath.Join(dir, fmt.Sprintf("attachment-%03d%s", i+1, ext))
		if err := writePrivateFile(path, value.Bytes); err != nil {
			cleanup()
			return nil, func() {}, "", err
		}
		files = append(files, LocalAttachment{
			Path:        path,
			PromptPath:  promptAttachmentPath(session, path),
			ContentType: strings.TrimSpace(value.ContentType),
			SourceID:    id,
		})
	}
	return files, cleanup, "", nil
}

func (b *Bridge) downloadReferenceFileAttachments(ctx context.Context, session *Session, msg ChatMessage) ([]LocalAttachment, func(), string, error) {
	if len(msg.Attachments) == 0 {
		return nil, func() {}, "", nil
	}
	var refs []MessageAttachment
	supportedCount := 0
	for _, attachment := range msg.Attachments {
		if !isSupportedReferenceAttachment(attachment) {
			return nil, func() {}, UnsupportedAttachmentMessage(msg.Attachments), nil
		}
		supportedCount++
		if len(refs) < maxReferenceAttachmentsPerMessage {
			refs = append(refs, attachment)
		}
	}
	if supportedCount > maxReferenceAttachmentsPerMessage {
		return nil, func() {}, fmt.Sprintf("Teams message has more than %d file attachments. Please send fewer files in one message.", maxReferenceAttachmentsPerMessage), nil
	}
	if len(refs) == 0 {
		return nil, func() {}, "", nil
	}
	if !fileAttachmentScopesEnabled() {
		return nil, func() {}, "Teams file attachment download requires `Files.Read` or `Files.ReadWrite` in CODEX_HELPER_TEAMS_READ_SCOPES. Re-authenticate with `codex-proxy teams auth read` after adding that scope.", nil
	}
	dir, err := createAttachmentTempDir(session, msg)
	if err != nil {
		return nil, func() {}, "", err
	}
	cleanup := func() {
		_ = os.RemoveAll(dir)
	}
	var files []LocalAttachment
	for i, attachment := range refs {
		value, err := b.readClient().GetSharedDriveItemContent(ctx, attachment.ContentURL)
		if err != nil {
			cleanup()
			return nil, func() {}, "", err
		}
		ext := attachmentExtension(firstNonEmptyString(value.ContentType, attachment.ContentType))
		if nameExt := filepath.Ext(safeAttachmentName(attachment.Name)); nameExt != "" {
			ext = nameExt
		}
		path := filepath.Join(dir, fmt.Sprintf("file-%03d%s", i+1, ext))
		if err := writePrivateFile(path, value.Bytes); err != nil {
			cleanup()
			return nil, func() {}, "", err
		}
		files = append(files, LocalAttachment{
			Path:        path,
			PromptPath:  promptAttachmentPath(session, path),
			ContentType: strings.TrimSpace(firstNonEmptyString(value.ContentType, attachment.ContentType)),
			SourceID:    strings.TrimSpace(firstNonEmptyString(attachment.ID, attachment.ContentURL)),
		})
	}
	return files, cleanup, "", nil
}

func isSupportedReferenceAttachment(attachment MessageAttachment) bool {
	if !strings.EqualFold(strings.TrimSpace(attachment.ContentType), "reference") {
		return false
	}
	u, err := url.Parse(strings.TrimSpace(attachment.ContentURL))
	if err != nil || u.Scheme != "https" || u.Host == "" || u.User != nil {
		return false
	}
	host := strings.ToLower(u.Hostname())
	return allowedSharePointHost(host)
}

func allowedSharePointHost(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "" {
		return false
	}
	configured := strings.TrimSpace(os.Getenv("CODEX_HELPER_TEAMS_ALLOWED_SHAREPOINT_HOSTS"))
	if configured == "" {
		return strings.HasSuffix(host, ".sharepoint.com")
	}
	for _, raw := range strings.FieldsFunc(configured, func(r rune) bool { return r == ',' || r == ';' || r == ' ' || r == '\n' || r == '\t' }) {
		allowed := strings.ToLower(strings.TrimSpace(raw))
		if allowed == "" {
			continue
		}
		if strings.HasPrefix(allowed, ".") {
			if strings.HasSuffix(host, allowed) {
				return true
			}
			continue
		}
		if host == allowed {
			return true
		}
	}
	return false
}

func fileAttachmentScopesEnabled() bool {
	cfg, err := DefaultEffectiveReadAuthConfig()
	if err != nil {
		return false
	}
	for _, scope := range strings.Fields(cfg.Scopes) {
		if scope == "Files.Read" || scope == "Files.ReadWrite" {
			return true
		}
	}
	return false
}

func safeAttachmentName(name string) string {
	name = filepath.Base(strings.TrimSpace(name))
	if name == "." || name == string(filepath.Separator) {
		return ""
	}
	return safePathPart(name)
}

func firstNonEmptyString(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func createAttachmentTempDir(session *Session, msg ChatMessage) (string, error) {
	if session != nil && strings.TrimSpace(session.Cwd) != "" {
		cwd := filepath.Clean(strings.TrimSpace(session.Cwd))
		root := filepath.Join(cwd, ".codex-helper", "teams-attachments")
		if err := os.MkdirAll(root, 0o700); err == nil {
			_ = os.Chmod(filepath.Join(cwd, ".codex-helper"), 0o700)
			_ = os.Chmod(root, 0o700)
			prefix := attachmentTempPrefix(session, msg)
			return os.MkdirTemp(root, prefix)
		}
	}
	base, err := os.UserCacheDir()
	if err != nil {
		return "", err
	}
	root := filepath.Join(base, "codex-helper", "teams-attachments")
	if err := os.MkdirAll(root, 0o700); err != nil {
		return "", err
	}
	if err := os.Chmod(root, 0o700); err != nil {
		return "", err
	}
	return os.MkdirTemp(root, attachmentTempPrefix(session, msg))
}

func attachmentTempPrefix(session *Session, msg ChatMessage) string {
	prefix := "message-"
	if session != nil && strings.TrimSpace(session.ID) != "" {
		prefix = safePathPart(session.ID) + "-"
	}
	if strings.TrimSpace(msg.ID) != "" {
		prefix += safePathPart(msg.ID) + "-"
	}
	return prefix
}

func promptAttachmentPath(session *Session, path string) string {
	if session == nil || strings.TrimSpace(session.Cwd) == "" {
		return path
	}
	cwd, err := filepath.Abs(filepath.Clean(strings.TrimSpace(session.Cwd)))
	if err != nil {
		return path
	}
	resolved, err := filepath.Abs(path)
	if err != nil {
		return path
	}
	rel, err := filepath.Rel(cwd, resolved)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return path
	}
	return filepath.ToSlash(rel)
}

func writePrivateFile(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	_, writeErr := f.Write(data)
	closeErr := f.Close()
	if writeErr != nil {
		return writeErr
	}
	return closeErr
}

func attachmentExtension(contentType string) string {
	contentType = strings.TrimSpace(strings.Split(contentType, ";")[0])
	if contentType == "" {
		return ".bin"
	}
	exts, err := mime.ExtensionsByType(contentType)
	if err == nil && len(exts) > 0 {
		return exts[0]
	}
	switch contentType {
	case "image/png":
		return ".png"
	case "image/jpeg":
		return ".jpg"
	case "image/gif":
		return ".gif"
	case "text/plain":
		return ".txt"
	default:
		return ".bin"
	}
}

func PromptWithLocalAttachments(text string, files []LocalAttachment) string {
	text = strings.TrimSpace(text)
	if len(files) == 0 {
		return text
	}
	if text == "" {
		text = "Please inspect the attached file(s)."
	}
	var b strings.Builder
	b.WriteString(text)
	b.WriteString("\n\nAttached files saved locally for this turn:\n")
	for _, file := range files {
		b.WriteString("- ")
		b.WriteString(firstNonEmptyString(file.PromptPath, file.Path))
		if file.ContentType != "" {
			b.WriteString(" (")
			b.WriteString(file.ContentType)
			b.WriteString(")")
		}
		b.WriteString("\n")
	}
	return strings.TrimSpace(b.String())
}

func safePathPart(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return "empty"
	}
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-' || r == '_' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
		if b.Len() >= 80 {
			break
		}
	}
	if b.Len() == 0 {
		return "empty"
	}
	return b.String()
}
