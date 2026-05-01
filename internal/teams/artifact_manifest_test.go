package teams

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseArtifactManifestRejectsBadPaths(t *testing.T) {
	badPaths := []string{
		"/tmp/result.txt",
		"../result.txt",
		"nested/../../result.txt",
		`C:\tmp\result.txt`,
		"",
	}
	for _, badPath := range badPaths {
		_, err := ParseArtifactManifest(testArtifactManifest(t, []ArtifactManifestEntry{{Path: badPath}}), ArtifactManifestOptions{
			OutboundRoot: t.TempDir(),
		})
		if err == nil {
			t.Fatalf("expected path %q to be rejected", badPath)
		}
	}
}

func TestParseArtifactManifestRejectsDirectoryOversizeSymlinkAndDisallowedExtension(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		info ArtifactManifestFileInfo
		max  int64
	}{
		{name: "directory", path: "result.txt", info: ArtifactManifestFileInfo{IsDir: true}},
		{name: "symlink", path: "result.txt", info: ArtifactManifestFileInfo{IsSymlink: true}},
		{name: "oversize", path: "result.txt", info: ArtifactManifestFileInfo{Size: 11}, max: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParseArtifactManifest(testArtifactManifest(t, []ArtifactManifestEntry{{Path: tc.path}}), ArtifactManifestOptions{
				OutboundRoot: t.TempDir(),
				MaxBytes:     tc.max,
				ValidateFile: func(ArtifactManifestValidationRequest) (ArtifactManifestFileInfo, error) {
					return tc.info, nil
				},
			})
			if err == nil {
				t.Fatalf("expected %s rejection", tc.name)
			}
		})
	}

	_, err := ParseArtifactManifest(testArtifactManifest(t, []ArtifactManifestEntry{{Path: "run.sh"}}), ArtifactManifestOptions{
		OutboundRoot: t.TempDir(),
	})
	if err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("expected disallowed extension rejection, got %v", err)
	}
}

func TestParseArtifactManifestPreservesValidFileOrdering(t *testing.T) {
	root := t.TempDir()
	var seen []string
	plan, err := ParseArtifactManifest(testArtifactManifest(t, []ArtifactManifestEntry{
		{Path: "reports/b.txt"},
		{Path: "images/a.png", Name: "final image.png", ContentType: "image/png"},
	}), ArtifactManifestOptions{
		OutboundRoot: root,
		SessionID:    "s001",
		TurnID:       "turn-009",
		ValidateFile: func(req ArtifactManifestValidationRequest) (ArtifactManifestFileInfo, error) {
			seen = append(seen, req.CleanPath)
			if req.LocalPath != filepath.Join(root, filepath.FromSlash(req.CleanPath)) {
				return ArtifactManifestFileInfo{}, fmt.Errorf("unexpected local path %q", req.LocalPath)
			}
			return ArtifactManifestFileInfo{Size: int64(len(req.CleanPath))}, nil
		},
	})
	if err != nil {
		t.Fatalf("ParseArtifactManifest error: %v", err)
	}
	if got := strings.Join(seen, ","); got != "reports/b.txt,images/a.png" {
		t.Fatalf("validation order = %q", got)
	}
	if len(plan.Files) != 2 {
		t.Fatalf("expected 2 files, got %#v", plan.Files)
	}
	if plan.Files[0].CleanPath != "reports/b.txt" || plan.Files[1].CleanPath != "images/a.png" {
		t.Fatalf("files reordered: %#v", plan.Files)
	}
	if !strings.Contains(plan.Files[0].UploadNameSeed, "s001") || !strings.Contains(plan.Files[0].UploadNameSeed, "turn-009") || !strings.HasSuffix(plan.Files[0].UploadNameSeed, "b.txt") {
		t.Fatalf("upload name seed missing session/turn/name: %q", plan.Files[0].UploadNameSeed)
	}
	if !strings.HasSuffix(plan.Files[1].UploadNameSeed, "final_image.png") {
		t.Fatalf("upload name seed should use safe visible name, got %q", plan.Files[1].UploadNameSeed)
	}
	if plan.Files[0].Size != int64(len("reports/b.txt")) {
		t.Fatalf("size not copied from validation hook: %#v", plan.Files[0])
	}
}

func TestCleanArtifactManifestPath(t *testing.T) {
	got, err := CleanArtifactManifestPath(`reports\final.txt`)
	if err != nil {
		t.Fatalf("CleanArtifactManifestPath error: %v", err)
	}
	if got != "reports/final.txt" {
		t.Fatalf("clean path = %q", got)
	}
}

func testArtifactManifest(t *testing.T, files []ArtifactManifestEntry) []byte {
	t.Helper()
	raw, err := json.Marshal(struct {
		Version int                     `json:"version"`
		Files   []ArtifactManifestEntry `json:"files"`
	}{
		Version: ArtifactManifestVersion,
		Files:   files,
	})
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	return raw
}
