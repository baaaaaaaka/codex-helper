package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestTeamsLocalSupervisorDocsStayAligned(t *testing.T) {
	repoRoot := sourceCheckoutRootForDocsDriftTest(t)
	read := func(path string) string {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		return string(data)
	}
	docs := map[string]string{
		"README.md":                            read(filepath.Join(repoRoot, "README.md")),
		"deployment guide":                     read(filepath.Join(repoRoot, "docs", "teams_source_deployment_guide.md")),
		"feature interference matrix":          read(filepath.Join(repoRoot, "docs", "cxp_feature_interference_matrix.md")),
		"security threat model":                read(filepath.Join(repoRoot, "docs", "teams_security_threat_model.md")),
		"integration execution plan":           read(filepath.Join(repoRoot, "docs", "teams_integration_execution_plan.md")),
		"integration plan":                     read(filepath.Join(repoRoot, "docs", "teams_integration_plan.md")),
		"built-in cxp skill command reference": read(filepath.Join(repoRoot, "internal", "skills", "builtin", "cxp", "references", "commands.md")),
	}
	for name, doc := range docs {
		for _, want := range []string{
			"local-supervisor",
			"systemd --user",
			"terminal close",
		} {
			if !strings.Contains(doc, want) {
				t.Fatalf("%s missing %q", name, want)
			}
		}
	}
	for name, doc := range map[string]string{
		"README.md":                   docs["README.md"],
		"deployment guide":            docs["deployment guide"],
		"security threat model":       docs["security threat model"],
		"integration execution plan":  docs["integration execution plan"],
		"integration plan":            docs["integration plan"],
		"feature interference matrix": docs["feature interference matrix"],
	} {
		if !strings.Contains(doc, "machine") || !strings.Contains(doc, "reboot") {
			t.Fatalf("%s missing local-supervisor reboot limitation", name)
		}
	}
	for name, doc := range docs {
		if !strings.Contains(doc, "sticky") && !strings.Contains(doc, "backend flapping") {
			t.Fatalf("%s missing local-supervisor sticky/backend-flapping semantics", name)
		}
	}
	for name, doc := range map[string]string{
		"README.md":                            docs["README.md"],
		"deployment guide":                     docs["deployment guide"],
		"integration plan":                     docs["integration plan"],
		"built-in cxp skill command reference": docs["built-in cxp skill command reference"],
	} {
		if !strings.Contains(doc, "WSL") || !strings.Contains(doc, "local-supervisor") {
			t.Fatalf("%s missing WSL local-supervisor backend semantics", name)
		}
	}
}

func sourceCheckoutRootForDocsDriftTest(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	start := dir
	for {
		if regularFileExists(filepath.Join(dir, "README.md")) &&
			regularFileExists(filepath.Join(dir, "internal", "skills", "builtin", "cxp", "references", "commands.md")) {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	t.Skipf("source checkout not available from %s; docs drift test runs in source-tree CI", start)
	return ""
}

func regularFileExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.Mode().IsRegular()
}
