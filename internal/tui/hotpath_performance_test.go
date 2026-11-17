package tui

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/baaaaaaaka/codex-helper/internal/codexhistory"
)

func BenchmarkHotProjectRefilter(b *testing.B) {
	for _, n := range []int{100, 1000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			root := b.TempDir()
			projects := make([]codexhistory.Project, n)
			for i := range projects {
				path := filepath.Join(root, fmt.Sprintf("project-%d", i))
				if e := os.MkdirAll(path, 0700); e != nil {
					b.Fatal(e)
				}
				projects[i] = codexhistory.Project{Key: path, Path: path, Sessions: []codexhistory.Session{{SessionID: fmt.Sprint(i), FirstPrompt: "task"}}}
			}
			for _, cwd := range []string{"", root} {
				name := "no-cwd"
				if cwd != "" {
					name = "with-cwd"
				}
				b.Run(name, func(b *testing.B) {
					s := newTestState(projects)
					s.projectItems(Options{DefaultCwd: cwd})
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						s.projectFilter = fmt.Sprint(i % 2)
						_ = s.projectItems(Options{DefaultCwd: cwd})
					}
				})
			}
		})
	}
}

func BenchmarkHotLayoutStatusOnly(b *testing.B) {
	body := strings.Repeat("answer 你好 with content\n", 80000)
	first := prepareLayout(context.Background(), layoutJob{lines: []string{"header", body}, body: body, width: 100})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		result := prepareLayout(context.Background(), layoutJob{lines: []string{"changed header", body, "Refreshing preview..."}, body: body, width: 100, reuse: first.entry})
		if len(result.entry.bodyLines) == 0 || &result.entry.bodyLines[0] != &first.entry.bodyLines[0] {
			b.Fatal("body rewrapped")
		}
	}
}
