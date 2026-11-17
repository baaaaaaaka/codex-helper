package codexhistory

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func BenchmarkHotNames(b *testing.B) {
	for _, n := range []int{1000, 100000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			setBenchmarkUserCacheDir(b)
			root := os.Getenv(EnvCodexDir)
			path := filepath.Join(root, threadNameIndexFile)
			var body strings.Builder
			for i := 0; i < n; i++ {
				fmt.Fprintf(&body, "{\"id\":\"s-%d\",\"thread_name\":\"name %d\"}\n", i, i)
			}
			if err := os.WriteFile(path, []byte(body.String()), 0600); err != nil {
				b.Fatal(err)
			}
			if _, err := loadThreadNameIndexContext(context.Background(), root); err != nil {
				b.Fatal(err)
			}
			b.Run("unchanged", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					names, e := loadThreadNameIndexContext(context.Background(), root)
					if e != nil || names.size() != n {
						b.Fatal(e)
					}
				}
			})
			b.Run("single-rename", func(b *testing.B) {
				f, e := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
				if e != nil {
					b.Fatal(e)
				}
				defer f.Close()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if _, e := fmt.Fprintf(f, "{\"id\":\"s-0\",\"thread_name\":\"renamed %d\"}\n", i); e != nil {
						b.Fatal(e)
					}
					names, e := loadThreadNameIndexContext(context.Background(), root)
					if e != nil || names.size() != n {
						b.Fatal(e)
					}
				}
			})
		})
	}
}
func BenchmarkHotDiscover(b *testing.B) {
	for _, n := range []int{1000, 10000} {
		b.Run(fmt.Sprint(n), func(b *testing.B) {
			setBenchmarkUserCacheDir(b)
			root := os.Getenv(EnvCodexDir)
			dir := filepath.Join(root, "sessions", "2026", "11", "17")
			if e := os.MkdirAll(dir, 0700); e != nil {
				b.Fatal(e)
			}
			for i := 0; i < n; i++ {
				id := fmt.Sprintf("00000000-0000-4000-8000-%012d", i)
				path := filepath.Join(dir, "rollout-2026-11-17T00-00-00-"+id+".jsonl")
				body := fmt.Sprintf("{\"type\":\"session_meta\",\"payload\":{\"id\":\"%s\",\"cwd\":\"/project\",\"source\":\"cli\"}}\n{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"user\",\"content\":[{\"type\":\"input_text\",\"text\":\"task\"}]}}\n", id)
				if e := os.WriteFile(path, []byte(body), 0600); e != nil {
					b.Fatal(e)
				}
			}
			if projects, e := DiscoverProjectSummariesContext(context.Background(), root); e != nil || len(projects) != 1 || len(projects[0].Sessions) != n {
				b.Fatalf("warm: %d %v", len(projects), e)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if projects, e := DiscoverProjectSummariesContext(context.Background(), root); e != nil || len(projects) != 1 || len(projects[0].Sessions) != n {
					b.Fatal(e)
				}
			}
		})
	}
}

func BenchmarkHotPartialMetadata(b *testing.B) {
	for _, partial := range []bool{false, true} {
		name := "complete"
		if partial {
			name = "unchanged-partial-tail"
		}
		b.Run(name, func(b *testing.B) {
			setBenchmarkUserCacheDir(b)
			path := filepath.Join(os.Getenv(EnvCodexDir), "metadata.jsonl")
			body := "{\"type\":\"session_meta\",\"payload\":{\"id\":\"s\",\"cwd\":\"/project\"}}\n" + strings.Repeat("{\"type\":\"response_item\",\"payload\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"task\"}]}}\n", 10000)
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				b.Fatal(err)
			}
			if _, err := readSessionFileMetaCached(path); err != nil {
				b.Fatal(err)
			}
			if partial {
				f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
				if err != nil {
					b.Fatal(err)
				}
				_, err = f.WriteString("{\"type\":")
				f.Close()
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				meta, err := readSessionFileMetaCached(path)
				if err != nil || meta.MessageCount != 10000 {
					b.Fatalf("count=%d err=%v", meta.MessageCount, err)
				}
			}
		})
	}
}
func BenchmarkHotPreviewRetention(b *testing.B) {
	for _, count := range []int{8800, 60000} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			setBenchmarkUserCacheDir(b)
			path := filepath.Join(os.Getenv(EnvCodexDir), "large.jsonl")
			var body strings.Builder
			for i := 0; i < count; i++ {
				fmt.Fprintf(&body, "{\"type\":\"response_item\",\"payload\":{\"id\":\"m-%d\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"%s\"}]}}\n", i, strings.Repeat("answer ", 32))
			}
			if e := os.WriteFile(path, []byte(body.String()), 0600); e != nil {
				b.Fatal(e)
			}
			if _, e := ReadSessionPreviewText(path, 0, 0); e != nil {
				b.Fatal(e)
			}
			sessionPreviewSQLiteState.mu.Lock()
			entries := len(sessionPreviewSQLiteState.memory)
			bytes := sessionPreviewSQLiteState.memoryBytes
			sessionPreviewSQLiteState.mu.Unlock()
			b.Logf("visible=%d process-memory-entries=%d estimated-memory=%d", count, entries, bytes)

			b.Run("unchanged", func(b *testing.B) {
				if _, e := ReadSessionPreviewText(path, 0, 0); e != nil {
					b.Fatal(e)
				}
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					if text, e := ReadSessionPreviewText(path, 0, 0); e != nil || text == "" {
						b.Fatal(e)
					}
				}
			})
			clock := time.Unix(1000000, 0)
			for _, visible := range []bool{false, true} {
				name := "append-hidden"
				if visible {
					name = "append-visible"
				}
				b.Run(name, func(b *testing.B) {
					oldNow := sessionPreviewNow
					sessionPreviewNow = func() time.Time { return clock }
					b.Cleanup(func() { sessionPreviewNow = oldNow })
					f, e := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
					if e != nil {
						b.Fatal(e)
					}
					defer f.Close()
					seed, err := f.Stat()
					if err != nil {
						b.Fatal(err)
					}
					warm, err := ReadSessionPreviewText(path, 0, 0)
					if err != nil {
						b.Fatal(err)
					}
					previousLen := len(warm)
					commits := 0
					oldHook := sessionPreviewSQLiteCommitHook
					sessionPreviewSQLiteCommitHook = func(string) { commits++ }
					b.Cleanup(func() { sessionPreviewSQLiteCommitHook = oldHook })
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						clock = clock.Add(time.Second)
						line := "{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call_output\",\"output\":\"hidden\"}}\n"
						if visible {
							line = fmt.Sprintf("{\"type\":\"response_item\",\"payload\":{\"id\":\"append-%d-%d\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"new answer\"}]}}\n", seed.Size(), i)
						}
						if _, e := f.WriteString(line); e != nil {
							b.Fatal(e)
						}
						text, e := ReadSessionPreviewText(path, 0, 0)
						if e != nil || len(text) < count*200 {
							b.Fatalf("text=%d err=%v", len(text), e)
						}
						if visible && len(text) <= previousLen {
							b.Fatal("visible benchmark reused an existing source ID")
						}
						previousLen = len(text)
					}
					b.StopTimer()
					b.ReportMetric(float64(commits)/float64(b.N), "commits/op")
					sessionPreviewSQLiteState.mu.Lock()
					entries := len(sessionPreviewSQLiteState.memory)
					bytes := sessionPreviewSQLiteState.memoryBytes
					sessionPreviewSQLiteState.mu.Unlock()
					b.Logf("post-append memory-entries=%d estimated-memory=%d", entries, bytes)
				})
			}
		})
	}
}
