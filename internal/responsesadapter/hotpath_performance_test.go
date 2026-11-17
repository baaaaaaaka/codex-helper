package responsesadapter

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

// Audit control: retain the same channel/select loop and only change how text
// is accumulated. This is not a production implementation or an end-to-end test.
func hotBuilderCollect(ctx context.Context, stream <-chan ProviderEvent) string {
	var text strings.Builder
	for {
		select {
		case <-ctx.Done():
			return ""
		case event, ok := <-stream:
			if !ok {
				return ""
			}
			switch event.Kind {
			case ProviderEventTextDelta:
				text.WriteString(event.Delta)
			case ProviderEventDone:
				return text.String()
			}
		}
	}
}

func BenchmarkHotTextAccumulator(b *testing.B) {
	for _, count := range []int{1000, 10000} {
		for _, builder := range []bool{false, true} {
			name := "current"
			if builder {
				name = "builder-control"
			}
			b.Run(fmt.Sprintf("%d/%s", count, name), func(b *testing.B) {
				stream := make(chan ProviderEvent, count+1)
				f := &Facade{}
				ctx := context.Background()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for j := 0; j < count; j++ {
						stream <- ProviderEvent{Kind: ProviderEventTextDelta, Delta: "abcdefgh"}
					}
					stream <- ProviderEvent{Kind: ProviderEventDone}
					var text string
					if builder {
						text = hotBuilderCollect(ctx, stream)
					} else {
						result, ok := f.collectProviderResult(ctx, stream, nil)
						if !ok {
							b.Fatal("collect failed")
						}
						text = result.text
					}
					if len(text) != count*8 {
						b.Fatal(len(text))
					}
				}
			})
		}
	}
}
