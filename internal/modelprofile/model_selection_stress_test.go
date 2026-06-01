package modelprofile

import (
	"fmt"
	"sync"
	"testing"
)

func TestProviderModelSelectionStressCI(t *testing.T) {
	deepseek, err := MustLookupProvider("deepseek")
	if err != nil {
		t.Fatalf("lookup deepseek: %v", err)
	}
	mimo, err := MustLookupProvider("mimo")
	if err != nil {
		t.Fatalf("lookup mimo: %v", err)
	}
	cases := []struct {
		provider ProviderSpec
		ref      string
		want     string
	}{
		{provider: deepseek, ref: "", want: "deepseek/deepseek-v4-flash"},
		{provider: deepseek, ref: "flash", want: "deepseek/deepseek-v4-flash"},
		{provider: deepseek, ref: "deepseek-v4-flash", want: "deepseek/deepseek-v4-flash"},
		{provider: deepseek, ref: "deepseek/deepseek-v4-flash", want: "deepseek/deepseek-v4-flash"},
		{provider: deepseek, ref: "pro", want: "deepseek/deepseek-v4-pro"},
		{provider: deepseek, ref: "deepseek-v4-pro", want: "deepseek/deepseek-v4-pro"},
		{provider: deepseek, ref: "deepseek/deepseek-v4-pro", want: "deepseek/deepseek-v4-pro"},
		{provider: mimo, ref: "", want: "mimo/mimo-v2.5"},
		{provider: mimo, ref: "base", want: "mimo/mimo-v2.5"},
		{provider: mimo, ref: "standard", want: "mimo/mimo-v2.5"},
		{provider: mimo, ref: "mimo25", want: "mimo/mimo-v2.5"},
		{provider: mimo, ref: "mimo-v2.5", want: "mimo/mimo-v2.5"},
		{provider: mimo, ref: "pro", want: "mimo/mimo-v2.5-pro"},
		{provider: mimo, ref: "mimo25-pro", want: "mimo/mimo-v2.5-pro"},
		{provider: mimo, ref: "mimo/mimo-v2.5-pro", want: "mimo/mimo-v2.5-pro"},
	}

	const workers = 48
	const iterations = 120
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for iter := 0; iter < iterations; iter++ {
				tc := cases[(worker+iter)%len(cases)]
				got, ok := tc.provider.ResolveModel(tc.ref)
				if !ok || got.PublicID() != tc.want {
					errCh <- fmt.Errorf("worker=%d iter=%d provider=%s ref=%q got=%q ok=%v want=%q", worker, iter, tc.provider.ID, tc.ref, got.PublicID(), ok, tc.want)
					return
				}
				if _, err := tc.provider.MustResolveModel(tc.ref); err != nil {
					errCh <- fmt.Errorf("worker=%d iter=%d MustResolveModel(%q): %w", worker, iter, tc.ref, err)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}
