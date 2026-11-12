package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	chteams "github.com/baaaaaaaka/codex-helper/internal/channel/teams"
	"github.com/spf13/cobra"
)

// Read commands reuse configured networking without the lifecycle-capable
// lease: no starting/retiring daemons and no helper state acquisition.
func newTeamsReadHTTPClient(ctx context.Context, root *rootOptions) (*http.Client, error) {
	if seam, e := loadTeamsTestSeam(); e != nil {
		return nil, e
	} else if seam != nil {
		return seam.httpClient(), nil
	}
	status, e := loadTeamsGraphProxyLocalStatus(ctx, root)
	if e != nil {
		return nil, e
	}
	if !status.Enabled {
		c := newTeamsDirectHTTPClient()
		c.Timeout = 0
		return c, nil
	}
	if status.ProfileErr != nil {
		return nil, status.ProfileErr
	}
	if status.HealthyInstance == nil {
		return nil, errors.New("Teams Graph proxy is enabled but no reusable proxy is available; start the configured proxy separately")
	}
	c, e := newTeamsProxyHTTPClient(fmt.Sprintf("http://127.0.0.1:%d", status.HealthyInstance.HTTPPort))
	if e == nil {
		c.Timeout = 0
	}
	return c, e
}

var openTeamsReader = func(ctx context.Context, root *rootOptions) (*chteams.Reader, error) {
	cfg, e := chteams.ResolveReadAuthConfig("")
	if e != nil {
		return nil, e
	}
	hc, e := newTeamsReadHTTPClient(ctx, root)
	if e != nil {
		return nil, e
	}
	auth := chteams.NewDelegatedAuth(cfg, hc)
	r, e := chteams.NewReader(chteams.Config{HTTP: hc, Tokens: auth, TenantID: cfg.TenantID, SharePointHosts: chteams.SharePointHostsFromEnv(os.Getenv("CODEX_HELPER_TEAMS_ALLOWED_SHAREPOINT_HOSTS"))})
	if e != nil {
		auth.Close()
	}
	return r, e
}

func teamsReadFlags(cmd *cobra.Command, b *chteams.ReadBudget, jsonOut *bool) {
	*b = chteams.DefaultReadBudget()
	cmd.Flags().BoolVar(jsonOut, "json", false, "Print versioned JSON, including partial results and cursors")
	cmd.Flags().IntVar(&b.MaxRequests, "max-requests", b.MaxRequests, "Logical Graph request budget (batch items and retries count)")
	cmd.Flags().Int64Var(&b.MaxBytes, "max-bytes", b.MaxBytes, "Total response byte budget")
	cmd.Flags().IntVar(&b.MaxChars, "max-chars", b.MaxChars, "Total text Unicode character budget")
	cmd.Flags().IntVar(&b.MaxMessages, "max-messages", b.MaxMessages, "Total returned message/candidate budget (1..200)")
	cmd.Flags().DurationVar(&b.Timeout, "timeout", b.Timeout, "Overall timeout, including retries")
}
func teamsCursorFlags(cmd *cobra.Command, cursor string, names ...string) error {
	if cursor != "" {
		for _, name := range names {
			if cmd.Flags().Changed(name) {
				return fmt.Errorf("--cursor cannot be combined with --%s", name)
			}
		}
	}
	return nil
}
func newTeamsReadCmd(root *rootOptions) *cobra.Command {
	var o chteams.ReadOptions
	var jsonOut bool
	cmd := &cobra.Command{Use: "read [Teams URL|ref ...]", Short: "Read Teams resources with explicit history or reply ranges", Long: "Read existing Teams resources with the current read identity. Bare read returns the linked resource only. --before/--latest apply to chats; --replies applies to channel messages. Does not send messages or change helper state or service lifecycle.", Args: cobra.ArbitraryArgs, RunE: func(cmd *cobra.Command, args []string) error {
		if e := teamsCursorFlags(cmd, o.Cursor, "before", "latest", "replies", "limit"); e != nil {
			return e
		}
		if cmd.Flags().Changed("limit") && !o.Replies && o.Cursor == "" {
			return errors.New("--limit requires --replies")
		}
		if cmd.Flags().Changed("before") && o.Before == 0 || cmd.Flags().Changed("latest") && o.Latest == 0 {
			return errors.New("explicit range counts must be positive")
		}
		if e := chteams.ValidateRead(args, o, ""); e != nil {
			return e
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.Budget.Timeout)
		defer cancel()
		r, e := openTeamsReader(ctx, root)
		if e != nil {
			return e
		}
		defer r.Close()
		result, e := r.Read(ctx, args, o)
		if we := writeTeamsRead(cmd, jsonOut, result); we != nil {
			return we
		}
		return e
	}}
	cmd.Flags().IntVar(&o.Before, "before", 0, "Messages created strictly before the linked chat message (1..10000)")
	cmd.Flags().IntVar(&o.Latest, "latest", 0, "Most recently created chat messages (1..10000)")
	cmd.Flags().BoolVar(&o.Replies, "replies", false, "Read the channel root and its replies")
	cmd.Flags().IntVar(&o.Limit, "limit", 50, "Reply count; continue with cursors when a call reaches its budget")
	cmd.Flags().StringVar(&o.Cursor, "cursor", "", "Continue one previously requested range")
	teamsReadFlags(cmd, &o.Budget, &jsonOut)
	return cmd
}
func newTeamsSearchCmd(root *rootOptions) *cobra.Command {
	var o chteams.SearchOptions
	var jsonOut bool
	cmd := &cobra.Command{Use: "search [query]", Short: "Search visible Teams messages, then read selected candidate refs", Long: "Search the signed-in user's Teams index. Results contain summaries and candidate refs, not complete bodies. This is not a complete search of a specific chat; missing hits do not prove absence.", Args: cobra.MaximumNArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if e := teamsCursorFlags(cmd, o.Cursor, "limit"); e != nil {
			return e
		}
		query := ""
		if len(args) > 0 {
			query = args[0]
		}
		if e := chteams.ValidateSearch(query, o, ""); e != nil {
			return e
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.Budget.Timeout)
		defer cancel()
		r, e := openTeamsReader(ctx, root)
		if e != nil {
			return e
		}
		defer r.Close()
		result, e := r.Search(ctx, query, o)
		if we := writeTeamsRead(cmd, jsonOut, result); we != nil {
			return we
		}
		return e
	}}
	cmd.Flags().IntVar(&o.Limit, "limit", 20, "Requested candidate count (1..10000)")
	cmd.Flags().StringVar(&o.Cursor, "cursor", "", "Continue the original search query")
	teamsReadFlags(cmd, &o.Budget, &jsonOut)
	return cmd
}
func newTeamsFetchCmd(root *rootOptions) *cobra.Command {
	var o chteams.FetchOptions
	var jsonOut bool
	cmd := &cobra.Command{Use: "fetch <attachment-ref>", Short: "Download one attachment from a Teams read result", Long: "Download an explicitly selected attachment, revalidating its source. Follows file redirects without Graph credentials and refuses existing outputs. Interrupted downloads retain private staging for the same ref and version.", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if e := chteams.ValidateFetch(args[0], o, ""); e != nil {
			return e
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), o.Timeout)
		defer cancel()
		r, e := openTeamsReader(ctx, root)
		if e != nil {
			return e
		}
		defer r.Close()
		result, e := r.Fetch(ctx, args[0], o)
		if e != nil {
			return e
		}
		return writeTeamsRead(cmd, jsonOut, result)
	}}
	cmd.Flags().StringVar(&o.Output, "output", "", "Destination path; existing targets are never replaced")
	cmd.Flags().Int64Var(&o.MaxBytes, "max-bytes", 100<<20, "Maximum file bytes (up to 2GiB)")
	cmd.Flags().DurationVar(&o.Timeout, "timeout", 300*time.Second, "Overall download timeout (up to 10m)")
	cmd.Flags().BoolVar(&jsonOut, "json", false, "Print download metadata as JSON")
	return cmd
}
func writeTeamsRead(cmd *cobra.Command, jsonOut bool, v any) error {
	// Serialize once, after the bounded operation; no growing JSON rewrites.
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	if len(b) > 512<<10 {
		return errors.New("Teams output exceeds 512KiB; reduce --max-chars/--max-messages")
	}
	if jsonOut {
		_, e = fmt.Fprintln(cmd.OutOrStdout(), string(b))
		return e
	}
	var rendered bytes.Buffer
	out := &rendered
	switch result := v.(type) {
	case chteams.ReadResult:
		for _, resource := range result.Resources {
			fmt.Fprintf(out, "%s %s\n%s\nref: %s\n", resource.Target.Kind, resource.Name, resource.URL, resource.Ref)
		}
		for _, m := range result.Messages {
			author := ""
			if m.Author != nil {
				author = m.Author.DisplayName
			}
			fmt.Fprintf(out, "\n[%s] %s %s\n%s\n%s\nref: %s\n", m.Created, author, m.Type, m.Text, m.URL, m.Ref)
			for _, a := range m.Attachments {
				fmt.Fprintf(out, "attachment: %s %s (%s, %s)\n", a.Kind, a.Name, a.ContentType, a.State)
				if a.Error != "" {
					fmt.Fprintln(out, a.Error)
				}
				if a.Ref != "" {
					fmt.Fprintf(out, "  ref: %s\n", a.Ref)
				}
			}
			if m.Truncated {
				fmt.Fprintf(out, "body truncated; cursor: %s\n", m.NextCursor)
			}
		}
		for _, rg := range result.Ranges {
			fmt.Fprintf(out, "\nrange %s: %s (%d/%d messages, %s..%s; %s)\n", rg.Kind, rg.State, len(rg.Refs), rg.Requested, rg.Oldest, rg.Newest, rg.Order)
			for _, ref := range rg.Refs {
				fmt.Fprintf(out, "  %s\n", ref)
			}
			if rg.Error != "" {
				fmt.Fprintln(out, rg.Error)
			}
			if rg.NextCursor != "" {
				fmt.Fprintf(out, "cursor: %s\n", rg.NextCursor)
			}
		}
		fmt.Fprintf(out, "\nusage: %d requests, %d response bytes, %d text characters\n", result.Usage.Requests, result.Usage.Bytes, result.Usage.Chars)
	case chteams.SearchResult:
		fmt.Fprintf(out, "scope: %s; query: %s\n", result.Scope, result.Query)
		for _, hit := range result.Hits {
			fmt.Fprintf(out, "\n%s\nstate: %s\n", hit.Summary, hit.State)
			if hit.Reason != "" {
				fmt.Fprintln(out, hit.Reason)
			}
			if hit.SummaryTruncated {
				fmt.Fprintln(out, "summary truncated; read the candidate ref for the original message")
			}
			if hit.Ref != "" {
				fmt.Fprintf(out, "ref: %s\n%s\n", hit.Ref, hit.URL)
			}
		}
		fmt.Fprintf(out, "\nsearch: %s\n", result.State)
		if result.Error != "" {
			fmt.Fprintln(out, result.Error)
		}
		if result.NextCursor != "" {
			fmt.Fprintf(out, "cursor: %s\n", result.NextCursor)
		}
		fmt.Fprintf(out, "usage: %d requests, %d response bytes, %d text characters\n", result.Usage.Requests, result.Usage.Bytes, result.Usage.Chars)
	case chteams.FetchResult:
		fmt.Fprintf(out, "%s\n%d bytes (%s)\nsha256: %s\n", result.Path, result.Bytes, result.ContentType, result.SHA256)
	default:
		return errors.New("unsupported Teams output type")
	}
	if rendered.Len() > 512<<10 {
		return errors.New("Teams text output exceeds 512KiB; reduce --max-chars/--max-messages")
	}
	_, e = cmd.OutOrStdout().Write(rendered.Bytes())
	return e
}
