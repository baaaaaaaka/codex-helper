package codexhistory

import "context"

// DiscoverSessionGraph returns the relationship-preserving flat view of Codex
// history. Existing callers that need the legacy tree-shaped result should
// continue using DiscoverProjects.
func DiscoverSessionGraph(codexDir string) ([]SessionGraphNode, error) {
	return DiscoverSessionGraphContext(context.Background(), codexDir)
}

// DiscoverSessionGraphContext is the context-aware form of
// DiscoverSessionGraph. On a partial discovery error it returns the flat view
// of the usable records together with the original error, matching
// DiscoverProjectsContext's partial-result contract.
func DiscoverSessionGraphContext(ctx context.Context, codexDir string) ([]SessionGraphNode, error) {
	projects, err := DiscoverProjectsContext(ctx, codexDir)
	return FlattenSessionGraph(projects), err
}
