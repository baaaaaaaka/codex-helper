package codexhistory

import "testing"

func TestFlattenSessionGraphPreservesNestedImmediateParents(t *testing.T) {
	projects := []Project{{
		Path: "/workspace",
		Sessions: []Session{
			{
				SessionID: "root",
				Subagents: []SubagentSession{{
					SessionID:       "child",
					ParentSessionID: "root",
					AgentID:         "thread_spawn",
					FilePath:        "/sessions/child.jsonl",
				}},
			},
			{
				SessionID:       "grandchild",
				FilePath:        "/sessions/grandchild.jsonl",
				ParentSessionID: "child",
				AgentID:         "thread_spawn",
			},
		},
	}}

	got := FlattenSessionGraph(projects)
	if len(got) != 3 {
		t.Fatalf("graph node count = %d, want 3: %#v", len(got), got)
	}
	byID := make(map[string]SessionGraphNode, len(got))
	for _, node := range got {
		byID[node.SessionID] = node
	}
	if byID["child"].ParentSessionID != "root" || byID["grandchild"].ParentSessionID != "child" {
		t.Fatalf("immediate parents were not preserved: %#v", byID)
	}
	if byID["grandchild"].AsSubagentSession().AgentID != "thread_spawn" {
		t.Fatalf("grandchild conversion lost agent type: %#v", byID["grandchild"])
	}
}
