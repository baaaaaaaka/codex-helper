package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSessionBindingChangesFencePollAttemptAcrossBackends(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		backend := "json"
		if useSQLite {
			backend = "sqlite"
		}
		t.Run(backend, func(t *testing.T) {
			for _, mutation := range []struct {
				name string
				fn   func(SessionContext) SessionContext
			}{
				{
					name: "rebind",
					fn: func(current SessionContext) SessionContext {
						current.TeamsChatID = "new-binding-chat"
						return current
					},
				},
				{
					name: "close",
					fn: func(current SessionContext) SessionContext {
						current.Status = SessionStatusClosed
						return current
					},
				},
			} {
				t.Run(mutation.name, func(t *testing.T) {
					ctx := context.Background()
					store := newTestStore(t)
					if err := store.Update(ctx, func(state *State) error {
						state.Sessions["binding-session"] = SessionContext{
							ID: "binding-session", Status: SessionStatusActive, TeamsChatID: "old-binding-chat",
						}
						state.ChatPolls["old-binding-chat"] = ChatPollState{
							ChatID: "old-binding-chat", Seeded: true, PollState: chatPollStateWarm,
						}
						return nil
					}); err != nil {
						t.Fatalf("seed state: %v", err)
					}
					if useSQLite {
						migrateStoreToSQLiteForTest(t, store)
					}

					now := time.Now().UTC()
					started, acquired, err := store.BeginChatPollAttempt(ctx, ChatPollAttemptRequest{
						ChatID: "old-binding-chat", Owner: "old-owner", ProcessIncarnation: "old-process",
						ExpectedFrontier: "head:/chats/old-binding-chat/messages?$top=20", Now: now, TTL: time.Minute,
					})
					if err != nil || !acquired || started.Attempt == nil {
						t.Fatalf("begin old attempt: acquired=%v attempt=%#v err=%v", acquired, started.Attempt, err)
					}
					oldCapability := ChatPollAttemptCapability{
						ID: started.Attempt.ID, Owner: started.Attempt.Owner,
						ProcessIncarnation: started.Attempt.ProcessIncarnation,
						LeaseGeneration:    started.Attempt.LeaseGeneration,
					}

					if _, changed, err := store.UpdateSessionContext(ctx, "binding-session", func(current SessionContext, found bool, _ time.Time) (SessionContext, bool, error) {
						if !found {
							t.Fatalf("session not found in context callback")
						}
						return mutation.fn(current), true, nil
					}); err != nil || !changed {
						t.Fatalf("apply %s: changed=%v err=%v", mutation.name, changed, err)
					}

					fenced, ok, err := store.ChatPoll(ctx, "old-binding-chat")
					if err != nil || !ok || fenced.Attempt != nil || fenced.PollRevision <= started.PollRevision {
						t.Fatalf("%s did not fence old poll: ok=%v poll=%#v err=%v", mutation.name, ok, fenced, err)
					}
					if _, committed, err := store.CommitChatPollAttemptWithCapability(ctx, "old-binding-chat", oldCapability, started.PollRevision, func(poll *ChatPollState) error {
						poll.LastError = "stale binding callback must not commit"
						return nil
					}); err != nil || committed {
						t.Fatalf("stale %s callback committed: committed=%v err=%v", mutation.name, committed, err)
					}
				})
			}
		})
	}
}

func TestSessionMetadataMutationDoesNotFenceUnrelatedPollAttempt(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		backend := "json"
		if useSQLite {
			backend = "sqlite"
		}
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["metadata-session"] = SessionContext{
					ID: "metadata-session", Status: SessionStatusActive, TeamsChatID: "metadata-chat",
				}
				state.ChatPolls["metadata-chat"] = ChatPollState{
					ChatID: "metadata-chat", Seeded: true, PollState: chatPollStateWarm,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed state: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			started, acquired, err := store.BeginChatPollAttempt(ctx, ChatPollAttemptRequest{
				ChatID: "metadata-chat", Owner: "metadata-owner", ProcessIncarnation: "metadata-process",
				ExpectedFrontier: "head:/chats/metadata-chat/messages?$top=20", Now: time.Now().UTC(), TTL: time.Minute,
			})
			if err != nil || !acquired || started.Attempt == nil {
				t.Fatalf("begin attempt: acquired=%v attempt=%#v err=%v", acquired, started.Attempt, err)
			}
			if _, changed, err := store.UpdateSessionContext(ctx, "metadata-session", func(current SessionContext, _ bool, _ time.Time) (SessionContext, bool, error) {
				current.UserTitle = "metadata-only"
				return current, true, nil
			}); err != nil || !changed {
				t.Fatalf("metadata update: changed=%v err=%v", changed, err)
			}
			capability := ChatPollAttemptCapability{
				ID: started.Attempt.ID, Owner: started.Attempt.Owner,
				ProcessIncarnation: started.Attempt.ProcessIncarnation,
				LeaseGeneration:    started.Attempt.LeaseGeneration,
			}
			if _, committed, err := store.CommitChatPollAttemptWithCapability(ctx, "metadata-chat", capability, started.PollRevision, func(poll *ChatPollState) error {
				poll.LastError = "metadata callback"
				return nil
			}); err != nil || !committed {
				t.Fatalf("metadata-only update unnecessarily fenced attempt: committed=%v err=%v", committed, err)
			}
		})
	}
}

func TestUpdateSessionBindingChangesFencePollAttemptAcrossBackends(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		backend := "json"
		if useSQLite {
			backend = "sqlite"
		}
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			if err := store.Update(ctx, func(state *State) error {
				state.Sessions["generic-binding-session"] = SessionContext{
					ID: "generic-binding-session", Status: SessionStatusActive, TeamsChatID: "generic-old-chat",
				}
				state.ChatPolls["generic-old-chat"] = ChatPollState{
					ChatID: "generic-old-chat", Seeded: true, PollState: chatPollStateWarm,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed state: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			started, acquired, err := store.BeginChatPollAttempt(ctx, ChatPollAttemptRequest{
				ChatID: "generic-old-chat", Owner: "generic-old-owner", ProcessIncarnation: "generic-old-process",
				ExpectedFrontier: "head:/chats/generic-old-chat/messages?$top=20", Now: time.Now().UTC(), TTL: time.Minute,
			})
			if err != nil || !acquired || started.Attempt == nil {
				t.Fatalf("begin attempt: acquired=%v attempt=%#v err=%v", acquired, started.Attempt, err)
			}
			if err := store.UpdateSession(ctx, "generic-binding-session", func(state *State) error {
				current := state.Sessions["generic-binding-session"]
				current.TeamsChatID = "generic-new-chat"
				state.Sessions[current.ID] = current
				return nil
			}); err != nil {
				t.Fatalf("generic rebind: %v", err)
			}
			fenced, ok, err := store.ChatPoll(ctx, "generic-old-chat")
			if err != nil || !ok || fenced.Attempt != nil || fenced.PollRevision <= started.PollRevision {
				t.Fatalf("generic update did not fence old poll: ok=%v poll=%#v err=%v", ok, fenced, err)
			}
			capability := ChatPollAttemptCapability{
				ID: started.Attempt.ID, Owner: started.Attempt.Owner,
				ProcessIncarnation: started.Attempt.ProcessIncarnation,
				LeaseGeneration:    started.Attempt.LeaseGeneration,
			}
			if _, committed, err := store.CommitChatPollAttemptWithCapability(ctx, "generic-old-chat", capability, started.PollRevision, func(poll *ChatPollState) error {
				poll.LastError = "stale generic binding callback"
				return nil
			}); err != nil || committed {
				t.Fatalf("stale generic binding callback committed: committed=%v err=%v", committed, err)
			}
		})
	}
}

func TestCompleteTurnWithFinalRejectsStaleSessionBindingAcrossBackends(t *testing.T) {
	for _, useSQLite := range []bool{false, true} {
		backend := "json"
		if useSQLite {
			backend = "sqlite"
		}
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			store := newTestStore(t)
			const sessionID = "completion-binding-session"
			const turnID = "completion-binding-turn"
			if err := store.Update(ctx, func(state *State) error {
				now := time.Now().UTC()
				state.Sessions[sessionID] = SessionContext{
					ID: sessionID, Status: SessionStatusActive, TeamsChatID: "completion-old-chat",
				}
				state.Turns[turnID] = Turn{
					ID: turnID, SessionID: sessionID, Status: TurnStatusRunning,
					CodexThreadID: "completion-thread", CodexTurnID: "completion-codex",
					StartedAt: now, CreatedAt: now, UpdatedAt: now,
				}
				return nil
			}); err != nil {
				t.Fatalf("seed state: %v", err)
			}
			if useSQLite {
				migrateStoreToSQLiteForTest(t, store)
			}
			if _, changed, err := store.UpdateSessionContext(ctx, sessionID, func(current SessionContext, found bool, _ time.Time) (SessionContext, bool, error) {
				if !found {
					t.Fatalf("session missing before rebind")
				}
				current.TeamsChatID = "completion-new-chat"
				return current, true, nil
			}); err != nil || !changed {
				t.Fatalf("rebind session: changed=%v err=%v", changed, err)
			}

			final := OutboxMessage{
				ID: "outbox:" + turnID + ":final", SessionID: sessionID, TurnID: turnID,
				TeamsChatID: "completion-old-chat", Kind: "final", NotificationKind: "turn_completed",
				Body: "stale final", SourceTextHash: "stale-final-hash", PartIndex: 1, PartCount: 1,
			}
			_, err := store.CompleteTurnWithFinal(ctx, CompleteTurnWithFinalRequest{
				SessionID: sessionID, TurnID: turnID, ExpectedTeamsChatID: "completion-old-chat",
				CodexThreadID: "completion-thread", CodexTurnID: "completion-codex",
				Progress:    TranscriptCheckpointProgress{ID: sessionTranscriptCheckpointID(sessionID), SessionID: sessionID},
				FinalOutbox: []OutboxMessage{final},
			})
			if !errors.Is(err, ErrCompletionOwnerLost) {
				t.Fatalf("stale completion error=%v, want ErrCompletionOwnerLost", err)
			}
			turn, found, err := store.TurnByID(ctx, turnID)
			if err != nil || !found {
				t.Fatalf("read turn after rejected completion: found=%v err=%v", found, err)
			}
			if turn.Status != TurnStatusRunning {
				t.Fatalf("stale completion changed turn status to %q", turn.Status)
			}
			if _, err := store.OutboxMessageByID(ctx, final.ID); err == nil {
				t.Fatalf("stale completion created old-chat final outbox")
			}
		})
	}
}
