package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"
)

// TestProbeSQLiteHotPath is an opt-in diagnostic for a copied production
// fixture. It is deliberately skipped in normal test runs; it exists while
// investigating a real-data startup/phase timeout and can be removed after
// the timings are recorded.
func TestProbeSQLiteHotPath(t *testing.T) {
	path := os.Getenv("CXP_TEAMS_PROBE_STORE_PATH")
	if path == "" {
		t.Skip("set CXP_TEAMS_PROBE_STORE_PATH to probe a copied SQLite store")
	}
	st, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	now := time.Now()
	measure := func(name string, fn func() error) {
		started := time.Now()
		err := fn()
		t.Logf("%s elapsed=%s err=%v", name, time.Since(started), err)
		if err != nil {
			t.Fatal(err)
		}
	}
	if os.Getenv("CXP_TEAMS_PROBE_PREPARE_SCHEMA") == "1" {
		measure("schema-preparation", func() error {
			return st.PrepareSQLiteSchemaBeforeOwner(ctx)
		})
	}
	if os.Getenv("CXP_TEAMS_PROBE_FORCE_OUTBOX_AUDIT") == "1" {
		measure("outbox-projection-prepare", func() error {
			return st.PrepareOutboxProjection(ctx)
		})
		measure("outbox-projection-explicit-audit", func() error {
			return st.RetryDeferredOutboxProjectionAudit(ctx)
		})
	}
	if sessionID := os.Getenv("CXP_TEAMS_PROBE_TRANSCRIPT_SESSION"); sessionID != "" {
		var timings []StoreTimingEvent
		st.SetTimingObserver(func(event StoreTimingEvent) {
			// Keep the callback free of testing output. It runs while the Store
			// mutex may be held, and logging here would distort the measurement.
			timings = append(timings, event)
		})
		measure("session-transcript-dedupe", func() error {
			_, err := st.SessionTranscriptDedupeSnapshot(ctx, sessionID, os.Getenv("CXP_TEAMS_PROBE_TRANSCRIPT_CHECKPOINT"))
			return err
		})
		st.SetTimingObserver(nil)
		for _, event := range timings {
			t.Logf("session-transcript-dedupe timing operation=%s stage=%s elapsed=%s err=%v", event.Operation, event.Stage, event.Duration, event.Err)
		}
		measure("session-outbox-scalar-indexed-candidate", func() error {
			return st.withStateLock(ctx, func() error {
				pointer, ok, err := st.currentSQLitePointerUnlocked()
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("sqlite pointer missing")
				}
				db, err := st.sqliteDBUnlocked(pointer)
				if err != nil {
					return err
				}
				messages := make(map[string]OutboxMessage)
				err = loadSQLiteOutboxMap(ctx, db, `SELECT `+sqliteOutboxProjectionSelect("o")+` FROM outbox_messages o WHERE o.session_id = ?`, messages, func(v OutboxMessage) string { return v.ID }, sessionID)
				t.Logf("session-outbox-scalar-indexed-candidate rows=%d", len(messages))
				return err
			})
		})
		if os.Getenv("CXP_TEAMS_PROBE_TRANSCRIPT_ONLY") == "1" {
			return
		}
	}
	var pendingStageEvents []string
	if os.Getenv("CXP_TEAMS_PROBE_DETAIL") == "1" {
		var nativeReady bool
		var nativeMarkerErr error
		var nativeDBPath string
		var nativeMarker, nativeProvenance, nativeDatabaseID string
		var nativeGeneration, nativeSchemaVersion int64
		var nativePhysicalRevision string
		nativeProbeErr := st.withStateLock(ctx, func() error {
			pointer, ok, err := st.currentSQLitePointerUnlocked()
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("sqlite pointer missing")
			}
			db, err := st.sqliteDBUnlocked(pointer)
			if err != nil {
				return err
			}
			nativeDBPath = st.sqliteDBPath
			nativeMarker, err = sqliteReadMetaValueContext(ctx, db, sqliteOutboxProjectionTrustKey)
			if err != nil {
				return err
			}
			nativeProvenance, err = sqliteReadMetaValueContext(ctx, db, sqliteOutboxProjectionProvenanceKey)
			if err != nil {
				return err
			}
			nativeDatabaseID, err = sqliteReadMetaValueContext(ctx, db, sqliteOutboxDatabaseIdentityKey)
			if err != nil {
				return err
			}
			nativeGeneration, err = sqliteReadOutboxGenerationContext(ctx, db)
			if err != nil {
				return err
			}
			nativeSchemaVersion, err = sqliteReadSchemaVersionContext(ctx, db)
			if err != nil {
				return err
			}
			physicalIdentity, err := sqliteReadOnlyFileIdentityForPath(nativeDBPath)
			if err != nil {
				return err
			}
			nativePhysicalRevision = physicalIdentity.Revision
			nativeReady, nativeMarkerErr = st.sqliteOutboxProjectionNativeMarkerReady(ctx, db, sqliteOutboxProjectionTrustKey)
			return nil
		})
		if nativeProbeErr != nil {
			t.Fatalf("probe native outbox capability: %v", nativeProbeErr)
		}
		t.Logf("detail outbox-native-ready=%t marker-err=%v db-path=%q marker=%q database-id=%q generation=%d schema-version=%d physical-revision=%q provenance=%s", nativeReady, nativeMarkerErr, nativeDBPath, nativeMarker, nativeDatabaseID, nativeGeneration, nativeSchemaVersion, nativePhysicalRevision, nativeProvenance)
	}
	if os.Getenv("CXP_TEAMS_PROBE_DETAIL") == "1" {
		stageStarted := time.Now()
		previousStageHook := sqliteOutboxPendingChatAdmissionStageTestHook
		sqliteOutboxPendingChatAdmissionStageTestHook = func(stage string) {
			pendingStageEvents = append(pendingStageEvents, fmt.Sprintf("%s +%s", stage, time.Since(stageStarted)))
		}
		t.Cleanup(func() { sqliteOutboxPendingChatAdmissionStageTestHook = previousStageHook })
	}
	if os.Getenv("CXP_TEAMS_PROBE_SKIP_PENDING_CHAT") == "1" {
		t.Log("pending-chat-ids skipped by diagnostic flag")
	} else if os.Getenv("CXP_TEAMS_PROBE_DETAIL") != "1" {
		measure("pending-chat-ids", func() error {
			_, err := st.PendingOutboxChatIDsAt(ctx, PendingOutboxQuery{Now: now}, 256)
			return err
		})
	} else {
		started := time.Now()
		_, pendingErr := st.PendingOutboxChatIDsAt(ctx, PendingOutboxQuery{Now: now}, 256)
		t.Logf("pending-chat-ids elapsed=%s err=%v", time.Since(started), pendingErr)
		for _, event := range pendingStageEvents {
			t.Logf("pending-chat-ids stage=%s", event)
		}
		if pendingErr != nil {
			t.Fatal(pendingErr)
		}
	}
	if os.Getenv("CXP_TEAMS_PROBE_DETAIL") == "1" {
		// This is an opt-in read-only breakdown of the two hot admission APIs.
		// Keep each substep on the same Store lock boundary as production, but do
		// not put the timing observer in the callback: the observer itself would
		// add work while the lock is held and distort the real-data result.
		withDB := func(fn func(*sql.DB) error) error {
			return st.withStateLock(ctx, func() error {
				pointer, ok, err := st.currentSQLitePointerUnlocked()
				if err != nil {
					return err
				}
				if !ok {
					return fmt.Errorf("sqlite pointer missing")
				}
				db, err := st.sqliteDBUnlocked(pointer)
				if err != nil {
					return err
				}
				return fn(db)
			})
		}
		var readyIDs []string
		measure("detail-ready-admission", func() error {
			err := withDB(func(db *sql.DB) error {
				var err error
				readyIDs, _, err = loadSQLiteHotPollReadyChatIDsForHotPath(ctx, db, os.Getenv("CXP_TEAMS_PROBE_CONTROL_CHAT"), now, sqliteHotPollReadyLimit)
				return err
			})
			t.Logf("detail-ready-admission ids=%d", len(readyIDs))
			return err
		})
		measure("detail-ready-base-state", func() error {
			return withDB(func(db *sql.DB) error {
				_, err := loadSQLiteHotPollBaseState(ctx, db)
				return err
			})
		})

		var selected State
		var selectedSessionIDs []string
		measure("detail-ready-selected-chat-polls", func() error {
			return withDB(func(db *sql.DB) error {
				chatPollQuery, chatPollArgs := sqliteChatPollSelectionQuery(readyIDs)
				var err error
				selected, err = loadSQLiteHotPollSelectedStateWithChatPollQuery(ctx, db, hotPollScheduleBaseFields, chatPollQuery, chatPollArgs...)
				t.Logf("detail-ready-selected-chat-polls chats=%d", len(selected.ChatPolls))
				return err
			})
		})
		measure("detail-ready-selected-session-ids", func() error {
			return withDB(func(db *sql.DB) error {
				var err error
				selectedSessionIDs, err = loadSQLiteSelectedSessionIDsForChats(ctx, db, readyIDs, nil)
				t.Logf("detail-ready-selected-session-ids sessions=%d", len(selectedSessionIDs))
				return err
			})
		})
		measure("detail-ready-active-turns", func() error {
			return withDB(func(db *sql.DB) error {
				return loadSQLiteHotPollActiveTurnsForIDs(ctx, db, selected, readyIDs, selectedSessionIDs)
			})
		})
		measure("detail-ready-import-checkpoints", func() error {
			return withDB(func(db *sql.DB) error {
				return loadSQLiteHotPollImportingCheckpointsForIDs(ctx, db, selected, readyIDs, selectedSessionIDs)
			})
		})

		measure("detail-work-markers-and-scalar-admission", func() error {
			return withDB(func(db *sql.DB) error {
				versionsCurrent, err := sqliteHotPollAdmissionVersionsCurrent(ctx, db, true)
				if err != nil {
					return err
				}
				rowFallback, err := sqliteHotPollAdmissionNeedsFallbackWithVersions(ctx, db, true, versionsCurrent)
				if err != nil {
					return err
				}
				t.Logf("detail-work-markers versions-current=%t row-fallback=%t", versionsCurrent, rowFallback)
				for _, table := range []string{"chat_polls", "sessions"} {
					predicate := sqliteChatPollAdmissionUntrustedSQL("")
					if table == "sessions" {
						predicate = "(" + sqliteProjectionUntrustedSQL("") + `)
  AND (trim(COALESCE(teams_chat_id, '')) != '' OR ` + sqliteCanonicalTextProjectionSQL("json", "$.teams_chat_id", "teams_chat_id") + ` != '')`
					}
					var one int
					probeErr := db.QueryRowContext(ctx, `SELECT 1 FROM `+table+` WHERE `+predicate+` LIMIT 1`).Scan(&one)
					t.Logf("detail-work-marker-table table=%s row=%d err=%v", table, one, probeErr)
				}
				routeExpr := `(trim(COALESCE(teams_chat_id, '')) != '' OR ` + sqliteCanonicalTextProjectionSQL("json", "$.teams_chat_id", "teams_chat_id") + ` != '')`
				untrustedExpr := sqliteProjectionUntrustedSQL("")
				rows, probeErr := db.QueryContext(ctx, `SELECT id, (`+untrustedExpr+`), `+routeExpr+`, ((`+untrustedExpr+`) AND (`+routeExpr+`)) FROM sessions WHERE `+untrustedExpr+` ORDER BY id`)
				if probeErr != nil {
					return probeErr
				}
				for rows.Next() {
					var id string
					var untrusted, routable, final int
					if probeErr := rows.Scan(&id, &untrusted, &routable, &final); probeErr != nil {
						_ = rows.Close()
						return probeErr
					}
					t.Logf("detail-work-untrusted-eval id=%q untrusted=%d routable=%d final=%d", id, untrusted, routable, final)
				}
				if probeErr := rows.Err(); probeErr != nil {
					_ = rows.Close()
					return probeErr
				}
				if probeErr := rows.Close(); probeErr != nil {
					return probeErr
				}
				rows, probeErr = db.QueryContext(ctx, `SELECT id, trim(COALESCE(teams_chat_id, '')), `+
					`json_valid(json), `+sqliteCanonicalTextProjectionSQL("json", "$.teams_chat_id", "teams_chat_id")+
					` FROM sessions WHERE `+sqliteProjectionUntrustedSQL("")+
					` ORDER BY id`)
				if probeErr != nil {
					return probeErr
				}
				for rows.Next() {
					var id, scalarChat, canonicalChat string
					var valid int
					if probeErr := rows.Scan(&id, &scalarChat, &valid, &canonicalChat); probeErr != nil {
						_ = rows.Close()
						return probeErr
					}
					t.Logf("detail-work-untrusted-session id=%q scalar-chat=%q json-valid=%d canonical-chat=%q", id, scalarChat, valid, canonicalChat)
				}
				if probeErr := rows.Err(); probeErr != nil {
					_ = rows.Close()
					return probeErr
				}
				if probeErr := rows.Close(); probeErr != nil {
					return probeErr
				}
				hasSessions, err := loadSQLiteHasSessions(ctx, db)
				if err != nil {
					return err
				}
				if !hasSessions {
					return nil
				}
				candidates, fallback, err := loadSQLiteHotPollWorkCandidatesTrustedAdmission(ctx, db, os.Getenv("CXP_TEAMS_PROBE_CONTROL_CHAT"), now.Add(-30*24*time.Hour), now, sqliteHotPollReadyLimit, true, false)
				t.Logf("detail-work-admission candidates=%d fallback=%v", len(candidates), fallback)
				return err
			})
		})
		measure("detail-pending-page-native", func() error {
			return withDB(func(db *sql.DB) error {
				chatID := os.Getenv("CXP_TEAMS_PROBE_CONTROL_CHAT")
				planRows, planErr := db.QueryContext(ctx, `EXPLAIN QUERY PLAN SELECT `+sqliteOutboxProjectionSelect("o")+
					` FROM outbox_messages o WHERE o.status IN ('queued', 'sending', 'accepted')
  AND (o.status = 'accepted' OR COALESCE((SELECT `+sqliteStoredInt64SQL("blocked_until")+` FROM chat_rate_limits WHERE chat_id = ?), 0) = 0
       OR COALESCE((SELECT `+sqliteStoredInt64SQL("blocked_until")+` FROM chat_rate_limits WHERE chat_id = ?), 0) <= ?)
  AND o.teams_chat_id = ? AND o.id IS NOT NULL AND trim(o.id) <> ''
ORDER BY o.created_at, o.id LIMIT ?`, GraphWriteAccountRateLimitKey, GraphWriteAccountRateLimitKey, now.UnixNano(), chatID, 65)
				if planErr != nil {
					return planErr
				}
				var plan []string
				for planRows.Next() {
					var selectID, parentID, unused int
					var detail string
					if planErr := planRows.Scan(&selectID, &parentID, &unused, &detail); planErr != nil {
						_ = planRows.Close()
						return planErr
					}
					plan = append(plan, detail)
				}
				if planErr := planRows.Err(); planErr != nil {
					_ = planRows.Close()
					return planErr
				}
				if planErr := planRows.Close(); planErr != nil {
					return planErr
				}
				t.Logf("detail-pending-page-native-plan=%s", plan)
				page, err := pendingOutboxPageAtSQLiteFast(ctx, db, PendingOutboxQuery{
					Now: now, TeamsChatID: chatID, Limit: 64,
				})
				t.Logf("detail-pending-page-native messages=%d more=%t err=%v", len(page.Messages), page.More, err)
				if err != nil && !errors.Is(err, errSQLiteOutboxProjectionFallback) {
					return err
				}
				return nil
			})
		})
	}
	if os.Getenv("CXP_TEAMS_PROBE_QUEUED_TURNS") == "1" {
		// This is an opt-in read-only measurement for a copied production
		// database. The listener's normal path now asks for this bounded page
		// before hydrating selected sessions; keep the two calls separate so the
		// probe distinguishes the durable existence check from candidate paging.
		var hasQueued bool
		var nativeHasQueued bool
		var nativeHandled bool
		var nativeHasErr error
		measure("queued-turn-native-capability", func() error {
			nativeHasQueued, nativeHandled, nativeHasErr = st.hasQueuedTurnsSQLite(ctx)
			t.Logf("queued-turn-native-capability result=%t handled=%t err=%v", nativeHasQueued, nativeHandled, nativeHasErr)
			return nil
		})
		measure("queued-turn-has-queued", func() error {
			var err error
			hasQueued, err = st.HasQueuedTurns(ctx)
			t.Logf("queued-turn-has-queued result=%t", hasQueued)
			return err
		})
		if hasQueued {
			measure("queued-turn-candidate-page", func() error {
				ids, more, err := st.QueuedTurnSessionIDs(ctx, "", 64)
				first, last := "", ""
				if len(ids) > 0 {
					first, last = ids[0], ids[len(ids)-1]
				}
				t.Logf("queued-turn-candidate-page ids=%d more=%t first=%q last=%q", len(ids), more, first, last)
				return err
			})
		}
	}
	if os.Getenv("CXP_TEAMS_PROBE_METADATA_ONLY") == "1" {
		return
	}
	measure("pending-page-control", func() error {
		_, err := st.PendingOutboxPageAt(ctx, PendingOutboxQuery{Now: now, TeamsChatID: os.Getenv("CXP_TEAMS_PROBE_CONTROL_CHAT"), Limit: 64})
		return err
	})
	predecessorChat := os.Getenv("CXP_TEAMS_PROBE_PREDECESSOR_CHAT")
	if predecessorChat == "" {
		predecessorChat = os.Getenv("CXP_TEAMS_PROBE_CONTROL_CHAT")
	}
	var predecessorTarget OutboxMessage
	measure("predecessor-page", func() error {
		page, err := st.PendingOutboxPageAt(ctx, PendingOutboxQuery{
			Now:         now,
			TeamsChatID: predecessorChat,
			Limit:       64,
		})
		if err != nil {
			return err
		}
		for _, message := range page.Messages {
			if message.Sequence > 1 && message.Status == OutboxStatusQueued {
				predecessorTarget = message
				break
			}
		}
		t.Logf("predecessor probe chat=%q target=%q sequence=%d status=%s", predecessorChat, predecessorTarget.ID, predecessorTarget.Sequence, predecessorTarget.Status)
		return nil
	})
	if predecessorTarget.ID != "" {
		measure("earlier-unsent", func() error {
			_, _, err := st.EarlierUnsentOutbox(ctx, predecessorTarget)
			return err
		})
		if os.Getenv("CXP_TEAMS_PROBE_SKIP_BATCH") != "1" {
			measure("earlier-unsent-all", func() error {
				_, err := st.EarlierUnsentOutboxes(ctx, predecessorTarget)
				return err
			})
		}
	}
	measure("hot-ready-schedule", func() error {
		_, err := st.HotPollReadyScheduleState(ctx, os.Getenv("CXP_TEAMS_PROBE_CONTROL_CHAT"), now)
		return err
	})
	measure("hot-candidates", func() error {
		_, _, err := st.HotPollWorkCandidatesExcludingIdleAt(ctx, os.Getenv("CXP_TEAMS_PROBE_CONTROL_CHAT"), now.Add(-30*24*time.Hour), now)
		return err
	})
	measure("fork-polling", func() error {
		_, err := st.ForkPollingSnapshot(ctx)
		return err
	})
}
