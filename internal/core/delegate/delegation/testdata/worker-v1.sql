-- Frozen pre-SQLite-only worker schema. Do not build this from current DDL.
CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
INSERT INTO meta VALUES('schema_version','1');
CREATE TABLE records(id TEXT PRIMARY KEY,json BLOB NOT NULL);
CREATE TABLE routes(id TEXT PRIMARY KEY,json BLOB NOT NULL);
CREATE TABLE remote_threads(id TEXT PRIMARY KEY,json BLOB NOT NULL);
CREATE TABLE executions(id TEXT PRIMARY KEY,json BLOB NOT NULL);
CREATE TABLE outbox(id TEXT PRIMARY KEY,json BLOB NOT NULL);
CREATE TABLE inbox_cursors(id TEXT PRIMARY KEY,json BLOB NOT NULL);
CREATE TABLE inbox_backoffs(id TEXT PRIMARY KEY,json BLOB NOT NULL);
INSERT INTO records VALUES('request-一','{"kind":"cxp.delegation.request.v1","record_id":"request-一","delegation_id":"task-一","source_key":"source","path":["second","first","second"],"claim_epoch":9007199254740993,"spec":{"title":"标题","objective":"目标\nline","constraints":["二","一","二"],"allowed_actions":["read","test"],"context":["context"],"artifact_refs":[{"kind":"file","name":"样例","path":"/synthetic/file","hash":"sha"}]},"thread_update":{"summary":"increment"},"created_at":"2026-11-10T03:00:00.000000001Z","expires_at":"2026-11-10T05:00:00Z"}');
INSERT INTO routes VALUES('task-一','{"delegation_id":"task-一","machine_id":"machine","inbox_ref":"inbox","inbox_generation":"generation"}');
INSERT INTO remote_threads VALUES('thread','{"thread_id":"thread","machine_id":"machine","state":"active","active_delegation_id":"task-一","generation":"old-generation","summary":"old summary"}');
INSERT INTO executions VALUES('task-一','{"delegation_id":"task-一","claim_id":"claim","claim_epoch":9007199254740993,"worker_instance_id":"worker","machine_id":"machine","status":"running","started_at":"2026-11-10T03:01:00Z"}');
INSERT INTO outbox VALUES('request-一','{"record_id":"request-一","delegation_id":"task-一","chat_id":"chat","status":"unknown","attempts":2,"message_id":"possible-message","error":"lost response","updated_at":"2026-11-10T03:02:00Z"}');
INSERT INTO inbox_cursors VALUES('chat','{"chat_id":"chat","last_head_message_id":"head","continuation_path":"page:next","updated_at":"2026-11-10T03:02:00Z"}');
INSERT INTO inbox_backoffs VALUES('chat','{"chat_id":"chat","blocked_until":"2026-11-10T03:05:00Z","reason":"429","updated_at":"2026-11-10T03:02:00Z"}');
