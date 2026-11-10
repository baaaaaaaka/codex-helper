package machineregistry

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/baaaaaaaka/codex-helper/internal/sqlitestate"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// CacheDB owns one connection. Cache rows never contain a serialized object.
type CacheDB struct{ db *sql.DB }

func OpenCacheDB(path string) (*CacheDB, error) {
	if strings.TrimSpace(path) == "" {
		return &CacheDB{}, nil
	}
	path = cacheDatabasePath(path)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	db, err := sqlitestate.Open(path, false)
	if err != nil {
		return nil, err
	}
	if err = migrateRegistryCache(db, strings.TrimSuffix(path, ".sqlite")+".json"); err != nil {
		db.Close()
		return nil, err
	}

	return &CacheDB{db: db}, nil
}
func cacheDatabasePath(path string) string {
	if strings.EqualFold(filepath.Ext(path), ".json") {
		return strings.TrimSuffix(path, filepath.Ext(path)) + ".sqlite"
	}
	return path
}
func (s *CacheDB) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}
func cacheTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339Nano)
}
func (s *CacheDB) Load() (Cache, error) {
	if s.db == nil {
		return Cache{}, nil
	}
	return readCache(s.db.QueryRow(`SELECT schema_version,tenant_id_hash,user_id_hash,registry_key,external_id,registry_chat_id,meeting_id,meeting_end_date_time,registry_generation,slot_message_id,slot_machine_id,inbox_machine_id,inbox_external_id,inbox_chat_id,inbox_meeting_id,inbox_meeting_end_date_time,inbox_generation,inbox_validated_at,inbox_refreshed_at,inbox_next_refresh_at,slot_created_at,next_slot_rotation_at,capability_fingerprint,validated_at,refreshed_at,next_refresh_at FROM registry_cache WHERE id=1`))
}
func readCache(row *sql.Row) (Cache, error) {
	var v Cache
	var inbox_validated_at string
	var inbox_refreshed_at string
	var inbox_next_refresh_at string
	var slot_created_at string
	var next_slot_rotation_at string
	var validated_at string
	var refreshed_at string
	var next_refresh_at string
	err := row.Scan(&v.SchemaVersion, &v.TenantIDHash, &v.UserIDHash, &v.RegistryKey, &v.ExternalID, &v.RegistryChatID, &v.MeetingID, &v.MeetingEndDateTime, &v.RegistryGeneration, &v.SlotMessageID, &v.SlotMachineID, &v.InboxMachineID, &v.InboxExternalID, &v.InboxChatID, &v.InboxMeetingID, &v.InboxMeetingEndDateTime, &v.InboxGeneration, &inbox_validated_at, &inbox_refreshed_at, &inbox_next_refresh_at, &slot_created_at, &next_slot_rotation_at, &v.CapabilityFingerprint, &validated_at, &refreshed_at, &next_refresh_at)
	if err == sql.ErrNoRows {
		return Cache{}, nil
	}
	if err != nil {
		return Cache{}, err
	}
	if inbox_validated_at != "" {
		v.InboxValidatedAt, err = time.Parse(time.RFC3339Nano, inbox_validated_at)
		if err != nil {
			return Cache{}, fmt.Errorf("invalid registry inbox_validated_at: %w", err)
		}
	}
	if inbox_refreshed_at != "" {
		v.InboxRefreshedAt, err = time.Parse(time.RFC3339Nano, inbox_refreshed_at)
		if err != nil {
			return Cache{}, fmt.Errorf("invalid registry inbox_refreshed_at: %w", err)
		}
	}
	if inbox_next_refresh_at != "" {
		v.InboxNextRefreshAt, err = time.Parse(time.RFC3339Nano, inbox_next_refresh_at)
		if err != nil {
			return Cache{}, fmt.Errorf("invalid registry inbox_next_refresh_at: %w", err)
		}
	}
	if slot_created_at != "" {
		v.SlotCreatedAt, err = time.Parse(time.RFC3339Nano, slot_created_at)
		if err != nil {
			return Cache{}, fmt.Errorf("invalid registry slot_created_at: %w", err)
		}
	}
	if next_slot_rotation_at != "" {
		v.NextSlotRotationAt, err = time.Parse(time.RFC3339Nano, next_slot_rotation_at)
		if err != nil {
			return Cache{}, fmt.Errorf("invalid registry next_slot_rotation_at: %w", err)
		}
	}
	if validated_at != "" {
		v.ValidatedAt, err = time.Parse(time.RFC3339Nano, validated_at)
		if err != nil {
			return Cache{}, fmt.Errorf("invalid registry validated_at: %w", err)
		}
	}
	if refreshed_at != "" {
		v.RefreshedAt, err = time.Parse(time.RFC3339Nano, refreshed_at)
		if err != nil {
			return Cache{}, fmt.Errorf("invalid registry refreshed_at: %w", err)
		}
	}
	if next_refresh_at != "" {
		v.NextRefreshAt, err = time.Parse(time.RFC3339Nano, next_refresh_at)
		if err != nil {
			return Cache{}, fmt.Errorf("invalid registry next_refresh_at: %w", err)
		}
	}
	return v, nil
}

const registryCacheDDL = `CREATE TABLE registry_cache(id INTEGER PRIMARY KEY CHECK(id=1),schema_version INTEGER NOT NULL,tenant_id_hash TEXT NOT NULL,user_id_hash TEXT NOT NULL,registry_key TEXT NOT NULL,external_id TEXT NOT NULL,registry_chat_id TEXT NOT NULL,meeting_id TEXT NOT NULL,meeting_end_date_time TEXT NOT NULL,registry_generation TEXT NOT NULL,slot_message_id TEXT NOT NULL,slot_machine_id TEXT NOT NULL,inbox_machine_id TEXT NOT NULL,inbox_external_id TEXT NOT NULL,inbox_chat_id TEXT NOT NULL,inbox_meeting_id TEXT NOT NULL,inbox_meeting_end_date_time TEXT NOT NULL,inbox_generation TEXT NOT NULL,inbox_validated_at TEXT NOT NULL,inbox_refreshed_at TEXT NOT NULL,inbox_next_refresh_at TEXT NOT NULL,slot_created_at TEXT NOT NULL,next_slot_rotation_at TEXT NOT NULL,capability_fingerprint TEXT NOT NULL,validated_at TEXT NOT NULL,refreshed_at TEXT NOT NULL,next_refresh_at TEXT NOT NULL)`
const registryCachePut = `INSERT INTO registry_cache(id,schema_version,tenant_id_hash,user_id_hash,registry_key,external_id,registry_chat_id,meeting_id,meeting_end_date_time,registry_generation,slot_message_id,slot_machine_id,inbox_machine_id,inbox_external_id,inbox_chat_id,inbox_meeting_id,inbox_meeting_end_date_time,inbox_generation,inbox_validated_at,inbox_refreshed_at,inbox_next_refresh_at,slot_created_at,next_slot_rotation_at,capability_fingerprint,validated_at,refreshed_at,next_refresh_at) VALUES(1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET schema_version=excluded.schema_version, tenant_id_hash=excluded.tenant_id_hash, user_id_hash=excluded.user_id_hash, registry_key=excluded.registry_key, external_id=excluded.external_id, registry_chat_id=excluded.registry_chat_id, meeting_id=excluded.meeting_id, meeting_end_date_time=excluded.meeting_end_date_time, registry_generation=excluded.registry_generation, slot_message_id=excluded.slot_message_id, slot_machine_id=excluded.slot_machine_id, inbox_machine_id=excluded.inbox_machine_id, inbox_external_id=excluded.inbox_external_id, inbox_chat_id=excluded.inbox_chat_id, inbox_meeting_id=excluded.inbox_meeting_id, inbox_meeting_end_date_time=excluded.inbox_meeting_end_date_time, inbox_generation=excluded.inbox_generation, inbox_validated_at=excluded.inbox_validated_at, inbox_refreshed_at=excluded.inbox_refreshed_at, inbox_next_refresh_at=excluded.inbox_next_refresh_at, slot_created_at=excluded.slot_created_at, next_slot_rotation_at=excluded.next_slot_rotation_at, capability_fingerprint=excluded.capability_fingerprint, validated_at=excluded.validated_at, refreshed_at=excluded.refreshed_at, next_refresh_at=excluded.next_refresh_at WHERE (schema_version,tenant_id_hash,user_id_hash,registry_key,external_id,registry_chat_id,meeting_id,meeting_end_date_time,registry_generation,slot_message_id,slot_machine_id,inbox_machine_id,inbox_external_id,inbox_chat_id,inbox_meeting_id,inbox_meeting_end_date_time,inbox_generation,inbox_validated_at,inbox_refreshed_at,inbox_next_refresh_at,slot_created_at,next_slot_rotation_at,capability_fingerprint,validated_at,refreshed_at,next_refresh_at) IS NOT (excluded.schema_version,excluded.tenant_id_hash,excluded.user_id_hash,excluded.registry_key,excluded.external_id,excluded.registry_chat_id,excluded.meeting_id,excluded.meeting_end_date_time,excluded.registry_generation,excluded.slot_message_id,excluded.slot_machine_id,excluded.inbox_machine_id,excluded.inbox_external_id,excluded.inbox_chat_id,excluded.inbox_meeting_id,excluded.inbox_meeting_end_date_time,excluded.inbox_generation,excluded.inbox_validated_at,excluded.inbox_refreshed_at,excluded.inbox_next_refresh_at,excluded.slot_created_at,excluded.next_slot_rotation_at,excluded.capability_fingerprint,excluded.validated_at,excluded.refreshed_at,excluded.next_refresh_at)`

func cacheArgs(v Cache) []any {
	return []any{v.SchemaVersion, v.TenantIDHash, v.UserIDHash, v.RegistryKey, v.ExternalID, v.RegistryChatID, v.MeetingID, v.MeetingEndDateTime, v.RegistryGeneration, v.SlotMessageID, v.SlotMachineID, v.InboxMachineID, v.InboxExternalID, v.InboxChatID, v.InboxMeetingID, v.InboxMeetingEndDateTime, v.InboxGeneration, cacheTime(v.InboxValidatedAt), cacheTime(v.InboxRefreshedAt), cacheTime(v.InboxNextRefreshAt), cacheTime(v.SlotCreatedAt), cacheTime(v.NextSlotRotationAt), v.CapabilityFingerprint, cacheTime(v.ValidatedAt), cacheTime(v.RefreshedAt), cacheTime(v.NextRefreshAt)}
}
func (s *CacheDB) Save(v Cache) (bool, error) {
	if s.db == nil {
		return false, nil
	}
	if v.SchemaVersion < 0 || v.SchemaVersion > 1 {
		return false, fmt.Errorf("unsupported registry cache version %d", v.SchemaVersion)
	}
	result, err := s.db.ExecContext(context.Background(), registryCachePut, cacheArgs(v)...)
	if err != nil {
		return false, err
	}
	n, err := result.RowsAffected()
	return n != 0, err
}
func (s Store) loadCache() (Cache, error) {
	if s.CacheDatabase != nil {
		return s.CacheDatabase.Load()
	}
	db, err := OpenCacheDB(s.CachePath)
	if err != nil {
		return Cache{}, err
	}
	defer db.Close()
	return db.Load()
}
func (s Store) SaveCache(v Cache) (bool, error) {
	if s.CacheDatabase != nil {
		return s.CacheDatabase.Save(v)
	}
	db, err := OpenCacheDB(s.CachePath)
	if err != nil {
		return false, err
	}
	defer db.Close()
	return db.Save(v)
}
func LoadCache(path string) (Cache, error) {
	db, err := OpenCacheDB(path)
	if err != nil {
		return Cache{}, err
	}
	defer db.Close()
	return db.Load()
}
func SaveCache(path string, v Cache) (bool, error) {
	db, err := OpenCacheDB(path)
	if err != nil {
		return false, err
	}
	defer db.Close()
	return db.Save(v)
}
