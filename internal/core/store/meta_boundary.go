package store

import "strings"

// This list is the review boundary for scalar metadata. New durable objects
// need concrete SQL APIs; an arbitrary key cannot reopen a JSON blob backend.
func scalarMetaKey(key string) bool {
	switch key {
	case "created_at", "migrated_from", "migration_summary", "migrated_at", "migrated_source_dir", "turn_key_prefix", "machine_id", "machine_label", "scope_id", "registry_owner_id", "owner_host", "paused", "paused_reason", "last_installed_tag", "workflow_enabled", "workflow_control_chat_id", "workflow_webhook_url_file", "engine_marker_selftest", "engine_next_label", "engine_mirror_initialized", "engine_legacy_rehashed", "engine_inbox_conv", "engine_running_version", "engine_control_user_title", "engine_update_next_check", "engine_update_skip", "engine_webhook_test", "engine_deleg_worker", "engine_default_model", "engine_default_effort", "engine_import_seq":
		return true
	}
	for _, prefix := range []string{"engine_announced:", "engine_user_title:", "engine_realign:", "engine_group:", "engine_discovered:", "engine_fork_child:", "engine_mirror_ns:", "engine_full_history:", "engine_imported:", "engine_question_marker:", "engine_resent:", "engine_deleg:", "engine_deleg_cancel:", "engine_deleg_thread:", "engine_deleg_remote:"} {
		if strings.HasPrefix(key, prefix) && len(key) > len(prefix) {
			return true
		}
	}
	return false
}
