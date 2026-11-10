package sqlitestate

// Windows does not support os.File.Sync on a directory. The marker's own
// contents are flushed before its no-clobber link publication, matching the
// existing native metadata publication boundary on this platform.
func syncPublicationDirectory(string) error { return nil }
