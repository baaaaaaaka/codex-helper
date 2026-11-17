package codexhistory

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// fileCacheKey captures both cheap metadata and stable file identity. Exact
// matches avoid all parsing; append paths additionally verify a bounded hash at
// the previous complete-line cursor before trusting an older cache row.
type fileCacheKey struct {
	Size          int64
	MtimeUnixNano int64
	Mode          uint32
	HasFileID     bool
	Dev           uint64
	DevKind       string // empty: native device/volume ID; otherwise explicitly tagged
	Ino           uint64
	HasCtime      bool
	CtimeUnixNano int64
}

var platformFileCacheKey = populatePlatformFileCacheKey

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func matchesFileInfo(path string, info os.FileInfo, key fileCacheKey) bool {
	if info == nil {
		return false
	}
	current := newFileCacheKey(path, info)
	if current.Size != key.Size || current.MtimeUnixNano != key.MtimeUnixNano || current.Mode != key.Mode {
		return false
	}
	if key.HasFileID && (!current.HasFileID || current.DevKind != key.DevKind || current.Dev != key.Dev || current.Ino != key.Ino) {
		return false
	}
	if key.HasCtime && (!current.HasCtime || current.CtimeUnixNano != key.CtimeUnixNano) {
		return false
	}
	return true
}

// The existing TEXT column can distinguish stable filesystem IDs from legacy
// device numbers without a schema reset. Old readers reject tagged IDs rather
// than accidentally trusting them as native device numbers.
func cacheDeviceID(key fileCacheKey) string {
	id := strconv.FormatUint(key.Dev, 10)
	if key.DevKind != "" {
		return key.DevKind + ":" + id
	}
	return id
}

func decodeCacheDeviceID(key *fileCacheKey, value string) {
	key.DevKind = ""
	if kind, id, ok := strings.Cut(value, ":"); ok {
		if kind != "ext4-fsid" {
			key.DevKind, key.Dev = "invalid", 0
			return
		}
		key.DevKind, value = kind, id
	}
	var err error
	key.Dev, err = strconv.ParseUint(value, 10, 64)
	if err != nil {
		key.DevKind = "invalid"
	}
}

func newFileCacheKey(path string, info os.FileInfo) fileCacheKey {
	key := fileCacheKey{}
	if info == nil {
		return key
	}
	key.Size = info.Size()
	key.MtimeUnixNano = info.ModTime().UnixNano()
	key.Mode = uint32(info.Mode())

	value := reflect.ValueOf(info.Sys())
	if !value.IsValid() {
		platformFileCacheKey(path, info, &key)
		return key
	}
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			platformFileCacheKey(path, info, &key)
			return key
		}
		value = value.Elem()
	}
	if value.Kind() == reflect.Struct {
		if dev, ok := statUintField(value, "Dev"); ok {
			if ino, ok := statUintField(value, "Ino"); ok {
				key.HasFileID = true
				key.Dev = dev
				key.Ino = ino
			}
		}
		if ctime, ok := statCtimeUnixNano(value); ok {
			key.HasCtime = true
			key.CtimeUnixNano = ctime
		}
	}
	platformFileCacheKey(path, info, &key)
	return key
}

func statUintField(value reflect.Value, name string) (uint64, bool) {
	field := value.FieldByName(name)
	if !field.IsValid() {
		return 0, false
	}
	switch field.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return field.Uint(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if field.Int() < 0 {
			return 0, false
		}
		return uint64(field.Int()), true
	default:
		return 0, false
	}
}

func statIntField(value reflect.Value, name string) (int64, bool) {
	field := value.FieldByName(name)
	if !field.IsValid() {
		return 0, false
	}
	switch field.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return field.Int(), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return int64(field.Uint()), true
	default:
		return 0, false
	}
}

func statTimespecUnixNano(value reflect.Value) (int64, bool) {
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return 0, false
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return 0, false
	}
	if sec, ok := statIntField(value, "Sec"); ok {
		if nsec, ok := statIntField(value, "Nsec"); ok {
			return sec*int64(time.Second) + nsec, true
		}
	}
	if sec, ok := statIntField(value, "Tv_sec"); ok {
		if nsec, ok := statIntField(value, "Tv_nsec"); ok {
			return sec*int64(time.Second) + nsec, true
		}
	}
	return 0, false
}

func statCtimeUnixNano(value reflect.Value) (int64, bool) {
	for _, name := range []string{"Ctim", "Ctimespec"} {
		field := value.FieldByName(name)
		if !field.IsValid() {
			continue
		}
		if ts, ok := statTimespecUnixNano(field); ok {
			return ts, true
		}
	}
	if sec, ok := statIntField(value, "Ctimesec"); ok {
		if nsec, ok := statIntField(value, "Ctimensec"); ok {
			return sec*int64(time.Second) + nsec, true
		}
	}
	if sec, ok := statIntField(value, "Ctime"); ok {
		return sec * int64(time.Second), true
	}
	return 0, false
}
