//go:build !windows

package cli

import (
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"syscall"
)

var execIdentityStat = os.Stat
var execIdentityLookupUserByID = user.LookupId
var execIdentityLookupUserGroups = func(u *user.User) ([]string, error) {
	if u == nil {
		return nil, nil
	}
	return u.GroupIds()
}
var execIdentityChown = os.Chown

func execIdentityForHome(home string) (*execIdentity, error) {
	home = filepath.Clean(strings.TrimSpace(home))
	if home == "" {
		return nil, nil
	}
	info, err := execIdentityStat(home)
	if err != nil {
		return nil, err
	}

	uid, gid, ok := statOwnerIDs(info)
	if !ok {
		return nil, nil
	}

	identity := &execIdentity{
		UID:         uid,
		GID:         gid,
		GroupsKnown: uid == 0,
		Home:        home,
	}
	if uid == 0 {
		return identity, nil
	}

	if u, err := execIdentityLookupUserByID(strconv.FormatUint(uint64(uid), 10)); err == nil && u != nil {
		return execIdentityForUser(u, home)
	}

	return nil, nil
}

func execIdentityForUser(u *user.User, homeOverride string) (*execIdentity, error) {
	if u == nil {
		return nil, nil
	}

	uid, ok := parseUint32(strings.TrimSpace(u.Uid))
	if !ok {
		return nil, nil
	}
	gid, ok := parseUint32(strings.TrimSpace(u.Gid))
	if !ok {
		return nil, nil
	}

	identity := &execIdentity{
		UID:      uid,
		GID:      gid,
		Username: strings.TrimSpace(u.Username),
	}
	if home := strings.TrimSpace(homeOverride); home != "" {
		identity.Home = filepath.Clean(home)
	} else if home := strings.TrimSpace(u.HomeDir); home != "" {
		identity.Home = filepath.Clean(home)
	}

	groupIDs, err := execIdentityLookupUserGroups(u)
	if err != nil {
		return nil, err
	}
	identity.Groups, err = parseSupplementaryGroupIDs(groupIDs, gid)
	if err != nil {
		return nil, err
	}
	identity.GroupsKnown = true
	return identity, nil
}

func parseSupplementaryGroupIDs(groupIDs []string, primaryGID uint32) ([]uint32, error) {
	if len(groupIDs) == 0 {
		return nil, nil
	}
	seen := make(map[uint32]struct{}, len(groupIDs))
	out := make([]uint32, 0, len(groupIDs))
	for _, raw := range groupIDs {
		gid, ok := parseUint32(strings.TrimSpace(raw))
		if !ok {
			return nil, fmt.Errorf("invalid supplementary gid %q", raw)
		}
		if gid == primaryGID {
			continue
		}
		if _, ok := seen[gid]; ok {
			continue
		}
		seen[gid] = struct{}{}
		out = append(out, gid)
	}
	return out, nil
}

func ensurePathOwnedByIdentity(path string, identity *execIdentity) error {
	if identity == nil || identity.UID == 0 || strings.TrimSpace(path) == "" {
		return nil
	}
	return execIdentityChown(path, int(identity.UID), int(identity.GID))
}

func applyPlatformExecIdentity(cmd *exec.Cmd, envVars []string, identity *execIdentity) ([]string, error) {
	if identity == nil || identity.UID == 0 {
		return envVars, nil
	}
	attr := cmd.SysProcAttr
	if attr == nil {
		attr = &syscall.SysProcAttr{}
	}
	if !identity.GroupsKnown {
		return envVars, fmt.Errorf("supplementary groups are unavailable for target uid %d", identity.UID)
	}
	attr.Credential = &syscall.Credential{
		Uid:    identity.UID,
		Gid:    identity.GID,
		Groups: append([]uint32(nil), identity.Groups...),
	}
	cmd.SysProcAttr = attr
	return envVars, nil
}

func statOwnerIDs(info os.FileInfo) (uint32, uint32, bool) {
	if info == nil || info.Sys() == nil {
		return 0, 0, false
	}
	value := reflect.ValueOf(info.Sys())
	if !value.IsValid() {
		return 0, 0, false
	}
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return 0, 0, false
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return 0, 0, false
	}

	uid, ok := statUint32Field(value, "Uid", "UID")
	if !ok {
		return 0, 0, false
	}
	gid, ok := statUint32Field(value, "Gid", "GID")
	if !ok {
		return 0, 0, false
	}
	return uid, gid, true
}

func statUint32Field(v reflect.Value, names ...string) (uint32, bool) {
	for _, name := range names {
		field := v.FieldByName(name)
		if !field.IsValid() {
			continue
		}
		switch field.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
			return uint32(field.Uint()), true
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			if field.Int() < 0 {
				return 0, false
			}
			return uint32(field.Int()), true
		}
	}
	return 0, false
}

func execIdentityRequiredError(home string) error {
	return &execIdentityRequired{home: home}
}
