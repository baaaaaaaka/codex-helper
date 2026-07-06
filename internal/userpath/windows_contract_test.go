package userpath

import (
	"strings"
	"testing"
)

func TestValidateWindowsAccountEnvironmentUsesCaseInsensitivePath(t *testing.T) {
	pathValue, home, err := validateWindowsAccountEnvironment([]string{
		"PATH=C:\\Users\\alice\\bin;C:\\Windows\\System32",
		"UserProfile=C:\\Users\\alice",
	}, "S-1-5-21-1000", "s-1-5-21-1000")
	if err != nil {
		t.Fatal(err)
	}
	if pathValue != "C:\\Users\\alice\\bin;C:\\Windows\\System32" || home != "C:\\Users\\alice" {
		t.Fatalf("path=%q home=%q", pathValue, home)
	}
}

func TestValidateWindowsAccountEnvironmentRejectsWrongSIDAndMissingPath(t *testing.T) {
	if _, _, err := validateWindowsAccountEnvironment([]string{"Path=C:\\Windows"}, "S-1-5-21-1000", "S-1-5-21-2000"); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("SID mismatch error = %v", err)
	}
	if _, _, err := validateWindowsAccountEnvironment([]string{"TEMP=C:\\Temp"}, "S-1-5-21-1000", ""); err == nil || !strings.Contains(err.Error(), "does not contain Path") {
		t.Fatalf("missing Path error = %v", err)
	}
}
