package userpath

import (
	"fmt"
	"strings"
)

func validateWindowsAccountEnvironment(environment []string, actualSID, expectedSID string) (string, string, error) {
	actualSID = strings.TrimSpace(actualSID)
	expectedSID = strings.TrimSpace(expectedSID)
	if actualSID == "" {
		return "", "", fmt.Errorf("Windows account token did not provide a SID")
	}
	if expectedSID != "" && !strings.EqualFold(expectedSID, actualSID) {
		return "", "", fmt.Errorf("Teams service token SID %s does not match configured target SID %s", actualSID, expectedSID)
	}
	pathValue, ok := EnvironmentValue(environment, "Path", true)
	if !ok || pathValue == "" {
		return "", "", fmt.Errorf("Windows account environment for SID %s does not contain Path", actualSID)
	}
	home, _ := EnvironmentValue(environment, "USERPROFILE", true)
	return pathValue, home, nil
}
