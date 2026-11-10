package lease

import (
	"encoding/json"
	"fmt"
)

// Old generations are immutable provenance. Decode their fields without
// rewriting their slots, which could mask an old owner's heartbeat or claim.
func decodeLegacySlot(body []byte) (Record, bool, error) {
	var r Record
	if err := json.Unmarshal(body, &r); err != nil {
		return r, true, err
	}
	if r.V != 1 {
		return r, true, fmt.Errorf("lease: legacy record version %d", r.V)
	}
	return r, true, nil
}
